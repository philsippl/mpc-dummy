use clap::Parser;
use deadpool::unmanaged::Pool;
use eyre::{Context, ContextCompat};
use hdrhistogram::Histogram;
use iris_mpc_common::galois::degree4::{GaloisRingElement, ShamirGaloisRingShare, basis};
use iris_mpc_cpu::{
    execution::{
        hawk_main::HawkArgs,
        local::generate_local_identities,
        player::{Role, RoleAssignment},
        session::{NetworkSession, Session},
    },
    network::tcp::{NetworkHandle, NetworkHandleArgs, build_network_handle},
    protocol::ops::{galois_ring_to_rep3, lt_zero_and_open_u16, setup_replicated_prf, sub_pub},
    shares::RingElement,
};
use itertools::Itertools;
use rand::{CryptoRng, Rng, SeedableRng, rngs::StdRng};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    cmp::{max, min},
    panic,
    sync::Arc,
    time::Instant,
};
use tokio::time::sleep;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

const VECTOR_SIZE: usize = 512;
const THRESHOLD: u16 = 1000;

// Defaults, can be overridden via CLI args or env vars
const MAX_DB_SIZE: usize = 10_000_000;
const SESSION_PER_REQUEST: usize = 4;
const CONNECTION_PARALLELISM: usize = 1;
const REQUEST_PARALLELISM: usize = 1;

#[derive(Parser, Debug)]
struct Cli {
    #[arg(short, long, env = "PARTY_INDEX")]
    party_index: usize,

    #[arg(short, long, env = "ADDRESSES", value_delimiter = ',')]
    addresses: Vec<String>,

    #[arg(long, env = "MAX_DB", default_value_t = MAX_DB_SIZE)]
    max_db: usize,

    #[arg(long, env = "SESSION_PER_REQUEST", default_value_t = SESSION_PER_REQUEST)]
    session_per_request: usize,

    #[arg(long, env = "CONNECTION_PARALLELISM", default_value_t = CONNECTION_PARALLELISM)]
    connection_parallelism: usize,

    #[arg(long, env = "REQUEST_PARALLELISM", default_value_t = REQUEST_PARALLELISM)]
    request_parallelism: usize,
}

#[derive(Clone)]
struct SecretSharedVector([u16; VECTOR_SIZE]);

#[derive(Clone)]
struct Vector([i8; VECTOR_SIZE]);

type SessionPool = Pool<Session>;

struct Network {
    _networking: Box<dyn NetworkHandle>,
    cancellation_token: CancellationToken,
    sessions: SessionPool,
}

enum ActorCommand {
    Comparison {
        index: usize,
        vector: SecretSharedVector,
    },
}

struct Actor {
    db: Arc<Vec<[u16; VECTOR_SIZE]>>,
    party_index: usize,
    network: Network,
    command_receiver: mpsc::UnboundedReceiver<ActorCommand>,
    results_sender: mpsc::UnboundedSender<(usize, Vec<bool>)>,
    total_comparisons: usize,
    total_requests: usize,
    total_hist: Histogram<u64>,
}

fn udot(a: &[u16], b: &[u16]) -> u16 {
    a.iter()
        .zip(b.iter())
        .map(|(&a, &b)| u16::wrapping_mul(a, b))
        .fold(0_u16, u16::wrapping_add)
}

impl SecretSharedVector {
    fn default() -> Self {
        SecretSharedVector([0u16; VECTOR_SIZE])
    }

    pub fn multiply_lagrange_coeffs(&mut self, id: usize) {
        let lagrange_coeffs = ShamirGaloisRingShare::deg_2_lagrange_polys_at_zero();
        for i in (0..self.0.len()).step_by(4) {
            let element = GaloisRingElement::<basis::Monomial>::from_coefs([
                self.0[i],
                self.0[i + 1],
                self.0[i + 2],
                self.0[i + 3],
            ]);
            // include lagrange coeffs
            let element: GaloisRingElement<basis::Monomial> = element * lagrange_coeffs[id - 1];
            let element = element.to_basis_B();
            self.0[i] = element.coefs[0];
            self.0[i + 1] = element.coefs[1];
            self.0[i + 2] = element.coefs[2];
            self.0[i + 3] = element.coefs[3];
        }
    }
}

impl Vector {
    fn random<R: CryptoRng + Rng>(rng: &mut R) -> Self {
        let mut vec = [0i8; VECTOR_SIZE];
        for element in &mut vec {
            *element = rng.gen_range(-8..7) // int4 range
        }
        Vector(vec)
    }

    fn dot(&self, other: &Vector) -> i16 {
        self.0
            .iter()
            .zip(other.0.iter())
            .map(|(&a, &b)| (a as i16) * (b as i16))
            .sum()
    }

    pub fn secret_share<R: CryptoRng + Rng>(&self, rng: &mut R) -> [SecretSharedVector; 3] {
        let mut shares = [
            SecretSharedVector::default(),
            SecretSharedVector::default(),
            SecretSharedVector::default(),
        ];
        for i in (0..VECTOR_SIZE).step_by(4) {
            let element = GaloisRingElement::<basis::A>::from_coefs([
                self.0[i] as u16,
                self.0[i + 1] as u16,
                self.0[i + 2] as u16,
                self.0[i + 3] as u16,
            ]);
            let element = element.to_monomial();
            let share = ShamirGaloisRingShare::encode_3_mat(&element.coefs, rng);
            for j in 0..3 {
                shares[j].0[i] = share[j].y.coefs[0];
                shares[j].0[i + 1] = share[j].y.coefs[1];
                shares[j].0[i + 2] = share[j].y.coefs[2];
                shares[j].0[i + 3] = share[j].y.coefs[3];
            }
        }
        shares
    }
}

impl Network {
    async fn new(
        party_index: usize,
        addresses: Vec<String>,
        connection_parallelism: usize,
        request_parallelism: usize,
        sessions_per_request: usize,
    ) -> eyre::Result<Network> {
        let identities = generate_local_identities();
        let role_assignments: RoleAssignment = identities
            .iter()
            .enumerate()
            .map(|(index, id)| (Role::new(index), id.clone()))
            .collect();

        // abuse the hawk args struct for now
        let args = HawkArgs {
            party_index,
            addresses,
            request_parallelism,
            connection_parallelism,
            hnsw_param_M: 0,
            hnsw_param_ef_search: 0,
            hnsw_param_ef_constr: 0,
            disable_persistence: false,
            hnsw_prf_key: None,
            tls: None,
            n_buckets: 0,
            match_distances_buffer_size: 0,
            numa: false,
        };

        let cancellation_token = CancellationToken::new();

        // TODO: encapsulate networking setup in a function
        let mut networking = build_network_handle(
            NetworkHandleArgs {
                party_index: args.party_index,
                addresses: args.addresses.clone(),
                connection_parallelism: args.connection_parallelism,
                request_parallelism: args.request_parallelism,
                sessions_per_request,
                tls: None,
            },
            cancellation_token.child_token(),
        )
        .await?;

        let networking_sessions = networking.make_network_sessions().await?;

        let mut sessions = Vec::new();
        // todo parallelize session setup
        for mut network_session in networking_sessions {
            let my_session_seed = rand::thread_rng().r#gen();
            let prf = setup_replicated_prf(&mut network_session, my_session_seed).await?;
            let session = Session {
                network_session,
                prf,
            };
            sessions.push(session);
        }
        tracing::info!("Networking sessions established.");

        tracing::info!(
            "Established {} sessions for party {}",
            sessions.len(),
            party_index
        );

        // Create unmanaged deadpool with pre-created sessions
        let pool = Pool::from(sessions);

        Ok(Self {
            _networking: networking, // Keep networking handle alive
            cancellation_token,
            sessions: pool,
        })
    }
}

impl Actor {
    fn print_hist(name: &str, hist: &Histogram<u64>) {
        tracing::info!(
            "{:12} min={:?} max={:?} mean={:?}",
            name,
            std::time::Duration::from_micros(hist.min()),
            std::time::Duration::from_micros(hist.max()),
            std::time::Duration::from_micros(hist.mean() as u64)
        );
    }

    #[allow(clippy::too_many_arguments)]
    async fn new(
        party_index: usize,
        addresses: Vec<String>,
        command_receiver: mpsc::UnboundedReceiver<ActorCommand>,
        results_sender: mpsc::UnboundedSender<(usize, Vec<bool>)>,
        connection_parallelism: usize,
        request_parallelism: usize,
        sessions_per_request: usize,
        max_db: usize,
    ) -> eyre::Result<Self> {
        let db = Arc::new(vec![[0u16; VECTOR_SIZE]; max_db]);
        let network = Network::new(
            party_index,
            addresses.clone(),
            connection_parallelism,
            request_parallelism,
            sessions_per_request,
        )
        .await?;
        Ok(Self {
            db,
            party_index,
            network,
            command_receiver,
            results_sender,
            total_comparisons: 0,
            total_requests: 0,
            total_hist: Histogram::new(3).unwrap(),
        })
    }

    fn fill_db_with_random_shares(&mut self) -> eyre::Result<Vec<Vector>> {
        let db_mut = Arc::get_mut(&mut self.db).context("Failed to get mutable db reference")?;
        let party_index = self.party_index;

        let results: Vec<(usize, [u16; VECTOR_SIZE], Vector)> = (0..db_mut.len())
            .into_par_iter()
            .map(|index| {
                let mut rng = StdRng::seed_from_u64(index as u64);
                let vector = Vector::random(&mut rng);
                let shares = vector.secret_share(&mut rng);
                (index, shares[party_index].0, vector)
            })
            .collect();

        let mut vectors = Vec::with_capacity(db_mut.len());
        for (index, share, vector) in results {
            db_mut[index] = share;
            vectors.push(vector);
        }

        Ok(vectors)
    }

    async fn run_comparison(
        &mut self,
        index: usize,
        vector: SecretSharedVector,
    ) -> eyre::Result<()> {
        let start_total = Instant::now();

        let mut preprocessed_vector = vector.clone();
        preprocessed_vector.multiply_lagrange_coeffs(self.party_index + 1);
        let shared_vector = Arc::new(preprocessed_vector);

        let num_network_workers = self.network.sessions.status().size;

        let db = Arc::clone(&self.db);
        let db_len = db.len();
        if db_len == 0 {
            return Ok(());
        }

        let chunk_size = max(1, db_len.div_ceil(num_network_workers * 4));
        let chunk_bounds = (0..db_len)
            .step_by(chunk_size)
            .map(|start| (start, min(start + chunk_size, db_len)))
            .collect_vec();
        let chunk_count = chunk_bounds.len();

        let worker_count = min(num_network_workers, chunk_count);
        let (result_tx, mut result_rx) = mpsc::channel::<(usize, Vec<bool>)>(chunk_count);

        // Fetch all sessions and sort to guarantee consistent assignment
        let mut sessions = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            sessions.push(self.network.sessions.get().await?);
        }
        sessions.sort_by_key(|s| s.network_session.session_id);

        // Start network workers
        let mut worker_senders = Vec::with_capacity(worker_count);
        let mut network_handles = Vec::with_capacity(worker_count);
        for mut session in sessions {
            let (worker_tx, mut worker_rx) = mpsc::unbounded_channel::<(usize, Vec<u16>)>();
            worker_senders.push(worker_tx);
            let result_tx = result_tx.clone();
            network_handles.push(tokio::spawn(async move {
                while let Some((chunk_id, distances)) = worker_rx.recv().await {
                    let mut chunk_distances =
                        galois_ring_to_rep3(&mut session, RingElement::convert_vec_rev(distances))
                            .await?;

                    chunk_distances.iter_mut().for_each(|share| {
                        sub_pub(&mut session, share, RingElement(THRESHOLD));
                    });

                    let chunk_results =
                        lt_zero_and_open_u16(&mut session, &chunk_distances).await?;

                    result_tx.send((chunk_id, chunk_results)).await?;
                }
                eyre::Result::<()>::Ok(())
            }));
        }
        drop(result_tx);

        // Start CPU workers
        let mut chunk_results = vec![Vec::new(); chunk_count];
        let mut cpu_tasks = JoinSet::new();
        for (chunk_id, (chunk_start, chunk_end)) in chunk_bounds.into_iter().enumerate() {
            let query = Arc::clone(&shared_vector);
            let db = Arc::clone(&db);
            cpu_tasks.spawn_blocking(move || {
                let distances = db[chunk_start..chunk_end]
                    .iter()
                    .map(|db_vec| udot(&query.0, db_vec))
                    .collect::<Vec<_>>();
                (chunk_id, distances)
            });
        }

        // Coordinate CPU and network workers
        // We need to ensure that chunks are sent to network workers in the same order on all parties
        // Some CPU workers might finish out-of-order, so we buffer them here
        let mut expected_chunks: Vec<usize> = (0..worker_count).collect();
        let mut buffered_chunks: std::collections::HashMap<usize, Vec<u16>> =
            std::collections::HashMap::new();

        while let Some(task_result) = cpu_tasks.join_next().await {
            let (chunk_id, distances) = task_result?;

            // Buffer this chunk
            buffered_chunks.insert(chunk_id, distances);

            // Try to send any buffered chunks that are now in order
            for worker_idx in 0..worker_count {
                while let Some(distances) = buffered_chunks.remove(&expected_chunks[worker_idx]) {
                    let chunk_id = expected_chunks[worker_idx];
                    let sender = worker_senders[worker_idx].clone();
                    sender.send((chunk_id, distances))?;
                    expected_chunks[worker_idx] += worker_count;
                }
            }
        }
        drop(worker_senders);

        while let Some((chunk_id, results)) = result_rx.recv().await {
            chunk_results[chunk_id] = results;
        }

        for handle in network_handles {
            handle.await??;
        }

        let results = chunk_results.into_iter().flatten().collect_vec();
        let total_us = start_total.elapsed().as_micros() as u64;

        // Record timings
        self.total_hist.record(total_us).ok();
        self.total_comparisons += results.len();
        self.total_requests += 1;

        // Print per-request summary
        tracing::info!(
            "Actor {} | Total: {:?} | {:.2}M comp/s",
            self.party_index,
            std::time::Duration::from_micros(total_us),
            (results.len() as f64) / (total_us as f64 / 1e6) / 1e6
        );

        // Sanity check
        if results.len() != self.db.len() {
            panic!(
                "Result length mismatch: expected {}, got {}",
                self.db.len(),
                results.len()
            );
        }

        // Send results through channel
        self.results_sender.send((index, results))?;

        Ok(())
    }

    async fn run(&mut self) -> eyre::Result<()> {
        while let Some(command) = self.command_receiver.recv().await {
            match command {
                ActorCommand::Comparison { index, vector } => {
                    self.run_comparison(index, vector).await?;
                }
            }
        }

        // Print final summary
        tracing::info!("═══════════════════════════════════════════════════════");
        tracing::info!(
            "Actor {} FINAL SUMMARY ({} requests)",
            self.party_index,
            self.total_requests
        );
        Self::print_hist("Total:", &self.total_hist);
        tracing::info!("═══════════════════════════════════════════════════════");

        sleep(std::time::Duration::from_secs(5)).await;
        self.network.cancellation_token.cancel();

        Ok(())
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    run(
        cli.party_index,
        cli.addresses,
        cli.max_db,
        cli.connection_parallelism,
        cli.request_parallelism,
        cli.session_per_request,
    )
    .await
}

async fn run(
    party_index: usize,
    addresses: Vec<String>,
    max_db: usize,
    connection_parallelism: usize,
    request_parallelism: usize,
    session_per_request: usize,
) -> eyre::Result<()> {
    tracing::info!("Running party {} of {}", party_index, addresses.len());
    tracing::info!(
        "Config: max_db={}, connection_parallelism={}, request_parallelism={}, session_per_request={}",
        max_db,
        connection_parallelism,
        request_parallelism,
        session_per_request
    );

    let (command_sender, command_receiver) = mpsc::unbounded_channel();
    let (results_sender, mut results_receiver) = mpsc::unbounded_channel();

    let mut actor = Actor::new(
        party_index,
        addresses,
        command_receiver,
        results_sender,
        connection_parallelism,
        request_parallelism,
        session_per_request,
        max_db,
    )
    .await?;

    tracing::info!("Filling database with random shares...");

    let plain_db = actor.fill_db_with_random_shares()?;

    tracing::info!("Actor {} initialized and ready", party_index);

    tokio::spawn(async move {
        for i in 0..100 {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let vector = Vector::random(&mut rng);
            let shares = vector.secret_share(&mut rng);

            if let Err(e) = command_sender.send(ActorCommand::Comparison {
                index: i,
                vector: shares[party_index].clone(),
            }) {
                tracing::error!("Failed to send comparison command {}: {}", i, e);
                break;
            }
            tracing::info!("Sent comparison request {} to actor {}", i + 1, party_index);
        }
    });

    // Spawn task to receive and process results
    tokio::spawn(async move {
        while let Some((index, results)) = results_receiver.recv().await {
            // Reconstruct the query vector for this request
            let query_vector = Vector::random(&mut StdRng::seed_from_u64(index as u64));

            for (db_index, &is_less_equal) in results.iter().enumerate() {
                let dot_product = plain_db[db_index].dot(&query_vector);
                let expected = dot_product < THRESHOLD as i16;
                if is_less_equal != expected {
                    tracing::error!(
                        "Mismatch on request {}, db index {}: expected {}, got {}",
                        index,
                        db_index,
                        expected,
                        is_less_equal
                    );
                    panic!("Result mismatch detected");
                }
            }
        }
        tracing::info!("Results receiver closed");
    });

    tracing::info!("Waiting for network connections and processing commands...");

    actor.run().await
}

#[cfg(test)]
mod tests {
    use crate::{Vector, udot};

    #[tokio::test]
    async fn test_galois_dot() {
        let mut rng = rand::thread_rng();
        let v1 = Vector::random(&mut rng);
        let v2 = Vector::random(&mut rng);

        let mut sv1 = v1.secret_share(&mut rand::thread_rng());
        let sv2 = v2.secret_share(&mut rand::thread_rng());

        let mut dot: u16 = 0;
        for i in 0..3 {
            sv1[i].multiply_lagrange_coeffs(i + 1);
            dot = u16::wrapping_add(dot, udot(&sv1[i].0, &sv2[i].0));
        }

        assert_eq!(dot, v1.dot(&v2) as u16);
    }
}
