use clap::{Parser, Subcommand};
use deadpool::unmanaged::Pool;
use eyre::{Context, bail};
use hdrhistogram::Histogram;
use iris_mpc_common::galois::degree4::{GaloisRingElement, ShamirGaloisRingShare, basis};
use iris_mpc_cpu::{
    execution::{
        hawk_main::HawkArgs,
        local::generate_local_identities,
        player::{Role, RoleAssignment},
        session::{NetworkSession, Session},
    },
    network::tcp::{NetworkHandle, build_network_handle},
    protocol::ops::{
        galois_ring_to_rep3, lte_threshold_and_open_u16, setup_replicated_prf, sub_pub,
    },
    shares::RingElement,
};
use itertools::Itertools;
use rand::{CryptoRng, Rng, SeedableRng, rngs::StdRng};
use std::{
    cmp::{max, min},
    sync::Arc,
    time::Instant,
};
use tokio::time::sleep;
use tokio::{
    sync::mpsc,
    task::{self, JoinSet},
};
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
    #[command(subcommand)]
    mode: Mode,

    #[arg(long, env = "MAX_DB", default_value_t = MAX_DB_SIZE)]
    max_db: usize,

    #[arg(long, env = "SESSION_PER_REQUEST", default_value_t = SESSION_PER_REQUEST)]
    session_per_request: usize,

    #[arg(long, env = "CONNECTION_PARALLELISM", default_value_t = CONNECTION_PARALLELISM)]
    connection_parallelism: usize,

    #[arg(long, env = "REQUEST_PARALLELISM", default_value_t = REQUEST_PARALLELISM)]
    request_parallelism: usize,
}

#[derive(Subcommand, Debug)]
enum Mode {
    Local,
    Remote {
        #[arg(short, long, env = "PARTY_INDEX")]
        party_index: usize,

        #[arg(short, long, env = "ADDRESSES", value_delimiter = ',')]
        addresses: Vec<String>,
    },
}

#[derive(Clone)]
struct SecretSharedVector([u16; VECTOR_SIZE]);

type SessionPool = Pool<Session>;

struct Network {
    _networking: Box<dyn NetworkHandle>,
    cancellation_token: CancellationToken,
    sessions: SessionPool,
}

enum ActorCommand {
    Comparison(SecretSharedVector),
}

struct Actor {
    db: Arc<Vec<[u16; VECTOR_SIZE]>>,
    party_index: usize,
    network: Network,
    command_receiver: mpsc::UnboundedReceiver<ActorCommand>,
    total_comparisons: usize,
    total_requests: usize,
    total_hist: Histogram<u64>,
    cpu_hist: Histogram<u64>,
    network_hist: Histogram<u64>,
    galois_ring_hist: Histogram<u64>,
    lte_threshold_hist: Histogram<u64>,
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

    pub fn create_shares<R: CryptoRng + Rng>(vector: &[u8; VECTOR_SIZE], rng: &mut R) -> [Self; 3] {
        let mut shares = [
            SecretSharedVector::default(),
            SecretSharedVector::default(),
            SecretSharedVector::default(),
        ];
        for i in (0..VECTOR_SIZE).step_by(4) {
            let element = GaloisRingElement::<basis::A>::from_coefs([
                vector[i] as u16,
                vector[i + 1] as u16,
                vector[i + 2] as u16,
                vector[i + 3] as u16,
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
        let role_assignments = std::sync::Arc::new(role_assignments);

        // abuse the hawk args struct for now
        let args = HawkArgs {
            party_index: party_index,
            addresses: addresses,
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
            &args,
            cancellation_token.child_token(),
            &identities,
            sessions_per_request,
        )
        .await?;

        let tcp_sessions = networking
            .as_mut()
            .make_sessions()
            .await
            .context("Making sessions")?;

        let networking_sessions = tcp_sessions
            .into_iter()
            .map(|tcp_session| NetworkSession {
                session_id: tcp_session.id(),
                role_assignments: role_assignments.clone(),
                networking: Box::new(tcp_session),
                own_role: Role::new(party_index),
            })
            .collect_vec();

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

    async fn new(
        party_index: usize,
        addresses: Vec<String>,
        command_receiver: mpsc::UnboundedReceiver<ActorCommand>,
        connection_parallelism: usize,
        request_parallelism: usize,
        sessions_per_request: usize,
        max_db: usize,
    ) -> eyre::Result<Self> {
        let db = Arc::new(vec![[1u16; VECTOR_SIZE]; max_db]);
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
            total_comparisons: 0,
            total_requests: 0,
            total_hist: Histogram::new(3).unwrap(),
            cpu_hist: Histogram::new(3).unwrap(),
            network_hist: Histogram::new(3).unwrap(),
            galois_ring_hist: Histogram::new(3).unwrap(),
            lte_threshold_hist: Histogram::new(3).unwrap(),
        })
    }

    async fn run_comparison(&mut self, vector: SecretSharedVector) -> eyre::Result<()> {
        let start_total = Instant::now();

        let mut preprocessed_vector = vector.clone();
        preprocessed_vector.multiply_lagrange_coeffs(self.party_index + 1);
        let shared_vector = Arc::new(preprocessed_vector);

        let num_network_workers = min(
            self.network.sessions.status().size,
            num_cpus::get_physical(),
        );
        if num_network_workers == 0 {
            bail!("No sessions available for comparison");
        }

        let db = Arc::clone(&self.db);
        let db_len = db.len();
        let target_chunks = max(1, num_network_workers * 8);
        let chunk_size = max(1, (db_len + target_chunks - 1) / target_chunks);

        let mut chunk_bounds = Vec::new();
        let mut offset = 0usize;
        while offset < db_len {
            let end = min(offset + chunk_size, db_len);
            chunk_bounds.push((offset, end));
            offset = end;
        }

        let chunk_count = chunk_bounds.len();
        if chunk_count == 0 {
            return Ok(());
        }

        let chunk_count = chunk_bounds.len();
        if chunk_count == 0 {
            return Ok(());
        }

        let worker_count = min(num_network_workers, chunk_count);
        let (result_tx, mut result_rx) =
            mpsc::channel::<(usize, Vec<bool>, u64, u64, u64)>(chunk_count);
        let mut worker_senders = Vec::with_capacity(worker_count);
        let mut network_handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (worker_tx, mut worker_rx) = mpsc::channel::<(usize, Vec<u16>, u64)>(max(
                1,
                (chunk_count + worker_count - 1) / worker_count,
            ));
            worker_senders.push(worker_tx);
            let result_tx = result_tx.clone();
            let mut session = self.network.sessions.get().await?;
            network_handles.push(tokio::spawn(async move {
                while let Some((chunk_id, distances, _cpu_us)) = worker_rx.recv().await {
                    let network_start = Instant::now();
                    let galois_start = Instant::now();
                    let mut chunk_distances =
                        galois_ring_to_rep3(&mut session, RingElement::convert_vec_rev(distances))
                            .await?;
                    let galois_us = galois_start.elapsed().as_micros() as u64;

                    chunk_distances.iter_mut().for_each(|share| {
                        sub_pub(&mut session, share, RingElement(THRESHOLD));
                    });

                    let lte_start = Instant::now();
                    let chunk_results =
                        lte_threshold_and_open_u16(&mut session, &chunk_distances).await?;
                    let lte_us = lte_start.elapsed().as_micros() as u64;
                    let network_us = network_start.elapsed().as_micros() as u64;

                    result_tx
                        .send((chunk_id, chunk_results, network_us, galois_us, lte_us))
                        .await
                        .context("Network stage failed to send chunk result")?;
                }
                eyre::Result::<()>::Ok(())
            }));
        }
        drop(result_tx);

        let mut cpu_times = vec![0u64; chunk_count];
        let mut chunk_results = vec![Vec::new(); chunk_count];
        let mut network_times = Vec::new();
        let mut galois_times = Vec::new();
        let mut lte_times = Vec::new();

        let mut cpu_tasks = JoinSet::new();
        for (chunk_id, (chunk_start, chunk_end)) in chunk_bounds.into_iter().enumerate() {
            let query = Arc::clone(&shared_vector);
            let db = Arc::clone(&db);
            cpu_tasks.spawn(async move {
                let handle = task::spawn_blocking(move || {
                    let cpu_start = Instant::now();
                    let distances = db[chunk_start..chunk_end]
                        .iter()
                        .map(|db_vec| udot(&query.0, db_vec))
                        .collect::<Vec<_>>();
                    let cpu_us = cpu_start.elapsed().as_micros() as u64;
                    (chunk_id, distances, cpu_us)
                });
                let result = handle
                    .await
                    .context("CPU worker join error within blocking task")?;
                eyre::Result::<_>::Ok(result)
            });
        }

        while let Some(task_result) = cpu_tasks.join_next().await {
            let (chunk_id, distances, cpu_us) =
                task_result.context("CPU producer task join error")??;
            cpu_times[chunk_id] = cpu_us;
            let worker_idx = chunk_id % worker_count;
            let sender = worker_senders[worker_idx].clone();
            sender
                .send((chunk_id, distances, cpu_us))
                .await
                .context("Failed to dispatch chunk to network worker")?;
        }
        drop(worker_senders);

        while let Some((chunk_id, results, network_us, galois_us, lte_us)) = result_rx.recv().await
        {
            chunk_results[chunk_id] = results;
            network_times.push(network_us);
            galois_times.push(galois_us);
            lte_times.push(lte_us);
        }

        for handle in network_handles {
            handle.await.context("Network worker join error")??;
        }

        let results = chunk_results.into_iter().flatten().collect_vec();
        let cpu_us = cpu_times.iter().max().copied().unwrap_or(0);
        let network_us = network_times.iter().max().copied().unwrap_or(0);
        let total_us = start_total.elapsed().as_micros() as u64;

        // Record sub-phase timings (use max across workers for consistency)
        if let Some(&max_galois) = galois_times.iter().max() {
            self.galois_ring_hist.record(max_galois).ok();
        }
        if let Some(&max_lte) = lte_times.iter().max() {
            self.lte_threshold_hist.record(max_lte).ok();
        }

        // Record timings
        self.total_hist.record(total_us).ok();
        self.cpu_hist.record(cpu_us).ok();
        self.network_hist.record(network_us).ok();
        self.total_comparisons += results.len();
        self.total_requests += 1;

        // Print per-request summary
        tracing::info!(
            "Actor {} | Total: {:?} (CPU: {:?}, Net: {:?}) | {:.2}M comp/s",
            self.party_index,
            std::time::Duration::from_micros(total_us),
            std::time::Duration::from_micros(cpu_us),
            std::time::Duration::from_micros(network_us),
            (results.len() as f64) / (total_us as f64 / 1e6) / 1e6
        );

        if results.len() != self.db.len() {
            panic!(
                "Result length mismatch: expected {}, got {}",
                self.db.len(),
                results.len()
            );
        }

        Ok(())
    }

    async fn run(&mut self) -> eyre::Result<()> {
        while let Some(command) = self.command_receiver.recv().await {
            match command {
                ActorCommand::Comparison(vector) => {
                    self.run_comparison(vector).await?;
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
        Self::print_hist("CPU:", &self.cpu_hist);
        Self::print_hist("Network:", &self.network_hist);
        Self::print_hist("  to_rep3:", &self.galois_ring_hist);
        Self::print_hist("  threshold:", &self.lte_threshold_hist);
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

    match cli.mode {
        Mode::Local => {
            run_local_mode(
                cli.max_db,
                cli.connection_parallelism,
                cli.request_parallelism,
                cli.session_per_request,
            )
            .await
        }
        Mode::Remote {
            party_index,
            addresses,
        } => {
            run_remote_mode(
                party_index,
                addresses,
                cli.max_db,
                cli.connection_parallelism,
                cli.request_parallelism,
                cli.session_per_request,
            )
            .await
        }
    }
}

async fn run_local_mode(
    max_db: usize,
    connection_parallelism: usize,
    request_parallelism: usize,
    session_per_request: usize,
) -> eyre::Result<()> {
    tracing::info!("Running in LOCAL mode (all actors on this machine)");
    tracing::info!(
        "Config: max_db={}, connection_parallelism={}, request_parallelism={}, session_per_request={}",
        max_db,
        connection_parallelism,
        request_parallelism,
        session_per_request
    );

    let addresses = vec![
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
        "127.0.0.1:7003".to_string(),
    ];

    let mut handles = Vec::new();
    let mut senders = Vec::new();

    tracing::info!("Starting actors...");

    for i in 0..3 {
        let addresses = addresses.clone();
        let (sender, receiver) = mpsc::unbounded_channel();
        senders.push(sender);
        handles.push(tokio::spawn(async move {
            Actor::new(
                i,
                addresses,
                receiver,
                connection_parallelism,
                request_parallelism,
                session_per_request,
                max_db,
            )
            .await?
            .run()
            .await
        }));
    }

    // Actors are running now, send them commands
    for _ in 0..10 {
        let query = [1u8; VECTOR_SIZE];
        let mut rng = rand::thread_rng();
        let shares = SecretSharedVector::create_shares(&query, &mut rng);

        for (index, sender) in senders.iter().enumerate() {
            sender.send(ActorCommand::Comparison(shares[index].clone()))?;
        }
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn run_remote_mode(
    party_index: usize,
    addresses: Vec<String>,
    max_db: usize,
    connection_parallelism: usize,
    request_parallelism: usize,
    session_per_request: usize,
) -> eyre::Result<()> {
    tracing::info!(
        "Running in REMOTE mode (party {} of {})",
        party_index,
        addresses.len()
    );
    tracing::info!("Addresses: {:?}", addresses);
    tracing::info!(
        "Config: max_db={}, connection_parallelism={}, request_parallelism={}, session_per_request={}",
        max_db,
        connection_parallelism,
        request_parallelism,
        session_per_request
    );

    let (sender, receiver) = mpsc::unbounded_channel();

    let mut actor = Actor::new(
        party_index,
        addresses,
        receiver,
        connection_parallelism,
        request_parallelism,
        session_per_request,
        max_db,
    )
    .await?;

    tracing::info!("Actor {} initialized and ready", party_index);

    tokio::spawn(async move {
        for i in 0..100 {
            let query = [1u8; VECTOR_SIZE];
            let mut rng = StdRng::seed_from_u64(42);
            let shares = SecretSharedVector::create_shares(&query, &mut rng);

            // Send the share corresponding to this party's index
            if let Err(e) = sender.send(ActorCommand::Comparison(shares[party_index].clone())) {
                tracing::error!("Failed to send comparison command {}: {}", i, e);
                break;
            }
            tracing::info!("Sent comparison request {} to actor {}", i + 1, party_index);
        }
    });

    tracing::info!("Waiting for network connections and processing commands...");

    // Run the actor (it will process commands from the channel)
    actor.run().await
}

#[cfg(test)]
mod tests {
    use crate::{SecretSharedVector, VECTOR_SIZE, udot};

    fn udot_u8(a: &[u8], b: &[u8]) -> u16 {
        a.iter()
            .zip(b.iter())
            .map(|(&a, &b)| u16::wrapping_mul(a as u16, b as u16))
            .fold(0_u16, u16::wrapping_add)
    }

    #[tokio::test]
    async fn test_galois_dot() {
        let v1 = [1u8; VECTOR_SIZE];
        let v2 = [2u8; VECTOR_SIZE];

        let dot_ref = udot_u8(&v1, &v2);

        let mut sv1 = SecretSharedVector::create_shares(&v1, &mut rand::thread_rng());
        let sv2 = SecretSharedVector::create_shares(&v2, &mut rand::thread_rng());

        let mut dot: u16 = 0;
        for i in 0..3 {
            sv1[i].multiply_lagrange_coeffs(i + 1);
            dot = u16::wrapping_add(dot, udot(&sv1[i].0, &sv2[i].0));
        }

        assert_eq!(dot, dot_ref);
    }
}
