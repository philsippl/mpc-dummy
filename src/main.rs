use clap::{Parser, Subcommand};
use eyre::{Context, ContextCompat, bail};
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
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::{
    cmp::min,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tokio::{sync::mpsc, task};
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

#[derive(Clone)]
struct SessionPool {
    inner: Arc<tokio::sync::Mutex<Vec<Session>>>,
}

impl SessionPool {
    fn new(sessions: Vec<Session>) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(sessions)),
        }
    }

    async fn checkout(&self) -> eyre::Result<SessionLease> {
        let mut guard = self.inner.lock().await;
        let session = guard.pop().context("No sessions available")?;
        Ok(SessionLease {
            pool: self.clone(),
            session: Some(session),
        })
    }

    async fn len(&self) -> usize {
        self.inner.lock().await.len()
    }

    async fn checkin(&self, session: Session) {
        self.inner.lock().await.push(session);
    }
}

struct SessionLease {
    pool: SessionPool,
    session: Option<Session>,
}

impl SessionLease {
    fn session_mut(&mut self) -> &mut Session {
        self.session.as_mut().expect("session already returned")
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let pool = self.pool.clone();
            if let Ok(mut guard) = pool.inner.try_lock() {
                guard.push(session);
                return;
            }
            tokio::spawn(async move {
                pool.checkin(session).await;
            });
        }
    }
}

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
    // Running statistics
    total_comparisons: usize,
    total_requests: usize,
    first_request_time: Option<Instant>,
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

        Ok(Self {
            _networking: networking, // Keep networking handle alive
            cancellation_token,
            sessions: SessionPool::new(sessions),
        })
    }
}

impl Actor {
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
            first_request_time: None,
        })
    }

    async fn run_comparison(&mut self, vector: SecretSharedVector) -> eyre::Result<()> {
        tracing::info!("Running comparison for actor {}", self.party_index);

        let now = Instant::now();

        let mut preprocessed_vector = vector.clone();
        preprocessed_vector.multiply_lagrange_coeffs(self.party_index + 1);

        let num_network_workers = min(self.network.sessions.len().await, num_cpus::get_physical());
        if num_network_workers == 0 {
            bail!("No sessions available for comparison");
        }

        // Phase 1: CPU computation - single par_iter over entire DB
        let db = Arc::clone(&self.db);
        let all_distances = task::spawn_blocking(move || {
            db.par_iter()
                .map(|db_vec| udot(&preprocessed_vector.0, db_vec))
                .collect::<Vec<_>>()
        })
        .await?;

        tracing::info!(
            "Actor {} completed CPU phase: {} distances computed in {:?}",
            self.party_index,
            all_distances.len(),
            now.elapsed()
        );

        let now2 = Instant::now();

        // Phase 2: Network operations - distribute to workers
        let (result_tx, mut result_rx) = mpsc::channel::<(usize, Vec<bool>)>(num_network_workers);

        let mut worker_handles = Vec::new();

        for worker_id in 0..num_network_workers {
            let lease = self.network.sessions.checkout().await?;
            let chunk_start = worker_id * all_distances.len() / num_network_workers;
            let chunk_end = ((worker_id + 1) * all_distances.len() / num_network_workers)
                .min(all_distances.len());
            let chunk_distances = all_distances[chunk_start..chunk_end].to_vec();
            let result_tx = result_tx.clone();

            let handle = task::spawn(async move {
                let mut lease = lease;

                // Convert to replicated shares
                let mut chunk_distances = galois_ring_to_rep3(
                    lease.session_mut(),
                    RingElement::convert_vec_rev(chunk_distances),
                )
                .await?;

                // Subtract threshold
                {
                    let session = lease.session_mut();
                    chunk_distances
                        .iter_mut()
                        .for_each(|share| sub_pub(session, share, RingElement(THRESHOLD)));
                }

                // Network: Compare to zero and open results
                let chunk_results =
                    lte_threshold_and_open_u16(lease.session_mut(), &chunk_distances).await?;

                // Send results back
                let _ = result_tx.send((worker_id, chunk_results)).await;

                eyre::Ok(())
            });
            worker_handles.push(handle);
        }
        drop(result_tx);

        // Collect results from workers
        let mut chunk_results = vec![Vec::new(); num_network_workers];
        while let Some((worker_id, results)) = result_rx.recv().await {
            chunk_results[worker_id] = results;
        }

        // Wait for all workers to finish
        for handle in worker_handles {
            handle.await??;
        }

        let results = chunk_results.into_iter().flatten().collect_vec();

        // Update running statistics
        if self.first_request_time.is_none() {
            self.first_request_time = Some(now);
        }
        self.total_comparisons += results.len();
        self.total_requests += 1;

        // Calculate running average
        let total_elapsed = self.first_request_time.unwrap().elapsed();
        let running_avg_comp_per_sec = self.total_comparisons as f64 / total_elapsed.as_secs_f64();

        tracing::info!(
            "Actor {} network phase completed in {:?}",
            self.party_index,
            now2.elapsed()
        );

        // Log results
        tracing::info!(
            "Actor {} comparison results[0]: {:?} (len: {})",
            self.party_index,
            results[0],
            results.len()
        );

        tracing::info!(
            "Actor {} comparison completed in {:?} ({:.2}M comp/s)",
            self.party_index,
            now.elapsed(),
            (results.len() as f64) / now.elapsed().as_secs_f64() / 1e6
        );

        tracing::info!(
            "Actor {} running average: {:.2}M comp/s (total: {} comparisons, {} requests, {:?} elapsed)",
            self.party_index,
            running_avg_comp_per_sec / 1e6,
            self.total_comparisons,
            self.total_requests,
            total_elapsed
        );

        if results.len() != self.db.len() {
            panic!(
                "Result length mismatch: expected {}, got {}",
                self.db.len(),
                results.len()
            );
        }

        if results.iter().any(|&r| r == false) {
            panic!(
                "Some comparisons resulted in false, which is unexpected given the database and query values."
            );
        }

        Ok(())
    }

    async fn run(&mut self) -> eyre::Result<()> {
        while let Some(command) = self.command_receiver.recv().await {
            match command {
                ActorCommand::Comparison(vector) => {
                    tracing::info!("Actor {} received command", self.party_index);
                    self.run_comparison(vector).await?;
                }
            }
        }

        sleep(Duration::from_secs(5)).await;
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
