use eyre::{Context, Ok};
use iris_mpc_common::galois::degree4::{GaloisRingElement, ShamirGaloisRingShare, basis};
use iris_mpc_cpu::{
    execution::{
        hawk_main::HawkArgs,
        local::generate_local_identities,
        player::{Role, RoleAssignment},
        session::{NetworkSession, Session},
    },
    network::{
        tcp::{NetworkHandle, build_network_handle},
        value::NetworkValue,
    },
    protocol::ops::{
        galois_ring_to_rep3, lte_threshold_and_open_u16, setup_replicated_prf, sub_pub,
    },
    shares::RingElement,
};
use itertools::Itertools;
use rand::{CryptoRng, Rng};
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tokio::{sync::mpsc, task};
use tokio_util::sync::CancellationToken;

const MAX_DB_SIZE: usize = 100_000;
const VECTOR_SIZE: usize = 512;
const THRESHOLD: u16 = 1000;
const SESSION_PER_REQUEST: usize = 4;
const CONNECTION_PARALLELISM: usize = 1;
const REQUEST_PARALLELISM: usize = 1;

#[derive(Clone)]
struct SecretSharedVector([u16; VECTOR_SIZE]);

struct Network {
    _networking: Box<dyn NetworkHandle>,
    cancellation_token: CancellationToken,
    sessions: Vec<Session>,
}

enum ActorCommand {
    Comparison(SecretSharedVector),
}

struct Actor {
    db: [[u16; VECTOR_SIZE]; MAX_DB_SIZE],
    party_index: usize,
    network: Network,
    command_receiver: mpsc::UnboundedReceiver<ActorCommand>,
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

        tracing::info!("Number of sessions available {}", sessions.len());

        Ok(Self {
            _networking: networking, // Keep networking handle alive
            cancellation_token,
            sessions,
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
    ) -> eyre::Result<Self> {
        println!("Initializing actor {}", party_index);
        let db = [[1u16; VECTOR_SIZE]; MAX_DB_SIZE];
        println!("Database initialized for actor {}", party_index);
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
        })
    }

    async fn connection_test(&mut self) -> eyre::Result<()> {
        let session = &mut self.network.sessions[0];
        session
            .network_session
            .send_next(NetworkValue::RingElement16(RingElement(
                self.party_index as u16,
            )))
            .await?;
        session
            .network_session
            .send_prev(NetworkValue::RingElement16(RingElement(
                self.party_index as u16,
            )))
            .await?;
        let next_response = session.network_session.receive_next().await?;
        if let NetworkValue::RingElement16(x) = next_response {
            if x.0 as usize != ((self.party_index + 1) % 3) {
                tracing::error!("Incorrect prev response value: {:?}", x);
            }
            tracing::info!("Received next response: {:?}", x);
        } else {
            tracing::error!("Unexpected response type for next_response");
        }
        let prev_response = session.network_session.receive_prev().await?;
        if let NetworkValue::RingElement16(x) = prev_response {
            if x.0 as usize != ((self.party_index + 2) % 3) {
                tracing::error!("Incorrect prev response value: {:?}", x);
            }
            tracing::info!("Received prev response: {:?}", x);
        } else {
            tracing::error!("Unexpected response type for prev_response");
        }
        tracing::info!("Anon stats server networking test complete.");

        Ok(())
    }

    async fn run_comparison(&mut self, vector: SecretSharedVector) -> eyre::Result<()> {
        tracing::info!("Running comparison for actor {}", self.party_index);
        let session = &mut self.network.sessions[0];

        let now = Instant::now();

        let mut preprocessed_vector = vector.clone();
        preprocessed_vector.multiply_lagrange_coeffs(self.party_index + 1);

        // Compute dot products against database
        let dot_now = Instant::now();
        let db = Arc::new(self.db);
        let pre = preprocessed_vector.0;
        let distances =
            task::spawn_blocking(move || db.par_iter().map(|db_vec| udot(&pre, db_vec)).collect())
                .await?;

        let dots_per_second = self.db.len() as f64 / dot_now.elapsed().as_secs_f64();

        // Convert results to replicated shares
        let mut distances =
            galois_ring_to_rep3(session, RingElement::convert_vec_rev(distances)).await?;

        // Subtract threshold
        distances
            .iter_mut()
            .for_each(|share| sub_pub(session, share, RingElement(THRESHOLD)));

        // Compare to zero and open results
        let results = lte_threshold_and_open_u16(session, &distances).await?;

        // Log results
        tracing::info!(
            "Actor {} comparison results: {:?}",
            self.party_index,
            results
        );

        tracing::info!(
            "Actor {} computed {} dot products in {:?} ({} dots/s)",
            self.party_index,
            self.db.len(),
            dot_now.elapsed(),
            dots_per_second
        );

        tracing::info!(
            "Actor {} comparison completed in {:?}",
            self.party_index,
            now.elapsed()
        );

        Ok(())
    }

    async fn run(&mut self) -> eyre::Result<()> {
        self.connection_test().await?;

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
    tracing_subscriber::fmt::init();

    let addresses = vec![
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
        "127.0.0.1:7003".to_string(),
    ];

    let mut handles = Vec::new();
    let mut senders = Vec::new();

    println!("Starting actors...");

    for i in 0..3 {
        let addresses = addresses.clone();
        let (sender, receiver) = mpsc::unbounded_channel();
        senders.push(sender);
        handles.push(tokio::spawn(async move {
            Actor::new(
                i,
                addresses,
                receiver,
                CONNECTION_PARALLELISM,
                REQUEST_PARALLELISM,
                SESSION_PER_REQUEST,
            )
            .await?
            .run()
            .await
        }));
    }

    // Actors are running now, send them commands
    let query = [1u8; VECTOR_SIZE];
    let mut rng = rand::thread_rng();
    let shares = SecretSharedVector::create_shares(&query, &mut rng);

    for (index, sender) in senders.iter().enumerate() {
        sender.send(ActorCommand::Comparison(shares[index].clone()))?;
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
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
