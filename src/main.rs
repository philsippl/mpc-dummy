use std::time::Duration;

use eyre::{Context, Ok};
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
    protocol::ops::setup_replicated_prf,
    shares::RingElement,
};
use itertools::Itertools;
use rand::Rng;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const MAX_DB_SIZE: usize = 10;
const VECTOR_SIZE: usize = 512;

struct Network {
    networking: Box<dyn NetworkHandle>,
    cancellation_token: CancellationToken,
    sessions: Vec<Session>,
}

struct Actor {
    db: [[u16; VECTOR_SIZE]; MAX_DB_SIZE],
    party_index: usize,
    network: Network,
}

impl Network {
    async fn new(party_index: usize, addresses: Vec<String>) -> eyre::Result<Network> {
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
            request_parallelism: 1,
            connection_parallelism: 1,
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
        let mut networking =
            build_network_handle(&args, cancellation_token.child_token(), &identities, 8).await?;

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

        Ok(Self {
            networking,
            cancellation_token,
            sessions,
        })
    }
}

impl Actor {
    async fn new(party_index: usize, addresses: Vec<String>) -> eyre::Result<Self> {
        let db = [[0u16; VECTOR_SIZE]; MAX_DB_SIZE];
        let network = Network::new(party_index, addresses.clone()).await?;
        Ok(Self {
            db,
            party_index,
            network,
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

    async fn run(&mut self) -> eyre::Result<()> {
        self.connection_test().await?;

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

    let handles: Vec<_> = (0..3)
        .map(|i| {
            let addresses = addresses.clone();
            tokio::spawn(async move { Actor::new(i, addresses).await?.run().await })
        })
        .collect();

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
