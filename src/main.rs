use eyre::Context;
use iris_mpc_cpu::{
    execution::{
        hawk_main::HawkArgs,
        local::generate_local_identities,
        player::{Role, RoleAssignment},
        session::{NetworkSession, Session},
    },
    network::{tcp::build_network_handle, value::NetworkValue},
    protocol::ops::setup_replicated_prf,
    shares::RingElement,
};
use itertools::Itertools;
use rand::Rng;
use tokio_util::sync::CancellationToken;

async fn run_actor(party_index: usize, addresses: Vec<String>) -> eyre::Result<()> {
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
    let ct = CancellationToken::new();

    // TODO: encapsulate networking setup in a function
    let mut networking = build_network_handle(&args, ct.child_token(), &identities, 8).await?;

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

    let session = &mut sessions[0];
    session
        .network_session
        .send_next(NetworkValue::RingElement16(RingElement(party_index as u16)))
        .await?;
    session
        .network_session
        .send_prev(NetworkValue::RingElement16(RingElement(party_index as u16)))
        .await?;
    let next_response = session.network_session.receive_next().await?;
    if let NetworkValue::RingElement16(x) = next_response {
        if x.0 as usize != ((party_index + 1) % 3) {
            tracing::error!("Incorrect prev response value: {:?}", x);
        }
        tracing::info!("Received next response: {:?}", x);
    } else {
        tracing::error!("Unexpected response type for next_response");
    }
    let prev_response = session.network_session.receive_prev().await?;
    if let NetworkValue::RingElement16(x) = prev_response {
        if x.0 as usize != ((party_index + 2) % 3) {
            tracing::error!("Incorrect prev response value: {:?}", x);
        }
        tracing::info!("Received prev response: {:?}", x);
    } else {
        tracing::error!("Unexpected response type for prev_response");
    }
    tracing::info!("Anon stats server networking test complete.");

    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt::init();

    let addresses = vec![
        "127.0.0.1:7001".to_string(),
        "127.0.0.1:7002".to_string(),
        "127.0.0.1:7003".to_string(),
    ];

    let handles = vec![
        tokio::spawn(run_actor(0, addresses.clone())),
        tokio::spawn(run_actor(1, addresses.clone())),
        tokio::spawn(run_actor(2, addresses.clone())),
    ];

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
