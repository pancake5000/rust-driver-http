use std::net::SocketAddr;
use std::sync::Arc;
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};

mod state;
mod handlers;
mod models;
mod tls;

use state::AppState;
use handlers::handle;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize session + prepared statements
    println!("Initializing Scylla session and application state...");
    let state = AppState::init().await?;
    let state = Arc::new(state);

    let make_svc = make_service_fn(move |_conn| {
        let state = state.clone();
        async move {
            Ok::<_, std::convert::Infallible>(service_fn(move |req| {
                let s = state.clone();
                handle(req, s)
            }))
        }
    });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Listening on http://{}", addr);
    Server::bind(&addr).serve(make_svc).await?;
    Ok(())
}