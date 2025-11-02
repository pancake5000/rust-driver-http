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

    let value = state.clone();
    let make_svc = make_service_fn(move |_conn| {
        let state = value.clone();
        async move {
            Ok::<_, std::convert::Infallible>(service_fn(move |req| {
                let s = state.clone();
                handle(req, s)
            }))
        }
    });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Listening on http://{}", addr);

    // Set up Ctrl+C handler
    let state_clone = state.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!("\nReceived Ctrl+C, initiating cleanup...");
                if let Err(e) = state_clone.cleanup().await {
                    eprintln!("Error during cleanup: {}", e);
                }
                std::process::exit(0);
            }
            Err(err) => {
                eprintln!("Error setting up Ctrl+C handler: {}", err);
            }
        }
    });

    // Run the server
    let server = Server::bind(&addr).serve(make_svc);
    let graceful = server.with_graceful_shutdown(async {
        tokio::signal::ctrl_c().await.ok();
    });

    // Run the server with graceful shutdown
    if let Err(e) = graceful.await {
        eprintln!("Server error: {}", e);
    }

    // Cleanup on normal shutdown
    println!("Server shutting down, cleaning up...");
    state.cleanup().await?;
    Ok(())
}