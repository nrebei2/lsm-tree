mod database;
use std::sync::Arc;

use client_stats::ClientStats;
use config::Config;
use connection::Connection;
use database::Database;
use tokio::{net::TcpListener, signal};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

mod client_stats;
mod command;
mod config;
mod connection;

#[tokio::main]
async fn main() {
    let config = Config::parse_from_args();

    // Starts up the database
    // If the data directory has contents at startup, reconstructs bloom filters and fence pointers for each file
    let db = Arc::new(Database::new(config.data_dir));

    // Starts up the server on localhost
    let listener = TcpListener::bind(("0.0.0.0", config.port)).await.unwrap();
    println!("Starting server on 0.0.0.0:{}!", config.port);

    let token = CancellationToken::new();
    let cloned_token = token.clone();

    // Ctrl-C hook for cleanup
    let tracker = TaskTracker::new();
    tracker.spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                cloned_token.cancel();
            }
            Err(err) => {
                eprintln!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    // Repeatedly accept incoming client connections
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, client) = accept_result.unwrap();
                let db_clone = db.clone();
                let cloned_token = token.clone();

                let mut connnection = Connection::new(stream, client, cloned_token);

                // Tokio will make each connection concurrent
                tracker.spawn(async move {
                    println!("New connection with {:?}", client);
                    let result = connnection.handle(db_clone).await;
                    connnection.stats.save_to_file();
                    println!("Closed connection with {client:?}: {result:?}");
                });
            }
            _ = token.cancelled() => {
                break;
            }
        }
    }

    tracker.close();

    // Wait for everything to finish.
    tracker.wait().await;

    // Level0 is in memory => save contents of level 0 to a level0 folder in database for retrieval on startup
    let db: Database = unsafe { Arc::try_unwrap(db).unwrap_unchecked() };
    db.cleanup();
}
