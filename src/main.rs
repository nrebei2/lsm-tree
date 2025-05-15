mod database;
use chrono::Local;
use std::{net::SocketAddr, sync::Arc};

use command::read_command;
use config::Config;
use database::Database;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    signal,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

mod command;
mod config;

#[tokio::main]
async fn main() {
    // Parse port (default of 1234) and directory of database (default of DEFAULT_DATABASE_DIRECTORY) from command line
    let config = Config::parse_from_args();

    // Starts up the database
    // If the data directory has contents at startup, reconstructs bloom filters and fence pointers for each file
    let db = Arc::new(Database::new(config.data_dir));

    // Starts up the server on localhost
    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
    println!("Starting server on 127.0.0.1:{}!", config.port);

    let token = CancellationToken::new();
    let cloned_token = token.clone();

    // Ctrl-C => cleanup database
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

                // Creates a new task that handles the connection
                // Because we are spawning a new task
                // Tokio will make each connection concurrent
                tracker.spawn(async move {
                    println!("New connection with {:?}", client);
                    // Spawns a connection
                    handle_connection(stream, client, db_clone, cloned_token).await;
                    println!("Closed connection with {:?}", client);
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

    // Level0 is in memory 
    // Save contents of level 0 to a level0 folder in database
    let db: Database = unsafe { Arc::try_unwrap(db).unwrap_unchecked() };
    db.cleanup();
}

// Handles one connection
async fn handle_connection(
    stream: TcpStream,
    _: SocketAddr,
    db: Arc<Database>,
    cancel_token: CancellationToken,
) {
    let now = Local::now();
    eprintln!("{}", now.format("%H:%M:%S%.6f"));

    let mut processed: usize = 0;
    let (read, mut write) = stream.into_split();
    let mut buf_read = BufReader::new(read);

    let mut out_buf = String::new();

    // repeatedly reads incoming commands from client
    // executes them 
    // then writes back the response to client
    loop {
        tokio::select! {
            read_res = read_command(&mut buf_read) => {
                let command = if let Ok(command) = read_res {
                    command
                } else {
                    break;
                };

                processed += 1;
                // println!("Received command {:?} from {:?}, this is the {processed} command", command, addr);

                if processed % 10_000 == 0 {
                    let now = Local::now();
                    eprintln!("{}", now.format("%H:%M:%S%.6f"));
                }

                command.execute(&db, &mut out_buf).await;

                let mut bytes = out_buf.into_bytes();
                // delimiter of 0 so the client knows when the response finishes
                bytes.push(0x00);
                write.write_all(&bytes).await.unwrap();

                bytes.clear();
                out_buf = unsafe { String::from_utf8_unchecked(bytes) };
            }
            _ = cancel_token.cancelled() => {
                break;
            }
        }
    }
}
