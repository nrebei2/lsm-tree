mod database;
use std::{net::SocketAddr, sync::Arc, time::Instant};

use chrono::Local;
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
    let config = Config::parse_from_args();

    // Starts up the database
    // If the data directory has contents at startup, reconstructs bloom filters and fence pointers for each file
    let db = Arc::new(Database::new(config.data_dir));

    // Starts up the server on localhost
    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
    println!("Starting server on 127.0.0.1:{}!", config.port);

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

                // Creates a new task that handles the connection
                // Because we are spawning a new task
                // Tokio will make each connection concurrent
                tracker.spawn(async move {
                    println!("New connection with {:?}", client);
                    let stats = handle_connection(stream, client, db_clone, cloned_token).await;
                    stats.save_to_file();
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

    // Level0 is in memory => save contents of level 0 to a level0 folder in database for retrieval on startup
    let db: Database = unsafe { Arc::try_unwrap(db).unwrap_unchecked() };
    db.cleanup();
}

use hdrhistogram::Histogram;
struct ClientStats {
    start_time: String,
    addr: SocketAddr,
    latencies_ns: Histogram<u64>, // per request
    blocks_read: Histogram<u64>,  // per request
    num_requests: u32,
}

impl ClientStats {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            start_time: Local::now().format("%H:%M:%S%.6f").to_string(),
            addr,
            latencies_ns: Histogram::new(3).unwrap(),
            blocks_read: Histogram::new(3).unwrap(),
            num_requests: 0,
        }
    }

    pub fn record_latency(&mut self, latency_ns: u64) {
        self.latencies_ns += latency_ns;
        self.num_requests += 1;
    }

    pub fn record_blocks_read(&mut self, blocks: u64) {
        self.blocks_read += blocks;
    }

    pub fn save_to_file(self) {
        let file = std::fs::File::create(format!("bench/client_{}.json", self.start_time)).unwrap();

        use serde::Serialize;

        #[derive(Serialize)]
        pub struct Percentiles {
            pub p50: u64,
            pub p90: u64,
            pub p99: u64,
        }

        impl Percentiles {
            fn from_histogram(h: &Histogram<u64>) -> Self {
                Self {
                    p50: h.value_at_quantile(0.50),
                    p90: h.value_at_quantile(0.90),
                    p99: h.value_at_quantile(0.99),
                }
            }
        }

        #[derive(Serialize)]
        pub struct StatsJson {
            pub client_addr: String,
            pub start_time: String,
            pub end_time: String,
            pub latencies_ns: Percentiles,
            pub blocks_read: Percentiles,
            pub num_requests: u32,
        }

        let stats = StatsJson {
            client_addr: self.addr.to_string(),
            start_time: self.start_time,
            end_time: Local::now().format("%H:%M:%S%.6f").to_string(),
            latencies_ns: Percentiles::from_histogram(&self.latencies_ns),
            blocks_read: Percentiles::from_histogram(&self.blocks_read),
            num_requests: self.num_requests,
        };

        if serde_json::to_writer_pretty(file, &stats).is_err() {
            eprintln!("Failed saving stats for client ")
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    db: Arc<Database>,
    cancel_token: CancellationToken,
) -> ClientStats {
    let (read, mut write) = stream.into_split();
    let mut buf_read = BufReader::new(read);

    let mut out_buf = String::new();

    let mut stats = ClientStats::new(addr);

    // repeatedly reads incoming commands from client
    // execute them
    // then writes back the response to client
    loop {
        tokio::select! {
            read_res = read_command(&mut buf_read) => {
                let command = if let Ok(command) = read_res {
                    command
                } else {
                    break;
                };

                // println!("Received command {:?} from {:?}, executing...", command, addr);
                let start = Instant::now();
                command.execute(&db, &mut out_buf, &mut stats).await;
                stats.record_latency(start.elapsed().as_nanos() as u64);

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

    stats
}
