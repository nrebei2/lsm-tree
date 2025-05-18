use std::net::SocketAddr;

use chrono::Local;
use hdrhistogram::Histogram;
pub struct ClientStats {
    start_time: Option<String>,
    addr: SocketAddr,
    database_size: Option<usize>,
    latencies_ns: Histogram<u64>, // per request
    blocks_read: Histogram<u64>,  // per request
    num_requests: u32,
}

impl ClientStats {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            start_time: None,
            addr,
            database_size: None,
            latencies_ns: Histogram::new(3).unwrap(),
            blocks_read: Histogram::new(3).unwrap(),
            num_requests: 0,
        }
    }

    pub fn begin(&mut self, database_size: usize) {
        if self.start_time.is_none() {
            self.start_time = Some(Local::now().format("%H:%M:%S%.6f").to_string());
            self.database_size = Some(database_size);
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
        let file = std::fs::File::create(format!(
            "bench/client_{}.json",
            self.start_time.clone().unwrap_or_default()
        ))
        .unwrap();

        use serde::Serialize;

        #[derive(Serialize)]
        struct Percentiles {
            p50: u64,
            p90: u64,
            p99: u64,
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
        struct StatsJson {
            client_addr: String,
            start_time: String,
            end_time: String,
            latencies_ns: Percentiles,
            blocks_read: Percentiles,
            database_size: usize,
            num_requests: u32,
        }

        let stats = StatsJson {
            client_addr: self.addr.to_string(),
            start_time: self.start_time.unwrap_or_default(),
            end_time: Local::now().format("%H:%M:%S%.6f").to_string(),
            latencies_ns: Percentiles::from_histogram(&self.latencies_ns),
            blocks_read: Percentiles::from_histogram(&self.blocks_read),
            num_requests: self.num_requests,
            database_size: self.database_size.unwrap_or_default(),
        };

        if serde_json::to_writer_pretty(file, &stats).is_err() {
            eprintln!("Failed saving stats for client ")
        }
    }
}
