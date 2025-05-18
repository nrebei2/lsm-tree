use std::{fmt::Arguments, net::SocketAddr, sync::Arc, time::Instant};

use tokio::{io::{self, AsyncWriteExt, BufReader, BufWriter}, net::{tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpStream}};
use tokio_util::sync::CancellationToken;
use std::fmt::Write;

use crate::{client_stats::ClientStats, command::read_command, database::Database};

pub struct Connection {
    pub reader: BufReader<OwnedReadHalf>,
    pub writer: BufWriter<OwnedWriteHalf>,
    addr: SocketAddr,
    cancel_token: CancellationToken,
    pub db: Arc<Database>,
    pub stats: ClientStats,
    format_buffer: String
}

impl Connection {
    pub fn new(stream: TcpStream, addr: SocketAddr, cancel_token: CancellationToken, db: Arc<Database>) -> Self {
        let (read, write) = stream.into_split();
        let buf_read = BufReader::new(read);
        let buf_write = BufWriter::new(write);
        Self {
            reader: buf_read,
            writer: buf_write,
            addr,
            cancel_token,
            db,
            stats: ClientStats::new(addr),
            format_buffer: String::new()
        }
    }

    pub async fn handle(&mut self) {
        // repeatedly reads incoming commands from client
        // execute them
        // then writes back the response to client
        loop {
            tokio::select! {
                read_res = read_command(&mut self.reader) => {
                    let command = if let Ok(command) = read_res {
                        command
                    } else {
                        break;
                    };

                    self.stats.begin(self.db.size_bytes().await);

                    // println!("Received command {:?} from {:?}, executing...", command, addr);
                    let start = Instant::now();
                    command.execute(self).await;
                    self.stats.record_latency(start.elapsed().as_nanos() as u64);

                    // delimiter of 0 so the client knows when the response finishes
                    self.writer.write_u8(0x00).await.unwrap();
                    self.writer.flush().await.unwrap();
                }
                _ = self.cancel_token.cancelled() => {
                    break;
                }
            }
        }
    }

    pub async fn write_fmt(&mut self, args: Arguments<'_>) -> io::Result<()> {
        self.format_buffer.clear();
        write!(self.format_buffer, "{}", args);

        self.writer.write_all(self.format_buffer.as_bytes()).await
    }
}
