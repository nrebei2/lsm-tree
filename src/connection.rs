use std::{
    io::{Cursor, Write},
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use tokio::{
    io::{self, AsyncWriteExt, BufReader, BufWriter},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{client_stats::ClientStats, command::read_command, database::Database};

pub struct Connection {
    pub reader: BufReader<OwnedReadHalf>,
    pub writer: BufWriter<OwnedWriteHalf>,
    addr: SocketAddr,
    cancel_token: CancellationToken,
    pub stats: ClientStats,
}

impl Connection {
    pub fn new(stream: TcpStream, addr: SocketAddr, cancel_token: CancellationToken) -> Self {
        let (read, write) = stream.into_split();
        let buf_read = BufReader::new(read);
        let buf_write = BufWriter::new(write);
        Self {
            reader: buf_read,
            writer: buf_write,
            addr,
            cancel_token,
            stats: ClientStats::new(addr),
        }
    }

    pub async fn handle(&mut self, db: Arc<Database>) -> io::Result<()> {
        // repeatedly reads incoming commands from client
        // execute them
        // then writes back the response to client
        loop {
            tokio::select! {
                read_res = read_command(&mut self.reader) => {
                    let command = if let Ok(command) = read_res {
                        command
                    } else {
                        break Ok(());
                    };

                    self.stats.begin(db.size_bytes().await);

                    // println!("Received command {:?} from {:?}, executing...", command, addr);
                    let start = Instant::now();
                    command.execute(self, &db).await?;
                    self.stats.record_latency(start.elapsed().as_nanos() as u64);

                    // delimiter of 0 so the client knows when the response finishes
                    self.writer.write_u8(0x00).await.unwrap();
                    self.writer.flush().await.unwrap();
                }
                _ = self.cancel_token.cancelled() => {
                    break Ok(());
                }
            }
        }
    }

    pub async fn write_int(&mut self, val: i32) -> io::Result<()> {
        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.writer.write_all(&buf.get_ref()[..pos]).await
    }

    pub async fn write_str(&mut self, str: &str) -> io::Result<()> {
        self.writer.write_all(str.as_bytes()).await
    }
}
