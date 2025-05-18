use std::fmt::Write;
use std::i32;
use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::database::Database;
use crate::ClientStats;

#[derive(Clone, Debug)]
pub enum Command {
    PUT { key: i32, val: i32 },
    GET { key: i32 },
    DELETE { key: i32 },
    LOAD { kv_pairs: u64 },
    RANGE { min_key: i32, max_key: i32 },
    STATS,
}

impl Command {
    pub async fn execute<R: AsyncBufReadExt + Unpin, W: AsyncWrite + Unpin>(
        self,
        db: &Database,
        stats: &mut ClientStats,
        reader: &mut R,
        writer: &mut W
    ) {
        match self {
            Self::GET { key } => {
                if let Some(val) = db.get(key, stats).await {
                    writer.write_all(val.to_string().as_bytes());
                }
            }
            Self::DELETE { key } => {
                db.delete(key).await;
                writer.write_all(b"OK");
            }
            Self::PUT { key, val } => {
                db.insert(key, val).await;
                writer.write_all(b"OK");
            }
            Self::LOAD { kv_pairs } => {
                let _ = db.load(kv_pairs, reader).await;
                writer.write_all(b"OK");
            }
            Self::RANGE { min_key, max_key } => {
                let buf = String::new();
                if let Some(iter) = db.range(min_key, max_key - 1, stats).await {
                    for (key, val) in iter {
                        write!(buf, "{key}:{} ", val).unwrap();
                        writer.write_all(buf.as_bytes());
                        buf.clear();
                    }
                }
            }
            Self::STATS => {
                db.write_stats(writer).await;
            }
        }
    }
}

pub async fn read_command<T: AsyncBufReadExt + Unpin>(reader: &mut T) -> io::Result<Command> {
    Ok(match reader.read_u8().await? {
        b'p' => {
            let key = reader.read_i32().await?;
            let val = reader.read_i32().await?;
            Command::PUT { key, val }
        }
        b'g' => {
            let key = reader.read_i32().await?;
            Command::GET { key }
        }
        b'd' => {
            let key = reader.read_i32().await?;
            Command::DELETE { key }
        }
        b'l' => {
            let kv_pairs = reader.read_u64().await?;
            Command::LOAD { kv_pairs }
        }
        b'r' => {
            let min_key = reader.read_i32().await?;
            let max_key = reader.read_i32().await?;
            Command::RANGE { min_key, max_key }
        }
        b's' => Command::STATS,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid incoming command!",
            ))
        }
    })
}
