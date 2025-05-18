use std::fmt::Write;
use std::i32;
use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::connection::Connection;
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
    pub async fn execute(self, connection: &mut Connection) {
        match self {
            Self::GET { key } => {
                if let Some(val) = connection.db.get(key, &mut connection.stats).await {
                    connection.write_fmt(format_args!("{}", val)).await;
                }
            }
            Self::DELETE { key } => {
                connection.db.delete(key).await;
                connection.writer.write_all(b"OK").await;
            }
            Self::PUT { key, val } => {
                connection.db.insert(key, val).await;
                connection.writer.write_all(b"OK").await;
            }
            Self::LOAD { kv_pairs } => {
                let _ = connection.db.load(kv_pairs, &mut connection.reader).await;
                connection.writer.write_all(b"OK").await;
            }
            Self::RANGE { min_key, max_key } => {
                if let Some(iter) = connection.db.range(min_key, max_key - 1, &mut connection.stats).await {
                    for (key, val) in iter {
                        connection.write_fmt(format_args!("{}:{} ", key, val)).await;
                    }
                }
            }
            Self::STATS => {
                connection.db.write_stats(&mut connection.writer).await;
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
