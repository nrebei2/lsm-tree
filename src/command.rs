use std::fmt::Write;
use std::i32;
use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;

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
    pub async fn execute<T: AsyncBufReadExt + Unpin>(self, db: &Database, stats: &mut ClientStats, reader: &mut T, out: &mut String) {
        match self {
            Self::GET { key } => {
                if let Some(val) = db.get(key, stats).await {
                    out.push_str(&val.to_string());
                }
            }
            Self::DELETE { key } => {
                db.delete(key).await;
                out.push_str("OK");
            }
            Self::PUT { key, val } => {
                db.insert(key, val).await;
                out.push_str("OK");
            }
            Self::LOAD { kv_pairs } => {
                let _ = db.load(kv_pairs, reader).await;
                out.push_str("OK");
            }
            Self::RANGE { min_key, max_key } => {
                if let Some(iter) = db.range(min_key, max_key - 1, stats).await {
                    for (key, val) in iter {
                        write!(out, "{key}:{} ", val).unwrap();
                    }
                }
            }
            Self::STATS => {
                db.write_stats(out).await;
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
