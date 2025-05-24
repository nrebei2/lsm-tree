use std::i32;
use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;

use crate::connection::Connection;
use crate::database::Database;

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
    pub async fn execute(self, connection: &mut Connection, db: &Database) -> io::Result<()> {
        match self {
            Self::GET { key } => {
                if let Some(val) = db.get(key, &mut connection.stats).await {
                    connection.write_int(val).await?;
                }
            }
            Self::DELETE { key } => {
                db.delete(key).await;
                connection.write_str("OK").await?;
            }
            Self::PUT { key, val } => {
                db.insert(key, val).await;
                connection.write_str("OK").await?;
            }
            Self::LOAD { kv_pairs } => {
                db.load(kv_pairs, &mut connection.reader).await?;
                connection.write_str("OK").await?;
            }
            Self::RANGE { min_key, max_key } => {
                if let Some(iter) = db.range(min_key, max_key - 1, &mut connection.stats).await {
                    for (key, val) in iter {
                        connection.write_int(key).await?;
                        connection.write_str(":").await?;
                        connection.write_int(val).await?;
                        connection.write_str(" ").await?;
                    }
                }
            }
            Self::STATS => {
                db.write_stats(connection).await?;
            }
        }
        Ok(())
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
