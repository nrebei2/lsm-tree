use tokio::io;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;

use crate::database::Database;

#[derive(Clone, Debug)]
pub enum Command {
    PUT { key: i32, val: i32 },
    GET { key: i32 },
    DELETE { key: i32 },
    LOAD { data: Vec<u8> }
    // TODO: range, stats
}

impl Command {
    pub async fn execute(self, db: &Database, out: &mut String) {
        match self {
            Self::GET { key } => {
                if let Some(val) = db.get(key).await {
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
            Self::LOAD { data } => {
                db.load(&data).await;
                out.push_str("OK");
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
            let mut buf = vec![0_u8; kv_pairs as usize * 8];

            reader.read_exact(&mut buf).await?;
            Command::LOAD { data: buf }
        }
        x => todo!("Read {}", x as char),
    })
}