use std::{
    fs::{self, metadata},
    io::{Read, Write},
    path::PathBuf,
};

use bytes::BufMut;
use relm4::tokio::io;

#[derive(Clone, Debug)]
pub enum Command {
    PUT { key: i32, val: i32 },
    GET { key: i32 },
    DELETE { key: i32 },
    LOAD { file: PathBuf },
    RANGE { min_key: i32, max_key: i32 },
    STATS,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    PUT,
    GET,
    DELETE,
    RANGE,
}

impl Command {
    pub fn to_type(&self) -> Option<CommandType> {
        Some(match self {
            Self::DELETE { .. } => CommandType::DELETE,
            Self::PUT { .. } => CommandType::PUT,
            Self::GET { .. } => CommandType::GET,
            Self::RANGE { .. } => CommandType::RANGE,
            _ => return None,
        })
    }
    pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buf = [0_u8; 9];
        let mut slc = buf.as_mut_slice();
        match self {
            Self::PUT { key, val } => {
                slc.put_u8(b'p');
                slc.put_i32(*key);
                slc.put_i32(*val);
                writer.write_all(&buf)?;
            }
            Self::GET { key } => {
                slc.put_u8(b'g');
                slc.put_i32(*key);
                writer.write_all(&buf[..5])?;
            }
            Self::DELETE { key } => {
                slc.put_u8(b'd');
                slc.put_i32(*key);
                writer.write_all(&buf[..5])?;
            }
            Self::LOAD { file } => {
                slc.put_u8(b'l');

                let file_size = metadata(file).unwrap().len();
                let kv_pairs = file_size / 8;

                slc.put_u64(kv_pairs);
                writer.write_all(&buf)?;
                std::io::copy(&mut fs::File::open(file).unwrap(), writer)?;
            }
            Self::RANGE { min_key, max_key } => {
                slc.put_u8(b'r');
                slc.put_i32(*min_key);
                slc.put_i32(*max_key);
                writer.write_all(&buf)?;
            }
            Self::STATS => {
                slc.put_u8(b's');
                writer.write_all(&buf[..1])?;
            }
        }
        Ok(())
    }

    pub fn from_input(input: &str) -> Option<Self> {
        let mut split_iter = input.split(' ');
        let tag = split_iter.next()?;

        match tag {
            "p" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                let val: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::PUT { key, val })
            }
            "g" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::GET { key })
            }
            "d" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::DELETE { key })
            }
            "l" => {
                let file: PathBuf = split_iter.next()?.parse().ok()?;

                if !file.is_file() {
                    return None;
                }

                Some(Command::LOAD { file })
            }
            "r" => {
                let min_key: i32 = split_iter.next()?.parse().ok()?;
                let max_key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::RANGE { min_key, max_key })
            }
            "s" => Some(Command::STATS),
            _ => None,
        }
    }
}
