use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;

use crate::config::BLOCK_SIZE_BYTES;

#[derive(Clone, Copy, Debug)]
pub enum Command {
    Delete(i32),
    Put(i32, i32),
}

impl Command {
    pub fn key(&self) -> i32 {
        match self {
            &Self::Delete(key) => key,
            &Self::Put(key, ..) => key,
        }
    }

    pub fn value(&self) -> Option<i32> {
        match self {
            Self::Delete(_) => None,
            &Self::Put(_, val) => Some(val),
        }
    }
}

/// Block Builder
pub struct BlockMut {
    pub commands: BytesMut,
    pub keys: Vec<i32>,
}

impl BlockMut {
    pub fn new() -> Self {
        Self {
            commands: BytesMut::with_capacity(BLOCK_SIZE_BYTES),
            keys: Vec::with_capacity(BLOCK_SIZE_BYTES >> 2),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn clear(&mut self) {
        self.commands.clear();
        self.keys.clear();
    }

    /// Returns whether the new command was able to fit inside the block
    pub fn push_command(&mut self, command: Command) -> bool {
        let bytes_to_write = match command {
            Command::Delete(..) => 5,
            Command::Put(..) => 9,
        };

        if self.commands.len() + bytes_to_write > self.commands.capacity() {
            let remaining_space = self.commands.capacity() - self.commands.len();

            // Pad the remaining space with 0xFF
            for _ in 0..remaining_space {
                self.commands.put_u8(0xFF);
            }

            return false;
        }

        match command {
            Command::Delete(key) => {
                self.commands.put_u8(1);
                self.commands.put_i32(key);
                self.keys.push(key);
            }
            Command::Put(key, val) => {
                self.commands.put_u8(0);
                self.commands.put_i32(key);
                self.commands.put_i32(val);
                self.keys.push(key);
            }
        }
        true
    }
}

pub struct BlockView {
    buf: [u8; BLOCK_SIZE_BYTES],
}

impl BlockView {
    pub fn new() -> Self {
        Self {
            buf: [0xFF; BLOCK_SIZE_BYTES],
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    pub fn iter(&self) -> BlockViewIter {
        BlockViewIter {
            commands: Cursor::new(&self.buf[..]),
        }
    }
}

pub struct BlockViewIter<'a> {
    commands: Cursor<&'a [u8]>,
}

impl<'a> Iterator for BlockViewIter<'a> {
    type Item = Command;

    fn next(&mut self) -> Option<Command> {
        if !self.commands.has_remaining() {
            return None;
        }

        match self.commands.get_u8() {
            0 => {
                let key = self.commands.get_i32();
                let val = self.commands.get_i32();
                Some(Command::Put(key, val))
            }
            1 => {
                let key = self.commands.get_i32();
                Some(Command::Delete(key))
            }
            0xFF => {
                // Fin
                None
            }
            _ => panic!("INVALID TAG!"),
        }
    }
}
