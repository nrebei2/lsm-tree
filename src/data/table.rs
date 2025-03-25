use super::bloom::Bloom;
use super::disk_level::DiskLevel;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::os::unix::fs::FileExt;
use std::{
    fs::{self, File},
    io::{Cursor, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime},
};

pub const MAX_FILE_SIZE_BYTES: usize = 1 << 28; // 64 MB
pub const MAX_FILE_SIZE_BLOCKS: usize = MAX_FILE_SIZE_BYTES >> 12; // 64 MB
pub const BLOOM_CAPACITY: usize = 1 << 16; // 64 MB

pub struct BlockMut {
    pub commands: BytesMut,
    pub keys: Vec<i32>,
}

impl BlockMut {
    pub fn new() -> Self {
        Self {
            commands: BytesMut::with_capacity(4096),
            keys: Vec::with_capacity(1024),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn clear(&mut self) {
        self.commands.clear();
        self.keys.clear();
    }

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

#[derive(Clone, Copy)]
pub enum Command {
    Delete(i32),
    Put(i32, i32),
}

impl Command {
    pub fn key(&self) -> i32 {
        match self {
            Self::Delete(key) => *key,
            Self::Put(key, ..) => *key,
        }
    }
}

pub struct TableBuilder {
    pub directory: PathBuf,
    pub file_path: PathBuf,
    pub file: File,
    pub min_key: Option<i32>,
    pub max_key: Option<i32>,
    pub bloom: Bloom,
    pub index: Vec<(i32, i32)>, // min/max key for each block in file
}

impl TableBuilder {
    pub fn new(directory: &Path) -> Self {
        let tmp_file_name = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();

        fs::create_dir_all(&directory).unwrap();

        let file_path = directory.join(tmp_file_name);
        let file = File::create_new(&file_path).unwrap();
        Self {
            directory: directory.to_path_buf(),
            min_key: None,
            max_key: None,
            bloom: Bloom::new(BLOOM_CAPACITY),
            index: Vec::with_capacity(MAX_FILE_SIZE_BLOCKS),
            file,
            file_path,
        }
    }

    pub fn insert_block(&mut self, block: &BlockMut) {
        let min = *block.keys.first().unwrap();
        let max = *block.keys.last().unwrap();

        if self.min_key.is_none() {
            self.min_key = Some(min);
        }
        self.max_key = Some(max);

        self.file.write_all(&block.commands).unwrap();
        self.index.push((min, max));

        for &key in block.keys.iter() {
            self.bloom.put(key);
        }
    }

    pub fn full(&self) -> bool {
        self.index.len() >= MAX_FILE_SIZE_BLOCKS
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn build(self) -> Table {
        let new_path = self.directory.join(format!(
            "{}-{}",
            self.min_key.unwrap(),
            self.max_key.unwrap()
        ));
        fs::rename(&self.file_path, &new_path).unwrap();

        // TODO: not really necessary
        let metadata = fs::metadata(&new_path).unwrap();

        Table {
            directory: self.directory,
            min_key: self.min_key.unwrap(),
            max_key: self.max_key.unwrap(),
            file_size: metadata.len(),
            bloom: self.bloom,
            index: self.index,
        }
    }
}

pub struct Table {
    pub directory: PathBuf,
    // file name = "{min_key}-{max_key}"
    pub min_key: i32,
    pub max_key: i32,
    pub file_size: u64, // in bytes
    pub bloom: Bloom,
    pub index: Vec<(i32, i32)>, // min/max key for each block in file
}

impl Table {
    pub fn view(&mut self) -> TableView {
        TableView::new(self)
    }

    pub fn intersects(&self, other: &Table) -> Ordering {
        if self.max_key < other.min_key {
            Ordering::Less
        } else if self.min_key > other.max_key {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }

    pub fn file_path(&self) -> PathBuf {
        self.directory.join(self.file_name())
    }

    pub fn file_name(&self) -> String {
        format!("{}-{}", self.min_key, self.max_key)
    }

    pub fn rename(&self, to_dir: &Path) {
        fs::rename(self.file_path(), to_dir.join(self.file_name())).unwrap();
    }

    pub fn delete_file(&self) {
        fs::remove_file(self.file_path()).unwrap();
    }
}

pub struct BlockView {
    buf: [u8; 4096],
}

impl BlockView {
    pub fn new() -> Self {
        Self { buf: [0xFF; 4096] }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
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
            _ => panic!("INVALID TAG!!!!!!!!!!"),
        }
    }
}

pub struct TableView<'a> {
    pub table: &'a Table,
    file: File,
    block_buf: BlockView,
    cur_block: usize,
}

impl<'a> TableView<'a> {
    fn new(table: &'a Table) -> Self {
        let file = File::open(table.file_path()).unwrap();

        Self {
            table,
            file,
            block_buf: BlockView::new(),
            cur_block: 0
        }
    }

    pub fn get_block_at(&mut self, index: usize) -> &BlockView {
        if index >= self.block_len() {
            panic!("Out of bounds!");
        }

        let bytes_read = self
            .file
            .read_at(self.block_buf.as_mut_slice(), (index * 4096) as u64)
            .unwrap();

        if bytes_read < 4096 {
            // this must be the last page
            assert_eq!(index, self.block_len() - 1);
            assert_eq!(bytes_read, self.table.file_size as usize % 4096);

            // sentinel of 0xFF
            self.block_buf.as_mut_slice()[bytes_read] = 0xFF;
        }

        &self.block_buf
    }

    pub fn block_len(&self) -> usize {
        self.table.index.len()
    }
}

impl<'a> Iterator for TableView<'a> {
    type Item = *const BlockView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_block > self.block_len() {
            self.table.delete_file();
            return None;
        }

        let block_view = self.get_block_at(self.cur_block) as *const BlockView;
        self.cur_block += 1;

        Some(block_view)
    }
}
