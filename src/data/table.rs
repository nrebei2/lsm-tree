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

#[derive(Clone, Copy, Debug)]
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
            "{}:{}",
            self.min_key.unwrap(),
            self.max_key.unwrap()
        ));
        fs::rename(&self.file_path, &new_path).unwrap();

        Table {
            directory: self.directory,
            min_key: self.min_key.unwrap(),
            max_key: self.max_key.unwrap(),
            bloom: self.bloom,
            index: self.index,
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub directory: PathBuf,
    // file name = "{min_key}-{max_key}"
    pub min_key: i32,
    pub max_key: i32,
    pub bloom: Bloom,
    pub index: Vec<(i32, i32)>, // min/max key for each block in file
}

impl Table {
    pub fn view(&self) -> TableView {
        TableView::new(self.file_path())
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
        format!("{}:{}", self.min_key, self.max_key)
    }

    pub fn rename(&mut self, to_dir: &Path) {
        let old_file_path = self.file_path();
        self.directory = to_dir.to_owned();
        let new_file_path = self.file_path();
        println!("Renaming {:?} to {:?}", old_file_path, new_file_path);
        fs::rename(old_file_path, new_file_path).unwrap();
    }

    pub fn create_from_existing(file_path: &Path) -> Self {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        let (min_key_str, max_key_str) = file_name
            .split_once(':')
            .expect("File name was tampered with...");

        let min_key: i32 = min_key_str.parse().expect("File name was tampered with...");
        let max_key: i32 = max_key_str.parse().expect("File name was tampered with...");

        let directory = file_path.parent().unwrap().to_owned();

        let mut bloom = Bloom::new(BLOOM_CAPACITY);

        let file_len = fs::metadata(file_path).unwrap().len();
        let block_count = file_len.div_ceil(4096);

        let mut index = Vec::with_capacity(block_count as usize);

        let table_view = TableView::new(file_path.to_path_buf());

        for block_ptr in table_view {
            let mut block_iter = unsafe { &*block_ptr }.iter();

            let first = block_iter.next().unwrap();
            let mut last = first;
            bloom.put(first.key());

            while let Some(command) = block_iter.next() {
                last = command;
                bloom.put(command.key());
            }

            index.push((first.key(), last.key()));
        }

        // TODO: maybe assert min_key = index.first.0, max_key = index.last.1 

        Table {
            directory,
            min_key,
            max_key,
            bloom,
            index,
        }
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

pub struct TableView {
    file_path: PathBuf,
    file: File,
    block_buf: BlockView,
    cur_block: usize,
}

impl TableView {
    fn new(file_path: PathBuf) -> Self {
        let file = File::open(&file_path).unwrap();

        Self {
            file_path,
            file,
            block_buf: BlockView::new(),
            cur_block: 0,
        }
    }

    pub fn get_block_at(&mut self, index: usize) -> Option<&BlockView> {
        let bytes_read = self
            .file
            .read_at(self.block_buf.as_mut_slice(), (index * 4096) as u64)
            .unwrap();

        if bytes_read == 0 {
            return None;
        }

        if bytes_read < 4096 {
            // this must be the last page
            // sentinel of 0xFF
            self.block_buf.as_mut_slice()[bytes_read] = 0xFF;
        }

        Some(&self.block_buf)
    }

    pub fn delete_file(&self) {
        fs::remove_file(&self.file_path).unwrap();
    }
}

impl Iterator for TableView {
    type Item = *const BlockView;

    fn next(&mut self) -> Option<Self::Item> {
        self.cur_block += 1;
        self.get_block_at(self.cur_block - 1)
            .map(|b| b as *const BlockView)
    }
}
