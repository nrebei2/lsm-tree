use crate::config::{BLOCK_SIZE_BYTES, BLOOM_CAPACITY, MAX_FILE_SIZE_BLOCKS};

use super::bloom::Bloom;
use super::once_done::OnceDoneTrait;
use block::*;
use std::cmp::Ordering;
use std::fmt::Debug;

#[cfg(windows)]
use std::os::windows::fs::FileExt;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    time::SystemTime,
};

pub mod block;

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

    pub fn is_full(&self) -> bool {
        self.index.len() >= MAX_FILE_SIZE_BLOCKS
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn build(self) -> Table {
        let new_path = self.directory.join(format!(
            "{}_{}",
            self.min_key.unwrap(),
            self.max_key.unwrap()
        ));
        fs::rename(&self.file_path, &new_path).unwrap();

        let file_size = fs::metadata(&new_path).unwrap().len();

        Table {
            directory: self.directory,
            min_key: self.min_key.unwrap(),
            max_key: self.max_key.unwrap(),
            file_size,
            bloom: self.bloom,
            index: self.index,
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub directory: PathBuf,
    // file name = "{min_key}_{max_key}"
    pub min_key: i32,
    pub max_key: i32,
    pub file_size: u64,
    pub bloom: Bloom,
    pub index: Vec<(i32, i32)>, // min/max key for each block in file
}

impl Table {
    pub fn view(&self) -> TableView {
        TableView::new(self.file_path(), 0)
    }

    pub fn view_from(&self, block_index: usize) -> TableView {
        TableView::new(self.file_path(), block_index)
    }

    pub fn commands(
        &self,
        start_at_block: usize,
        delete_on_finish: bool,
    ) -> impl Iterator<Item = Command> {
        self.commands_ext(start_at_block, delete_on_finish, || {})
    }

    pub fn commands_ext<T: Fn()>(
        &self,
        start_at_block: usize,
        delete_on_finish: bool,
        on_block: T,
    ) -> impl Iterator<Item = Command> {
        self.view_from(start_at_block)
            .once_done(move |v| {
                if delete_on_finish {
                    v.delete_file()
                }
            })
            .flat_map(move |b| {
                on_block();
                unsafe { b.as_ref().unwrap().iter() }
            })
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

        fs::rename(old_file_path, new_file_path).unwrap();
    }

    pub fn create_from_existing(file_path: &Path) -> Self {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        let (min_key_str, max_key_str) = file_name
            .split_once('_')
            .expect("File name was tampered with...");

        let min_key: i32 = min_key_str.parse().expect("File name was tampered with...");
        let max_key: i32 = max_key_str.parse().expect("File name was tampered with...");

        let directory = file_path.parent().unwrap().to_owned();

        let mut bloom = Bloom::new(BLOOM_CAPACITY);

        let file_size = fs::metadata(file_path).unwrap().len();
        let block_count = file_size.div_ceil(BLOCK_SIZE_BYTES as u64);

        let mut index = Vec::with_capacity(block_count as usize);

        let table_view = TableView::new(file_path.to_path_buf(), 0);

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

        Table {
            directory,
            min_key,
            max_key,
            file_size,
            bloom,
            index,
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
    pub fn new(file_path: PathBuf, cur_block: usize) -> Self {
        let file = File::open(&file_path).unwrap();

        Self {
            file_path,
            file,
            block_buf: BlockView::new(),
            cur_block,
        }
    }

    #[cfg(windows)]
    fn read_block(&mut self, index: usize) -> usize {
        self.file
            .seek_read(
                self.block_buf.as_mut_slice(),
                (index * BLOCK_SIZE_BYTES) as u64,
            )
            .unwrap()
    }

    #[cfg(unix)]
    fn read_block(&mut self, index: usize) -> usize {
        self.file
            .read_at(
                self.block_buf.as_mut_slice(),
                (index * BLOCK_SIZE_BYTES) as u64,
            )
            .unwrap()
    }

    pub fn get_block_at(&mut self, index: usize) -> Option<&BlockView> {
        let bytes_read = self.read_block(index);

        if bytes_read == 0 {
            return None;
        }

        if bytes_read < BLOCK_SIZE_BYTES {
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
