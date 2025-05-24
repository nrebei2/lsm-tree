use std::{
    cmp::Ordering,
    fs,
    path::{Path, PathBuf},
};

use crate::config::{LEVEL1_FILE_CAPACITY, MAX_FILE_SIZE_BYTES, SIZE_MULTIPLIER};

use super::{
    table::{block::Command, Table},
    GetResult,
};

pub struct LocateResult {
    pub table_index: usize,
    pub block_index: usize,
}

#[derive(Debug)]
pub struct DiskLevel {
    pub level: u32,
    pub level_directory: PathBuf,
    pub tables: Vec<Table>, // sorted array by keys
}

impl DiskLevel {
    pub fn new(data_directory: &Path, level: u32) -> Self {
        let mut level_directory = PathBuf::from(data_directory);
        level_directory.push(format!("level{level}"));

        fs::create_dir_all(&level_directory).unwrap();

        let mut tables = vec![];

        for entry in fs::read_dir(&level_directory).unwrap() {
            let entry = entry.unwrap();
            tables.push(Table::create_from_existing(&entry.path()));
        }

        let mut res = Self {
            level,
            level_directory,
            tables,
        };
        res.sort_tables();
        res
    }

    pub fn sort_tables(&mut self) {
        self.tables.sort_by_key(|t| t.min_key);
    }

    pub fn is_over_file_capacity(&self) -> bool {
        self.tables.len() > self.file_capacity()
    }

    fn file_capacity(&self) -> usize {
        LEVEL1_FILE_CAPACITY * usize::pow(SIZE_MULTIPLIER, self.level - 1)
    }

    pub fn average_table_utilization(&self) -> f32 {
        self.tables
            .iter()
            .map(|t| t.file_size as f32 / MAX_FILE_SIZE_BYTES as f32)
            .sum::<f32>()
            / self.tables.len() as f32
    }

    fn find_table(&self, key: i32) -> Result<usize, usize> {
        self.tables.binary_search_by(|t| {
            if key >= t.min_key && key <= t.max_key {
                Ordering::Equal
            } else if key < t.min_key {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        })
    }

    fn find_block_in_table(&self, table: &Table, key: i32) -> Result<usize, usize> {
        table.index.binary_search_by(|&(min_key, max_key)| {
            if key >= min_key && key <= max_key {
                Ordering::Equal
            } else if key < min_key {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        })
    }

    /// Finds the first block with a key higher or equal to `key`. Used for range queries.
    pub fn locate_start_block(&self, key: i32) -> Option<LocateResult> {
        let table_index = match self.find_table(key) {
            Ok(idx) => idx,
            Err(idx) => {
                return if idx == self.tables.len() {
                    None
                } else {
                    Some(LocateResult {
                        table_index: idx,
                        block_index: 0,
                    })
                }
            }
        };

        let block_index = match self.find_block_in_table(&self.tables[table_index], key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        Some(LocateResult {
            table_index,
            block_index,
        })
    }

    pub fn get(&self, key: i32) -> GetResult {
        // find table
        let table = match self.find_table(key) {
            Ok(idx) => &self.tables[idx],
            _ => return GetResult::NotFound(false),
        };

        // consult bloom filter
        if !table.bloom.maybe_contains(key) {
            return GetResult::NotFound(false);
        }

        // find block in table
        let block_num = match self.find_block_in_table(table, key) {
            Ok(idx) => idx,
            _ => return GetResult::NotFound(false),
        };

        // read block in table
        for command in table.view().get_block_at(block_num).unwrap().iter() {
            if command.key() > key {
                // block is sorted => can break early
                break;
            }

            if command.key() == key {
                match command {
                    Command::Delete(..) => return GetResult::Deleted,
                    Command::Put(_, val) => return GetResult::Value(val),
                }
            }
        }

        GetResult::NotFound(true)
    }

    pub fn size_bytes(&self) -> usize {
        self.tables.iter().map(|t| t.file_size).sum::<u64>() as usize
    }
}
