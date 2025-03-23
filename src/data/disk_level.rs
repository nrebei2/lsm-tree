use std::{cmp::Ordering, path::{Path, PathBuf}};

use super::{table::{BlockView, Command, Table}, GetResult};

pub struct DiskLevel {
    pub level: u32,
    pub level_directory: PathBuf,
    pub tables: Vec<Table> // sorted array
}

impl DiskLevel {
    pub const SIZE_MULTIPLIER: usize = 2;

    pub fn new(data_directory: &Path, level: u32) -> Self {
        let mut level_directory = PathBuf::from(data_directory);
        level_directory.push(format!("level{level}"));

        // TODO: read from directory and populate tables

        Self {
            level,
            level_directory,
            tables: vec![]
        }
    }

    pub fn new_tmp(data_directory: &Path, level: u32) -> Self {
        let mut level_directory = PathBuf::from(data_directory);
        level_directory.push(format!("level{level}"));
        level_directory.push("tmp");

        Self {
            level,
            level_directory,
            tables: vec![]
        }
    }

    pub fn overwrite(&mut self, to: DiskLevel) {
        assert_eq!(self.level, to.level);
    }


    pub fn is_over_capacity(&self) -> bool {
        self.tables.len() > self.file_capacity()
    }

    fn file_capacity(&self) -> usize {
        4 * usize::pow(Self::SIZE_MULTIPLIER, self.level)
    }

    pub fn get(&self, key: i32) -> GetResult { 
        // find table
        let table = match self.tables.binary_search_by(|t| {
            if key >= t.min_key && key <= t.max_key {
                Ordering::Equal
            } else if key < t.min_key {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }) {
            Ok(idx) => &self.tables[idx],
            _ => return GetResult::NotFound
        };

        // find block in table
        if !table.bloom.maybe_contains(key) {
            return GetResult::NotFound;
        }

        let block_num = match table.index.binary_search_by(|&(min_key, max_key)| {
            if key >= min_key && key <= max_key {
                Ordering::Equal
            } else if key < min_key {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }) {
            Ok(idx) => idx,
            _ => return GetResult::NotFound
        }; 

        // read block in table
        let mut block_view = BlockView::new();
        table.view().get_block_at(block_num, &mut block_view);
        for command in block_view.iter() {
            if command.key() > key {
                // block is sorted, break early
                break;
            }

            if command.key() == key {
                match command {
                    Command::Delete(..) => return GetResult::Deleted,
                    Command::Put(_, val) => return GetResult::Value(val)
                }
            }
        }

        GetResult::NotFound
    }
}