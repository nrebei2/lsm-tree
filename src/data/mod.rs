use std::path::{Path, PathBuf};

use disk_level::DiskLevel;
use mem_level::MemLevel;
use table::{BlockView, Table};

pub mod mem_level;
pub mod disk_level;
pub mod table;
pub mod bloom;

const NUM_LEVELS: usize = 6;
const DATABASE_DIRECTORY: &'static str = "/Users/noahr/dev/rust/lsm-tree/database";

pub enum GetResult {
    NotFound,
    Deleted,
    Value(i32)
}

pub struct Database {
    data_directory: PathBuf,
    memory: MemLevel,
    disk: [DiskLevel; NUM_LEVELS]
}

impl Database {
    pub fn new() -> Self {
        let data_directory = PathBuf::from(DATABASE_DIRECTORY);

        let memory = MemLevel::new();
        let disk: [DiskLevel; NUM_LEVELS] = std::array::from_fn(|idx| {
            DiskLevel::new(&data_directory, (idx + 1) as u32)
        });

        Self { data_directory, memory, disk }
    }

    pub fn insert(&mut self, key: i32, value: i32) {
        self.memory.insert(key, value);
        self.check_mem_overflow();
    }

    pub fn delete(&mut self, key: i32) {
        self.memory.delete(key);
        self.check_mem_overflow();
    }

    fn check_mem_overflow(&mut self) {
        if self.memory.len() > MemLevel::CAPACITY as usize {
            let l0_table = self.memory.write_to_table(self.data_directory.join("level0").as_path());
            merge(&[l0_table], &self.disk[0], &self.data_directory);
        }

        for i in 0..(NUM_LEVELS-1) {
            if self.disk[i].is_over_capacity() {
                let tmp_lvl = merge(&self.disk[i].tables, &self.disk[i+1], &self.data_directory);
                self.disk[i+1].overwrite(tmp_lvl);
            }
        }

        if self.disk[NUM_LEVELS - 1].is_over_capacity() {
            eprintln!("Final level is over capacity!");
        }
    }

    pub fn get(&self, key: i32) -> Option<i32> {
        match self.memory.get(key) {
            GetResult::Deleted => return None,
            GetResult::Value(val) => return Some(val),
            GetResult::NotFound => {}
        };

        for i in 0..NUM_LEVELS {
            match self.disk[i].get(key) {
                GetResult::Deleted => return None,
                GetResult::Value(val) => return Some(val),
                GetResult::NotFound => {}
            };
        }

        None
    }
}

pub fn merge(l1: &[Table], l2: &DiskLevel, data_directory: &Path) -> DiskLevel {
    let tmp_level = DiskLevel::new_tmp(data_directory, l2.level);

    let l1_views: Vec<_> = l1.iter().map(|t| t.view()).collect();

    // let mut block_view = BlockView::new();
    // let block_view_ref = &mut block_view;
    // let l1_commands = l1_views.iter().flat_map(|mut t| (0..10).map(|index| t.get_block_at(index, block_view_ref)));


    todo!()
}