use std::{collections::BTreeMap, ops::Deref, path::{Path, PathBuf}};

use crate::data::merge;

use super::{disk_level::DiskLevel, table::{BlockMut, Command, Table, TableBuilder}, GetResult};

pub struct MemLevel {
    data: BTreeMap<i32, Option<i32>>
}

impl Deref for MemLevel {
    type Target = BTreeMap<i32, Option<i32>>;

    fn deref(&self) -> &Self::Target {
        return &self.data;    
    }
}

impl MemLevel {
    pub const CAPACITY: u32 = 10;

    pub const fn new() -> Self {
        return Self { data: BTreeMap::new() }
    }

    pub fn insert(&mut self, key: i32, value: i32) {
        self.data.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: i32) {
        self.data.insert(key, None);
    }

    pub fn get(&self, key: i32) -> GetResult {
        match self.data.get(&key).cloned() {
            None => GetResult::NotFound,
            Some(None) => GetResult::Deleted,
            Some(Some(val)) => GetResult::Value(val) 
        }
    }

    pub fn write_to_table(&self, to_dir: &Path) -> Table {
        let mut iter = self.iter();
        let mut tb = TableBuilder::new(to_dir);
        
        let mut block = BlockMut::new();
        while let Some((&key, &val)) = iter.next() {
            let command = match val {
                None => Command::Delete(key),
                Some(val) => Command::Put(key, val)
            };

            if !block.push_command(command) {
                tb.insert_block(&block);
                block.clear();
                block.push_command(command);
            }
        }
        if !block.is_empty() {
            tb.insert_block(&block); 
        }

        tb.build()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }
}