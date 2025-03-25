use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
};

use disk_level::DiskLevel;
use mem_level::MemLevel;
use merge_iter::merge_sorted_commands;
use once_done::OnceDoneTrait;
use table::{BlockMut, Table, TableBuilder};

pub mod bloom;
pub mod disk_level;
pub mod mem_level;
pub mod merge_iter;
pub mod once_done;
pub mod table;

const NUM_LEVELS: usize = 6;
const DATABASE_DIRECTORY: &'static str = "/Users/noahr/dev/rust/lsm-tree/database";

pub enum GetResult {
    NotFound,
    Deleted,
    Value(i32),
}

pub struct Database {
    data_directory: PathBuf,
    memory: MemLevel,
    disk: [DiskLevel; NUM_LEVELS],
}

impl Database {
    pub fn new() -> Self {
        let data_directory = PathBuf::from(DATABASE_DIRECTORY);

        let memory = MemLevel::new();
        let disk: [DiskLevel; NUM_LEVELS] =
            std::array::from_fn(|idx| DiskLevel::new(&data_directory, (idx + 1) as u32));

        Self {
            data_directory,
            memory,
            disk,
        }
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
            let l0_table = self
                .memory
                .write_to_table(self.data_directory.join("level0").as_path());
            merge(&mut vec![l0_table], &mut self.disk[0]);
            self.memory.clear();
        }

        for i in 0..(NUM_LEVELS - 1) {
            if self.disk[i].is_over_capacity() {
                let (left, right) = self.disk.split_at_mut(i + 1);
                merge(&mut left[i].tables, &mut right[0]);
            } else {
                break
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

pub fn merge(l1: &mut Vec<Table>, l2: &mut DiskLevel) {
    let intersections = find_intersections(l1, &l2.tables);

    match intersections {
        IntersectionResult::NoIntersections(indices) => {
            for &idx in indices.iter().rev() {
                let table = &mut l1[idx];
                table.rename(&l2.level_directory);
                l2.tables.push(l1.remove(idx));
            }
        }
        IntersectionResult::IntersectingGroups(groups) => {
            let mut block = BlockMut::new();
            let mut new_tables = vec![];

            for group in groups {
                let (slice_start, slice_end) = group.tables1;
                let l1_commands = (&mut l1[slice_start..slice_end]).iter_mut().flat_map(|t| {
                    (t.view().once_done(|v| v.table.delete_file()))
                        .flat_map(|b| unsafe { b.as_ref().unwrap().iter() })
                });

                let (slice_start, slice_end) = group.tables2;
                let l2_commands = (&mut l2.tables[slice_start..slice_end])
                    .iter_mut()
                    .flat_map(|t| {
                        (t.view().once_done(|v| v.table.delete_file()))
                            .flat_map(|b| unsafe { b.as_ref().unwrap().iter() })
                    });

                let mut merge_commands_iter = merge_sorted_commands(l1_commands, l2_commands);

                let mut tb = TableBuilder::new(&l2.level_directory);
                while let Some(command) = merge_commands_iter.next() {
                    if !block.push_command(command) {
                        tb.insert_block(&block);

                        if tb.full() {
                            let new_table = tb.build();
                            tb = TableBuilder::new(&l2.level_directory);
                            new_tables.push(new_table);
                        }
                        block.clear();
                        block.push_command(command);
                    }
                }
                if !block.is_empty() {
                    tb.insert_block(&block);
                    block.clear();
                }
                if !tb.is_empty() {
                    new_tables.push(tb.build());
                }
            }
            l2.tables.append(&mut new_tables);
        }
    }

    l2.sort_tables();
}

enum IntersectionResult {
    NoIntersections(Vec<usize>),
    IntersectingGroups(Vec<IntersectionGroup>),
}

struct IntersectionGroup {
    tables1: (usize, usize),
    tables2: (usize, usize),
}

fn find_intersections<'a>(tables_l1: &'a [Table], tables_l2: &'a [Table]) -> IntersectionResult {
    let mut non_intersecting = Vec::new();
    let mut intersecting_groups = Vec::new();

    let mut i = 0;
    let mut j = 0;

    while i < tables_l1.len() {
        let start_i = i;

        while j < tables_l2.len() && tables_l1[i].intersects(&tables_l2[j]) == Ordering::Greater {
            j += 1;
        }

        let start_j = j;

        let mut intersected = false;
        while j < tables_l2.len() && tables_l1[i].intersects(&tables_l2[j]) == Ordering::Equal {
            intersected = true;
            j += 1;
        }

        if intersected {
            i += 1;

            while i < tables_l1.len() {
                let intersects_prev = tables_l1[i].intersects(&tables_l2[j - 1]);
                let intersects_cur = if j < tables_l2.len() {
                    tables_l1[i].intersects(&tables_l2[j])
                } else {
                    Ordering::Less
                };
                if intersects_prev == Ordering::Equal || intersects_cur == Ordering::Equal {
                    if intersects_cur == Ordering::Equal {
                        j += 1;
                    }
                    while j < tables_l2.len()
                        && tables_l1[i].intersects(&tables_l2[j]) == Ordering::Equal
                    {
                        j += 1;
                    }
                } else {
                    break;
                }
                i += 1;
            }

            intersecting_groups.push(IntersectionGroup {
                tables1: (start_i, i),
                tables2: (start_j, j),
            });
        } else {
            non_intersecting.push(i);
            i += 1;
        }
    }

    if !non_intersecting.is_empty() {
        IntersectionResult::NoIntersections(non_intersecting)
    } else {
        IntersectionResult::IntersectingGroups(intersecting_groups)
    }
}
