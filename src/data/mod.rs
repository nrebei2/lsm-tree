use std::{cmp::Ordering, ops::DerefMut, path::PathBuf};

use disk_level::DiskLevel;
use mem_level::MemLevel;
use merge_iter::merge_sorted_commands;
use once_done::OnceDoneTrait;
use table::{BlockMut, Table, TableBuilder};
use tokio::sync::RwLock;

use crate::config::{MEM_CAPACITY, NUM_LEVELS};

pub mod bloom;
pub mod disk_level;
pub mod mem_level;
pub mod merge_iter;
pub mod once_done;
pub mod table;

pub enum GetResult {
    NotFound,
    Deleted,
    Value(i32),
}

pub struct Database {
    data_directory: PathBuf,
    memory: RwLock<MemLevel>,
    disk: [RwLock<DiskLevel>; NUM_LEVELS],
}

impl Database {
    pub fn new(data_directory: PathBuf) -> Self {
        let memory = MemLevel::new(&data_directory);
        let disk: [RwLock<DiskLevel>; NUM_LEVELS] =
            std::array::from_fn(|idx| RwLock::new(DiskLevel::new(&data_directory, (idx + 1) as u32)));

        Self {
            data_directory,
            memory: RwLock::new(memory),
            disk,
        }
    }

    pub async fn insert(&self, key: i32, value: i32) {
        let mut mem_write = self.memory.write().await;
        mem_write.insert(key, value);

        if mem_write.len() >= MEM_CAPACITY as usize {
            let old_mem = mem_write.clear();
            self.handle_overflow(old_mem).await;
        } 
    }

    pub async fn delete(&self, key: i32) {
        let mut mem_write = self.memory.write().await;
        mem_write.delete(key);
        if mem_write.len() >= MEM_CAPACITY as usize {
            let old_mem = mem_write.clear();
            self.handle_overflow(old_mem).await;
        }
    }

    async fn handle_overflow(&self, mem: MemLevel) {
        let l0_table = mem
            .write_to_table(self.data_directory.join("level0").as_path());  

        let mut cur = self.disk[0].write().await;
        merge(&mut vec![l0_table], cur.deref_mut());

        for i in 0..(NUM_LEVELS - 1) {
            if cur.is_over_file_capacity() {
                let mut next = self.disk[i+1].write().await;
                merge(&mut cur.tables, next.deref_mut());
                cur = next;
            } else {
                break;
            }
        }

        if cur.is_over_file_capacity() {
            eprintln!("Final level is over capacity!");
        }
    }

    pub async fn get(&self, key: i32) -> Option<i32> {
        match self.memory.read().await.get(key) {
            GetResult::Deleted => return None,
            GetResult::Value(val) => return Some(val),
            GetResult::NotFound => {}
        };

        for i in 0..NUM_LEVELS {
            match self.disk[i].read().await.get(key) {
                GetResult::Deleted => return None,
                GetResult::Value(val) => return Some(val),
                GetResult::NotFound => {}
            };
        }

        None
    }

    pub fn finalize(self) {
        let mem = self.memory.into_inner();
        mem.write_to_table(self.data_directory.join("level0").as_path());  
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

            for group in groups.iter() {
                let (slice_start, slice_end) = group.tables1;
                let l1_commands = (&mut l1[slice_start..slice_end]).iter_mut().flat_map(|t| {
                    (t.view().once_done(|v| v.delete_file()))
                        .flat_map(|b| unsafe { b.as_ref().unwrap().iter() })
                });

                let (slice_start, slice_end) = group.tables2;
                let l2_commands = (&mut l2.tables[slice_start..slice_end])
                    .iter_mut()
                    .flat_map(|t| {
                        (t.view().once_done(|v| v.delete_file()))
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

            for idx in groups.iter().flat_map(|g| g.tables1.0..g.tables1.1).rev() {
                l1.remove(idx);
            }

            for idx in groups.iter().flat_map(|g| g.tables2.0..g.tables2.1).rev() {
                l2.tables.remove(idx);
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
