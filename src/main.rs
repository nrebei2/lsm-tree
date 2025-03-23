use std::ops::Deref;

use data::{mem_level::MemLevel, Database};

mod data;

fn main() {
    let db = Database::new();

    println!("Hello, world!");
}
