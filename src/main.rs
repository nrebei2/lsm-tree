mod data;
use data::Database;

fn main() {
    let mut db = Database::new();

    for key in 0..15 {
        db.insert(key, key + 10);
    }

    println!("{:?}", db.get(10));    
}
