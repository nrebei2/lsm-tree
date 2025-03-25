mod data;
use std::collections::HashMap;

use data::Database;
use rand::{distr::{Distribution, Uniform}, Rng};


fn main() {
    let mut db = Database::new();
    // let mut db_mock = HashMap::new();

    // let range = -100_000..500_000;

    // let between = Uniform::try_from(range.clone()).unwrap();
    // let mut rng = rand::rng();
    // for _ in 0..range.len()/2 {
    //     if rng.random_bool(0.1) {
    //         let key = between.sample(&mut rng);
    //         db.delete(key);
    //         db_mock.remove(&key);
    //     } else {
    //         let key = between.sample(&mut rng);
    //         let val = between.sample(&mut rng);
    //         db.insert(key, val);
    //         db_mock.insert(key, val);
    //     }
    // }

    // for key in range {
    //     assert_eq!(db.get(key), db_mock.get(&key).cloned());  
    // }

    assert_eq!(db.get(-99_998), None)
}
