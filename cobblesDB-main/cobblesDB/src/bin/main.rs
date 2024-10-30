
use futures_util::{StreamExt, SinkExt};
use cobblesDB::compact::CompactionOptions;
use serde::{Deserialize, Serialize};

use tempfile::tempdir;

use std::path::PathBuf;
use std::str::from_utf8;
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio::sync::Mutex;
use cobblesDB::lsm_storage::CobblesDB;
use cobblesDB::lsm_storage::LsmStorageOptions;
mod wrapper;
use wrapper::cobblesDB_wrapper;
use std::time::Instant;
use cobblesDB_wrapper::compact::{
    LeveledCompactionController, LeveledCompactionOptions, SimpleLeveledCompactionController,
    SimpleLeveledCompactionOptions, TieredCompactionController, TieredCompactionOptions,
};

fn write_test(lsm: Arc<CobblesDB>, data_size: usize) {
    let start = Instant::now();
    for i in 0..data_size{
        let i_temp = i.clone();
        let key_str = i_temp.to_string(); 
        let key = key_str.as_bytes();
        lsm.put(key, key);
    }
    println!("Write test : {:?}", start.elapsed());
}

fn read_test(lsm: Arc<CobblesDB>, data_size: usize) {

    for i in 0..data_size{
        let i_temp = i.clone();
        let key_str = i_temp.to_string(); 
        let key = key_str.as_bytes();
        match lsm.put(key, key){
            Ok(()) => {
                // print!("put key : {}, value : {}\n", key_str, key_str);
            },
            _ => {
                panic!("Error");
            }
        }
    }

    let start = Instant::now();
    for i in 0..data_size{
        let i_temp = i.clone();
        let key_str = i_temp.to_string(); 
        let key = key_str.as_bytes();
        match lsm.get(key){
            Ok(Some(value)) => {
                let value_str = from_utf8(&value).unwrap();
                // println!("Key : {}, Value : {}", key_str, value_str);
            },
            _ => {
                panic!("Error");
            }
        }
    }
    println!("Read test : {:?}", start.elapsed());
}

#[tokio::main]
async fn main() {
    let leveled_options = CompactionOptions::Leveled(
        LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 100,
            level_size_multiplier: 2,
            base_level_size_mb: 128,
            max_levels: 4,
        },
    );
    let simple_leveled_options = CompactionOptions::Simple(
            SimpleLeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2,
                max_levels: 3,
                size_ratio_percent: 200,
            },
        );
    let tiered_options = CompactionOptions::Tiered(
            TieredCompactionOptions {
                num_tiers: 3,
                max_size_amplification_percent: 200,
                size_ratio: 1,
                min_merge_width: 2,
            },
        );
    let simpled_leveled_dir : PathBuf = "simple_leveled.db".into();
    let simple_leveled_db = CobblesDB::open(
        simpled_leveled_dir,
        LsmStorageOptions::default_option(simple_leveled_options),
    ).unwrap();

    let tiered_dir : PathBuf = "tiered.db".into();
    let tiered_db = CobblesDB::open(
        tiered_dir,
        LsmStorageOptions::default_option(tiered_options),
    ).unwrap();

    let leveled_dir : PathBuf = "leveled.db".into();
    let leveled_db = CobblesDB::open(
        leveled_dir,
        LsmStorageOptions::default_option(leveled_options),
    ).unwrap();

    let ten_thousand = 10000;
    let hundred_thousand = 100000;
    let million = 1000000;
    let ten_million = 10000000;
    let twenty_million = 20000000;
    let hundred_million = 100000000;
    let n =  hundred_thousand;
    // write_test(simple_leveled_db, n);
    // write_test(tiered_db, n);
    // write_test(leveled_db, n);
    
    // read_test(simple_leveled_db, n);
    // read_test(tiered_db, n);
    read_test(leveled_db, n);

}
