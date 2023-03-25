use rocksdb::{self, DB};
use serde_json;
use std::env;

fn main() {
    let prefix = b"bigarray:";
    let args: Vec<String> = env::args().collect();
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(prefix.len()));
    let default_cf_name = "default";
    let db = DB::open_cf(&options, "/tmp/rocksdb", &[default_cf_name]).unwrap();
    let default_cf = db.cf_handle(&default_cf_name).unwrap();

    let len_key = b"len";
    let n;
    match db.get_cf(default_cf, len_key) {
        Ok(Some(value)) => {
            n = u32::from_be_bytes(value.as_slice().try_into().unwrap());
        }
        Ok(None) => {
            n = 0;
        }
        Err(error) => {
            panic!("Error reading value: {}", error);
        }
    }
    match args.len() {
        0 => {
            panic!("Invalid number of arguments")
        }
        1 => {
            let mut iterator = db.prefix_iterator_cf(default_cf, prefix);
            while let Some(i) = iterator.next() {
                let (y, z) = i.unwrap();
                let k = y.into_vec();
                let v = String::from_utf8(z.into_vec()).unwrap();
                if k == b"len" {
                    continue;
                }
                let (_, b) = k.split_at(prefix.len());
                let a: [u8; 4] = b.try_into().unwrap();
                let i = i32::from_be_bytes(a);
                println!("[{}] {}", i, v);
            }
        }
        2 => match args[1].as_str() {
            "length" => match db.get_cf(default_cf, len_key) {
                Ok(Some(value)) => {
                    let n = u32::from_be_bytes(value.as_slice().try_into().unwrap());
                    println!("{}", n);
                }
                Ok(None) => {
                    println!("0");
                }
                Err(error) => {
                    panic!("Error reading value: {}", error);
                }
            },

            "clear" => {
                let to_key = [&prefix[..], &n.to_be_bytes()[..]].concat();
                db.delete_range_cf(&default_cf, &prefix[..], &to_key)
                    .unwrap();
                db.delete(len_key).unwrap();
            }
            _ => {
                panic!("Invalid command");
            }
        },
        _ => match args[1].as_str() {
            "push" => {
                let mut batch = rocksdb::WriteBatch::default();
                for i in 2..args.len() {
                    match serde_json::from_str::<serde_json::Value>(&args[i]) {
                        Ok(_) => {
                            let j = n + i as u32 - 2;
                            let key = [&prefix[..], &j.to_be_bytes()[..]].concat();
                            batch.put_cf(&default_cf, key, &args[i].as_bytes());
                        }
                        Err(error) => {
                            println!("Error parsing json: {}", error);
                        }
                    }
                }
                batch.put_cf(
                    &default_cf,
                    len_key,
                    &(n + args.len() as u32 - 2).to_be_bytes(),
                );
                db.write(batch).unwrap();
                drop(db);
                return;
            }
            // for testing the lexographical ordering of keys and iterators
            "put" => {
                let i = args[2].parse::<u32>().unwrap();
                let key = [&prefix[..], &i.to_be_bytes()[..]].concat();
                db.put_cf(&default_cf, key, &args[3].as_bytes()).unwrap();
                return;
            }
            "get" => {
                let key = [&prefix[..], &args[2].as_bytes()[..]].concat();
                let value = db.get_cf(default_cf, key).unwrap();
                match value {
                    Some(value) => {
                        println!("{}", String::from_utf8(value).unwrap());
                    }
                    None => {
                        println!("key {} not found", args[2]);
                    }
                }
                return;
            }
            _ => {
                println!("Invalid command");
                return;
            }
        },
    }
}
