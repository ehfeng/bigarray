use rocksdb::merge_operator::MergeOperands;
use rocksdb::{self, DB};
use serde_json;
use std::env;

fn increment_fn(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    // dbg!(&existing_val);
    let mut n = 0;
    if let Some(v) = existing_val {
        n = u32::from_be_bytes(v.try_into().unwrap());
    }
    for op in operands {
        n = n + u32::from_be_bytes(op.try_into().unwrap());
    }
    Some(n.to_be_bytes().to_vec())
}

fn main() {
    let prefix = b"bigarray:";
    let len_key = b"bigarray:len";

    let args: Vec<String> = env::args().collect();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(prefix.len()));
    opts.set_merge_operator_associative("incr", increment_fn);
    let db = DB::open(&opts, "/tmp/rocksdb").unwrap();

    let n;
    match db.get(len_key) {
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
            let mut iterator = db.prefix_iterator(prefix);
            while let Some(i) = iterator.next() {
                let (y, z) = i.unwrap();
                let k = y.into_vec();
                let v = String::from_utf8(z.into_vec()).unwrap();
                if k == len_key {
                    continue;
                }
                let (_, b) = k.split_at(prefix.len());
                if let Ok(c) = b.try_into() {
                    let a: [u8; 4] = c;
                    let i = i32::from_be_bytes(a);
                    println!("[{}] {}", i, v);
                }
            }
        }
        2 => match args[1].as_str() {
            "length" => match db.get(len_key) {
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
                let mut batch = rocksdb::WriteBatch::default();
                let to = [&prefix[..], &[255; 4][..]].concat();
                batch.delete_range(prefix.to_vec(), to);
                db.write(batch).unwrap();
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
                            batch.put(key, &args[i].as_bytes());
                        }
                        Err(error) => {
                            println!("Error parsing json: {}", error);
                        }
                    }
                }
                batch.merge(len_key, &(args.len() as u32 - 2).to_be_bytes());
                db.write(batch).unwrap();
                drop(db);
                return;
            }
            // for testing the lexographical ordering of keys and iterators
            "put" => {
                let i = args[2].parse::<u32>().unwrap();
                let key = [&prefix[..], &i.to_be_bytes()[..]].concat();
                db.put(key, &args[3].as_bytes()).unwrap();
                return;
            }
            "get" => {
                let key = [&prefix[..], &args[2].as_bytes()[..]].concat();
                let value = db.get(key).unwrap();
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
