use rocksdb::{self, merge_operator, DB};
use serde_json;
use std::env;

fn increment_fn(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &merge_operator::MergeOperands,
) -> Option<Vec<u8>> {
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
    let args: Vec<String> = env::args().collect();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(
        bigarray::PREFIX.len(),
    ));
    opts.set_merge_operator_associative("incr", increment_fn);
    let db = DB::open(&opts, "/tmp/rocksdb").unwrap();

    let n;
    match db.get(bigarray::LEN_KEY) {
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
            bigarray::slice(db, 0, None);
        }
        2 => match args[1].as_str() {
            "length" => match db.get(bigarray::LEN_KEY) {
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
                let to = [&bigarray::PREFIX[..], &[255; 4][..]].concat();
                batch.delete_range(bigarray::PREFIX.to_vec(), to);
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
                            let key = [&bigarray::PREFIX[..], &j.to_be_bytes()[..]].concat();
                            batch.put(key, &args[i].as_bytes());
                        }
                        Err(error) => {
                            println!("Error parsing json: {}", error);
                        }
                    }
                }
                batch.merge(bigarray::LEN_KEY, &(args.len() as u32 - 2).to_be_bytes());
                db.write(batch).unwrap();
                drop(db);
                return;
            }
            // for testing the lexographical ordering of keys and iterators
            "put" => {
                let i = args[2].parse::<u32>().unwrap();
                let key = [&bigarray::PREFIX[..], &i.to_be_bytes()[..]].concat();
                db.put(key, &args[3].as_bytes()).unwrap();
                return;
            }
            "get" => {
                let key = [&bigarray::PREFIX[..], &args[2].as_bytes()[..]].concat();
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
            "slice" => {
                let start = args[2].parse::<u32>().unwrap();
                if args.len() < 4 {
                    bigarray::slice(db, start, None);
                } else {
                    let stop = args[3].parse::<u32>().unwrap();
                    bigarray::slice(db, start, Some(stop));
                }
            }
            // "reduce" => {
            //     let platform = v8::new_default_platform(0, false).make_shared();
            // }
            _ => {
                println!("Invalid command");
                return;
            }
        },
    }
}
