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
        n = i32::from_be_bytes(v.try_into().unwrap());
    }
    for op in operands {
        n = n + i32::from_be_bytes(op.try_into().unwrap());
    }
    Some(n.to_be_bytes().to_vec())
}

enum Cmd {
    All,
    Get(i32),
    Push(Vec<String>),
    Slice(i32, Option<i32>),
    Pop,
    Length,
    Clear,
}

fn cmd_from_args(args: Vec<String>) -> Option<Cmd> {
    if args.len() < 2 {
        return Some(Cmd::All);
    }
    match args[1].as_str() {
        "push" => Some(Cmd::Push(args[2..].to_vec())),
        "slice" => {
            if args.len() < 3 {
                return None;
            }
            let start = args[2].parse::<i32>().unwrap();
            if args.len() < 4 {
                Some(Cmd::Slice(start, None))
            } else {
                let stop = args[3].parse::<i32>().unwrap();
                Some(Cmd::Slice(start, Some(stop)))
            }
        }
        "pop" => Some(Cmd::Pop),
        "length" => Some(Cmd::Length),
        "clear" => Some(Cmd::Clear),
        "get" => {
            if args.len() < 3 {
                return None;
            }
            let i = args[2].parse::<i32>().unwrap();
            Some(Cmd::Get(i))
        }
        _ => None,
    }
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
            n = i32::from_be_bytes(value.as_slice().try_into().unwrap());
        }
        Ok(None) => {
            n = 0;
        }
        Err(error) => {
            panic!("Error reading value: {}", error);
        }
    }
    match cmd_from_args(args) {
        Some(cmd) => match cmd {
            Cmd::All => {
                bigarray::slice(db, 0, None);
            }
            Cmd::Push(elements) => {
                let mut batch = rocksdb::WriteBatch::default();
                for (i, element) in elements.iter().enumerate() {
                    match serde_json::from_str::<serde_json::Value>(&element) {
                        Ok(_) => {
                            let j = n + i as i32;
                            let key = [&bigarray::PREFIX[..], &j.to_be_bytes()[..]].concat();
                            batch.put(key, &element.as_bytes());
                        }
                        Err(error) => {
                            eprintln!("{}", element);
                            eprintln!("Error parsing json: {}", error);
                        }
                    }
                }
                batch.merge(bigarray::LEN_KEY, &(elements.len() as i32).to_be_bytes());
                db.write(batch).unwrap();
                drop(db);
                return;
            }
            Cmd::Pop => {
                let key = [&bigarray::PREFIX[..], &(n - 1).to_be_bytes()[..]].concat();
                let mut batch = rocksdb::WriteBatch::default();
                if n == 0 {
                    return;
                }
                let v = db.get(key.clone()).unwrap();
                println!("{}", String::from_utf8(v.unwrap()).unwrap());
                batch.delete(key);
                batch.merge(bigarray::LEN_KEY, &(-1 as i32).to_be_bytes());
                db.write(batch).unwrap();
            }
            Cmd::Slice(start, stop) => bigarray::slice(db, start, stop),
            Cmd::Length => match db.get(bigarray::LEN_KEY) {
                Ok(Some(value)) => {
                    let n = i32::from_be_bytes(value.as_slice().try_into().unwrap());
                    println!("{}", n);
                }
                Ok(None) => {
                    println!("0");
                }
                Err(error) => {
                    panic!("Error reading value: {}", error);
                }
            },
            Cmd::Clear => {
                let mut batch = rocksdb::WriteBatch::default();
                let to = [&bigarray::PREFIX[..], &[255; 4][..]].concat();
                batch.delete_range(bigarray::PREFIX.to_vec(), to);
                db.write(batch).unwrap();
            }
            Cmd::Get(i) => {
                let key = [&bigarray::PREFIX[..], &i.to_be_bytes()[..]].concat();
                if let Some(value) = db.get(key).unwrap() {
                    println!("{}", String::from_utf8(value).unwrap())
                }
            }
        },
        None => {
            eprintln!("Usage: bigarray push <json>...")
        }
    }
}
