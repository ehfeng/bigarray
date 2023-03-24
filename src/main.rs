use rocksdb::{self, DB};
use std::env;

fn main() {
    let prefix = b"bigarray:";
    let args: Vec<String> = env::args().collect();
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(prefix.len());
    options.set_prefix_extractor(prefix_extractor);
    let db = DB::open_cf(&options, "/tmp/rocksdb", &["default"]).unwrap();

    let default_cf = db.cf_handle(&"default").unwrap();
    match args.len() {
        0 => {
            panic!("Invalid number of arguments")
        }
        1 => {
            panic!("no handling inputs just yet")
        }
        2 => {
            if args[1].as_str() == "clear" {
                let to_key = [&prefix[..], &b"z"[..]].concat();
                db.delete_range_cf(&default_cf, &prefix[..], &to_key)
                    .unwrap();
            } else {
                panic!("Invalid command");
            }
        }
        _ => match args[1].as_str() {
            "clear" => {
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
            "put" => {
                let key = [&prefix[..], &args[2].as_bytes()[..]].concat();
                db.put_cf(&default_cf, key, args[3].as_bytes()).unwrap();
                drop(db);
                return;
            }
            _ => {
                println!("Invalid command");
                return;
            }
        },
    }
}
