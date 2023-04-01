use rocksdb::{self, merge_operator, DB};
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
    Push(Vec<String>),
    Slice(i32, Option<i32>),
    Pop,
    Length,
    Reduce(String, Option<String>),
    Reducers,
    Clear,
    Attach(String, String, Option<String>),
    Get(String),
    Del(String),
}

fn cmd_from_args(args: Vec<String>) -> Option<Cmd> {
    if args.len() == 1 {
        return None;
    }
    match args[1].as_str() {
        "push" => Some(Cmd::Push(args[2..].to_vec())),
        "slice" => {
            if args.len() < 2 {
                return None;
            }
            let start;
            if args.len() == 2 {
                start = 0;
            } else {
                start = args[2].parse::<i32>().unwrap();
            }
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
        "reducers" => Some(Cmd::Reducers),
        "reduce" => {
            if args.len() < 3 {
                return None;
            }
            let script = args[2].clone();
            let initial_state = args[3].clone();
            Some(Cmd::Reduce(script, Some(initial_state)))
        }
        "attach" => {
            if args.len() < 4 {
                return None;
            }
            let name = args[2].clone();
            let script = args[3].clone();
            let initial_state = args[4].clone(); // this is optional
            Some(Cmd::Attach(name, script, Some(initial_state)))
        }
        "get" => {
            if args.len() < 3 {
                return None;
            }
            Some(Cmd::Get(args[2].clone()))
        }
        "del" => {
            if args.len() < 3 {
                return None;
            }
            Some(Cmd::Del(args[2].clone()))
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
    match db.get(bigarray::LENGTH) {
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
    let platform = v8::new_default_platform(0, false).make_shared();
    v8::V8::initialize_platform(platform);
    v8::V8::initialize();
    match cmd_from_args(args) {
        Some(Cmd::Push(elements)) => {
            let n = bigarray::push(&db, elements, n);
            println!("{n}");
        }
        Some(Cmd::Pop) => {
            let key = [&bigarray::PREFIX[..], &(n - 1).to_be_bytes()[..]].concat();
            let mut batch = rocksdb::WriteBatch::default();
            if n == 0 {
                return;
            }
            let v = db.get(key.clone()).unwrap();
            println!("{}", String::from_utf8(v.unwrap()).unwrap());
            batch.delete(key);
            batch.merge(bigarray::LENGTH, &(-1 as i32).to_be_bytes());
            db.write(batch).unwrap();
        }
        Some(Cmd::Slice(start, stop)) => {
            println!("{:?}", bigarray::slice(&db, start, stop))
        }
        Some(Cmd::Length) => match db.get(bigarray::LENGTH) {
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
        Some(Cmd::Clear) => {
            let mut batch = rocksdb::WriteBatch::default();
            let to = [&bigarray::PREFIX[..], &[255; 4][..]].concat();
            batch.delete_range(bigarray::PREFIX.to_vec(), to);
            db.write(batch).unwrap();
        }
        Some(Cmd::Reduce(reducer, initial_state)) => {
            let array = bigarray::slice(&db, 0, None);
            println!("{}", bigarray::reduce(array, reducer, initial_state));
        }
        Some(Cmd::Attach(name, script, initial_state)) => {
            if name == "length" {
                eprintln!("Name cannot be 'length'");
                return;
            }
            if !bigarray::valid_var_name(&name) {
                eprintln!("Invalid name: {}", name);
                return;
            }
            let attached = bigarray::attach(&db, name, script, initial_state);
            println!("{}", attached);
        }
        Some(Cmd::Get(name)) => {
            let state = bigarray::state(db, name);
            match state {
                Some(state) => println!("{}", state),
                None => println!("null"),
            }
        }
        Some(Cmd::Del(name)) => {
            let mut batch = rocksdb::WriteBatch::default();
            let key = [&bigarray::REDUCER[..], &name.as_bytes()[..]].concat();
            batch.delete(key);
            let key = [&bigarray::AGGREGATE[..], &name.as_bytes()[..]].concat();
            batch.delete(key);
            db.write(batch).unwrap();
        }
        Some(Cmd::Reducers) => {
            let reducers = bigarray::reducers(&db);
            println!("{:?}", reducers);
        }
        None => {
            eprintln!("Usage: bigarray push <json>...")
        }
    }
    unsafe {
        v8::V8::dispose();
    }
    v8::V8::dispose_platform();
}
