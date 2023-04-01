use rocksdb::{self, DBWithThreadMode, SingleThreaded};
use std::collections::HashMap;

pub const AGGREGATE: &[u8; 3] = b"agg"; // iterable
pub const LENGTH: &[u8; 3] = b"len"; // singular value
pub const REDUCER: &[u8; 3] = b"red"; // iteratable
pub const PREFIX: &[u8; 3] = b"val";

pub fn valid_var_name(name: &String) -> bool {
    let mut is_valid_var = false;
    {
        let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
        let handle_scope = &mut v8::HandleScope::new(isolate);
        let context = v8::Context::new(handle_scope);
        let scope = &mut v8::ContextScope::new(handle_scope, context);
        let mut var_name = String::from("var ");
        var_name.push_str(name.as_str());
        let var = v8::String::new(scope, var_name.as_str()).unwrap();
        if let Some(script) = v8::Script::compile(scope, var, None) {
            if let Some(_) = script.run(scope) {
                is_valid_var = true;
            }
        }
    }
    is_valid_var
}

pub fn push(db: &DBWithThreadMode<SingleThreaded>, elements: Vec<String>, n: i32) -> i32 {
    let mut batch = rocksdb::WriteBatch::default();
    for (i, element) in elements.iter().enumerate() {
        match serde_json::from_str::<serde_json::Value>(&element) {
            Ok(_) => {
                let j = n + i as i32;
                let key = [&PREFIX[..], &j.to_be_bytes()[..]].concat();
                batch.put(key, &element.as_bytes());
            }
            Err(error) => {
                eprintln!("{}", element);
                eprintln!("Error parsing json: {}", error);
            }
        }
    }
    batch.merge(LENGTH, &(elements.len() as i32).to_be_bytes());

    let mut read_opts = rocksdb::ReadOptions::default();
    read_opts.set_iterate_upper_bound(&PREFIX[..]);
    let mut iterator = db.iterator_opt(
        rocksdb::IteratorMode::From(&REDUCER[..], rocksdb::Direction::Forward),
        read_opts,
    );
    while let Some(i) = iterator.next() {
        let (y, z) = i.unwrap();

        let script = String::from_utf8(z.into_vec()).unwrap();

        let reducer_name = String::from_utf8(y.into_vec()[3..].to_vec()).unwrap();
        let reducer_key = [&AGGREGATE[..], &reducer_name.clone().into_bytes()].concat();
        let state = db.get(reducer_key).unwrap().unwrap();
        let initial_value = String::from_utf8(state).unwrap();

        let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
        let handle_scope = &mut v8::HandleScope::new(isolate);
        let context = v8::Context::new(handle_scope);
        let scope = &mut v8::ContextScope::new(handle_scope, context);

        let code = v8::String::new(scope, script.as_str()).unwrap();
        let code = v8::Script::compile(scope, code, None).unwrap();
        let function: v8::Local<v8::Function> = code.run(scope).unwrap().try_into().unwrap();
        let initial_value = v8::String::new(scope, initial_value.as_str()).unwrap();
        let mut initial_value = v8::json::parse(scope, initial_value).unwrap();
        let undef: v8::Local<v8::Value> = v8::undefined(scope).into();
        for (i, elm) in elements.iter().enumerate() {
            let local_str = v8::String::new(scope, elm.as_str()).unwrap();
            let value = v8::json::parse(scope, local_str).unwrap();
            let idx = v8::Integer::new(scope, i as i32).into();
            let args = [initial_value, value, idx];
            initial_value = function.call(scope, undef, &args).unwrap();
        }
        let state_key = [&AGGREGATE[..], &reducer_name.into_bytes()].concat();
        let state_value = initial_value.to_rust_string_lossy(scope).into_bytes();
        batch.put(state_key, state_value);
    }
    db.write(batch).unwrap();

    match db.get(LENGTH) {
        Ok(Some(value)) => {
            return i32::from_be_bytes(value.as_slice().try_into().unwrap());
        }
        Ok(None) => return 0,
        Err(error) => {
            panic!("Error reading value: {}", error);
        }
    }
}

pub fn slice(db: &DBWithThreadMode<SingleThreaded>, start: i32, stop: Option<i32>) -> Vec<String> {
    let n;
    match db.get(LENGTH) {
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
    let upper_bound;
    if stop != None && stop.unwrap() < n {
        upper_bound = stop.unwrap();
    } else {
        upper_bound = n;
    }
    let start_key = [&PREFIX[..], &start.to_be_bytes()[..]].concat();
    let mut iterator = db.iterator(rocksdb::IteratorMode::From(
        &start_key,
        rocksdb::Direction::Forward,
    ));
    let mut l: Vec<String> = Vec::new();
    while let Some(i) = iterator.next() {
        let (y, z) = i.unwrap();
        let k = y.into_vec();
        let v = String::from_utf8(z.into_vec()).unwrap();

        let (_, b) = k.split_at(PREFIX.len());
        let i;
        if let Ok(c) = b.try_into() {
            let a: [u8; 4] = c;
            i = i32::from_be_bytes(a);
            if i + 1 > upper_bound as i32 {
                break;
            }
            l.push(v);
        }
    }
    l
}

pub fn reduce(array: Vec<String>, reducer: String, initial_state: Option<String>) -> String {
    let aggregator_str;
    {
        let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
        let handle_scope = &mut v8::HandleScope::new(isolate);
        let context = v8::Context::new(handle_scope);

        let scope = &mut v8::ContextScope::new(handle_scope, context);

        let code = v8::String::new(scope, reducer.as_str()).unwrap();
        let script = v8::Script::compile(scope, code, None).unwrap();
        let function: v8::Local<v8::Function> = script.run(scope).unwrap().try_into().unwrap();
        let undef: v8::Local<v8::Value> = v8::undefined(scope).into();

        let mut aggregator = v8::undefined(scope).into();
        if let Some(state) = initial_state {
            if let Some(json_string) = v8::String::new(scope, state.as_str()) {
                aggregator = v8::json::parse(scope, json_string).unwrap()
            }
        }

        for elm in array {
            let local_str = v8::String::new(scope, elm.as_str()).unwrap();
            let args = [aggregator, v8::json::parse(scope, local_str).unwrap()];
            aggregator = function.call(scope, undef, &args).unwrap();
        }
        aggregator_str = aggregator.to_rust_string_lossy(scope);
    }
    aggregator_str
}

pub fn reducers(db: &DBWithThreadMode<SingleThreaded>) -> HashMap<String, String> {
    let mode = rocksdb::IteratorMode::From(&REDUCER[..], rocksdb::Direction::Forward);
    let mut read_opts = rocksdb::ReadOptions::default();
    read_opts.set_iterate_upper_bound(&PREFIX[..]);
    let mut iterator = db.iterator_opt(mode, read_opts);
    let mut h: HashMap<String, String> = HashMap::new();
    while let Some(i) = iterator.next() {
        let (x, y) = i.unwrap();
        let k = x.into_vec();
        let reducer_name = String::from_utf8(k).unwrap();
        let script = String::from_utf8(y.into_vec()).unwrap();
        h.insert(reducer_name, script);
    }
    h
}

pub fn attach(
    db: &DBWithThreadMode<SingleThreaded>,
    name: String,
    script: String,
    initial_value: Option<String>,
) -> bool {
    let elements = slice(db, 0, None);
    {
        let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
        let handle_scope = &mut v8::HandleScope::new(isolate);
        let context = v8::Context::new(handle_scope);
        let scope = &mut v8::ContextScope::new(handle_scope, context);
        let initial_value = v8::String::new(scope, initial_value.unwrap().as_str()).unwrap();
        let mut initial_value = v8::json::parse(scope, initial_value).unwrap();
        let code = v8::String::new(scope, script.as_str()).unwrap();
        if let Some(code) = v8::Script::compile(scope, code, None) {
            let mut batch = rocksdb::WriteBatch::default();
            let reducer_key = [&REDUCER[..], &name.clone().into_bytes()].concat();
            let function: v8::Local<v8::Function> = code.run(scope).unwrap().try_into().unwrap();

            batch.put(reducer_key, script.into_bytes());
            let undef: v8::Local<v8::Value> = v8::undefined(scope).into();

            for (i, elm) in elements.iter().enumerate() {
                let local_str = v8::String::new(scope, elm.as_str()).unwrap();
                let value = v8::json::parse(scope, local_str).unwrap();
                let idx = v8::Integer::new(scope, i as i32).into();
                let args = [initial_value, value, idx];
                initial_value = function.call(scope, undef, &args).unwrap();
            }

            let state_key = [&AGGREGATE[..], &name.into_bytes()].concat();
            let state_value = initial_value.to_rust_string_lossy(scope).into_bytes();
            batch.put(state_key, state_value);
            db.write(batch).unwrap();
        }
    }
    true
}

pub fn state(db: DBWithThreadMode<SingleThreaded>, name: String) -> Option<String> {
    let state_key = [&AGGREGATE[..], &name.into_bytes()].concat();
    if let Ok(state_value) = db.get(state_key) {
        if let Some(state_value) = state_value {
            return Some(String::from_utf8(state_value).unwrap());
        }
    }
    None
}
