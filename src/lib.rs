use rocksdb::{self, DBWithThreadMode, SingleThreaded};

pub const PREFIX: &[u8; 9] = b"bigarray:";
pub const LEN_KEY: &[u8; 12] = b"bigarray:len";

pub fn slice(db: DBWithThreadMode<SingleThreaded>, start: i32, stop: Option<i32>) -> Vec<String> {
    let n;
    match db.get(LEN_KEY) {
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
    let platform = v8::new_default_platform(0, false).make_shared();
    v8::V8::initialize_platform(platform);
    v8::V8::initialize();
    let aggregator_str;
    {
        // Create a new Isolate and make it the current one.
        let isolate = &mut v8::Isolate::new(v8::CreateParams::default());

        // Create a stack-allocated handle scope.
        let handle_scope = &mut v8::HandleScope::new(isolate);

        // Create a new context.
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
    unsafe {
        v8::V8::dispose();
    }
    v8::V8::dispose_platform();
    aggregator_str
}
