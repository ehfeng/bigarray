use rocksdb::{self, DBWithThreadMode, SingleThreaded};

pub const PREFIX: &[u8; 9] = b"bigarray:";
pub const LEN_KEY: &[u8; 12] = b"bigarray:len";

pub fn slice(db: DBWithThreadMode<SingleThreaded>, start: u32, stop: Option<u32>) {
    let n;
    match db.get(LEN_KEY) {
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
    let mut s: &[String];
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
            println!("[{}] {}", i, v);
        }
    }
}
