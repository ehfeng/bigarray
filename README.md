# BigArray

Toy cli for learning rust, rusty_v8 and rocksdb.

```sh
bigarray push '{"greeting": "hi"}' '{"greeting": "hello"}'
bigarray length # 2
bigarray clear
bigarray # {"greeting": "hello"} {"greeting": "hi"}
bigarray slice 0 1 # {"greeting": "hello"} 
bigarray reduce "(s, v) => if (v.greeting == 'hello') s + 1 else s" 0 # 1
```

## Install

```sh
export RUSTY_V8_ARCHIVE=$RUSTY_V8_MIRROR/v0.66.0/librusty_v8_release_aarch64-apple-darwin.a
cargo build
```
