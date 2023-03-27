# BigArray

Toy cli for learning rust, rusty_v8 and rocksdb.

```sh
bigarray push '{"greeting": "hi"}' '{"greeting": "hello"}' # 2
bigarray length # 2
bigarray clear
bigarray # {"greeting": "hello"} {"greeting": "hi"}
bigarray slice 0 1 # {"greeting": "hello"} 
bigarray get x # 2
# javascript
bigarray reduce "(s, v) => if (v.greeting == 'hello') s + 1 else s" 0 # 1
bigarray attach x "(s, v) => if (v.greeting == 'hello') s + 1 else s" 0 # 1
bigarray push '{"greeting": "hello"}' # 3
bigarray x # 2
```

## Install

```sh
export RUSTY_V8_ARCHIVE=$RUSTY_V8_MIRROR/v0.66.0/librusty_v8_release_aarch64-apple-darwin.a
cargo build
```
