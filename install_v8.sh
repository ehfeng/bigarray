#!/bin/bash

# see https://github.com/denoland/rusty_v8/releases

for REL in v0.66.0; do
  mkdir -p $RUSTY_V8_MIRROR/$REL
  for FILE in \
    librusty_v8_release_aarch64-apple-darwin.a \
    librusty_v8_debug_aarch64-apple-darwin.a \
  ; do
    if [ ! -f $RUSTY_V8_MIRROR/$REL/$FILE ]; then
      wget -O $RUSTY_V8_MIRROR/$REL/$FILE \
        https://github.com/denoland/rusty_v8/releases/download/$REL/$FILE
    fi
  done
done