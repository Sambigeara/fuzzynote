#!/bin/sh
# Builds the engine for the browser into web/pkg. The page is then this directory as it stands:
# point a relay at it with `fzn relay --web web`.
#
# Needs the wasm32-unknown-unknown target (rustup target add wasm32-unknown-unknown) and the
# wasm-bindgen command at the version crates/fzn-web/Cargo.toml names
# (cargo install wasm-bindgen-cli --version 0.2.129).
set -e
cd "$(dirname "$0")/.."
cargo build -p fzn-web --release --target wasm32-unknown-unknown
wasm-bindgen --target web --no-typescript --out-dir web/pkg \
    target/wasm32-unknown-unknown/release/fzn_web.wasm
ls -l web/pkg
