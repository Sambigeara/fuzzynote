#!/bin/sh
# Builds the engine for the browser into web/pkg. The page is then this directory as it stands:
# point a relay at it with `fzn relay --web web`, or build fzn after it, which carries the page.
#
# Needs the wasm32-unknown-unknown target (rustup target add wasm32-unknown-unknown) and the
# wasm-bindgen command at the version crates/fzn-web/Cargo.toml names
# (cargo install wasm-bindgen-cli --version 0.2.129). The build is --locked, so a Cargo.lock that
# no longer matches the manifests is said here rather than quietly rewritten.
#
# The page's reading of markdown and the body's editor are built apart, by web/editor/build.sh,
# which needs npm, into web/vendor, which is committed. They are built again only by running
# that on purpose; CI checks that the committed ones are what it makes.
set -e
cd "$(dirname "$0")/.."
cargo build --locked -p fzn-web --release --target wasm32-unknown-unknown
# Where cargo puts what it builds, which need not be ./target.
target_dir="$(cargo metadata --format-version 1 --no-deps | sed 's/.*"target_directory":"\([^"]*\)".*/\1/')"
wasm-bindgen --target web --no-typescript --out-dir web/pkg \
    "$target_dir/wasm32-unknown-unknown/release/fzn_web.wasm"
ls -l web/pkg
