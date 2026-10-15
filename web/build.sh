#!/bin/sh
# Builds the engine for the browser into web/pkg. The page is then this directory as it stands:
# point a relay at it with `fzn relay --web web`.
#
# Needs the wasm32-unknown-unknown target (rustup target add wasm32-unknown-unknown) and the
# wasm-bindgen command at the version crates/fzn-web/Cargo.toml names
# (cargo install wasm-bindgen-cli --version 0.2.129).
#
# The page's reading of markdown and the body's editor are built apart, by web/editor/build.sh,
# which needs npm, into web/vendor, which is committed. They are built again here only when npm
# is at hand and the editor's bundle is missing or older than anything they are built from.
set -e
cd "$(dirname "$0")/.."
cargo build -p fzn-web --release --target wasm32-unknown-unknown
wasm-bindgen --target web --no-typescript --out-dir web/pkg \
    target/wasm32-unknown-unknown/release/fzn_web.wasm
ls -l web/pkg
# The bundles, when what they are built from has changed since they were built; otherwise the
# committed ones stand.
bundle=web/vendor/editor.js
if command -v npm >/dev/null 2>&1 && { [ ! -f "$bundle" ] ||
    [ -n "$(find web/editor -path web/editor/node_modules -prune -o -type f -newer "$bundle" -print)" ]; }; then
    sh web/editor/build.sh
else
    echo "using the committed $bundle"
fi
