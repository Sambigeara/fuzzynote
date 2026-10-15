#!/bin/sh
# Builds the engine for the browser into web/pkg. The page is then this directory as it stands:
# point a relay at it with `fzn relay --web web`.
#
# Needs the wasm32-unknown-unknown target (rustup target add wasm32-unknown-unknown) and the
# wasm-bindgen command at the version crates/fzn-web/Cargo.toml names
# (cargo install wasm-bindgen-cli --version 0.2.129).
#
# The body's editor is built apart, by web/editor/build.sh, which needs npm, and its bundle,
# web/vendor/editor.js, is committed. It is built again here only when npm is at hand and the
# bundle is missing or older than what it is built from.
set -e
cd "$(dirname "$0")/.."
cargo build -p fzn-web --release --target wasm32-unknown-unknown
wasm-bindgen --target web --no-typescript --out-dir web/pkg \
    target/wasm32-unknown-unknown/release/fzn_web.wasm
ls -l web/pkg
# The editor, when what it is built from has changed since it was built; otherwise the
# committed bundle stands.
bundle=web/vendor/editor.js
if command -v npm >/dev/null 2>&1 && { [ ! -f "$bundle" ] || [ web/editor/editor.js -nt "$bundle" ] ||
    [ web/editor/package.json -nt "$bundle" ] || [ web/editor/package-lock.json -nt "$bundle" ]; }; then
    sh web/editor/build.sh
else
    echo "using the committed $bundle"
fi
