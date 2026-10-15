#!/bin/sh
# Bundles the body's editor, CodeMirror and its vim mode, into one file the page imports when a
# body first opens: web/vendor/editor.js. The output is committed, so the relay and CI
# see only static files and nothing is built on the way to the page. The versions are pinned in
# package.json and package-lock.json; the same inputs give a byte-identical file. Needs npm.
set -e
cd "$(dirname "$0")"
npm ci --ignore-scripts --no-audit --no-fund
./node_modules/.bin/esbuild editor.js --bundle --minify --format=esm --target=es2022 \
    --outfile=../vendor/editor.js
ls -l ../vendor/editor.js
