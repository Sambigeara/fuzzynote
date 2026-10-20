#!/bin/sh
# Bundles the page's reading of markdown and the note's editor into web/vendor. render.js is
# Lezer's markdown parser, the highlighting and the renderer. editor.js is CodeMirror and its vim
# mode, the head of the row under the cursor and the open body, and it shares render.js's code
# rather than carrying a copy; the page imports both as it starts. Each language of code is a
# file of its own under code/, imported the first time a block in it is read or written, and what
# several files share is a chunk of its own. The output is committed, so the relay, its machine
# and CI see only static files and nothing is built on the way to the page. The versions are
# pinned in package.json and package-lock.json; the same inputs give byte-identical files. Needs
# npm.
set -e
cd "$(dirname "$0")"
npm ci --ignore-scripts --no-audit --no-fund
rm -rf ../vendor
./node_modules/.bin/esbuild render.js editor.js code/*.js --bundle --splitting --minify --format=esm \
    --target=es2022 --outdir=../vendor
ls -lR ../vendor
