# Working in fuzzynote

## DESIGN.md is the source of truth
Read it before changing anything. Its Principles say what the app is; its Decisions are dated
and give their reasons. When a decision changes, update DESIGN.md in the same change. A
reversal is recorded as a new decision that names what it replaces; the old reasoning is not
deleted, because why something was dropped is part of the design.

## Layout
- `crates/fzn-engine`: the whole app with no screen and no keyboard. A pure library.
- `crates/fzn`: the terminal face, and `fzn relay`, which syncs devices and hands out the page.
- `crates/fzn-web`: the engine built for wasm32, with the text boundary the page talks to.
- `web/`: the browser page; `web/build.sh` rebuilds its bundle into `web/pkg`.

`fzn-engine` does no I/O, reads no clock and opens no socket. The wasm build fails if it does,
and that build is how the rule is kept. Time, new ids and randomness come from `Host`, so a
test can stop the clock and nothing depends on when it runs.

## The shape of the app
- Actions, not keys. The session is never told which key was pressed. Each face owns its key
  table, because a terminal's chords cannot be a browser's and touch has none.
- The footer, or the page's hint list, shows only what applies right now.
- No key whose meaning depends on the previous key. That is one more thing to remember.
- Nothing is added to the engine until daily use asks for it. No evaluation harnesses, no
  labelling work; the app is tuned by using it.
- A link is the target's id held on the note, however it was made. There is one kind.
- One writer per log. Lines are opaque to replicas, except a log's first line, which may be a
  compaction header.

## Privacy
Nothing recognisable from the author's notes may enter the tree: not in tests, fixtures, doc
examples or commit messages. Use invented material only: Boiler, House, a garden, a training
app; device id `3f9a01c2`; host `nas.local`. Before any commit, scan the diff and the unpushed
history for names. The author's notes live under `~/.fzn` and `~/fzn-corpus`. Never read them
into a test and never commit anything taken from them.

## Working rules
- Commit only when asked. Never push unless asked.
- Verification is all four of:
  `cargo fmt --all --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`,
  `cargo build -p fzn-web --release --target wasm32-unknown-unknown`.
  CI runs the same four.
- Check the terminal app in tmux on a scratch root: point `FZN_ROOT` and `XDG_CONFIG_HOME` at
  a temporary directory. Never run it against `~/.fzn`.
- `web/build.sh` rebuilds the browser bundle. It needs the wasm-bindgen CLI at the version
  `crates/fzn-web/Cargo.toml` names.

## Style
- Module docs explain why the module is as it is, not what it contains.
- Test names are sentences.
- Comments are prose. No bullet-point comments.
- Match the density of comments already in the file.
