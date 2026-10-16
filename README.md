# fzn

fzn is a blank page with one input. Typing finds: as you type, the notes you have already
written that belong with it come up, each with the reason it was found. Enter writes: the line
becomes a new note. Tab on a note goes into it, and what you write there is
about that note and links to it; a tag such as `#house` is gone into the same way, and what you
write there carries it. Where you are is what you are talking about. There are no folders, no
documents and no tree, only notes and the links between them.

The same engine runs in a terminal and in a browser tab. Devices sync through a small relay you
run yourself, and every note is sealed on the device before it leaves, under a key derived from
a passphrase the relay never sees. Everything is local and instant; the network is only ever
the way between your own machines.

## Status

A rewrite in Rust, in progress as of October 2026. Expect the formats and keys to move. The
original Go app is kept at the tag `go-final`. Releases are tagged `v<version>` and built by the
Release workflow into a draft, which is published by hand.

## Install

No release is published yet, so for now fzn is built from source, which needs a Rust
toolchain:

```sh
cargo install --path crates/fzn --locked
fzn
fzn --help
```

From the first published release,

```sh
curl -fsSL https://github.com/Sambigeara/fuzzynote/releases/latest/download/install.sh | sh
```

puts one static binary in `~/.local/bin` (`sh -s -- --system` for `/usr/local/bin`,
`--version 0.26.0` to pin one, `--uninstall` to remove it), after checking its SHA-256. Run it
again to take up a release. There are builds for Linux on x86_64 and arm64, and for Apple
silicon; anything else builds from source as above.

`FZN_RELEASES` points the installer somewhere other than GitHub: anything laid out as GitHub
lays out releases, such as `dist/` after `release.sh`, served over http or read as `file://`.

The config file is `~/.config/fzn/config.toml`. Notes live in spaces, separate sets of notes
kept apart from each other, one directory each under `~/.fzn/<space>/`. Plain `fzn` opens the
space named in the config file (`main` to begin with); `fzn --space garden` opens another and
creates it if it is new.

The whole app in one sentence: type to find or to write; Enter writes the line, or opens a new
note under the one you are on; Tab goes into the note you are on, or from the line into the
tag, person, day or note the line names; Esc clears the line, or comes back.
Every other key is shown in the footer, and only when it applies where the cursor is. To edit a
note at length, Ctrl-O opens it in `$EDITOR`.

A note is plain text: its first line is its head, and `#tag`, `@person`, dates written
`2026-10-16` and URLs mean what they look like. For example:

```
Boiler service #house
Booked for /fri, ask about the pressure drop
```

`/fri`, `/today`, `/+3`, `/-mon` and the like are written out as dates when the note is saved.
A note whose head starts `[ ]` is a task: archiving it ticks it, `[x]`, and bringing it back
unticks it.

On the line, words find and everything else holds. `"oil tank"` must be in a note, `#house`,
`@joe` and `/fri` must be on it, `[ ]` holds the open tasks, and a span holds what was written in
it: `/-mon 18:00..` since six on Monday evening, `2026-10-01..2026-10-05` those five days. `~` at
the start of the line finds heads by their letters in order, so `~bsv` finds Boiler service. A
line of nothing but labels, such as `[ ] #house /today`, is gone into with one Tab.

## Sync

Run the relay on a machine all your devices can reach:

```sh
fzn relay --listen 0.0.0.0:3960
```

and name it on each device, in `~/.config/fzn/config.toml`:

```toml
relay = "ws://nas.local:3960"
```

(or `--relay`, or `$FZN_RELAY`). While fzn is open it keeps the space in step with the relay,
and works as before when the relay is out of reach. With no relay named, nothing leaves the
machine.

The relay holds sealed lines it cannot read. The first time a space is opened on a machine,
fzn asks for its passphrase (twice if the space is new) and keeps the key it derives; the same
passphrase on another device opens the same notes. A forgotten passphrase cannot be recovered.

`fzn --version` says the version and the commit and day it was built from, so
`ssh nas.local fzn --version` says what the relay runs.

The relay has no authentication and no TLS yet. Anyone who can reach it can copy the sealed
lines or add lines of their own, so keep it on a private network.

## Browser

The relay carries the page. Open its address, `http://nas.local:3960`. A tab is one more device:
it keeps the space in the browser between visits, asks for the passphrase once, and syncs with
the relay it was loaded from.

To work on the page, build its engine and point a relay at the directory, which it reads again
for every request:

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.129
web/build.sh
fzn relay --listen 0.0.0.0:3960 --web web
```

A build of fzn after `web/build.sh` carries the page as it then stands; one without `web/pkg`
carries none, and says so.

## Import and export

```sh
fzn export > notes.jsonl         # every live note, one plain JSON line each
fzn export '#house /-mon..'      # what that line finds, archived notes too, in its order
fzn tick ID ...                  # tick these tasks, by the ids the export gives them
fzn untick ID ...                # and bring them back
fzn import-jsonl notes.jsonl     # the way back in
fzn import-markdown ~/pages      # a directory of markdown pages
```

Each takes `--space` like the app. The export is plain and unsealed, so your notes can always be
read and taken away without fzn. `tick` does what Ctrl-V does to a task, `[ ]` to `[x]` and
archived, to every note it names or to none, and writes each as the export would. It works with
fzn open on the space, which shows the tick within a second.

## Design

[DESIGN.md](DESIGN.md) is the source of truth: vision, principles, the model, the engine, storage
and sync, the clients, and the dated decisions behind them.

## Licence

AGPL-3.0. See [LICENSE](LICENSE).
