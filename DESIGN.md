# Fuzzynote

## Vision
A blank page with a single blinking cursor. You type a thought, a note, a TODO. As you type, the
engine finds what you have already written that belongs with it and shows you why. You can lock
the link in, follow it, or ignore it. Everything is local, private and instant. The same engine
runs in a terminal and in a browser, syncs seamlessly between your devices, and can be shared with
other people when you choose.

## Principles
- One flat space of notes. No folders, no documents, no tree. Structure is a graph: the links
  you make and the links the engine finds.
- The input is the search is the composer. Typing is finding; Enter is creating.
- A note is text. Its first line is its head. There is no separate "line" and "note".
- Instant. Every keystroke resolves within a frame over tens of thousands of notes. Nothing on the
  hot path touches disk or network.
- Private by construction. The engine never calls out. Sync moves opaque events through a relay
  that cannot read them.
- Explainable relevance. Every suggestion carries its reason. No black box in the loop by default.
- Converge, never conflict. Every change is an event; devices merge; nothing is lost.
- One core, thin faces. The engine is a library with no UI. Terminal and browser are renderers.
- Keyboard first, almost no chrome.
- Archived is not forgotten. The list hides archived notes; the engine still remembers them.

## What stays from the original fzn (the soul)
- The search line is the composer: filter live, Enter turns the search into a new note carrying
  that text.
- Zoom on what is shared: select notes, press Enter, and whatever they have in common becomes
  the context: tags, people, rare words. The old common-prefix zoom is the special case.
- Search groups: Tab-separated terms, `~` fuzzy, `!` inverse, substring by default.
- Hashtags, @mentions, `{d}` date expansion, URLs opened or copied with one key.
- Archive with one key; show/hide archived globally.
- Undo/redo, move up/down, copy/paste of whole notes.
- Local-first, remote-second. Offline is the normal case.

## Model
- **Space**: a set of notes plus explicit links. One space per person for now; sharing is later.
- **Note**: stable id; text (a text CRDT); created and updated times; archived flag; pinned
  flag. No manual position. Derived, never stored: head (first line), body, tags, mentions,
  wikilinks, dates, URLs.
- **Link**: from, to, kind, created. Only explicit links are stored (the user locked them in, or
  wrote `[[head]]` or `@person`). Everything else is recomputed on demand.
- **Syntax that the engine understands**: `#tag`, `@person`, `[[note head]]`, `{d}` expanding to
  a date on entry, URLs. Nothing else is special. In particular the old ` >> ` prefix paths are
  plain text; the words in them still count, the hierarchy does not.
- **Context inheritance**: a note created while a context is active (tags, people, a zoomed
  neighbourhood) inherits that context. This is what prefix prepending used to do, without a
  tree. Built for the simplest case: a note created while looking around another links to it.

## Engine
The engine is a library with three parts. It owns no UI and makes no network calls.

**Index.** Unicode-aware tokeniser, lowercased, light stemming optional. An inverted index with
term frequencies and document lengths, and a trigram index for fuzzy matching. Both update
incrementally on every note change. The old corpus is 8.7k notes, 284k tokens and 12k distinct
terms; the index for a space ten times that size fits comfortably in memory.

**Scorers.** Each takes the current input plus context and returns candidate notes with a score
and a human-readable reason. Cheapest first:
1. *Explicit*: shared tags, mentions, wikilinks, locked links, and words of the query that appear
   in a note's head (weighted by rarity, so a note titled with what you typed outranks a
   one-liner that merely mentions it; BM25's length normalisation would otherwise bury long
   notes).
2. *Lexical*: BM25 over the inverted index. Rare shared words count far more than common ones.
   The word under the cursor is a prefix, not a word: it stands for every vocabulary word it
   starts, in the word scorer and in the head signal, and a note earns credit for its best
   completion only. This is what makes results right while a word is still being typed.
3. *Fuzzy*: trigram overlap, so a misspelling still finds its note.
4. *Temporal*: created or edited close in time; dates written in the text.
5. *Structural*: notes that sat adjacent in the list, notes you navigated between.
6. *Feedback*: a locked link boosts shared terms between the two notes; a dismissal dampens.
7. *Reserved*: a local embedding scorer, on device, if semantic similarity is ever wanted. It is
   another scorer, not a dependency.

**Combiner.** Weighted sum with per-scorer caps, top-k with reasons attached. Budget: under five
milliseconds per keystroke for ten thousand notes.

**Tuning happens in the product.** Every suggestion shows its reasons; a key locks a link, which
is stored as `[[head]]` in the note's own text; the weights live in a config file; a toggle shows
scores. There is no evaluation harness and no labelling step. Nothing is added to the engine
until daily use asks for it.

## Storage and sync
- **v1: one append-only log per device.** Each line is a full snapshot of one note: id, text,
  created, updated, archived, device. Loading replays every log in the directory and the newest
  version of each id wins, ties broken by device id. A device only ever writes its own file, so
  the directory can be synced by anything that copies files, Syncthing included, with no
  conflicts. Last-writer-wins on a whole note is the known limitation, accepted for now.
  Deletion is a tombstone line, kept so an older version arriving later cannot resurrect the
  note.
- **Later: a text CRDT per note** (Yrs, the Rust port of Yjs, or Loro). The log lines then carry
  CRDT updates instead of snapshots and nothing above the store module changes. In the browser
  the same log lives in IndexedDB or OPFS.
- Sync is "send me every line since the last one I have". The relay is a single self-hosted
  binary that stores and fans out opaque lines per space over a websocket. It never decodes
  them, so end-to-end encryption is a client-side concern and needs no relay changes.
- Sharing is sharing a subset of updates. Deferred, but nothing above prevents it.
- Optional plain-text mirror: export the space as a directory of markdown files for grep,
  Obsidian or Silverbullet. The mirror is derived, never the source of truth.

## Clients
- **Terminal**: Rust and ratatui, with the original fzn keys, because that is where the muscle
  memory is. The input is the top line; Down moves onto the list, Up and Escape come back, and
  typing always goes to the top line. A chord acts on the top line when the cursor is there and on
  the selected note otherwise: Enter creates or zooms, Ctrl-D clears or deletes, Ctrl-V shows
  archived or archives. Ctrl-O opens the note in your editor, Ctrl-U and Ctrl-R undo and redo any
  change, Ctrl-P pastes a `[[link]]` to the selected note into the input, Ctrl-C copies its first
  URL (or its head) to the clipboard, Ctrl-_ opens the URL, Ctrl-B shows scores, Escape twice
  quits. No inline multi-line editor in v1. Not yet reinstated: Ctrl-S multi-select with Enter
  zooming on what the selection shares, Tab search groups, Ctrl-^ export.
- **Zoom.** Enter on a note makes it the context: the list becomes its neighbourhood, links in
  and out, shared tags and people, shared words, shared dates, each with its reason. Enter again
  walks a hop further; Escape steps back to where you came from, with the input you had
  restored. Typing inside a context narrows the neighbourhood, and
  a note created inside one links back to it, which is context inheritance in its simplest form.
  This is the ego graph as a flat list; following a link is picking its target here.
- **Browser**: the same core compiled to wasm32 via wasm-bindgen, with a thin DOM renderer. The
  graph view lives here as an ego graph of the current note, one or two hops out.

## Decisions
- **Rust, not Go and not Zig.** One codebase must run natively and in WASM with small binaries
  and no GC. A text CRDT per note is a day-one requirement once line and body are one text, and
  Yrs and Loro exist in Rust. wasm-bindgen, ratatui and the unicode crates cover the rest. Zig
  would mean hand-writing the CRDT, the JS bridge and unicode segmentation on a pre-1.0 language;
  none of that is the interesting problem. Go stays the language of the relay if pollen's
  transport is ever wanted there.
- **Line and body are one text.** The head is the first line.
- **CRDT from a library, behind a narrow trait.** Last-writer-wins on whole notes is not
  acceptable once notes are texts edited on two devices offline.
- **Graph, not tree.** Prefix paths are not syntax. Explicit structure is flat labels and links:
  tags, people, wikilinks, locked links. No note contains another. The old ` >> ` convention
  imports as plain text and is found by the lexical scorer.
- **No manual ordering (provisional).** The list is recency plus pins, so the position CRDT
  goes. Sam to confirm.
- **Dumb relay, self-hosted, after the browser build exists.**
- **Same repository.** The Go implementation is tagged and kept reachable; the Rust workspace
  takes over the root. History is part of the project.

## Open questions
- One CRDT document per note or one per space? Per note makes sharing a note trivial; per space is
  cheaper to load. Likely per note with a space-level index document.
- Should related-notes search include archived notes by default? The old corpus is 97% archived,
  so almost certainly yes, shown dimmed.
- How do `[[head]]` links survive a head being edited? Resolve by id at lock-in time, display by
  current head.
- Name. `fzn` stays unless something better appears.

## Milestones
0. **Data.** Done 2026-10-12. The 2022 corpus and the 2026 Silverbullet pages are
   in `~/fzn-corpus/`, outside this repo.
1. **The simplest live version, in the terminal.** Built 2026-10-12 as a Cargo workspace beside
   the Go code: `crates/fzn-engine` (model, index, three scorers, no I/O) and `crates/fzn` (the
   app: config, per-device log store, importers, terminal UI). Both corpora imported. Daily use
   from here; tuning follows use. Then the Go code is removed in one commit.
2. **Same core in the browser.** WASM build, DOM renderer, ego graph.
3. **Relay and sync.** Single binary, opaque lines, multiple devices. Until then, Syncthing on
   the notes directory.
4. **Text CRDT per note.** Behind the store module.
5. **Sharing and collaboration.**
