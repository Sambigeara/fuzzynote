# Fuzzynote

## Vision
A blank page with a single blinking cursor. You type a thought, a note, a TODO. As you type, the
engine finds what you have already written that belongs with it and shows you why. Go into one of
those and write there, and what you write belongs to it: where you are is what you are talking
about. Everything is local, private and instant. The same engine
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
- One core, thin faces. The engine is the whole app with no screen and no keyboard: a face says
  what was asked for and paints what it is told to show. Its keys, its gestures and its pixels
  are all a face owns, so no rule of the app can differ between a terminal and a browser.
- Keyboard first, almost no chrome.
- Ergonomics is a primary goal, and it has two halves: nothing visible ever waits, and there is
  almost nothing to remember. The whole app fits in one sentence: type to find or to write; Enter
  goes into the note under the cursor, or writes the line; Esc comes back; and where you are is
  what you are talking about. Every other key is maintenance, and the footer shows only the ones
  that apply right now. A key is justified by the flow it sits in, never inherited.
- Archived is not forgotten. The list hides archived notes; the engine still remembers them.

## What stays from the original fzn (the soul)
- The search line is the composer: filter live, Enter turns the search into a new note carrying
  that text.
- Zoom on what is shared: select notes, press Enter, and whatever they have in common becomes
  the context: tags, people, rare words. The old common-prefix zoom is the special case.
- Search groups: Tab-separated terms, `~` fuzzy, `!` inverse, substring by default.
- Hashtags, @mentions, date expansion, URLs opened or copied with one key.
- Archive with one key; show/hide archived globally.
- Undo/redo, move up/down, copy/paste of whole notes.
- Local-first, remote-second. Offline is the normal case.

## Model
- **Space**: a set of notes plus explicit links, kept apart from every other space. On disk a
  space is one directory of logs under the root. Plain `fzn` opens the space named in the config
  file (`main` to begin with) and `--space NAME` opens another. Sharing a space is later.
- **Note**: stable id; text (a text CRDT); links; created and updated times; archived flag;
  pinned flag. No manual position. Derived, never stored: head (first line), body, tags,
  mentions, dates, URLs.
- **Link**: from one note to another, held on the note as the target's id. There is one kind.
  It survives a rename and adds no words to the text. Links are made by position: a note
  written while looking around another is about it, and links to it. Nobody has to say "link".
  Brackets are the way of typing one from inside the editor: when a note is saved, each
  `[[head]]` that names an existing note becomes a link, and a link whose brackets were taken
  out is dropped. The one explicit gesture is a correction: a chord on a note in a zoom links it
  or unlinks it, for when the engine was wrong or the note was written in the wrong place. The
  engine does not read brackets when it searches. A link adds no words, and still a note is
  found by the name of what it is about: when you type, the head of a note that a note links to
  counts as its own head does, one hop and no further. Only explicit links are stored;
  everything else is recomputed on demand.
- **Syntax that the engine understands**: `#tag`, `@person`, dates, URLs, and `[[note head]]`
  as a way of typing a link. Nothing else is special. In particular the old ` >> ` prefix paths
  are plain text; the words in them still count, the hierarchy does not.
- **Date**: plain text in the note, written `2026-10-16`, and never stored apart from it. The
  engine reads it on load, as it reads tags. A link is held as an id because a note's head can
  change; a date's name cannot, so it lives in the text. The day a note was written is not
  typed at all: it is the note's `created`. `/today`, `/tomorrow`, `/yesterday`, a day of the
  week (`/fri` is the next Friday, never today) and `/+3` or `/-2` are quicker ways of typing a
  date, and any beginning that only one of them has will do (`/tom`, `/f`). Any other slash is
  left alone. The top line shows the date a word stands for and searches with it. Saving a
  note, from the top line or from the editor, writes the date in its place, at the moment a
  `[[head]]` becomes a link. A date typed as a search finds the notes that mention the day and
  the notes written on it, and counts as a date and not as the numbers in it. The old
  `Mon, Jan 2, 2006` form is still read.
- **Context inheritance**: where you are is what you are talking about. A note created while a
  context is active (tags, people, a zoomed neighbourhood) inherits that context. This is what
  prefix prepending used to do, without a tree and without the typing: the old ` >> ` prefix was
  this context written by hand on every line, and Ctrl-P pasted it. Built for the simplest case:
  a note created while looking around another links to it.

## Engine
The engine is a library, and it is the app. It owns no UI, opens no file, reads no clock and
makes no network call, so the same code runs in a terminal, in a browser tab and in a test.

**Session.** What a face drives, one per person per space. Three things go in and two come
out.
- In: an *action*, which is what was asked for and never which key asked: the line now reads
  this, move the cursor, write the line, go into the note, step back, archive, delete, link,
  undo, the text of a note as it was edited. An action that does not apply where the cursor is
  does nothing.
- In: versions of notes written elsewhere, by another device or another face on this one.
- In, from the host: the time, and an id for a new note.
- Out: *effects*, which are what the face has to do about an action: keep this version, put
  this note in front of the person to edit, copy this, open this URL. The session has already
  done its own part, so an effect that fails costs a message and nothing else.
- Out: the *view*, which is everything there is to paint: the path, the line and the dates its
  date words stand for, the rows with their reasons, where the cursor is, the note to read,
  what just happened, and what can be asked for where the cursor is, each with what to call it
  there. A face puts its own key or button beside each.

Everything decided about how the app behaves lives here: what going into a note does with the
line, what is listed first, what a note written in place links to, what is offered where. Under
the session is the part that finds things, in three parts.

**Index.** Unicode-aware tokeniser, lowercased, light stemming optional. An inverted index with
term frequencies and document lengths, and a trigram index for fuzzy matching. Both update
incrementally on every note change. The old corpus is 8.7k notes, 284k tokens and 12k distinct
terms; the index for a space ten times that size fits comfortably in memory.

**Scorers.** Each takes the current input plus context and returns candidate notes with a score
and a human-readable reason. Cheapest first:
1. *Explicit*: shared tags, mentions, link targets, and words of the query that appear in a
   note's head (weighted by rarity, so a note titled with what you typed outranks a
   one-liner that merely mentions it; BM25's length normalisation would otherwise bury long
   notes). For what is typed, a word in the head of a note that a note links to counts too, at
   a little less than a word in its own (`about` in the weights), with the reason
   `about Boiler`.
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

**Combiner.** Weighted sum with per-scorer caps, top-k with reasons attached. Two facts come
before any score: what is linked to the note being looked around, and the note headed with
what was typed. Budget: under five milliseconds per keystroke for ten thousand notes.

**Tuning happens in the product.** Every suggestion shows its reasons; writing where you are
makes a link, and a chord corrects one, stored on the note as the target's id; the weights live
in a config file; a toggle shows scores. There is no evaluation harness and no labelling step.
Nothing is added to the engine until daily use asks for it.

## Storage and sync
- **The note is plain text; what holds it need not be.** All of a note's meaning is in its
  characters, in forms people already know: `#tag`, `@person`, `2026-10-16`, a URL, a first
  line. That is the agreed structure, and the test for adding to it is whether the note still
  reads right on paper. Beside the text go the facts it cannot carry: id, links, times,
  archived, deleted. What you said goes in the text; what happened to the note goes beside it.
  A file per note is not the store: files cannot merge two devices' edits, and most notes are
  one line.
- **v1: one append-only log per device, per space.** Each line is a full snapshot of one note
  as JSON: `v`, the version of the line format, then id, text, links, created, updated, stamp,
  archived, deleted, device. Times are RFC 3339 with the offset of the clock that wrote them,
  `2026-10-13T10:12:56+01:00`, so a line reads without the app, and a note keeps the day it
  was written on wherever it is read. Lines from before versions hold unix seconds and are
  still read; a line with a later version than the build knows is left alone, not misread.
  `updated` is when the text last changed, so archiving a note does not re-date it. `stamp` is when the version was written,
  and a local change is always stamped past the version it replaces. Loading replays every log
  in the space and the version with the newest stamp wins, ties broken by device id. A device
  only ever writes its own file, so the root can be synced by anything that copies files,
  Syncthing included, with no conflicts. Last-writer-wins on a whole note is the known
  limitation, accepted for now. Deletion is a tombstone line, kept so an older version arriving
  later cannot resurrect the note. What a line says, and which of two versions is the newer,
  is the engine's to settle, so that every device agrees. Where the lines are kept is the
  face's: files under the terminal.
- **Logs are reread as they grow.** Whenever the keyboard has been idle for a second the app
  replays what has been appended since it last looked, so a second instance or a file-sync
  delivery shows up without a restart. The relay will feed the same path.
- **Later: a text CRDT per note** (Yrs, the Rust port of Yjs, or Loro). The log lines then carry
  CRDT updates instead of snapshots and nothing above the store module changes. In the browser
  the same log lives in IndexedDB or OPFS.
- Sync is "send me every line since the last one I have". The relay is a single self-hosted
  binary that stores and fans out opaque lines per space over a websocket. It never decodes
  them, so end-to-end encryption is a client-side concern and needs no relay changes.
- Sharing is sharing a subset of updates. Deferred, but nothing above prevents it.
- **The snapshot line is the way in and out, for good.** `fzn export` writes every live note
  as one such line, and a file of them put in a space's directory is read as one more log.
  When the logs carry CRDT updates, or are encrypted, this format stays, so notes can always be
  read and taken away without the app. It carries the facts beside the text as well as the
  text: the 2022 corpus is what it looks like when only the text gets out (creation time
  survives on 7% of its notes).
- A directory of markdown files for grep or Obsidian is a possible second export. It would be
  derived, never the source of truth.

## Clients
- **Terminal**: Rust and ratatui, with the original fzn keys where they still earn their place,
  because that is where the muscle memory is. It is a face on the session and adds four things:
  its key table, the editing of the top line, the painting, and carrying out the effects, which
  here means a log file, your editor, the clipboard and the system's way of opening a URL. The
  input is the top line; Down moves onto the
  list, Up and Escape come back, and typing always goes to the top line. A chord acts on the top
  line when the cursor is there and on the selected note otherwise: Enter writes or goes in,
  Ctrl-D clears or deletes, Ctrl-V shows archived or archives. Archived notes are out of every
  list, typed results included, until Ctrl-V on the top line asks for them. Ctrl-O opens the
  note in your editor, Ctrl-U and Ctrl-R undo and redo any change, Ctrl-L links or unlinks the
  selected note (see Zoom), Ctrl-C copies its first URL (or its head) to the clipboard, Ctrl-_
  opens the URL, Ctrl-B shows scores, Escape twice quits. A date word on the top line has the
  date it stands for shown beside the line, dimmed. The title is a breadcrumb saying where
  you are, and the footer lists only the keys that apply to where the cursor is: on the top
  line, on a note, inside a zoom. No inline multi-line editor in v1. Not yet reinstated: Ctrl-S
  multi-select with Enter zooming on what the selection shares, Tab search groups, Ctrl-^
  export.
- **Zoom.** Enter on a note makes it the context, and the breadcrumb grows by one. The list
  becomes its neighbourhood, in two parts. What is linked to or from it comes first, newest on
  top: a link is a fact, and this part is a list you keep. What the engine finds besides follows,
  best first: shared tags and people, shared words, shared dates, each with its reason. The line
  comes with you, under one rule: if the note you went into contains what you typed, or its
  head and the heads of the notes it links to have every word of it, you were searching for it,
  so the line clears and Escape brings it back; otherwise you were writing, and the draft is
  carried in to be written here. Escape never throws away what you typed: a line
  written inside stays on the way out, and the search comes back only to an empty line. Either
  way, what you write inside is about the note
  and links to it, and lands at the top of the linked part; this is context inheritance in its
  simplest form. Typing inside a context narrows the neighbourhood and ranks what is left. Ctrl-L
  moves a note between the two parts by linking or unlinking it; it is a correction, not the
  mechanism, and outside a zoom it has nothing to do. Enter again walks a hop further, and Enter
  on a note already on the path goes back to it, so the path never repeats; Escape steps back one
  hop, onto the top line. This is the ego graph as a flat list; following a link is picking its
  target here.
- **Browser**: the same session compiled to wasm32 via wasm-bindgen, with a thin DOM renderer.
  Its keys are its own: a browser keeps Ctrl-W, Ctrl-R, Ctrl-L and most of the terminal's
  chords for itself, and a phone has none. Its line is an ordinary text field. The graph view
  lives here as an ego graph of the current note, one or two hops out.

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
  tags, people, links. No note contains another. The old ` >> ` convention
  imports as plain text and is found by the lexical scorer.
- **No manual ordering (provisional).** The list is recency plus pins, so the position CRDT
  goes. Sam to confirm.
- **A link is a fact, not a score, and there is one kind.** Every link is the target's id held
  on the note, however it was made. Linked notes are listed ahead of suggestions, and no weight
  can bury one.
- **A name is a fact too.** Decided 2026-10-13. The note whose head is what you typed comes
  first, whatever it scores and whatever the limit, and says `its name`. That means every word
  of its head typed and no other, in any order and case, with the last word allowed to be
  unfinished: `boiler se` puts Boiler Service first, and `boiler` alone does not. The
  scores could not be made to do this. In `main` the median note is 23 words and the longest is
  7,493, so a document earns almost nothing for its own words and the notes that say its name
  outrank it: one document was fifth for its own name and another note eighth. Three settings
  of `bm25_b` across five names got none of them all right, the wins were by hundredths, and
  with no length penalty long documents turn up for searches that are not about them. A rule
  is also what lets "type the name, Down, Enter" be done without reading the list. Notes that
  share a head all come first, in the order of their scores.
- **Position is the link.** Writing inside a note's zoom is how one note comes to be about
  another; nothing is typed and no key is pressed for it. The explicit chord is a correction.
  There is no marking of links while drafting: to put a draft somewhere, go there and write it,
  and the line comes with you. A Tab-to-link was considered and dropped, because it was one more
  thing to remember for a flow position already covers.
- **A name finds what was written in it.** Decided 2026-10-13. Typing `boiler` lists Boiler
  and the notes written in it, each saying `about Boiler`, and `boiler quoted` finds the one
  note among them that says "quoted". So typing a note's name shows what going into it shows,
  and the names after the arrow on a row count as words of that row. In 2022 this was the main
  way of finding: 85% of notes carried a ` >> ` path, most of them three to six labels deep,
  and any label gathered its group. Position now makes the link without the typing, so without
  this every note written in place would be out of reach of the word it was filed under, and
  a link would be a folder that has to be opened. The name is read when the search is made and
  not copied onto the note, so a rename is followed and the text stays as it was written. It
  goes one hop: `house` finds Boiler and not what was written in Boiler, which is what going
  into House shows and what the row says. A neighbourhood is not widened this way; what is
  linked is listed there already. The carry rule reads the same names, or a note found by one
  would take the search in with it as a draft. Measured on the 2022 notes with a hub made for
  every first label: up to a fifth more time per keystroke, and every keystroke under 3 ms.
- **The app is headless.** Decided 2026-10-13. Until then the engine was the part that finds
  things, and the app was the terminal: the carry rule, the path, what Escape does and what the
  footer offers were written against one terminal library's keys and widgets. A second face
  would have copied all of it and the copies would have drifted. Now the session is the app and
  a face cannot reach around it. Four calls shaped the interface.
  - *Actions, not keys.* The terminal's chords cannot be a browser's, and touch has none, so
    the session is never told which key was pressed. "A chord acts on the top line or on the
    selected note" is the terminal's way of saving keys and lives in its key table; to the
    session, clearing the line and deleting a note are two different things to ask for.
  - *The face edits the line.* A terminal has to move a cursor through a string itself. A
    browser is given a text field that already does, with the phone's keyboard and its
    autocorrect. So the session is told what the line now reads and nothing finer.
  - *Effects out, nothing done.* A version to keep comes out as data and the face decides where
    it goes, so the session needs no file in a terminal and no database in a tab. The editor,
    the clipboard and opening a URL are asked for the same way.
  - *The time is the host's.* The session asks the host for the time and for new ids. A test
    stops the clock, so nothing in the app's tests depends on when they run.

  Checked by running the binaries from before and after with the same 55 keys on a copy of
  `main`: the screens matched but for one reworded message, and so did the lines written.
- **The engine sees only what the text carries, plus where you were.** Words, tags, dates and
  position are the free signals. The paths you walk between notes are the next one: going from A
  to B again and again is a relation expressed without saying so, and a scorer that reads it can
  still give its reason ("you came here from A"). Embeddings come after that, if ever. Neither
  before daily use shows the gap.
- **Archived is hidden until asked for.** Everywhere, typed results included. The engine still
  indexes archived notes; the lists leave them out.
- **Spaces, not one pile.** Notes that do not belong together live in separate spaces, and one
  of them is home.
- **A date is text, and `/fri` is a way of typing one.** Decided 2026-10-13, replacing `{d}`.
  The old token only wrote today, and that is what it was used for in the 2022 corpus. A note now records the
  day it was written, so the mechanism is for the days that are not today. There is no `/d`,
  which would be a code to remember. The word becomes the date when the note is saved and not
  as it is typed, so no key does anything unusual and the editor works the same way. There is
  no general slash-command system: one trigger, one job, until a second expansion is wanted.
- **Days are local.** A note's day is the day it was where it was written, kept by the offset
  in its times, and today is this machine's today. The offset comes from the system through
  one libc call, the only unsafe code in the app.
- **Imported pages become the notes they would have been.** A page named only for a date or a
  time, a journal page say, is not a note: each thing written on it becomes a note of that day.
  A page that is nothing but a list becomes a note, with each item a note about it. Any other
  page is a document and stays whole, headed by its name. On the first two kinds a task is a
  note at any depth, archived if it was ticked and linked to the note it sat under; other
  nested lines are the body; a bullet holding only tags hands them down. A note carries the
  tags in force where it was written, on the line under its head, and the tags its own line
  started with go there too: a head is what was said, not how it was filed. A line of nothing
  but tags does not count as a body in the list. Folders and front-matter tags are tags, except a folder
  that only holds dates, which is a calendar and not a subject. A `[[page]]` is rewritten to
  the head its page has now, or to the date if the page was a day. Pages that are programs for
  Silverbullet are left out. Importing again brings a note up to date with its page unless its
  text was changed here since, and keeps what was done to it here: its links, whether it is
  archived, that it was deleted.
- **Dumb relay, self-hosted, after the browser build exists.**
- **Same repository.** The Go implementation is tagged and kept reachable; the Rust workspace
  takes over the root. History is part of the project.

## Open questions
- One CRDT document per note or one per space? Per note makes sharing a note trivial; per space is
  cheaper to load. Likely per note with a space-level index document.
- Brackets that name a note which does not exist yet stay plain text until the note is saved
  again. Should creating the target link the notes that were waiting for it?
- The carry rule ("the note contains what you typed" means it was a search) is a heuristic. Its
  bad case is clearing a one-word draft, which costs one word to retype; the case it must never
  produce is a note called `trai` under `trainingapp`. Daily use decides whether the rule holds.
- One hop or the whole path? With House › Boiler › a draft, `house` finds Boiler and not the draft.
  The 2022 habit was the whole path. One hop is what the row shows; use decides.
- Documents and one-liners share one ranking, and BM25 measures a document's length against a
  mean that the one-liners set, so a long document earns almost nothing for its own words. Its
  name now finds it; part of its name, or what it says, may not. Left until use shows it
  matters. `bm25_b` is the knob, and no setting of it was right on its own.
- How notes reach a browser. The session runs in a tab and needs its lines from somewhere, and
  the relay comes after the browser in the milestones. Something has to hand a tab the lines of
  a space and take back the ones it writes. Not settled.
- What the browser's keys are, and what stands in for Escape, Control and the arrows on a
  phone. Each is an ergonomics question of its own.
- Editing a note in the page. The terminal hands the text to your editor; a browser has none.
- A summon key. Starting should feel like switching a window, so the terminal build wants a
  keybinding that raises a running instance (a special workspace on Hyprland). Outside the app.
- Name. `fzn` stays unless something better appears.

## Milestones
0. **Data.** Done 2026-10-12. The 2022 corpus and the 2026 Silverbullet pages are
   in `~/fzn-corpus/`, outside this repo.
1. **The simplest live version, in the terminal.** Built 2026-10-12 as a Cargo workspace beside
   the Go code: `crates/fzn-engine` (model, index, three scorers, no I/O) and `crates/fzn` (the
   app: config, per-device log store, importers, terminal UI). The Silverbullet pages are the
   `main` space and the 2022 corpus is the `fzn-2022` space. Daily use from here; tuning follows
   use. Then the Go code is removed in one commit.
2. **Same core in the browser.** WASM build, DOM renderer, ego graph. First step done
   2026-10-13: the app moved out of the terminal into the engine as a session, and the engine
   builds for wasm32 as it stands. `crates/fzn-engine` is now the index, the scorers, the note
   as it is kept, and the session; `crates/fzn` is the terminal face, the log files, the system
   clock, config and the importers.
3. **Relay and sync.** Single binary, opaque lines, multiple devices. Until then, Syncthing on
   the notes directory.
4. **Text CRDT per note.** Behind the store module.
5. **Sharing and collaboration.**
