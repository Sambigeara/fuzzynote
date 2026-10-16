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
- Private by construction. The engine never calls out. Notes are sealed on the device with a key
  the relay never has, so sync moves lines through a relay that cannot read them.
- Explainable relevance. Every suggestion carries its reason. No black box in the loop by default.
- Converge, never conflict. Every change is an event; devices merge; nothing is lost.
- One core, thin faces. The engine is the whole app with no screen and no keyboard: a face says
  what was asked for and paints what it is told to show. Its keys, its gestures and its pixels
  are all a face owns, so no rule of the app can differ between a terminal and a browser.
- Keyboard first, almost no chrome.
- Ergonomics is a primary goal, and it has two halves: nothing visible ever waits, and there is
  almost nothing to remember. The whole app fits in one sentence: type to find, or into the row
  you are on to change it; Enter writes the line, or opens a new row under the one you are on;
  Ctrl-O opens the body; the arrow at the end of the head goes in, and at the end of the line
  goes into what the line names; Esc clears the line, or comes back; and where you are is what
  you are talking about. Every other key is maintenance, and the footer shows only the ones
  that apply right now. A key is justified by the flow it sits in, never inherited.
- One cursor. There is exactly one on the screen and it moves: the caret on the line, or the
  caret in the head of the row the cursor is on. Nothing else is highlighted and almost nothing
  else is drawn: no borders, no counts, no colour. The body read is that of what the cursor is
  on, apart from the list, never under a row or the line (see "A body is read apart from its
  row"). What is not being asked for is not on the screen.
- Archived is not forgotten. The list hides archived notes; the engine still remembers them.

## What stays from the original fzn (the soul)
- The search line is the composer: filter live, Enter turns the search into a new note carrying
  that text.
- Enter, then type into the new row. The original's rhythm for a list was Enter, type, Enter,
  type, with the search line as a standing context; the zoom is that context now, and Enter on
  an empty line opens a new row to write in. An empty row left empty was never anything.
- Every line is editable where it is. The original had one text cursor over the whole screen,
  and so does this: the head of the row the cursor is on is the field, typing changes it, and
  each change is kept as it is typed. Browsing is editing, and a stray key is one undo.
- Zoom on what is shared: select notes, press Enter, and whatever they have in common becomes
  the context: tags, people, rare words. The old common-prefix zoom is the special case. One
  label is reinstated (2026-10-14): a tag, a person or a day typed on the line is gone into with
  the arrow and is the context, and since 2026-10-15 labels gone into one after another are one
  context, and so are a note and the labels gone into after it. Selecting notes, and a context
  of plain words, are not.
- One line to search with: words find and everything else holds, so `"…"` is the exact text and
  a tag, a person, a day, the open tasks' `[ ]` or a span narrows what is listed, and `~` at the
  start of the line is the original's fuzzy, letters in order in heads (see "Words find;
  everything else holds"). The original's Tab-separated groups and `!` inverse are not built yet.
- Hashtags, @mentions, date expansion, URLs opened or copied with one key.
- Archive with one key, which ticks a task; show/hide archived globally.
- Undo/redo, move up/down, copy/paste of whole notes.
- Local-first, remote-second. Offline is the normal case.

## Model
- **Space**: a set of notes plus explicit links, kept apart from every other space. On disk a
  space is one directory of logs under the root. Plain `fzn` opens the space named in the config
  file (`main` to begin with) and `--space NAME` opens another. Sharing a space is later.
- **Note**: stable id; text (a text CRDT); links; created and updated times; archived flag;
  where it sits in the list, as the note it sits after, with a clock of its own (see "Where a
  note sits is kept"). Derived, never stored: head (first line), body, tags, mentions, dates,
  URLs, and whether it is a task.
- **Link**: from one note to another, held on the note as the target's id. There is one kind.
  It survives a rename and adds no words to the text. Links are made by position: a note
  written while looking around another is about it, and links to it. Nobody has to say "link".
  Brackets are the way of typing one from inside the editor: when a note is saved, each
  `[[head]]` that names an existing note becomes a link, and a link whose brackets were taken
  out is dropped. The one explicit gesture is a correction: a chord on a note in a note's zoom
  links it or unlinks it, for when the engine was wrong or the note was written in the wrong
  place. The engine does not read brackets when it searches. A link adds no words, and still a
  note is found by the name of what it is about: when you type, the head of a note that a note
  links to counts as its own head does, one hop and no further. Only explicit links are stored;
  everything else is recomputed on demand.
- **Syntax that the engine understands**: `#tag`, `@person`, dates, URLs, `[[note head]]` as a
  way of typing a link, and since 2026-10-16 a box, `[ ]` or `[x]`, at the start of a note,
  which makes it a task (see "A task is a note headed by its box"). Nothing else is special in a
  note. In particular the old ` >> ` prefix paths are plain text; the words in them still count,
  the hierarchy does not. On the line, and only there, three more forms are read: a phrase in
  quotes, a span of when notes were written, and `~` at its start (see "Words find; everything
  else holds").
- **Date**: plain text in the note, written `2026-10-16`, and never stored apart from it. The
  engine reads it on load, as it reads tags. A link is held as an id because a note's head can
  change; a date's name cannot, so it lives in the text. The day a note was written is not
  typed at all: it is the note's `created`. `/today`, `/tomorrow`, `/yesterday`, a day of the
  week (`/fri` is the next Friday and `/-mon` the last Monday, never today) and `/+3` or `/-2`
  are quicker ways of typing a date, and any beginning that only one of them has will do (`/tom`,
  `/f`, `/-tu`). Any other slash is left alone. The top line shows the date a word stands for and
  holds with it. Saving a note, from the top line or from the editor, writes the date in its
  place, at the moment a `[[head]]` becomes a link. A date typed on the line holds the notes
  that mention the day or were written on it, and counts as a date and not as the numbers in it;
  until 2026-10-15 it found them among whatever else the line found (see "Words find; everything
  else holds"). The old `Mon, Jan 2, 2006` form is still read.
- **Context inheritance**: where you are is what you are talking about. A note created while a
  context is active (tags, people, a zoomed neighbourhood) inherits that context. This is what
  prefix prepending used to do, without a tree and without the typing: the old ` >> ` prefix was
  this context written by hand on every line, and Ctrl-P pasted it. Built for the simplest case:
  a note created while looking around another links to it. And since 2026-10-14 for the next: a
  note created inside a tag, a person or a day ends its head with the label, as it would have
  been typed, and since 2026-10-15 a new row there opens with the label already in it. Also
  since 2026-10-15, labels gone into one after another are one context and are all inherited,
  so a note written in `#house › today` carries both, and a note on the path is one context with
  the labels gone into after it, so a note written in `Boiler › today` links to Boiler and is
  of today. Labels before the last note on the path are only how it was reached: in
  `#house › Boiler` a note links to Boiler and carries no tag. Since 2026-10-16 the open tasks
  are a context too, and a note created among them starts with the box.

## Engine
The engine is a library, and it is the app. It owns no UI, opens no file, reads no clock and
makes no network call, so the same code runs in a terminal, in a browser tab and in a test.

**Session.** What a face drives, one per person per space. Three things go in and two come
out.
- In: an *action*, which is what was asked for and never which key asked: the line now reads
  this, move the cursor, write the line, go into a note or a label, step back, archive, delete,
  link, undo, the text of a note as it was edited. An action that does not apply where the
  cursor is does nothing.
- In: versions of notes written elsewhere, by another device or another face on this one.
- In, from the host: the time, and an id for a new note.
- Out: *effects*, which are what the face has to do about an action: keep this version, put
  this note in front of the person to edit, copy this, open this URL. The session has already
  done its own part, so an effect that fails costs a message and nothing else.
- Out: the *view*, which is everything there is to paint: the path, the line and what its date
  words and spans stand for, the rows with their reasons, where the cursor is, the note under it as
  it is being written, the note you are in, what just happened, and what can be asked for where
  the cursor is, each with what to call it there. A face puts its own key or button beside each.

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
3. *Fuzzy*: trigram overlap, so a misspelling still finds its note. With `~` at the start of
   the line the words are letters instead, found in order in heads and scored as fzf scores
   them, and no other scorer runs (see "Words find; everything else holds").
4. *Temporal*: created or edited close in time; dates written in the text.
5. *Structural*: notes that sat adjacent in the list, notes you navigated between.
6. *Feedback*: a locked link boosts shared terms between the two notes; a dismissal dampens.
7. *Reserved*: a local embedding scorer, on device, if semantic similarity is ever wanted. It is
   another scorer, not a dependency.

**Combiner.** Weighted sum with per-scorer caps, top-k with reasons attached. What the line
holds comes before anything: a note that does not hold it is not listed at all, so the scores
only ever order what holds. Two facts come before any score: what is in the place being looked
around, linked to its note and carrying its labels, and the note headed with what was typed.
With nothing on the line to rank by no scorer runs, at the top or in a place: a place's facts
are a set, the note's links kept to the notes on the index's list for every label. Budget:
under five milliseconds per keystroke for ten thousand notes.

**Tuning happens in the product.** Every suggestion shows its reasons; writing where you are
makes a link, and a chord corrects one, stored on the note as the target's id; the weights live
in a config file; a toggle shows scores. There is no evaluation harness and no labelling step.
Nothing is added to the engine until daily use asks for it.

## Storage and sync
- **The note is plain text; what holds it need not be.** All of a note's meaning is in its
  characters, in forms people already know: `#tag`, `@person`, `2026-10-16`, a URL, a first
  line, a box `[ ]`. That is the agreed structure, and the test for adding to it is whether the
  note still reads right on paper. Beside the text go the facts it cannot carry: id, links,
  times, archived, deleted. What you said goes in the text; what happened to the note goes beside
  it. A file per note is not the store: files cannot merge two devices' edits, and most notes
  are one line.
- **v1: one append-only log per writer, per space.** Each line is a full snapshot of one note
  as JSON: `v`, the version of the line format, then id, text, links, created, updated, stamp,
  after, placed, archived, deleted, device. The format is version 3 since times went to the
  millisecond, and was version 2 from when `after` and `placed` were added. Times are RFC 3339
  to the millisecond with the offset of the clock that wrote them,
  `2026-10-13T10:12:56.250+01:00`, so a line reads without the app, and a note keeps the day it
  was written on wherever it is read. Lines from before versions hold unix seconds and are still
  read, and so are lines from before milliseconds, whose times fall on the second; a line with a
  later version than the build knows is left alone, not misread. `updated` is when the text last
  changed, so archiving a note does not re-date it. `stamp` is when what the version says was
  written, and a local change is always stamped past the version it replaces, a millisecond past
  it when the clock has not moved on since. `after` is the note it sits after and `placed` when
  it was put there, a clock of the place's own, kept and stepped the same way. Loading replays
  every log in the space and the version with the newest stamp gives the text, links, times and
  flags, ties broken by writer id, and the version newest by `placed` gives the place, ties
  broken by the note it is after. Last-writer-wins on what a note says is the known limitation,
  accepted for now. Deletion is a tombstone line, kept so an older version arriving later
  cannot resurrect the note. What a line says, and which of two versions is the newer, is the
  engine's to settle, so that every device agrees. Where the lines are kept is the face's:
  files under the terminal, IndexedDB in a tab.
- **Lines are sealed before they are kept.** Decided 2026-10-14. What goes into a log, and so
  to the relay, is not the snapshot but an envelope around it: `{"s":1,"c":"…"}`, where `c` is
  a random 24-byte nonce and then the snapshot encrypted with XChaCha20-Poly1305 under the
  space's key, base64. The key is 32 bytes derived from a passphrase with Argon2id (19 MiB,
  two passes, one lane) and a salt that is the hash of the space's name, so that the same
  passphrase on another device gives the same key and nothing has to be sent to get it.
  Each device derives the key once and keeps it, in the config directory under the terminal
  and in the browser's storage in a tab, so the passphrase is typed once a device. Lines from
  before sealing are still read as they are; the first open with a key compacts the writer's
  own log into a sealed one (see Sync), so plain lines leave the writer's log at once and the
  relay as soon as it has the sealed one. The key is verified by opening a line: a device that
  has seen a sealed line and opened none has the wrong passphrase, says so, and writes nothing,
  since a line sealed under a second key would split the space in two. A space with no sealed
  line yet takes the first key it is given. Forgetting a passphrase is losing the notes; there
  is no recovery and no reset, and sharing a space will be sharing its passphrase until there
  is something better. What the relay still sees: the names of the logs, how many lines each
  has and how long they are, when they arrive, and the compaction headers. What a relay can
  still do: withhold lines, or hand back an old prefix; it cannot forge, alter or reorder one.
- **fzn is the only thing that writes a space's directory.** Decided 2026-10-14. One fzn at a
  time has a space open on a machine, settled by a lock on a file in the directory, and a
  second window is told so and stops. Nothing is reread while the app runs: what arrives from
  the relay comes through the engine, which asks the face to keep it. Syncthing and the like
  are not supported and not needed; the relay is the way between machines.
- **Later: a text CRDT per note** (Yrs, the Rust port of Yjs, or Loro). The log lines then carry
  sealed CRDT updates instead of sealed snapshots. The replica and the relay do not change,
  since they never read a line; the session and `Notes` do, since the session reads and makes
  `Record`s directly in a couple of dozen places and undo holds whole versions. Count on a
  day in `session.rs` and `record.rs`, not a swap behind the store. Compaction already turns
  a history into one snapshot a note, which is what a CRDT log needs from time to time too.
- Sharing is sharing a subset of updates. Deferred, but nothing above prevents it.
- **The snapshot line is the way in and out, for good.** `fzn export` writes every live note
  as one plain line, by your own hand and to wherever you point it, so notes can always be
  read and taken away without the app; `fzn export QUERY` writes only what that line finds at
  the top of the space, archived notes too (see "Export takes the line"). `fzn import-jsonl`
  is the way back in; a plain file is no longer read as a log by being put in the directory,
  since it would then be sent in the clear. When the logs carry CRDT updates this format stays.
  It carries the facts beside the text as well as the text: the 2022 corpus is what it looks
  like when only the text gets out (creation time survives on 7% of its notes).
- A directory of markdown files for grep or Obsidian is a possible second export. It would be
  derived, never the source of truth.

## Sync
A space gets from one device to another by each side sending the other the rest of each log.
That is the whole of it. It is in the engine as `Replica`, with no I/O, so a device, a browser
tab and the relay all run the same code.

- **A space is a set of logs, and a log has one writer.** The writer only ever adds lines to
  the end, so any copy of a log is the start of the writer's own. A *replica* is one place's
  copy of a space. What it holds of a log is one number, and what a peer lacks is the rest.
  Which log a line sits in is bookkeeping: the notes are what all the lines add up to, the
  newest of each side of each, whatever order the lines come in and however many times. A log
  is named for its writer, and a writer's id is kept with the space it writes, in the space's
  directory under the terminal and in the browser's storage for a tab, so that the writer of
  one space is not the writer of another.
- **Compaction is a new log.** Decided 2026-10-14. A log only grows, so a writer that wants a
  shorter one starts another: it takes a new writer id, and the first line of the new log is a
  header, `{"compacts":["3f9a01c2"],"snapshot":293}`, naming every log it has had before and
  how many lines follow as the snapshot. The snapshot is the writer's latest version of every
  note it ever wrote, tombstones included, one sealed line each; after it the log goes on as
  any log does. A replica that holds the header and the whole snapshot *retires* the logs the
  header names: it drops them from what it holds and says, ignores a peer that offers them,
  and deletes them from wherever it keeps lines. Until it holds the whole snapshot it keeps
  both, and the notes come out the same, since a version is a version whichever log it is in.
  The writer compacts at open when its own log holds lines from before sealing, or has grown
  past four times the versions that are live in it. Putting a fork right is the same move: the
  writer takes a new log, and the one with two writers is retired by the header. Nothing is
  state: which logs are retired follows from the logs held, and the header names the whole
  chain so that it still follows after the middle of the chain is gone.
- **A mark is a point in a log**: how many lines in, and a digest of those lines. The digest
  is FNV-1a, 64 bits, over each line and its newline, written as sixteen hex digits. It is a
  check against accidents, not a defence against anyone.
- **Four messages**, each one JSON object. A message a build does not know is passed over.
  - `{"have":{"3f9a01c2":{"lines":293,"digest":"…"}}}` is every log the sender holds and its
    mark in each. Said on connecting.
  - `{"lines":{"log":"3f9a01c2","after":{"lines":293,"digest":"…"},"lines":["…"]}}` is lines
    of a log, and the mark they come straight after.
  - `{"forked":"3f9a01c2"}` says that the sender's log of this name and the receiver's are
    not one log.
  - `{"now":"2026-10-15T09:30:00.250Z"}` is the sender's clock. A relay says it first, before
    `have`, so that a device can say when its own clock is far off (see "A clock far from the
    relay's is said"). A replica makes nothing of it.
- **Four rules.**
  1. Told what a peer has, send it the rest of every log it holds less of, having checked its
     mark against the same point in your own.
  2. Add lines only where they continue exactly what you hold, which the mark they come after
     says. Lines you hold already are passed over. Lines from further on than you hold are
     answered with `have`, and the peer sends from there.
  3. Lines you add, pass on to every other peer that lacks them. This is all a relay does.
  4. Where a mark does not match, the two are different logs with one name: a fork. Nothing
     of that log passes between the two, both are told, neither is changed, and every other
     log goes on as before.
- **Nothing else is state.** No cursors, no acknowledgements, no sequence numbers in the
  lines. What a replica holds is all it knows, so a new device says it has nothing and is sent
  everything, and a relay that has lost its disk is filled again by the next device to
  connect.
- **Lines are never read on the way, but one.** They are copied byte for byte, and sealed, so
  a replica could not read them if it tried. A line from a later format passes through a
  replica that does not know it. The one line a replica reads is the first of a log, to see
  whether it is a compaction header, which is bookkeeping about logs and not about notes.
  This is what lets the text CRDT arrive without the relay changing.
- **A connection** is a websocket to `/1/main` on the relay: the version of this exchange,
  then the space. Each end pings a quiet peer and gives up on one silent for 45 seconds.
- **The relay** is a replica that writes nothing of its own, and it is `fzn relay`. It serves
  every space under its root and keeps each as the same files a device keeps, sealed, and
  holds the same lock on each, so nothing else writes them while it runs. It listens on
  `127.0.0.1:3960` until told otherwise. It has no password and no TLS yet: anyone who can
  connect can take a copy of the sealed lines and add lines of their own, which no device
  will be able to open. What keeps it private is the key, which it never has; what keeps it
  tidy is the address it listens on and whom the machine lets reach it. As a systemd service,
  `IPAddressAllow=` says whom.
- **A device syncs by having a relay named**, in the config file, in `$FZN_RELAY` or with
  `--relay`. There is no command for it. While the app is open a thread keeps the space's
  logs in step with the relay, and the status row says how that is going. With no relay
  named, nothing leaves the machine.
- **The engine is where sync and the app meet.** Decided 2026-10-14, replacing the files. A
  *device* is a session and a replica joined in memory, with the space's key, and it is in the
  engine as `Device`: the same code in the terminal, in a tab and in a test. A version the
  session wants kept is sealed, becomes a line of this writer's log, and the replica sends it
  on; a line the replica takes from the relay is opened and given to the session. The face is
  left with what only it can do: the socket, and keeping lines for the next time. In the
  terminal that is one thread holding the websocket and passing text both ways over channels,
  and a directory of files that only this process writes. Nothing is reread, nothing is
  locked per write, and nothing can go stale under the replica.
- **A fork is how a mistake shows.** The one that will happen is a writer's id copied to a
  second machine along with the rest of a directory, so that two machines write different
  logs under one name. Without the digest neither would send, each believing the other had its
  lines, and notes would stay on one machine with nothing to say so. It puts itself right:
  a device told that its own log is forked compacts, taking a new id, and its header retires
  the forked name. Both machines do this, each from its own copy, so nothing either wrote is
  lost, and the relay ends up with two logs where it had one shared name.
- Not covered yet: who may connect to a relay, TLS, and history beyond what compaction keeps.
  The header is taken on trust, and it is the one line a replica acts on: anyone who can
  connect could send a log whose header names other writers' logs, and every replica but
  each writer's own would retire them until that writer compacted again. Nothing is lost,
  since a writer's own log is never retired on a peer's word, but it is the first thing
  that authenticating a connection, or a header, has to close.

## Clients
- **The cursor, in both faces.** It is in one of two places, the line or the head of a row, and
  typing goes to the one it is in. On the line, typing finds; Enter with text writes the line as
  a note and takes the cursor to it, the caret at the end of its head; Enter with nothing on the
  line opens a new row at the top of the list, its head the field; Down moves onto the list;
  Right at the end of the line goes into what it names, and Esc clears it, or on an empty line
  comes back (see Zoom). On a row, the head is the field and typing changes it, with the caret
  landing at its end, and each change is kept at once; Up, Down and Esc leave the row, Esc
  keeping what was typed, as an editor's hands expect; Enter opens a new row directly under it,
  its head the field, as the original's Enter added a line under the current one; Ctrl-O opens
  the body, which in the terminal is your editor and in the browser a sheet over the page that
  holds the whole note, where the arrows move within it and Esc closes it back onto the head, or
  with vim's keys `:q`; Right at the end of the head goes in (see Zoom); undo puts the head
  back as it was when the cursor landed, and otherwise takes back the last change, which for a
  row is the whole visit, one key for both. A chord acts on the note as it reads. A
  head is a version at every change, a new row's from its first letter, which creates the note
  with the moment Enter was pressed as its `created`, linked to the note being looked around and
  to whatever it names in brackets; date words are written out when the cursor leaves the row,
  a new row in a tag, a person or a day has its labels from the start, and a body is kept when
  it closes. So Enter, type, Enter, type, Enter, type, Esc writes a list, from the line or from
  any row, each note on every device as it is typed. A new row left with nothing in its head
  goes when the cursor leaves it, the arrow landing past where it was, up on the row above
  or down on the row that was under it; one never typed into reaches neither the log nor the
  relay, and one typed into and cleared has its deletion written and leaves nothing to undo.
  Enter on a new row with nothing in its head does nothing: there is already an empty row to
  write in. A head cleared to be typed again is not written while it is empty. Backspace on a
  row with nothing in its head takes the row away and goes up to the row above, or onto the
  line, as it takes an empty line away in any editor; a new row goes, a note emptied is deleted
  and undo brings it back, and a note with a body stays, since the body is not in the head to
  see. A note emptied in place is deleted when the cursor leaves it, and undo brings it back; an
  empty file back from the editor does the same. A note stays where it was written, and an edit
  never moves it (see "Where a note sits is kept"). The list holds still while the cursor walks
  it, so that the arrow that leaves a head lands where it was aimed, and what arrived from
  elsewhere meanwhile is listed when the cursor comes back to the line.
- **Terminal**: Rust and ratatui, with the original fzn chords where they still earn their
  place, because that is where the muscle memory is. It is a face on the session and adds four
  things: its key table, the editing of the line and of the head of the row the cursor is on,
  the painting, and carrying out the effects, which here means a log file, your editor, the
  clipboard and the system's way of opening a URL. A chord acts on the line when the cursor is
  there and on the note under the cursor otherwise: Ctrl-D clears or deletes, Ctrl-V shows
  archived or archives, which ticks a task and is named `tick` there (see "A task is a note
  headed by its box"). Archived notes are out of every list, typed results included, until
  Ctrl-V on the line asks for them. Enter on a row opens a new row under it, Backspace on an
  empty head takes the row away, and Ctrl-O opens the note in your editor, which is how a body
  is written here; the field in the row is its head, and the body is read at the foot of the
  list. Right at the end of a head goes into the note, and at the end of the line into what the
  line names; short of either end it is the caret's. Ctrl-Up and Ctrl-Down move the note under
  the cursor a row, the cursor with it. Ctrl-U and Ctrl-R undo and redo any change, Ctrl-L links
  or unlinks the note under the cursor (see Zoom), Ctrl-C copies its first URL (or its head) to
  the clipboard, Ctrl-_ opens the URL, Ctrl-B shows scores. Escape comes back a rung at a time:
  off the row, writing out its date words, off the text on the line, out of the zoom, and twice
  on an empty line outside one quits; Ctrl-D on the line is a second key for clearing it.
  Ctrl-Q quits from anywhere, writing out the head first. The screen is flat: no borders, no
  boxes. The path is a dim row above the line and only in a zoom; the line is the first row,
  after a dim `>` in the column the rows leave empty, so that its text lines up with theirs;
  what its date words and spans stand for is shown after it, dimmed, `→ Fri 2026-10-16` or
  `→ Mon 2026-10-12 18:00 – now`. The rows follow the line directly, each a head and then,
  dimmed, a `¶` if it has a body, the names of what it links to after an arrow, and its reasons;
  the row the cursor is on has the terminal's cursor at the end of its head, the head in bold,
  and its date at the right. The foot of the list is kept for
  the body of what the cursor is on: the note on its row, and on the line in a zoom the note you
  are in. It is a row of nothing, then up to five rows of the body, dimmed, set in two columns
  from the rows' text and cut with a `…` when it goes on. The rows are kept whatever the note
  holds, on a row and on the line in a zoom alike, so walking the list never changes how much
  of it is shown; on the line outside a zoom the list has the whole height. On a short screen
  they shrink, keeping the list at least half its room, and go when fewer than two would be
  left. One dim footer row names the chords that apply where the cursor is, ending with
  Escape's next rung, `Esc clear`, `Esc back` or `Esc up`, or `Esc Esc quits` where there is
  none; a status row above it exists only while there is something to say, and where the body
  has its rows it takes the last of them, so that the list keeps its height. Nothing says how
  many notes there are or that sync is working; `not syncing` is said when it is not, and in its
  place, while syncing, how far this machine's clock is from the relay's when that is more than
  two minutes, `this device's clock is 5 min ahead`. Not yet reinstated: Ctrl-S multi-select
  with Enter zooming on what the selection shares, Tab search groups and `!` inverse, Ctrl-^
  export. The first time a space is opened on a machine fzn asks for its passphrase, twice if
  the space is new, and keeps the key it derives; `fzn key` asks again, for when it was
  mistyped. `$FZN_PASSPHRASE` answers for scripts.
- **Zoom.** Right at the end of a note's head makes it the context, and the path grows by one;
  short of the end, Right is the caret. A tag, a person, a day or the open tasks can be the
  context too (see "A tag is a place you can go into" and "A task is a note headed by its box"):
  Right at the end of the line goes into what the line names: the label if the line is one label
  and nothing else, `#house`, `@joe`, `/fri` or `[ ]`, or back to it if it is on the path; each
  label in turn if the line is several, `#house /today`, as if Right had been pressed after each
  but passing over a label the place has already (see "The arrow at the end of a line of labels
  goes into each"); and otherwise the one note in the space the line is the name of and that
  holds what the line holds, by the rule in "A name is a fact too". A span is not a place (see
  "A span is not a place"). In a zoom that rule holds as it does at the top: the note the line
  names is listed first, saying `its name`, wherever it is and whatever the limit, so Right goes
  where the list shows, and typing a name is the way from any place to any note. A line that
  names neither, or names where you already are, leaves Right doing nothing. The path shows a
  label as it is written: `#house`, `@joe`, `2026-10-16`, `[ ]`. The place is the last note on
  the path, if there is one,
  together with every label gone into after it (see "Labels gone into one after another are one
  place" and "A place is the last note on the path and the labels after it"); labels before the
  note are only how you got there. Its facts are the notes linked to or from the note that carry
  every one of those labels: the links alone when no label follows the note, and with no note on
  the path every note carrying all the labels, which for a day is every note written on it or
  mentioning it. A link is a fact and so is a label. The list works as the top of the space does
  (see "A place lists what is in it; typing finds"): with nothing on the line to rank by it is
  what is in the place and holds what the line holds, its facts in the order the space keeps
  them in, however many, and nothing the engine suggests; a row says how it is linked, `links
  here`, `linked from here` or `linked both ways`, and nothing scored. Typing words finds: the
  note the line names first, then the place's facts that match, then what the engine finds in
  its neighbourhood that matches, each best first with its reasons and each holding what the
  line holds, and a row of nothing between the last fact and the first find. Around a note the
  neighbourhood is what shares its tags and people, words and dates, or links to the same things,
  with only the notes carrying the labels counted as facts; around labels alone it is what their
  words find. The line comes with you, under one rule:
  if the note you went into contains what you typed, or holds what the line holds and its head
  and the heads of the notes it links to have every word of it outside its phrases (with `~`,
  its head has the letters), you were searching for it, so the line clears and stepping back
  brings it back; otherwise you were writing, and the draft is carried in to be written here. A
  place gone into from the line was searched for by definition, so the line clears. Stepping back
  never throws away what you typed: a line written inside stays on the way out, and the search
  comes back only to an empty line. Escape is no longer only the step back: with text on the line
  it clears the line, and only on an empty line does it step back, so that inside a place "found
  it, clear, stay" is one key. The faces bind Right at the end of the line to going in and
  Escape to that, and the session offers `clear` as the way back while the line has text and
  `back` once it is empty. Either way, what you write inside is about the place: inside a note
  it links to it and lands at the top of the place, and inside a label its head ends with
  the label, as it would have been typed, unless the note carries it already: in its text, or
  for a day by being written on it, since the day a note was written is its `created`. Among the
  open tasks the head starts with the box instead, since that is where a box is written. A note
  written from the line has it added as it is written; a new row opens with it already in its
  head. Inside labels gone into one after another the head ends with each of them it does not
  carry, in the order of the path: in `#house › @joe` a new row opens ` #house @joe`, in
  `#house › [ ]` it opens `[ ]  #house` with the caret after the box, and in
  `#house › today` a note reads `Order oil #house`. Only a note created there inherits, and only
  from the place: in `Boiler › today` a new note links to Boiler and is of today by being
  written on it, in `Boiler › #house` a new row opens ` #house` and links to Boiler, and in
  `#house › Boiler` a new note links to Boiler and carries no tag. This is context inheritance in
  its simplest form.
  Ctrl-L links the note under the cursor to the place's note or unlinks it, which takes it into
  the place or out of it when it carries the place's labels: on an empty line an unlinked note
  leaves the list, and a note to link is one that typing found. It is a correction, not the
  mechanism, and with no note on the path, at the top or inside labels alone, it has nothing to
  do and says so. Right at the end of a head again walks a hop further, and on a place already on
  the path goes back to it, so the path never repeats; stepping back goes one hop, onto the line.
  This is the ego graph as a flat list; following a link is picking its target here.
- **Browser**: the engine's `Device` compiled to wasm32 via wasm-bindgen, with a page of plain
  JavaScript and CSS to drive it: no framework, and one family of dependencies, CodeMirror and
  its parser Lezer, for the body's editor and the page's reading of markdown, built by hand into
  committed files (see "The body is a CodeMirror editor, loaded when a body first opens" and "A
  body's markdown is rendered where it is read and styled where it is written"). A tab is one
  more device. It has a writer id and a log of its own for each space, keeps every log of the
  space in the browser between visits, asks for the passphrase once and keeps the key, and
  syncs with the relay it was loaded from, so there is one address to open and nothing to set
  up. A relay hands the page out when it is pointed at the `web` directory with `--web`.
  - *The page.* One column, a little left of centre, with a third of the room beside it on its
    left, text left-aligned, and the line a little above a third of the way down, where it
    stays: it does not move when you type or when results arrive. A faint hairline under the
    line sets it apart from the rows. The blank page is the caret and, under it in faint grey,
    the heads at the top of the list fading out by the eighth. As you type, what is found takes
    their place; only a row that is new fades in, and nothing else moves. The path, in a zoom, is
    in the margin above the line. No colour: paper, ink and two greys, light and dark by the
    system's preference. On a page at least 75rem wide the body of what the cursor is on is read
    beside the list: the note under the cursor, or with the cursor on the line in a zoom the note
    you are in. It is rendered as markdown, in the soft grey, from the top of the first row's
    text, with no border, scrolled on its own and faded at its foot as the list is. A narrower
    page has no room for it: a note's body is read there by opening it, and the note you are in
    is read above the line, rendered, under the path and in the path's smaller size, clamped to
    six lines and opened with a tap, so that where you are is all above the line and what is
    found follows it directly. The margin grows downwards only for a body longer than it has
    room for. A row's head shows a markdown link as its text and a bare address as itself, each
    a link, and a press on one opens it in a tab of its own and moves nothing. (Until 2026-10-14
    the note you were in was read under the line; Sam found it read as part of the line, and it
    put a gap between the line and the first result. The terminal showed it under the line
    until 2026-10-15, where its line is the first row and a body above it would move the line.
    Until 2026-10-15 the note under the cursor was read under its row, and on a wide page the
    note you are in above the line: see "A body is read apart from its row".)
  - *Keys.* The four that are the app are the same: type, Enter, Escape and the arrows. The
    terminal's chords are not here, because a browser keeps most of them for itself, but two it
    lets go: Ctrl-D clears the line or deletes the note under the cursor, as in the terminal,
    and inside the body box it is the browser's, and vim's in vim mode; Ctrl-O opens the body of
    the note under the cursor, as in the terminal, and does nothing on the line; Ctrl with an
    arrow moves the note under the cursor a row, as in the terminal, and on a phone `move up`
    and `move down` in the foot do it. Backspace on an empty head takes the row away, as in the
    terminal. Right at the end of the line, with nothing selected, goes into what the line
    names, as Right at the end of a head goes into the note; Escape on the line clears it while
    it has text, and on an empty line in a zoom steps back. Every other thing that can be asked
    for is a word at the foot of the page, the footer of the terminal made pressable, and Tab
    reaches them from a row without leaving it. Only what applies where the cursor is gets one,
    and no key is written beside a word.
  - *The line* is an ordinary text field, so the phone's keyboard and the browser's own editing
    work in it. A cross after it clears it when it has text, and a `›` after the cross, in the
    column of the selected row's, goes into what it names when it names somewhere. Each keeps
    its place while it is not there, so that neither moves the other, or the dates, by coming or
    going; and since they are on the line, clearing and going in are not also words in the foot.
  - *Writing in a row.* The head of the row the cursor is on is a text box, and typing changes
    it; Enter opens a new row under it; Ctrl-O opens the whole note in the sheet, a bounded
    overlay over the column with the page dimmed slightly behind it, which scrolls on its own,
    and in it Enter is a line break and the arrows move within it, and Esc closes it back onto
    the head. Leaving it for anywhere else on the page closes it, and a press on another row goes
    there; a press in its own margin, or on its row behind it, does not, and nor does leaving the
    window. The words in the foot stay above the dimming, since they are about the note. There
    is no second editor: the sheet is the body's editor, for a long document too, and with it
    shut the head is written in its row. While the body is open the sheet holds a CodeMirror
    editor, loaded the first time one opens: the markdown styled where it stands with every mark
    in sight, soft wrap, step undo, a grey selection, and vim mode for the browser that asked
    for it with the word in the foot, where Esc is vim's and `:q`, `ZZ` or the word in the foot
    closes it, and `:w` writes and stays, scrolled where it was. Vim's prompt for a command is
    at the foot of the sheet, under the text and a hairline. The head stays an ordinary field.
    Until the editor has loaded, and if it never does, the sheet holds a plain text area. The
    `editor` the engine offers is not shown here.
  - *Touch.* A tap puts the cursor on a note, a second tap on it off its head opens its body
    (the head is a text box by then, and a tap in it places the caret), a tap on the `›` at the
    end of the selected row goes into the note and on the one after the line into what the line
    names, a tap on a link in a row opens it and leaves the cursor where it was, and the path is
    the way back.
  - *One tab at a time* is the device. A tab that opens says so and the older one stands
    down, since two of them writing one log would fork it.
  - Not yet: the page itself needs the relay to load, the graph view, and anything a browser
    only allows a page served over TLS, which a relay does not do.

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
  goes. Sam to confirm. Reversed 2026-10-15: see "Where a note sits is kept".
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
  is also what lets "type the name, Down, Right" be done without reading the list. Notes that
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
  but tags does not count as a body in the list. Folders and front-matter tags are tags, except
  a folder that only holds dates, which is a calendar and not a subject. A `[[page]]` is
  rewritten to the head its page has now, or to the date if the page was a day. Pages that are
  programs for Silverbullet are left out. Importing again brings a note up to date with its
  page unless its text was changed here since, and keeps what was done to it here: its links,
  whether it is archived, that it was deleted, and since places were kept, where it sits.
  Revised 2026-10-16: a task keeps its box, headed `[ ]`, or `[x]` and archived if it was
  ticked (see "A task is a note headed by its box").
- **A space is its logs, and sync is the rest of each.** Decided 2026-10-13. The other way
  is for the relay to keep one journal in the order things arrive and each device a cursor
  into it. That is less to build, but the cursor is state beside the data: lose the relay's
  journal, or put a device back from a backup, and the two no longer agree about what was
  sent. Here there is nothing beside the data to disagree about. It also makes every replica
  the same, so two devices can sync with no relay between them, and the relay has no logic of
  its own to get wrong. The cost is a rule that has to hold, one writer to a log, and the
  digest is there to notice when it does not. Merging is per note for now: two edits of one
  note made apart leave the newer one showing and the other in the log.
- **Compaction is a new log, not a rewritten one.** Decided 2026-10-14. Rewriting a log in
  place is a fork by the rule above, and any message saying "I rewrote it" is state a relay
  can miss. A new log under a new id, with a header naming the old ones, is read the same by
  a replica that was there and one that was not, and the retirement of the old logs follows
  from what is held. It also makes fork repair the same move, and gives the text CRDT the
  snapshot-and-go-on it will need. What it costs is the one line a replica reads.
- **The engine owns sync.** Decided 2026-10-14, at Sam's direction, replacing the files as
  the meeting point. The files bought Syncthing and a second window at the price of two
  readers of one directory, a lock on every write, and a "stale, start again" path. Neither
  is wanted: the relay is the way between machines, and a second window on one space is told
  to use the first. `Device` had already been written for the browser, where there are no
  files; it was the general thing all along, so it moved into the engine and the terminal
  drives it too.
- **Sealed on the device, from now.** Decided 2026-10-14, at Sam's direction. The lines were
  already opaque to the relay by design; sealing them costs a key and a passphrase prompt a
  device, and buys a relay that can sit anywhere. A passphrase, derived with Argon2id and a
  salt from the space's name, was chosen over a generated key copied between devices because
  a phone has a keyboard and not a clipboard from the terminal. There is no auth and no TLS
  yet, by the same decision: the key is what protects the notes, and the address is what
  protects the relay's disk and time.
- **Per-note last-writer-wins stays, for now.** Revisited 2026-10-14 when asked whether the
  CRDT could be made more robust. The stamp is already a per-note logical clock (a local
  change is always stamped past the version it replaces, whatever the wall clock says), and
  the merge is deterministic in any order. What is not robust is two devices editing one note
  apart, where the newer whole text wins; that is the text CRDT, which is milestone 4 and
  which compaction and sealing were shaped not to obstruct. Pulling Yrs or Loro in now would
  be the dependency decision Sam deferred, made in passing. Revised 2026-10-15 by "Stamps are
  kept to the millisecond": the logical clock stepped a second at a time, which ran ahead of the
  wall clock once a head was a version a keystroke, so it now steps a millisecond.
- **Sync is part of opening the app.** Decided 2026-10-13, at Sam's suggestion, over an
  `fzn sync` to be run beside it. A second thing to run is a second thing to remember, and
  one that fails without being seen. What it cost was two things to get right: only one of
  two open windows may sync, and a device syncs only while the app is open, which is when it
  is read and written anyway. The relay is one command for the same reason, and the same
  binary.
- **The page crosses to the engine as text.** Decided 2026-10-13. An action goes in as JSON
  and the view comes back as JSON, with a hint's action being exactly what to send back to
  ask for it, so the buttons at the foot of the page are the view's hints and nothing more.
  The alternative was a typed binding for every part of the view. Text keeps the boundary to
  seven calls, makes the page readable without the engine's types, and costs a parse that is
  far below a frame.
- **The relay hands out the page.** Decided 2026-10-13. A page loaded from the relay knows
  where its relay is, is covered by whatever lets a device reach the relay, and needs no
  second server. The page is a directory the relay is pointed at, not something built into
  the binary, so that changing how the page looks is a reload.
- **Dumb relay, self-hosted, before the browser.** Decided 2026-10-13, reversing the order.
  A tab needs its notes from somewhere, and a tab is one more device, so what syncs two
  machines is also what gives a browser its notes.
- **Same repository.** The Go implementation is at the tag `go-final` and kept reachable; the
  Rust workspace took over the root on 2026-10-14. History is part of the project.
- **Enter edits, the arrow goes in.** Decided 2026-10-14, at Sam's direction, replacing Enter
  as the zoom key. Enter on a row had gone into its neighbourhood, and editing a note meant the
  external editor. Now Enter opens the row to write in it, which is what Enter means in every
  spreadsheet, and the zoom is Right arrow: unmodified, universal in a terminal and a browser,
  and it walks the path the way the path is drawn. Sam had suggested a modifier with Z. Ctrl-Z
  is the browser's undo and forty years of suspend in a terminal; Alt-Z is reliable but carries
  no meaning and is the fallback if the arrow is refused; Shift-Enter and Ctrl-Enter are plain
  Enter in most terminals without the kitty keyboard protocol, so a key that works on one
  machine and not the next; Tab is how a page moves focus and what search groups want back.
  Enter now means four things by state, each visible, each named in the footer, and nothing
  about it depends on the key before it. Esc comes back a step, as before; Left does nothing
  on a row, so that there is one back key. Revised 2026-10-14, at Sam's direction, after a
  day's use: Enter opens the body, since the head needs no key to be edited (see the revision
  of the next decision), and Right goes in from the end of the head, where the caret lands,
  since Right short of the end is the caret. The reasoning on Ctrl-Z, Alt-Z, Shift-Enter and
  Tab stands, and Alt-Z is still the fallback. Revised again 2026-10-14, at Sam's direction,
  later the same day: Enter on a row opens a new row under it, which is what the original's
  Enter did and the quickest way to write several notes in a run, and the body is Ctrl-O in
  both faces, the original's chord for exactly this. Two things decided it. Enter as the body
  had proved invisible in the browser: the opened note looked like the row with its body shown
  under it, so a day's use found no way to open a note. And a draft's Enter had been left to
  use to settle (see Open questions, now closed) between the editor and another row; the
  original's hands wanted the row. The browser lets Ctrl-O go, as it lets Ctrl-D go, so the two
  faces keep one key for the body. The earlier argument that Ctrl-O was a second binding
  inherited rather than earned is answered: it is now the only binding for the body, and Enter
  has a better claim on the row. What stands: Right at the end of the head goes in, Esc comes
  back a step, Left does nothing on a row.
- **A note is written in place.** Decided 2026-10-14, at Sam's direction, which is what the
  original did and the revival had put off. Three calls shaped it. *Select, then edit*, not the
  original's one text cursor over the whole screen: typing on a row still goes to the line, so
  browsing is never editing and Down then type still narrows; the edit costs one Enter more.
  *A version is written when the row closes*, once, if the text changed: the original wrote a
  version a keystroke, which is a sealed line a character, and a timer would be the face's,
  writing at moments nobody sees. Undo is one entry a visit, and undo while the row is open puts
  the text back. *A new note is not written until it has text*: writing it at once would put an
  empty row on every device and a tombstone in every log for a thought that was not had. Esc
  keeps rather than cancels, because that is already the line's rule and because an editor's
  hands press Esc to finish. A note emptied is deleted, from the row and from the editor alike,
  replacing "left unchanged". The state of an open row lives in the session, so the rules are in
  one place, the hints can say "done" and "put back", and the flow is tested with the stopped
  clock. The terminal's in-place field is one line, the head, and "no inline multi-line editor
  in v1" stands: the body is Ctrl-O. The browser's row is a text box holding the whole note and
  is its only editor; the panel with Ctrl-Enter to save and Escape to cancel is gone, and with it
  the two text boxes with two Enters. Revised 2026-10-14, at Sam's direction: the head is a
  field whenever the cursor is on it, as in the original. Select-then-edit cost one Enter per
  fix and put a state between the cursor and the text; a day's use found the Enter in the way
  and the state a surprise when Down left a multi-line note instead of moving within it.
  Browsing is editing, and a stray key is one undo. What stands: a version when the cursor
  leaves the row or the body closes, once, if the text changed; a new note not written until it
  has text; Esc keeps; the session holds the field. What goes: the open and closed states of a
  row, Ctrl-O (Enter is the editor's key, and a second binding for one action is inherited,
  not earned), and the gutter mark, since the caret itself is in the head. Undo while on a row
  puts a changed head back and otherwise is undo, one key for the last change written or not.
  The list holds still while the cursor walks it, so that the arrow that writes a head lands
  where it was aimed. The contention this brings with two devices editing one note at once is
  known and not for now. Added 2026-10-14, with the second revision of the decision above: a
  new row opens under any row, not only at the top of the list, and an empty one goes from
  wherever it is when the cursor leaves it, the arrow landing past it as if the row had not
  been there. It takes nothing from the line, where the original prepended the search text to
  the new line: a zoom links what is written in it, which is the revival's way of saying what a
  note is about, and a search is not a zoom. Added 2026-10-14, at Sam's direction: Backspace on
  an empty row takes it away and steps up, to the row above or onto the line. It is what
  Backspace does to an empty line everywhere, and it is the quickest way back out of a run of
  new rows that went one too far. A note whose head is empty but which has a body stays, with a
  status saying why: the body is not shown in the head, and deleting it with the key being held
  down to clear a head would be a surprise. Ctrl-D is the key that deletes a note whatever it
  holds. Added 2026-10-14, at Sam's direction: Enter with text on the line takes the cursor to
  the note it wrote, the caret at the end of its head, as the original's did. The note just
  written is the one most likely to be gone on with, and Enter there opens a row under it, so a
  run of notes starts from the line as it does from any row; left on the line, the cursor had
  nothing to say where the note went. Revised again 2026-10-14: a head is now kept as it is
  typed (see that decision).
- **One cursor, no chrome.** Decided 2026-10-14, at Sam's direction, replacing the first page
  and the bordered terminal. The page had a header with a path, a prompt glyph, an underlined
  input, a titled list with a count, a date and a reason line on every row, a panel beside the
  list reading the first note unasked, and a foot of key badges, a count and a sync light; the
  terminal had three bordered boxes and two footer rows. Each was defensible; together they were
  a dashboard with the cursor one element among twelve. What stays is what the Vision says: a
  cursor, and what you typed, and what belongs with it. The page is one centred column with the
  line fixed a third of the way down, recent heads faint under it on the blank page, results
  that only fade in when new and otherwise hold still, the system's sans at three sizes, and
  paper, ink and two greys with no accent at all, since the accent had been doing five jobs that
  are now gone or are ink. The terminal is flat rows with the terminal's own cursor in the gutter
  of the selected row, its head bold, its body under it, and one dim row of chords, in the
  terminal's own colours. What was dropped and why: counts answer nothing the rows do not; dates
  on every row say what the order already says; a reason line under every row makes a table; a
  panel is a second copy of the text and a second place to look; a sync light is the only thing
  on the page that is not text, and silence while it works says more.
- **A head is kept as it is typed.** Decided 2026-10-14, at Sam's direction, replacing the call
  in "A note is written in place" that a version is written when the row closes, and with it
  that a new note is not written until the cursor leaves it. A new row looked unwritten until
  Enter or an arrow left it: no date, half the chords, nothing on any other device, and a key to
  press for what the page already showed. Now every change to a head is a version at once, a new
  row's from its first letter, as the original wrote a line a keystroke; there is nothing to
  press and nothing to lose. What it costs is the reason the call went the other way, a sealed
  line a character, and it is paid as the original paid it: a log grown past four times its
  notes is compacted when the device opens. Three things keep it from writing what nobody meant.
  Date words are written out when the cursor leaves the row, so `/f` on the way to `/fri` is not
  made a date mid-word. A head cleared to be typed again is not written while it is empty: the
  note reads as it last did until the cursor leaves, and is deleted then if the head is still
  empty, as before; a new row typed into and cleared goes as an untouched one does, with its
  deletion written and nothing to undo. And undo is still one entry a visit: the versions
  written while the cursor is on a row are gathered into one, so undo after leaving takes back
  the whole visit, and undo on the row puts the head back as it was when the cursor landed and
  leaves no entry. The body is not kept as it is typed: it is kept when it closes, or on `:w`,
  as the terminal's editor keeps a note when it exits, since a version is the whole note and a
  keystroke in a long body would write all of it. The status says nothing as a head is kept; the
  row's date appearing is the sign.
- **The body is a CodeMirror editor, loaded when a body first opens.** Decided 2026-10-14, at
  Sam's direction, revising the Browser decision that the page is plain JavaScript and CSS "with
  no framework and no build step but the engine's". The body had been a textarea, which is what
  the no-dependency rule bought: nothing to build, nothing to vendor, and the phone's own
  editing. What it could not give was an editor: no vim mode, no step undo worth the name across
  a long note, and a caret the page had to find through a mirror of the text. The body is now
  CodeMirror 6, the three packages that are the editor and the vim mode, about 409 KB minified
  in one file under `web/vendor/`, built by hand from a pinned lockfile in `web/editor/` and
  committed, so that the relay and CI still see static files and nothing is built on the
  way to the page. It is imported the first time a body opens and never before, and fetched
  under idle time once the cursor is on a row; the textarea remains the body until it arrives
  and if it does not. The note stays plain text: no document model, no markdown grammar
  (measured at 85 KB for syntax the notes do not use, and it would read `#house` as a heading);
  nothing is rendered, and marking the engine's own syntax in place is left until use asks. The
  head field and the line stay ordinary inputs. The page's caret stays the page's, drawn from
  the editor's coordinates, and its shape says the mode: a block in normal and visual mode, a
  bar in insert mode, as a terminal shows vim's. Vim mode is this browser's choice, off by
  default, a word in the foot while a body is open. Esc keeps its meaning: in normal mode the
  editor lets it through and the page closes the body, so from insert mode it is Esc, Esc; `:q`,
  `:wq`, `:x` and `ZZ` close the body, which is when a version is written; `:w` writes and
  stays, as it does in vim, by closing and reopening the body under the hands with the caret and
  history kept, and shown as the engine keeps it, so a date word is expanded then; `:q!` and
  `ZQ` put the text back first, if there is anything to. Ctrl-D inside the body is the browser's
  outside vim mode and vim's inside it. The relay's page policy allows inline styles for this,
  since the editor's own stylesheet is injected; scripts are still the page's own only. The
  other editors measured: Monaco at 4.6 MB and unsupported on phones; ProseMirror, TipTap,
  Milkdown, Lexical and Quill as document editors whose round trip through a schema changes
  plain text; Ace as a code canvas. What stands from the Browser decision: no framework, the
  page as a directory the relay is pointed at, the line and the head as ordinary text fields,
  and the four keys. Revised 2026-10-14, at Sam's direction: in vim mode Esc is vim's alone and
  never closes the body. Hands that press Esc twice to be sure of normal mode closed the note
  under themselves. The body closes with `:q`, `:wq`, `:x` or `ZZ`, with `:q!` or `ZQ` putting
  the text back first, with the word in the foot, or by leaving it for somewhere else on the
  page. Revised 2026-10-15 by "A body's markdown is rendered where it is read and styled where it
  is written": the note stays plain text, and the editor now reads it as markdown, with every
  mark in sight. The grammar is Lezer's own at 39 KB, not the 85 KB measured here, and it reads
  `#house` as a tag. The editor is 392 KB of its own and 470 KB with what it shares with the
  page's reading of markdown, which the page fetches as it starts.
- **A tag is a place you can go into.** Decided 2026-10-14, at Sam's direction. A tag, a person
  or a day typed on the line is gone into with the arrow at the end of the line, as a note is
  gone into from the end of its head, and the context of the zoom is then the label: every note
  carrying it comes first, newest on top, then what the engine finds for its words, and a note
  written there ends its head with it. It closes two open questions and reverses a line of the
  Zoom paragraph. The first question was whether it was a loss that a note added from a search
  for `#house` was plain and fell out of the list when the search ran again. It was: the
  original's search line was a standing context, and the prefix it put on every new line was
  what kept such a note in its list. Going into `#house` is that standing context with the
  typing done once. The second was that Right was dead at the end of the line. It now goes into
  what the line names, and does nothing when the line names nothing, so the key that walks the
  path steps onto it from the line too. A note is gone into by its name with the same key, when
  the line names exactly one note in the space by the rule in "A name is a fact too", which at
  the top is where Down and then Right would go: typing the name and pressing Right is one
  gesture whether the name is a tag or a head. Inside a place the rule holds as it does at the
  top: the note the line names need not be in the neighbourhood, and it is listed first, saying
  `its name`, whatever the neighbourhood and the limit, so the list and the arrow agree and
  typing a name is the way from any context to any note. Either way the line was a search for
  the place, so it clears on the way in and comes back to an empty line on the way out. What it
  reverses is "Escape never throws away what you typed", for the line: Escape with text on the
  line now clears it, and only on an empty line steps back. Three reasons. Inside a standing
  context the common case is "found it, clear, stay", not "take this draft out". Ctrl-D already
  threw the line away, so the line had no protection to lose. And a line costs a line to retype.
  It also takes away one of the two reasons "A note is written in place" gave for Esc keeping
  what was typed on a row, that keeping was already the line's rule; the other, that an editor's
  hands press Esc to finish, stands, and on a row Esc still keeps. The step back itself still
  keeps what is on the line, and the browser's path, tapped once a step, still asks for it.
  Ctrl-D on the line is now a second binding for what Escape does while the line has text, kept
  for the hands that have it; dropping it is Sam's to call. The label lands at the end of the
  head, as it would have been typed, and not on the line under the head where the importer files
  tags, because the note then reads exactly as one written with the tag on the line has always
  read. A note written in place gets it when the cursor leaves its row, as its date words are
  written out, so that while it is being typed the head reads as typed; and a note that carries
  the label already is left as it is, whether its text has it anywhere or, for a day, the note
  was written on that day, since the day a note was written is not typed but is its `created`.
  So in a zoom on today nothing is added, and in a zoom on `/fri` a note written today ends with
  `2026-10-16`. Revised 2026-10-15: a new row opens with the label in its head, rather than
  getting it on leaving (see "A new row in a label opens with the label"). Only a note created
  in the place inherits, and only from the innermost place: in `#house › Boiler` a new note
  links to Boiler and gets no tag, exactly as a note in a note's zoom always has, since the path
  says how you got there and its last step says where you are.
  One label at a time: several labels as one context, and a plain-word search as a context, are
  left out, the second because a plain-word search could only hand down the old prefix, the same
  words written on every line. Revised 2026-10-15: labels gone into one after another are one
  context (see "Labels gone into one after another are one place"); a note on the path still
  starts afresh, and a plain-word search is still not a context. Nothing is linked in a label,
  so Ctrl-L there does nothing but say so, and the first part of the list is the notes that
  carry it, whatever the weights make of them. Remembering the path across restarts is left out
  too: the summon key in the open questions is the answer to coming back to where you were.
  Revised 2026-10-15: on an empty line the list is only the notes that carry the label, and
  what its words find comes when typing (see "A place lists what is in it; typing finds"); and
  a note on the path no longer starts afresh, but is one place with the labels gone into after
  it (see "A place is the last note on the path and the labels after it").
- **A new row in a label opens with the label.** Decided 2026-10-15, agreed with Sam, replacing
  the call in "A tag is a place you can go into" that a note written in place gets the label
  when the cursor leaves its row. A head is kept as it is typed, so the note was on every other
  device from its first letter, but without the label until Enter, an arrow or Escape left the
  row: until then every other device held `Bleed the radiators` with no `#house`, and its own
  `#house` did not list it. A label added on leaving reached other devices late, and a note's
  link was never late, since it is in the note's first version. Now a new row opened inside a
  tag, a person or a day reads ` #house` from the moment it opens, a space and the label as it
  was typed, with the caret at its start. Nothing is written until the first letter, and that
  letter writes `B #house`, so the note is in its place on every device at once and what the
  writer sees is exactly what is written. The label is text like any other: the writer can see
  where the note will go, and can delete it, and then it stays out. A row with nothing in it but
  the label it opened with is empty, as a row with nothing in it was: opening it writes nothing,
  Enter and the chords do nothing there, leaving it takes it away, putting it back returns it to
  the label, and Backspace at its start takes it away. The caret lands at the start and not the
  end, and goes back there when the row is put back, so that what is typed goes before the
  label. A day is put in only when it is not the day the note is written on, since that day is
  its `created`: a zoom on today opens an empty row, one on `/fri` opens ` 2026-10-16`. A note
  written from the line inside a label still has the label added as it is written, since the
  line was the search and that is its one version. Taken out a letter at a time from its end,
  as Backspace does and as a phone's keyboard only can, what is left of the label is no more the
  writer's than the label was, so nothing is written until a letter that is. What it costs: a
  new row holding only the start of its label, `#ho` in `#house`, is empty too and goes when it
  is left, and the label taken out from its start writes each step, as typing does. Revised
  2026-10-16: among the open tasks the row opens `[ ]  #house` with the caret after the box, where
  everything said here of the start holds instead, and how the labels are spaced does not count
  (see "A task is a note headed by its box").
- **Labels gone into one after another are one place.** Decided 2026-10-15, agreed with Sam,
  replacing "One label at a time" and "only from the innermost place" in "A tag is a place you
  can go into", for labels. Going into today from inside `#house` reads as today's notes in that
  tag, and it was not: the list was today's notes, and a note written there carried today by
  being written on it and got no `#house`, so it fell out of the tag without a word. Now the
  labels at the end of the path, back to the nearest note on it or its start, are taken
  together. The facts are the notes that carry every one of them, as one label's are the notes
  that carry it, and what the engine finds besides, and the narrowing by the line, are for them
  all. A note written there inherits each label it does not carry already, in the order of the
  path, at the end of its head as one label always was: in `#house › today` a note from the line
  reads `Order oil #house` and a new row opens ` #house`, in `#house › @joe` one opens
  ` #house @joe`, and in `#house › /fri` one opens ` #house 2026-10-16`. A row holding only what
  it opened with, or a start of it, is empty as before. A note on the path starts afresh, as it
  always did, and is never taken together with anything: in `#house › Boiler` a new note links to
  Boiler and carries no tag, and in `#house › Boiler › today` only today counts. The path,
  going in, going back a step at a time, and going back to a place already on it are as they
  were; nothing about the run is kept but the path itself. Revised 2026-10-15: the run is taken
  together with the last note on the path before it, and only the labels before that note start
  nothing (see "A place is the last note on the path and the labels after it").
- **Where a note sits is kept.** Decided 2026-10-15, at Sam's direction, reversing "No manual
  ordering (provisional)". The empty line's list was in the order of `updated`, and so were a
  place's facts, by the Zoom paragraph and "A tag is a place you can go into", and that had two
  faults. A note written under a row stayed there only on the device that wrote it, and only
  until the cursor came back to the line: everywhere else, and then here too, it went to the
  top, so Enter, type, Enter, type wrote a run that did not stay where it was written. And an
  old note edited went to the top, so that putting a word right reordered the list. The 2022
  corpus says the same from the other side: its order was kept by hand, because when a note was
  written was not what its place meant. So where a note sits is stored and
  replicated, as it was in the original fzn, whose tree CRDT comes back as one field with a
  clock of its own. A version carries `after`, the id of the note it sits after or none for the
  top, and `placed`, when it was put there. Two versions merge side by side, as the original
  kept content and position in separate registers: the text, links, times and flags come from
  the version newest by `stamp`, a tie broken by writer id, and the place from the version
  newest by `placed`, a tie broken by the note it is after. An edit leaves `placed` as it was
  and a move leaves `stamp`, and each carries the side it leaves as it was written, writer and
  all, so an edit on one device and a move on another both hold, and neither decides anything
  about the side it carries. The order is the walk of the tree that `after` makes: at the top
  every note placed at the top, newest placed first, and each note followed by the notes placed
  after it, newest placed first, and theirs. A note is placed at a moment past every note
  already placed after the same one, so a note placed after another sits directly under it
  whatever the clocks that placed the rest; two devices writing under one note at once both sit
  under it, the later placed nearer, each run whole. A note from before places sits by when it
  was written. A note placed after one that no version is held for sits at the top until that
  one arrives, and then moves under it. Deleted notes, and archived ones while they are hidden,
  are walked through and not listed, so that what was placed after them keeps its place. Two
  devices moving at once can make a cycle, which nothing at the top reaches; it is listed as if
  it were at the top, with what hangs from it, so that no note is lost. Enter on a row places
  the new note after that row in its first version, so it is in its place on every device at
  once; Enter on the line, and a note written from the line, place it at the top. The empty
  line's list is the top of the order and an edit never moves a note, Sam's choice over a list
  of what was recently written beside it. A move is the original's, one note alone: down places
  it after the row below, and up after the note the row above follows in the space's order, or
  at the top, so that either way it lands beside the row it passes and goes past nothing else,
  in the list or among a place's facts, where the notes between two facts are not shown. What
  was placed after it is placed after the note it followed instead, oldest first so that its
  order holds, so a run written under a note stays where it was and no cycle is made. What the
  line finds is in the order of its scores, so there nothing is moved and nothing is offered.
  The terminal moves a note with Ctrl-Up and Ctrl-Down; the page with Ctrl and an arrow, and
  with `move up` and `move down` in the foot. A move is one entry to undo, though it writes a
  version of every note it places, and undo puts the note back after the note it followed;
  moving the top up, or the last down, writes nothing. Undo takes back only the side a change
  made, so that undoing a move leaves what has been written since, here or on another device,
  and undoing an edit leaves where the note has been moved since; putting a head back is the
  same. The line format went to version 2, so that a build that knows only version 1 leaves
  these lines alone rather than rewriting a note without its place. `placed` was kept to the
  second here, and a note placed in the same second as another a second past it; since
  2026-10-15 it is kept to the millisecond and stepped a millisecond, and the format is
  version 3 (see "Stamps are kept to the millisecond"). The pinned flag the model
  once listed was never built, and moving a note to the top is what it was for. The order is
  walked afresh each time the list is made, O(n log n) in the notes: 5 to 7 ms over 30,000 notes
  in a release build, against under 1 ms for the sort by `updated` it replaces, so nothing is
  cached.
- **Stamps are kept to the millisecond.** Decided 2026-10-15, at Sam's direction, revising
  "Per-note last-writer-wins stays, for now" in what its logical clock steps by, the times in
  the line format under Storage and sync, and `placed` in "Where a note sits is kept"; the line
  format goes from version 2 to 3. A version's `stamp`, which decides whose text wins, and its
  `placed`, which decides whose place wins, are a moment past what is held for the note, or the
  clock if that is later. Moments counted whole seconds, so the step was a second, and since "A
  head is kept as it is typed" a head is a version a keystroke: k keys a second for T seconds
  put the note's stamp about (k−1)·T seconds ahead of the clock, half a minute for a head of
  forty characters typed at speed. Inside that window another device's edit of the same note,
  made later without having seen this one, lost. Now a moment is unix milliseconds with its
  offset, every comparison and rank is in milliseconds, the step is one, and both faces read the
  clock to the millisecond, the terminal from the system and the page from `Date.now()`. A burst
  now runs ahead by a millisecond a key at most. Times are written with three digits of fraction;
  a line from before reads as it did, its times on the second, and two such versions in one
  second are still decided by writer id. Days, and every time shown, are unchanged. The format
  went to 3 because a build of version 2 reads the fraction and drops it, so two versions of one
  note in one second, told apart here by their milliseconds, would be a tie there decided by
  writer id, and two devices would hold different text for the same note; by the rule under
  Storage and sync a build that cannot read a line as it was meant leaves it alone instead. So
  every binary moves together, as it did for version 2. The relay passes lines on unread and does
  not care. The textbook form of this is a hybrid logical clock, wall time with a counter beside
  it that breaks ties within a tick and carries causality past a clock that is behind; this
  stamp already steps past what it has seen, which is the counter's work, and milliseconds take
  most of the rest of the gain (a step small enough that typing no longer outruns the clock) for
  the least change, one field of one type and no new field in the line. A clock that is simply
  wrong, by minutes or hours, still wins or loses by that much; that is said, not corrected (see
  "A clock far from the relay's is said").
- **A clock far from the relay's is said.** Decided 2026-10-15, at Sam's direction. A device
  whose clock is ahead stamps its versions ahead, and wins against later edits made elsewhere;
  one behind loses to earlier ones. It cannot be put right quietly: which of two versions is
  newer is decided on every device from the stamps alone, so a device that adjusted its own
  stamps, or read others' through an estimate of their clock, would decide differently from one
  that did not, and the space would hold two answers. What can be done is to make it seen, so the
  clock gets set. The relay's first message on connecting is now its clock, `{"now":"…"}`, and a
  device compares that with its own `Host` clock in `Device`, which both faces drive, and says
  `this device's clock is 5 min ahead`, or behind, in the place of `not syncing` at the foot,
  when the two are more than two minutes apart, and nothing otherwise. Two minutes is well past
  any network's delay and well short of what loses an edit that matters. It is forgotten when the
  connection goes and learnt again when it comes back. The clock was meant to be one field of the
  relay's first message, which says what it holds, but a build from before reads that message
  as exactly one key, and one with a field it does not know would be passed over whole, and
  what the relay holds with it; a message of its own is passed over by such a build as any
  message it does not know is, and a relay from before sends none, so nothing is said. Nothing
  stored is touched.
- **A line of a later format is left as it is.** Decided 2026-10-15, in review of the move to
  version 3, revising "Compaction is a new log" in what a snapshot holds and "Lines are sealed
  before they are kept" in what proves a key. Storage and sync says a line with a later version
  than the build knows is left alone, not misread, but a line the key opened and the build
  could not read was taken as garbage. So a build of version 2, given a space of nothing but
  version 3 lines, said its passphrase did not open it, with the right one. And a build of
  version 2 whose own log held more than a thousand lines that version 3 had written on the
  same machine found the log grown past four times the notes it could read, and compacted it,
  leaving those lines out of the snapshot; the header then retired them on every device, and
  what that machine had written since was gone. Now an envelope the key opens proves the key,
  whatever is inside; a line of a later format counts as a note when a log is judged grown; and
  a compaction carries every such line into the new log as it was, after the snapshot, counted
  in the header. Nothing about a line this build reads changes. It holds from version 3 on:
  builds of version 2 behave as they did, so none is to open a space that a build of version 3
  has written to on the same machine.
- **A place lists what is in it; typing finds.** Decided 2026-10-15, agreed with Sam, replacing
  the neighbourhood in two parts that the Zoom paragraph and "A tag is a place you can go into"
  listed on an empty line, the facts and then the engine's suggestions with their reasons. A
  zoom on a day, or on `#house › today`, showed lots of other notes: the suggestions were each
  label's carriers less the notes carrying them all, so the place never read as the set it is.
  Now every place works as the top of the space already did, where an empty line is every note
  in your order with no reasons, and typing finds, ranked and with reasons. With nothing on the
  line a place, whether a note, labels or both, lists only its facts, in the order the space
  keeps them in, and nothing the engine suggests. A fact carries no scorer's reason, as a row at
  the top carries none; how it is linked, `links here`, `linked from here` or `linked both
  ways`, is a fact and not a score, and stays. No scorer runs for it: the facts are a set, the
  note's links kept to what the index lists for each label, and the list is the walk of the
  space's order that the top already makes, kept to that set. With something on the line the
  list is the note the line names, as before wherever it is, then the place's facts that match,
  then what the engine finds in its neighbourhood that matches, each with its reasons; facts
  before finds, where before they were ranked together. The row of nothing that both faces drew
  between the facts and the suggestions on an empty line is drawn where facts and finds now meet,
  with something on the line. Three reasons. The Vision ties finding to typing and going in to
  belonging: "as you type, the engine finds", and "go into one of those and write there, and
  what you write belongs to it". The top level already listed a set on an empty line and found
  when typed, so a place that did otherwise was a second rule. And a label is typed on purpose,
  so a place is exactly what a note written there joins, and its list should be that and no
  more. What it costs: going into a note no longer shows the notes related to it until
  something is typed, and a note that says a tag's word without carrying the tag, `house move
  plan` in `#house`, shows only when typing. Since every row on an empty line is now a fact, all
  of them can be moved, and Ctrl-L there can only unlink, which takes the note out of the list;
  a note to link is one the line found.
- **A place is the last note on the path and the labels after it.** Decided 2026-10-15, agreed
  with Sam, replacing "a note on the path starts afresh … in `#house › Boiler › today` only today
  counts" in "Labels gone into one after another are one place". A note on the path was never
  taken together with anything, so in `Boiler › today` the list was all of today's notes and a
  note written there did not link to Boiler: it fell out of Boiler, the fault that "Labels gone
  into one after another are one place" put right for labels earlier the same day. Now the place
  is the last note on the path, if there is one, together with every label gone into after it. Its
  facts are the notes linked to or from that note which carry every label in the run: the links
  alone with no label after the note, and the carriers alone with no note on the path. A note
  written there links to the note, as in a note's zoom, and inherits each label it does not carry
  yet, as a run of labels does, and a new row opens with them: in `Boiler › #house` it opens
  ` #house` and links to Boiler. Ctrl-L there links or unlinks to the note, as in a note's zoom;
  with labels alone it says what it said. Typing finds in the note's neighbourhood, with only the
  notes carrying the labels counted as facts, so a note linked to Boiler but written another day
  is a find in `Boiler › today`. The note's body is shown where a note's zoom shows it. Its name
  typed there lists it first, as any note the line names, and Right goes back to it, as to any
  place on the path; Ctrl-L on it has nothing to do, since a note does not link to itself. The
  labels before the note are only how it was reached: `#house › Boiler › today` is the place
  `Boiler › today` is, and in `#house › Boiler` a new note links to Boiler and carries no tag, as
  before. Going into a note from any of these starts a new place at that note. The reasons: the
  same place reached by two routes should give the same note. What is before the note is already
  recorded, by the links that put the note where it is, so copying `#house` onto a note written in
  Boiler would be a second copy of it, and one that goes stale when a note moves. And how far up a
  path finding reaches is a question for search, which today goes one hop down from a name (see "A
  name finds what was written in it" and the open question "One hop or the whole path?"), and is
  not written onto notes. The engine is asked about every place as one thing, a note, labels or
  both, where it had one call for a note and another for labels.
- **A body is read apart from its row.** Decided 2026-10-15, at Sam's direction, replacing the
  body shown under the row the cursor is on in both faces: in the terminal "its body under it,
  clamped to six rows" (the Terminal paragraph and "One cursor, no chrome"), and in the browser
  "the note under the cursor is read under its row, clamped to six lines". It also replaces "no
  panels" in the Principles' "One cursor", and on the page "There is no panel beside the list".
  A row that expanded the note under it as the arrows went up and down the list was hard to follow: every row below moved down and back at each step, a note with a body
  looked like several rows, and the eye had to find the list again after each key. Now the list
  is one row a note wherever the cursor is, and the body is read in a place of its own. In the
  terminal that is the foot of the list, above the status and the keys: a row of nothing, then
  up to five rows of the body in the dim grey, set in two columns from the rows' text, wrapped
  to the width and cut with a `…`, so that it does not read as rows. The rows are kept whenever
  the cursor is on a row, for a note with no body too, so walking the list never changes how
  many rows it shows; on the line the list has the whole height. They take at most half of the
  list's room, which a short screen shrinks, and go when fewer than two would be left. The body
  is of the note as it is being written. The note you are in stays under the path, as it was.
  On a page 75rem wide or more the body is beside the list, read-only, in the soft grey, from
  the top of the first row's text, with no border and nothing round it, and it scrolls by itself
  and fades at its foot as the list does; a narrower page, a phone, has no room for it and a
  body is read there by opening it. To leave the room, the column sits left of centre, a third
  of the space beside it on its left, which Sam wanted anyway. The panel dropped on 2026-10-14
  was a second copy of the text and a second place to look, read unasked even with the cursor
  on the line; this one says nothing until the cursor is on a note and is the one place a body
  is read, so the reasoning stands for what it was about. The view's `preview`, the note that
  panel read, had been read by no face since, and went. Revised the same day, at Sam's
  direction: the body read is always that of what the cursor is on, which on the line in a zoom
  is the note you are in. It replaces, above, "The note you are in stays under the path, as it
  was", and in the Terminal paragraph "in a zoom the body of the note you are in sits under the
  line, dimmed and clamped". In the terminal that body still opened between the line and the
  list, about seven dim rows ending in a `…` with the list below them, which is the fault this
  decision was for: a body expanded under a line, pushing the list down. Now nothing is drawn
  between the line and the list; on the line in a zoom the foot reads the note you are in, and
  its rows are kept there as on a row, so the list keeps its height between the line and the
  rows; outside a zoom the line still has the whole height. On a page wide enough for the body
  beside the list it is read there from the line too, and not above the line as well; a
  narrower page, with nowhere beside the list, still reads it above the line, under the path.
  And the status row, which had pushed the body and the list up a row whenever there was
  something to say, takes the last of the body's rows instead, so that a key that says
  something and the key after it, which clears it, do not change how much of the list is shown.
- **The line is told apart from the rows.** Decided 2026-10-15, at Sam's direction. With the
  cursor on the line, nothing said that the first row of text was where typing goes and the
  rows under it were not, and on the page the path above it was the only other text in the same
  column. In the terminal the line starts with a dim `>` in the column the rows leave empty, so
  the line reads as a prompt and its text lines up with theirs; nothing else moves. Tried: `·`,
  which already separates the app from the space on the path and a row's reasons; `▸`, too near
  the path's `›`, and of ambiguous width, two columns in a terminal set for East Asian text;
  and `:`, which is vim's command line. `>` is one column everywhere and says prompt. On the
  page a hairline runs under the line, in a grey fainter than the faint one, and under the
  passphrase field, which stands where the line will be; the path above stays small and faint.
  No colour.
- **An open body is a sheet over the page.** Decided 2026-10-15, at Sam's direction, replacing
  "Ctrl-O grows the box to the whole note and takes the other rows off the page, so that the note
  is the page" in the Browser paragraph, and "the row is the editor" with it. Ctrl-O, or the
  word in the foot, opens the note in the sheet: a dedicated, bounded editor over the column,
  about the size the body had in the page, scrolled on its own, with the page dimmed slightly
  behind it. The 2026-10-14 reason for taking the rows away was that rows only gone faint looked
  the same as a row with its body shown, and nothing said the note had opened; a sheet says it
  and leaves the list where it was underneath. What the keys and the engine do is unchanged:
  Escape, or vim's `:q`, `:wq`, `:x`, `ZZ` and `ZQ`, closes it, `:w` writes and stays, a version
  is written when it closes, leaving it for elsewhere on the page closes it, and leaving the
  window does not. The sheet stays scrolled where it was through `:w`, which closes and opens it
  again, where the row had gone back to the top of the note. A press in its margin, or on its
  own row behind it, keeps it open, so that the press does not close it for the tap to open it
  again. The words in the foot stay above the dimming, since they are about the note. The sheet
  is placed from the top and given a height, and is not pinned to the foot of the screen,
  which iOS Safari misplaces around its keyboard (WebKit bug 297779); with the keyboard up a
  browser that honours `interactive-widget=resizes-content` shortens it. It is not a
  `<dialog>`: a modal one makes the foot inert, and its Escape cancels, which vim's Escape would
  have had to fight; it is an element with the dialog role. With the editor bundle out of reach
  it holds the plain text area. Vim's prompt for a command now sits at the sheet's foot, under
  the text and a hairline. It had floated over the text: CodeMirror sticks its panels to the
  foot of whatever scrolls the editor, which was the page, so with a long body open Escape then
  `:` put the prompt over a line of the note. The editor is given the sheet's foot for its
  panels instead, which changed the editor's source by one extension and the bundle by 64
  bytes.
- **A body's markdown is rendered where it is read and styled where it is written.** Decided
  2026-10-15, agreed with Sam, revising "The body is a CodeMirror editor, loaded when a body
  first opens" where it says "The note stays plain text: no document model, no markdown grammar
  (measured at 85 KB for syntax the notes do not use, and it would read `#house` as a heading);
  nothing is rendered". Sam asked for markdown rendered, at least in an open body, a link in a row shown as a link, and code blocks highlighted. Each reason for the old call has gone.
  CommonMark wants a space after `#`, so to the grammar `#house` is a tag too, at the start of a
  line as anywhere. Since the SilverBullet import the documents in `main` are markdown, so the
  syntax is used. And Lezer's markdown parser with GitHub's tables, tasks, strikethrough and bare
  addresses costs 39 KB taken directly (13 KB gzipped), not 85; `@codemirror/lang-markdown`,
  which brings the HTML, CSS and JavaScript parsers for HTML in a note, would have cost 233 KB.
  The note stays plain text, and neither the engine nor the terminal changes: this is how the
  page reads.

  What is being written shows its marks; what is being read is rendered. One parse and one
  highlighting serve three places. *The sheet* is styled source, as iA Writer writes, which Sam
  chose over a live preview: every character stays where it was typed and in sight, the marks
  (`#`, `**`, `_`, backticks, brackets and the address after them, list and quote marks, fences)
  in the faint grey and the text in ink, headings and strong text heavier, emphasis slanted, and
  code in the monospace, highlighted by its language. Nothing is hidden and nothing is drawn in
  the place of text, so nothing moves as the caret walks, vim's motions go over exactly what is
  shown, and the caret the page draws from the editor's coordinates needs nothing new: headings
  are set apart by weight and not size, so the page keeps its three sizes and every line is as
  high as the next, and a block of code, a tenth smaller in the monospace to match the sans by
  eye, keeps that height too. The plain text area the sheet holds until the editor arrives stays
  plain. *Where a body is only read*, beside the list and above the line, it is rendered:
  headings, paragraphs, lists, tasks as a drawn box, ticked and faint when done, never pressed,
  quotes, rules, tables with their columns aligned, code highlighted by its language, and in a
  line emphasis, strong text, code, strikethrough and links. A paragraph keeps the lines it was
  written on, where markdown would run them together, since most bodies are lines written one
  under another. It is built as nodes, never as markup. *A row's head* reads as one line of
  markdown, only as far as a list bears: a link shows its text, a bare `http(s)` address is a
  link, a leading heading mark is dropped, and every other mark stays as typed. The row under
  the cursor is its text box and shows the source, as it always did. A press on a link opens it
  and moves nothing; a press anywhere else on the row is what it was.

  A link goes only to `http:`, `https:` or `mailto:`, in a tab of its own and with no referrer.
  Any other address, a link by reference to an address given elsewhere in the note, raw HTML, an
  entity such as `&amp;`, and `[[Boiler]]`, which is read whole as fzn's way of typing a link
  rather than as a markdown link in brackets, are shown as typed. An image is never loaded but
  is a link reading its alt text, and inside a link only its alt text: the page never calls out
  ("Private by construction"), and the relay's `img-src 'self' data:` would refuse it anyway. There is still no colour: the
  highlighting is in paper, ink and the two greys, light and dark, with keywords and
  definitions heavier, strings, numbers and constants soft, comments faint and slanted, and
  punctuation and a diff's removed lines faint. Code read is set in ink where the rest of a body
  read is in the soft grey, so that what would be copied out reads crisply. A link is in the
  colour of what it is in, with a faint underline.

  It is built as the editor was, by hand from pinned packages by one script, the same bytes
  from the same inputs, into committed files that are now several. `web/editor/render.js` is the
  page's reading of markdown, the parser, the highlighter and the renderer; the page imports it
  as it starts and never waits for it, so until it arrives, and for good if it cannot be
  fetched, everything reads as typed, and is painted again when it comes. The editor imports
  the parser and the highlighter from it, and the two share chunks rather than each carry a
  copy. Each language of code is a file of its own under `web/vendor/code/`, fetched the first
  time a block in it is read or written: shell, JavaScript, TypeScript, JSON, Python, Rust, Go,
  SQL, YAML, TOML, HTML, CSS, diff, Dockerfile, C and C++, and Java, each under its usual short
  names too. A language is a Lezer grammar where there is one, and otherwise CodeMirror 5's
  stream mode, as shell, SQL, TOML, diff and Dockerfile are, run by CodeMirror in the editor and
  by a few lines over CodeMirror's string stream in the page. `@codemirror/language-data` would
  have had every language, as about 124 files and 1.7 MB committed. Sizes raw, since the relay
  sends no compression, and gzipped after: the page fetches 78 KB (27 KB) more as it starts,
  the render module and the two chunks it shares with the editor; the editor, 411 KB (133 KB)
  in one file before, is 392 KB (127 KB) fetched when a body first opens and 470 KB (154 KB) with
  what it shares; a language is from 0.3 KB for diff to 103 KB for C++, and the grammars share
  27 KB of Lezer's parser. Only the one place a body is read that the page's width shows, beside
  the list or above the line, is rendered. A long body is rendered at first only as far as
  either shows, its first 5,000 letters, and whole once the page is idle: a note of 6,300 words
  took 7 to 12 ms to render and 10 to 15 more to lay out, against 3 to 7 to lay out as plain
  text, and so cost the arrow that landed on it a frame; its first 5,000 letters take 4 to 6 ms,
  laid out. A head with no colon holds no address the page would follow and is not parsed: the
  500 rows the top of the list paints when the line is cleared took 5 to 6 ms to read as
  markdown, against well under 1 as text. Nested past what the stack holds, 1,500 lists or 3,000
  quotes deep in Chrome, a body is read as typed.
- **Words find; everything else holds.** Decided 2026-10-15, agreed with Sam. It replaces "A
  date typed as a search finds the notes that mention the day and the notes written on it" under
  Model, and how a tag or a person typed on the line counted until now, as one more thing to
  score: `#house boiler` listed every boiler, the one in the house first. It revises "Search
  groups" in what stays from the original, and in "A place lists what is in it; typing finds"
  what an empty line is: a line with nothing on it to rank by. The line has one grammar, read in
  the engine, so that both faces have it. Words rank exactly as they did: BM25, prefixes,
  look-alikes, the head signal, and the name rule. Everything else on the line is something a
  note has to hold to be listed at all, and none of it changes how the words rank.
  - `"…"` is a phrase: the note's whole text, head and body, has it, in any case, as plain text.
    An unclosed quote runs to the end of the line, the space typed last included, so the list
    narrows as the phrase is typed. A phone's keyboard curls quotes as they are typed, so `“`,
    `”` and `„` are quotes too, any of them closing what any of them opened. An empty phrase holds
    nothing. A phrase's words are words as well, so a phrase in a head outranks one in a body.
  - `#tag`, `@person` and a day hold: the note carries the tag or the person, or is of the day,
    by mentioning it or by having been written on it. The last thing typed holds every tag or
    person it is the start of until whitespace follows it, `#ho` holding `#house` and
    `#hosting`, so the list does not empty mid-word. The name rule reads a label typed as the
    words it says, since a head says its tags as words: `oil tank #house` names the note headed
    `Oil tank #house`.
  - A span, `START..END`, `START..` or `..END` with no spaces round the dots, holds the notes
    written in it. Each end is a date written out or a date word, with a 24-hour `HH:MM` after
    it if wanted: `/-mon 18:00..` is since six on Monday evening, and `2026-10-01..2026-10-05`
    runs through the end of the 5th; a span runs from the start of its first day or minute to
    the end of its last. A time is read by the clock of the place each note was written in, as a
    note's day is, a note with no time of writing is in no span, and a time anywhere else on the
    line is words. Membership needs the moment a note was written, which the note relevance
    sees now carries in place of its day; nothing stored changed. `/-mon`, the last Monday before
    today, is new with spans, the mirror of `/fri`, and a date word now ends at the `..` of a
    span. The line says what a span stands for where it says a date word's date, `→ Mon
    2026-10-12 18:00 – now`. The engine words both, so that the faces paint text and keep no
    calendar of their own; an open start is `…`.
  - `~` as the first thing on the line makes its words letters, found in order in heads, as the
    original's fuzzy found them: `~bsv` finds Boiler service. Every word of them must be there.
    They rank as fzf ranks: a letter at the start of a word earns more, twice as much for the
    first letter typed, a letter running on from the one before earns at least what the first of
    its run did, and a gap costs more the longer it is; then the shorter head, then the space's
    order. Every row says `letters: bsv`. Letters name no note, so Right does nothing after
    them, and a phrase holds in this mode without ranking. The matcher is written here; a
    dependency for it would have been a decision of its own.

  A line with nothing to rank by lists exactly what holds what it holds, in the space's order
  and with no reasons, as a place lists its facts on an empty line: at the top it is cut at the
  500 rows the top of the space is cut at, and in a place it is not cut. A single letter is not
  a word, so `#house a` lists what `#house` lists. With words as well, they rank among the notes
  that hold. The note the line names has to hold it too. Inside a place, the line narrows the
  place's facts and its finds alike, and a line of labels in a place lists the facts that carry
  them, which for labels not on the path is what going into them lists. The rows of such a line
  are in the space's order, so they can be moved as the rows of an empty line can.

  Going into a note with something on the line was a search for it, and the line clears, when
  the note contains the line as typed, or holds what the line holds and is named by the words
  outside its phrases (with `~`, has its letters in its head), or the line has nothing else but
  what holds. A phrase that found a note in its body was a search, and so was `/today`, which
  until now went in as a draft. A draft with a tag in it, `boiler needs bleeding #house`, is
  still carried in, and so is a line with nothing on it to rank by or to hold, a letter or a
  mark, as before. Enter writes the line as it reads: quotes, `~` and spans stay as typed, and
  date words are written out as before, after a span's dots too.

  It is open to more. A new kind of filter, `!word`, archived or has a body, is one more form
  read from the line and one more way for a note to hold it; the words are not touched. Labels
  are postings the index has, intersected once a keystroke; a span and a phrase are asked of
  each note left after that, a phrase last since it reads the whole text, and among the
  suggestions only until the limit is reached. Measured on synthetic spaces in a release build,
  each line typed a key at a time: at 10,000 notes every line stays under 4 ms a keystroke (the
  worst, a first letter of `~bo #house`, 3.6 ms; letters 2.5 to 2.9 ms; phrases and spans under
  1.7 ms but for a first key), against 1.2 ms for words before and after. At 30,000 notes words,
  labels, phrases and spans are 2 to 5 ms with first keys up to 12 ms, as words were before, and
  letters are 15 ms, two cold passes over every note: the heads, and the space's order for the
  ties.

  What it costs. A draft whose words find nothing carrying its tag lists nothing to step down
  to, where the tag alone used to list every note carrying it; going into the tag first is the
  way to write there. `a` alone at the top lists the top of the space, where it used to list
  nothing. A date word now ends at two dots, so `../fri` in a note is written out as a date.
  Tab-separated groups, which the original had, and its `!` inverse stay unbuilt. Revised
  2026-10-16: the box of an open task, `[ ]`, is one more label, the first form added in the way
  this decision left open (see "A task is a note headed by its box").
- **A span is not a place.** Decided 2026-10-15, agreed with Sam. A span holds the notes written
  in it, Sam's choice over the notes written or edited in it, and a note written now was not
  written in a span gone by: one written inside a span would fall out of it as it was written.
  So a line holding a span, a phrase or `~` is not gone into as a line of labels is, whatever
  else is on it: Right goes only to the note the line names, if one is named and holds the line,
  as on any line that is not labels alone, and letters name none.
- **The arrow at the end of a line of labels goes into each.** Decided 2026-10-15, at Sam's ask,
  so that opening the app in the morning is one keypress: `#house /today`, Right, and the path is
  `#house › 2026-10-13`, one place by "Labels gone into one after another are one place". It
  goes into each label in turn as Right after each would, a label on the path before the
  place's note being gone back to, but of several a label the place has already is passed over,
  so that the rest are added to it and the path never repeats: `#house @joe` in `#house › today`
  goes to `#house › today › @joe`. One label alone is gone back to wherever it is on the path,
  as before: `#house` in `#house › today` goes back to `#house`. The line comes back on the way
  out of the first place it went into. Passing over a label typed alone too, so that `#house` in
  `#house › today` did nothing, was tried the same day and dropped at Sam's direction: typing a
  label on the path is how he goes back to it.
- **Export takes the line.** Decided 2026-10-15, agreed with Sam. `fzn export QUERY` writes the
  notes that line finds at the top of the space, every one of them and archived ones too, in the
  order the line lists them, one JSON line each as the plain export writes them, ranked with the
  config's weights. Archived notes are in because archiving is how a task is ticked, and a log
  of what was done wants them; each line carries the flag. Without a query the export is as it
  was. It takes no lock, so it runs while the app is open.
- **A task is a note headed by its box.** Decided 2026-10-16, agreed with Sam. A task is a note
  whose head starts with a box, `[ ]` while it is open and `[x]` once it is done, and nothing
  else is one: a `- [ ]` in a body is markdown, read as a drawn box where a body is read, and
  makes no note a task. Until now a task was a note like any other, archived when it was done:
  the importer took the box off each one and archived the ticked ones, and since 2026-10-15 Sam
  had been typing `[ ] ` at the start of a head by hand and archiving the note when it was done.
  Nothing listed what was still to do, and nothing in a note said that it was a task. Four calls
  shaped it.
  - *The box is text.* What you said goes in the text, and the test for adding to the agreed
    structure is whether a note still reads right on paper (see "The note is plain text"); a
    ticked box does, where the archived flag beside the text does not. So the box is written at
    the start of the head and read from there, the engine's syntax gains one form, and the line
    format does not change: a build from before reads `[ ] Order oil` as a note that says so,
    and old and new binaries and pages sync side by side. The head has to start with it: `Order
    oil [ ]` is not a task, nor is a head with a space before its box, and `[X]` typed by hand
    is not one either, though the importer reads it as `[x]`.
  - *Ticking is archiving.* What is done is archived already, so the box and the flag say one
    thing, and one key does both. Ctrl-V on a task, or the word in the foot, ticks it, writing
    `[x]` and archiving it, and on an archived one, with archived notes shown, unticks it,
    writing `[ ]` and bringing it back. Each is one version, so that no device sees the tick
    without the archiving or the other way round, and undo puts back both. The box can still be
    typed, and a build from before archives without ticking, so the two can disagree: an open
    task is one whose box and flag both say so, and the key puts a disagreement right. The foot
    says `tick` or `untick` where it said `archive` or `unarchive`, so the page has the word on a
    phone with no box of its own to tap, which is not built (see Open questions). A tick changes
    the text, so it is an edit and dates the note's `updated`, as any edit does; archiving
    anything else still does not. Ctrl-V on anything but a task is as it was.
  - *`[ ]` is a label.* Typed on the line it holds the open tasks, the notes headed `[ ]` and
    not archived, as `#house` holds what carries the tag, and the rest of the line still
    applies: `[ ] #house` is the open tasks of the house, and `[ ] oil` finds among the open
    tasks. It is the one form with a space in it, and it is read only whole: `[` and `[ ` are not
    words either, so they hold nothing and the list goes from the top of the space to the open
    tasks once, at the `]`, and a `[[` on its way to a link lists no tasks on the way. It starts a
    word, as a date word does. The index lists the open tasks, so the box costs what a tag costs.
    A done task is not a label: `"[x]"`, a phrase, holds the done tasks with archived notes shown,
    and any note with a ticked box in its body, and `fzn export '"[x]" #house'` writes what was
    done in the house.
  - *It is a place, as a tag is.* Right goes into `[ ]`, and into both from `#house [ ]`, and the
    path says `[ ]`. A note written there starts with the box, since that is where a box is
    written, and the other labels go at the end as before: a new row in `#house › [ ]` opens
    `[ ] ▍ #house`, the box, the caret and the tag, whichever was gone into first, and `Order
    oil` written from the line there is `[ ] Order oil #house`. The session says where the caret
    goes, so that neither face has to find the box, and both put it there and take the row away
    with Backspace there, as at the start of any new row. A head of nothing but what it opened
    with is empty as before, now however it is spaced, so a space typed or taken out beside the
    box writes nothing.

  The importer keeps the box: `- [ ] Renew passport` imports as `[ ] Renew passport`, and `- [x]`
  or `- [X]` as `[x]` and archived, as before. Its ids, which are where a note sat on its page,
  and everything else it does are as they were, a box with nothing after it included. Importing
  the same pages again is how a space imported before gets its boxes back: a note whose text was
  not changed here since is brought up to date with its page, which now has the box, and keeps
  its links, whether it is archived, that it was deleted, and where it sits, since a page places
  nothing and the place held is newer than none. Its links are added to and never taken from, so
  a link to the note it sat under that was taken off here comes back. Archiving has not dated a
  note's `updated` since 2026-10-13, so a task imported since and archived here is still as its
  page gave it and gets its box back too, still archived; one edited here is left as it is,
  without one. A box that disagrees with its note's flag, archived by hand or by a build from
  before, or ticked on its page and brought back here, is put right by a rewrite done once
  through `fzn export` and `fzn import-jsonl`, and no code was added for it. The rewrite keeps
  each note's `updated`, since when it was done is not known, and so that importing the export
  taken before it puts it back. Until every device runs this build, one from before archives a
  task without ticking it; unticking and ticking it here puts it right.

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
- A fork is put right the moment it is seen, by compacting. Whether that is too eager, as when
  a file was corrupted rather than copied, is for the first fork to say; the cost of a needless
  compaction is one log replaced by a shorter one.
- Key rotation. A space's key is for life. Changing a passphrase would mean every device
  compacting under the new key and the relay keeping both until all have, which is the
  compaction machinery and nothing more, but nobody has asked for it.
- What the relay still sees: log names, counts, sizes and timing, and the compaction headers.
  Padding lines to a few sizes would hide the sizes; nothing hides the timing.
- A version's stamp is whole seconds, and a second version of a note in the same second is
  stamped a second ahead so that it supersedes. A head is now a version a keystroke, so typing
  fast runs a note's stamp ahead of the clock, half a minute for a long head, and another
  device's change to that note inside that half minute loses even when it came later. Stamps
  finer than a second are the fix if two devices ever edit one note at once. Closed 2026-10-15
  by "Stamps are kept to the millisecond", made before it was felt.
- The browser's keys past the four, and Ctrl-D and Ctrl-O. Words at the foot reached with Tab
  are a start: nothing to remember, but slower than a chord for archiving ten notes in a row.
  Undo has no key at all yet.
- Touch. A tap puts the cursor on a note, a second opens its body, the `›` goes in, and the
  path at the top is the way back. Whether any of that is right is for use on a phone to say.
  Seen 2026-10-15 in Chromium's and WebKit's touch emulation: once the cursor is on a row its
  head is the text box and fills the row, so a second tap on the head puts the caret there. A
  second tap off it, on the date under the head on a phone, opens the body when it lands there,
  but a browser takes a tap that near a text box into the box: WebKit every time it was tried,
  Chromium some of the time. Until 2026-10-15 the body read under the row was where a second
  tap opened it in both, and it went with the body; on a phone the word in the foot is now the
  one sure way to open a body, and whether the row wants a target of its own for it is open.
- Right at the end of the head as the zoom key, or Alt-Z. Right is dead at the end of the line,
  and a hand that thinks the cursor is on the line when it is on a row goes in instead of moving
  the caret; the one cursor is the tell. Esc keeps rather than cancels, which spreadsheet hands
  will find out once. Both are for daily use to confirm. Closed 2026-10-14 for the line (see "A
  tag is a place you can go into"): Right at the end of the line goes into what the line names,
  and Esc on a line with text clears it. On a row Esc still keeps, and the rest is still for use
  to say.
- A new row under a row takes nothing from the line, where the original prepended the search
  text to every new line, so that a note added while looking at `#house` was a house note. Here
  the note added inside House's zoom links to House; one added from a search for `#house` is
  plain, and may fall out of the list when the cursor returns to the line and the search runs
  again. Whether that is a loss is for use to say. The terminal has no newline in place at all;
  a body is the editor. Closed 2026-10-14 (see "A tag is a place you can go into"): it was a
  loss, and `#house` is now gone into, and what is written there carries the tag. A search is
  still not a zoom, so a note added from a search for `#house` is still plain; the zoom is one
  Right away.
- A head held changed while another device edits the same note loses one side to
  last-writer-wins when it is written, which the editor already risked. The text CRDT is the
  answer.
- A summon key. Starting should feel like switching a window, so the terminal build wants a
  keybinding that raises a running instance (a special workspace on Hyprland). Outside the app.
- Vim mode: whether the caret's shape is enough to say the mode, and whether the word in the
  foot is where the switch belongs; daily use with it on decides. Marking the rest of the
  engine's syntax (tags, people, dates) in place in the body, in the two greys, is a small
  decoration left until use asks; URLs and bracketed names are marked as markdown since
  2026-10-15.
- The page's reading of markdown, 78 KB, is fetched on every page load, and the editor, 392 KB
  more, on each one in which a body opens, since the relay sends no validators or compression;
  whether that is felt on the phone is for use to say, and `Last-Modified` with 304s, or long
  caching for the files whose names are already their hashes, is the fix.
- Whether the body should be kept as it is typed too. A version is the whole note, so a
  keystroke in a long body writes all of it; it is kept when it closes, as the terminal's editor
  keeps it. Use will say whether that is felt.
- A log is compacted only when its device opens, and a head is now a sealed line a keystroke; a
  tab left open for days grows its log until it is reloaded. Compacting when grown, not only
  when opened, is the fix if it is felt.
- The clock is compared once a connection, when the relay's `now` is heard, so a clock set right
  while connected is still said to be off until the next connection. And the terminal hears the
  relay only between keys: a connection made again while the editor has the screen is heard
  when the editor closes, and the wait reads as this clock running ahead, two minutes after a
  long edit. A `now` said again from time to time is the fix for the first, and the time a
  message was heard going with it to the device for the second, if either is felt.
- A box in the page's row to tap, so that a task is ticked on a phone the way it reads. The word
  in the foot ticks it until then.
- Name. `fzn` stays unless something better appears.

## Milestones
0. **Data.** Done 2026-10-12. The 2022 corpus and the 2026 Silverbullet pages are
   in `~/fzn-corpus/`, outside this repo.
1. **The simplest live version, in the terminal.** Built 2026-10-12 as a Cargo workspace beside
   the Go code: `crates/fzn-engine` (model, index, three scorers, no I/O) and `crates/fzn` (the
   app: config, per-device log store, importers, terminal UI). The Silverbullet pages are the
   `main` space and the 2022 corpus is the `fzn-2022` space. Daily use from here; tuning follows
   use. The Go code was removed in one commit on 2026-10-14.
2. **Relay and sync.** Single binary, opaque lines, multiple devices. Built 2026-10-13: the
   exchange is in the engine as `Replica`, `fzn relay` is the relay, and fzn syncs with one
   whenever one is named. Running since 2026-10-13 as a system service on a machine that is
   always on, reached over a private network, where a note takes about a second to get from
   one window to another. Reworked 2026-10-14: the engine owns sync through `Device`, lines
   are sealed, and compaction is a new log.
3. **Same core in the browser**, as one more device on the relay. WASM build, DOM renderer,
   ego graph. Built 2026-10-13 without the graph: `crates/fzn-web` is a tab as a device, and
   `web` is the page. Before that, the same day, the app moved out of the terminal into the
   engine as a session. `crates/fzn-engine` is now the index, the scorers, the note as it is
   kept, the session and the replica; `crates/fzn` is the terminal face, the log files, the
   relay, the system clock, config and the importers.
4. **Text CRDT per note.** Behind the store module.
5. **Sharing and collaboration.**
