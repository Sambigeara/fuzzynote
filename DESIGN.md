# Fuzzynote

## Vision
A blank page with a single blinking cursor. You type a thought, a note, a TODO. As you type, the
engine finds what you have already written that belongs with it and shows you why. Go into one of
those and write there, and what you write belongs to it: where you are is what you are talking
about. Everything is local, private and instant. The same engine runs in a terminal and in a
browser, syncs seamlessly between your devices, and can be shared with other people when you
choose.

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
  almost nothing to remember. The whole app fits in one sentence: type to find, or into the row you
  are on to change it; Enter writes the line, or opens a new row under the one you are on; Ctrl-O
  opens the body; Tab goes in, from a head into its note and from the line into what the line names;
  Esc clears the line, or comes back; and where you are is what you are talking about. Every other
  key is maintenance, and the footer shows only the ones that apply right now. A key is justified by
  the flow it sits in, never inherited.
- One cursor. There is exactly one on the screen and it moves: the caret on the line, or the
  caret in the head of the row the cursor is on. Nothing else is highlighted and almost nothing
  else is drawn: no borders, no counts, no colour. The body read is that of what the cursor is
  on, in a place of its own apart from the list, never under a row or the line. What is not
  being asked for is not on the screen.
- Archived is not forgotten. The list hides archived notes; the engine still remembers them.

## What stays from the original fzn (the soul)
- The search line is the composer: filter live, Enter turns the search into a new note carrying
  that text.
- Enter, then type into the new row. The original's rhythm for a list was Enter, type, Enter,
  type, with the search line as a standing context; the place you are in is that context now,
  and Enter on an empty line opens a new row to write in. An empty row left empty was never
  anything.
- Every line is editable where it is. The original had one text cursor over the whole screen,
  and so does this: the head of the row the cursor is on is the field, typing changes it, and
  each change is kept as it is typed. Browsing is editing, and a stray key is one undo.
- Zoom on what is shared. The original selected notes and pressed Enter, and whatever they had in
  common became the context; the old common-prefix zoom was the special case. What is reinstated is
  the label: a tag, a person, a day or the open tasks' box typed on the line is gone into with Tab
  and is the context, labels gone into one after another are one context, and so are a note and the
  labels gone into after it. Selecting notes, and a context of plain words, are not (see "A tag is a
  place you can go into" for why).
- One line to search with: words find and everything else holds, so `"…"` is the exact text and a
  tag, a person, a day, the open tasks' `[ ]` or a span narrows what is listed, and `~` at the start
  of the line is the original's fuzzy, letters in order in heads. The original's Tab-separated
  groups are given up, Tab being the key that goes in, and its `!` inverse is not built yet.
- Hashtags, @mentions, date expansion, URLs opened or copied with one key.
- Archive with one key, which ticks a task; show/hide archived globally.
- Undo/redo, move up/down, copy of whole notes (paste is not built yet).
- Local-first, remote-second. Offline is the normal case.

## Model
- **Space**: a set of notes plus explicit links, kept apart from every other space. On disk a
  space is one directory of logs under the root. Plain `fzn` opens the space named in the config
  file (`main` to begin with) and `--space NAME` opens another; a tab opens `main`, or the space
  `?space=garden` names. Sharing a space is later.
- **Note**: stable id; text; links; created and updated times; archived flag; where it sits in
  the list, as the note it sits after, with a clock of its own (see "Where a note sits" under
  Storage and sync). Derived, never stored: head (first line), body, tags, mentions, dates,
  URLs, and whether it is a task.
- **Link**: from one note to another, held on the note as the target's id. There is one kind.
  It survives a rename and adds no words to the text. Links are made by position: a note
  written while looking around another is about it, and links to it. Nobody has to say "link".
  Brackets are the way of typing one from inside the editor: when a note is saved, each
  `[[head]]` that names an existing note becomes a link, and a link whose brackets were taken
  out is dropped. The one explicit gesture is a correction: a chord on a note in a note's place
  links it or unlinks it, for when the engine was wrong or the note was written in the wrong
  place. The engine does not read brackets when it searches. A link adds no words, and still a
  note is found by the name of what it is about: when you type, the head of a note that a note
  links to counts as its own head does, one hop and no further. Only explicit links are stored;
  everything else is recomputed on demand.
- **Syntax that the engine understands**: `#tag`, `@person`, dates, URLs, `[[note head]]` as a
  way of typing a link, and a box, `[ ]` or `[x]`, at the start of a note, which makes it a task
  (see "A task is a note headed by its box"). Nothing else is special in a note. In particular
  the old ` >> ` prefix paths are plain text; the words in them still count, the hierarchy does
  not. On the line, and only there, three more forms are read: a phrase in quotes, a span of
  when notes were written, and `~` at its start (see "The line" under Clients).
- **Date**: plain text in the note, written `2026-10-16`, and never stored apart from it. The
  engine reads it on load, as it reads tags. A link is held as an id because a note's head can
  change; a date's name cannot, so it lives in the text. The day a note was written is not
  typed at all: it is the note's `created`. `/today`, `/tomorrow`, `/yesterday`, a day of the
  week (`/fri` is the next Friday and `/-mon` the last Monday, never today) and `/+3` or `/-2`
  are quicker ways of typing a date, and any beginning that only one of them has will do (`/tom`,
  `/f`, `/-tu`). Any other slash is left alone, and a date word ends at the `..` of a span. The
  line shows the date a word stands for and holds with it. Saving a note, from the line or from
  the editor, writes the date in its place, at the moment a `[[head]]` becomes a link. A date
  typed on the line holds the notes that mention the day or were written on it, and counts as a
  date and not as the numbers in it. The old `Mon, Jan 2, 2006` form is still read.
- **Context inheritance**: where you are is what you are talking about. A note created in a
  place inherits it, and nothing else does: it links to the place's note and carries the place's
  labels. This is what prefix prepending used to do, without a tree and without the typing: the
  old ` >> ` prefix was this context written by hand on every line, and Ctrl-P pasted it. The
  rules are under "Zoom and places".

## Engine
The engine is a library, and it is the app. It owns no UI, opens no file, reads no clock and
makes no network call, so the same code runs in a terminal, in a browser tab and in a test.

**Session.** What a face drives, one per person per space. Three things go in and two come
out.
- In: an *action*, which is what was asked for and never which key asked: the line now reads
  this, move the cursor, write the line, go into a note or a label, step back, archive, delete,
  link, undo, the text of a note as it was edited. An action that does not apply where the
  cursor is does nothing.
- In: versions of notes written elsewhere, by another device or another writer on this one.
- In, from the host: the time, an id for a new note, and randomness for sealing.
- Out: *effects*, which are what the face has to do about an action: keep this version, put
  this note in front of the person to edit, copy this, open this URL. The session has already
  done its own part, so an effect that fails costs a message and nothing else.
- Out: the *view*, which is everything there is to paint: the path, the line and what its date
  words and spans stand for, the rows with their reasons, where the cursor is, the note under it
  as it is being written, the note you are in, what just happened, and what can be asked for
  where the cursor is, each with what to call it there. A face puts its own key or button beside
  each.

Everything decided about how the app behaves lives here: what going into a note does with the
line, what is listed first, what a note written in place links to, what is offered where. Under
the session is the part that finds things, in three parts.

**Index.** A Unicode-aware tokeniser, lowercased, with a URL counted as its host; no stemming.
An inverted index with term frequencies and document lengths, a trigram index over the
vocabulary for fuzzy matching, and a label index for tags, people, days and the open tasks' box.
All update incrementally on every note change. The old corpus is 8.7k notes, 284k tokens and 12k
distinct terms; the index for a space ten times that size fits comfortably in memory.

**Scorers.** Each takes the current input plus context and returns candidate notes with a score
and a human-readable reason. Three are built, cheapest first.
1. *Explicit*: shared tags, mentions, link targets, and words of the query that appear in a
   note's head (weighted by rarity, so a note titled with what you typed outranks a one-liner
   that merely mentions it; BM25's length normalisation would otherwise bury long notes). A word
   in the head of a note that a note links to counts too, at a little less than a word in its own
   (`about` in the weights), with the reason `about Boiler`.
2. *Lexical*: BM25 over the inverted index. Rare shared words count far more than common ones.
   The word under the cursor is a prefix, not a word: it stands for every vocabulary word it
   starts, in the word scorer and in the head signal, and a note earns credit for its best
   completion only. This is what makes results right while a word is still being typed. A
   misspelling still finds its note by trigram overlap, at a fraction of an exact word's worth.
   With `~` at the start of the line the words are letters instead, found in order in heads and
   scored as fzf scores them, and no other scorer runs.
3. *Temporal*: created or edited close in time; dates written in the text.

Intended and not built, until daily use asks: *structural* (notes that sat adjacent in the list,
notes you navigated between), *feedback* (a locked link boosts shared terms between the two
notes; a dismissal dampens), and a local embedding scorer, on device, if semantic similarity is
ever wanted. That would be another scorer, not a dependency.

**Combiner.** Weighted sum with per-scorer caps, top-k with reasons attached. What the line
holds comes before anything: a note that does not hold it is not listed at all, so the scores
only ever order what holds. Two facts come before any score: what is in the place being looked
around, linked to its note and carrying its labels, and the note headed with what was typed.
With nothing on the line to rank by no scorer runs, at the top or in a place: a place's facts
are a set, the note's links kept to the notes on the index's list for every label. Budget:
under five milliseconds per keystroke for ten thousand notes.

**Tuning happens in the product.** Every suggestion shows its reasons; writing where you are
makes a link, and a chord corrects one, stored on the note as the target's id; the weights live
in the config file; a toggle shows scores. There is no evaluation harness and no labelling step.
Nothing is added to the engine until daily use asks for it.

## Storage and sync
- **The note is plain text; what holds it need not be.** All of a note's meaning is in its
  characters, in forms people already know: `#tag`, `@person`, `2026-10-16`, a URL, a first
  line, a box `[ ]`. That is the agreed structure, and the test for adding to it is whether the
  note still reads right on paper. Beside the text go the facts it cannot carry: id, links,
  times, archived, deleted. What you said goes in the text; what happened to the note goes beside
  it. A file per note is not the store: files cannot merge two devices' edits, and most notes
  are one line.
- **One append-only log per writer, per space.** Each line is a full snapshot of one note as
  JSON: `v`, the version of the line format, then id, text, links, created, updated, stamp,
  after, placed, archived, deleted, device. The format is version 3: version 2 added `after` and
  `placed`, version 3 took times to the millisecond, and lines from before versions hold unix
  seconds. All are still read; a line with a later version than the build knows is left alone,
  not misread (see "A line of a later format is left as it is"). Times are RFC 3339 to the
  millisecond with the offset of the clock that wrote them, `2026-10-13T10:12:56.250+01:00`, so
  a line reads without the app and a note keeps the day it was written on wherever it is read.
  `updated` is when the text last changed, so archiving a note does not re-date it. `stamp` is
  when what the version says was written, and a local change is always stamped past the version
  it replaces, a millisecond past it when the clock has not moved on since. `after` is the note
  it sits after and `placed` when it was put there, a clock of the place's own, kept and stepped
  the same way. Loading replays every log in the space: the version with the newest stamp gives
  the text, links, times and flags, ties broken by writer id, and the version newest by `placed`
  gives the place, ties broken by the note it is after. An edit leaves `placed` as it was and a
  move leaves `stamp`, and each carries the side it leaves as it was written, writer and all, so
  an edit on one device and a move on another both hold, and neither decides anything about the
  side it carries. Last-writer-wins on what a note says is the known limitation, accepted for
  now. Deletion is a tombstone line, kept so an older version arriving later cannot resurrect
  the note. What a line says, and which of two versions is the newer, is the engine's to settle,
  so that every device agrees. Where the lines are kept is the face's: files under the terminal,
  IndexedDB in a tab.
- **Where a note sits** is the walk of the tree that `after` makes: at the top every note placed
  at the top, newest placed first, and each note followed by the notes placed after it, newest
  placed first, and theirs. A note is placed at a moment past every note already placed after
  the same one, so it sits directly under it whatever the clocks that placed the rest, and two
  devices writing under one note at once both sit under it, each run whole. A note from before
  places sits by when it was written; one placed after a note no version is held for sits at
  the top until that one arrives. Deleted notes, and archived ones while hidden, are walked
  through and not listed, so what was placed after them keeps its place. A cycle, which two
  devices moving at once can make, is listed as if it were at the top, so that no note is lost.
  Enter on a row places the new note after that row in its first version; Enter on the line, and
  a note written from the line, place it at the top. An edit never moves a note. A move is one
  note alone: down places it after the row below, and up after the note the row above follows in
  the space's order, or at the top, so that it lands beside the row it passes and goes past
  nothing else, even among a place's facts, where the notes between two facts are not shown.
  What was placed after it is placed after the note it followed instead, oldest first, so a run
  written under a note stays together and no cycle is made. A move is one entry to undo, though
  it writes a version of every note it places; moving the top up, or the last down, writes
  nothing. Undo takes back only the side a change made, so undoing a move leaves what has been
  written since and undoing an edit leaves where the note has been moved since. The order is
  walked afresh each time the list is made and nothing is cached.
- **Lines are sealed before they are kept.** What goes into a log, and so to the relay, is not
  the snapshot but an envelope around it: `{"s":1,"c":"…"}`, where `c` is a random 24-byte nonce
  and then the snapshot encrypted with XChaCha20-Poly1305 under the space's key, base64. The key
  is 32 bytes derived from a passphrase with Argon2id (19 MiB, two passes, one lane) and a salt
  that is the first 16 bytes of the SHA-256 of `fzn/space/` and the space's name, so that the
  same passphrase on another device gives the same key and nothing has to be sent to get it.
  Each device derives the key once and keeps it, under the terminal in the config directory
  (`keys/<space>`, readable only by its owner, never beside the notes) and in a tab in the
  browser's storage, so the passphrase is typed once a device. The first time a space is opened
  on a machine fzn asks for its passphrase, twice if the space is new; `fzn key` asks again, for
  when it was mistyped, and the page has a `passphrase` word in its foot for the same;
  `$FZN_PASSPHRASE` answers for scripts. Lines from before sealing are still read as they are;
  the first open with a key compacts the writer's own log into a sealed one (see Sync), so plain
  lines leave the writer's log at once and the relay as soon as it has the sealed one. The key
  is verified by opening a line: a device that has seen a sealed line and opened none has the
  wrong passphrase, says so, and writes nothing, since a line sealed under a second key would
  split the space in two. A space with no sealed line yet takes the first key it is given.
  Forgetting a passphrase is losing the notes; there is no recovery and no reset, and sharing a
  space will be sharing its passphrase until there is something better. What the relay still
  sees: the names of the logs, how many lines each has and how long they are, when they arrive,
  and the compaction headers. What a relay can still do: withhold lines, or hand back an old
  prefix; it cannot forge, alter or reorder one.
- **Who writes a space's directory.** The app, one window at a time, settled by a lock on a file
  in the directory: a second window is told so and stops, and so is a relay asked to serve a
  space an fzn has open, or an fzn opening a space a relay holds. The importers take the same
  lock, so they run while the app is closed. `fzn tick` and `fzn untick` write beside the open
  app, each run appending to a log of the commands' own under a lock of their own, never the
  app's; the app looks in the directory about once a second for what has been added to logs it
  does not write, and takes that in as it takes in the relay's lines (see "A command writes
  beside the app, as a writer of its own"). `fzn export` takes no lock. Nothing else writes
  there: Syncthing and the like are not supported and not needed; the relay is the way between
  machines. What arrives from the relay comes through the engine, which asks the face to keep it.
- **Later: a text CRDT per note** (Yrs, the Rust port of Yjs, or Loro). The log lines then carry
  sealed CRDT updates instead of sealed snapshots. The replica and the relay do not change,
  since they never read a line; the session and `Notes` do, since the session reads and makes
  `Record`s directly in a couple of dozen places and undo holds whole versions. Count on a day
  in `session.rs` and `record.rs`, not a swap behind the store. Compaction already turns a
  history into one snapshot a note, which is what a CRDT log needs from time to time too.
- Sharing is sharing a subset of updates. Deferred, but nothing above prevents it.
- **The snapshot line is the way in and out, for good.** `fzn export` writes every live note as
  one plain line, by your own hand and to wherever you point it, so notes can always be read and
  taken away without the app; `fzn export QUERY` writes only what that line finds at the top of
  the space, archived notes too, in the line's order (see "Export takes the line"). `fzn tick ID …`
  and `fzn untick ID …` do to the notes the export names what Ctrl-V does to a task, and write
  each as the export would. `fzn import-jsonl` is the way back in, and `fzn import-markdown` reads
  a directory of pages (see "Imported pages become the notes they would have been"); a plain file
  is not read as a log by being put in the directory, since it would then be sent in the clear.
  When the logs carry CRDT updates this format stays. It carries the facts beside the text as
  well as the text: the 2022 corpus is what it looks like when only the text gets out (creation
  time survives on 7% of its notes).
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
  directory under the terminal (`.writer` for the app, `.commands` for the commands) and in the
  browser's storage for a tab, so that the writer of one space is not the writer of another.
- **Compaction is a new log.** A log only grows, so a writer that wants a shorter one starts
  another: it takes a new writer id, and the first line of the new log is a header,
  `{"compacts":["3f9a01c2"],"snapshot":293}`, naming every log it has had before and how many
  lines follow as the snapshot. The snapshot is the writer's latest version of every note it
  ever wrote, tombstones included, one sealed line each, and then every line of a later format
  as it was; after it the log goes on as any log does. A replica that holds the header and the
  whole snapshot *retires* the logs the header names: it drops them from what it holds and says,
  ignores a peer that offers them, and deletes them from wherever it keeps lines. Until it holds
  the whole snapshot it keeps both, and the notes come out the same, since a version is a
  version whichever log it is in. The writer compacts at open when its own log holds lines from
  before sealing, when a peer's header has retired it, or when it holds more than a thousand
  lines and has grown past four times the notes in it. Putting a fork right is the same move:
  the writer takes a new log, and the one with two writers is retired by the header. Nothing is
  state: which logs are retired follows from the logs held, and the header names the whole chain
  so that it still follows after the middle of the chain is gone.
- **A mark is a point in a log**: how many lines in, and a digest of those lines. The digest
  is FNV-1a, 64 bits, over each line and its newline, written as sixteen hex digits. It is a
  check against accidents, not a defence against anyone.
- **Four messages**, each one JSON object with one key. A message a build does not know is
  passed over.
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
  then the space. Each end pings a quiet peer every fifteen seconds and gives up on one silent
  for 45. A lost relay is dialled again, waiting longer each time up to half a minute, and a
  device says what it holds whenever it connects, so nothing said while there was no connection
  needs keeping.
- **The relay** is a replica that writes nothing of its own, and it is `fzn relay`. It serves
  every space under its root and keeps each as the same files a device keeps, sealed, and holds
  the same lock on each, so nothing else writes them while it runs. It listens on
  `127.0.0.1:3960` until told otherwise. It has no password and no TLS yet (`wss://` is refused):
  anyone who can connect can take a copy of the sealed lines and add lines of their own, which
  no device will be able to open. What keeps it private is the key, which it never has; what
  keeps it tidy is the address it listens on and whom the machine lets reach it. As a systemd
  service, `IPAddressAllow=` says whom. It carries the page and hands it out on the same
  address, so a browser has one address to open; `--web` points it at a directory instead.
- **A device syncs by having a relay named**, in the config file, in `$FZN_RELAY` or with
  `--relay`. There is no command for it. While the app is open a thread keeps the space's logs
  in step with the relay, and the foot says only when that is not going. With no relay named,
  nothing leaves the machine.
- **The engine is where sync and the app meet.** A *device* is a session and a replica joined
  in memory, with the space's key, and it is in the engine as `Device`: the same code in the
  terminal, in a tab and in a test. A version the session wants kept is sealed, becomes a line
  of this writer's log, and the replica sends it on; a line the replica takes from the relay, or
  that another writer on the machine added where the face keeps lines, is opened and given to
  the session. The face is left with what only it can do: the socket, and keeping lines for the
  next time. In the terminal that is one thread holding the websocket and passing text both ways
  over channels, and a directory of files. Nothing is locked per write and nothing can go stale
  under the replica: each line of another writer's log is read once, as a peer's line is.
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

### The cursor, in both faces
It is in one of two places, the line or the head of a row, and typing goes to the one it is in.

On the line, typing finds. Enter with text writes the line as a note and takes the cursor to it, the
caret at the end of its head; Enter with nothing on the line opens a new row at the top of the list,
its head the field. Down moves onto the list. Tab goes into what the line names, wherever the caret
is, and Esc clears it, or on an empty line comes back (see "Zoom and places").

On a row, the head is the field and typing changes it, with the caret landing at its end, and each
change is kept at once. Up, Down and Esc leave the row, Esc keeping what was typed, as an editor's
hands expect. Enter opens a new row directly under it, its head the field, as the original's Enter
added a line under the current one. Ctrl-O opens the body, which in the terminal is your editor and
in the browser a sheet over the page that holds the whole note. Tab goes in. Right and Left are the
caret's, on the line and in a head, and nothing else. Undo puts the head back as it was when the
cursor landed, and otherwise takes back the last change, which for a row is the whole visit, one key
for both. A chord acts on the note as it reads.

A head is a version at every change, a new row's from its first letter, which creates the note
with the moment Enter was pressed as its `created`, linked to the note being looked around and
to whatever it names in brackets. Date words are written out when the cursor leaves the row, a
new row in a place has its labels from the start, and a body is kept when it closes. So Enter,
type, Enter, type, Enter, type, Esc writes a list, from the line or from any row, each note on
every device as it is typed. A new row left with nothing in its head but what it opened with goes
when the cursor leaves it, the arrow landing past where it was; one never typed into reaches
neither the log nor the relay, and one typed into and cleared has its deletion written and
leaves nothing to undo. Enter on such a row does nothing: there is already an empty row to write
in. A head cleared to be typed again is not written while it is empty, and a note emptied is
deleted when the cursor leaves it, or when an empty file comes back from the editor; undo brings
it back. Backspace on a row with nothing in its head takes the row away and goes up to the row
above, or onto the line, as it takes an empty line away in any editor; a note with a body stays,
since the body is not in the head to see. A note stays where it was written, and an edit never
moves it. The list holds still while the cursor walks it, so that the arrow that leaves a head
lands where it was aimed, and what arrived from elsewhere meanwhile is listed when the cursor
comes back to the line.

### The line
The line has one grammar, read in the engine so that both faces have it: words find and
everything else holds. Words rank, by BM25, prefixes, look-alikes, the head signal and the name
rule (see "A name is a fact too"). Everything else on the line is something a note has to hold
to be listed at all, and none of it changes how the words rank.
- `"…"` is a phrase: the note's whole text, head and body, has it, in any case, as plain text.
  An unclosed quote runs to the end of the line, the space typed last included, so the list
  narrows as the phrase is typed. `“`, `”` and `„` are quotes too, any of them closing what any
  of them opened, since a phone's keyboard curls them. An empty phrase holds nothing. A phrase's
  words are words as well, so a phrase in a head outranks one in a body.
- `#tag`, `@person` and a day hold: the note carries the tag or the person, or is of the day, by
  mentioning it or by having been written on it. The last thing typed holds every tag or person
  it is the start of until whitespace follows it, `#ho` holding `#house` and `#hosting`, so the
  list does not empty mid-word. The name rule reads a label typed as the words it says, since a
  head says its tags as words: `oil tank #house` names the note headed `Oil tank #house`.
- `[ ]` holds the open tasks, the notes headed `[ ]` and not archived, and the rest of the line
  still applies: `[ ] #house` is the open tasks of the house, and `[ ] oil` finds among them. It
  is read only whole: `[` and `[ ` are not words either, so they hold nothing, and a `[[` on its
  way to a link lists no tasks on the way. A done task is not a label: `"[x]"`, a phrase, holds
  the done tasks with archived notes shown.
- A span, `START..END`, `START..` or `..END` with no spaces round the dots, holds the notes
  written in it. Each end is a date written out or a date word, with a 24-hour `HH:MM` after it
  if wanted: `/-mon 18:00..` is since six on Monday evening, and `2026-10-01..2026-10-05` runs
  through the end of the 5th; a span runs from the start of its first day or minute to the end
  of its last. A time is read by the clock of the place each note was written in, as a note's
  day is, a note with no time of writing is in no span, and a time anywhere else on the line is
  words. The line says what a span stands for where it says a date word's date, `→ Mon
  2026-10-12 18:00 – now`; the engine words both, so that the faces keep no calendar of their
  own, and an open start is `…`.
- `~` as the first thing on the line makes its words letters, found in order in heads, as the
  original's fuzzy found them: `~bsv` finds Boiler service. Every word of them must be there. They
  rank as fzf ranks: a letter at the start of a word earns more, twice as much for the first letter
  typed, a letter running on from the one before earns at least what the first of its run did, and a
  gap costs more the longer it is; then the shorter head, then the space's order. Every row says
  `letters: bsv`. Letters name no note, so Tab does nothing after them, and a phrase holds in this
  mode without ranking.

A line with nothing to rank by lists exactly what holds what it holds, in the space's order and
with no reasons, as a place lists its facts on an empty line: at the top it is cut at the 500
rows the top of the space is cut at, and in a place it is not cut. A single letter is not a
word, so `#house a` lists what `#house` lists. With words as well, they rank among the notes that
hold. The note the line names has to hold it too. Inside a place, the line narrows the place's
facts and its finds alike, and a line of labels in a place lists the facts that carry them. The
rows of such a line are in the space's order, so they can be moved as the rows of an empty line
can. Enter writes the line as it reads: quotes, `~` and spans stay as typed, and date words are
written out, after a span's dots too. Archived notes are out of every list, typed results
included, until they are asked for.

The line comes with you into a place, under one rule: if the note you went into contains what
you typed, or holds what the line holds and its head and the heads of the notes it links to have
every word of it outside its phrases (with `~`, its head has the letters), or the line has
nothing else but what holds, you were searching for it, so the line clears and stepping back
brings it back; otherwise you were writing, and the draft is carried in to be written here. So a
phrase that found a note in its body was a search, and so was `/today`, and a draft with a tag in
it, `boiler needs bleeding #house`, is carried in, as is a line with nothing on it to rank by or
to hold, a letter or a mark. A place gone into from the line was searched for by definition, so
the line clears.

### Zoom and places
Tab on a note's head makes it the place, and the path grows by one, wherever the caret is in the
head. A tag, a person, a day or the open tasks can be the place too: Tab on the line goes into what
the line names. That is the label if the line is one label and nothing else, `#house`, `@joe`,
`/fri` or `[ ]`, or back to it if it is on the path; each label in turn if the line is several,
`#house /today`, as if Tab had been pressed after each but passing over a label the place has
already; and otherwise the one note in the space the line is the name of and that holds what the
line holds, by the rule in "A name is a fact too". A line holding a span, a phrase or `~` is never
gone into as labels are, whatever else is on it (see "A span is not a place"). In a place that rule
holds as it does at the top: the note the line names is listed first, saying `its name`, wherever it
is and whatever the limit, so Tab goes where the list shows, and typing a name is the way from any
place to any note. A line that names neither, or names where you already are, leaves Tab doing
nothing, and types nothing: a tab is never written into the line or a head. The path shows a label
as it is written: `#house`, `@joe`, `2026-10-16`, `[ ]`.

The place is the last note on the path, if there is one, together with every label gone into
after it; labels before the note are only how you got there, so `#house › Boiler › today` is the
place `Boiler › today` is. Its facts are the notes linked to or from the note that carry every
one of those labels: the links alone when no label follows the note, and with no note on the
path every note carrying all the labels, which for a day is every note written on it or
mentioning it. A link is a fact and so is a label.

The list works as the top of the space does. With nothing on the line to rank by it is what is
in the place and holds what the line holds, its facts in the order the space keeps them in,
however many, and nothing the engine suggests; a row says how it is linked, `links here`,
`linked from here` or `linked both ways`, and nothing scored. Typing words finds: the note the
line names first, then the place's facts that match, then what the engine finds in its
neighbourhood that matches, each best first with its reasons and each holding what the line
holds, and a row of nothing between the last fact and the first find. Around a note the
neighbourhood is what shares its tags and people, words and dates, or links to the same things,
with only the notes carrying the labels counted as facts; around labels alone it is what their
words find.

Stepping back never throws away what you typed: a line written inside stays on the way out, and
the search comes back only to an empty line. Esc with text on the line clears it, and only on an
empty line steps back, so that inside a place "found it, clear, stay" is one key; the session
offers `clear` as the way back while the line has text and `back` once it is empty.

What you write inside is about the place. Inside a note it links to it and lands at the top of
the place. Inside a label its head ends with the label, as it would have been typed, unless the
note carries it already: in its text, or for a day by being written on it, since the day a note
was written is its `created`. Among the open tasks the head starts with the box instead, since
that is where a box is written. A note written from the line has the labels added as it is
written; a new row opens with them already in its head, the caret where the writer's text goes,
and a row with nothing in it but what it opened with, or a start of it, is empty, however it is
spaced. Inside labels gone into one after another the head ends with each of them it does not
carry, in the order of the path: in `#house › @joe` a new row opens ` #house @joe`, in
`#house › [ ]` it opens `[ ]  #house` with the caret after the box, and in `#house › today` a
note reads `Order oil #house`. A zoom on today opens an empty row, one on `/fri` opens
` 2026-10-16`. Only a note created there inherits, and only from the place: in `Boiler › today` a
new note links to Boiler and is of today by being written on it, in `Boiler › #house` a new row
opens ` #house` and links to Boiler, and in `#house › Boiler` a new note links to Boiler and
carries no tag.

The link chord, Ctrl-L in the terminal and `link` or `unlink` in the page's foot, links the note
under the cursor to the place's note or unlinks it, which takes it into the place or out of it when
it carries the place's labels: on an empty line an unlinked note leaves the list, and a note to link
is one that typing found. It is a correction, not the mechanism, and with no note on the path, at
the top or inside labels alone, it has nothing to do and says so. Tab on a head again walks a hop
further, and on a place already on the path goes back to it, so the path never repeats; stepping
back goes one hop, onto the line. This is the ego graph as a flat list; following a link is picking
its target here.

### Terminal
Rust and ratatui, with the original fzn chords where they still earn their place, because that
is where the muscle memory is. It is a face on the device and adds four things: its key table,
the editing of the line and of the head of the row the cursor is on, the painting, and carrying
out the effects, which here means the space's directory, your editor, the clipboard (through the
terminal's own OSC 52 sequence) and the system's way of opening a URL.

The keys. A chord acts on the line when the cursor is there and on the note under the cursor
otherwise. Ctrl-D clears or deletes; Ctrl-V shows archived or archives, which ticks a task and is
named `tick` there; the line's right-hand end says `archived shown` while they are. Enter writes the
line or opens a new row under the row; Backspace on an empty head takes the row away; Ctrl-O opens
the note in your editor, which is how a body is written here, and an editor that cannot be run is
said on the status row and the app stays (see "The editor's file is yours alone"). Tab goes into the
note under the cursor, and on the line into what the line names, wherever the caret is; Right and
Left are the caret's, and Right at the end of the text does nothing. Shift-Tab is not bound, and
Ctrl-I, which a terminal sends as Tab, carries no chord. Up and Down move the cursor, PageUp and
PageDown by ten; Ctrl-Up and Ctrl-Down move the note under the cursor a row, the cursor with it.
Ctrl-U and Ctrl-R undo and redo, Ctrl-L links or unlinks the note under the cursor, Ctrl-C copies
its first URL (or its head), Ctrl-_ opens the URL, Ctrl-B shows scores. The line and the head take
the usual editing keys: Home and End or Ctrl-A and Ctrl-E, Delete, Ctrl-W. A paste goes into the
line or the head at the caret as one change, its tabs and line breaks made spaces and the breaks at
its ends dropped. Escape comes back a rung at a time: off the row, writing out its date words, off
the text on the line, out of the place, and twice on an empty line outside one quits, the first
saying `Escape again to quit`. Ctrl-Q quits from anywhere, writing out the head first.

The screen is flat: no borders, no boxes, the terminal's own colours. The path is a dim row
above the line and only in a place, headed by the space's name, ` main › Boiler › #house `, the
middle of a long path given up to `…`; at the top there is no path row, and the app sets no
window title. The line is the
first row, after a dim `>` in the column the rows leave empty, so that its text lines up with
theirs; what its date words and spans stand for is shown after it, dimmed, `→ Fri 2026-10-16` or
`→ Mon 2026-10-12 18:00 – now`. The rows follow the line directly, each a head and then, dimmed,
a `¶` if it has a body, the names of what it links to after an arrow, and its reasons joined by
`·`; the row the cursor is on has the terminal's cursor at the end of its head, the head in bold,
and its date at the right. The foot of the list is kept for the body of what the cursor is on:
the note on its row, and on the line in a place the note you are in. It is a row of nothing, then
up to five rows of the body, dimmed, set in two columns from the rows' text and cut with a `…`
when it goes on. The rows are kept whatever the note holds, on a row and on the line in a place
alike, so walking the list never changes how much of it is shown; on the line outside a place
the list has the whole height. On a short screen they shrink, keeping the list at least half its
room, and go when fewer than two would be left. One dim footer row names the chords that apply
where the cursor is, `^V tick`, `Tab go in`, ending with Escape's next rung, `Esc clear`,
`Esc back` or `Esc up`, or `Esc Esc quits` where there is none; a narrow terminal loses chords
from the middle, never the first or the way back. A status row above it exists only while there
is something to say, each key clearing what the one before said, and where the body has its rows
it takes the last of them, so that the list keeps its height. Nothing says how many notes there
are or that sync is working; `not syncing` is said in the corner of the foot when it is not, and
in its place, while syncing, how far this machine's clock is from the relay's when that is more
than two minutes, `this device's clock is 5 min ahead`.

Not yet reinstated: Ctrl-S multi-select with Enter zooming on what the selection shares, `!`
inverse, Ctrl-^ export.

### Browser
The engine's `Device` compiled to wasm32 via wasm-bindgen, with a page of plain JavaScript and
CSS to drive it: no framework, and one family of dependencies, CodeMirror and its parser Lezer,
for the body's editor and the page's reading of markdown, built by hand into committed files
(see "The body is a CodeMirror editor, loaded when a body first opens" and "A body's markdown is
rendered where it is read and styled where it is written"). Everything crosses to the engine as
text: an action goes in as JSON and what to do and what to show come back as JSON, a hint's
action being exactly what to send back to ask for it. A tab is one more device. It has a writer
id and a log of its own for each space, keeps every log of the space in IndexedDB between
visits, asks for the passphrase once and keeps the key, and syncs with the relay it was loaded
from, so there is one address to open and nothing to set up. The relay carries the page, built
into fzn from the `web` directory; `--web web` points it at the directory instead while the page
is worked on. Every answer the relay gives for the page names its build, in `Fzn-Build`, and
`index.html` carries it in `<meta name="fzn-build">`, so a tab knows which page it runs and can
ask with one `HEAD` which the relay now serves.

*The page.* One column, a little left of centre, with a third of the room beside it on its left,
text left-aligned, and the line a little above a third of the way down, where it stays: it does
not move when you type or when results arrive. A faint hairline under the line sets it apart
from the rows. The blank page is the caret and, under it in faint grey, the heads at the top of
the list fading out by the eighth. As you type, what is found takes their place; only a row that
is new fades in, and nothing else moves. The path, in a place, is in the margin above the line,
`fzn › Boiler`, each step a way back to it, and on a phone a long path gives up its middle. No
colour: paper, ink and two greys, light and dark by the system's preference. On a page at least
75rem wide the body of what the cursor is on is read beside the list: the note under the cursor,
or with the cursor on the line in a place the note you are in. It is rendered as markdown, in
the soft grey, from the top of the first row's text, with no border, scrolled on its own and
faded at its foot as the list is. A narrower page has no room for it: a note's body is read
there by opening it, and the note you are in is read above the line, rendered, under the path
and in the path's smaller size, clamped to six lines and opened with a tap, so that where you
are is all above the line and what is found follows it directly. The margin grows downwards only
for a body longer than it has room for.

*Markdown.* A body is rendered where it is only read and styled where it is written; the note
stays plain text, and neither the engine nor the terminal reads markdown. Read, a body shows
headings, paragraphs keeping the lines they were written on, lists, tasks as a drawn box, ticked
and faint when done and never pressed, quotes, rules, tables with their columns aligned, code
highlighted by its language, and in a line emphasis, strong text, code, strikethrough and
links, built as nodes and never as markup. A link goes only to `http:`, `https:` or `mailto:`,
in a tab of its own and with no referrer; any other address, a link by reference, raw HTML, an
entity such as `&amp;`, and `[[Boiler]]`, read whole as fzn's link, are shown as typed, and an
image is never loaded but is a link reading its alt text. A row's head reads as one line of
markdown, only as far as a list bears: a link shows its text, a bare `http(s)` address is a
link, a leading heading mark is dropped, and every other mark stays as typed; the row under the
cursor is its text box and shows the source. Highlighting is in paper, ink and the two greys:
keywords and definitions heavier, strings, numbers and constants soft, comments faint and
slanted, punctuation and a diff's removed lines faint; code read is set in ink, and a link is in
the colour of what it is in, with a faint underline. A long body is rendered at first only as
far as it shows, its first 5,000 letters, and whole once the page is idle; a head with no colon
is not parsed; nested past what the stack holds, a body is read as typed.

*Keys.* The four that are the app are the same: type, Enter, Escape and the arrows. The
terminal's chords are not here, because a browser keeps most of them for itself, but two it
lets go: Ctrl-D clears the line or deletes the note under the cursor, as in the terminal, and
inside the sheet it is the browser's, and vim's in vim mode; Ctrl-O opens the body of the note
under the cursor, as in the terminal, and does nothing on the line. Ctrl with an arrow moves the
note under the cursor a row, as in the terminal, and on a phone `move up` and `move down` in the
foot do it. PageUp and PageDown move the cursor by ten. Backspace on an empty head takes the row
away, as in the terminal. Right at the end of the line, with nothing selected and Shift not
held, goes into what the line names, as Right at the end of a head goes into the note; Escape on
the line clears it while it has text, and on an empty line in a place steps back. Every other
thing that can be asked for is a word at the foot of the page, the footer of the terminal made
pressable, and Tab reaches them from a row without leaving it. Only what applies where the
cursor is gets one, and no key is written beside a word; a word says its key when the pointer
rests on it. Undo has no key: it is the word `undo`, or `put back` on a row whose head has
changed.

*The line* is an ordinary text field, so the phone's keyboard and the browser's own editing
work in it. A cross after it clears it when it has text, and a `›` after the cross, in the
column of the selected row's, goes into what it names when it names somewhere. Each keeps its
place while it is not there, so that neither moves the other, or the dates, by coming or going;
and since they are on the line, clearing and going in are not also words in the foot.

*Writing in a row.* The head of the row the cursor is on is a text box, and typing changes it;
Enter opens a new row under it; Ctrl-O, or `body` in the foot, opens the whole note in the sheet,
a bounded overlay over the column with the page dimmed slightly behind it, which scrolls on its
own, and in it Enter is a line break and the arrows move within it, and Esc closes it back onto
the head. Leaving it for anywhere else on the page closes it, and a press on another row goes
there; a press in its own margin, or on its row behind it, does not, and nor does leaving the
window, though the tab going out of sight writes what it holds. The words in the foot stay above
the dimming, since they are about the note. There is no second editor: the sheet is the body's
editor, for a long document too, and with it shut the head is written in its row. While the body
is open the sheet holds a CodeMirror editor, loaded the first time one opens and fetched under
idle time once the cursor is on a row: the markdown styled where it stands with every mark in
sight, the marks in the faint grey and the text in ink, headings and strong text heavier,
emphasis slanted, code in the monospace, soft wrap, step undo, a grey selection, and vim mode for
the browser that asked for it with the word `vim` in the foot. In vim mode Esc is vim's alone;
`:q`, `:wq`, `:x`, `ZZ` or the word in the foot closes the body, `:q!` and `ZQ` put the text back
first, and `:w` writes and stays, scrolled where it was. The page's caret is drawn from the
editor's coordinates, a block in normal and visual mode and a bar in insert mode. Vim's prompt
for a command is at the foot of the sheet, under the text and a hairline. The head stays an
ordinary field. Until the editor has loaded, and if it never does, the sheet holds a plain text
area. The `editor` the engine offers is not shown here.

*Touch.* A tap puts the cursor on a note, a second tap on it off its head opens its body (the
head is a text box by then, and a tap in it places the caret), a tap on the `›` at the end of
the selected row goes into the note and on the one after the line into what the line names, a
tap on a link in a row opens it and leaves the cursor where it was, and the path is the way back.

*One tab at a time* is the device. A tab that opens says so over a broadcast channel and the
older one stands down, closing its socket and offering `use it here`, since two of them writing
one log would fork it.

Not yet: the page itself needs the relay to load, the graph view, and anything a browser only
allows a page served over TLS, which a relay does not do.

## Decisions
A dated log, oldest first. Each entry says what was decided, why, and what it replaced; a
reversal is its own entry that names what it replaces, and the old reasoning stays, because why
something was dropped is part of the design. What the app now does is described above; an entry
repeats it only where the reason needs it.

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
  tags, people, links. No note contains another. The old ` >> ` convention imports as plain text
  and is found by the lexical scorer.
- **No manual ordering (provisional).** The list was to be recency plus pins, so the position
  CRDT went, pending Sam's confirmation. Reversed 2026-10-15 by "Where a note sits is kept".
- **A link is a fact, not a score, and there is one kind.** Every link is the target's id held
  on the note, however it was made. Linked notes are listed ahead of suggestions, and no weight
  can bury one.
- **Position is the link.** Writing inside a note's place is how one note comes to be about
  another; nothing is typed and no key is pressed for it. The explicit chord is a correction.
  There is no marking of links while drafting: to put a draft somewhere, go there and write it,
  and the line comes with you. A Tab-to-link was considered and dropped, because it was one more
  thing to remember for a flow position already covers.
- **The engine sees only what the text carries, plus where you were.** Words, tags, dates and
  position are the free signals. The paths you walk between notes are the next one: going from A
  to B again and again is a relation expressed without saying so, and a scorer that reads it can
  still give its reason ("you came here from A"). Embeddings come after that, if ever. Neither
  before daily use shows the gap.
- **Archived is hidden until asked for.** Everywhere, typed results included. The engine still
  indexes archived notes; the lists leave them out.
- **Spaces, not one pile.** Notes that do not belong together live in separate spaces, and one
  of them is home.
- **Days are local.** A note's day is the day it was where it was written, kept by the offset
  in its times, and today is this machine's today. The offset comes from the system through
  one libc call, the only unsafe code in the app.
- **A name is a fact too.** Decided 2026-10-13. The note whose head is what you typed comes
  first, whatever it scores and whatever the limit, and says `its name`: every word of its head
  typed and no other, in any order and case, the last word allowed to be unfinished, so
  `boiler se` puts Boiler Service first and `boiler` alone does not. The scores could not be
  made to do this. In `main` the median note is 23 words and the longest is 7,493, so a document
  earns almost nothing for its own words and the notes that say its name outrank it: one
  document was fifth for its own name and another note eighth. Three settings of `bm25_b` across
  five names got none of them all right, the wins were by hundredths, and with no length penalty
  long documents turn up for searches that are not about them. A rule is also what lets "type
  the name, Down, Right" be done without reading the list. Notes that share a head all come
  first, in the order of their scores.
- **A name finds what was written in it.** Decided 2026-10-13. Typing `boiler` lists Boiler
  and the notes written in it, each saying `about Boiler`, and `boiler quoted` finds the one
  among them that says "quoted": typing a note's name shows what going into it shows, and the
  names after the arrow on a row count as words of that row. In 2022 this was the main way of
  finding: 85% of notes carried a ` >> ` path, most of them three to six labels deep, and any
  label gathered its group. Position now makes the link without the typing, so without this
  every note written in place would be out of reach of the word it was filed under, and a link
  would be a folder that has to be opened. The name is read when the search is made and not
  copied onto the note, so a rename is followed and the text stays as written. It goes one hop:
  `house` finds Boiler and not what was written in Boiler, which is what going into House shows
  and what the row says. A neighbourhood is not widened this way; what is linked is listed
  there already. The carry rule reads the same names, or a note found by one would take the
  search in with it as a draft. Measured on the 2022 notes with a hub made for every first
  label: up to a fifth more time per keystroke, and every keystroke under 3 ms.
- **The app is headless.** Decided 2026-10-13. Until then the engine was the part that finds
  things, and the app was the terminal: the carry rule, the path, what Escape does and what the
  footer offers were written against one terminal library's keys and widgets. A second face
  would have copied all of it and the copies would have drifted. Now the session is the app and
  a face cannot reach around it. Four calls shaped the interface. *Actions, not keys*: the
  terminal's chords cannot be a browser's, and touch has none, so the session is never told
  which key was pressed; "a chord acts on the line or on the selected note" is the terminal's
  way of saving keys and lives in its key table, and to the session clearing the line and
  deleting a note are two different things to ask for. *The face edits the line*: a terminal
  has to move a cursor through a string itself, and a browser is given a text field that
  already does, with the phone's keyboard and its autocorrect, so the session is told what the
  line now reads and nothing finer. *Effects out, nothing done*: a version to keep comes out as
  data and the face decides where it goes, so the session needs no file in a terminal and no
  database in a tab; the editor, the clipboard and opening a URL are asked for the same way.
  *The time is the host's*, and so are new ids, so a test stops the clock and nothing in the
  tests depends on when they run. Checked by running the binaries from before and after with
  the same 55 keys on a copy of `main`: the screens matched but for one reworded message, and so
  did the lines written.
- **A date is text, and `/fri` is a way of typing one.** Decided 2026-10-13, replacing `{d}`.
  The old token only wrote today, and that is what it was used for in the 2022 corpus. A note now records the day
  it was written, so the mechanism is for the days that are not today. There is no `/d`, which
  would be a code to remember. The word becomes the date when the note is saved and not as it is
  typed, so no key does anything unusual and the editor works the same way. There is no general
  slash-command system: one trigger, one job, until a second expansion is wanted.
- **Imported pages become the notes they would have been.** A page named only for a date or a
  time, a journal page say, is not a note: each thing written on it becomes a note of that day.
  A page that is nothing but a list becomes a note, with each item a note about it. Any other
  page is a document and stays whole, headed by its name. On the first two kinds a task is a
  note at any depth, headed by its box, archived if it was ticked and linked to the note it sat
  under (the box was taken off until 2026-10-16: see "A task is a note headed by its box");
  other nested lines are the body; a bullet holding only tags hands them down. A note carries
  the tags in force where it was written, on the line under its head, and the tags its own line
  started with go there too: a head is what was said, not how it was filed. A line of nothing
  but tags does not count as a body in the list. Folders and front-matter tags are tags, except
  a folder that only holds dates, which is a calendar and not a subject. A `[[page]]` is
  rewritten to the head its page has now, or to the date if the page was a day. Pages that are
  programs for Silverbullet are left out. Importing again brings a note up to date with its
  page unless its text was changed here since, and keeps what was done to it here: its links,
  whether it is archived, that it was deleted, and where it sits.
- **A space is its logs, and sync is the rest of each.** Decided 2026-10-13. The other way is
  for the relay to keep one journal in the order things arrive and each device a cursor into
  it. That is less to build, but the cursor is state beside the data: lose the relay's journal,
  or put a device back from a backup, and the two no longer agree about what was sent. Here
  there is nothing beside the data to disagree about. It also makes every replica the same, so
  two devices can sync with no relay between them, and the relay has no logic of its own to get
  wrong. The cost is a rule that has to hold, one writer to a log, and the digest is there to
  notice when it does not. Merging is per note for now: two edits of one note made apart leave
  the newer one showing and the other in the log.
- **Sync is part of opening the app.** Decided 2026-10-13, at Sam's suggestion, over an
  `fzn sync` to be run beside it. A second thing to run is a second thing to remember, and one
  that fails without being seen. What it cost was two things to get right: only one of two open
  windows may sync, and a device syncs only while the app is open, which is when it is read and
  written anyway. The relay is one command for the same reason, and the same binary.
- **The page crosses to the engine as text.** Decided 2026-10-13. An action goes in as JSON and
  the view comes back as JSON, with a hint's action being exactly what to send back to ask for
  it, so the buttons at the foot of the page are the view's hints and nothing more. The
  alternative was a typed binding for every part of the view. Text keeps the boundary to a
  handful of calls (seven then, a dozen now), makes the page readable without the engine's
  types, and costs a parse that is far below a frame.
- **The relay hands out the page.** Decided 2026-10-13. A page loaded from the relay knows
  where its relay is, is covered by whatever lets a device reach the relay, and needs no second
  server. The page is a directory the relay is pointed at, not something built into the binary,
  so that changing how the page looks is a reload. Revised 2026-10-16 by "The relay carries the
  page; a directory stands in for it while the page is worked on".
- **Dumb relay, self-hosted, before the browser.** Decided 2026-10-13, reversing the order. A
  tab needs its notes from somewhere, and a tab is one more device, so what syncs two machines
  is also what gives a browser its notes.
- **Same repository.** The Go implementation is at the tag `go-final` and kept reachable; the
  Rust workspace took over the root on 2026-10-14. History is part of the project.
- **Compaction is a new log, not a rewritten one.** Decided 2026-10-14. Rewriting a log in
  place is a fork by the rule above, and any message saying "I rewrote it" is state a relay can
  miss. A new log under a new id, with a header naming the old ones, is read the same by a
  replica that was there and one that was not, and the retirement of the old logs follows from
  what is held. It also makes fork repair the same move, and gives the text CRDT the
  snapshot-and-go-on it will need. What it costs is the one line a replica reads.
- **The engine owns sync.** Decided 2026-10-14, at Sam's direction, replacing the files as the
  meeting point. The files bought Syncthing and a second window at the price of two readers of
  one directory, a lock on every write, and a "stale, start again" path. Neither is wanted: the
  relay is the way between machines, and a second window on one space is told to use the first.
  `Device` had already been written for the browser, where there are no files; it was the
  general thing all along, so it moved into the engine and the terminal drives it too. Revised
  2026-10-16 by "A command writes beside the app, as a writer of its own": the app does read the
  directory while it runs after all, for logs it does not write, still without a lock per write.
- **Sealed on the device, from now.** Decided 2026-10-14, at Sam's direction. The lines were
  already opaque to the relay by design; sealing them costs a key and a passphrase prompt a
  device, and buys a relay that can sit anywhere. A passphrase, derived with Argon2id and a salt
  from the space's name, was chosen over a generated key copied between devices because a phone
  has a keyboard and not a clipboard from the terminal. There is no auth and no TLS yet, by the
  same decision: the key is what protects the notes, and the address is what protects the
  relay's disk and time.
- **Per-note last-writer-wins stays, for now.** Revisited 2026-10-14 when asked whether the
  CRDT could be made more robust. The stamp is already a per-note logical clock (a local change
  is always stamped past the version it replaces, whatever the wall clock says), and the merge
  is deterministic in any order. What is not robust is two devices editing one note apart, where
  the newer whole text wins; that is the text CRDT, which is milestone 4 and which compaction
  and sealing were shaped not to obstruct. Pulling Yrs or Loro in now would be the dependency
  decision Sam deferred, made in passing. The clock stepped a second at a time until 2026-10-15
  (see "Stamps are kept to the millisecond").
- **Enter edits, the arrow goes in.** Decided 2026-10-14, at Sam's direction, replacing Enter as
  the zoom key. Enter on a row had gone into its neighbourhood, and editing a note meant the
  external editor. Now Enter opens the row to write in it, which is what Enter means in every
  spreadsheet, and the zoom is Right arrow: unmodified, universal in a terminal and a browser,
  and it walks the path the way the path is drawn. Sam had suggested a modifier with Z. Ctrl-Z is
  the browser's undo and forty years of suspend in a terminal; Alt-Z is reliable but carries no
  meaning and is the fallback if the arrow is refused; Shift-Enter and Ctrl-Enter are plain
  Enter in most terminals without the kitty keyboard protocol, so a key that works on one
  machine and not the next; Tab is how a page moves focus and what search groups want back.
  Enter now means four things by state, each visible, each named in the footer, and nothing
  about it depends on the key before it. Esc comes back a step, as before; Left does nothing on
  a row, so that there is one back key. Revised the same day, twice: see the next two entries.
- **Enter opens the body; Right goes in from the end of the head.** Decided 2026-10-14, at
  Sam's direction, after a day's use, revising the entry above. Enter opens the body, since the
  head needs no key to be edited (see "The head is a field whenever the cursor is on it"), and
  Right goes in from the end of the head, where the caret lands, since Right short of the end is
  the caret. The reasoning on Ctrl-Z, Alt-Z, Shift-Enter and Tab stands, and Alt-Z is still the
  fallback. Replaced later the same day by the next entry.
- **Enter opens a row under; the body is Ctrl-O.** Decided 2026-10-14, at Sam's direction,
  later the same day, replacing the entry above. Enter on a row opens a new row under it, which
  is what the original's Enter did and the quickest way to write several notes in a run, and the
  body is Ctrl-O in both faces, the original's chord for exactly this. Two things decided it.
  Enter as the body had proved invisible in the browser: the opened note looked like the row
  with its body shown under it, so a day's use found no way to open a note. And a draft's Enter
  had been left to use to settle between the editor and another row; the original's hands wanted
  the row. The browser lets Ctrl-O go, as it lets Ctrl-D go, so the two faces keep one key for
  the body. The earlier argument that Ctrl-O was a second binding inherited rather than earned
  is answered: it is now the only binding for the body, and Enter has a better claim on the row.
  What stands: Right at the end of the head goes in, Esc comes back a step, Left does nothing on
  a row.
- **A note is written in place.** Decided 2026-10-14, at Sam's direction, which is what the
  original did and the revival had put off. Three calls shaped it. *Select, then edit*, not the
  original's one text cursor over the whole screen: typing on a row still went to the line, so
  browsing was never editing and Down then type still narrowed; the edit cost one Enter more.
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
  the two text boxes with two Enters. The contention this brings with two devices editing one
  note at once is known and not for now. The first and second calls were replaced the same day
  (see "The head is a field whenever the cursor is on it" and "A head is kept as it is typed").
- **The head is a field whenever the cursor is on it.** Decided 2026-10-14, at Sam's direction,
  replacing "select, then edit" in the entry above, as in the original. Select-then-edit cost
  one Enter per fix and put a state between the cursor and the text; a day's use found the Enter
  in the way and the state a surprise when Down left a multi-line note instead of moving within
  it. Browsing is editing, and a stray key is one undo. What stands: a version when the cursor
  leaves the row or the body closes, once, if the text changed; a new note not written until it
  has text; Esc keeps; the session holds the field. What goes: the open and closed states of a
  row, Ctrl-O (Enter was then the editor's key, and a second binding for one action is
  inherited, not earned), and the gutter mark, since the caret itself is in the head. Undo while
  on a row puts a changed head back and otherwise is undo, one key for the last change written
  or not. The list holds still while the cursor walks it, so that the arrow that writes a head
  lands where it was aimed.
- **A new row opens under any row.** Decided 2026-10-14, with "Enter opens a row under; the body
  is Ctrl-O": not only at the top of the list, and an empty one goes from wherever it is when
  the cursor leaves it, the arrow landing past it as if the row had not been there. It takes
  nothing from the line, where the original prepended the search text to the new line: a place
  links what is written in it, which is the revival's way of saying what a note is about, and a
  search is not a place.
- **Backspace on an empty row takes it away.** Decided 2026-10-14, at Sam's direction. It steps
  up, to the row above or onto the line. It is what Backspace does to an empty line everywhere,
  and it is the quickest way back out of a run of new rows that went one too far. A note whose
  head is empty but which has a body stays, with a status saying why: the body is not shown in
  the head, and deleting it with the key being held down to clear a head would be a surprise.
  Ctrl-D is the key that deletes a note whatever it holds.
- **Enter with text on the line takes the cursor to the note it wrote.** Decided 2026-10-14, at
  Sam's direction, as the original's did, the caret at the end of its head. The note just
  written is the one most likely to be gone on with, and Enter there opens a row under it, so a
  run of notes starts from the line as it does from any row; left on the line, the cursor had
  nothing to say where the note went.
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
  of the selected row, its head bold, its body under it (moved to the foot of the list on
  2026-10-15, see "A body is read apart from its row"), and one dim row of chords, in the
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
  notes is compacted when the device opens. Three things keep it from writing what nobody meant:
  date words are written out when the cursor leaves the row, so `/f` on the way to `/fri` is not
  made a date mid-word; a head cleared to be typed again is not written while it is empty, and
  a new row typed into and cleared goes as an untouched one does; and undo is still one entry a
  visit, the versions written while the cursor is on a row gathered into one. The body is not
  kept as it is typed: it is kept when it closes, or on `:w`, as the terminal's editor keeps a
  note when it exits, since a version is the whole note and a keystroke in a long body would
  write all of it. The status says nothing as a head is kept; the row's date appearing is the
  sign.
- **The body is a CodeMirror editor, loaded when a body first opens.** Decided 2026-10-14, at
  Sam's direction, revising the Browser description that the page is plain JavaScript and CSS
  "with no framework and no build step but the engine's". The body had been a textarea, which is
  what the no-dependency rule bought: nothing to build, nothing to vendor, and the phone's own
  editing. What it could not give was an editor: no vim mode, no step undo worth the name across
  a long note, and a caret the page had to find through a mirror of the text. The body is now
  CodeMirror 6, the three packages that are the editor and the vim mode, about 409 KB minified
  in one file under `web/vendor/`, built by hand from a pinned lockfile in `web/editor/` and
  committed, so that the relay, its machine and CI see static files and nothing is built on the
  way to the page. It is imported the first time a body opens and never before, and the textarea
  remains the body until it arrives and if it does not. The note stays plain text: no document
  model, and at first no markdown grammar (measured at 85 KB for syntax the notes did not use,
  and it would read `#house` as a heading), nothing rendered, and marking the engine's own
  syntax in place left until use asks; that part was revised 2026-10-15 by "A body's markdown is
  rendered where it is read and styled where it is written". The head field and the line stay
  ordinary inputs. The page's caret stays the page's, drawn from the editor's coordinates, and
  its shape says the mode, as a terminal shows vim's. Vim mode is this browser's choice, off by
  default. Esc kept its meaning at first: in normal mode the editor let it through and the page
  closed the body, so from insert mode it was Esc, Esc (revised the same day, see the next
  entry). `:w` writes and stays, as it does in vim, by closing and reopening the body under the
  hands with the caret and history kept, and shown as the engine keeps it, so a date word is
  expanded then. The relay's page policy allows inline styles for this, since the editor's own
  stylesheet is injected; scripts are still the page's own only. The other editors measured:
  Monaco at 4.6 MB and unsupported on phones; ProseMirror, TipTap, Milkdown, Lexical and Quill
  as document editors whose round trip through a schema changes plain text; Ace as a code
  canvas. What stands from the Browser description: no framework, the page as a directory the
  relay is pointed at, the line and the head as ordinary text fields, and the four keys.
- **In vim mode Esc is vim's alone.** Decided 2026-10-14, at Sam's direction, revising the entry
  above: Esc never closes the body in vim mode. Hands that press Esc twice to be sure of normal
  mode closed the note under themselves. The body closes with `:q`, `:wq`, `:x` or `ZZ`, with
  `:q!` or `ZQ` putting the text back first, with the word in the foot, or by leaving it for
  somewhere else on the page.
- **A tag is a place you can go into.** Decided 2026-10-14, at Sam's direction. A tag, a person
  or a day typed on the line is gone into with the arrow at the end of the line, as a note is
  gone into from the end of its head, and the context is then the label: every note carrying it
  came first, newest on top, then what the engine found for its words, and a note written there
  ends its head with it. It closed two open questions. The first: was it a loss that a note
  added from a search for `#house` was plain and fell out of the list when the search ran again?
  It was. The original's search line was a standing context, and the prefix it put on every new
  line was what kept such a note in its list; going into `#house` is that standing context with
  the typing done once. The second: Right was dead at the end of the line. It now goes into what
  the line names, and does nothing when the line names nothing, so the key that walks the path
  steps onto it from the line too. A note is gone into by its name with the same key, by the
  rule in "A name is a fact too", which at the top is where Down and then Right would go, and
  inside a place that note is listed first whatever the neighbourhood and the limit, so the list
  and the arrow agree and typing a name is the way from any context to any note. Either way the
  line was a search for the place, so it clears on the way in and comes back to an empty line on
  the way out. What it reverses is "Escape never throws away what you typed", for the line:
  Escape with text on the line now clears it, and only on an empty line steps back. Three
  reasons: inside a standing context the common case is "found it, clear, stay", not "take this
  draft out"; Ctrl-D already threw the line away, so the line had no protection to lose; and a
  line costs a line to retype. That takes away one of the two reasons "A note is written in
  place" gave for Esc keeping what was typed on a row, that keeping was already the line's rule;
  the other, that an editor's hands press Esc to finish, stands, and on a row Esc still keeps.
  The step back itself still keeps what is on the line. Ctrl-D on the line is now a second
  binding for what Escape does while the line has text, kept for the hands that have it;
  dropping it is Sam's to call. The label lands at the end of the head, as it would have been
  typed, and not on the line under the head where the importer files tags, because the note then
  reads exactly as one written with the tag on the line has always read; a note that carries the
  label already, in its text or for a day by being written on it, is left as it is. Nothing is
  linked in a label, so Ctrl-L there does nothing but say so. Remembering the path across
  restarts is left out: the summon key in the open questions is the answer to coming back to
  where you were. A plain-word search as a context is left out, because it could only hand down
  the old prefix, the same words written on every line. Four calls here were replaced the next
  day, each by its own entry: the label added when the cursor left the row ("A new row in a
  label opens with the label"); one label at a time, and a note on the path starting afresh
  ("Labels gone into one after another are one place", "A place is the last note on the path
  and the labels after it"); and the list on an empty line being the carriers and then what the
  words find ("A place lists what is in it; typing finds").
- **A new row in a label opens with the label.** Decided 2026-10-15, agreed with Sam, replacing
  the call in "A tag is a place you can go into" that a note written in place gets the label
  when the cursor leaves its row. A head is kept as it is typed, so the note was on every other
  device from its first letter, but without the label until Enter, an arrow or Escape left the
  row: until then every other device held `Bleed the radiators` with no `#house`, and its own
  `#house` did not list it. A label added on leaving reached other devices late, and a note's
  link was never late, since it is in the note's first version. Now the row reads ` #house` from
  the moment it opens, and the first letter writes `B #house`, so the note is in its place on
  every device at once and what the writer sees is exactly what is written. The label is text
  like any other: the writer can see where the note will go, and can delete it, and then it
  stays out. The caret lands at the start and not the end, so that what is typed goes before
  the label. A day is put in only when it is not the day the note is written on, since that day
  is its `created`. A note written from the line still has the label added as it is written,
  since the line was the search and that is its one version. Taken out a letter at a time from
  its end, as Backspace does and as a phone's keyboard only can, what is left of the label is no
  more the writer's than the label was, so nothing is written until a letter that is. What it
  costs: a new row holding only the start of its label, `#ho` in `#house`, is empty too and goes
  when it is left, and the label taken out from its start writes each step, as typing does.
  Among the open tasks, since 2026-10-16, the box comes first and the caret after it, and how
  the labels are spaced does not count (see "A task is a note headed by its box").
- **Labels gone into one after another are one place.** Decided 2026-10-15, agreed with Sam,
  replacing "one label at a time" and "only from the innermost place" in "A tag is a place you
  can go into", for labels. Going into today from inside `#house` reads as today's notes in that
  tag, and it was not: the list was today's notes, and a note written there carried today by
  being written on it and got no `#house`, so it fell out of the tag without a word. Now the
  labels at the end of the path, back to the nearest note on it or its start, are taken
  together: the facts are the notes that carry every one of them, what the engine finds besides
  and the narrowing by the line are for them all, and a note written there inherits each label
  it does not carry already, in the order of the path, at the end of its head as one label
  always was. The path, going in, going back a step at a time, and going back to a place already
  on it are as they were; nothing about the run is kept but the path itself. A note on the path
  still started afresh here, never taken together with anything, so that in
  `#house › Boiler › today` only today counted; that was replaced the same day by "A place is
  the last note on the path and the labels after it".
- **Where a note sits is kept.** Decided 2026-10-15, at Sam's direction, reversing "No manual
  ordering (provisional)". The empty line's list was in the order of `updated`, and so were a
  place's facts, and that had two faults. A note written under a row stayed there only on the
  device that wrote it, and only until the cursor came back to the line: everywhere else, and
  then here too, it went to the top, so Enter, type, Enter, type wrote a run that did not stay
  where it was written. And an old note edited went to the top, so that putting a word right
  reordered the list. The 2022 corpus says the same from the other side: its order was kept by
  hand, because when a note was written was not what its place meant. So
  where a note sits is stored and replicated, as it was in the original fzn, whose tree CRDT
  comes back as one field with a clock of its own: `after` and `placed`, merged side by side
  with the text as the original kept content and position in separate registers (the mechanics
  are under Storage and sync). The empty line's list is the top of the order and an edit never
  moves a note, Sam's choice over a list of what was recently written beside it. A move is the
  original's, one note alone, and what the line finds is in the order of its scores, so there
  nothing is moved and nothing is offered. The line format went to version 2, so that a build
  that knows only version 1 leaves these lines alone rather than rewriting a note without its
  place. `placed` was kept to the second here, and a note placed in the same second as another a
  second past it; later the same day it went to the millisecond (see "Stamps are kept to the
  millisecond"). The pinned flag the model once listed was never built, and moving a note to the
  top is what it was for. The order is walked afresh each time the list is made, O(n log n) in
  the notes: 5 to 7 ms over 30,000 notes in a release build, against under 1 ms for the sort by
  `updated` it replaces, so nothing is cached.
- **Stamps are kept to the millisecond.** Decided 2026-10-15, at Sam's direction, revising
  "Per-note last-writer-wins stays, for now" in what its logical clock steps by, the times in
  the line format under Storage and sync, and `placed` in "Where a note sits is kept"; the line
  format goes from version 2 to 3. A version's `stamp`, which decides whose text wins, and its
  `placed`, which decides whose place wins, are a moment past what is held for the note, or the
  clock if that is later. Moments counted whole seconds, so the step was a second, and since "A
  head is kept as it is typed" a head is a version a keystroke: k keys a second for T seconds
  put the note's stamp about (k−1)·T seconds ahead of the clock, half a minute for a head of
  forty characters typed at speed, and inside that window another device's edit of the same
  note, made later without having seen this one, lost. Now a moment is unix milliseconds with
  its offset, the step is one, and both faces read the clock to the millisecond, the terminal
  from the system and the page from `Date.now()`, so a burst runs ahead by a millisecond a key
  at most. A line from before reads as it did, its times on the second, and two such versions in
  one second are still decided by writer id; days, and every time shown, are unchanged. The
  format went to 3 because a build of version 2 reads the fraction and drops it, so two versions
  of one note in one second, told apart here by their milliseconds, would be a tie there decided
  by writer id, and two devices would hold different text for the same note; by the rule under
  Storage and sync a build that cannot read a line as it was meant leaves it alone instead. So
  every binary moves together, as it did for version 2. The relay passes lines on unread and
  does not care. The textbook form of this is a hybrid logical clock, wall time with a counter
  beside it that breaks ties within a tick and carries causality past a clock that is behind;
  this stamp already steps past what it has seen, which is the counter's work, and milliseconds
  take most of the rest of the gain (a step small enough that typing no longer outruns the
  clock) for the least change, one field of one type and no new field in the line. A clock that
  is simply wrong, by minutes or hours, still wins or loses by that much; that is said, not
  corrected (see the next entry).
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
  before they are kept" in what proves a key. Storage and sync said a line with a later version
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
  in your order with no reasons, and typing finds, ranked and with reasons; facts come before
  finds, where before they were ranked together, and the row of nothing both faces drew between
  facts and suggestions is drawn where facts and finds now meet. A fact carries no scorer's
  reason, as a row at the top carries none; how it is linked is a fact, not a score, and stays.
  Three reasons. The Vision ties finding to typing and going in to belonging: "as you type, the
  engine finds", and "go into one of those and write there, and what you write belongs to it".
  The top level already listed a set on an empty line and found when typed, so a place that did
  otherwise was a second rule. And a label is typed on purpose, so a place is exactly what a
  note written there joins, and its list should be that and no more. What it costs: going into
  a note no longer shows the notes related to it until something is typed, and a note that says
  a tag's word without carrying the tag, `house move plan` in `#house`, shows only when typing.
  Since every row on an empty line is now a fact, all of them can be moved, and Ctrl-L there can
  only unlink; a note to link is one the line found.
- **A place is the last note on the path and the labels after it.** Decided 2026-10-15, agreed
  with Sam, replacing "a note on the path starts afresh … in `#house › Boiler › today` only today
  counts" in "Labels gone into one after another are one place". A note on the path was never
  taken together with anything, so in `Boiler › today` the list was all of today's notes and a
  note written there did not link to Boiler: it fell out of Boiler, the fault that "Labels gone
  into one after another are one place" put right for labels earlier the same day. Now the place
  is the last note on the path, if there is one, together with every label gone into after it,
  and the labels before the note are only how it was reached. Typing finds in the note's
  neighbourhood, with only the notes carrying the labels counted as facts, so a note linked to
  Boiler but written another day is a find in `Boiler › today`. The reasons: the same place
  reached by two routes should give the same note. What is before the note is already recorded,
  by the links that put the note where it is, so copying `#house` onto a note written in Boiler
  would be a second copy of it, and one that goes stale when a note moves. And how far up a path
  finding reaches is a question for search, which today goes one hop down from a name (see "A
  name finds what was written in it" and the open question "One hop or the whole path?"), and
  is not written onto notes. The engine is asked about every place as one thing, a note, labels
  or both, where it had one call for a note and another for labels.
- **A body is read apart from its row.** Decided 2026-10-15, at Sam's direction, replacing the
  body shown under the row the cursor is on in both faces: in the terminal "its body under it,
  clamped to six rows" (the Terminal paragraph and "One cursor, no chrome"), and in the browser
  "the note under the cursor is read under its row, clamped to six lines". It also replaces "no
  panels" in the Principles' "One cursor", and on the page "There is no panel beside the list".
  A row that expanded the note under it as the arrows went up and down the list was hard to follow: every row below moved down and back at each step, a note with a body
  looked like several rows, and the eye had to find the list again after each key. Now the list
  is one row a note wherever the cursor is, and the body is read in a place of its own: in the
  terminal the foot of the list, set in two columns from the rows' text so that it does not
  read as rows, and kept whenever the cursor is on a row so that walking the list never changes
  how many rows it shows; on a wide page beside the list, and on a phone by opening it. To leave
  the room, the column sits left of centre, a third of the space beside it on its left, which
  Sam wanted anyway. The panel dropped on 2026-10-14 was a second copy of the text and a second
  place to look, read unasked even with the cursor on the line; this one says nothing until the
  cursor is on a note and is the one place a body is read, so the reasoning stands for what it
  was about. The view's `preview`, the note that panel read, had been read by no face since,
  and went.
- **The body read is that of what the cursor is on.** Decided 2026-10-15, at Sam's direction,
  revising the entry above, where the note you are in stayed under the path, and the Terminal
  paragraph, where "in a zoom the body of the note you are in sits under the line, dimmed and
  clamped". In the terminal that body still opened between the line and the list, about seven
  dim rows ending in a `…`, which is the fault the entry above was for: a body expanded under a
  line, pushing the list down. On the page, until 2026-10-14 the note you were in was read under
  the line, and Sam found it read as part of the line and put a gap between the line and the
  first result; from then until this decision a wide page read it above the line. Now nothing is
  drawn between the line and the list: the terminal's foot reads the note you are in, with its
  rows kept as on a row so that the list keeps its height, and a wide page reads it beside the
  list and not above the line as well. And the status row, which had pushed the body and the
  list up a row whenever there was something to say, takes the last of the body's rows instead,
  so that a key that says something and the key after it, which clears it, do not change how
  much of the list is shown.
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
  is the page" in the Browser paragraph, and "the row is the editor" with it. The 2026-10-14
  reason for taking the rows away was that rows only gone faint looked the same as a row with its
  body shown, and nothing said the note had opened; a sheet, a bounded editor over the column
  about the size the body had in the page with the page dimmed behind it, says it and leaves the
  list where it was underneath. What the keys and the engine do is unchanged. The sheet stays
  scrolled where it was through `:w`, where the row had gone back to the top of the note. A
  press in its margin, or on its own row behind it, keeps it open, so that the press does not
  close it for the tap to open it again. The words in the foot stay above the dimming, since
  they are about the note. The sheet is placed from the top and given a height, not pinned to
  the foot of the screen, which iOS Safari misplaces around its keyboard (WebKit bug 297779);
  with the keyboard up a browser that honours `interactive-widget=resizes-content` shortens it.
  It is not a `<dialog>`: a modal one makes the foot inert, and its Escape cancels, which vim's
  Escape would have had to fight; it is an element with the dialog role. Vim's prompt for a
  command sits at the sheet's foot. It had floated over the text, since CodeMirror sticks its
  panels to the foot of whatever scrolls the editor, which was the page, so with a long body
  open Escape then `:` put the prompt over a line of the note; the editor is given the sheet's
  foot for its panels instead, which changed the editor's source by one extension and the
  bundle by 64 bytes.
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
  page reads (what is rendered where is under Browser). One parse and one highlighting serve
  three places. The sheet is styled source, as iA Writer writes, which Sam chose over a live
  preview: every character stays where it was typed and in sight, so nothing moves as the caret
  walks, vim's motions go over exactly what is shown, and the caret the page draws from the
  editor's coordinates needs nothing new; headings are set apart by weight and not size, so the
  page keeps its three sizes and every line is as high as the next, and a block of code, a
  tenth smaller in the monospace to match the sans by eye, keeps that height too. A body read is
  built as nodes and never as markup, and a paragraph keeps the lines it was written on, where
  markdown would run them together, since most bodies are lines written one under another. A
  row's head is read only as far as a list bears, since full markdown in the list would be
  troublesome. A link goes only to the web or to mail, an image is never loaded, and
  `[[Boiler]]` is read whole as fzn's link: the page never calls out ("Private by
  construction"), and the relay's `img-src 'self' data:` would refuse an image anyway. Code read
  is set in ink where the rest of a body read is in the soft grey, so that what would be copied
  out reads crisply. It is built as the editor was, by hand from pinned packages by one script,
  the same bytes from the same inputs, into committed files that are now several:
  `web/editor/render.js`, the parser, highlighter and renderer, which the page imports as it
  starts and never waits for, so everything reads as typed until it arrives and for good if it
  cannot be fetched; the editor, which imports the parser and highlighter from it, the two
  sharing chunks rather than each carrying a copy; and a file a language under
  `web/vendor/code/`, fetched the first time a block in it is read or written: shell,
  JavaScript, TypeScript, JSON, Python, Rust, Go, SQL, YAML, TOML, HTML, CSS, diff, Dockerfile,
  C and C++, and Java, each under its usual short names too, a Lezer grammar where there is one
  and otherwise CodeMirror 5's stream mode, as shell, SQL, TOML, diff and Dockerfile are, run by
  CodeMirror in the editor and by a few lines over CodeMirror's string stream in the page.
  `@codemirror/language-data` would have had every language, as about 124 files and 1.7 MB
  committed. Sizes raw, since the relay sends no compression, and gzipped after: the page
  fetches 78 KB (27 KB) more as it starts, the render module and the two chunks it shares with
  the editor; the editor, 411 KB (133 KB) in one file before, is 392 KB (127 KB) fetched when a
  body first opens and 470 KB (154 KB) with what it shares; a language is from 0.3 KB for diff
  to 103 KB for C++, and the grammars share 27 KB of Lezer's parser. Only the one place a body
  is read that the page's width shows is rendered, and a long body is rendered at first only to
  its first 5,000 letters and whole once the page is idle: a note of 6,300 words took 7 to 12 ms
  to render and 10 to 15 more to lay out, against 3 to 7 to lay out as plain text, and so cost
  the arrow that landed on it a frame; its first 5,000 letters take 4 to 6 ms, laid out. A head
  with no colon holds no address the page would follow and is not parsed: the 500 rows the top
  of the list paints when the line is cleared took 5 to 6 ms to read as markdown, against well
  under 1 as text. Nested past what the stack holds, 1,500 lists or 3,000 quotes deep in Chrome,
  a body is read as typed.
- **Words find; everything else holds.** Decided 2026-10-15, agreed with Sam. It replaces "A
  date typed as a search finds the notes that mention the day and the notes written on it" under
  Model, and how a tag or a person typed on the line counted until then, as one more thing to
  score: `#house boiler` listed every boiler, the one in the house first. It revises "Search
  groups" in what stays from the original, and in "A place lists what is in it; typing finds"
  what an empty line is: a line with nothing on it to rank by. The grammar is under "The line".
  Words rank exactly as they did, and everything else is something a note has to hold to be
  listed at all, and none of it changes how the words rank, so a new kind of filter, `!word`,
  archived or has a body, is one more form read from the line and one more way for a note to
  hold it, and the words are not touched; the box of an open task, `[ ]`, became the first such
  form on 2026-10-16. Membership of a span needs the moment a note was written, which the note
  relevance sees now carries in place of its day; nothing stored changed. `/-mon`, the last
  Monday before today, is new with spans, the mirror of `/fri`. The letters matcher is written
  here; a dependency for it would have been a decision of its own. A phrase that found a note
  in its body was a search, and so was `/today`, which until then went in as a draft. Labels
  are postings the index has, intersected once a keystroke; a span and a phrase are asked of
  each note left after that, a phrase last since it reads the whole text, and among the
  suggestions only until the limit is reached. Measured on synthetic spaces in a release build,
  each line typed a key at a time: at 10,000 notes every line stays under 4 ms a keystroke (the
  worst, a first letter of `~bo #house`, 3.6 ms; letters 2.5 to 2.9 ms; phrases and spans under
  1.7 ms but for a first key), against 1.2 ms for words before and after. At 30,000 notes
  words, labels, phrases and spans are 2 to 5 ms with first keys up to 12 ms, as words were
  before, and letters are 15 ms, two cold passes over every note: the heads, and the space's
  order for the ties. What it costs: a draft whose words find nothing carrying its tag lists
  nothing to step down to, where the tag alone used to list every note carrying it, so going
  into the tag first is the way to write there; `a` alone at the top lists the top of the
  space, where it used to list nothing; a date word now ends at two dots, so `../fri` in a note
  is written out as a date. Tab-separated groups, which the original had, and its `!` inverse
  stay unbuilt.
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
  whose head starts with a box, `[ ]` while it is open and `[x]` once it is done, and nothing else
  is one: a `- [ ]` in a body is markdown, read as a drawn box where a body is read, and makes no
  note a task. Until then a task was a note like any other, archived when it was done: the importer
  took the box off each one and archived the ticked ones, and since 2026-10-15 Sam had been typing
  `[ ] ` at the start of a head by hand and archiving the note when it was done. Nothing listed what
  was still to do, and nothing in a note said that it was a task. Four calls shaped it. *The box is
  text*: what you said goes in the text, and the test for adding to the agreed structure is whether
  a note still reads right on paper (see "The note is plain text"); a ticked box does, where the
  archived flag beside the text does not. So the engine's syntax gains one form and the line format
  does not change: a build from before reads `[ ] Order oil` as a note that says so, and old and new
  binaries and pages sync side by side. The head has to start with it, so `Order oil [ ]` is not a
  task, nor a head with a space before its box, nor `[X]` typed by hand, though the importer reads
  that as `[x]`. *Ticking is archiving*: what is done is archived already, so the box and the flag
  say one thing and one key does both, in one version, so that no device sees the tick without the
  archiving or the other way round, and undo puts back both. The box can still be typed, and a build
  from before archives without ticking, so the two can disagree: an open task is one whose box and
  flag both say so, and the key puts a disagreement right. The foot says `tick` or `untick` where it
  said `archive` or `unarchive`, so the page has the word on a phone with no box of its own to tap,
  which is not built (see Open questions). A tick changes the text, so it is an edit and dates the
  note's `updated`; archiving anything else still does not. *`[ ]` is a label*, read only whole, so
  that the list goes from the top of the space to the open tasks once, at the `]`, and a `[[` on its
  way to a link lists no tasks on the way; it starts a word, as a date word does, and the index
  lists the open tasks, so the box costs what a tag costs. A done task is not a label, since
  `"[x]"`, a phrase, holds the done tasks with archived notes shown, and `fzn export '"[x]" #house'`
  writes what was done in the house. *It is a place, as a tag is*, and a note written there starts
  with the box, since that is where a box is written, the other labels at the end as before; the
  session says where the caret goes, so that neither face has to find the box. The importer keeps
  the box, `- [ ] Renew passport` as `[ ] Renew passport` and `- [x]` or `- [X]` as `[x]` and
  archived, and everything else it does is as it was; importing the same pages again is how a space
  imported before gets its boxes back: a note whose text was not changed here since is brought up to
  date with its page, which now has the box, and keeps its links, whether it is archived, that it
  was deleted, and where it sits, since a page places nothing and the place held is newer than none.
  Its links are added to and never taken from, so a link to the note it sat under that was taken off
  here comes back. Archiving has not dated a note's `updated` since 2026-10-13, so a task imported
  since and archived here is still as its page gave it and gets its box back too, still archived;
  one edited here is left as it is, without one. A box that disagrees with its note's flag, archived
  by hand or by a build from before, or ticked on its page and brought back here, is put right by a
  rewrite done once through `fzn export` and `fzn import-jsonl`, and no code was added for it. The
  rewrite keeps each note's `updated`, since when it was done is not known, and so that importing
  the export taken before it puts it back. Until every device runs this build, one from before
  archives a task without ticking it; unticking and ticking it here puts it right.
- **A command writes beside the app, as a writer of its own.** Decided 2026-10-16, agreed with
  Sam, revising "The engine owns sync" and the storage rule that fzn is the only thing that
  writes a space's directory, where they say nothing is reread while the app runs. `fzn tick ID …`
  does to each note it names, by the id `fzn export` writes, what Ctrl-V does to a task in the
  app, by the same code, and `fzn untick ID …` takes it back; as with Ctrl-V, the flag decides
  which way a task goes. All or none: an id that names no note, or a note that is not a task the
  command can act on, is said by its id, nothing is written, and it exits with an error. It
  takes ids and never a query, on purpose: a query that matched one note too many would tick it
  unseen. It is for the triage skill, which ticks feedback off while the app has the same space
  open and may have the terminal, so it asks for nothing: it takes the key kept for the space,
  says so when none is kept, and with a key that opens nothing there writes nothing. It is a
  second writer on the machine, with an id of its own kept as `.commands` beside the app's
  `.writer`, so that one run after another goes on with one log; it compacts that log, or starts
  another when a header has retired it, by the rule any writer follows when it opens; it never
  takes the space's lock, which the app holds, but locks `.commands`, so that two at once take
  turns; it reads every log as far as its last whole line, since the app may be adding one, and
  connects to no relay. The app takes in what it added as it takes in the relay's lines, the
  session saying `1 changed elsewhere` and the replica sending them on. An app from before this
  build does not look: it shows a tick once it is opened again, and a note edited there before
  then is written over the tick, as an edit made apart on two devices is. Why a writer of its
  own and not the app's log: a log has one process appending to it, and two sharing one would
  mean a lock around every save and the app reading its own log back after each, which is what
  "The engine owns sync" did away with; two writers on one machine cannot conflict, since a
  version settles by its stamp whichever log it is in. Why not through the relay: it would need
  a relay running and reachable, and the copy on this machine would lag by the relay's round
  trip. Why not a socket into the app: it would be a second way in, local only, kept beside the
  relay's. What else it changed: since a log may be read while another process appends to it,
  an unfinished last line is cut only by the process about to append to that log, where only it
  could have left one, and no longer from every log when a space is opened; a log is a file, not
  a link to one, so a file gone between listing the directory and reading it is a log a
  compaction retired, and the directory is listed again for its successor; neither writer
  deletes the log the other writes, though a header from elsewhere retires it, since a copy of
  the directory that wrote under the same id elsewhere may have left it holding lines nobody
  else has, and its writer carries them into a log of its own, and keeps the new id, before it
  goes, as the replica already keeps a device's own log from a peer's header. Undo in the app
  takes back only what the app did, so it cannot take back a tick, as it cannot an edit from
  another device; `fzn untick` is the undo.
- **The relay carries the page; a directory stands in for it while the page is worked on.**
  Decided 2026-10-16, revising "The relay hands out the page" where it says the page is "not
  something built into the binary, so that changing how the page looks is a reload". The page
  is built into fzn by its build script, so a release is one file, nothing has to be found, and
  the page and the policy that serves it cannot be at different versions, which the
  hand-copied deploy once let happen: a page with the editor on a relay whose policy forbade its
  styles. `--web DIR` serves a directory instead, read again for every request, which is how the
  page is worked on, so the old reason holds where it mattered; in deployment a page change is a
  release like any other, and a tab reloads itself when the relay's page changes. The page is
  the directory's listed files, by place and kind, never the whole of `web/`, where the bundles'
  sources and their `node_modules` live. The relay names the page's build: sixteen hex digits of
  a SHA-256 over its files, the same whether built in or read from a directory, a content hash
  so that the same files put back are the same build and no tab reloads for nothing. It says it
  in `Fzn-Build` on every answer and writes it into the page's `<meta name="fzn-build">`, since
  a page cannot read the headers of the response that loaded it, and not in the websocket's
  first messages, which are about logs, nor in `Server-Timing`, which a browser hides over plain
  http. With each built-in file it sends an `ETag` and answers a browser that still holds it
  with a `304`, a gzip twin made at build time to a browser that takes one, so the relay
  carries no compressor, and a year's caching for the chunks whose names are their hashes; it
  answers `HEAD` as `GET` without the body. A directory is named again at most once a second,
  sent whole, and serves only the files that would be built in, so a file the list lacks is
  missed while the page is worked on and not once it ships. The alternative, an archive
  unpacked to a data directory the relay looks in by default, was a second thing to install and
  a chain of places to look. What it costs: about 2.5 MB in every binary, 1.8 MB of page and
  0.7 MB of gzip twins, the terminal's included, which never serves it. A build with no page
  built (`web/build.sh`) carries none and says so, one whose page is older than the engine's
  sources says that, and a release builds the page itself first.
- **A release is one static binary that a script fetches.** Decided 2026-10-16, at Sam's
  direction. Three builds, static musl for Linux on x86_64 and arm64 and one for Apple silicon,
  made by a workflow on a tag `v<version>` equal to the workspace version, into a draft release
  that is published by hand after a look. `install.sh` is POSIX sh: it checks the archive's
  SHA-256, puts the binary in `~/.local/bin` by one rename so a running relay keeps the file it
  has open, touches no rc file and keeps no receipt, does nothing when the version asked for is
  already there, and `--uninstall` takes the binary and leaves the notes and keys. `release.sh`
  builds and lays a release out as GitHub lays releases out, and the workflow runs it per
  platform, so a release built there and one built by hand cannot drift, and the installer can be
  pointed at either. One variable, `FZN_RELEASES`, names where releases live, since the
  repository may move or go private; a mirror directory is the answer to a private one, rather
  than teaching the installer tokens. `fzn --version` says the version, the commit and the day,
  so a machine can be asked what it runs. The version continues the Go app's line at 0.26.0,
  since its tags are in this repository and tools that take the highest tag as the latest would
  otherwise take `v0.25.5`. The release profile is fat LTO, one codegen unit and no symbols for
  the binary and the wasm alike: a third off the binary (3.6 MB to 2.4 MB before the page) and a
  quarter off the wasm (1,058 KB to 788 KB). Not `wasm-opt`, which took 6% more and would be one
  more downloaded tool rewriting the module every release carries. Not `panic = "abort"`, which would make a panic in one of the relay's connections the end of the
  relay, and not `opt-level = "z"`, which halves the wasm but is slower on the loops a keystroke
  runs. Not cargo-dist, which generates ten times the lines for the same three builds and an
  installer with a receipt and an updater; not signatures yet, since attestations need an
  enterprise plan on a private repository and minisign a key to keep: a signed tag and the sums
  are what there is. Intel Macs and Windows are left out until someone has one.
- **Tab goes in.** Decided 2026-10-16, at Sam's direction, reversing the part of "Enter edits, the
  arrow goes in" (2026-10-14) that made Right the key that goes in and rejected Tab, and with it the
  rule of "Enter opens the body; Right goes in from the end of the head" that it went in only from
  the end. Tab goes in, in both faces and from both places: from a row into its note, and from the
  line into what it names, wherever the caret is, since a tab means nothing in a line of text and so
  needs no rule about the end. Right and Left are the caret's and nothing else, and at the end of
  the text Right does nothing. Where an earlier decision says Right or the arrow goes in ("A tag is
  a place you can go into", "A name is a fact too", "A span is not a place", "The arrow at the end
  of a line of labels goes into each" and the rest), the key is now Tab; what each decided about
  where it goes stands. Three reasons. Tab is quick, one key in the same place on every keyboard,
  found by the little finger without looking. It matches what the line already is, a completion: Tab
  takes the completion in a shell and in every editor, and "type the name, Tab" is that gesture. And
  the arrow's two meanings, the caret's short of the end and the zoom's at it, were a rule to
  remember, the one the Open question "Right at the end of the head as the zoom key, or Alt-Z" kept
  open: a hand that thought the cursor was on the line went in instead of moving the caret. What the
  2026-10-14 reasoning held against Tab is answered. A page moves focus with it, but when you are in
  fzn the keys bound to it are for fzn, so the page takes Tab as the terminal does and its words at
  the foot are pressed or tapped; the `›` in the foot stays, for touch. In the body's sheet, which
  is a text editor and goes nowhere, Tab indents. The original's Tab-separated search groups,
  unbuilt since the rewrite, are given up for good; its `!` inverse is still possible. Shift-Tab is
  not bound: Escape is the way back, a second key for it would be inherited rather than earned, and
  terminals do not agree on what they send for it; the page takes Shift-Tab and does nothing with
  it. In a terminal Ctrl-I is Tab, so no chord sits on Ctrl-I, and were the kitty keyboard protocol
  ever turned on both would have to be bound. A pasted tab must not go in, which is one reason the
  terminal now takes a paste whole (see "A paste is one change"). The footer says `Tab go in`,
  keeping the engine's label, which the page's `›` in the foot shares. Alt-Z is no longer a
  fallback, since Tab is everywhere. The Tab-to-link that "Position is the link" dropped stays
  dropped: this Tab links nothing. The Open question is closed.
- **The path starts with the space.** Decided 2026-10-16, at Sam's direction. The path read `fzn ›
  #house`, or `fzn · garden › #house` in a space that was not the usual one. The app's name says
  nothing the person does not know; the space's name says where they are, which is what the path is
  for. Now the first step of the path is the space, in both faces: ` main › #house `, and in the
  page that first step is pressed to go back to the top, as every step is a way back to it; the
  page's title stays `fzn`, or `fzn · garden`, which is for the tab and not for where you are in it.
  At the top, outside any place, the terminal still draws no path row, since the space does not
  change while the app is open and what is not asked for is not on the screen; a dim `main` above
  the line at all times was weighed for knowing which space a window is on, and left to Sam to ask
  for. The terminal sets no window title: a window rule matches the terminal's class, which the
  launch sets (`--class fzn`), and a title that followed the path would change on every zoom. Were a
  summon key ever to want one, it would be the app's name and not the path.
- **A paste is one change.** Decided 2026-10-16, with "Tab goes in". The terminal had not asked for
  bracketed paste, so a paste arrived as keystrokes: a sealed version a letter, Enter at every line
  break writing the line and moving the cursor onto the note it wrote, and the rest typed into that
  note's head. Once Tab goes in, a pasted tab would have gone in mid-paste. Now the terminal asks
  for the paste whole, and turns it off around the editor and on the way out: it goes into the line
  or the head at the caret as one change, one version, with each tab and line break inside it made a
  space, since the line and a head are one line each and the body is where a paragraph goes, and the
  breaks at its ends dropped with the blanks beside them, since a line copied from a terminal or a
  page comes with its break. Two other readings of a line break were weighed and left to use to ask
  for: each line a note, the original's Enter-and-type rhythm, good for pasting a list and
  surprising for a wrapped paragraph; and the first line the head and the rest the body, which the
  body would keep only on closing.
- **The editor's file is yours alone.** Decided 2026-10-16. A note handed to the editor was written
  to the temporary directory with the process's umask, readable by every user on the machine, and
  left there when the editor exited with an error; an editor that could not be run, a mistyped
  `editor =` say, threw you out of the app. Notes are sealed on the device and this was the one
  place their text sat open on disk for others, so it is a fault of privacy and not of polish. Now
  the file is made new and readable by its owner only, in `$XDG_RUNTIME_DIR`, which on Linux is the
  user's own and kept in memory. Where there is none, or the one named does not take the file, as
  when it is left over from another user under `su`, the file goes into a directory of its own in
  the temporary directory, which only its owner can open, so that the swap and backup files an
  editor makes beside the file are as private as the note. The file, or that directory and all in
  it, is removed whatever the editor did, and only a killed process leaves it. What the editor left
  is saved before the terminal is taken back, so that failing to take it back loses nothing. An
  editor that cannot be run is said on the status row and the app stays, and `fzn` with no terminal
  to draw on says so in one sentence. The editor is still a command split on spaces, so `code
  --wait` works and a path with a space in it does not; the config's template says so.

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
- The browser's keys past the four, and Ctrl-D and Ctrl-O. Words at the foot, pressed or tapped, are
  a start: nothing to remember, but slower than a chord for archiving ten notes in a row. Undo has
  no key at all yet.
- Touch. A tap puts the cursor on a note, a second opens its body, the `›` goes in, and the
  path at the top is the way back. Whether any of that is right is for use on a phone to say.
  Seen 2026-10-15 in Chromium's and WebKit's touch emulation: once the cursor is on a row its
  head is the text box and fills the row, so a second tap on the head puts the caret there. A
  second tap off it, on the date under the head on a phone, opens the body when it lands there,
  but a browser takes a tap that near a text box into the box: WebKit every time it was tried,
  Chromium some of the time. Until 2026-10-15 the body read under the row was where a second
  tap opened it in both, and it went with the body; on a phone the word in the foot is now the
  one sure way to open a body, and whether the row wants a target of its own for it is open.
- On a row Esc keeps rather than cancels, which spreadsheet hands will find out once; daily use
  confirms it. (Which key goes in, open here beside it until 2026-10-16, is settled by "Tab goes
  in".)
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
- Whether the body should be kept as it is typed too. A version is the whole note, so a
  keystroke in a long body writes all of it; it is kept when it closes, as the terminal's editor
  keeps it. Use will say whether that is felt.
- A log is compacted only when its device opens, and a head is now a sealed line a keystroke; a
  tab left open for days grows its log until it is reloaded. Compacting when grown, not only
  when opened, is the fix if it is felt.
- A fork in the commands' log. A space's directory copied to a second machine takes `.commands`
  with it, and once both machines tick, two logs have one name. The relay finds it and the app
  says so, as for any log not its own, but the commands hear no relay, so neither side starts a
  new log for it as the app does for its own: what the second machine ticked stays on it until
  a header retires the shared name, which only the other side's commands compacting would write.
  Copying a directory is not supported, so this waits until it is seen; the app taking the
  commands' log into its next compaction is one way to close it.
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
