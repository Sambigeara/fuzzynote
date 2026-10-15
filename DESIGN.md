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
  else is drawn: no borders, no panels, no counts, no colour. What is not being asked for is not
  on the screen.
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
  the arrow and is the context. Selecting notes, and a context of several labels or of plain
  words, are not.
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
  out is dropped. The one explicit gesture is a correction: a chord on a note in a note's zoom
  links it or unlinks it, for when the engine was wrong or the note was written in the wrong
  place. The engine does not read brackets when it searches. A link adds no words, and still a
  note is found by the name of what it is about: when you type, the head of a note that a note
  links to counts as its own head does, one hop and no further. Only explicit links are stored;
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
  a note created while looking around another links to it. And since 2026-10-14 for the next: a
  note created inside a tag, a person or a day ends its head with the label, as it would have
  been typed. Only the innermost place is inherited.

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
before any score: what is linked to the note being looked around, or carries the label being
looked around, and the note headed with what was typed. Budget: under five milliseconds per
keystroke for ten thousand notes.

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
- **v1: one append-only log per writer, per space.** Each line is a full snapshot of one note
  as JSON: `v`, the version of the line format, then id, text, links, created, updated, stamp,
  archived, deleted, device. Times are RFC 3339 with the offset of the clock that wrote them,
  `2026-10-13T10:12:56+01:00`, so a line reads without the app, and a note keeps the day it
  was written on wherever it is read. Lines from before versions hold unix seconds and are
  still read; a line with a later version than the build knows is left alone, not misread.
  `updated` is when the text last changed, so archiving a note does not re-date it. `stamp` is
  when the version was written, and a local change is always stamped past the version it
  replaces. Loading replays every log in the space and the version with the newest stamp wins,
  ties broken by writer id. Last-writer-wins on a whole note is the known limitation, accepted
  for now. Deletion is a tombstone line, kept so an older version arriving later cannot
  resurrect the note. What a line says, and which of two versions is the newer, is the
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
  read and taken away without the app. `fzn import-jsonl` is the way back in; a plain file is
  no longer read as a log by being put in the directory, since it would then be sent in the
  clear. When the logs carry CRDT updates this format stays. It carries the facts beside the
  text as well as the text: the 2022 corpus is what it looks like when only the text gets out
  (creation time survives on 7% of its notes).
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
  newest version of each, whatever order the lines come in and however many times. A log is
  named for its writer, and a writer's id is kept with the space it writes, in the space's
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
- **Three messages**, each one JSON object.
  - `{"have":{"3f9a01c2":{"lines":293,"digest":"…"}}}` is every log the sender holds and its
    mark in each. Said on connecting.
  - `{"lines":{"log":"3f9a01c2","after":{"lines":293,"digest":"…"},"lines":["…"]}}` is lines
    of a log, and the mark they come straight after.
  - `{"forked":"3f9a01c2"}` says that the sender's log of this name and the receiver's are
    not one log.
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
  the body, which in the terminal is your editor and in the browser the row grown to hold the
  whole note with the other rows gone, where the arrows move within it and Esc closes it back
  onto the head, or with vim's keys `:q`; Right at the end of the head goes in (see Zoom); undo
  puts the head back as it was when the cursor landed, and otherwise takes back the last change,
  which for a row is the whole visit, one key for both. A chord acts on the note as it reads. A
  head is a version at every change, a new row's from its first letter, which creates the note
  with the moment Enter was pressed as its `created`, linked to the note being looked around and
  to whatever it names in brackets; date words are written out when the cursor leaves the row,
  and so is the label of the tag, person or day a new note was written in, and a body is kept
  when it closes. So Enter, type, Enter, type, Enter, type, Esc writes a list, from the line or
  from any row, each note on every device as it is typed. A new row left with nothing in its
  head goes when the cursor leaves it, the arrow landing past where it was, up on the row above
  or down on the row that was under it; one never typed into reaches neither the log nor the
  relay, and one typed into and cleared has its deletion written and leaves nothing to undo.
  Enter on a new row with nothing in its head does nothing: there is already an empty row to
  write in. A head cleared to be typed again is not written while it is empty. Backspace on a
  row with nothing in its head takes the row away and goes up to the row above, or onto the
  line, as it takes an empty line away in any editor; a new row goes, a note emptied is deleted
  and undo brings it back, and a note with a body stays, since the body is not in the head to
  see. A note emptied in place is deleted when the cursor leaves it, and undo brings it back; an
  empty file back from the editor does the same. The list holds still while the cursor walks it,
  so that the arrow that leaves a head lands where it was aimed, and is put in order when the
  cursor comes back to the line.
- **Terminal**: Rust and ratatui, with the original fzn chords where they still earn their
  place, because that is where the muscle memory is. It is a face on the session and adds four
  things: its key table, the editing of the line and of the head of the row the cursor is on,
  the painting, and carrying out the effects, which here means a log file, your editor, the
  clipboard and the system's way of opening a URL. A chord acts on the line when the cursor is
  there and on the note under the cursor otherwise: Ctrl-D clears or deletes, Ctrl-V shows
  archived or archives. Archived notes are out of every list, typed results included, until
  Ctrl-V on the line asks for them. Enter on a row opens a new row under it, Backspace on an
  empty head takes the row away, and Ctrl-O opens the note in your editor, which is how a body
  is written here; the field in the row is its head, and the body stays shown under it. Right at
  the end of a head goes into the note, and at the end of the line into what the line names;
  short of either end it is the caret's. Ctrl-U and Ctrl-R undo and redo any change, Ctrl-L
  links or unlinks the note under the cursor (see Zoom), Ctrl-C copies its first URL (or its
  head) to the clipboard, Ctrl-_ opens the URL, Ctrl-B shows scores. Escape comes back a rung at
  a time: off the row, writing out its date words and a new note's label, off the text on the
  line, out of the zoom, and twice on an empty line outside one quits; Ctrl-D on the line is a
  second key for clearing it. Ctrl-Q quits from anywhere, writing out the head first. The screen
  is flat: no borders, no panel. The path is a dim row above the line and only in a zoom; the
  line is the first row; a date word has the date it stands for shown after it, dimmed; in a
  zoom the body of the note you are in sits under the line, dimmed and clamped. The rows follow,
  each a head and then, dimmed, a `¶` if it has a body, the names of what it links to after an
  arrow, and its reasons; the row the cursor is on has the terminal's cursor at the end of its
  head, the head in bold, its date at the right, and its body under it, clamped to six rows. One
  dim footer row names the chords that apply where the cursor is, ending with Escape's next
  rung, `Esc clear`, `Esc back` or `Esc up`, or `Esc Esc quits` where there is none; a status
  row above it exists only while there is something to say. Nothing says how many notes there
  are or that sync is working; `not syncing` is said when it is not. Not yet reinstated: Ctrl-S
  multi-select with Enter zooming on what the selection shares, Tab search groups, Ctrl-^
  export. The first time a space is opened on a machine fzn asks for its passphrase, twice if
  the space is new, and keeps the key it derives; `fzn key` asks again, for when it was
  mistyped. `$FZN_PASSPHRASE` answers for scripts.
- **Zoom.** Right at the end of a note's head makes it the context, and the path grows by one;
  short of the end, Right is the caret. A tag, a person or a day can be the context too (see "A
  tag is a place you can go into"): Right at the end of the line goes into what the line names,
  the label if the line is one label and nothing else, `#house`, `@joe` or `/fri`, and otherwise
  the one note in the space the line is the name of, by the rule in "A name is a fact too". In a
  zoom that rule holds as it does at the top: the note the line names is listed first, saying
  `its name`, wherever it is and whatever the limit, so Right goes where the list shows, and
  typing a name is the way from any place to any note. A line that names neither, or names where
  you already are, leaves Right doing nothing. The path shows a label as it is written:
  `#house`, `@joe`, `2026-10-16`. The list becomes the place's neighbourhood, in two parts. The
  facts come first, newest on top: what is linked to or from the note, or every note carrying
  the label, which for a day is every note written on it or mentioning it. A link is a fact and
  so is a label, and this part is a list you keep, however long it is. What the engine finds
  besides follows, best first: shared tags and people, shared words, shared dates, each with its
  reason; for a label, what its words find. The line comes with you, under one rule: if the note
  you went into contains what you typed, or its head and the heads of the notes it links to have
  every word of it, you were searching for it, so the line clears and stepping back brings it
  back; otherwise you were writing, and the draft is carried in to be written here. A place gone
  into from the line was searched for by definition, so the line clears. Stepping back never
  throws away what you typed: a line written inside stays on the way out, and the search comes
  back only to an empty line. Escape is no longer only the step back: with text on the line it
  clears the line, and only on an empty line does it step back, so that inside a place "found
  it, clear, stay" is one key. The faces bind Right at the end of the line to going in and
  Escape to that, and the session offers `clear` as the way back while the line has text and
  `back` once it is empty. Either way, what you write inside is about the place: inside a note
  it links to it and lands at the top of the linked part, and inside a label its head ends with
  the label, as it would have been typed, unless the note carries it already: in its text, or
  for a day by being written on it, since the day a note was written is its `created`. A note
  written from the line has it at once; one written in place has it when the cursor leaves its
  row, as its date words are written out, so the head reads as typed while it is typed. Only a
  note created there inherits, and only from the innermost place: in `#house › Boiler` a new
  note links to Boiler and carries no tag. This is context inheritance in its simplest form.
  Typing inside a context narrows the neighbourhood and ranks what is left, with the note the
  line names first. Ctrl-L moves a note between the two parts by linking or unlinking it; it is
  a correction, not the mechanism, and outside a note's zoom, at the top or inside a label, it
  has nothing to do and says so. Right at the end of a head again walks a hop further, and on a
  place already on the path goes back to it, so the path never repeats; stepping back goes one
  hop, onto the line. This is the ego graph as a flat list; following a link is picking its
  target here.
- **Browser**: the engine's `Device` compiled to wasm32 via wasm-bindgen, with a page of plain
  JavaScript and CSS to drive it: no framework, and one dependency, the body's editor, built by
  hand into one committed file (see the decision). A tab is one more device. It has a writer id
  and a log of its own for each space, keeps every log of the space in the browser between
  visits, asks for the passphrase once and keeps the key, and syncs with the relay it was loaded
  from, so there is one address to open and nothing to set up. A relay hands the page out when
  it is pointed at the `web` directory with `--web`.
  - *The page.* One column, centred, text left-aligned, and the line a little above a third of
    the way down, where it stays: it does not move when you type or when results arrive. The
    blank page is the caret and, under it in faint grey, the heads of recent notes fading out by
    the eighth. As you type, what is found takes their place; only a row that is new fades in,
    and nothing else moves. The path, in a zoom, is in the margin above the line. No colour:
    paper, ink and two greys, light and dark by the system's preference. The note under the
    cursor is read under its row, clamped to six lines and opened with a tap; the note you are
    in is read the same way above the line, under the path and in the path's smaller size, so
    that where you are is all above the line and what is found follows it directly. The margin
    grows downwards only for a body longer than it has room for. There is no panel beside the
    list. (Until 2026-10-14 the note you were in was read under the line; Sam found it read as
    part of the line, and it put a gap between the line and the first result. The terminal
    still shows it under the line, where its line is the first row and a body above it would
    move the line.)
  - *Keys.* The four that are the app are the same: type, Enter, Escape and the arrows. The
    terminal's chords are not here, because a browser keeps most of them for itself, but two it
    lets go: Ctrl-D clears the line or deletes the note under the cursor, as in the terminal,
    and inside the body box it is the browser's, and vim's in vim mode; Ctrl-O opens the body of
    the note under the cursor, as in the terminal, and does nothing on the line. Backspace on an
    empty head takes the row away, as in the terminal. Right at the end of the line, with
    nothing selected, goes into what the line names, as Right at the end of a head goes into the
    note; Escape on the line clears it while it has text, and on an empty line in a zoom steps
    back. Every other thing that can be asked for is a word at the foot of the page, the footer
    of the terminal made pressable, and Tab reaches them from a row without leaving it. Only
    what applies where the cursor is gets one, and no key is written beside a word.
  - *The line* is an ordinary text field, so the phone's keyboard and the browser's own editing
    work in it. A cross after it clears it when it has text, and a `›` after the cross, in the
    column of the selected row's, goes into what it names when it names somewhere. Each keeps
    its place while it is not there, so that neither moves the other, or the dates, by coming or
    going; and since they are on the line, clearing and going in are not also words in the foot.
  - *Writing in a row.* The head of the row the cursor is on is a text box, and typing changes
    it; Enter opens a new row under it; Ctrl-O grows the box to the whole note and takes the
    other rows off the page, so that the note is the page and there is no mistaking that it
    opened, and in it Enter is a line break and the arrows move within it, and Esc closes it
    back onto the head and brings the rows back. There is no second editor: the row is the
    editor, for a long document too. While the body is open the box is a CodeMirror editor,
    loaded the first time one opens: soft wrap, step undo, a grey selection, and vim mode for
    the browser that asked for it with the word in the foot, where Esc is vim's and `:q`, `ZZ`
    or the word in the foot closes it, and `:w` writes and stays. The head stays an ordinary
    field. Until the editor has loaded, and if it never does, the box is a plain text area. The
    `editor` the engine offers is not shown here.
  - *Touch.* A tap puts the cursor on a note, a second tap on it opens its body, a tap on the
    `›` at the end of the selected row goes into the note and on the one after the line into
    what the line names, and the path is the way back.
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
  whether it is archived, that it was deleted.
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
  be the dependency decision Sam deferred, made in passing.
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
  page.
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
  `2026-10-16`. Only a note created in the place inherits, and only from the innermost place: in
  `#house › Boiler` a new note links to Boiler and gets no tag, exactly as a note in a note's
  zoom always has, since the path says how you got there and its last step says where you are.
  One label at a time: several labels as one context, and a plain-word search as a context, are
  left out, the second because a plain-word search could only hand down the old prefix, the same
  words written on every line. Nothing is linked in a label, so Ctrl-L there does nothing but
  say so, and the first part of the list is the notes that carry it, whatever the weights make
  of them. Remembering the path across restarts is left out too: the summon key in the open
  questions is the answer to coming back to where you were.

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
  finer than a second are the fix if two devices ever edit one note at once.
- The browser's keys past the four, and Ctrl-D and Ctrl-O. Words at the foot reached with Tab
  are a start: nothing to remember, but slower than a chord for archiving ten notes in a row.
  Undo has no key at all yet.
- Touch. A tap puts the cursor on a note, a second opens its body, the `›` goes in, and the
  path at the top is the way back. Whether any of that is right is for use on a phone to say.
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
  foot is where the switch belongs; daily use with it on decides. Marking the engine's syntax
  (tags, people, dates, URLs, bracketed names) in place in the body, in the two greys, is a 1 KB
  decoration left until use asks.
- The editor is one file of about 409 KB fetched on each page load in which a body opens, since
  the relay sends no validators or compression; whether that is felt on the phone is for use to
  say, and `Last-Modified` with 304s or a hashed name is the fix.
- Whether the body should be kept as it is typed too. A version is the whole note, so a
  keystroke in a long body writes all of it; it is kept when it closes, as the terminal's editor
  keeps it. Use will say whether that is felt.
- A log is compacted only when its device opens, and a head is now a sealed line a keystroke; a
  tab left open for days grows its log until it is reloaded. Compacting when grown, not only
  when opened, is the fix if it is felt.
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
