# Fuzzynote

## Vision
A blank page with a single blinking cursor. You type a thought, a note, a TODO. As you type, the
engine finds what you have already written that belongs with it and shows you why. Go into one of
those and write there, and what you write belongs to it: where you are is what you are talking
about. Everything is local, private and instant. The same engine runs in a terminal and in a
browser, syncs seamlessly between your devices, and can be shared with other people when you choose.

## Principles
- One flat space of notes. No folders, no documents, no tree. Structure is a graph: the links you
  make and the links the engine finds.
- The input is the search is the composer. Typing is finding; Enter is creating.
- A note is text. Its first line is its head. There is no separate "line" and "note".
- Instant. Every keystroke resolves within a frame over tens of thousands of notes. Nothing on the
  hot path touches disk or network.
- Private by construction. The engine never calls out. Notes are sealed on the device with a key the
  relay never has, so sync moves lines through a relay that cannot read them.
- Explainable relevance. Every suggestion carries its reason. No black box in the loop by default.
- Converge, never conflict. Every change is an event; devices merge; nothing is lost.
- One core, thin faces. The engine is the whole app with no screen and no keyboard: a face says what
  was asked for and paints what it is told to show. Its keys, its gestures and its pixels are all a
  face owns, so no rule of the app can differ between a terminal and a browser.
- Keyboard first, almost no chrome.
- Ergonomics is a primary goal, and it has two halves: nothing visible ever waits, and there is
  almost nothing to remember. The whole app fits in one sentence: type to find, or into the row you
  are on to change it; Enter writes the line, or opens a new row under the one you are on; Ctrl-O
  opens the body; Tab goes in, from a head into its note and from the line into what the line names;
  Esc clears the line, or comes back; and where you are is what you are talking about. Every other
  key is maintenance, and the footer shows only the ones that apply right now. A key is justified by
  the flow it sits in, never inherited.
- One cursor. There is exactly one on the screen and it moves: the caret on the line, or the caret
  in the head of the row the cursor is on. Nothing else is highlighted and almost nothing else is
  drawn: no borders, no counts, no colour. The body read is that of what the cursor is on, in a
  place of its own apart from the list, never under a row or the line. What is not being asked for
  is not on the screen.
- Archived is not forgotten. The list hides archived notes; the engine still remembers them.

## What stays from the original fzn (the soul)
- The search line is the composer: filter live, Enter turns the search into a new note carrying that
  text.
- Enter, then type into the new row. The original's rhythm for a list was Enter, type, Enter, type,
  with the search line as a standing context; the place you are in is that context now, and Enter on
  an empty line opens a new row to write in. An empty row left empty was never anything.
- Every line is editable where it is. The original had one text cursor over the whole screen, and so
  does this: the head of the row the cursor is on is the field, typing changes it, and each change
  is kept as it is typed. Browsing is editing, and a stray key is one undo.
- Zoom on what is shared. The original selected notes and pressed Enter, and whatever they had in
  common became the context; the old common-prefix zoom was the special case. What is reinstated is
  the label: a tag, a person, a day or a task's box typed on the line is gone into with Tab
  and is the context, labels gone into one after another are one context, and so are a note and the
  labels gone into after it. Selecting notes, and a context of plain words, are not (see "A tag is a
  place you can go into" for why).
- One line to search with: words find and everything else holds, so `"…"` is the exact text and a
  tag, a person, a day, the open tasks' `[ ]`, the done tasks' `[x]` or a span narrows what is
  listed, and `~` at the start of the line is the original's fuzzy, letters in order in heads. The
  original's Tab-separated groups are given up, Tab being the key that goes in, and its `!` inverse
  is not built yet.
- Hashtags, @mentions, date expansion, URLs opened or copied with one key.
- Archive with one key, which ticks a task; show/hide archived globally.
- Undo/redo and move up/down. Copying whole notes, and pasting them, are not built yet.
- Local-first, remote-second. Offline is the normal case.

## Model
- **Space**: a set of notes plus explicit links, kept apart from every other space. On disk a space
  is one directory of logs under the root. Plain `fzn` opens the space named in the config file
  (`main` to begin with) and `--space NAME` opens another; a tab opens `main`, or the space
  `?space=garden` names. Sharing a space is later.
- **Note**: stable id; text; links; created and updated times; archived flag, and when it was
  archived, which for a task is when it was done; where it sits in the list, as the note it sits
  after, with a clock of its own (see "Where a note sits" under Storage and sync). Derived, never
  stored: head (first line), body, tags, mentions, dates, URLs, and whether it is a task.
- **Link**: from one note to another, held on the note as the target's id. There is one kind. It
  survives a rename and adds no words to the text. Links are made by position: a note written while
  looking around another is about it, and links to it. Nobody has to say "link". Brackets are the
  way of typing one from inside the editor: when a note is saved, each `[[head]]` that names an
  existing note becomes a link, and a link whose brackets were taken out is dropped. The one
  explicit gesture is a correction: a chord on a note in a note's place links it or unlinks it, for
  when the engine was wrong or the note was written in the wrong place. The engine does not read
  brackets when it searches. A link adds no words, and still a note is found by the name of what it
  is about: when you type, the head of a note that a note links to counts as its own head does, one
  hop and no further. Only explicit links are stored; everything else is recomputed on demand.
- **Syntax that the engine understands**: `#tag`, `@person`, dates, URLs, `[[note head]]` as a way
  of typing a link, and a box, `[ ]` or `[x]`, at the start of a note, which makes it a task (see "A
  task is a note headed by its box"). Nothing else is special in a note. In particular the old
  ` >> ` prefix paths are plain text; the words in them still count, the hierarchy does not. On the
  line, and only there, three more forms are read: a phrase in quotes, a span of when notes were
  written or done, and `~` at its start (see "The line" under Clients).
- **Date**: plain text in the note, written `2026-10-16`, and never stored apart from it. The engine
  reads it on load, as it reads tags. A link is held as an id because a note's head can change; a
  date's name cannot, so it lives in the text. The day a note was written is not typed at all: it is
  the note's `created`. `/today`, `/tomorrow`, `/yesterday`, a day of the week (`/fri` is the next
  Friday and `/-mon` the last Monday, never today) and `/+3` or `/-2` are quicker ways of typing a
  date, and any beginning that only one of them has will do (`/tom`, `/f`, `/-tu`). Any other slash
  is left alone, and a date word ends at the `..` of a span. The line shows the date a word stands
  for and holds with it. Saving a note, from the line or from the editor, writes the date in its
  place, at the moment a `[[head]]` becomes a link. A date typed on the line holds the notes that
  mention the day or were written on it, and counts as a date and not as the numbers in it. The old
  `Mon, Jan 2, 2006` form is still read.
- **Context inheritance**: where you are is what you are talking about. A note created in a place
  inherits it, and nothing else does: it links to the place's note and carries the place's labels.
  This is what prefix prepending used to do, without a tree and without the typing: the old ` >> `
  prefix was this context written by hand on every line, and Ctrl-P pasted it. The rules are under
  "Zoom and places".

## Engine
The engine is a library, and it is the app. It owns no UI, opens no file, reads no clock and makes
no network call, so the same code runs in a terminal, in a browser tab and in a test.

**Session.** What a face drives, one per person per space. Three things go in and two come out.
- In: an *action*, which is what was asked for and never which key asked: the line now reads this,
  move the cursor, write the line, go into a note or a label, step back, archive, delete, link,
  undo, the text of a note as it was edited, keep the note as it stands, go back to a path a face
  kept. An action that does not apply where the cursor is does nothing.
- In: versions of notes written elsewhere, by another device or another writer on this one.
- In: notes given from outside, each as a line of `fzn query` has it, changed or not, and a line
  with no id as a new note at the top: written by the app's own rules, the text as the editor saves
  it, brackets followed when the text changes and a task's box and flag kept together, all or none.
  A line that changes nothing writes nothing. A line whose `stamp` and `device` are not the note's
  as held is refused, since the note has changed since it was read; a line with no `stamp` says
  nothing of when it was read and is not checked. A note moved since it was read keeps its new place
  and takes the text given, since an edit and a move merge apart; a move asked for is refused until
  use asks for one.
- In, from the host: the time, an id for a new note, and randomness for sealing.
- Out: *effects*, which are what the face has to do about an action: keep this version, put this
  note in front of the person to edit, copy this, open this URL. The session has already done its
  own part, so an effect that fails costs a message and nothing else.
- Out: the *view*, which is everything there is to paint: the path, the line and what its date words
  and spans stand for, the rows with their reasons, each with the head it shows off the cursor and
  whether it is a task and ticked, where what a place only finds begins, where the cursor is, the
  note under it as it is being written and when it was written, the note you are in, what just
  happened, and what can be asked for where the cursor is, each with what to call it there. A face
  puts its own key or button beside each. What the view says is worked out once an action or an
  arrival is done with, so a face may ask for it as often as it paints.
- Out, when a face asks: the path as *steps*, for a face about to be reloaded under the person to
  keep and give back, `[{"note":"…","search":"boi"},{"label":"#house","search":"#house"}]`: each
  note by its id and each label as it was typed, with the search each was found with. Given back,
  they are gone into from the top as going in from the line would, passing over a note that has
  gone, a label that is not one and a place already on the path, and the cursor is left on an empty
  line for the face to type the line it kept. A face that never reloads pays nothing for it.

Everything decided about how the app behaves lives here: what going into a note does with the line,
what is listed first, what a note written in place links to, what is offered where. Under the
session is the part that finds things, in three parts.

**Index.** A Unicode-aware tokeniser, lowercased, with a URL counted as its host; no stemming. An
inverted index with term frequencies and document lengths, a trigram index over the vocabulary for
fuzzy matching, and a label index for tags, people, days and the two boxes, open and done. Beside
each note's text is the text lowercased once, a character at a time, which a phrase and letters are
matched against, so that a keystroke does not lowercase every note it asks. All update incrementally
on every note change. The old corpus is 8.7k notes, 284k tokens and 12k distinct terms; the index
for a space ten times that size fits comfortably in memory.

**Scorers.** Each takes the current input plus context and returns candidate notes with a score and
a human-readable reason. Three are built, cheapest first.
1. *Explicit*: shared tags, mentions, link targets, and words of the query that appear in a note's
   head (weighted by rarity, so a note titled with what you typed outranks a one-liner that merely
   mentions it; BM25's length normalisation would otherwise bury long notes). A word in the head of
   a note that a note links to counts too, at a little less than a word in its own (`about` in the
   weights), with the reason `about Boiler`.
2. *Lexical*: BM25 over the inverted index. Rare shared words count far more than common ones. The
   word under the cursor is a prefix, not a word: it stands for every vocabulary word it starts, in
   the word scorer and in the head signal, and a note earns credit for its best completion only.
   This is what makes results right while a word is still being typed. A misspelling still finds its
   note by trigram overlap, at a fraction of an exact word's worth. With `~` at the start of the
   line the words are letters instead, found in order in heads and scored as fzf scores them, and no
   other scorer runs.
3. *Temporal*: created or edited close in time; dates written in the text.

Intended and not built, until daily use asks: *structural* (notes that sat adjacent in the list,
notes you navigated between), *feedback* (a locked link boosts shared terms between the two notes; a
dismissal dampens), and a local embedding scorer, on device, if semantic similarity is ever wanted.
That would be another scorer, not a dependency.

**Combiner.** Weighted sum with per-scorer caps, top-k with reasons attached. What the line holds
comes before anything: a note that does not hold it is not listed at all, so the scores only ever
order what holds. Two facts come before any score: what is in the place being looked around, linked
to its note and carrying its labels, and the note headed with what was typed. With nothing on the
line to rank by no scorer runs, at the top or in a place: a place's facts are a set, the note's
links kept to the notes on the index's list for every label. Budget: under five milliseconds per
keystroke for ten thousand notes.

**Tuning happens in the product.** Every suggestion shows its reasons; writing where you are makes a
link, and a chord corrects one, stored on the note as the target's id; the weights live in the
config file; a toggle shows scores. There is no evaluation harness and no labelling step. Nothing is
added to the engine until daily use asks for it.

## Storage and sync
- **The note is plain text; what holds it need not be.** All of a note's meaning is in its
  characters, in forms people already know: `#tag`, `@person`, `2026-10-16`, a URL, a first line, a
  box `[ ]`. That is the agreed structure, and the test for adding to it is whether the note still
  reads right on paper. Beside the text go the facts it cannot carry: id, links, times, archived and
  when, deleted. What you said goes in the text; what happened to the note goes beside it. A file
  per note is not the store: files cannot merge two devices' edits, and most notes are one line.
- **One append-only log per writer, per space.** Each line is a full snapshot of one note as JSON:
  `v`, the version of the line format, then id, text, links, created, updated, stamp, after, placed,
  archived, done, deleted, device. The format is version 3: version 2 added `after` and `placed`,
  version 3 took times to the millisecond, and lines from before versions hold unix seconds. All
  are still read; a line with a later version than the build knows is left alone, not misread
  (see "A line of a later format is left as it is"). A field a build from before can do without,
  as `done` is, comes without a new version, since such a build reads the line and passes over the
  field. Times are RFC 3339 to the
  millisecond with the offset of the clock that wrote them, `2026-10-13T10:12:56.250+01:00`, so a
  line reads without the app and a note keeps the day it was written on wherever it is read.
  `updated` is when the text last changed, so archiving anything but a task does not re-date it.
  `done` is when the note was archived, which for a task is when it was ticked, and null while it
  is not: the version that archives a note, by the key, by `fzn tick` or by `fzn apply`, sets it to
  its own moment, as does a new note's version headed `[x]`, bringing the note back clears it, and
  an edit leaves it as it was. A line that is archived and says nothing of when, as a build from
  before `done` writes one, reads its `done` as its `updated`, the latest it can have been, and an
  importer does the same for a page's ticked task. A line of `fzn query` carries it, and `fzn apply`
  takes it as fzn's own, as it takes `updated`. `stamp` is when what the version says was written,
  and a local change is always stamped past the version it replaces, a millisecond past it when the
  clock has not moved on since. `after` is the note it sits after and `placed` when it was put
  there, a clock of the place's own, kept and stepped the same way. Loading replays every log in the
  space: the version with the newest stamp gives the text, links, times and flags, ties broken by
  writer id, and the version newest by `placed` gives the place, ties broken by the note it is
  after. An edit leaves `placed` as it was and a move leaves `stamp`, and each carries the side it
  leaves as it was written, writer and all, so an edit on one device and a move on another both
  hold, and neither decides anything about the side it carries. Last-writer-wins on what a note says
  is the known limitation, accepted for now. Deletion is a tombstone line, kept so an older version
  arriving later cannot resurrect the note. What a line says, and which of two versions is the
  newer, is the engine's to settle, so that every device agrees. Where the lines are kept is the
  face's: files under the terminal, IndexedDB in a tab.
- **Where a note sits** is the walk of the tree that `after` makes: at the top every note placed at
  the top, newest placed first, and each note followed by the notes placed after it, newest placed
  first, and theirs. A note is placed at a moment past every note already placed after the same one,
  so it sits directly under it whatever the clocks that placed the rest, and two devices writing
  under one note at once both sit under it, each run whole. A note from before places sits by when
  it was written; one placed after a note no version is held for sits at the top until that one
  arrives. Deleted notes, and archived ones while hidden, are walked through and not listed, so what
  was placed after them keeps its place. A cycle, which two devices moving at once can make, is
  listed as if it were at the top, so that no note is lost. Enter on a row places the new note after
  that row in its first version; Enter on the line, and a note written from the line, place it at
  the top. An edit never moves a note. A move is one note alone: down places it after the row below,
  and up after the note the row above follows in the space's order, or at the top, so that it lands
  beside the row it passes and goes past nothing else, even among a place's facts, where the notes
  between two facts are not shown. What was placed after it is placed after the note it followed
  instead, oldest first, so a run written under a note stays together and no cycle is made. A move
  is one entry to undo, though it writes a version of every note it places; moving the top up, or
  the last down, writes nothing. Undo takes back only the side a change made, so undoing a move
  leaves what has been written since and undoing an edit leaves where the note has been moved since.
  The order is kept between lists and walked again only when a version moves a note or a note
  arrives that does not plainly go first where it lands, as one written here always does; typing
  into a note keeps it. While the space holds a cycle, or once more than sixteen notes have arrived
  since it was last read, as when a space comes from the relay at once, it is walked again when next
  read.
- **Lines are sealed before they are kept.** What goes into a log, and so to the relay, is not the
  snapshot but an envelope around it: `{"s":1,"c":"…"}`, where `c` is a random 24-byte nonce and
  then the snapshot encrypted with XChaCha20-Poly1305 under the space's key, base64. The key is 32
  bytes derived from a passphrase with Argon2id (19 MiB, two passes, one lane) and a salt that is
  the first 16 bytes of the SHA-256 of `fzn/space/` and the space's name, so that the same
  passphrase on another device gives the same key and nothing has to be sent to get it. Each device
  derives the key once and keeps it, under the terminal in the config directory (`keys/<space>`,
  readable only by its owner, never beside the notes) and in a tab in the browser's storage, so the
  passphrase is typed once a device. The first time a space is opened on a machine fzn asks for its
  passphrase, twice if the space is new; `fzn key` asks again, for when it was mistyped, as does the
  word `passphrase` in the page's foot in a space with no notes yet; `$FZN_PASSPHRASE` answers for
  scripts. Lines from before sealing are still read as they are; the first open with a key compacts
  the writer's own log into a sealed one (see Sync), so plain lines leave the writer's log at once
  and the relay as soon as it has the sealed one. The key is verified by opening a line: a device
  that has seen a sealed line and opened none has the wrong passphrase, says so, and writes nothing,
  since a line sealed under a second key would split the space in two. A space with no sealed line
  yet takes the first key it is given. Forgetting a passphrase is losing the notes; there is no
  recovery and no reset, and sharing a space will be sharing its passphrase until there is something
  better. What the relay still sees: the names of the logs, how many lines each has and how long
  they are, when they arrive, the compaction headers, and each space's auth key, from which nothing
  of the space's key can be had (see Sync). What a relay can still do: withhold lines, or hand back
  an old prefix; it cannot forge, alter or reorder one.
- **Who writes a space's directory.** Every client open on it, each a writer of its own: each
  window, each command that writes beside them (`fzn apply`, `fzn tick`, `fzn untick`) and an
  importer. A client takes the lowest free slot of a small pool for its life, `.writer` then
  `.writer.1` and on, a lock on that file and the writer id kept in it, and appends only to the log
  that id names; the next holder of a slot goes on with its log, so the pool grows to the most
  clients ever open at once and no further. Nobody waits for a slot and nothing is locked per write.
  A window looks in the directory four times a second for what has been added to logs it does not
  write, and takes that in as it takes in the relay's lines, which is how windows on one space see
  each other, each in its own place, and how one lists what an importer or a command beside it
  wrote, saying `1 changed elsewhere`. A command asks for nothing: it takes the key kept for the
  space, says so when none is kept, and writes nothing when that key opens nothing there; with no
  window open it takes slot 0. With a relay named and no window holding the keeper's lease, it
  holds the lease for its run and syncs once (see "A device syncs by having a relay named");
  commands take turns at that by a lock of their own, `.relay.command`, which windows never take,
  so that a command finding the lease held knows a window holds it. Beside a window a command
  connects to nothing, and what it writes reaches the relay through the keeper. Since a
  log may be read while another process appends to it, every log is read only as far as its last
  whole line, and an unfinished last line is cut only by the process about to append to that log. A
  log is a file, not a link to one, so a file gone between listing the directory and reading it is a
  log a compaction retired, and the directory is listed again for its successor. No client deletes a
  log another slot writes, though a header from elsewhere retires it, since a copy of the directory
  that wrote under the same id elsewhere may have left it holding lines nobody else has: its own
  writer carries them into a new log, and keeps the new id, before it goes, as the replica keeps a
  device's own log from a peer's header. Clients share a lock on `.lock`; a relay holds it alone, so
  a relay asked to serve a space a client has open is refused, and so is a client opening a space a
  relay holds. `fzn query` and `fzn export` take neither a lock nor a slot to read; with a relay
  named and no window keeping the space in step, they first share the lock and take the lease and a
  slot for the exchange, which keeps the relay's lines here, and let them go before they read.
  Nothing else writes there: Syncthing and the like are not supported and not needed; the relay is
  the way between machines. What arrives from the relay comes through the engine, which asks the
  face to keep it.
- **Later: a text CRDT per note** (Yrs, the Rust port of Yjs, or Loro). The log lines then carry
  sealed CRDT updates instead of sealed snapshots. The replica and the relay do not change, since
  they never read a line; the session and `Notes` do, since the session reads and makes `Record`s
  directly in a couple of dozen places and undo holds whole versions. Count on a day in `session/`
  and `record.rs`, not a swap behind the store. Compaction already turns a history into one snapshot
  a note, which is what a CRDT log needs from time to time too.
- Sharing is sharing a subset of updates. Deferred, but nothing above prevents it.
- **The snapshot line is the way in and out, for good.** `fzn export` writes every live note as one
  plain line, oldest first, by your own hand and to wherever you point it, so notes can always be
  read and taken away without the app; it takes no line, and given one it names `fzn query` and
  writes nothing. `fzn query LINE` writes what that line finds at the top of the space, archived
  notes too, in the order the app lists them, which its `--help` says, and never cut at the app's
  500 rows. `fzn apply` takes such lines
  back and writes each note as it then reads, by the session's rules for notes given from outside,
  all or none, beside the open app; its lines are numbered as given, so a blank one before the end
  is a line that is not a note. `fzn tick ID …` and `fzn untick ID …` do to the notes named, by the
  ids `query` writes, what Ctrl-V does to a task, all or none, the flag deciding which way a task
  goes. Each of the three writes out every note it was given as it now stands, the line `query`
  would write, so a change comes with its receipt, and running it again writes nothing. Every
  command says what went wrong on one line, `fzn: ` and why, and exits 1 when nothing was done and 2
  when its arguments made no sense, without printing the manual. `fzn import-jsonl` is the way back
  in for a whole export, a restore and not an edit, since it takes ids and times as they are given,
  and `fzn import-markdown` reads a directory of pages (see "Imported pages become the notes they
  would have been"); a plain file is not read as a log by being put in the directory, since it would
  then be sent in the clear. When the logs carry CRDT updates this format stays. It carries the
  facts beside the text as well as the text: the 2022 corpus is what it looks like when only the
  text gets out (creation time survives on 7% of its notes).
- A directory of markdown files for grep or Obsidian is a possible second export. It would be
  derived, never the source of truth.

## Sync
A space gets from one device to another by each side sending the other the rest of each log. That is
the whole of it. It is in the engine as `Replica`, with no I/O, so a device, a browser tab and the
relay all run the same code.

- **A space is a set of logs, and a log has one writer.** The writer only ever adds lines to the
  end, so any copy of a log is the start of the writer's own. A *replica* is one place's copy of a
  space. What it holds of a log is one number, and what a peer lacks is the rest. Which log a line
  sits in is bookkeeping: the notes are what all the lines add up to, the newest of each side of
  each, whatever order the lines come in and however many times. A log is named for its writer, and
  a writer's id is kept with the space it writes, in a slot's file in the space's directory under
  the terminal (`.writer`, `.writer.1` and on) and under the slot's key in the browser's storage, so
  that the writer of one space is not the writer of another.
- **Compaction is a new log.** A log only grows, so a writer that wants a shorter one starts
  another: it takes a new writer id, and the first line of the new log is a header,
  `{"compacts":["3f9a01c2"],"snapshot":293}`, naming every log it has had before and how many lines
  follow as the snapshot. The snapshot is the writer's latest version of every note it ever wrote,
  tombstones included, one sealed line each, and then every line of a later format as it was; after
  it the log goes on as any log does. A replica that holds the header and the whole snapshot
  *retires* the logs the header names: it drops them from what it holds and says, ignores a peer
  that offers them, and deletes them from wherever it keeps lines. Until it holds the whole snapshot
  it keeps both, and the notes come out the same, since a version is a version whichever log it is
  in. The writer compacts as soon as a peer's header has retired its log, and at open when its own
  log holds lines from before sealing, or more than a thousand lines and over four times the notes
  in it. Putting a fork right is the same move: the writer takes a new log, and the one with two
  writers is retired by the header. Nothing is state: which logs are retired follows from the logs
  held, and the header names the whole chain so that it still follows after the middle of the chain
  is gone.
- **A mark is a point in a log**: how many lines in, and a digest of those lines. The digest is
  FNV-1a, 64 bits, over each line and its newline, written as sixteen hex digits. It is a check
  against accidents, not a defence against anyone.
- **Four messages**, each one JSON object with one key. A message a build does not know is passed
  over.
  - `{"have":{"3f9a01c2":{"lines":293,"digest":"…"}}}` is every log the sender holds and its mark in
    each. Said on connecting.
  - `{"lines":{"log":"3f9a01c2","after":{"lines":293,"digest":"…"},"lines":["…"]}}` is lines of a
    log, and the mark they come straight after.
  - `{"forked":"3f9a01c2"}` says that the sender's log of this name and the receiver's are not one
    log.
  - `{"now":"2026-10-15T09:30:00.250Z"}` is the sender's clock. A relay says it first, before
    `have`, so that a device can say when its own clock is far off (see "A clock far from the
    relay's is said"). A replica makes nothing of it.
- **Four more are between a device and its relay**, and a replica makes nothing of them. A relay
  that asks a device to show it holds the space's key says
  `{"challenge":{"nonce":"…","claimed":true}}` after its clock: a nonce, and whether it keeps an
  auth key for the space yet. The auth key is SHA-256 of `fzn/auth/` and the space's key, so a relay
  that keeps it can let a device in and read nothing. The device answers `{"proof":"…"}`, the nonce
  sealed under the auth key as a line is sealed, or, when the relay keeps none, `{"claim":"…"}`, the
  auth key itself; and then `have` again, since a relay passes over what comes before the answer. A
  device never claims while it holds sealed lines and its key has opened none of them, nor when its
  key opens none of the lines a relay offers with its challenge for a space it keeps no auth key for
  but holds lines of, `"lines":["…"]`: the first sealed line of each of its logs, up to sixteen,
  which show nothing a copy of the lines would not, so that one log under another key cannot shut
  the space. Opening none is a passphrase mistyped on a new device, which would otherwise make its
  key the space's and shut out every device with the right one; the device says so, keeps it as it
  keeps a refusal, through whatever the relay says next of the answer it did not hear, and sends
  nothing more. `{"refused":"…"}` is why a relay will not go on; the device says it and keeps it
  until a relay takes it in. The passphrase is in doubt when the key opens nothing the device holds,
  when it opened none of the space's lines a relay offered, whatever it opens here, or when a relay
  refused the device after it answered the challenge and nothing it holds says the key is right; a
  refusal before the answer is of a token or a name, not of the key. A device that hears no
  challenge syncs as it always did.
- **Four rules.**
  1. Told what a peer has, send it the rest of every log it holds less of, having checked its mark
     against the same point in your own.
  2. Add lines only where they continue exactly what you hold, which the mark they come after says.
     Lines you hold already are passed over. Lines from further on than you hold are answered with
     `have`, and the peer sends from there.
  3. Lines you add, pass on to every other peer that lacks them. This is all a relay does.
  4. Where a mark does not match, the two are different logs with one name: a fork. Nothing of that
     log passes between the two, both are told, neither is changed, and every other log goes on as
     before.
- **Nothing else is state.** No cursors, no acknowledgements, no sequence numbers in the lines. What
  a replica holds is all it knows, so a new device says it has nothing and is sent everything, and a
  relay that has lost its disk is filled again by the next device to connect.
- **Lines are never read on the way, but one.** They are copied byte for byte, and sealed, so a
  replica could not read them if it tried. A line from a later format passes through a replica that
  does not know it. The one line a replica reads is the first of a log, to see whether it is a
  compaction header, which is bookkeeping about logs and not about notes. This is what lets the text
  CRDT arrive without the relay changing.
- **A connection** is a websocket to `/1/main` on the relay: the version of this exchange, then the
  space, or for a space under a name, `/1/joe/main`. It is entered with the space's key: the relay
  says its clock and then its challenge, and nothing of the space until the device has answered. A
  device that makes a space offers a token in its greeting, as the subprotocol `fzn.token.<hex>`
  beside `fzn`, and the relay says back `fzn` alone. A message is at most 8 MiB at either end, which
  only a single note larger than that could reach. Each end pings a quiet peer every fifteen seconds
  and gives up on one silent for 45. A lost relay is dialled again, waiting twice as long each time
  from a second up to half a minute, a connection that ends within a few seconds counting as one not
  made, so that a device the relay refuses does not ask again every second; and a device says what
  it holds whenever it connects, so nothing said while there was no connection needs keeping.
- **The relay** is a replica that writes nothing of its own, and it is `fzn relay`. It serves every
  space under its root and keeps each as the same files a device keeps, sealed, and holds each
  space's lock alone, where clients share it, so nothing else writes them while it runs. It listens
  on `127.0.0.1:3960` until told otherwise. Beside each space's logs it keeps the space's auth key,
  in `.auth`, readable only by itself, and it lets a connection in only once the device has proved
  that key; the first device to answer for a space it keeps none for gives it the key, and the space
  is that key's from then on. A connection goes in this order: the clock; the name resolved, and one
  the relay does not have refused; for a space with no key kept yet, the token offered checked
  against the name's, or nothing checked at an open root; the challenge, with the first sealed line
  of each of the space's logs, up to sixteen, when it keeps no key but holds lines; whatever the
  device says before its answer passed over; the answer checked, or the claim kept; and only then
  the space opened and the device given what it lacks. Nothing is opened, made or written for a
  connection before that, and each step of the greeting has five seconds, so a peer with no key
  holds a thread for fifteen seconds at most and puts nothing on disk; past 64 connections in their
  greeting at once, or 8 from one address, so that one peer cannot keep the rest out, a new one is
  closed straight away. A refusal of a name, a token or a key is said as `{"refused":"…"}` after the
  greeting, since a browser is never shown why a greeting failed, and logged with the address, the
  name and the space, never a token, a nonce or a key; the relay's own trouble after the answer, a
  space it cannot open or a key it cannot keep, is only logged, and the connection closed, so that a
  device tries again and does not doubt its passphrase. A space's key is kept only with a token:
  under a name the relay's admin has given one, `fzn relay add joe` for joe's spaces, under `joe` in
  its root, and `fzn relay token` for the spaces at the root, which anyone who connects may make
  until then, as the relay says when it starts. That is when a space is made, and again if its key
  file is ever lost. It keeps only the SHA-256 of each token, in its config directory as
  `relay/tenants/joe` and `relay/token`; whether a name is there is looked at on every connection
  and its token read whenever a space has no key yet, so a copy of its files gives no token and a
  name given or taken away holds without a restart. It has no TLS, and the terminal refuses
  `wss://`: on a private network the network encrypts the wire, and elsewhere a proxy in front of
  the relay has to. What keeps a relay private is the key, which it never has; what keeps its disk
  is the token. It carries the page and hands it out on the same address, so a browser has one
  address to open (see "Where the page comes from" under Browser).
- **A device syncs by having a relay named**, in the config file, in `$FZN_RELAY` or with `--relay`.
  There is no command for it. While the app is open a thread keeps the space's logs in step with the
  relay, and the foot says only when that is not going. With no window holding the keeper's lease,
  the commands that read and write notes, `query`, `export`, `apply`, `tick` and `untick`, sync once
  each: they take the commands' turn, `.relay.command`, waiting while another command has it (at
  most four greetings, past which the turn is said as the reason they are not synced), then the
  lease, connect, carry messages until the device holds every line the relay said it holds (a log
  forked with the relay apart), do their work, send what they wrote, and close once the relay has
  closed too, letting the lease go after their last append. A relay out of reach, one that refuses,
  and one silent for a greeting are said on one line, `fzn: not synced with nas.local: …`, and the
  command goes on with this machine's copy; one that works says nothing. With several windows open
  on a space, one holds the socket and keeps the relay's lines, the keeper, by a lease on `.relay`;
  the others reach the relay through it, by way of the directory. The next window takes the lease
  within a look of the keeper's going and reads what the last keeper kept before it appends, with
  nothing handed over, since the directory is the state. A keeper that hands the terminal to an
  editor lets the lease go first, since it hears and looks at nothing until the editor is done, and
  tries to take it back at its next look after. The address may carry a token,
  `ws://joe:TOKEN@nas.local`, which is what lets a device make a space on the relay: it is read with
  a URI parser, goes no further than the greeting (see "A connection"), and is never said, the
  status row naming the relay by its host alone. With no relay named, nothing leaves the machine.
- **The engine is where sync and the app meet.** A *device* is a session and a replica joined in
  memory, with the space's key, and it is in the engine as `Device`: the same code in the terminal,
  in a tab and in a test. A version the session wants kept is sealed, becomes a line of this
  writer's log, and the replica sends it on; a line the replica takes from the relay, or that
  another writer on the machine added where the face keeps lines, is opened and given to the
  session. Its peers are the relay, which every face numbers 0, and its siblings, 1, 2 and on: other
  clients open on the same space on the same machine, each writing a log of its own. A sibling is a
  peer like the relay, so what one writes reaches the others as any line does and what one hears
  from the relay it passes on; the relay's clock is the only one taken, since a sibling's is this
  machine's own. The face is left with what only it can do: the connections, and keeping lines for
  the next time. In the terminal that is one thread holding the websocket and passing text both ways
  over channels, in the window that holds the keeper's lease, or with no window open a command's
  one exchange, carried on the command's own thread, and a directory of files, which is the wire
  between windows; in a tab it is a socket of its own and a broadcast channel to the other tabs (see
  "Several tabs" under Browser). Nothing is locked per write and nothing can go stale under the
  replica: each line of another writer's log is read once, as a peer's line is.
- **A fork is how a mistake shows.** The one that will happen is a writer's id copied to a second
  machine along with the rest of a directory, so that two machines write different logs under one
  name. Without the digest neither would send, each believing the other had its lines, and notes
  would stay on one machine with nothing to say so. It puts itself right: a device told that its own
  log is forked compacts, taking a new id, and its header retires the forked name. Both machines do
  this, each from its own copy, so nothing either wrote is lost, and the relay ends up with two logs
  where it had one shared name.
- Not covered yet: TLS, history beyond what compaction keeps, and two devices syncing with no relay
  between them, which would each have to challenge the other. The header is taken on trust inside a
  space, and that is where it belongs: it is the one line a replica acts on, and it reaches a
  replica only from a device that proved the space's key, which holds every note in the space
  already. A writer's own log is never retired on a peer's word, so even a member's header loses
  nothing.

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

A head is a version at every change, a new row's from its first letter, which creates the note with
the moment Enter was pressed as its `created`, linked to the note being looked around and to
whatever it names in brackets. Date words are written out when the cursor leaves the row, a new row
in a place has its labels from the start, and a body is kept when it closes. So Enter, type, Enter,
type, Enter, type, Esc writes a list, from the line or from any row, each note on every device as it
is typed. A new row left with nothing in its head but what it opened with goes when the cursor
leaves it, the arrow landing past where it was; one never typed into reaches neither the log nor the
relay, and one typed into and cleared has its deletion written and leaves nothing to undo. Enter on
such a row does nothing: there is already an empty row to write in. A head cleared to be typed again
is not written while it is empty, and a note emptied is deleted when the cursor leaves it, or when
an empty file comes back from the editor; undo brings it back. Backspace on a row with nothing in
its head takes the row away and goes up to the row above, or onto the line, as it takes an empty
line away in any editor; a note with a body stays, since the body is not in the head to see. A note
stays where it was written, and an edit never moves it. The list holds still while the cursor walks
it, so that the arrow that leaves a head lands where it was aimed, and what arrived from elsewhere
meanwhile is listed when the cursor comes back to the line.

### The line
The line has one grammar, read in the engine so that both faces have it: words find and everything
else holds. Words rank, by BM25, prefixes, look-alikes, the head signal and the name rule (see "A
name is a fact too"). Everything else on the line is something a note has to hold to be listed at
all, and none of it changes how the words rank.
- `"…"` is a phrase: the note's whole text, head and body, has it, in any case, as plain text. An
  unclosed quote runs to the end of the line, the space typed last included, so the list narrows as
  the phrase is typed. `“`, `”` and `„` are quotes too, any of them closing what any of them opened,
  since a phone's keyboard curls them. An empty phrase holds nothing. A phrase's words are words as
  well, so a phrase in a head outranks one in a body.
- `#tag`, `@person` and a day hold: the note carries the tag or the person, or is of the day, by
  mentioning it or by having been written on it. The last thing typed holds every tag or person it
  is the start of until whitespace follows it, `#ho` holding `#house` and `#hosting`, so the list
  does not empty mid-word. The name rule reads a label typed as the words it says, since a head says
  its tags as words: `oil tank #house` names the note headed `Oil tank #house`.
- `[ ]` holds the open tasks, the notes headed `[ ]` and not archived, and the rest of the line
  still applies: `[ ] #house` is the open tasks of the house, and `[ ] oil` finds among them. `[x]`
  holds the done tasks, the notes headed `[x]`, archived or not; typing it is asking for them, so
  they are listed though archived notes are otherwise hidden. Each is read only whole: `[` and
  `[ ` are not words either, so they hold nothing, and a `[[` on its way to a link lists no tasks
  on the way. `"[x]"` is still a phrase, held by any note with it in its text, archived notes
  shown or not as they are.
- A span, `START..END`, `START..` or `..END` with no spaces round the dots, holds the notes written
  in it, and `..` as a word of its own is a span of all time. With `[x]` on the line, or in the
  place, it holds the done tasks done in it instead, by `done`, or for a box ticked by hand and
  never archived by `updated`: `[x] /-mon.. #work` is this week's done work, whenever it was
  written. Each end is a date written out or a date word, with a 24-hour `HH:MM` after it if wanted:
  `/-mon 18:00..` is since six on Monday evening, and `2026-10-01..2026-10-05` runs through the end
  of the 5th; a span runs from the start of its first day or minute to the end of its last. A time
  is read by the clock of the place each note was written in, as a note's day is, a note with no
  moment to measure is in no span, and a time anywhere else on the line is words. The line says what
  a span stands for where it says a date word's date, `→ Mon 2026-10-12 18:00 – now`; the engine
  words both, so that the faces keep no calendar of their own, and an open start is `…`.
- `~` as the first thing on the line makes its words letters, found in order in heads, as the
  original's fuzzy found them: `~bsv` finds Boiler service. Every word of them must be there. They
  rank as fzf ranks: a letter at the start of a word earns more, twice as much for the first letter
  typed, a letter running on from the one before earns at least what the first of its run did, and a
  gap costs more the longer it is; then the shorter head, then the space's order. Every row says
  `letters: bsv`. Letters name no note, so Tab does nothing after them, and a phrase holds in this
  mode without ranking.

A line with nothing to rank by lists exactly what holds what it holds, in the space's order and with
no reasons, as a place lists its facts on an empty line; a line that holds a span lists it in time
order instead, oldest first, by the moment the span reads, when written or with `[x]` when done, so
`..` lists every note in the order it was written and `[x] ..` every done task in the order it was
done. At the top it is cut at the 500 rows the top of the space is cut at, a list of time keeping
the newest 500 so that it still ends at now, and in a place it is not cut; `fzn query` is never
cut. A single letter is not a word, so `#house a` lists what `#house`
lists. With words as well, they rank among the notes that hold, by the scores, span or not. The note
the line names has to hold it too. Inside a place, the line narrows the place's facts and its finds
alike, and a line of labels in a place lists the facts that carry them. The rows of a line with
nothing to rank by and no span are in the space's order, so they can be moved as the rows of an
empty line can; the rows of a line of time are not, and are not moved. Enter writes the line as it
reads: quotes, `~` and spans stay as typed, and date words are written out, after a span's dots too.
Archived notes are out of every list, typed results included, until they are asked for, or `[x]`
asks for the done tasks.

The line comes with you into a place, under one rule: if the note you went into contains what you
typed, or holds what the line holds and its head and the heads of the notes it links to have every
word of it outside its phrases (with `~`, its head has the letters), or the line has nothing else
but what holds, you were searching for it, so the line clears and stepping back brings it back;
otherwise you were writing, and the draft is carried in to be written here. So a phrase that found a
note in its body was a search, and so was `/today`, and a draft with a tag in it,
`boiler needs bleeding #house`, is carried in, as is a line with nothing on it to rank by or to
hold, a letter or a mark. A place gone into from the line was searched for by definition, so the
line clears.

### Zoom and places
Tab on a note's head makes it the place, and the path grows by one, wherever the caret is in the
head. A tag, a person, a day or the open or done tasks can be the place too: Tab on the line goes
into what the line names. That is the label if the line is one label and nothing else, `#house`,
`@joe`, `/fri`, `[ ]` or `[x]`, or back to it if it is on the path; each label in turn if the line
is several, `#house /today`, as if Tab had been pressed after each but passing over a label the
place has already; and otherwise the one note in the space the line is the name of and that holds
what the line holds, by the rule in "A name is a fact too". A line holding a span, a phrase or `~`
is never gone into as labels are, whatever else is on it (see "A span is not a place"). In a place
that rule holds as it does at the top: the note the line names is listed first, saying `its name`,
wherever it is and whatever the limit, so Tab goes where the list shows, and typing a name is the
way from any place to any note. A line that names neither, or names where you already are, leaves
Tab doing nothing, and types nothing: a tab is never written into the line or a head. The path shows
a label as it is written: `#house`, `@joe`, `2026-10-16`, `[ ]`, `[x]`, and a note as its row read
where it was gone into from: `#house › Boiler`, not `#house › Boiler #house`, and `Boiler`, not
`# Boiler`.

A row that is not under the cursor shows its head without the place's labels where they end it, when
the note is a head only, since the place already says them: inside `#house`, `Order oil #house`
reads `Order oil`. The place's labels are those gone into since the last note on the path, which a
note written there would inherit; they come off in any order and any case, but only as whole words,
`#House` for `#house` and not `#house,`. The box at the start stays; a head that would be left with
nothing, or only its box, shows whole; a note with a body keeps its head whole, since that head is
its name; and the row under the cursor shows its text as it is. A label only typed on the line hides
nothing. The engine says the head each row shows, so neither face carries the rule. A head is named
without the labels that end it too, in a second tier: only when no note's whole head is what was
typed, and inside a place only for a note that carries the place's labels, both for the list and for
Tab, so that typing what a row reads is typing its name.

The place is the last note on the path, if there is one, together with every label gone into after
it; labels before the note are only how you got there, so `#house › Boiler › today` is the place
`Boiler › today` is. Its facts are the notes linked to or from the note that carry every one of
those labels: the links alone when no label follows the note, and with no note on the path every
note carrying all the labels, which for a day is every note written on it or mentioning it. A link
is a fact and so is a label.

The list works as the top of the space does. With nothing on the line to rank by it is what is in
the place and holds what the line holds, its facts in the order the space keeps them in, or with a
span in time order, however many, and nothing the engine suggests; a row says how it is linked,
`links here`, `linked from here` or `linked both ways`, and nothing scored. Typing words finds: the
note the line names first, then the place's facts that match, then what the engine finds in its
neighbourhood that matches, each best first with its reasons and each holding what the line holds,
and a row of nothing between the last fact and the first find, where the view says the finds begin.
Around a note the neighbourhood is what shares its tags and people, words and dates, or links to the
same things, with only the notes carrying the labels counted as facts; around labels alone it is
what their words find.

Stepping back never throws away what you typed: a line written inside stays on the way out, and the
search comes back only to an empty line. Esc with text on the line clears it, and only on an empty
line steps back, so that inside a place "found it, clear, stay" is one key; the session offers
`clear` as the way back while the line has text and `back` once it is empty.

What you write inside is about the place. Inside a note it links to it and lands at the top of the
place. Inside a label its head ends with the label, as it would have been typed, unless the note
carries it already: in its text, or for a day by being written on it, since the day a note was
written is its `created`. Among the open or done tasks the head starts with the box instead, since
that is where a box is written, and among the done tasks the note is then a task done now, as any
new note headed `[x]` is (see "A task is a note headed by its box"), so that something finished is
logged straight into the week. A note written from the line has the labels added as it is written; a
new row opens with them already in its head, the caret where the writer's text goes, and a row with
nothing in it but what it opened with, or a start of it, is empty, however it is spaced. Inside
labels gone into one after another the head ends with each of them it does not carry, in the order
of the path: in `#house › @joe` a new row opens ` #house @joe`, in `#house › [ ]` it opens `[ ]
#house` with the caret after the box, in `#work › [x]` it opens `[x] #work`, and in `#house › today`
a note reads `Order oil #house`. A zoom on today opens an empty row, one on `/fri` opens `
2026-10-16`. Only a note created there inherits, and only from the place: in `Boiler › today` a new
note links to Boiler and is of today by being written on it, in `Boiler › #house` a new row opens `
#house` and links to Boiler, and in `#house › Boiler` a new note links to Boiler and carries no tag.

The link chord, Ctrl-L in the terminal and `link` or `unlink` in the page's foot, links the note
under the cursor to the place's note or unlinks it, which takes it into the place or out of it when
it carries the place's labels: on an empty line an unlinked note leaves the list, and a note to link
is one that typing found. It is a correction, not the mechanism, and with no note on the path, at
the top or inside labels alone, it has nothing to do and says so. Tab on a head again walks a hop
further, and on a place already on the path goes back to it, so the path never repeats; stepping
back goes one hop, onto the line. This is the ego graph as a flat list; following a link is picking
its target here.

### Terminal
Rust and ratatui, with the original fzn chords where they still earn their place, because that is
where the muscle memory is. It is a face on the device and adds four things: its key table, the
editing of the line and of the head of the row the cursor is on, the painting, and carrying out the
effects, which here means the space's directory, your editor, the clipboard (through the terminal's
own OSC 52 sequence) and the system's way of opening a URL.

The keys. A chord acts on the line when the cursor is there and on the note under the cursor
otherwise. Ctrl-D clears or deletes; Ctrl-V shows archived or archives, which ticks a task and is
named `tick` there; the line's right-hand end says `archived shown` while they are. Ctrl-O opens the
note in your editor, which is how a body is written here: in a file made new and readable only by
its owner, in `$XDG_RUNTIME_DIR` or else in a directory of its own in the temporary directory that
only its owner can open, and removed, with that directory, whatever the editor did, so that only a
killed process leaves it; what the editor left is kept before the terminal is taken back. An editor
that cannot be run is said on the status row and the app stays, and `fzn` with no terminal to draw
on says so in one sentence. Tab goes in wherever the caret is, and Right at the end of the text does
nothing. Shift-Tab is not bound, and Ctrl-I, which a terminal sends as Tab, carries no chord. Up and
Down move the cursor, PageUp and PageDown by ten; Ctrl-Up and Ctrl-Down move the note under the
cursor a row, the cursor with it. Ctrl-U and Ctrl-R undo and redo, Ctrl-L links or unlinks the note
under the cursor, Ctrl-C copies its first URL (or its head), Ctrl-_ opens the URL, Ctrl-B shows
scores. The line and the head take the usual editing keys: Home and End or Ctrl-A and Ctrl-E,
Delete, Ctrl-W. A paste goes into the line or the head at the caret as one change, its tabs and line
breaks made spaces and the breaks at its ends dropped. Escape comes back a rung at a time: off the
row, writing out its date words, off the text on the line, out of the place, and twice on an empty
line outside one quits, the first saying `Escape again to quit`. Ctrl-Q quits from anywhere, writing
out the head first.

The screen is flat: no borders, no boxes, the terminal's own colours. Everything is measured in the
columns the terminal draws it in, two for a Chinese letter, and the caret moves and Backspace takes
a letter as it is drawn, an accent with its letter. The path is a dim row above the line and only in
a place, headed by the space's name, ` main › Boiler › #house `, the middle of a long path given up
to `…`; at the top there is no path row, and the app sets no window title. The line is the first
row, after a dim `>` in the column the rows leave empty, so that its text lines up with theirs; what
its date words and spans stand for is shown after it, dimmed, `→ Fri 2026-10-16` or
`→ Mon 2026-10-12 18:00 – now`. The rows follow the line directly, each a head and then, dimmed, a
`¶` if it has a body, the names of what it links to after an arrow, and its reasons joined by `·`;
the row the cursor is on has the terminal's cursor at the end of its head, the head in bold, and the
day its note was written at the right, the day a day label and a span read too, or none for a note
that never said. The foot of the list is kept for the body of what the cursor is on: the note on
its row, and on the line in a place the note you are in. It is a row of nothing, then up to five
rows of the body, dimmed, set in two columns from the rows' text and cut with a `…` when it goes on.
The rows are kept whatever the note holds, on a row and on the line in a place alike, so walking the
list never changes how much of it is shown; on the line outside a place the list has the whole
height. On a short screen they shrink, keeping the list at least half its room, and go when fewer
than two would be left. One dim footer row names the chords that apply where the cursor is,
`^V tick`, `Tab go in`, ending with Escape's next rung, `Esc clear`, `Esc back` or `Esc up`, or
`Esc Esc quits` where there is none; a narrow terminal loses chords from the middle, never the first
or the way back. A status row above it exists only while there is something to say, each key
clearing what the one before said, and where the body has its rows it takes the last of them, so
that the list keeps its height. Nothing says how many notes there are or that sync is working;
`not syncing` is said in the corner of the foot when it is not, and in its place, while syncing, how
far this machine's clock is from the relay's when that is more than two minutes,
`this device's clock is 5 min ahead`. A window that does not hold the socket says neither, since
what it writes reaches the relay through the one that does. Why a relay refused this device is said
on the status row, after the relay's host, until a relay takes it in.

Not yet reinstated: Ctrl-S multi-select with Enter zooming on what the selection shares, `!`
inverse, Ctrl-^ export.

### Browser
The engine's `Device` compiled to wasm32 via wasm-bindgen, with a page of plain JavaScript and CSS
to drive it: no framework, and one family of dependencies, CodeMirror and its parser Lezer, for the
body's editor and the page's reading of markdown, built by hand into committed files (see "The body
is a CodeMirror editor, loaded when a body first opens" and "A body's markdown is rendered where it
is read and styled where it is written"). Everything crosses to the engine as text: an action goes
in as JSON and what to do and what to show come back as JSON, a hint's action being exactly what to
send back to ask for it. A tab is one more device. It has a writer id and a log of its own for each
space, keeps every log of the space in IndexedDB between visits, asks for the passphrase once and
keeps the key, and syncs with the relay it was loaded from, so there is one address to open and
nothing to set up. A line is sent only once IndexedDB has kept it, so that the relay never holds a
line of a tab's log that the tab lost by closing, which the next tab to write that log would write
over: a fork. A relay that says who may make spaces on it gives a link with a token, `#joe:3f9a…`,
or `#3f9a…` for its root, opened once: the page keeps the token for the browser and takes it off the
address, asks for its spaces under the name, `/1/joe/main`, and offers the token as a subprotocol
beside `fzn`, the one place a page can put a credential on a socket. A space that is there already
is entered with its key alone. A refusal from the relay stays in the foot until a relay takes the
tab in, and a connection the relay ends at once does not shorten the wait before the next. The word
`passphrase` in the foot types the passphrase again, and is there only in a space with no notes yet.
A passphrase in doubt, a key that opens nothing here or one the relay refused that nothing here has
proved, says so, with its own way to type it again.

*Where the page comes from.* The relay carries the page, built into fzn from the files of the `web`
directory that make it; `fzn relay --web web` serves the directory instead while the page is worked
on, its files read again for every request. The relay names the page's build, sixteen hex digits of
a SHA-256 over its files, the same whether built in or read from a directory (where it is worked out
again at most once a second), and says it in `Fzn-Build` on every answer for the page and in the
page's `<meta name="fzn-build">`. A built-in file is sent with an `ETag`, answered with a `304`
while the browser holds it, as a gzip twin to a browser that takes one, and cached for a year when
its name is its hash; a directory's files are none of these. `HEAD` is answered as `GET` without the
body. A build of fzn with no page built (`web/build.sh`) carries none and says so as it is built and
when the relay starts, one whose page is older than the engine's sources says so as it is built, and
a release builds the page first. A tab asks which build the relay serves, with an uncached `HEAD` of
the page, when it comes back into sight and when its socket comes back to a relay that went away, at
most once in thirty seconds; a page whose meta is empty never asks, and one the browser gives back
from its cache of pages gone back from is reloaded whatever the relay serves. A newer build is
reloaded onto unless the person is in the middle of something: a body open and changed, a
composition, a key in the last two seconds, a field with the focus on a touch screen, or the tab out
of sight, which asks again when it is seen; while busy it tries again every two seconds. Before the
reload the field is kept, as `Keep` keeps it, and the page waits a second at most for the store to
hold it and takes no more actions. Where the page is goes into the history's entry as `kept`, a
second after the last change and at once when the tab is hidden: the path's steps as the engine
gives them, the line, the note under the cursor, how far the list was scrolled, the build wanted and
when. On load, before anything is painted, the page gives the steps back, types the line, puts the
cursor on the note it was on if that is still listed, and says nothing; the entries behind the one
it reloaded in lead back out, so a path put back shorter, for a note that has gone, goes back
through them to the depth it has. A reload that did not bring the build it was made for gives that
build up, the entries stepped into from there carrying it, and it is not reloaded onto again; nor is
a reload made within a minute of the last.

*The page.* One column, a little left of centre, with a third of the room beside it on its left,
text left-aligned, and the line a little above a third of the way down, where it stays: it does not
move when you type or when results arrive. A faint hairline under the line sets it apart from the
rows. The blank page is the caret and, under it in faint grey, the heads at the top of the list
fading out by the eighth. As you type, what is found takes their place; only a row that is new fades
in, and nothing else moves. The document never scrolls, at any width: the list scrolls under the
line. Only the rows that can be seen are made: those that fit and twenty more, at least twenty past
the cursor, and fifty more whenever the list is scrolled to within two rows of its end; a new list
starts again from what fits, and a row made because the list was scrolled or walked to it does not
fade in. The path, in a place, is in the margin above the line, `main › Boiler`: the space's name
and then each step, each a way back to it, the first to the top, a note's step read as its row is.
The tab's title stays `fzn`, or `fzn · garden` in another space. No colour: paper, ink and two
greys, light and dark by the system's preference. On a page at least 75rem wide the body of what the
cursor is on is read beside the list: the note under the cursor, or with the cursor on the line in a
place the note you are in. It is rendered as markdown, in the soft grey, from the top of the first
row's text, with no border, scrolled on its own and faded at its foot as the list is. A narrower
page has no room for it: a note's body is read there by opening it, and the note you are in is read
above the line, rendered, under the path and in the path's smaller size, clamped to six lines, and
opened with a tap, so that where you are is all above the line and what is found follows it
directly; the margin grows downwards only for a body longer than it has room for. At a phone's
width, 640px and under, the column is the screen's width with the line at the top, a long path gives
up its middle, the note you are in is clamped to three lines and, on a touch screen, hidden while
the keyboard is up, and the foot is a bar (see "Keys"). A narrow window with a mouse keeps it in
sight: it has the room, and a field there has the focus almost always.

*Back.* The path is the browser's history too: each step in, a note, a label or a body opened,
pushes an entry holding its depth, so the browser's Back, a mouse's back button or a phone's back
gesture closes an open body or steps out of a place, a step at a time, though unlike Escape it does
not first leave the row or clear the line. A step out made on the page, by Escape or the path, goes
back through the history rather than pushing, so the history never holds more entries than the path
has steps; nothing else is done to the history until the browser says it has gone, so a quick in,
out and in again settles where the keys left it. A step in pushes inside the gesture that made it.
Forward has nothing to go into and is gone back from at once.

*Markdown.* A body is rendered where it is only read and styled where it is written; the note stays
plain text, and neither the engine nor the terminal reads markdown. Read, a body shows headings,
paragraphs keeping the lines they were written on, lists, tasks as a drawn box, ticked and faint
when done and never pressed, quotes, rules, tables with their columns aligned, code highlighted by
its language, and in a line emphasis, strong text, code, strikethrough and links, built as nodes and
never as markup. A link goes only to a whole address, `http://` or `https://` and a host, or
`mailto:` and someone, in a tab of its own and with no referrer, since a browser takes `http:boiler`
as a path on the relay; any other address, a link by reference, raw HTML, an entity such as `&amp;`,
and `[[Boiler]]`, read whole as fzn's link, are shown as typed, and an image is never loaded but is
a link reading its alt text. A row's head reads as one line of the same markdown, so that a line
reads one way in the list and beside it: emphasis, strong text, code, strikethrough, links, bare and
bracketed addresses, an image as a link reading its alt text, escapes read and a leading heading
mark dropped, and a line that would be another kind of block, a list item or a quote, shown as
typed; the row under the cursor is its text box and shows the source. Tags, people and dates are not
marked. Highlighting is in paper, ink and the two greys: keywords and definitions heavier, strings,
numbers and constants soft, comments faint and slanted, punctuation and a diff's removed lines
faint; code read is set in ink. A link is in the colour of what it is in, underlined a pixel thin in
a tint of that colour and clear of the descenders, in full under the pointer; an address read as
itself is shown without its scheme, `example.org/garden`, and whole under the pointer. A long body
is rendered at first only as far as it shows, its first 5,000 letters, and whole once the page is
idle. A head is parsed only if it holds a character that can begin a mark, ``* _ ` ~ \ [ <``, `://`
or `mailto:`; nested past what the stack holds, a body is read as typed.

*Keys.* The four that are the app are the same, type, Enter, Escape and the arrows, and Tab goes in,
as in the terminal. The terminal's chords are not here, because a browser keeps most of them for
itself, but two it lets go: Ctrl-D clears the line or deletes the note under the cursor, and inside
the sheet it is the browser's, and vim's in vim mode; Ctrl-O opens the body of the note under the
cursor, and does nothing on the line. Ctrl with an arrow moves the note under the cursor a row,
PageUp and PageDown move the cursor by ten, and Backspace on an empty head takes the row away, all
as in the terminal. Shift-Tab does nothing, so no key takes the focus off the page's text. The page
is not marked as an application for assistive technology, which would stop a screen reader reading
the body beside the list as a document; the line and the head are text fields, and that says enough.
Every other thing that can be asked for is a word at the foot of the page, pressed or tapped, the
footer of the terminal made pressable. Only what applies where the cursor is gets one, and no key is
written beside a word; a word says its key when the pointer rests on it. Undo has no key: it is the
word `undo`, or `put back` on a row whose head has changed. At a phone's width the words are a bar
at the foot, one row of them, each as tall as a thumb, the way back (`back`, `up` or `done`) pinned
at its right and the rest scrolling under a fade when there are more than fit, going back to its
start whenever the cursor moves to another row; `move up` and `move down` there move the note. What
was said floats above the bar at the right, and nothing is drawn there when nothing is said.

*The line* is an ordinary text field, so the phone's keyboard and the browser's own editing work in
it. A cross after it clears it when it has text, and a `›` after the cross, in the column of the
selected row's, goes into what it names when it names somewhere. Each keeps its place while it is
not there, so that neither moves the other, or the dates, by coming or going; and since they are on
the line, clearing and going in are not also words in the foot.

*Writing in a row.* The head of the row the cursor is on is a text box, with the day its note was
written at the end of its row, or under the head at a phone's width, and typing changes it; Enter
opens a new row under it; Ctrl-O, or `body` in the foot, opens the whole note in the sheet, a
bounded overlay over the column with the page dimmed slightly behind it, which scrolls on its own,
and in it Enter is a line break, Tab indents and Shift-Tab takes the indent back, the arrows move
within it, and Esc closes it back onto the head. Leaving it for anywhere else on the page closes it,
and a press on another row goes there; a press in its own margin, or on its row behind it, does not,
and nor does leaving the window, though the tab going out of sight writes what it holds. The words
in the foot stay above the dimming, since they are about the note. There is no second editor: the
sheet is the body's editor, for a long document too, and with it shut the head is written in its
row. While the body is open the sheet holds a CodeMirror editor, fetched after the first pause in
keys once the page has painted, or when a body opens if that is sooner: the markdown styled where it
stands with every mark in sight, the marks in the faint grey and the text in ink, headings and
strong text heavier, emphasis slanted, code in the monospace, soft wrap, step undo, a grey
selection, and vim mode for the browser that asked for it with the word `vim` in the foot, which a
touch screen, whose keyboard has no Escape, is not offered. In vim mode `j` and `k` move over the
lines as shown, as `gj` and `gk` do, since a note's long lines wrap, but in a line-wise selection,
which goes by vim's lines. Esc is vim's alone; `:q`, `:wq`, `:x`, `ZZ` or the word in the foot
closes the body, `:q!` and `ZQ` put the text back first, and `:w` writes and stays, scrolled where
it was. The page's caret is drawn from the editor's coordinates, a block in normal and visual mode
and a bar in insert mode. Vim's prompt for a command is at the foot of the sheet, under the text and
a hairline. The head stays an ordinary field. Until the editor has loaded, and if it never does, the
sheet holds a plain text area. The `editor` the engine offers is not shown here. At a phone's width
the sheet is the whole frame above the bar, and the bar the sheet's colour while it is open; a wider
touch screen keeps it in the column, sized from the frame. On a touch screen it opens without the
keyboard, which a tap in the text raises; putting the keyboard away leaves it open, and `done`, the
back gesture, or a tap on the page beside it closes it.

*Touch.* On a touch screen, at any width, the page is a frame the size of the visual viewport, the
part of the screen the keyboard and the browser's bars have left, so that the foot sits on the
keyboard. The frame follows `visualViewport` through two variables, trusted only while a field has
the focus, since iOS has been seen to leave its offset where the keyboard put it; a browser that
shrinks its layout for the keyboard gives the same numbers. Every target is 44px each way, drawn
small and given its room. A tap on the words of a head puts the cursor on the note and the caret
under the finger, and the keyboard comes; a tap beside them, off the lines of the head's words as
they are laid out, a few pixels round them counting for a finger, puts the cursor there and nothing
more; and a second tap beside them, on the row under the cursor, judged by where the finger landed,
opens the body, though while the head is being written a tap in it is the caret's. The box at the
start of a task is drawn in its row on every width, but for the row under the cursor, whose head is
its text, and a tap or a click on it ticks the task where it reads, the cursor and the caret staying
where they were; its room for a finger grows into the margin before it and stops short of the head's
first letter. It is drawn a little larger than a letter, so that it reads at a glance. The `›` at
the end of the selected row goes into the note and the one after the line into what the line names;
a link in a row opens and leaves the cursor where it was; the path is the way back, and so is the
phone's own back gesture. The keyboard is the person's: the page moves the focus between fields
while one has it, and otherwise gives it only for something that asked to write, a finger on a
head's words, a key pressed on a keyboard joined to the device, or the word `new note`; no control
takes the focus, on any width, so the words, the cross, the arrows and the path's steps act on where
the cursor is and leave the keyboard as it was. The caret is drawn where the cursor is whether or
not the keyboard is up, still while it is down. There is no gesture of the app's own: tap, scroll
and the system's back are the whole of it. A manifest and an icon let the page be added to a home
screen as an app of its own, the browser's bars in the paper's colour, opening the space it was
added from. The passphrase's field waits for a tap, with the resting caret at its start, the
keyboard's key says `go`, and a word `open` under the sentence opens the space.

*Several tabs.* Each tab open on a space is a client of its own, with its own cursor and path and a
log of its own. Where the browser gives the page Web Locks, a tab holds the lowest slot no other tab
holds, a lock named `fzn:<space>:writer:<n>`, for as long as it is open, and writes under the id
kept for that slot, under `fzn.writer.<space>`, where a browser kept its one id before, for slot 0,
and `fzn.writer.<space>.1` and on for the rest, so that the next tab to hold a slot goes on with its
log; the browser lets the lock go when the tab closes or dies. A browser gives Web Locks only to a
page served over TLS or from the machine itself, and a relay elsewhere serves neither. Without them
a tab cannot tell a tab that has gone from one the browser has frozen, so it pools nothing: its id
is its own while the browser keeps the tab, in the tab's own storage, and is gone on with only when
the page before it said as it went, on a `pagehide` not into the back-forward cache, that it had
left. What it leaves is a token one page alone can spend, a random word in the tab's storage and the
same in the browser's under the id, taken as the id is gone on with, since a tab's storage can be
copied after its page has left, into a window it opened or by Duplicate, and only the first of the
copies to open may have the log. A reload, the reload onto a new build and a closed tab opened again
keep their log; a new tab, a duplicated one and one the browser killed write a new one. Over a
broadcast channel the tabs are peers of one another, as the relay is over the socket, and each has a
socket of its own: what one writes is listed in the others in the next frame or two, what one hears
from the relay it passes on, and with the relay gone they still converge. The store takes a line put
twice as one, and since a tab sends a line only once the store has kept it, the relay never holds
more of a slot's log than the store does. Nothing stands down and nothing has to be pressed; a page
of the build before this one, which knew one tab at a time, is told to stand down as it did.

Not yet: the page itself needs the relay to load, the graph view, and anything a browser only allows
a page served over TLS, which a relay does not do.

## Decisions
A dated log, oldest first. Each entry says what was decided, why, and what it replaced; a reversal
is its own entry that names what it replaces, and the old reasoning stays, because why something was
dropped is part of the design; an entry revised later ends by saying which entry revised it. What
the app now does is described above; an entry repeats it only where the reason needs it.

- **Rust, not Go and not Zig.** One codebase must run natively and in WASM with small binaries and
  no GC. A text CRDT per note is a day-one requirement once line and body are one text, and Yrs and
  Loro exist in Rust. wasm-bindgen, ratatui and the unicode crates cover the rest. Zig would mean
  hand-writing the CRDT, the JS bridge and unicode segmentation on a pre-1.0 language; none of that
  is the interesting problem. Go stays the language of the relay if pollen's transport is ever
  wanted there.

- **Line and body are one text.** The head is the first line.

- **CRDT from a library, behind a narrow trait.** Last-writer-wins on whole notes is not acceptable
  once notes are texts edited on two devices offline.

- **Graph, not tree.** Prefix paths are not syntax. Explicit structure is flat labels and links:
  tags, people, links. No note contains another. The old ` >> ` convention imports as plain text and
  is found by the lexical scorer.

- **No manual ordering (provisional).** The list was to be recency plus pins, so the position CRDT
  went, pending Sam's confirmation. Reversed 2026-10-15 by "Where a note sits is kept".

- **A link is a fact, not a score, and there is one kind.** Every link is the target's id held on
  the note, however it was made. Linked notes are listed ahead of suggestions, and no weight can
  bury one.

- **Position is the link.** Writing inside a note's place is how one note comes to be about another;
  nothing is typed and no key is pressed for it. The explicit chord is a correction. There is no
  marking of links while drafting: to put a draft somewhere, go there and write it, and the line
  comes with you. A Tab-to-link was considered and dropped, because it was one more thing to
  remember for a flow position already covers.

- **The engine sees only what the text carries, plus where you were.** Words, tags, dates and
  position are the free signals. The paths you walk between notes are the next one: going from A to
  B again and again is a relation expressed without saying so, and a scorer that reads it can still
  give its reason ("you came here from A"). Embeddings come after that, if ever. Neither before
  daily use shows the gap.

- **Archived is hidden until asked for.** Everywhere, typed results included. The engine still
  indexes archived notes; the lists leave them out.

- **Spaces, not one pile.** Notes that do not belong together live in separate spaces, and one of
  them is home.

- **Days are local.** A note's day is the day it was where it was written, kept by the offset in its
  times, and today is this machine's today. The offset comes from the system through one libc call,
  the only unsafe code in the app.

- **A name is a fact too.** Decided 2026-10-13. The note whose head is what you typed comes first,
  whatever it scores and whatever the limit, and says `its name`: every word of its head typed and
  no other, in any order and case, the last word allowed to be unfinished, so `boiler se` puts
  Boiler Service first and `boiler` alone does not. The scores could not be made to do this. In
  `main` the median note is 23 words and the longest is 7,493, so a document earns almost nothing
  for its own words and the notes that say its name outrank it: one document was fifth for its own
  name and another note eighth. Three settings of `bm25_b` across five names got none of them all
  right, the wins were by hundredths, and with no length penalty long documents turn up for searches
  that are not about them. A rule is also what lets "type the name, Down, Right" be done without
  reading the list. Notes that share a head all come first, in the order of their scores. Revised
  2026-10-16: the key is Tab ("Tab goes in"), and a head is also named, in a second tier, without
  the labels that end it ("A row hides the place's labels").

- **A name finds what was written in it.** Decided 2026-10-13. Typing `boiler` lists Boiler and the
  notes written in it, each saying `about Boiler`, and `boiler quoted` finds the one among them that
  says "quoted": typing a note's name shows what going into it shows, and the names after the arrow
  on a row count as words of that row. In 2022 this was the main way of finding: 85% of notes
  carried a ` >> ` path, most of them three to six labels deep, and any label gathered its group.
  Position now makes the link without the typing, so without this every note written in place would
  be out of reach of the word it was filed under, and a link would be a folder that has to be
  opened. The name is read when the search is made and not copied onto the note, so a rename is
  followed and the text stays as written. It goes one hop: `house` finds Boiler and not what was
  written in Boiler, which is what going into House shows and what the row says. A neighbourhood is
  not widened this way; what is linked is listed there already. The carry rule reads the same names,
  or a note found by one would take the search in with it as a draft. Measured on the 2022 notes
  with a hub made for every first label: up to a fifth more time per keystroke, and every keystroke
  under 3 ms.

- **The app is headless.** Decided 2026-10-13. Until then the engine was the part that finds things,
  and the app was the terminal: the carry rule, the path, what Escape does and what the footer
  offers were written against one terminal library's keys and widgets. A second face would have
  copied all of it and the copies would have drifted. Now the session is the app and a face cannot
  reach around it. Four calls shaped the interface. *Actions, not keys*: the terminal's chords
  cannot be a browser's, and touch has none, so the session is never told which key was pressed; "a
  chord acts on the line or on the selected note" is the terminal's way of saving keys and lives in
  its key table, and to the session clearing the line and deleting a note are two different things
  to ask for. *The face edits the line*: a terminal has to move a cursor through a string itself,
  and a browser is given a text field that already does, with the phone's keyboard and its
  autocorrect, so the session is told what the line now reads and nothing finer. *Effects out,
  nothing done*: a version to keep comes out as data and the face decides where it goes, so the
  session needs no file in a terminal and no database in a tab; the editor, the clipboard and
  opening a URL are asked for the same way. *The time is the host's*, and so are new ids, so a test
  stops the clock and nothing in the tests depends on when they run. Checked by running the binaries
  from before and after with the same 55 keys on a copy of `main`: the screens matched but for one
  reworded message, and so did the lines written.

- **A date is text, and `/fri` is a way of typing one.** Decided 2026-10-13, replacing `{d}`. The
  old token only wrote today, and that is what it was used for in the 2022 corpus. A note now
  records the day it was written, so the mechanism is for the days that are not today. There is no
  `/d`, which would be a code to remember. The word becomes the date when the note is saved and not
  as it is typed, so no key does anything unusual and the editor works the same way. There is no
  general slash-command system: one trigger, one job, until a second expansion is wanted.

- **Imported pages become the notes they would have been.** A page named only for a date or a time,
  a journal page say, is not a note: each thing written on it becomes a note of that day. A page
  that is nothing but a list becomes a note, with each item a note about it. Any other page is a
  document and stays whole, headed by its name. On the first two kinds a task is a note at any
  depth, headed by its box, archived if it was ticked and linked to the note it sat under (the box
  was taken off until 2026-10-16: see "A task is a note headed by its box"); other nested lines are
  the body; a bullet holding only tags hands them down. A note carries the tags in force where it
  was written, on the line under its head, and the tags its own line started with go there too: a
  head is what was said, not how it was filed. A line of nothing but tags does not count as a body
  in the list. Folders and front-matter tags are tags, except a folder that only holds dates, which
  is a calendar and not a subject. A `[[page]]` is rewritten to the head its page has now, or to the
  date if the page was a day. Pages that are programs for Silverbullet are left out. Importing again
  brings a note up to date with its page unless its text was changed here since, and keeps what was
  done to it here: its links, whether it is archived, that it was deleted, and where it sits.

- **A space is its logs, and sync is the rest of each.** Decided 2026-10-13. The other way is for
  the relay to keep one journal in the order things arrive and each device a cursor into it. That is
  less to build, but the cursor is state beside the data: lose the relay's journal, or put a device
  back from a backup, and the two no longer agree about what was sent. Here there is nothing beside
  the data to disagree about. It also makes every replica the same, so two devices can sync with no
  relay between them, and the relay has no logic of its own to get wrong. The cost is a rule that
  has to hold, one writer to a log, and the digest is there to notice when it does not. Merging is
  per note for now: two edits of one note made apart leave the newer one showing and the other in
  the log.

- **Sync is part of opening the app.** Decided 2026-10-13, at Sam's suggestion, over an `fzn sync`
  to be run beside it. A second thing to run is a second thing to remember, and one that fails
  without being seen. What it cost was two things to get right: only one of two open windows may
  sync, and a device syncs only while the app is open, which is when it is read and written anyway.
  The relay is one command for the same reason, and the same binary. Revised 2026-10-16 by "A client
  is a writer of its own, leased from a pool": which of the open windows syncs is settled by the
  keeper's lease, taken by the first and by the next when it goes. Revised 2026-10-17 by "A command
  syncs when no window does": a device is read and written by commands too while the app is shut,
  and a command with no window here to keep the space in step syncs once for its run.

- **The page crosses to the engine as text.** Decided 2026-10-13. An action goes in as JSON and the
  view comes back as JSON, with a hint's action being exactly what to send back to ask for it, so
  the buttons at the foot of the page are the view's hints and nothing more. The alternative was a
  typed binding for every part of the view. Text keeps the boundary to a handful of calls (seven
  then, sixteen by 2026-10-16), makes the page readable without the engine's types, and costs a
  parse that is far below a frame.

- **The relay hands out the page.** Decided 2026-10-13. A page loaded from the relay knows where its
  relay is, is covered by whatever lets a device reach the relay, and needs no second server. The
  page is a directory the relay is pointed at, not something built into the binary, so that changing
  how the page looks is a reload. Revised 2026-10-16 by "The relay carries the page; a directory
  stands in for it while the page is worked on".

- **Dumb relay, self-hosted, before the browser.** Decided 2026-10-13, reversing the order. A tab
  needs its notes from somewhere, and a tab is one more device, so what syncs two machines is also
  what gives a browser its notes.

- **Same repository.** The Go implementation is at the tag `go-final` and kept reachable; the Rust
  workspace took over the root on 2026-10-14. History is part of the project.

- **Compaction is a new log, not a rewritten one.** Decided 2026-10-14. Rewriting a log in place is
  a fork by the rule above, and any message saying "I rewrote it" is state a relay can miss. A new
  log under a new id, with a header naming the old ones, is read the same by a replica that was
  there and one that was not, and the retirement of the old logs follows from what is held. It also
  makes fork repair the same move, and gives the text CRDT the snapshot-and-go-on it will need. What
  it costs is the one line a replica reads.

- **The engine owns sync.** Decided 2026-10-14, at Sam's direction, replacing the files as the
  meeting point. The files bought Syncthing and a second window at the price of two readers of one
  directory, a lock on every write, and a "stale, start again" path. Neither is wanted: the relay is
  the way between machines, and a second window on one space is told to use the first. `Device` had
  already been written for the browser, where there are no files; it was the general thing all
  along, so it moved into the engine and the terminal drives it too. Revised twice on 2026-10-16:
  the app reads the directory while it runs after all, for logs it does not write, still with no
  lock per write ("A command writes beside the app, as a writer of its own"), and a second window is
  a writer of its own, not told to use the first ("A client is a writer of its own, leased from a
  pool").

- **Sealed on the device, from now.** Decided 2026-10-14, at Sam's direction. The lines were already
  opaque to the relay by design; sealing them costs a key and a passphrase prompt a device, and buys
  a relay that can sit anywhere. A passphrase, derived with Argon2id and a salt from the space's
  name, was chosen over a generated key copied between devices because a phone has a keyboard and
  not a clipboard from the terminal. There is no auth and no TLS yet, by the same decision: the key
  is what protects the notes, and the address is what protects the relay's disk and time. Revised
  2026-10-16 by "A space is entered with its key, and made with a token": there is auth now, made
  from the same key, and still no TLS.

- **Per-note last-writer-wins stays, for now.** Revisited 2026-10-14 when asked whether the CRDT
  could be made more robust. The stamp is already a per-note logical clock (a local change is always
  stamped past the version it replaces, whatever the wall clock says), and the merge is
  deterministic in any order. What is not robust is two devices editing one note apart, where the
  newer whole text wins; that is the text CRDT, which is milestone 4 and which compaction and
  sealing were shaped not to obstruct. Pulling Yrs or Loro in now would be the dependency decision
  Sam deferred, made in passing. The clock stepped a second at a time until 2026-10-15 (see "Stamps
  are kept to the millisecond").

- **Enter edits, the arrow goes in.** Decided 2026-10-14, at Sam's direction, replacing Enter as the
  zoom key. Enter on a row had gone into its neighbourhood, and editing a note meant the external
  editor. Now Enter opens the row to write in it, which is what Enter means in every spreadsheet,
  and the zoom is Right arrow: unmodified, universal in a terminal and a browser, and it walks the
  path the way the path is drawn. Sam had suggested a modifier with Z. Ctrl-Z is the browser's undo
  and forty years of suspend in a terminal; Alt-Z is reliable but carries no meaning and is the
  fallback if the arrow is refused; Shift-Enter and Ctrl-Enter are plain Enter in most terminals
  without the kitty keyboard protocol, so a key that works on one machine and not the next; Tab is
  how a page moves focus and what search groups want back. Enter now means four things by state,
  each visible, each named in the footer, and nothing about it depends on the key before it. Esc
  comes back a step, as before; Left does nothing on a row, so that there is one back key. Revised
  the same day, twice: see the next two entries. Reversed in part 2026-10-16 by "Tab goes in", which
  made Tab the key that goes in and answers the case against it here.

- **Enter opens the body; Right goes in from the end of the head.** Decided 2026-10-14, at Sam's
  direction, after a day's use, revising the entry above. Enter opens the body, since the head needs
  no key to be edited (see "The head is a field whenever the cursor is on it"), and Right goes in
  from the end of the head, where the caret lands, since Right short of the end is the caret. The
  reasoning on Ctrl-Z, Alt-Z, Shift-Enter and Tab stands, and Alt-Z is still the fallback. Replaced
  later the same day by the next entry, and the rule of going in only from the end by "Tab goes in"
  (2026-10-16).

- **Enter opens a row under; the body is Ctrl-O.** Decided 2026-10-14, at Sam's direction, later the
  same day, replacing the entry above. Enter on a row opens a new row under it, which is what the
  original's Enter did and the quickest way to write several notes in a run, and the body is Ctrl-O
  in both faces, the original's chord for exactly this. Two things decided it. Enter as the body had
  proved invisible in the browser: the opened note looked like the row with its body shown under it,
  so a day's use found no way to open a note. And a draft's Enter had been left to use to settle
  between the editor and another row; the original's hands wanted the row. The browser lets Ctrl-O
  go, as it lets Ctrl-D go, so the two faces keep one key for the body. The earlier argument that
  Ctrl-O was a second binding inherited rather than earned is answered: it is now the only binding
  for the body, and Enter has a better claim on the row. What stands: Right at the end of the head
  goes in (Tab since 2026-10-16, see "Tab goes in"), Esc comes back a step, Left does nothing on a
  row.

- **A note is written in place.** Decided 2026-10-14, at Sam's direction, which is what the original
  did and the revival had put off. Three calls shaped it. *Select, then edit*, not the original's
  one text cursor over the whole screen: typing on a row still went to the line, so browsing was
  never editing and Down then type still narrowed; the edit cost one Enter more. *A version is
  written when the row closes*, once, if the text changed: the original wrote a version a keystroke,
  which is a sealed line a character, and a timer would be the face's, writing at moments nobody
  sees. Undo is one entry a visit, and undo while the row is open puts the text back. *A new note is
  not written until it has text*: writing it at once would put an empty row on every device and a
  tombstone in every log for a thought that was not had. Esc keeps rather than cancels, because that
  is already the line's rule and because an editor's hands press Esc to finish. A note emptied is
  deleted, from the row and from the editor alike, replacing "left unchanged". The state of an open
  row lives in the session, so the rules are in one place, the hints can say "done" and "put back",
  and the flow is tested with the stopped clock. The terminal's in-place field is one line, the
  head, and "no inline multi-line editor in v1" stands: the body is Ctrl-O. The browser's row is a
  text box holding the whole note and is its only editor; the panel with Ctrl-Enter to save and
  Escape to cancel is gone, and with it the two text boxes with two Enters. The contention this
  brings with two devices editing one note at once is known and not for now. The first and second
  calls were replaced the same day (see "The head is a field whenever the cursor is on it" and "A
  head is kept as it is typed").

- **The head is a field whenever the cursor is on it.** Decided 2026-10-14, at Sam's direction,
  replacing "select, then edit" in the entry above, as in the original. Select-then-edit cost one
  Enter per fix and put a state between the cursor and the text; a day's use found the Enter in the
  way and the state a surprise when Down left a multi-line note instead of moving within it.
  Browsing is editing, and a stray key is one undo. What stands: a version when the cursor leaves
  the row or the body closes, once, if the text changed; a new note not written until it has text;
  Esc keeps; the session holds the field. What goes: the open and closed states of a row, Ctrl-O
  (Enter was then the editor's key, and a second binding for one action is inherited, not earned),
  and the gutter mark, since the caret itself is in the head. Undo while on a row puts a changed
  head back and otherwise is undo, one key for the last change written or not. The list holds still
  while the cursor walks it, so that the arrow that writes a head lands where it was aimed.

- **A new row opens under any row.** Decided 2026-10-14, with "Enter opens a row under; the body is
  Ctrl-O": not only at the top of the list, and an empty one goes from wherever it is when the
  cursor leaves it, the arrow landing past it as if the row had not been there. It takes nothing
  from the line, where the original prepended the search text to the new line: a place links what is
  written in it, which is the revival's way of saying what a note is about, and a search is not a
  place.

- **Backspace on an empty row takes it away.** Decided 2026-10-14, at Sam's direction. It steps up,
  to the row above or onto the line. It is what Backspace does to an empty line everywhere, and it
  is the quickest way back out of a run of new rows that went one too far. A note whose head is
  empty but which has a body stays, with a status saying why: the body is not shown in the head, and
  deleting it with the key being held down to clear a head would be a surprise. Ctrl-D is the key
  that deletes a note whatever it holds.

- **Enter with text on the line takes the cursor to the note it wrote.** Decided 2026-10-14, at
  Sam's direction, as the original's did, the caret at the end of its head. The note just written is
  the one most likely to be gone on with, and Enter there opens a row under it, so a run of notes
  starts from the line as it does from any row; left on the line, the cursor had nothing to say
  where the note went.

- **One cursor, no chrome.** Decided 2026-10-14, at Sam's direction, replacing the first page and
  the bordered terminal. The page had a header with a path, a prompt glyph, an underlined input, a
  titled list with a count, a date and a reason line on every row, a panel beside the list reading
  the first note unasked, and a foot of key badges, a count and a sync light; the terminal had three
  bordered boxes and two footer rows. Each was defensible; together they were a dashboard with the
  cursor one element among twelve. What stays is what the Vision says: a cursor, and what you typed,
  and what belongs with it. The page is one centred column with the line fixed a third of the way
  down, recent heads faint under it on the blank page, results that only fade in when new and
  otherwise hold still, the system's sans at three sizes, and paper, ink and two greys with no
  accent at all, since the accent had been doing five jobs that are now gone or are ink. The
  terminal is flat rows with the terminal's own cursor in the gutter of the selected row, its head
  bold, its body under it (moved to the foot of the list on 2026-10-15, see "A body is read apart
  from its row"), and one dim row of chords, in the terminal's own colours. What was dropped and
  why: counts answer nothing the rows do not; dates on every row say what the order already says; a
  reason line under every row makes a table; a panel is a second copy of the text and a second place
  to look; a sync light is the only thing on the page that is not text, and silence while it works
  says more. Revised 2026-10-15: the column sits left of centre ("A body is read apart from its
  row").

- **A head is kept as it is typed.** Decided 2026-10-14, at Sam's direction, replacing the call in
  "A note is written in place" that a version is written when the row closes, and with it that a new
  note is not written until the cursor leaves it. A new row looked unwritten until Enter or an arrow
  left it: no date, half the chords, nothing on any other device, and a key to press for what the
  page already showed. Now every change to a head is a version at once, a new row's from its first
  letter, as the original wrote a line a keystroke; there is nothing to press and nothing to lose.
  What it costs is the reason the call went the other way, a sealed line a character, and it is paid
  as the original paid it: a log grown past four times its notes is compacted when the device opens.
  Three things keep it from writing what nobody meant: date words are written out when the cursor
  leaves the row, so `/f` on the way to `/fri` is not made a date mid-word; a head cleared to be
  typed again is not written while it is empty, and a new row typed into and cleared goes as an
  untouched one does; and undo is still one entry a visit, the versions written while the cursor is
  on a row gathered into one. The body is not kept as it is typed: it is kept when it closes, or on
  `:w`, as the terminal's editor keeps a note when it exits, since a version is the whole note and a
  keystroke in a long body would write all of it. The status says nothing as a head is kept; the
  row's date appearing is the sign.

- **The body is a CodeMirror editor, loaded when a body first opens.** Decided 2026-10-14, at Sam's
  direction, revising the Browser description that the page is plain JavaScript and CSS "with no
  framework and no build step but the engine's". The body had been a textarea, which is what the
  no-dependency rule bought: nothing to build, nothing to vendor, and the phone's own editing. What
  it could not give was an editor: no vim mode, no step undo worth the name across a long note, and
  a caret the page had to find through a mirror of the text. The body is now CodeMirror 6, the three
  packages that are the editor and the vim mode, about 409 KB minified in one file under
  `web/vendor/`, built by hand from a pinned lockfile in `web/editor/` and committed, so that the
  relay, its machine and CI see static files and nothing is built on the way to the page. It is
  imported the first time a body opens and never before, and the textarea remains the body until it
  arrives and if it does not. The note stays plain text: no document model, and at first no markdown
  grammar (measured at 85 KB for syntax the notes did not use, and it would read `#house` as a
  heading), nothing rendered, and marking the engine's own syntax in place left until use asks; that
  part was revised 2026-10-15 by "A body's markdown is rendered where it is read and styled where it
  is written". The head field and the line stay ordinary inputs. The page's caret stays the page's,
  drawn from the editor's coordinates, and its shape says the mode, as a terminal shows vim's. Vim
  mode is this browser's choice, off by default. Esc kept its meaning at first: in normal mode the
  editor let it through and the page closed the body, so from insert mode it was Esc, Esc (revised
  the same day, see the next entry). `:w` writes and stays, as it does in vim, by closing and
  reopening the body under the hands with the caret and history kept, and shown as the engine keeps
  it, so a date word is expanded then. The relay's page policy allows inline styles for this, since
  the editor's own stylesheet is injected; scripts are still the page's own only. The other editors
  measured: Monaco at 4.6 MB and unsupported on phones; ProseMirror, TipTap, Milkdown, Lexical and
  Quill as document editors whose round trip through a schema changes plain text; Ace as a code
  canvas. What stands from the Browser description: no framework, the page as a directory the relay
  is pointed at, the line and the head as ordinary text fields, and the four keys. Revised
  2026-10-16: `:w` writes by `Keep` and does not reopen the body ("Keep writes and stays"), and the
  page is built into the relay ("The relay carries the page; a directory stands in for it while the
  page is worked on").

- **In vim mode Esc is vim's alone.** Decided 2026-10-14, at Sam's direction, revising the entry
  above: Esc never closes the body in vim mode. Hands that press Esc twice to be sure of normal mode
  closed the note under themselves. The body closes with `:q`, `:wq`, `:x` or `ZZ`, with `:q!` or
  `ZQ` putting the text back first, with the word in the foot, or by leaving it for somewhere else
  on the page.

- **A tag is a place you can go into.** Decided 2026-10-14, at Sam's direction. A tag, a person or a
  day typed on the line is gone into with the arrow at the end of the line, as a note is gone into
  from the end of its head, and the context is then the label: every note carrying it came first,
  newest on top, then what the engine found for its words, and a note written there ends its head
  with it. It closed two open questions. The first: was it a loss that a note added from a search
  for `#house` was plain and fell out of the list when the search ran again? It was. The original's
  search line was a standing context, and the prefix it put on every new line was what kept such a
  note in its list; going into `#house` is that standing context with the typing done once. The
  second: Right was dead at the end of the line. It now goes into what the line names, and does
  nothing when the line names nothing, so the key that walks the path steps onto it from the line
  too. A note is gone into by its name with the same key, by the rule in "A name is a fact too",
  which at the top is where Down and then Right would go, and inside a place that note is listed
  first whatever the neighbourhood and the limit, so the list and the arrow agree and typing a name
  is the way from any context to any note. Either way the line was a search for the place, so it
  clears on the way in and comes back to an empty line on the way out. What it reverses is "Escape
  never throws away what you typed", for the line: Escape with text on the line now clears it, and
  only on an empty line steps back. Three reasons: inside a standing context the common case is
  "found it, clear, stay", not "take this draft out"; Ctrl-D already threw the line away, so the
  line had no protection to lose; and a line costs a line to retype. That takes away one of the two
  reasons "A note is written in place" gave for Esc keeping what was typed on a row, that keeping
  was already the line's rule; the other, that an editor's hands press Esc to finish, stands, and on
  a row Esc still keeps. The step back itself still keeps what is on the line. Ctrl-D on the line is
  now a second binding for what Escape does while the line has text, kept for the hands that have
  it; dropping it is Sam's to call. The label lands at the end of the head, as it would have been
  typed, and not on the line under the head where the importer files tags, because the note then
  reads exactly as one written with the tag on the line has always read; a note that carries the
  label already, in its text or for a day by being written on it, is left as it is. Nothing is
  linked in a label, so Ctrl-L there does nothing but say so. Remembering the path across restarts
  is left out: the summon key in the open questions is the answer to coming back to where you were.
  A plain-word search as a context is left out, because it could only hand down the old prefix, the
  same words written on every line. Four calls here were replaced the next day, each by its own
  entry: the label added when the cursor left the row ("A new row in a label opens with the label");
  one label at a time, and a note on the path starting afresh ("Labels gone into one after another
  are one place", "A place is the last note on the path and the labels after it"); and the list on
  an empty line being the carriers and then what the words find ("A place lists what is in it;
  typing finds"). Revised 2026-10-16: the key is Tab, from anywhere in the line ("Tab goes in"), and
  a page keeps the path across a reload ("A face keeps the path as steps").

- **A new row in a label opens with the label.** Decided 2026-10-15, agreed with Sam, replacing the
  call in "A tag is a place you can go into" that a note written in place gets the label when the
  cursor leaves its row. A head is kept as it is typed, so the note was on every other device from
  its first letter, but without the label until Enter, an arrow or Escape left the row: until then
  every other device held `Bleed the radiators` with no `#house`, and its own `#house` did not list
  it. A label added on leaving reached other devices late, where a note's link never was, since it
  is in the note's first version. Now the row reads ` #house` from the moment it opens and the first
  letter writes `B #house`, so the note is in its place on every device at once and what the writer
  sees is exactly what is written. The label is text like any other: the writer sees where the note
  will go, and can delete the label, and then it stays out. The caret lands at the start, so that
  what is typed goes before the label. A day is put in only when it is not the day the note is
  written on, since that day is its `created`. A note written from the line still has the label
  added as it is written, since the line was the search and that is its one version. A label taken
  out a letter at a time from its end, as Backspace does and as a phone's keyboard only can, leaves
  what is no more the writer's than the label was, so nothing is written until a letter that is.
  What it costs: a new row holding only the start of its label, `#ho` in `#house`, is empty too and
  goes when it is left, and the label taken out from its start writes each step, as typing does.
  Since 2026-10-16 the box comes first among the open tasks, the caret after it, and how the labels
  are spaced does not count ("A task is a note headed by its box").

- **Labels gone into one after another are one place.** Decided 2026-10-15, agreed with Sam,
  replacing "one label at a time" and "only from the innermost place" in "A tag is a place you can
  go into", for labels. Going into today from inside `#house` reads as today's notes in that tag,
  and it was not: the list was today's notes, and a note written there carried today by being
  written on it and got no `#house`, so it fell out of the tag without a word. Now the labels at the
  end of the path, back to the nearest note on it or its start, are taken together: the facts carry
  every one of them, what the engine finds and the line's narrowing are for them all, and a note
  written there inherits each label it does not carry, in the order of the path, at the end of its
  head as one label always was. The path, going in, going back a step at a time and going back to a
  place already on it are as they were; nothing about the run is kept but the path itself. A note on
  the path still started afresh, never taken together with anything, so in `#house › Boiler › today`
  only today counted; that was replaced the same day by "A place is the last note on the path and
  the labels after it".

- **Where a note sits is kept.** Decided 2026-10-15, at Sam's direction, reversing "No manual
  ordering (provisional)". The empty line's list was in the order of `updated`, and so were a
  place's facts, which had two faults. A note written under a row stayed there only on the device
  that wrote it, and only until the cursor came back to the line: everywhere else, and then here
  too, it went to the top, so Enter, type, Enter, type wrote a run that did not stay where it was
  written. And an old note edited went to the top, so putting a word right reordered the list. The
  2022 corpus says the same from the other side: its order was kept by hand, because when a note was
  written was not what its place meant. So where a note sits is stored and replicated, as it was in
  the original fzn, whose tree CRDT comes back as one field with a clock of its own, `after` and
  `placed`, merged beside the text as the original kept content and position in separate registers
  (the mechanics are under Storage and sync). The empty line's list is the top of the order and an
  edit never moves a note, Sam's choice over a list of what was recently written beside it. A move
  is the original's, one note alone; what the line finds is in the order of its scores, so there
  nothing is moved and nothing is offered. The line format went to version 2, so that a build
  knowing only version 1 leaves these lines alone rather than rewriting a note without its place.
  `placed` was kept to the second, a note placed in the same second as another going a second past
  it, until later the same day ("Stamps are kept to the millisecond"). The pinned flag the model
  once listed was never built; moving a note to the top is what it was for. The order is walked
  afresh each time the list is made, O(n log n) in the notes: 5 to 7 ms over 30,000 notes in a
  release build, against under 1 ms for the sort by `updated` it replaces, so nothing is cached.
  Revised 2026-10-16 by "The order is kept": the walk is kept between lists.

- **Stamps are kept to the millisecond.** Decided 2026-10-15, at Sam's direction, revising what the
  logical clock of "Per-note last-writer-wins stays, for now" steps by, the times in the line
  format, and `placed` in "Where a note sits is kept"; the line format goes from version 2 to 3. A
  version's `stamp`, which decides whose text wins, and its `placed`, which decides whose place
  wins, are a moment past what is held for the note, or the clock if that is later. Moments were
  whole seconds, so the step was a second, and since "A head is kept as it is typed" a head is a
  version a keystroke: k keys a second for T seconds put the note's stamp about (k−1)·T seconds
  ahead of the clock, half a minute for a head of forty characters typed at speed, and inside that
  window another device's edit of the same note, made later without having seen this one, lost. Now
  a moment is unix milliseconds with its offset, the step is one, and both faces read the clock to
  the millisecond, the terminal from the system and the page from `Date.now()`, so a burst runs
  ahead by a millisecond a key at most. A line from before reads as it did, on the second, and two
  such versions in one second are still decided by writer id; days, and every time shown, are
  unchanged. The format went to 3 because a build of version 2 reads the fraction and drops it, so
  two versions of one note in one second, told apart here by their milliseconds, would be a tie
  there decided by writer id, and two devices would hold different text for the same note; a build
  that cannot read a line as it was meant leaves it alone instead. So every binary moves together,
  as for version 2; the relay passes lines on unread and does not care. The textbook form is a
  hybrid logical clock, wall time with a counter beside it that breaks ties within a tick and
  carries causality past a clock that is behind; this stamp already steps past what it has seen,
  which is the counter's work, and milliseconds take most of the rest of the gain (a step small
  enough that typing no longer outruns the clock) for the least change, one field of one type and no
  new field in the line. A clock that is simply wrong, by minutes or hours, still wins or loses by
  that much; that is said, not corrected (see the next entry).

- **A clock far from the relay's is said.** Decided 2026-10-15, at Sam's direction. A device whose
  clock is ahead stamps its versions ahead, and wins against later edits made elsewhere; one behind
  loses to earlier ones. It cannot be put right quietly: which of two versions is newer is decided
  on every device from the stamps alone, so a device that adjusted its own stamps, or read others'
  through an estimate of their clock, would decide differently from one that did not, and the space
  would hold two answers. What can be done is to make it seen, so that the clock gets set. The relay
  says its clock first (see "Four messages"), `Device`, which both faces drive, compares it with the
  `Host`'s, and the foot says `this device's clock is 5 min ahead`, or behind, where it would say
  `not syncing`, when the two are more than two minutes apart: well past any network's delay, and
  well short of what loses an edit that matters. It is forgotten when the connection goes and learnt
  again when it comes back. The clock was meant to be a field of the relay's first message, which
  says what it holds, but a build from before reads that message as exactly one key and would pass
  over one with a field it does not know, and what the relay holds with it; a message of its own is
  passed over by such a build as any unknown message is, and a relay from before sends none, so
  nothing is said. Nothing stored is touched.

- **A line of a later format is left as it is.** Decided 2026-10-15, in review of the move to
  version 3, revising "Compaction is a new log" in what a snapshot holds and "Lines are sealed
  before they are kept" in what proves a key. A line of a later version was to be left alone, not
  misread, but a line the key opened and the build could not read was taken as garbage. So a build
  of version 2, given a space of nothing but version 3 lines, said its passphrase did not open it,
  with the right one. And a build of version 2 whose own log held more than a thousand lines that
  version 3 had written on the same machine found the log grown past four times the notes it could
  read, and compacted it, leaving those lines out of the snapshot; the header then retired them on
  every device, and what that machine had written since was gone. Now an envelope the key opens
  proves the key, whatever is inside; a line of a later format counts as a note when a log is judged
  grown; and a compaction carries every such line into the new log as it was, after the snapshot,
  counted in the header. Nothing about a line this build reads changes. It holds from version 3 on:
  builds of version 2 behave as they did, so none is to open a space that a build of version 3 has
  written to on the same machine.

- **A place lists what is in it; typing finds.** Decided 2026-10-15, agreed with Sam, replacing the
  neighbourhood in two parts that the Zoom paragraph and "A tag is a place you can go into" listed
  on an empty line, the facts and then the engine's suggestions with their reasons. A zoom on a day,
  or on `#house › today`, showed lots of other notes: the suggestions were each label's carriers
  less the notes carrying them all, so the place never read as the set it is. Now every place works
  as the top of the space did, where an empty line is every note in your order with no reasons and
  typing finds, ranked and with reasons; facts come before finds, where before they were ranked
  together, and the row of nothing that was drawn between facts and suggestions is drawn where facts
  and finds meet. A fact carries no scorer's reason, as a row at the top carries none; how it is
  linked is a fact, not a score, and stays. Three reasons. The Vision ties finding to typing and
  going in to belonging: "as you type, the engine finds", and "go into one of those and write there,
  and what you write belongs to it". The top already listed a set on an empty line and found when
  typed, so a place that did otherwise was a second rule. And a label is typed on purpose, so a
  place is exactly what a note written there joins, and its list should be that and no more. What it
  costs: going into a note no longer shows the notes related to it until something is typed, and a
  note that says a tag's word without carrying the tag, `house move plan` in `#house`, shows only
  when typing. Since every row on an empty line is a fact, all of them can be moved, and Ctrl-L
  there can only unlink; a note to link is one the line found.

- **A place is the last note on the path and the labels after it.** Decided 2026-10-15, agreed with
  Sam, replacing "a note on the path starts afresh … in `#house › Boiler › today` only today counts"
  in "Labels gone into one after another are one place". A note on the path was never taken together
  with anything, so in `Boiler › today` the list was all of today's notes and a note written there
  did not link to Boiler: it fell out of Boiler, the fault that "Labels gone into one after another
  are one place" put right for labels earlier the same day. Now the place is the last note on the
  path, if there is one, with every label gone into after it, and the labels before the note are
  only how it was reached. Typing finds in the note's neighbourhood, with only the notes carrying
  the labels counted as facts, so a note linked to Boiler but written another day is a find in
  `Boiler › today`. The reasons: the same place reached by two routes should give the same note.
  What is before the note is already recorded, by the links that put the note where it is, so
  copying `#house` onto a note written in Boiler would be a second copy of it, and one that goes
  stale when a note moves. And how far up a path finding reaches is a question for search, which
  goes one hop down from a name (see "A name finds what was written in it" and the open question
  "One hop or the whole path?"), and is not written onto notes. The engine is asked about every
  place as one thing, a note, labels or both, where it had one call for a note and another for
  labels.

- **A body is read apart from its row.** Decided 2026-10-15, at Sam's direction, replacing the body
  shown under the row the cursor is on in both faces: in the terminal "its body under it, clamped to
  six rows" (the Terminal paragraph and "One cursor, no chrome"), and in the browser "the note under
  the cursor is read under its row, clamped to six lines". It also replaces "no panels" in the
  Principles' "One cursor", and on the page "There is no panel beside the list". A row that expanded
  the note under it as the arrows went up and down the list was hard to follow: every row below
  moved down and back at each step, a note with a body looked like several rows, and the eye had to
  find the list again after each key. Now the list is one row a note wherever the cursor is, and the
  body is read in a place of its own: in the terminal at the foot of the list, set in two columns
  from the rows' text so that it does not read as rows, and kept whenever the cursor is on a row so
  that walking the list never changes how many rows it shows; on a wide page beside the list; on a
  phone by opening it. To leave the room, the column sits left of centre, a third of the space
  beside it on its left, which Sam wanted anyway. The panel dropped on 2026-10-14 was a second copy
  of the text and a second place to look, read unasked even with the cursor on the line; this one
  says nothing until the cursor is on a note and is the one place a body is read, so that reasoning
  stands for what it was about. The view's `preview`, the note that panel read, had been read by no
  face since, and went.

- **The body read is that of what the cursor is on.** Decided 2026-10-15, at Sam's direction,
  revising the entry above, where the note you are in stayed under the path, and the Terminal
  paragraph, where "in a zoom the body of the note you are in sits under the line, dimmed and
  clamped". In the terminal that body still opened between the line and the list, about seven dim
  rows ending in a `…`: the fault the entry above was for, a body expanded under a line and pushing
  the list down. On the page, until 2026-10-14 the note you were in was read under the line, and Sam
  found it read as part of the line and put a gap between the line and the first result; from then
  until this decision a wide page read it above the line. Now nothing is drawn between the line and
  the list: the terminal's foot reads the note you are in, its rows kept as on a row so that the
  list keeps its height, and a wide page reads it beside the list and not above the line as well.
  And the status row, which had pushed the body and the list up a row whenever there was something
  to say, takes the last of the body's rows instead, so that a key that says something and the key
  after it, which clears it, do not change how much of the list is shown.

- **The line is told apart from the rows.** Decided 2026-10-15, at Sam's direction. With the cursor
  on the line, nothing said that the first row of text was where typing goes and the rows under it
  were not, and on the page the path above it was the only other text in the same column. In the
  terminal the line starts with a dim `>` in the column the rows leave empty, so the line reads as a
  prompt and its text lines up with theirs; nothing else moves. Tried: `·`, which then separated the
  app from the space on the path, and a row's reasons; `▸`, too near the path's `›`, and of
  ambiguous width, two columns in a terminal set for East Asian text; and `:`, which is vim's
  command line. `>` is one column everywhere and says prompt. On the page a hairline runs under the
  line, in a grey fainter than the faint one, and under the passphrase field, which stands where the
  line will be; the path above stays small and faint. No colour.

- **An open body is a sheet over the page.** Decided 2026-10-15, at Sam's direction, replacing
  "Ctrl-O grows the box to the whole note and takes the other rows off the page, so that the note is
  the page" in the Browser paragraph, and "the row is the editor" with it. The 2026-10-14 reason for
  taking the rows away was that rows only gone faint looked the same as a row with its body shown,
  and nothing said the note had opened; a sheet, a bounded editor over the column about the size the
  body had in the page with the page dimmed behind it, says it and leaves the list where it was
  underneath. What the keys and the engine do is unchanged. The sheet stays scrolled where it was
  through `:w`, where the row had gone back to the top of the note. A press in its margin, or on its
  own row behind it, keeps it open, so that the press does not close it for the tap to open it
  again. The words in the foot stay above the dimming, since they are about the note. The sheet is
  placed from the top and given a height, not pinned to the foot of the screen, which iOS Safari
  misplaces around its keyboard (WebKit bug 297779); a browser that honours
  `interactive-widget=resizes-content` shortens it while the keyboard is up. It is not a `<dialog>`,
  since a modal one makes the foot inert and its Escape cancels, which vim's Escape would have had
  to fight; it is an element with the dialog role. Vim's prompt for a command had floated over the
  text, since CodeMirror sticks its panels to the foot of whatever scrolls the editor, which was the
  page, so with a long body open Escape then `:` put the prompt over a line of the note; the editor
  is given the sheet's foot for its panels instead, one extension in its source and 64 bytes in the
  bundle.

- **A body's markdown is rendered where it is read and styled where it is written.** Decided
  2026-10-15, agreed with Sam, revising "The body is a CodeMirror editor, loaded when a body first
  opens" where it says "The note stays plain text: no document model, no markdown grammar (measured
  at 85 KB for syntax the notes do not use, and it would read `#house` as a heading); nothing is
  rendered". Sam asked for markdown rendered, at least in an open body, a link in a row shown as a
  link, and code blocks highlighted. Each reason for the old call has gone. CommonMark wants a space
  after `#`, so to the grammar `#house` is a tag too, at the start of a line as anywhere. Since the
  SilverBullet import the documents in `main` are markdown, so the syntax is used. And Lezer's
  markdown parser with GitHub's tables, tasks, strikethrough and bare addresses costs 39 KB taken
  directly (13 KB gzipped), not 85; `@codemirror/lang-markdown`, which brings the HTML, CSS and
  JavaScript parsers for HTML in a note, would have cost 233 KB. The note stays plain text, and
  neither the engine nor the terminal changes: this is how the page reads (what is rendered where is
  under Browser), one parse and one highlighting serving three places. The sheet is styled source,
  as iA Writer writes, which Sam chose over a live preview: every character stays where it was typed
  and in sight, so nothing moves as the caret walks, vim's motions go over exactly what is shown,
  and the caret the page draws from the editor's coordinates needs nothing new. Headings are set
  apart by weight and not size, so the page keeps its three sizes and every line is as high as the
  next, and a block of code, a tenth smaller in the monospace to match the sans by eye, keeps that
  height too. A body read is built as nodes and never as markup, and a paragraph keeps the lines it
  was written on, where markdown would run them together, since most bodies are lines written one
  under another. A row's head is read only as far as a list bears, since full markdown in the list
  would be troublesome. A link goes only to the web or to mail, an image is never loaded, and
  `[[Boiler]]` is read whole as fzn's link: the page never calls out ("Private by construction"),
  and the relay's `img-src 'self' data:` would refuse an image anyway. Code read is set in ink where
  the rest of a body read is in the soft grey, so that what would be copied out reads crisply. It is
  built as the editor was, by hand from pinned packages by one script, the same bytes from the same
  inputs, into committed files: `web/editor/render.js`, the parser, highlighter and renderer, which
  the page imports as it starts and never waits for, so everything reads as typed until it arrives
  and for good if it cannot be fetched; the editor, which imports the parser and highlighter from
  it, the two sharing chunks rather than each carrying a copy; and a file a language under
  `web/vendor/code/`, fetched the first time a block in it is read or written: shell, JavaScript,
  TypeScript, JSON, Python, Rust, Go, SQL, YAML, TOML, HTML, CSS, diff, Dockerfile, C and C++, and
  Java, each under its usual short names too, a Lezer grammar where there is one and otherwise
  CodeMirror 5's stream mode, as for shell, SQL, TOML, diff and Dockerfile, run by CodeMirror in the
  editor and by a few lines over its string stream in the page. `@codemirror/language-data` would
  have had every language, as about 124 files and 1.7 MB committed. Sizes raw, since the relay then
  sent no compression, and gzipped after: the page fetches 78 KB (27 KB) more as it starts, the
  render module and the two chunks it shares with the editor; the editor, 411 KB (133 KB) in one
  file before, is 392 KB (127 KB) fetched when a body first opens and 470 KB (154 KB) with what it
  shares; a language is from 0.3 KB for diff to 103 KB for C++, and the grammars share 27 KB of
  Lezer's parser. Only the one place a body is read that the page's width shows is rendered, and a
  long body at first only to its first 5,000 letters, whole once the page is idle: a note of 6,300
  words took 7 to 12 ms to render and 10 to 15 more to lay out, against 3 to 7 to lay out as plain
  text, and so cost the arrow that landed on it a frame; its first 5,000 letters take 4 to 6 ms,
  laid out. A head with no colon holds no address the page would follow and is not parsed: the 500
  rows the top of the list paints when the line is cleared took 5 to 6 ms to read as markdown,
  against well under 1 as text. Nested past what the stack holds, 1,500 lists or 3,000 quotes deep
  in Chrome, a body is read as typed. Revised 2026-10-16: a head reads every inline mark and is
  parsed when it holds a character that can begin one ("A head reads every inline mark a body
  does"), only the rows that can be seen are made ("Only the rows that can be seen are made"), and
  the relay sends the page compressed ("The relay carries the page; a directory stands in for it
  while the page is worked on").

- **Words find; everything else holds.** Decided 2026-10-15, agreed with Sam. It replaces "A date
  typed as a search finds the notes that mention the day and the notes written on it" under Model,
  and how a tag or a person typed on the line counted until then, as one more thing to score:
  `#house boiler` listed every boiler, the one in the house first. It revises "Search groups" in
  what stays from the original, and in "A place lists what is in it; typing finds" what an empty
  line is: a line with nothing on it to rank by. The grammar is under "The line". Words rank exactly
  as they did; everything else is something a note has to hold to be listed at all and changes
  nothing of how the words rank, so a new kind of filter, `!word`, archived or has a body, is one
  more form read from the line and one more way for a note to hold it, and the words are not
  touched; the box of an open task, `[ ]`, became the first such form on 2026-10-16. Membership of a
  span needs the moment a note was written, which the note relevance sees now carries in place of
  its day; nothing stored changed. `/-mon`, the last Monday before today, is new with spans, the
  mirror of `/fri`. The letters matcher is written here; a dependency for it would have been a
  decision of its own. A phrase that found a note in its body was a search, and so was `/today`,
  which until then went in as a draft. Labels are postings the index has, intersected once a
  keystroke; a span and a phrase are asked of each note left after that, a phrase last since it
  reads the whole text, and among the suggestions only until the limit is reached. Measured on
  synthetic spaces in a release build, each line typed a key at a time: at 10,000 notes every line
  stays under 4 ms a keystroke (the worst, a first letter of `~bo #house`, 3.6 ms; letters 2.5 to
  2.9 ms; phrases and spans under 1.7 ms but for a first key), against 1.2 ms for words before and
  after. At 30,000 notes words, labels, phrases and spans are 2 to 5 ms with first keys up to 12 ms,
  as words were before, and letters are 15 ms, two cold passes over every note: the heads, and the
  space's order for the ties. What it costs: a draft whose words find nothing carrying its tag lists
  nothing to step down to, where the tag alone used to list every note carrying it, so going into
  the tag first is the way to write there; `a` alone at the top lists the top of the space, where it
  used to list nothing; and a date word now ends at two dots, so `../fri` in a note is written out
  as a date. The original's Tab-separated groups and its `!` inverse stay unbuilt; the groups were
  given up for good on 2026-10-16 ("Tab goes in"). Revised 2026-10-17 by "Time on the line is
  time order": a line holding a span with nothing to rank by lists in time order, not the space's.

- **A span is not a place.** Decided 2026-10-15, agreed with Sam. A span holds the notes written in
  it, Sam's choice over the notes written or edited in it, and a note written now was not written in
  a span gone by: one written inside a span would fall out of it as it was written. So a line
  holding a span, a phrase or `~` is not gone into as a line of labels is, whatever else is on it:
  Right goes only to the note the line names, if one is named and holds the line, as on any line
  that is not labels alone, and letters name none. The key is Tab since 2026-10-16 ("Tab goes in").
  Since 2026-10-17 a span with `[x]` holds the tasks done in it ("When a note was done is kept, and
  `[x]` holds the done tasks"); a note written among the done tasks is done now, and so is in a
  span to now either way.

- **The arrow at the end of a line of labels goes into each.** Decided 2026-10-15, at Sam's ask, so
  that opening the app in the morning is one keypress: `#house /today`, Right, and the path is
  `#house › 2026-10-13`, one place by "Labels gone into one after another are one place". It goes
  into each label in turn as Right after each would, a label on the path before the place's note
  being gone back to, but of several a label the place has already is passed over, so that the rest
  are added to it and the path never repeats: `#house @joe` in `#house › today` goes to
  `#house › today › @joe`. One label alone is gone back to wherever it is on the path, as before:
  `#house` in `#house › today` goes back to `#house`. The line comes back on the way out of the
  first place it went into. Passing over a label typed alone too, so that `#house` in
  `#house › today` did nothing, was tried the same day and dropped at Sam's direction: typing a
  label on the path is how he goes back to it. The key is Tab since 2026-10-16, from anywhere in the
  line ("Tab goes in").

- **Export takes the line.** Decided 2026-10-15, agreed with Sam. `fzn export QUERY` writes the
  notes that line finds at the top of the space, every one of them and archived ones too, in the
  order the line lists them, one JSON line each as the plain export writes them, ranked with the
  config's weights. Archived notes are in because archiving is how a task is ticked, and a log of
  what was done wants them; each line carries the flag. Without a query the export is as it was. It
  takes no lock, so it runs while the app is open. Revised 2026-10-16 by "The line is a query, and a
  snapshot is a write": the line is asked with `fzn query`, and `fzn export` takes none.

- **A task is a note headed by its box.** Decided 2026-10-16, agreed with Sam. Until then a task was
  a note like any other, archived when it was done: the importer took the box off each one and
  archived the ticked ones, and since 2026-10-15 Sam had been typing `[ ] ` at the start of a head
  by hand and archiving the note when it was done. Nothing listed what was still to do, and nothing
  in a note said that it was a task. Now a task is a note headed by `[ ]` or `[x]`, and a `- [ ]` in
  a body is markdown and makes no note one. Four calls shaped it. *The box is text*: what you said
  goes in the text, and the test for adding to the agreed structure is whether a note still reads
  right on paper ("The note is plain text"); a ticked box does, where the archived flag beside the
  text does not. So the engine's syntax gains one form and the line format does not change: a build
  from before reads `[ ] Order oil` as a note that says so, and old and new binaries and pages sync
  side by side. The head has to start with it, so `Order oil [ ]` is not a task, nor a head with a
  space before its box, nor `[X]` typed by hand, though the importer reads that as `[x]`. *Ticking
  is archiving*: what is done is archived already, so the box and the flag say one thing and one key
  does both, in one version, so that no device sees the tick without the archiving or the other way
  round, and undo puts back both. The box can still be typed, and a build from before archives
  without ticking, so the two can disagree: an open task is one whose box and flag both say so, and
  the key puts a disagreement right. The foot says `tick` or `untick` where it said `archive` or
  `unarchive`, so that a phone, with no box of its own to tap, had the word; the box was drawn in
  the page's row later the same day ("The phone is a frame with a bar"). A tick changes the text, so
  it is an edit and dates the note's `updated`; archiving anything else still does not. *`[ ]` is a
  label*, read only whole, so that the list goes from the top of the space to the open tasks once,
  at the `]`, and a `[[` on its way to a link lists no tasks; it starts a word, as a date word does,
  and the index lists the open tasks, so the box costs what a tag costs. A done task is not a label,
  since `"[x]"`, a phrase, holds the done tasks with archived notes shown:
  `fzn query '"[x]" #house'` writes what was done in the house. *It is a place, as a tag is*, and a
  note written there starts with the box, since that is where a box is written; the session says
  where the caret goes, so that neither face has to find the box. The importer keeps the box,
  `- [ ] Renew passport` as `[ ] Renew passport` and `- [x]` or `- [X]` as `[x]` and archived, and
  is otherwise as it was. Importing the same pages again is how a space imported before gets its
  boxes back: a note whose text was not changed here since is brought up to date with its page,
  which now has the box, and keeps its links, whether it is archived, that it was deleted, and where
  it sits, since a page places nothing and the place held is newer than none. Its links are added to
  and never taken from, so a link to the note it sat under that was taken off here comes back.
  Archiving has not dated a note's `updated` since 2026-10-13, so a task imported since and archived
  here is still as its page gave it and gets its box back too, still archived; one edited here is
  left as it is, without one. A box that disagrees with its note's flag, archived by hand or by a
  build from before, or ticked on its page and brought back here, is put right by a rewrite done
  once through `fzn export` and `fzn import-jsonl`, with no code added for it; the rewrite keeps
  each note's `updated`, since when it was done is not known, and so that importing the export taken
  before it puts it back. Until every device runs this build, one from before archives a task
  without ticking it; unticking and ticking it here puts it right. Revised 2026-10-17 by "When a
  note was done is kept, and `[x]` holds the done tasks": `[x]` is a label, and a new note
  headed `[x]` is done as it is written.

- **A command writes beside the app, as a writer of its own.** Decided 2026-10-16, agreed with Sam,
  revising "The engine owns sync" and the storage rule that fzn is the only thing that writes a
  space's directory, where they say nothing is reread while the app runs. `fzn tick ID …` does to
  each note it names what Ctrl-V does to a task in the app, by the same code, and `fzn untick ID …`
  takes it back (see "The snapshot line is the way in and out, for good"). It takes ids and never a
  query, on purpose: a query that matched one note too many would tick it unseen. It is for the
  triage skill, which ticks feedback off while the app has the same space open and may have the
  terminal, so it asks for nothing and connects to no relay. It was a second writer on the machine,
  with an id of its own kept as `.commands` beside the app's `.writer` so that one run after another
  goes on with one log, and a lock on `.commands` so that two at once take turns, never the space's
  lock, which the app then held alone. An app from before this build does not look: it shows a tick
  once it is opened again, and a note edited there before then is written over the tick, as an edit
  made apart on two devices is. Why a writer of its own and not the app's log: two processes
  appending to one log would mean a lock around every save and the app reading its own log back
  after each, which is what "The engine owns sync" did away with; two writers on one machine cannot
  conflict, since a version settles by its stamp whichever log it is in. Why not through the relay:
  it would need a relay running and reachable, and the copy on this machine would lag by the relay's
  round trip. Why not a socket into the app: it would be a second way in, local only, kept beside
  the relay's. A log read while another process appends to it changed how a log's last line is cut,
  how a log gone from the directory is read, and who carries a retired log's lines on (see "Who
  writes a space's directory"). Undo in the app takes back only what the app did, so it cannot take
  back a tick, as it cannot an edit from another device; `fzn untick` is the undo. Revised the same
  day by "A client is a writer of its own, leased from a pool": `.commands` was the pool's second
  slot named by hand, and is slot 1; a command takes the first free slot and never waits. Revised
  2026-10-17 by "A command syncs when no window does": with a relay named and no window here holding
  the keeper's lease, a command takes the lease for its run and has one exchange with the relay; it
  still writes to this machine first, and beside a window it still connects to nothing.

- **The relay carries the page; a directory stands in for it while the page is worked on.** Decided
  2026-10-16, revising "The relay hands out the page" where it says the page is "not something built
  into the binary, so that changing how the page looks is a reload". The page is built into fzn, so
  a release is one file, nothing has to be found, and the page and the policy that serves it cannot
  be at different versions, which the hand-copied deploy once let happen: a page with the editor on
  a relay whose policy forbade its styles. `--web DIR` serves a directory instead, which is how the
  page is worked on, so the old reason holds where it mattered; in deployment a page change is a
  release like any other, and a tab reloads itself when the relay's page changes. A directory serves
  only the files that would be built in, never the whole of `web/`, where the bundles' sources and
  their `node_modules` live, so that a file the list lacks is missed while the page is worked on and
  not once it ships. The build is named by a hash of its files, so the same files put back are the
  same build and no tab reloads for nothing. The name is in a header and in the page's meta, since a
  page cannot read the headers of the response that loaded it; not in the websocket's first
  messages, which are about logs, nor in `Server-Timing`, which a browser hides over plain http.
  Gzip twins are made at build time so that the relay carries no compressor. The alternative, an
  archive unpacked to a data directory the relay looks in by default, was a second thing to install
  and a chain of places to look. What it costs: about 2.5 MB in every binary, 1.8 MB of page and 0.7
  MB of gzip twins, the terminal's included, which never serves it. How the page is served is under
  "Where the page comes from".

- **A space is entered with its key, and made with a token.** Decided 2026-10-16, at Sam's direction
  that other people should be able to use his relay, revising "Sealed on the device, from now" where
  it says there is no auth yet. The relay had no gate but its address: anyone who could connect
  could copy the sealed lines, add lines no device could open, and send a log whose header retired
  other writers' logs on every replica but each writer's own; and two people's `main` on one relay
  would have been one directory, the second told only that the passphrase was wrong. Now the relay
  challenges every connection and a device answers with a proof made from the space's key by the
  seal it already has, which the relay checks with an auth key it can read nothing with (the
  messages are under Sync), so nothing new is remembered and the passphrase stays the one thing a
  person types. The first device to answer for a space the relay keeps no auth key for claims it:
  that is every space on a relay when it first learns this, so it is trust on first use, bounded by
  whatever let the device reach the relay then and, once the relay has a token, by the token. A
  device does not claim with a key that has opened none of the sealed lines it holds, nor with one
  that opens none of the lines the relay offers: a passphrase mistyped on a new device would
  otherwise make its key the space's and shut out every device with the right one. One line a log,
  not one for the space, so that a log once written under another key cannot shut out the right one.
  That check rests on the device being honest and is for a person's own mistake; the gate against a
  stranger is the token. A space is made under a name the relay's admin gave a token, carried in the
  relay's address in the config and given to a browser once in a link, so that making spaces, and
  the disk they take, is the admin's to grant and two people's spaces are two directories; the
  relay's own spaces stay at its root, so every device that had them keeps them. The token is
  offered as a subprotocol, the one place a browser can put a credential in its greeting and one a
  proxy does not log as it logs a query. It is checked whenever a space has no key yet, when it is
  made or its key file has been lost, so a space that has its key is entered from any name with its
  passphrase alone, which is what sharing will need, and nobody without the token claims one that
  has none. The order of a connection lets a device tell why it was refused: the name and the token
  are settled before the challenge and the key only after the answer, so a refusal after its answer
  is a reason to doubt the passphrase and one before it is not, and the relay's own trouble after
  the answer is not said as a refusal at all. The relay passes over what a device says before its
  answer, because a device says what it holds as soon as it connects, so that it still syncs with a
  relay from before this, and says it again once it is let in. A build from before this is refused
  after the greeting, and the relay's log says that is the likely reason; every binary moves
  together, as for a new line format. The header stays unsigned, because membership of a space is
  the boundary: whoever has the key has the notes, and could compact a tombstone of every one. No
  quotas yet: the token already bounds who may take the disk, the first names are people the admin
  knows, and a quota is a number to choose and a full space to explain, for a problem nobody has
  had. Not covered: there is no channel binding, so a man in the middle can relay a proof live, and
  the one claim of a space crosses the wire as it is, so whoever listens then has the auth key and
  can pose as a member to that relay, which only the private network or TLS covers; and the auth key
  is symmetric, so a copy of the relay's disk lets its holder pose as a member to that relay until
  the key is replaced (see Open questions). Rejected: accounts with a password the relay checks, a
  second secret for a server that reads nothing, with registration and sessions to build; the
  private network's identity of a machine, which ties the relay to one network and has holes for
  shared and tagged machines; a directory named by a hash of the key, where a mistyped passphrase
  lands in a new empty space and writes there unseen; a key per device, which a phone has no
  clipboard to receive; a MAC on every line, a change to the line's format for what the gate already
  gives; a signature over the challenge, better against a stolen disk but a signature dependency in
  the page's build, and the upgrade if that is wanted, in the same message's shape; and a token on
  every connection, which would shut out every device that has none and stand in the way of sharing.

- **A release is one static binary that a script fetches.** Decided 2026-10-16, at Sam's direction.
  Three builds, static musl for Linux on x86_64 and arm64 and one for Apple silicon, made by a
  workflow on a tag `v<version>` equal to the workspace version, into a draft release that is
  published by hand after a look. `install.sh` is POSIX sh: it checks the archive's SHA-256, puts
  the binary in `~/.local/bin` by one rename so a running relay keeps the file it has open, touches
  no rc file and keeps no receipt, does nothing when it can tell that the version asked for is
  already there, and `--uninstall` takes the binary and leaves the notes and keys. `release.sh`
  builds and lays a release out as GitHub lays releases out, and the workflow runs it per platform,
  so a release built there and one built by hand cannot drift, and the installer can be pointed at
  either. One variable, `FZN_RELEASES`, names where releases live, since the repository may move or
  go private; a mirror directory is the answer to a private one, rather than teaching the installer
  tokens. `fzn --version` says the version, the commit and the day, so a machine can be asked what
  it runs. The version continues the Go app's line at 0.26.0, since its tags are in this repository
  and tools that take the highest tag as the latest would otherwise take `v0.25.5`. The release
  profile is fat LTO, one codegen unit and no symbols for the binary and the wasm alike: a third off
  the binary (3.6 MB to 2.4 MB before the page) and a quarter off the wasm (1,058 KB to 788 KB). Not
  `wasm-opt`, which took 6% more and would be one more downloaded tool rewriting the module every
  release carries. Not `panic = "abort"`, which would make a panic in one of the relay's connections
  the end of the relay, and not `opt-level = "z"`, which halves the wasm but is slower on the loops
  a keystroke runs. Not cargo-dist, which generates ten times the lines for the same three builds
  and an installer with a receipt and an updater; not signatures yet, since attestations need an
  enterprise plan on a private repository and minisign a key to keep: a signed tag and the sums are
  what there is. Intel Macs and Windows are left out until someone has one.

- **Tab goes in.** Decided 2026-10-16, at Sam's direction, reversing the part of "Enter edits, the
  arrow goes in" (2026-10-14) that made Right the key that goes in and rejected Tab, and with it the
  rule of "Enter opens the body; Right goes in from the end of the head" that it went in only from
  the end. Tab goes in, in both faces and from both places, from a row into its note and from the
  line into what it names, wherever the caret is, since a tab means nothing in a line of text and so
  needs no rule about the end; Right and Left are the caret's and nothing else. The earlier entries
  that say Right or the arrow goes in each point here; what each decided about where it goes stands.
  Three reasons. Tab is quick, one key in the same place on every keyboard, found by the little
  finger without looking. It matches what the line already is, a completion: Tab takes the
  completion in a shell and in every editor, and "type the name, Tab" is that gesture. And the
  arrow's two meanings, the caret's short of the end and the zoom's at it, were a rule to remember,
  the one the open question "Right at the end of the head as the zoom key, or Alt-Z" kept open: a
  hand that thought the cursor was on the line went in instead of moving the caret. What the
  2026-10-14 reasoning held against Tab is answered. A page moves focus with it, but when you are in
  fzn the keys bound to it are for fzn, so the page takes Tab as the terminal does and its words at
  the foot are pressed or tapped; the `›` after the line and on the row under the cursor stay, for
  touch. In the body's sheet, a text editor that goes nowhere, Tab indents, and Escape, `:q` or the
  word in the foot close it, which is the way out a keyboard trap needs. The original's
  Tab-separated search groups, unbuilt since the rewrite, are given up for good; its `!` inverse is
  still possible. Shift-Tab is not bound: Escape is the way back, a second key for it would be
  inherited rather than earned, and terminals do not agree on what they send for it. In a terminal
  Ctrl-I is Tab, so no chord sits on Ctrl-I, and were the kitty keyboard protocol ever turned on
  both would have to be bound. A pasted tab must not go in, which is one reason the terminal now
  takes a paste whole ("A paste is one change"). The footer says `Tab go in`, keeping the engine's
  label, which the page's word shares. Alt-Z is no longer a fallback, since Tab is everywhere. The
  Tab-to-link that "Position is the link" dropped stays dropped: this Tab links nothing. The open
  question is closed.

- **The path starts with the space.** Decided 2026-10-16, at Sam's direction. The path read
  `fzn › #house`, or `fzn · garden › #house` in a space that was not the usual one. The app's name
  says nothing the person does not know; the space's name says where they are, which is what the
  path is for. So the first step of the path is the space, in both faces, and on the page it is
  pressed to go back to the top, as every step is a way back to it; the page's title stays `fzn`, or
  `fzn · garden`, which is for the tab and not for where you are in it. A note's step on the page
  reads as its row does, a link in it read as its text, since a step is a button. At the top,
  outside any place, the terminal still draws no path row, since the space does not change while the
  app is open and what is not asked for is not on the screen; a dim `main` above the line at all
  times was weighed for knowing which space a window is on, and left to Sam to ask for. The terminal
  sets no window title: a window rule matches the terminal's class, which the launch sets
  (`--class fzn`), and a title that followed the path would change on every zoom. Were a summon key
  ever to want one, it would be the app's name and not the path.

- **A paste is one change.** Decided 2026-10-16, with "Tab goes in". The terminal had not asked for
  bracketed paste, so a paste arrived as keystrokes: a sealed version a letter, Enter at every line
  break writing the line and moving the cursor onto the note it wrote, and the rest typed into that
  note's head. Once Tab goes in, a pasted tab would have gone in mid-paste. Now the terminal asks
  for the paste whole, and turns that off around the editor and on the way out: it goes in at the
  caret as one change, one version, each tab and line break inside it made a space, since the line
  and a head are one line each and the body is where a paragraph goes, and the breaks at its ends
  dropped, since a line copied from a terminal or a page comes with its break. Two other readings of
  a line break were weighed and left to use to ask for: each line a note, the original's
  Enter-and-type rhythm, good for pasting a list and surprising for a wrapped paragraph; and the
  first line the head and the rest the body, which the body would keep only on closing.

- **The editor's file is yours alone.** Decided 2026-10-16. A note handed to the editor was written
  to the temporary directory with the process's umask, readable by every user on the machine, and
  left there when the editor exited with an error; an editor that could not be run, a mistyped
  `editor =` say, threw you out of the app. Notes are sealed on the device and this was the one
  place their text sat open on disk for others, so it is a fault of privacy and not of polish. Now
  the file is its owner's alone and always goes (see Terminal). It is made in `$XDG_RUNTIME_DIR`
  because on Linux that is the user's own and kept in memory, and otherwise in a directory of its
  own, so that the swap and backup files an editor makes beside the file are as private as the note;
  a runtime directory that does not take the file, as when it is left over from another user under
  `su`, counts as none. What the editor left is saved before the terminal is taken back, so that
  failing to take it back loses nothing. The editor is still a command split at whitespace, so
  `code --wait` works and a path with a space in it does not; the config's template says so.

- **A row hides the place's labels.** Decided 2026-10-16, at Sam's direction, revising "A name is a
  fact too" by one clause. Inside `#house` every row ended `#house`, which the path already says;
  now a row not under the cursor leaves the place's labels off the end of a head-only note (the rule
  is under "Zoom and places"). What counts is what a note written there would inherit, so what is
  hidden is exactly what the place would have added. The labels before the last note on the path
  show, since a note in `Boiler` need not carry them, and a label only typed on the line hides
  nothing, since rows would lose and regain it as `#ho` became `#house`. The box stays: it is at the
  start, not the end, it tells `[ ]` from `[x]` when archived notes are shown, and it is what a
  thumb ticks. A head that would be left with nothing, or only its box, shows whole, since a row of
  nothing says less than a row of its label, and a note with a body keeps its head whole, since that
  head is the note's name. The engine gives each row the head it shows, as a string, so that neither
  face carries the rule and the page does not slice characters by its own units. The path names a
  note by the same rule, without a heading mark at its start, as a rendered heading reads. Mail
  clients hide the label you are reading inside and show it in search results; task apps hide a
  project inside that project. What it revises: names come in two tiers, a head named by every word
  of it first, and by its words without the labels that end it only when no head is what was typed,
  so `order oil` names `Order oil #house` when no note is headed `Order oil`, and `order oil house`
  still names it. A second tier, and not a second name of the same rank, because a name must never
  stop naming the note it named: beside a hub `Standup`, the dated `Standup 2026-10-12` and
  `Standup 2026-10-13` would otherwise make `standup` name three notes and go into none. Inside a
  place, a note named only in the second tier is listed as named and gone into only if it carries
  the place's labels, since those are the rows that read so: inside `#house`, `order oil` goes into
  `Order oil #house` though `Order oil #garden` is called the same, and only the first says `its
  name`. Without the second tier a row would read one thing and answer to another.

- **A face keeps the path as steps.** Decided 2026-10-16, at Sam's direction, revising "A tag is a
  place you can go into" where it left out remembering the path across restarts. A page is reloaded
  under the person when a new build is served, and a reload must not throw them out of their place.
  So the engine gives the path as steps and takes them back as an action (see the engine's Session).
  A note is kept by its id, since a head is neither unique nor stable, and the search each step was
  found with is kept, so that stepping back after a reload brings back what it would have before. It
  is an action because it must leave an open field first, as any move does, and because the page
  crosses to the engine as text. The cursor's row, an open body and the undo stack are not in the
  steps: the list may differ under a new build, the body is written before the reload, and a reload
  is a restart. The page keeps the note under the cursor beside the steps and puts the cursor back
  only if that note is still listed ("The page follows the relay's build"). Steps are asked for, not
  put in every view, so the terminal, which is not reloaded, pays nothing.

- **Keep writes and stays.** Decided 2026-10-16. A face sometimes needs the note under the cursor
  written without the cursor moving: vim's `:w`, a tab being hidden, a page about to be reloaded.
  Until now the page closed the body and opened it again, which had to put back where it was
  scrolled and wrote through two actions. `Keep` writes the field as it stands, date words written
  out and an open body as a version, and leaves the body open and the cursor where it is; a new row
  with nothing in it has nothing to keep. A head emptied, or cleared to be typed again, is not kept,
  since only leaving the row writes that: a tab hidden mid-retype must not delete the note or make
  its body's first line its head.

- **The view says what both faces worked out.** Decided 2026-10-16, in the checkpoint's review of
  the engine. Both faces drew the row of nothing between a place's facts and its finds by the same
  rule written twice, and the page was about to carry the rule for a task's box; each was one rule
  in two places, which is how faces drift. The view now says where the finds begin and whether a row
  is a task and ticked. It also says no more than a face reads: the date of the cursor's row is the
  field's, not on every row, which was a seventh of a long view's JSON; the note you are in is its
  id and text, not the record with its stamps; and opening and closing a body are `open` and
  `close`, since `edit`, `edited` and `editor` were three words from one stem for different things,
  and `done` did not say what it did. The line is read, and the hints and dates made, once an
  action, where a terminal that asked for the view four times a key read the line four times. A
  session filled from nothing, as a new device is by its first sync, says nothing of it, however
  many logs the space arrives in, until something is first asked of it: the space arriving is not
  news, and `N changed elsewhere` is kept for what changes after. Revised 2026-10-17 by "A row's
  date is the day its note was written": the field's date is its `created`.

- **A head reads every inline mark a body does.** Decided 2026-10-16, at Sam's direction, revising
  "A body's markdown is rendered where it is read and styled where it is written" where it kept a
  row's head to its links and a leading heading mark, "only as far as a list bears". Sam asked for
  every inline mark wherever the cursor is not, and for links that look better. A head is not a
  title field: it is the first line of the note's text, and the same text is read in the body beside
  the list, so what a body renders a head renders, or one line reads two ways on one screen, as
  `<https://…>`, an image, an escape and `mailto:` did. The row under the cursor is already the
  editor, so the reveal Bear and Obsidian make span by span is fzn's cursor rule made row by row:
  nothing new to learn, and no text moving under a moving caret. Products whose rows are titles keep
  them plain (Things, Todoist, GitHub's issues), and GitLab took titles off its markdown citing the
  flaws inconsistent rendering let in; neither applies, since the page builds nodes and never markup
  and a head is not a title. So a head reads as one line of the body's markdown through the same
  code (see "Markdown" under Browser), and a line that would be a block of another kind stays as
  typed, since a head is not a block. A link has no colour, so its underline is its mark, as on
  paper, and the work is in making it quiet: in a tint of the text it is under rather than a fixed
  grey, so it reads alike under ink, soft and faint text. An address read as itself is shown without
  its scheme, as an address bar shows one, since the scheme is the part nobody reads. Rejected: a
  glyph after a link, which is chrome when `›` already means going in; cutting a long address in its
  middle, which the ellipsis at the row's end already does better. Tags, people and dates are not
  marked: the ones that only file a note are to be taken off the rows where they are noise, the ones
  left are words of the sentence, `Ask @joe about the fence`, which a grey would break, and the
  terminal marks none, so the page would be the only face to. `[[Boiler]]` stays as typed for the
  same reasons, and because a press on it would want to go into Boiler, which is the `›`'s job and a
  decision of its own. A head is parsed only if it holds a character that can begin a mark: measured
  over 10,000 invented notes, seven in ten of them marked, a marked head takes 7 µs to read, a plain
  one nothing to speak of.

- **The path is the browser's history.** Decided 2026-10-16, with the phone (Sam asked for the best
  that can be made there), on every width. A phone's back gesture, and a mouse's back button, are
  how a person leaves a place in every app they use; a page that ignored them left fzn altogether on
  the gesture that should have stepped out of `#house`. So each step in is an entry of the history,
  and Back steps out (see "Back" under Browser). A step out made on the page goes back through the
  history rather than pushing, so that leaving fzn by Back is as many presses as the path is long.
  Forward has nothing to go into, since the page does not keep what a step out left. A step in
  pushes inside the gesture that made it, since that is when Safari honours one. The entry's state
  holds the depth beside whatever else the page keeps there, which every write leaves alone. Not the
  address: a place in the address would have to be put back on load, which is the reload's work, and
  an address that named a note would put it in the browser's history, which a browser may sync off
  the device. Once a place could be saved and given back to the engine, an entry could hold its
  steps and Forward go back in; the steps the page keeps for a reload make that possible now, and it
  is not done.

- **Only the rows that can be seen are made.** Decided 2026-10-16, revising the costs given in "A
  body's markdown is rendered where it is read and styled where it is written". The engine lists up
  to 500 rows at the top, and the page made every one whatever fitted: clearing the line made 500
  rows anew with their style and layout, 28 to 47 ms over 10,000 notes, and the first Down restyled
  500 tails, so the two commonest keys after typing each took several frames against the principle
  that every keystroke resolves within one. Now the page makes what fits and a margin past it and
  past the cursor, so the row under the cursor is always there to follow, and more as the list is
  scrolled (see "The page" under Browser); a row made because it was scrolled or walked to does not
  fade in, since it is not new. Clearing the line now paints in 7 ms, typing a label in 2, and no
  key measured took more than two frames from its press to its paint, Playwright's hop included. A
  narrow page scrolled the document rather than the list and still made every row, until "The phone
  is a frame with a bar" left the document unscrolled at every width.

- **The order is kept.** Decided 2026-10-16, in the checkpoint review, revising "Where a note sits
  is kept" where it says nothing is cached. The walk of the order was most of what the most pressed
  keys cost: an empty line, a label alone, the way back and the return to the line each walked it, 4
  to 9 ms over 30,000 notes, which on a phone, two to four times slower, is a dropped frame. The
  decision not to cache was made for the desktop, before the page was meant for a phone. The order
  changes only when a version moves a note or a note arrives, so the walk is kept until then, and a
  note that arrives as the newest where it lands, as one written here always does, is put into the
  kept walk where the walk would put it, unless a note was waiting for it. Not when the space holds
  a cycle, whose head a newer note under it can change, and not past sixteen arrivals between two
  reads: putting a note in costs a pass over the notes, so a space arriving whole is walked once
  when it is next read, 30,000 notes in 19 ms, where slotting each in took over a second. With it a
  phrase and letters read a lowercased copy of each text made once, where each keystroke lowercased
  every note it asked. On the synthetic space of 30,000 notes an empty line went from 8 ms to under
  1, a label alone from 7 to 3, Enter from 9 to 3, letters from 18 to 8 and a phrase from 17 to 8.
  What is still over 5 ms there is a word of two letters being typed, whose prefix expands to
  hundreds of words; bounding that changes the ranking, and waits for a measurement on a real space.

- **The line is a query, and a snapshot is a write.** Decided 2026-10-16, agreed with Sam, revising
  "Export takes the line" and "The snapshot line is the way in and out, for good".
  `fzn export QUERY` was two things under one name, and the triage skill, the one script, had to be
  told never to leave the query off, since without it every note in the space is written out. Now
  `fzn query LINE` writes what the line finds and `fzn export` writes every live note and takes no
  line. There is no alias, since one name is one thing: a script that still says `export QUERY` is
  told the new name and nothing is written, where an alias kept quietly would one day be a whole
  space written to the wrong place. Ordering is not a form of the line: the line filters, the order
  is the space's or the scores', and `jq` orders for scripts, from `created`, `updated` and
  `archived`, which are on every line. For writing from outside, three shapes were weighed: waiting
  for the first real refactor, which would then be done through `import-jsonl` and the space's lock,
  or by hand against the app's rules; a set of verbs addressed by id, as `gh` has, six names
  restating what one line already says, each with its own all-or-none story; and the snapshot line
  as the way in, as `task import` and `kubectl apply` take it. The third adds one word, `fzn apply`,
  which takes back the lines `query` writes, by the app's own rules and all or none, beside the open
  app as a writer of its own as `fzn tick` does (the rules are under the engine's Session, the
  command under "The snapshot line is the way in and out, for good"). It refuses a note changed
  after it was read, so that nothing is written over unseen, and a move, since moving from outside
  waits until use asks; and it writes out each note as it now stands, so that
  `fzn query '#old' | jq … | fzn apply` is a change with its receipt. `tick` and `untick` stay as
  the common case, shorter and safer than a `jq` expression for what is done daily. Every command
  says what went wrong on one line and exits with a code that says whether anything was done, which
  is what a script or an agent checks, and a usage mistake no longer prints the manual. An MCP
  server, if one is wanted, is these commands as tools and nothing more. Revised 2026-10-17 by
  "Time on the line is time order": the order is time's on a line holding a span, so that a week
  needs no `jq` to read in order.

- **A client is a writer of its own, leased from a pool.** Decided 2026-10-16, at Sam's direction,
  reversing the rule that a space's directory is written by the app one window at a time and the
  Browser's "one tab at a time is the device", and revising "The engine owns sync", "Sync is part of
  opening the app" and "A command writes beside the app, as a writer of its own". A second tab made
  the first stand down and a second window was refused, because two processes appending to one log
  is a fork, and Sam wants several tabs and windows on one space, each in its own place, converging.
  Now every client, a window, a tab, a command or an importer, holds a slot of a small pool for its
  life, a lock and a writer id, and writes the log its slot names and no other, so a log still has
  one appender (a terminal's slots are under "Who writes a space's directory", a tab's under
  "Several tabs"). The next holder of a slot goes on with its log, so the pool grows to the most
  clients ever open at once and leaves no log a visit behind, as a fresh id for each client would.
  Web Locks exist only in secure contexts, so a page a relay serves over plain http on a private
  network has none; there a token a page leaves as it goes keeps one appender, at the cost of a
  small log for every tab that is duplicated, opened from another or killed, and then writes. What a
  client writes reaches its siblings as a peer's lines reach it, over a broadcast channel in a
  browser and through the directory in a terminal, which a window already read for `fzn tick` and
  now reads four times a second, so that a note typed in one window is listed in another within a
  quarter of a second. The relay's lines are logs too and want one appender, so in the terminal one
  window, the keeper, holds the socket and keeps them (see "A device syncs by having a relay
  named"). A socket for each window was the other way, and would have had two windows appending the
  relay's lines to one file, or a lock and a position on every append. In the browser a line has one
  key, so every tab keeps a socket of its own and the store takes a line put twice as one. This
  answers the reason "The engine owns sync" gave for telling a second window to use the first: the
  price it named was two readers of one directory, a lock on every write and a "stale, start again"
  path, and none is paid here, since the lock is one a client for its life (in a browser without Web
  Locks, the token), each file has one appender, and the directory is read forward from where the
  last read ended and never again. A leader tab or window that wrote for the others was weighed and
  rejected: a follower's note would be on no disk until the leader appended it, which wants
  acknowledgements and resending, state beside the data that "A space is its logs, and sync is the
  rest of each" was decided against. One device for every tab in a shared worker was rejected for
  the phone, which lacked one until lately, and for every keystroke's view crossing to the worker.
  Nothing is said on the screen about any of it: a second window or tab opens at the top of the
  list, and what another writes is `1 changed elsewhere`, as from any device.

- **The page follows the relay's build.** Decided 2026-10-16, at Sam's direction. A tab left open
  across a deploy, or one that Back returns to, went on running the page it was loaded with, and the
  only way onto the new one was a reload by hand, which threw the person out of their place. Now a
  tab reloads onto a new build where it was, unless the person is in the middle of something (see
  "Where the page comes from"). It asks when there is reason to think the build has changed: coming
  back into sight, and its socket coming back to a relay that went away, which is what a deploy
  looks like from a tab. A page given back from the browser's cache of pages gone back from is
  reloaded whatever the relay serves, since the other tabs let it go when it was put away. Text on
  the line, the cursor on a row and a head half written are not the middle of anything, since a head
  is written as it is typed and the rest is carried; a phone with its keyboard up is. The place goes
  into the history's entry as it changes, so that a reload by hand, or a page Back returns to that
  the browser did not keep, comes back where it was too; not as the page is unloaded, since Chrome
  keeps nothing written to an entry then. The guard means that a relay, or something between it and
  the browser, that names one build and serves another costs one reload and no more. Rejected:
  asking on a timer, which would ask all night of a tab left open for nothing the moments above do
  not already catch; naming the build over the websocket, which belongs to the engine and is the
  thing most likely to be down when the page is stale; a file or a route of its own for the build,
  since the page it is the build of already answers `HEAD`, and a deploy between fetching the script
  and asking would leave the tab believing it was new; and `sessionStorage` for the guard and the
  place, which works, but the history's entry goes with Back and Forward and so carries the right
  place with it.

- **The phone is a frame with a bar.** Decided 2026-10-16, at Sam's direction: he had not used the
  page on a phone and asked for the best that can be made there. The page on a phone was the desktop
  page squeezed: the document scrolled as a whole, the line stuck to its top and the foot's words to
  the foot of the layout, where iOS Safari leaves them under the keyboard, since it neither shrinks
  its layout for one nor implements `interactive-widget`; the words ran off the screen's edge with
  nothing to say so, the way back last among them; and the cross, the path's steps and the words
  were too small for a thumb. Now a touch screen has a frame the size of what the keyboard leaves,
  and a phone's width a bar of words at its foot with the way back pinned at the right, where it is
  found without looking (the mechanics are under Browser). The document never scrolls, so Safari has
  nothing to scroll when the keyboard comes, and the list scrolls inside the frame, so only the rows
  that can be seen are made there too, which ended the narrow page's exception in "Only the rows
  that can be seen are made". The bar goes back to its start when the cursor moves to another row,
  not only when its words change, since two rows mostly offer the same words and a bar left scrolled
  would hide the first of them. The line stays at the top rather than riding the keyboard as a
  message composer does, since the order path, line, what is found is the app's in both faces and
  the keyboard brings the line to the thumb anyway. Rejected: a bar moved by `visualViewport` under
  a scrolling document, the common fix, which leaves Safari a document to scroll on every focus that
  the sticky line, the bar and the sheet would each have to correct; and a bar wrapped to two rows,
  which would change height between states and move the list. The keyboard is the person's: a phone
  shows no keyboard for a focus nobody asked for, so the page types into nothing, or raises one
  under the hands, and iOS raises one only for a focus given inside the press; `new note` takes the
  focus because it would otherwise leave an empty row to be tapped a second time. Putting the
  keyboard away is not leaving, so an open body stays open, and a tap beside it closes it as a click
  does on a desktop. A tap beside a head's words picks the note and a tap on them writes in it; the
  second tap, which opens the body, is judged by where the finger landed, since a browser takes a
  tap that near a text box into the box: Chromium did, on a short head's own line and on the date
  under it, until the page read the touch itself. While the head is being written a tap in it is the
  caret's, so that a finger can put the caret at the end of the line. This closed the open question
  on touch. The caret is drawn while the keyboard is down, still rather than breathing, so that the
  blank page has its cursor before a tap; it is the same one cursor. There is no gesture of the
  app's own: swipe actions on rows, a long press to move and a pull to refresh were weighed against
  Mail, Reminders and Things and dropped, since a gesture that is not seen must be redundant with a
  word anyway, a sideways swipe fights the edge swipe back and the bar's own scroll, and sync is
  live, so there is nothing to pull for. A task's box is drawn in its row and ticks on a tap, or on
  a desktop a click: the one new control, and the row reading as it does on paper, where a phone
  could tick only by finding `tick` in the bar. The cursor and the caret stay put, so that boxes can
  be ticked down what the line found, and the box's room for a finger stops short of the head, so
  that a tap meant to write never ticks. The engine says which rows are tasks and where the box is,
  so the page does not judge a task by its text. This closed the open question on a box in the
  page's row. A manifest lets the page be added to a home screen; it names no address to start at,
  so an app added from a space opens that space. A home-screen app on iOS keeps storage apart from
  Safari's, so it asks for the passphrase once more, and reaches a space under a name only once the
  link with the token has been opened in it, which an app with no address bar makes awkward; that
  waits until someone with a name on the relay adds one. If such an app needs TLS to open
  standalone, that is one more reason on that pile (see Open questions). The word `open` under the
  passphrase is there so that a keyboard put away first is not a dead end, and `passphrase` stays in
  the foot on a phone, where the first design for the phone took it off as chrome, since it shows
  only in a space with no notes yet, which is where a mistyped passphrase makes a space of its own
  and typing it again matters.

- **When a note was done is kept, and `[x]` holds the done tasks.** Decided 2026-10-17, agreed with
  Sam, revising "A task is a note headed by its box" where it says a done task is not a label, and
  what a line carries under Storage and sync. Sam writes a
  weekly roundup for colleagues: what he did this week, which is mostly tasks, and many of them
  written weeks before they were done. `[ ] Fix the fence #work`, written three weeks ago and
  ticked on Wednesday, belongs in this week's roundup and not in the one of the week it was
  written. Nothing could list it: a span held the notes written in it, a done task was reachable
  only as the phrase `"[x]"` with archived notes shown, and nothing recorded when a task was
  ticked, since `updated` moves with any edit after it. Three changes answer it. *The moment is
  kept*: a note gains `done`, when it was archived, which for a task is when it was ticked, beside
  the text and not in it, since what you said goes in the text and what happened to the note goes
  beside it. The version that archives a note sets it, so ticking stays one version with the box
  and the flag together; bringing the note back clears it, and an edit leaves it, so putting a word
  right in a done task does not move it into this week. The session sets it whatever wrote the
  version, the key, `fzn tick` or `fzn apply`, so a script cannot give a done task a moment of its
  own, as it cannot give one an `updated`, and a query read and applied back still writes nothing.
  *A new note headed `[x]` is done as it is written*, on the line, in a new row anywhere, or from
  `fzn apply`, so that something finished that was never written down is logged straight into the
  week, and the faces and `apply` agree: for the whole of the visit that opened it its flag follows
  its box, so a box typed and taken out again leaves an open note. A note written before is ticked
  only by the key; typing a box into it changes its text and nothing else, as before. *`[x]` is a
  label*, the done tasks, as `[ ]` is the open ones: the notes headed `[x]`, archived or not, so
  that a box ticked by hand counts as well. Typing it asks for archived notes, since that is where
  done tasks are, and so lists them though archived notes are otherwise hidden; that is what the
  phrase with archived notes shown was standing in for, and the phrase still holds as any phrase
  does. It is a place too, where a new row opens with the box, `[x]  #work` in `#work › [x]`. A
  line of both boxes names no place, since no note is both. *A span measures what the line asks
  about*: the notes written in it, or with `[x]` on the line or in the place, the done tasks done
  in it, by `done`, falling back to `updated` for a box ticked by hand and never archived. So
  `[x] /-mon.. #work` is the week's done work in `#work`, whenever it was written, and `fzn query`
  of the same line is the roundup's draft. The box on the line changing what the span measures was
  chosen over a second form for when, `done:/-mon..` say, which would be a code to remember for the
  one case where the line already says it is asking about the done tasks. A box that disagrees with
  its flag is handled as before: a task archived with its box open is in neither `[ ]` nor `[x]`,
  and the key puts it right; a box ticked by hand and never archived is in `[x]`. The line format
  stays at version 3. A new version was weighed, as for versions 2 and 3, and not taken: a build
  that finds a later version leaves the line alone, so a build of 0.26 would have skipped every new
  line and, worse, gone on writing from its own stale copy of each note, over versions it could not
  see; while serde reads a line with a field it does not know and passes over the field, so a 0.26
  build reads these lines whole but for `done`, and nothing has to move together. A line that is
  archived and says nothing of when reads `done` as its `updated`, which is exact for a task ticked
  by a build from before, since the tick is in the text. What it costs: a build without `done` that
  edits a done note writes it without `done`, and it then reads as done at that edit's moment.

- **Time on the line is time order.** Decided 2026-10-17, agreed with Sam, revising "Words find;
  everything else holds" where a line with nothing to rank by lists what holds in the space's
  order, for a line that holds a span. The weekly roundup is read as a week: what was done on
  Monday, then Tuesday, `[x] /-mon.. #work` from the first thing ticked to the last, where the
  space's order put the fence fixed on Wednesday wherever the fence note had been written three
  weeks before. A span is a question about time, so a line that holds one and has nothing to rank
  by lists in the order of the moment the span reads, when written or with `[x]` when done,
  oldest first, as a diary and a log read; ties keep the space's order. `..` alone is a span of
  all time, so `..` is every note in the order it was written and `[x] ..` every done task in the
  order it was done, which is the way to read a space's history without the app's own order in
  the way. Oldest first, because a roundup and a diary are read forwards and a week copied out of
  the list should read Monday to Friday; newest first would put a span's last day above its
  first. With words on the line the order is the scores', as for any search, since what is typed
  is a question about what and not when. In a place the same rule orders the place's facts. These
  rows are not in the space's order, so moving a note is not offered among them, as it is not
  among ranked rows: a move there would land somewhere the list does not show. The top of the
  space is still cut at 500 rows in the app, and a list of time is cut from its old end, still
  listed oldest first, so that `..` in the app ends at now with the newest 500, where cutting the
  newest would leave a span to now without the week it was typed for; `fzn query` is not cut,
  which is where a whole history is read.

- **A row's date is the day its note was written.** Decided 2026-10-17, agreed with Sam, revising
  the view under the engine's Session, where the note under the cursor came with when it last
  changed, and "The view says what both faces worked out", where the date of the cursor's row was
  the field's, by which it meant the note's `updated`. The date on the row under the cursor, at the
  right in the terminal and on the page at the row's end or under its head, said when the note last
  changed, so `[ ] Fix the fence #work`, written three weeks ago and ticked on Wednesday, said
  Wednesday on a line of last month's notes, and putting a word right in an old note made it read as
  today's. Everything else that reads a day reads `created`: a day typed on the line holds the notes
  written on it, a span holds the notes written in it, and a note written in a day's place is of
  that day by being written on it. So the row says the day the note belongs to, the one those agree
  on, and `Field.updated` became the field's `created`, which both faces read. A note with no
  `created` shows no date, which is most of a space imported from the 2022 notes, where the time of
  writing survives on 7% of them: a date there would be a guess. When a note was last changed is
  still kept, and still what orders a ranked list's recency; it is only not said on the row.

- **A command syncs when no window does.** Decided 2026-10-17, at Sam's direction, revising "Sync is
  part of opening the app", where a device syncs only while the app is open, and "A command writes
  beside the app, as a writer of its own", where a command connects to no relay. A note written in
  the browser could not be found by `fzn query` on a machine whose app was shut, nor ticked by `fzn
  tick`, until the app had been opened there, and a tick made with the app shut reached the relay
  only when it next was: the triage skill read a copy of the space that was behind, and could not
  tick what it could not see. Sync waited for the app because a device was read and written only
  while the app was open, and that stopped being so once commands read and wrote without it. So
  `query`, `export`, `tick`, `untick` and `apply`, with a relay named and no window here holding the
  keeper's lease, take the lease for their run, connect, take in what the relay has that this
  machine lacks, do what they were asked, send what they wrote and whatever else the relay lacks,
  and close once the relay has heard it. A window holding the lease keeps the space in step already,
  and what a command writes beside it reaches the relay through it within a look, as before; the
  command never waits on a window and asks for nothing. Commands take turns at the lease, by a lock
  of their own that windows never take, since a command that found it held by another command would
  leave what it wrote unsent: the other has had its one exchange and does not look again. A relay
  that cannot be reached, refuses, or says nothing for a greeting's five seconds is said in one line
  on standard error, `not synced with nas.local: …`, and the command goes on with this machine's
  copy, since a copy a little behind is still an answer and what is written here goes at the next
  sync; one that works says nothing. Writing still goes to this machine first, so the reasons the
  earlier decision gave against writing through the relay hold. The cost is one exchange with the
  relay per command, a fraction of a second at home and a greeting's wait at most when the relay is
  away. The importers are left as they were: they write a space's start, once, and the app sends it
  when it opens.

- **In vim mode j and k go by the lines as shown.** Decided 2026-10-17, at Sam's direction. A body
  soft-wraps, and a paragraph is one line to vim, so `j` jumped a whole paragraph where the eye
  expected the next line. `gj` and `gk` are vim's own motions for the lines as shown, and what many
  editors' vim modes offer as an option, so `j` and `k` step as they do in normal and visual mode,
  and a count goes with them: `3j` is three lines down as they are laid out. A line-wise selection
  still goes by vim's lines, since it is made of them: with `gj` there the selection took in the
  whole paragraph at the first press and the next presses showed nothing, and `V3jd` took fewer
  lines than vim would. After an operator they keep the line-wise motion too, since `dj` taking
  part of a wrapped line would surprise; it still takes two whole lines. The cost is the line-wise
  `j` for whoever wants it, who has the arrows, or `+` and `-`.

## Open questions
- One CRDT document per note or one per space? Per note makes sharing a note trivial; per space is
  cheaper to load. Likely per note with a space-level index document; building the text CRDT
  (milestone 4) decides.
- Brackets that name a note which does not exist yet stay plain text until the note is saved again.
  Should creating the target link the notes that were waiting for it? The first link found missing
  that way decides.
- The carry rule ("the note contains what you typed" means it was a search) is a heuristic. Its bad
  case is clearing a one-word draft, which costs one word to retype; the case it must never produce
  is a note called `trai` under `trainingapp`. Daily use decides whether the rule holds.
- One hop or the whole path? With House › Boiler › a draft, `house` finds Boiler and not the draft.
  The 2022 habit was the whole path. One hop is what the row shows; use decides.
- Documents and one-liners share one ranking, and BM25 measures a document's length against a mean
  that the one-liners set, so a long document earns almost nothing for its own words. Its name now
  finds it; part of its name, or what it says, may not. Left until use shows it matters. `bm25_b` is
  the knob, and no setting of it was right on its own.
- A line kept as a page closes, the date words of a head or an open body, can finish being kept
  after the next page on the same log has read the store, since a browser lets a closing page's
  writes finish. That page then writes its own line in the same place, and the closing page never
  sent its line, so one edit is lost. The window is milliseconds, on a reload in one tab, and
  nothing cheap closes it; it waits for the first edit it loses.
- A fork is put right the moment it is seen, by compacting. Whether that is too eager, as when a
  file was corrupted rather than copied, is for the first fork to say; the cost of a needless
  compaction is one log replaced by a shorter one.
- Key rotation. A space's key is for life. Changing a passphrase would mean every device compacting
  under the new key and the relay keeping both until all have, which is the compaction machinery and
  nothing more, but nobody has asked for it.
- What the relay still sees (see "Lines are sealed before they are kept"). Padding lines to a few
  sizes would hide the sizes; nothing hides the timing. Padding is worth its bytes once people use a
  relay run by someone they do not trust.
- The relay's auth key is symmetric: a copy of the relay's disk lets its holder pose as a member to
  that relay, adding lines and retiring logs there, until the key is replaced. A signature over the
  challenge would close that in the same message's shape, for a signature dependency the page's
  build would carry; it is wanted once a relay's disk is somewhere its admin does not control.
- A shared space keeps its name on every device, because the key's salt is the name. Sharing will
  want the salt to travel with the space, or a space's name on a device to differ from its name on
  the relay; sharing (milestone 5) decides which.
- Quotas. A name may make as many spaces, as large, as it likes; its token is the only bound. A
  quota per name, with a compaction always let through so that a full space can shrink, is the
  answer once a name is someone the admin does not know.
- `wss://` from the terminal, for a relay behind a proxy that has TLS. The page already takes `wss`
  when it was loaded over `https`. The first relay put behind such a proxy asks for it.
- The browser's keys past the four, and Ctrl-D, Ctrl-O and Tab. Words at the foot, pressed or
  tapped, are a start: nothing to remember, but slower than a chord for archiving ten notes in a
  row. Undo has no key at all yet. On a phone every one of them is a word in the bar. Daily use of
  the page says which want a key.
- The phone, which nobody has used the page on yet. Whether WebKit there sends a second tap beside a
  head where Chromium does: in Chromium's and WebKit's touch emulation that tap was taken into the
  head's text box as often as not until the page read the touch itself, and `body` in the bar is the
  sure way meanwhile. And whether a home-screen app opens standalone over plain http. Use on a phone
  decides both.
- On a row Esc keeps rather than cancels, which spreadsheet hands will find out once; daily use
  confirms it.
- A head held changed while another device edits the same note loses one side to last-writer-wins
  when it is written, which the editor already risked. The text CRDT is the answer.
- A summon key. Starting should feel like switching a window, so the terminal build wants a
  keybinding that raises a running instance (a special workspace on Hyprland). Outside the app.
- Vim mode: whether the caret's shape is enough to say the mode, and whether the word in the foot is
  where the switch belongs; daily use with it on decides. Marking the rest of the engine's syntax
  (tags, people, dates) in place in the body, in the two greys, is a small decoration left until use
  asks; URLs and bracketed names are marked as markdown since 2026-10-15.
- Whether the body should be kept as it is typed too. A version is the whole note, so a keystroke in
  a long body writes all of it; it is kept when it closes, as the terminal's editor keeps it. Use
  will say whether that is felt.
- A log is compacted only when its device opens, and a head is now a sealed line a keystroke; a tab
  left open for days grows its log until it is reloaded. Compacting when grown, not only when
  opened, is the fix if it is felt.
- A fork in a slot's log. A space's directory copied to a second machine takes its slot files with
  it, and once both machines write in the same slot, two logs have one name. The relay finds it and
  the keeper says so, as for any log not its own, but only the slot's holder starts a new log for
  it, and a holder hears the relay only while it is also the keeper: a slot that only commands take
  beside a window, and the windows that do not hold the socket, leave what the second machine wrote
  there on it until a header retires the shared name. Copying a directory is not supported, so this
  waits until it is seen; the keeper compacting a sibling's forked log for it is one way to close
  it. A keeper holding the relay's lines of a sibling slot's log that are ahead of the disk, as
  after a power cut lost a tail that had reached the relay, appends them under that slot's lock when
  nobody holds it; when a sibling does, it leaves them, and the sibling appends at the gap and forks
  its own log, which is then put right as any fork is.
- A slot used once keeps a log for good: a log of a few lines a slot, on this machine and on the
  relay, as many as the most clients ever open at once on a space. Bytes, and so left.
- The clock is compared once a connection, when the relay's `now` is heard, so a clock set right
  while connected is still said to be off until the next connection. And the terminal hears the
  relay only between keys: a connection made again while the editor has the screen is heard when the
  editor closes, and the wait reads as this clock running ahead, two minutes after a long edit. A
  `now` said again from time to time is the fix for the first, and the time a message was heard
  going with it to the device for the second, if either is felt.
- Name. `fzn` stays unless something better appears.

## Milestones
0. **Data.** Done 2026-10-12. The 2022 corpus and the 2026 Silverbullet pages are in
   `~/fzn-corpus/`, outside this repo.
1. **The simplest live version, in the terminal.** Built 2026-10-12 as a Cargo workspace beside the
   Go code: `crates/fzn-engine` (model, index, three scorers, no I/O) and `crates/fzn` (the app:
   config, per-device log store, importers, terminal UI). The Silverbullet pages are the `main`
   space and the 2022 corpus is the `fzn-2022` space. Daily use from here; tuning follows use. The
   Go code was removed in one commit on 2026-10-14.
2. **Relay and sync.** Single binary, opaque lines, multiple devices. Built 2026-10-13: the exchange
   is in the engine as `Replica`, `fzn relay` is the relay, and fzn syncs with one whenever one is
   named. Running since 2026-10-13 as a system service on a machine that is always on, reached over
   a private network, where a note takes about a second to get from one window to another. Reworked
   2026-10-14: the engine owns sync through `Device`, lines are sealed, and compaction is a new log.
3. **Same core in the browser**, as one more device on the relay. WASM build, DOM renderer, ego
   graph. Built 2026-10-13 without the graph: `crates/fzn-web` is a tab as a device, and `web` is
   the page. Before that, the same day, the app moved out of the terminal into the engine as a
   session. `crates/fzn-engine` is now the index, the scorers, the note as it is kept, the session,
   the replica and the device that joins them; `crates/fzn` is the terminal face, the log files, the
   relay, the system clock, config and the importers.
4. **Text CRDT per note.** In the session and the note as it is kept, not behind the store (see
   "Later: a text CRDT per note").
5. **Sharing and collaboration.** The relay is ready for it: a space there is entered by whoever has
   its passphrase, whatever name they make their own spaces under.
