//! The terminal face: one line, the rows under it, and one cursor that moves between them.
//!
//! The device is the app. This is what a terminal adds to it: which key asks for what, editing
//! the line and the head of the row the cursor is on, painting the view, and carrying out what
//! the device asks for in return, which is keeping lines in the space's directory, saying things
//! to the relay, running your editor, the clipboard and opening a URL.
//!
//! The cursor is on the line or on a row. On the line, typing finds, Enter writes the line and
//! goes to the note it wrote or, with nothing on it, opens a new row at the top to write in, and
//! Down moves onto the list. On a row, its head is the field: the caret lands at its end, or, in a
//! new row that opens with the labels of the place, before them and after a task's box, and typing
//! changes it and is written at once, as it was in fzn, with its date words written out when the
//! arrows up and down, or Escape, leave it. Enter opens a new note under it, so that a run of
//! notes is Enter and typing; a new note left empty goes. Ctrl-O hands the note to your editor,
//! which is where a body is written. Tab goes in, wherever the caret is: on a row into its note,
//! and on the line into the tag, person, day, box or note the line names. Right and Left are the
//! caret's and nothing else. A paste is one change, made as if it were typed in one key, its
//! tabs and line breaks made spaces, since the line and a head are one line each and a tab
//! typed would go in. A chord acts on the note where on the line it acts on the line, as it did
//! in fzn: Ctrl-D deletes or clears, Ctrl-V archives, which ticks a task, or shows archived notes,
//! and Ctrl-U puts a changed head back before it undoes anything else. Control with an arrow moves
//! the note under the cursor up or down a row, and the cursor with it. Escape comes back a rung at
//! a time: off the row, off the text on the line, out of the place, and on an empty line outside
//! any place two of them quit. The footer shows only the keys that apply where the cursor is.
//! Shift-Enter on a row ticks as Ctrl-V does, where the terminal can tell it from Enter.
//!
//! A terminal sends some keys alike unless it is asked to tell them apart: Shift-Enter is Enter,
//! and in some it is lost altogether. So where the terminal says it has the kitty keyboard
//! protocol, the first of its flags is pushed, and inside tmux, which answers for the terminal and
//! has no such protocol, xterm's modifyOtherKeys is asked for, but only when tmux writes it in the
//! form crossterm reads, since the other form reaches it as nothing at all. Either is taken back
//! before the editor runs, on the way out and on a panic, because a terminal keeps it past the
//! alternate screen and the next program would inherit it. Under the flags Ctrl-I, Ctrl-M and
//! Ctrl-[ come apart from Tab, Enter and Escape, so each is bound to what the hand meant by it.
//!
//! With `vim = true` a row walked to is in vim's normal mode, where letters are commands and
//! nothing is typed, and a row written in is in insert, as without vim; the terminal's cursor is a
//! block in the one and a bar in the other. Normal mode keeps a caret in the head, which vim's
//! motions move as they do on one line of vim, so that typing starts where vim's hands expect,
//! and Escape there goes up to the line, as it goes off a row without vim. The line is always
//! typing, so it is as it is without vim, and every key in normal mode is a single key, so that
//! none depends on the one before.
//!
//! The caret moves and Backspace takes a letter as it is drawn, a grapheme cluster, so that an
//! accent goes with its letter; and everything is measured in the columns the terminal draws it
//! in, so that a Chinese head, two columns a letter, is cut and its date placed where they are
//! seen.
//!
//! The screen is painted by hand and flat, in the terminal's own colours, because there is
//! nothing to frame: the terminal's cursor in a bold head is the row being written in, and dim is
//! what can be ignored. The line starts with a dim `>` in the column the rows leave empty, so that
//! it reads as where typing goes and its text lines up with theirs. Nothing is drawn between the
//! line and the list, nor under a row, where a body would open and close as the cursor walks past
//! and move every row below. The body read is that of what the cursor is on, the note under it or,
//! on the line in a place, the place's note, in rows kept at the foot of the list, the same rows
//! whatever the note holds. A status row is there only while there is something to say, and each
//! key clears what the one before it said. The row under the cursor is one row for the same
//! reason, however long its head: a head too long for it reads from its start, cut, until a key
//! writes in it or moves its caret, and then scrolls sideways under the caret as any one-line
//! field does, so that text moves only in answer to the caret; and the foot reads it whole.
//!
//! Several windows may have a space open, each a writer of its own in a slot of the directory's
//! (see `store`). Between keys, four times a second, a window looks in the space's directory for
//! what another writer on this machine has added, another window or `fzn tick` say, and that
//! goes to the device as the relay's lines do: that is how windows see each other. With a relay
//! named, the one window that holds the keeper's lease has a thread holding the socket, and hears
//! from it between keys, waiting on the keyboard a twentieth of a second at a time; another window
//! takes the lease within a look of its going. What the relay says goes to the device, and what
//! the device wants kept or said comes back here to be done, like anything else it asks for.
//! Nothing is said while sync works, and `not syncing` while it does not, in the corner of the
//! foot that also says when this machine's clock is far from the relay's; a window that does not
//! hold the socket says nothing of sync, since what it writes reaches the relay through the one
//! that does.

use std::fs;
use std::io::{IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use fzn_engine::{
    Action, Device, Editing, Hint, Key, Moment, RELAY, Row, Todo, View, Weights, time,
};
use ratatui::crossterm::cursor::SetCursorStyle;
use ratatui::crossterm::event::{
    self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, enable_raw_mode, supports_keyboard_enhancement,
};
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::text::{Line, Span};
use ratatui::{DefaultTerminal, Frame};
use unicode_segmentation::UnicodeSegmentation as _;
use unicode_width::UnicodeWidthStr as _;

use crate::store::{self, Keeper, Logs, Slot};
use crate::sync::{self, Link};
use crate::system::System;

/// How long the keyboard is waited on before the relay is listened to, when there is one.
const TICK: Duration = Duration::from_millis(50);
/// How often the space's directory is looked at for what another writer here has added. It is
/// how long a note typed in one window takes to be listed in another, and to reach the relay
/// from a window that does not hold the socket, so it is short; a look is a listing of the
/// directory and a read of each log from where the last one ended, well under a millisecond.
const LOOK: Duration = Duration::from_millis(250);
/// The most rows the body of what the cursor is on takes at the foot of the list.
const PREVIEW_ROWS: usize = 5;
/// The mark before the text of the line, in the column the rows leave empty, so that the line
/// reads as where typing goes and the rows' text lines up under its own.
const PROMPT: &str = ">";
/// What an empty space says, until the first key or the first note to come from elsewhere.
const NO_NOTES: &str = "no notes yet: type and press Enter, or see fzn --help to import";
/// What the status row says for as long as the passphrase opens nothing the relay has sent.
const WRONG_KEY: &str =
    "the passphrase does not open this space: quit, and run fzn key to enter it again";

pub struct App {
    device: Device,
    logs: Logs,
    /// The space's directory, where the keeper's lease is taken.
    dir: PathBuf,
    /// The slot this window writes in, held for as long as it is open.
    slot: Slot,
    /// The relay named for the space, which this window dials once it holds the keeper's lease.
    relay: Option<Relay>,
    /// The keeper's lease, while this window is the one that holds the socket to the relay.
    keeper: Option<Keeper>,
    editor: String,
    /// The space's name, which heads the path.
    space: String,
    /// Cursor position in the line, in letters as drawn.
    cursor: usize,
    /// Cursor position in the head of the row the cursor is on, in letters as drawn.
    row_cursor: usize,
    /// The first letter of that head drawn, once a key has written in it or moved its caret; until
    /// then, from landing on the row, `None`, and the head reads from its start.
    row_left: Option<usize>,
    /// The first letter of the line drawn, which moves only as far as keeps its caret in sight.
    line_left: usize,
    /// The first row painted, kept so that the list moves only when the cursor would leave it.
    top: usize,
    /// Whether the previous key was Escape, which makes a second one quit.
    escaped: bool,
    /// The relay this window syncs with, as the status row names it, and the link to it: only
    /// while it holds the keeper's lease.
    sync: Option<(String, Link)>,
    /// Whether the relay has been lost or could not be reached, and is not back yet.
    not_syncing: bool,
    /// Why the relay could not be reached, said until it is.
    unreachable: Option<String>,
    /// When the space's directory was last looked at for what another writer here added.
    looked: Instant,
    /// What the terminal was asked for so that it tells Shift-Enter from Enter, once it has been
    /// asked; until then, as in tests, nothing was.
    keyboard: Option<Keyboard>,
    /// Whether the list takes vim's keys.
    vim: bool,
    /// With vim's keys, whether the row the cursor is on is in normal mode rather than insert.
    normal: bool,
    /// The cursor's shape as it was last set, which only vim's keys set.
    shape: Option<SetCursorStyle>,
}

/// What the terminal was asked for so that it tells apart keys its plain encoding sends alike,
/// Shift-Enter from Enter above all. It is decided once, when the app first takes the terminal,
/// and asked for again from that answer each time the terminal is taken back from the editor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Keyboard {
    /// Nothing: Shift-Enter is Enter, where it arrives at all.
    Plain,
    /// The kitty protocol's first flag, pushed where the terminal says it has the protocol.
    Kitty,
    /// xterm's modifyOtherKeys in its first mode, asked of a tmux that writes it as CSI u.
    Modified,
}

/// The relay named for a space: how the status row names it, and how to start the thread that
/// holds the socket to it.
struct Relay {
    host: String,
    dial: Box<dyn FnMut() -> Link>,
}

/// What the act of a key is measured against: the line, the note under the cursor and whether
/// it is a new one, as they were before it.
struct Before {
    line: String,
    on: Option<String>,
    fresh: bool,
}

impl Before {
    fn of(view: &View) -> Self {
        Self {
            line: view.line.to_owned(),
            on: under_cursor(view),
            fresh: view.field.as_ref().and_then(|field| field.fresh).is_some(),
        }
    }
}

enum Flow {
    Continue,
    /// A note is to be opened in the editor, which needs the terminal.
    Editor {
        id: String,
        text: String,
    },
    Quit,
}

/// Where the cursor is, which decides what a key asks for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    Line,
    /// On a row, whose head is the field.
    Row,
}

/// A key as the hand meant it. Under the kitty flags Ctrl-I, Ctrl-M and Ctrl-[ arrive as
/// themselves, where a terminal otherwise sends Tab, Enter and Escape for them, so they are made
/// those keys again; and Ctrl-Backspace, which they make a Backspace with Control held where it
/// was Ctrl-H, takes a word as Ctrl-W does, as it does in the page's fields. Ctrl-H is left alone,
/// since a few terminals send it for Backspace.
fn meant(key: KeyEvent) -> (KeyCode, KeyModifiers) {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    match (key.code, ctrl) {
        (KeyCode::Char('i'), true) => (KeyCode::Tab, KeyModifiers::NONE),
        (KeyCode::Char('m'), true) => (KeyCode::Enter, KeyModifiers::NONE),
        (KeyCode::Char('['), true) => (KeyCode::Esc, KeyModifiers::NONE),
        (KeyCode::Backspace, true) => (KeyCode::Char('w'), KeyModifiers::CONTROL),
        (code, _) => (code, key.modifiers),
    }
}

/// What a key asks for. A chord acts on the line when the cursor is there and on the note under
/// the cursor otherwise. `empty` is of the text the cursor is in, the line or the head. Tab goes
/// in wherever the caret is, since a tab means nothing in a line of text: from a head into the
/// note, and from the line into what the line names, which the session decides. Shift-Tab is
/// not bound, Escape being the way back. Shift-Enter on a row ticks, as Ctrl-V does, and on the
/// line is Enter, as it is wherever the terminal cannot tell it from Enter. Ctrl-/ opens a URL,
/// and so do Ctrl-7 and Ctrl-_, since a terminal sends Ctrl-/ and Ctrl-_ as one byte that
/// crossterm reads as Ctrl-7; under the kitty flags Ctrl-_ comes as Ctrl-Shift-minus. Backspace
/// takes a row away only when its head is empty, as a new row with only its labels is while the
/// caret is where it landed; with text in the head it is the caret's. Escape comes back a rung:
/// off the row, then off the text on the line, then out of the place.
fn bound(
    code: KeyCode,
    modifiers: KeyModifiers,
    place: Place,
    zoomed: bool,
    empty: bool,
) -> Option<Action> {
    let on_line = place == Place::Line;
    let ctrl = modifiers.contains(KeyModifiers::CONTROL);
    let shift = modifiers.contains(KeyModifiers::SHIFT);
    Some(match (code, ctrl) {
        (KeyCode::Enter, false) if shift && !on_line => Action::Archive,
        (KeyCode::Enter, _) => Action::Write,
        (KeyCode::Tab, _) => Action::GoIn,
        (KeyCode::Backspace, _) if !on_line && empty => Action::Erase,
        (KeyCode::Char('o'), true) if !on_line => Action::Editor,
        (KeyCode::Esc, _) if !on_line => Action::Select(None),
        (KeyCode::Esc, _) if !empty => Action::Type(String::new()),
        (KeyCode::Esc, _) if zoomed => Action::Back,
        (KeyCode::Char('d'), true) if on_line => Action::Type(String::new()),
        (KeyCode::Char('d'), true) => Action::Delete,
        (KeyCode::Char('v'), true) if on_line => Action::ShowArchived,
        (KeyCode::Char('v'), true) => Action::Archive,
        (KeyCode::Char('c'), true) if !on_line => Action::Copy,
        (KeyCode::Char('/' | '7' | '_'), true) if !on_line => Action::OpenUrl,
        (KeyCode::Char('-'), true) if shift && !on_line => Action::OpenUrl,
        (KeyCode::Char('u'), true) => Action::Undo,
        (KeyCode::Char('r'), true) => Action::Redo,
        (KeyCode::Char('l'), true) => Action::Link,
        (KeyCode::Char('b'), true) => Action::ShowScores,
        (KeyCode::Up, true) if !on_line => Action::Shift(-1),
        (KeyCode::Down, true) if !on_line => Action::Shift(1),
        (KeyCode::Up, _) => Action::Move(-1),
        (KeyCode::Down, _) => Action::Move(1),
        (KeyCode::PageUp, _) => Action::Move(-10),
        (KeyCode::PageDown, _) => Action::Move(10),
        _ => return None,
    })
}

/// What a key asks for on a row in vim's normal mode, beyond what [`bound`] gives it.
#[derive(Debug, PartialEq)]
enum Normal {
    /// What the key asks for of the session, as it would anywhere.
    Act(Action),
    /// Down the list by this many rows, or up it, stopping at its first and last rows, so that a
    /// stray `k` never lands on the line to be typed there.
    Walk(i64),
    /// Into the note, the cursor landing on the first row inside.
    In,
    /// The caret along the head, nothing written.
    Caret(Motion),
    /// Typing in the head, from where the caret is or at one of its ends.
    Type(Typing),
    /// A new row above this one.
    Above,
    /// To the line, the caret at the end of its text.
    ToLine,
}

/// Where a motion takes the caret in a head, as vim's do on one line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Motion {
    /// One letter back, `h` or Left.
    Back,
    /// One letter on, `l` or Right.
    On,
    /// The start of the head, `0` or Home.
    Start,
    /// The first letter after a task's box and its spaces, `^`.
    First,
    /// The end of the head, after its last letter, `$` or End.
    End,
    /// The start of the next word, `w`.
    Word,
    /// The start of this word or the one before, `b`.
    WordBack,
    /// The last letter of this word or the next, `e`.
    WordEnd,
}

/// Where typing starts in a head: before the letter under the caret, after it, at the start
/// after a task's box, or at the end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Typing {
    Before,
    After,
    Start,
    End,
}

/// What a key asks for on a row in vim's normal mode, where letters are commands and nothing is
/// typed: vim's single keys for what fzn does, the caret's motions along the head, and the rest
/// of fzn's keys as ever, except that Ctrl-D and Ctrl-U page by ten as vim's do, `D` and `u`
/// deleting and undoing here, and Escape goes to the line, as it goes off a row without vim's
/// keys. The arrows, Home and End move the caret only without modifiers, so that nothing is
/// selected in normal mode. Any other letter, and the keys that edit, do nothing.
fn normal(code: KeyCode, modifiers: KeyModifiers, zoomed: bool, empty: bool) -> Option<Normal> {
    let ctrl = modifiers.contains(KeyModifiers::CONTROL);
    let plain = modifiers.is_empty();
    Some(match (code, ctrl) {
        (KeyCode::Esc, _) | (KeyCode::Char('/'), false) => Normal::ToLine,
        (KeyCode::Char('j'), false) => Normal::Walk(1),
        (KeyCode::Char('k'), false) => Normal::Walk(-1),
        (KeyCode::Char('d'), true) => Normal::Walk(10),
        (KeyCode::Char('u'), true) => Normal::Walk(-10),
        (KeyCode::Tab, _) => Normal::In,
        (KeyCode::Char('h'), false) => Normal::Caret(Motion::Back),
        (KeyCode::Left, _) if plain => Normal::Caret(Motion::Back),
        (KeyCode::Char('l'), false) => Normal::Caret(Motion::On),
        (KeyCode::Right, _) if plain => Normal::Caret(Motion::On),
        (KeyCode::Char('0'), false) => Normal::Caret(Motion::Start),
        (KeyCode::Home, _) if plain => Normal::Caret(Motion::Start),
        (KeyCode::Char('^'), false) => Normal::Caret(Motion::First),
        (KeyCode::Char('$'), false) => Normal::Caret(Motion::End),
        (KeyCode::End, _) if plain => Normal::Caret(Motion::End),
        (KeyCode::Char('w'), false) => Normal::Caret(Motion::Word),
        (KeyCode::Char('b'), false) => Normal::Caret(Motion::WordBack),
        (KeyCode::Char('e'), false) => Normal::Caret(Motion::WordEnd),
        (KeyCode::Char('i'), false) => Normal::Type(Typing::Before),
        (KeyCode::Char('a'), false) => Normal::Type(Typing::After),
        (KeyCode::Char('I'), false) => Normal::Type(Typing::Start),
        (KeyCode::Char('A'), false) => Normal::Type(Typing::End),
        (KeyCode::Char('o'), false) => Normal::Act(Action::Write),
        (KeyCode::Char('O'), false) => Normal::Above,
        (KeyCode::Char('x'), false) => Normal::Act(Action::Archive),
        (KeyCode::Char('D'), false) => Normal::Act(Action::Delete),
        (KeyCode::Char('y'), false) => Normal::Act(Action::Copy),
        (KeyCode::Char('u'), false) => Normal::Act(Action::Undo),
        (KeyCode::Char(_), false) => return None,
        (KeyCode::Left | KeyCode::Right | KeyCode::Home | KeyCode::End, _) => return None,
        _ => Normal::Act(bound(code, modifiers, Place::Row, zoomed, empty)?),
    })
}

/// Where the letters of a task's box, and the spaces after it, end in a head: the first letter
/// typed at the start of a task, or the start of a head that is not one. `spaces` says how many
/// spaces after the box are counted, `I` taking one and `^` all of them.
fn after_box(head: &str, task: bool, spaces: usize) -> usize {
    if !task {
        return 0;
    }
    let blanks = head.chars().skip(3).take(spaces).take_while(|c| *c == ' ');
    letters_in(head, 3 + blanks.count())
}

/// Which of vim's small words a letter is in: a run of letters, digits and `_`, or a run of other
/// characters that are not blank, with blanks parting them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Blank,
    Word,
    Other,
}

fn kind(letter: &str) -> Kind {
    match letter.chars().next() {
        Some(c) if c.is_whitespace() => Kind::Blank,
        Some(c) if c.is_alphanumeric() || c == '_' => Kind::Word,
        _ => Kind::Other,
    }
}

/// Where a motion takes the caret, in letters as drawn, in a head whose first letter after a
/// task's box and its spaces is `first`. The caret goes from the start to the end, the end being
/// after the last letter, and never past either. `e` with no word after the caret stays.
fn moved(head: &str, caret: usize, motion: Motion, first: usize) -> usize {
    let kinds: Vec<Kind> = head.graphemes(true).map(kind).collect();
    let end = kinds.len();
    let caret = caret.min(end);
    let blank = |i: usize| kinds[i] == Kind::Blank;
    match motion {
        Motion::Back => caret.saturating_sub(1),
        Motion::On => (caret + 1).min(end),
        Motion::Start => 0,
        Motion::First => first.min(end),
        Motion::End => end,
        Motion::Word => {
            let mut i = caret;
            if i < end && !blank(i) {
                let was = kinds[i];
                while i < end && kinds[i] == was {
                    i += 1;
                }
            }
            while i < end && blank(i) {
                i += 1;
            }
            i
        }
        Motion::WordBack => {
            let mut i = caret;
            while i > 0 && blank(i - 1) {
                i -= 1;
            }
            if i > 0 {
                let was = kinds[i - 1];
                while i > 0 && kinds[i - 1] == was {
                    i -= 1;
                }
            }
            i
        }
        Motion::WordEnd => {
            let mut i = caret + 1;
            while i < end && blank(i) {
                i += 1;
            }
            if i >= end {
                return caret;
            }
            while i + 1 < end && kinds[i + 1] == kinds[i] {
                i += 1;
            }
            i
        }
    }
}

/// Which keys are in use, for the footer: whether Shift-Enter can be told from Enter, and, with
/// vim's keys, the mode of the row the cursor is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Keys {
    shift_enter: bool,
    mode: Mode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Without vim's keys, or on the line, which is always typing.
    Plain,
    /// With vim's keys, on a row being written in.
    Insert,
    /// With vim's keys, on a row walked to.
    Normal,
}

/// The key that asks for an action, as the footer names it. In normal mode it is vim's letter
/// where there is one, Tab going in and Escape going up as they do without vim's keys, and tick
/// is Shift-Enter wherever the terminal can tell it from Enter; Ctrl-V, which ticks in every
/// terminal, is then the key the footer leaves out.
fn key_for(action: &Action, keys: Keys) -> &'static str {
    if keys.mode == Mode::Normal {
        match action {
            Action::Write => return "o",
            Action::Archive => return "x",
            Action::Delete => return "D",
            Action::Copy => return "y",
            Action::Undo => return "u",
            _ => {}
        }
    }
    match action {
        Action::Write => "Enter",
        Action::Editor => "^O",
        Action::GoIn => "Tab",
        Action::Move(_) => "↓",
        Action::Shift(by) if *by < 0 => "^↑",
        Action::Shift(_) => "^↓",
        Action::Select(_) | Action::Back | Action::Close => "Esc",
        // Clearing the line is the rung Escape takes while it has text; Ctrl-D is its second key.
        Action::Type(text) if text.is_empty() => "Esc",
        Action::Archive if keys.shift_enter => "⇧Enter",
        Action::ShowArchived | Action::Archive => "^V",
        Action::Delete => "^D",
        Action::Link => "^L",
        Action::Copy => "^C",
        Action::OpenUrl => "^/",
        Action::Undo => "^U",
        Action::Redo => "^R",
        Action::ShowScores => "^B",
        // Typing and the editor ask for the rest, not a key. A body is written in the editor here,
        // so opening one in place has no key either. Backspace on an empty row asks for Erase,
        // and the footer does not name it, since taking an empty line away is what Backspace
        // does everywhere.
        Action::Open
        | Action::Type(_)
        | Action::Edited(_)
        | Action::Save { .. }
        | Action::Keep
        | Action::Erase
        | Action::Restore(_) => "",
    }
}

/// What is on offer that a key asks for: not what no key asks for.
fn keyed(hints: &[Hint], keys: Keys) -> Vec<&Hint> {
    hints
        .iter()
        .filter(|hint| !key_for(&hint.action, keys).is_empty())
        .collect()
}

/// Whether an action is one of Escape's rungs back: off the row, off the text, out of the place.
fn comes_back(action: &Action) -> bool {
    match action {
        Action::Select(None) | Action::Back => true,
        Action::Type(text) => text.is_empty(),
        _ => false,
    }
}

/// The keys that do something where the cursor is, for the footer. The way back is always last,
/// and where the session has no way back for it to take, two Escapes quit. In normal mode it
/// starts with typing, which is the face's own and not the session's, and the caret's motions are
/// not written, being the field's own as Left and Right are without vim's keys; in insert Escape
/// is vim's, and says it is done.
fn footer(hints: &[Hint], keys: Keys, room: usize) -> String {
    let hints = keyed(hints, keys);
    let mut said: Vec<String> = hints
        .iter()
        .map(|hint| {
            let label = match (&hint.action, keys.mode) {
                (Action::Select(None), Mode::Insert) => "done",
                _ => hint.label,
            };
            format!("{} {label}", key_for(&hint.action, keys))
        })
        .collect();
    let back = hints.last().is_some_and(|h| comes_back(&h.action));
    if keys.mode == Mode::Normal {
        said.insert(0, "i type".to_owned());
    }
    if !back {
        said.push("Esc Esc quits".to_owned());
    }
    // A narrow terminal loses chords from the middle, never the first or the way back.
    let width = |said: &[String]| said.iter().map(|k| k.width() + 2).sum::<usize>() - 2;
    while said.len() > 2 && width(&said) > room {
        said.remove(said.len() - 2);
    }
    said.join("  ")
}

/// What a key does to a line of text, the line or a row's head, when it is one that edits it or
/// moves along it: the text as it then reads, and where its cursor then is, in letters as drawn,
/// each a grapheme cluster. Right at the end stays there, and Tab is never typed, since it is the
/// key that goes in.
fn edited(line: &str, cursor: usize, code: KeyCode, ctrl: bool) -> Option<(String, usize)> {
    let end = letters(line);
    let cursor = cursor.min(end);
    let without = |from: usize, to: usize| {
        let (from, to) = (byte_index(line, from), byte_index(line, to));
        format!("{}{}", &line[..from], &line[to..])
    };
    let with = |c: char| spliced(line, cursor, c.encode_utf8(&mut [0; 4]));
    Some(match (code, ctrl) {
        (KeyCode::Char('a'), true) | (KeyCode::Home, _) => (line.to_owned(), 0),
        (KeyCode::Char('e'), true) | (KeyCode::End, _) => (line.to_owned(), end),
        (KeyCode::Left, _) => (line.to_owned(), cursor.saturating_sub(1)),
        (KeyCode::Right, _) => (line.to_owned(), (cursor + 1).min(end)),
        (KeyCode::Backspace, _) if cursor > 0 => (without(cursor - 1, cursor), cursor - 1),
        (KeyCode::Delete, _) if cursor < end => (without(cursor, cursor + 1), cursor),
        (KeyCode::Char('w'), true) => {
            let blank: Vec<bool> = line
                .graphemes(true)
                .map(|g| g.chars().all(char::is_whitespace))
                .collect();
            let mut start = cursor;
            while start > 0 && blank[start - 1] {
                start -= 1;
            }
            while start > 0 && !blank[start - 1] {
                start -= 1;
            }
            (without(start, cursor), start)
        }
        (KeyCode::Char(c), false) => with(c),
        _ => return None,
    })
}

/// Text pasted, as it goes into a line of text. The line breaks at its ends go, with the blanks
/// they leave, since a line copied from a terminal comes with its break; one inside it, `\r\n`
/// as much as `\n` or a Unicode line or paragraph separator, and a tab are each a space; and any
/// other control character is left out.
fn pasted(text: &str) -> String {
    let breaks = |c: char| matches!(c, '\n' | '\r' | '\u{2028}' | '\u{2029}');
    let end = text.trim_end();
    let text = if text[end.len()..].contains(breaks) {
        end
    } else {
        text
    };
    let start = text.trim_start();
    let text = if text[..text.len() - start.len()].contains(breaks) {
        start
    } else {
        text
    };
    text.replace("\r\n", "\n")
        .chars()
        .map(|c| if breaks(c) || c == '\t' { ' ' } else { c })
        .filter(|c| !c.is_control())
        .collect()
}

/// `text` put into `line` at `cursor`, in letters, and where the cursor is then, after it. An
/// accent typed after a letter joins it, and the cursor stays after the two.
fn spliced(line: &str, cursor: usize, text: &str) -> (String, usize) {
    let at = byte_index(line, cursor);
    let line = format!("{}{text}{}", &line[..at], &line[at..]);
    let cursor = letters(&line[..at + text.len()]);
    (line, cursor)
}

/// How many letters as drawn, grapheme clusters, the text holds.
fn letters(s: &str) -> usize {
    s.graphemes(true).count()
}

/// How many letters as drawn the first `chars` characters of the text make, for a place the
/// session gives in characters.
fn letters_in(s: &str, chars: usize) -> usize {
    let at = s.char_indices().nth(chars).map_or(s.len(), |(b, _)| b);
    letters(&s[..at])
}

/// Cut to at most `max` columns as the terminal draws them, marking the cut with `…`.
fn fit(s: &str, max: usize) -> String {
    if s.width() <= max {
        return s.to_owned();
    }
    let mut out = String::new();
    let mut used = 0;
    for g in s.graphemes(true) {
        let n = g.width();
        if used + n + 1 > max {
            break;
        }
        out.push_str(g);
        used += n;
    }
    if max > 0 {
        out.push('…');
    }
    out
}

/// The columns kept in sight beside the caret in a line of text scrolled sideways, as vim's
/// `sidescrolloff` keeps them, so that what is being written is read in its words.
const BESIDE: usize = 5;

/// A line of text in `room` columns with its caret, `caret` letters in, as a one-line field
/// shows it: what is drawn, the column the caret is then at, and the first letter drawn, kept by
/// the caller for the next frame. `left` is that letter as the last frame had it, or `None` for
/// a row landed on whose caret no key has moved yet: then the text reads from its start, cut with
/// `…` when it does not fit, and the caret stands on the `…`, which is where the rest is. Once a
/// key has written in it or moved its caret the text moves only when the caret would come
/// within `margin` columns of an edge, and then by as little as keeps it there, so that moving
/// the caret within what is shown moves no text; each edge that hides text shows `…` in its
/// column. A text that fits is drawn whole, the caret at its end standing in the column after
/// `room`, which the caller leaves for it. Widths are as drawn, so a letter two columns wide is
/// never split at an edge.
fn sidescrolled(
    text: &str,
    caret: usize,
    room: usize,
    left: Option<usize>,
    margin: usize,
) -> (String, usize, Option<usize>) {
    let letters: Vec<&str> = text.graphemes(true).collect();
    let widths: Vec<usize> = letters.iter().map(|g| g.width()).collect();
    let across = |from: usize, to: usize| -> usize { widths[from..to].iter().sum() };
    let room = room.max(3);
    let end = letters.len();
    let caret = caret.min(end);
    if across(0, end) <= room {
        return (text.to_owned(), across(0, caret), left.map(|_| 0));
    }
    let Some(left) = left else {
        let (shown, cut_at) = cut(&letters, 0, room);
        return (shown, across(0, caret).min(cut_at), None);
    };
    // The caret keeps `margin` columns before it when text is hidden there, and its letter and
    // `margin` more after it, or a column of its own at the end, before an edge that hides text;
    // the `…` at each such edge takes a column of its own.
    let margin = margin.min((room - 3) / 2);
    let mut from = left.min(caret);
    while from > 0 && across(from, caret) < margin {
        from -= 1;
    }
    let mut beyond = (caret + 1).min(end);
    while beyond < end && across(caret + 1, beyond) < margin {
        beyond += 1;
    }
    let after = if caret == end {
        1
    } else {
        across(caret, beyond)
    };
    while from < caret {
        let lead = usize::from(from > 0);
        let hidden = lead + across(from, end) > room;
        if lead + across(from, caret) + after + usize::from(hidden) <= room {
            break;
        }
        from += 1;
    }
    // A field never shows blank columns at its end while text is hidden at its start: what is
    // taken off its end, by a delete, an undo or another device, brings back what was before,
    // leaving a column after the end for the caret.
    while from > 0 && usize::from(from > 1) + across(from - 1, end) < room {
        from -= 1;
    }
    let lead = usize::from(from > 0);
    let (cut, _) = cut(&letters, from, room - lead);
    let shown = if lead > 0 { format!("…{cut}") } else { cut };
    (shown, lead + across(from, caret), Some(from))
}

/// The letters from `from` in `room` columns, ending in `…` when they do not all fit, with no
/// blank left before it, and the column the `…` is in, or the end.
fn cut(letters: &[&str], from: usize, room: usize) -> (String, usize) {
    let all: usize = letters[from..].iter().map(|g| g.width()).sum();
    if all <= room {
        return (letters[from..].concat(), all);
    }
    let (mut kept, mut used) = (from, 0);
    while kept < letters.len() && used + letters[kept].width() < room {
        used += letters[kept].width();
        kept += 1;
    }
    while kept > from && letters[kept - 1].trim().is_empty() {
        kept -= 1;
        used -= letters[kept].width();
    }
    (format!("{}…", letters[from..kept].concat()), used)
}

/// What fzn says when it is run with no terminal to draw on, as from a script.
const NO_TERMINAL: &str = "there is no terminal to draw on; to a pipe, fzn query LINE writes \
                           what a line finds and fzn export every note";

/// Says so when there is no terminal to draw on, in place of a backtrace.
pub fn need_terminal() -> Result<()> {
    if std::io::stdout().is_terminal() && std::io::stdin().is_terminal() {
        Ok(())
    } else {
        Err(anyhow!(NO_TERMINAL))
    }
}

/// Takes the terminal for the app the first time: raw, the alternate screen, a paste sent whole
/// rather than as keys, and Shift-Enter told from Enter where the terminal can be asked to. The
/// terminal is asked what it has, which can wait two seconds on one that answers nothing, and
/// the answer comes back to be kept. ratatui's own panic hook is installed here, once, so that
/// the one fzn sets after it runs first.
fn take_terminal() -> Result<(DefaultTerminal, Keyboard)> {
    need_terminal()?;
    let terminal = ratatui::try_init().map_err(|_| {
        ratatui::restore();
        anyhow!(NO_TERMINAL)
    })?;
    let keyboard = ask_keyboard();
    if let Err(why) =
        execute!(std::io::stdout(), EnableBracketedPaste).and_then(|()| tell_keys_apart(keyboard))
    {
        give_terminal_back(false);
        return Err(why.into());
    }
    Ok((terminal, keyboard))
}

/// Takes the terminal back after the editor, as it was taken the first time and from the answer
/// kept then. It does not go through ratatui's `try_init`, which would install its panic hook
/// again, outside fzn's, so that a panic would leave the alternate screen before the keyboard
/// was put back on it.
fn take_terminal_back(terminal: &mut DefaultTerminal, keyboard: Keyboard) -> Result<()> {
    let taken = enable_raw_mode()
        .and_then(|()| {
            execute!(
                std::io::stdout(),
                EnterAlternateScreen,
                EnableBracketedPaste
            )
        })
        .and_then(|()| tell_keys_apart(keyboard))
        .and_then(|()| terminal.clear());
    if let Err(why) = taken {
        give_terminal_back(false);
        return Err(why.into());
    }
    Ok(())
}

/// Gives the terminal back as it was found: the keyboard as it was, the person's own cursor
/// where vim's keys `shaped` it, and a paste as keys again. The keyboard is put back before the
/// alternate screen is left, since a terminal keeps a pushed flag for the screen it was pushed on.
fn give_terminal_back(shaped: bool) {
    let _ = stop_telling_keys_apart();
    if shaped {
        let _ = execute!(std::io::stdout(), SetCursorStyle::DefaultUserShape);
    }
    let _ = execute!(std::io::stdout(), DisableBracketedPaste);
    ratatui::restore();
}

/// What the terminal can be asked for so that Shift-Enter is told from Enter. The kitty
/// protocol, where the terminal answers that it has it. Inside tmux, which answers for the
/// terminal and has no such protocol, modifyOtherKeys, but only when tmux passes keys on in the
/// form of CSI u: in its own default form crossterm drops Shift-Enter altogether, where unasked
/// it is Enter.
fn ask_keyboard() -> Keyboard {
    if supports_keyboard_enhancement().unwrap_or(false) {
        Keyboard::Kitty
    } else if std::env::var_os("TMUX").is_some_and(|tmux| !tmux.is_empty())
        && tmux_extended_keys().is_some_and(|said| passes_csi_u(&said))
    {
        Keyboard::Modified
    } else {
        Keyboard::Plain
    }
}

/// How long tmux is given to say how it passes keys on, so that a tmux that does not answer
/// never keeps fzn from starting.
const TMUX_WAIT: Duration = Duration::from_millis(200);

/// What the tmux fzn runs in says of its `extended-keys` and `extended-keys-format`, as
/// `on|csi-u`, or nothing if it does not say within [`TMUX_WAIT`].
fn tmux_extended_keys() -> Option<String> {
    use std::io::Read as _;
    use std::process::Stdio;
    let mut child = Command::new("tmux")
        .args(["display", "-p", "#{extended-keys}|#{extended-keys-format}"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => break,
            Ok(None) if started.elapsed() < TMUX_WAIT => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(Some(_)) => return None,
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
    let mut said = String::new();
    child.stdout.take()?.read_to_string(&mut said).ok()?;
    Some(said)
}

/// Whether tmux, by what it said of its `extended-keys` and `extended-keys-format`, passes keys
/// on as CSI u: only with extended keys on, or always, and the format `csi-u`.
fn passes_csi_u(said: &str) -> bool {
    said.trim()
        .split_once('|')
        .is_some_and(|(keys, format)| matches!(keys, "on" | "always") && format == "csi-u")
}

/// What has been asked of the terminal and not yet taken back, as a [`Keyboard`]'s number, so
/// that each push is popped once and only once, by whichever of the way out, the editor, an
/// error or a panic comes first.
static ASKED: AtomicU8 = AtomicU8::new(Keyboard::Plain as u8);

/// Asks the terminal to tell Shift-Enter from Enter. Of the kitty flags only the first: the
/// one that reports releases would stop a held arrow repeating, since only presses are read, and
/// the one that reports the base layout's key loses a non-Latin layout's own letter.
fn tell_keys_apart(keyboard: Keyboard) -> std::io::Result<()> {
    let mut out = std::io::stdout();
    match keyboard {
        Keyboard::Plain => return Ok(()),
        Keyboard::Kitty => execute!(
            out,
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        )?,
        Keyboard::Modified => write!(out, "\x1b[>4;1m").and_then(|()| out.flush())?,
    }
    ASKED.store(keyboard as u8, Ordering::SeqCst);
    Ok(())
}

/// Takes back what [`tell_keys_apart`] asked for, if anything is still asked.
fn stop_telling_keys_apart() -> std::io::Result<()> {
    let mut out = std::io::stdout();
    match ASKED.swap(Keyboard::Plain as u8, Ordering::SeqCst) {
        asked if asked == Keyboard::Kitty as u8 => execute!(out, PopKeyboardEnhancementFlags),
        asked if asked == Keyboard::Modified as u8 => {
            write!(out, "\x1b[>4;0m").and_then(|()| out.flush())
        }
        _ => Ok(()),
    }
}

/// Where you are: the space, then each place gone into, outermost first, in at most `max`
/// columns. Past three steps the middle ones give way to `…`. The space is a step like the
/// others, so that a long name cannot crowd out the place: every step gets an equal share of the
/// room, and what a short step does not use goes to the longer ones.
fn breadcrumb(space: &str, heads: &[&str], max: usize) -> String {
    let mut steps: Vec<&str> = match heads {
        [first, .., before, last] if heads.len() > 3 => vec![first, "…", before, last],
        _ => heads.to_vec(),
    };
    steps.insert(0, space);
    let mut room = max.saturating_sub(2 + 3 * (steps.len() - 1));
    let mut widths = vec![0; steps.len()];
    let mut order: Vec<usize> = (0..steps.len()).collect();
    order.sort_by_key(|&i| steps[i].width());
    for (k, &i) in order.iter().enumerate() {
        widths[i] = steps[i].width().min(room / (steps.len() - k));
        room -= widths[i];
    }
    let steps: Vec<String> = steps.iter().zip(widths).map(|(s, w)| fit(s, w)).collect();
    format!(" {} ", steps.join(" › "))
}

/// Where in the text, in bytes, its letter `letter` as drawn begins, or its end.
fn byte_index(s: &str, letter: usize) -> usize {
    s.grapheme_indices(true)
        .nth(letter)
        .map_or(s.len(), |(b, _)| b)
}

fn sanitise(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Standard base64 with padding, for the OSC 52 clipboard sequence.
fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &b)| n | (u32::from(b) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(TABLE[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Puts text on the clipboard via OSC 52, and says whether the terminal took it.
fn copy(text: &str) -> bool {
    let mut out = std::io::stdout();
    write!(out, "\x1b]52;c;{}\x07", base64(text.as_bytes())).is_ok() && out.flush().is_ok()
}

/// Opens a URL with whatever the system opens URLs with, or says why it could not.
fn open(url: &str) -> Result<(), String> {
    let opener = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    Command::new(opener)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("{opener}: {e}"))
}

/// An error as the status row says it, without the number the system gives it.
fn plainly(error: &std::io::Error) -> String {
    let said = error.to_string();
    match said.find(" (os error") {
        Some(at) => said[..at].to_owned(),
        None => said,
    }
}

/// The file a note is handed to the editor in, which is the owner's alone and goes when this
/// does, however the editor finished, and the directory of its own it is put in where the
/// directory it would share is not the owner's.
struct Handed {
    path: PathBuf,
    own: Option<PathBuf>,
}

impl Handed {
    /// Writes `text` to a new file for the note `id`, readable by its owner only, in
    /// `$XDG_RUNTIME_DIR` when there is one that takes it.
    fn new(id: &str, text: &str) -> std::io::Result<Self> {
        let runtime = std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from);
        Self::within(runtime, &std::env::temp_dir(), id, text)
    }

    /// Writes the note in `runtime`, the user's own directory and kept in memory, and failing
    /// that, as when it is left over from another user under `su`, in a directory of its own
    /// under `temp`, which is the owner's alone too, so that the editor's swap and backup files
    /// beside it are as private as the note.
    fn within(
        runtime: Option<PathBuf>,
        temp: &Path,
        id: &str,
        text: &str,
    ) -> std::io::Result<Self> {
        let name = format!("fzn-{}-{}.md", sanitise(id), std::process::id());
        if let Some(dir) = runtime.filter(|dir| dir.is_absolute() && dir.is_dir())
            && let Ok(handed) = Self::write(dir.join(&name), None, text)
        {
            return Ok(handed);
        }
        let own = temp.join(format!("fzn-{}", std::process::id()));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        // A directory of that name is taken as nobody's: it is never written into.
        builder.create(&own)?;
        Self::write(own.join(&name), Some(own), text)
    }

    fn write(path: PathBuf, own: Option<PathBuf>, text: &str) -> std::io::Result<Self> {
        let handed = Self { path, own };
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        // A name that is somehow taken is never written through.
        options.open(&handed.path)?.write_all(text.as_bytes())?;
        Ok(handed)
    }
}

impl Drop for Handed {
    fn drop(&mut self) {
        match &self.own {
            Some(dir) => {
                let _ = fs::remove_dir_all(dir);
            }
            None => {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}

/// Hands a note's text to the editor and waits for it. Gives back the text as it was left, or
/// what to say when the editor could not be run or did not finish well. The editor is a program
/// and its arguments, split on spaces.
fn edit_in(editor: &str, id: &str, text: &str) -> Result<String, String> {
    let mut parts = editor.split_whitespace();
    let Some(program) = parts.next() else {
        return Err("no editor is set; note unchanged".to_owned());
    };
    let handed = Handed::new(id, text)
        .map_err(|e| format!("could not hand the note over: {}", plainly(&e)))?;
    let started = {
        #[cfg(test)]
        let _starting = store::starting();
        Command::new(program).args(parts).arg(&handed.path).spawn()
    };
    let status = started
        .and_then(|mut child| child.wait())
        .map_err(|e| format!("could not run {program}: {}", plainly(&e)))?;
    if !status.success() {
        let how = status
            .code()
            .map_or_else(|| status.to_string(), |code| code.to_string());
        return Err(format!("{program} exited with {how}; note unchanged"));
    }
    fs::read_to_string(&handed.path)
        .map_err(|e| format!("could not read what {program} left: {}", plainly(&e)))
}

/// What the date words and spans on the line stand for, to show at the end of its row.
fn dates_shown(dates: &[String]) -> String {
    if dates.is_empty() {
        String::new()
    } else {
        format!("→ {}", dates.join(", "))
    }
}

/// A note's head, its first line, and the body under it, without the blank lines between.
fn head_and_body(text: &str) -> (&str, &str) {
    match text.split_once('\n') {
        Some((head, body)) => (head, body.trim_start_matches(['\n', '\r']).trim_end()),
        None => (text, ""),
    }
}

/// The note the cursor is on, if it is on one. A row is landed on when this changes, which it
/// does when the cursor moves and also when the note at the same place on the list is another.
fn under_cursor(view: &View) -> Option<String> {
    view.cursor
        .and_then(|i| view.rows.get(i))
        .map(|row| row.id.clone())
}

/// What follows a row's head, to be dimmed: a mark if there is more to read, when `mark` asks
/// for it, the notes it links to, and why it is listed.
fn tail(row: &Row, mark: bool) -> String {
    let mut tail = String::new();
    if mark && row.has_body {
        tail.push_str(" ¶");
    }
    if !row.links.is_empty() {
        tail.push_str("  → ");
        tail.push_str(&row.links.join(", "));
    }
    if !row.reasons.is_empty() {
        tail.push_str("  ");
        tail.push_str(&row.reasons.join(" · "));
    }
    tail
}

/// Text broken between words into rows at most `width` columns wide, a word longer than a row
/// across as many as it takes. Each line of the text starts a row of its own. A letter is one as
/// drawn, so that an accent or a joined emoji is never parted from itself, and as wide as the
/// terminal draws it, so that a row of wide letters, Chinese say, is broken where it meets the
/// edge rather than cut off there.
fn wrap(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut rows = Vec::new();
    for line in text.lines() {
        let (mut row, mut len) = (String::new(), 0);
        for word in line.split(' ') {
            let n = word.width();
            if len > 0 && len + 1 + n > width {
                rows.push(std::mem::take(&mut row));
                len = 0;
            } else if len > 0 {
                row.push(' ');
                len += 1;
            }
            for g in word.graphemes(true) {
                let n = g.width();
                if len > 0 && len + n > width {
                    rows.push(std::mem::take(&mut row));
                    len = 0;
                }
                row.push_str(g);
                len += n;
            }
        }
        rows.push(row);
    }
    rows
}

/// A body as at most `most` rows, set in two columns from the rows' text in a screen `width`
/// wide so that it does not read as one of them, the last a dim `…` when there is more.
fn body_rows(body: &str, width: usize, most: usize) -> Vec<Line<'static>> {
    let pad = "   ";
    let wrapped = wrap(body, width.saturating_sub(pad.len() + 1));
    let more = wrapped.len() > most;
    let mut rows: Vec<Line> = wrapped
        .into_iter()
        .take(if more { most.saturating_sub(1) } else { most })
        .map(|row| Line::from(format!("{pad}{row}")))
        .collect();
    if more {
        rows.push(Line::from(format!("{pad}…")).dim());
    }
    rows
}

/// How many rows the body of what the cursor is on is given at the foot of the list, out of the
/// `room` there is for the list: as many as [`PREVIEW_ROWS`], and a row of nothing above them,
/// while that leaves the list at least half the room, and none when that would be fewer than two.
fn preview_rows(room: usize) -> usize {
    match (room / 2).saturating_sub(1).min(PREVIEW_ROWS) {
        0 | 1 => 0,
        rows => rows,
    }
}

/// The score before a row's head, when scores are shown.
fn score_of(row: &Row, shown: bool) -> String {
    match (shown, row.score) {
        (true, Some(score)) => format!("{score:.2} "),
        _ => String::new(),
    }
}

/// A row of the list as it is when the cursor is not on it: the head as the row shows it, then as
/// much of the tail as there is room for. The head is cut only when it alone does not fit.
fn row_line(row: &Row, scores: bool, width: usize) -> Line<'static> {
    let score = score_of(row, scores);
    let room = width.saturating_sub(2 + score.width());
    let head = fit(row.shown.as_deref().unwrap_or(&row.head), room);
    let rest = room - head.width();
    let tail = tail(row, true);
    let mut spans = vec![Span::raw(" "), Span::raw(score).dim(), Span::raw(head)];
    if !tail.is_empty() && rest > 3 {
        spans.push(Span::raw(fit(&tail, rest)).dim());
    }
    let line = Line::from(spans);
    if row.archived { line.dim() } else { line }
}

/// The row the cursor is on as it is painted: its one line, the column the terminal's cursor is
/// at in it, the first letter of the head drawn, to be kept for the next frame, and whether the
/// head is cut, so that the foot of the list reads it whole.
struct Selected {
    line: Line<'static>,
    x: usize,
    left: Option<usize>,
    cut: bool,
}

/// The row the cursor is on, `head` being its head as it is now typed: one row, however long its
/// head or its tail, the head bold, and at the right a `¶` when the note has a body and the day
/// it was `created` at, the day it belongs to. The head is a one-line field, `caret` letters in,
/// read from its start while `left` is `None` and scrolled sideways under the caret once a key
/// has moved it (see [`sidescrolled`]); the tail follows it as far as there is room, as off the
/// cursor.
fn selected_row(
    row: &Row,
    head: &str,
    created: Option<Moment>,
    (caret, left): (usize, Option<usize>),
    scores: bool,
    width: usize,
) -> Selected {
    let score = score_of(row, scores);
    let date = created.map_or_else(String::new, |m| time::format_day(m.day()));
    let right = [if row.has_body { "¶" } else { "" }, &date]
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let righted = if right.is_empty() {
        0
    } else {
        right.width() + 2
    };
    // On a screen too narrow to leave the head three columns beside them, the mark and the date
    // go, as the tail goes from a row off the cursor.
    let narrow = width.saturating_sub(2 + score.width() + righted) < 3;
    let right = if narrow { String::new() } else { right };
    let righted = if narrow { 0 } else { righted };
    let room = width.saturating_sub(2 + score.width() + righted).max(1);
    let (shown, at, left) = sidescrolled(head, caret, room, left, BESIDE);
    let cut = shown != head;
    let x = 1 + score.width() + at;
    let rest = room.saturating_sub(shown.width());
    let mut used = 1 + score.width() + shown.width();
    let mut spans = vec![
        Span::raw(" "),
        Span::raw(score).dim(),
        Span::raw(shown).bold(),
    ];
    let tail = tail(row, false);
    if !tail.is_empty() && rest > 3 {
        let tail = fit(&tail, rest);
        used += tail.width();
        spans.push(Span::raw(tail).dim());
    }
    if !right.is_empty() {
        let pad = width.saturating_sub(used + right.width() + 1);
        spans.push(Span::raw(" ".repeat(pad)));
        spans.push(Span::raw(right).dim());
    }
    let line = Line::from(spans);
    Selected {
        line: if row.archived { line.dim() } else { line },
        x,
        left,
        cut,
    }
}

/// Paints one row of the screen, `y` rows down, if the screen reaches that far.
fn paint(frame: &mut Frame, y: usize, x: usize, line: Line) {
    let area = frame.area();
    let (Ok(y), Ok(x)) = (u16::try_from(y), u16::try_from(x)) else {
        return;
    };
    if y < area.height && x < area.width {
        let at = Rect::new(area.x + x, area.y + y, area.width - x, 1);
        frame.render_widget(line, at);
    }
}

/// Paints text dimmed so that it ends a column short of the right edge, as dates and the state
/// of things are.
fn paint_right(frame: &mut Frame, y: usize, text: &str) {
    let width = usize::from(frame.area().width);
    let len = text.width();
    if !text.is_empty() && len + 2 < width {
        paint(frame, y, width - len - 1, Line::from(text.to_owned()).dim());
    }
}

impl App {
    /// Opens a space's directory as a device writing in `slot`: every log is given to the
    /// device, what it asks for is done, and then whatever opening it is due, a compaction say.
    /// Fails if the key opens none of the sealed lines there, since nothing could be written.
    pub fn open(
        dir: &Path,
        space: &str,
        mut slot: Slot,
        key: Key,
        weights: Weights,
        editor: String,
    ) -> Result<Self> {
        let mut device = Device::new(Box::new(System), slot.id(), weights, key);
        let logs = store::open(dir, space, &mut slot, &mut device)?;
        let mut app = Self {
            device,
            logs,
            dir: dir.to_owned(),
            slot,
            relay: None,
            keeper: None,
            editor,
            space: space.to_owned(),
            cursor: 0,
            row_cursor: 0,
            row_left: None,
            line_left: 0,
            top: 0,
            escaped: false,
            sync: None,
            not_syncing: false,
            unreachable: None,
            looked: Instant::now(),
            keyboard: None,
            vim: false,
            normal: true,
            shape: None,
        };
        // An empty space says how to begin, until the first key.
        if app.device.view().status.is_empty() {
            match (app.device.unopened(), app.device.view().notes) {
                (0, 0) => app.device.say(NO_NOTES),
                (0, _) => {}
                (shut, _) => app.device.say(format!(
                    "{shut} lines here that this passphrase does not open"
                )),
            }
        }
        Ok(app)
    }

    /// Gives the list vim's keys: a row walked to is in normal mode, and one written in is in
    /// insert.
    pub fn use_vim(&mut self) {
        self.vim = true;
    }

    /// Syncs with a relay while this window holds the keeper's lease, which it takes now if no
    /// other window has it and otherwise when the one that has it goes. `host` is how the status
    /// row names the relay, and `dial` starts the thread that holds the socket.
    pub fn relay(&mut self, host: String, dial: impl FnMut() -> Link + 'static) -> Result<()> {
        self.relay = Some(Relay {
            host,
            dial: Box::new(dial),
        });
        self.keep_relay()?;
        Ok(())
    }

    /// Gives up the keeper's lease, and with it the socket, when this window holds them.
    fn let_relay_go(&mut self) {
        if self.keeper.take().is_some() {
            self.sync = None;
            self.not_syncing = false;
            self.unreachable = None;
            self.device.disconnected(RELAY);
        }
    }

    /// Takes the keeper's lease if a relay is named and no window holds it, and with it the
    /// socket, and says whether anything was taken in. What the last keeper kept is read first,
    /// since this window is to append to those logs from now on and has to continue from their
    /// ends; the last keeper let go of the lease only once it would append nothing more.
    fn keep_relay(&mut self) -> Result<bool> {
        if self.keeper.is_some() || self.relay.is_none() {
            return Ok(false);
        }
        let Some(keeper) = Keeper::take(&self.dir)? else {
            return Ok(false);
        };
        self.keeper = Some(keeper);
        let changed = self.look()?;
        if let Some(relay) = &mut self.relay {
            self.sync = Some((relay.host.clone(), (relay.dial)()));
        }
        Ok(changed)
    }

    pub fn run(mut self) -> Result<()> {
        let (mut terminal, keyboard) = take_terminal()?;
        self.keyboard = Some(keyboard);
        // A panic gives the terminal back too, its keyboard first, while the alternate screen it
        // was asked on is still up: ratatui's own hook restores the rest.
        let hook = std::panic::take_hook();
        let vim = self.vim;
        std::panic::set_hook(Box::new(move |info| {
            let _ = stop_telling_keys_apart();
            if vim {
                let _ = execute!(std::io::stdout(), SetCursorStyle::DefaultUserShape);
            }
            let _ = execute!(std::io::stdout(), DisableBracketedPaste);
            hook(info);
        }));
        let outcome = self.event_loop(&mut terminal);
        give_terminal_back(self.shape.is_some());
        outcome
    }

    fn event_loop(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        loop {
            self.paint(terminal)?;
            while !event::poll(self.wait())? {
                let mut changed = self.hear()?;
                if self.looked.elapsed() >= LOOK {
                    self.looked = Instant::now();
                    changed |= self.look()?;
                    changed |= self.keep_relay()?;
                }
                if changed {
                    self.paint(terminal)?;
                }
            }
            let flow = match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => self.handle(key)?,
                Event::Paste(text) => self.paste(&text)?,
                _ => Flow::Continue,
            };
            match flow {
                Flow::Continue => {}
                Flow::Editor { id, text } => self.hand_to_editor(terminal, id, &text)?,
                Flow::Quit => return Ok(()),
            }
        }
    }

    /// Draws the view, and sets the cursor's shape where vim's keys say the mode by it.
    fn paint(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        terminal.draw(|frame| self.draw(frame))?;
        let shape = self.shape_wanted(&self.device.view());
        if shape != self.shape {
            if let Some(shape) = shape {
                execute!(std::io::stdout(), shape)?;
            }
            self.shape = shape;
        }
        Ok(())
    }

    /// The cursor's shape: with vim's keys, a block on a row in normal mode, where letters are
    /// commands, and a bar wherever they are typed; without them, the person's own, untouched.
    fn shape_wanted(&self, view: &View) -> Option<SetCursorStyle> {
        match self.keys_here(view).mode {
            _ if !self.vim => None,
            Mode::Normal => Some(SetCursorStyle::SteadyBlock),
            Mode::Plain | Mode::Insert => Some(SetCursorStyle::SteadyBar),
        }
    }

    /// Which keys are in use where the cursor is, for the footer.
    fn keys_here(&self, view: &View) -> Keys {
        let mode = match (self.vim, view.field.is_some(), self.normal) {
            (true, true, true) => Mode::Normal,
            (true, true, false) => Mode::Insert,
            _ => Mode::Plain,
        };
        Keys {
            shift_enter: matches!(self.keyboard, Some(Keyboard::Kitty | Keyboard::Modified)),
            mode,
        }
    }

    /// How long the keyboard is waited on before the app hears the relay or looks again: a
    /// twentieth of a second with a relay to hear, and with none what is left until it is time
    /// to look, so that a run of keys never keeps it from looking.
    fn wait(&self) -> Duration {
        if self.sync.is_some() {
            TICK
        } else {
            LOOK.saturating_sub(self.looked.elapsed())
        }
    }

    fn handle(&mut self, key: KeyEvent) -> Result<Flow> {
        let (code, modifiers) = meant(key);
        let ctrl = modifiers.contains(KeyModifiers::CONTROL);
        let escaped = std::mem::take(&mut self.escaped);
        // What the key acts on is read from this view, so that it costs two views and not five.
        let (zoomed, before, field, (place, empty)) = {
            let view = self.device.view();
            let field = view.field.as_ref().map(|field| field.text.to_owned());
            let caret = self.caret(&view);
            (!view.path.is_empty(), Before::of(&view), field, caret)
        };
        let line = before.line.clone();
        // Each key clears what the one before it said, so the status row says only what this
        // one did.
        self.device.say("");
        if (code, ctrl) == (KeyCode::Char('q'), true) {
            if place == Place::Row {
                self.act_from(before, Action::Close)?;
            }
            return Ok(Flow::Quit);
        }
        // With vim's keys a row walked to takes vim's, and in a row written in Escape is vim's
        // alone, keeping the row and going to normal mode with the caret a letter back, as vim's
        // Escape moves it, and Tab lands on the first row inside. The caret follows the text it
        // was before, so that a date word written out ahead of it does not move it onto another
        // letter, and where keeping changed that text it is only kept within the head. A new row
        // with only the labels it opened with has its caret where it belongs. The row stays
        // scrolled with its caret, as it was written, until the cursor leaves it.
        if self.vim && place == Place::Row {
            if self.normal {
                return match normal(code, modifiers, zoomed, empty) {
                    Some(asked) => self.in_normal(before, asked),
                    None => Ok(Flow::Continue),
                };
            }
            match code {
                KeyCode::Esc => {
                    let text = field.unwrap_or_default();
                    let after: String = head_and_body(&text)
                        .0
                        .graphemes(true)
                        .skip(self.row_cursor)
                        .collect();
                    let flow = self.act_from(before, Action::Keep)?;
                    self.normal = true;
                    if let Some(field) = &self.device.view().field {
                        let head = head_and_body(field.text).0;
                        self.row_cursor = match field.fresh {
                            Some(at) => letters_in(head, at),
                            None if head.ends_with(&after) => {
                                (letters(head) - letters(&after)).saturating_sub(1)
                            }
                            None => self.row_cursor.saturating_sub(1),
                        };
                        if field.fresh.is_none() {
                            self.row_left = Some(self.row_left.unwrap_or(0));
                        }
                    }
                    return Ok(flow);
                }
                KeyCode::Tab => return self.go_in_and_land(before),
                _ => {}
            }
        }
        if let Some(action) = bound(code, modifiers, place, zoomed, empty) {
            return self.act_from(before, action);
        }
        match code {
            KeyCode::Esc if escaped => return Ok(Flow::Quit),
            KeyCode::Esc => {
                self.escaped = true;
                self.device.say("Escape again to quit");
            }
            // On a row its head is edited, and the device is told what the whole note now
            // reads, the body under the head as it was. Left at the start of the head stays
            // there: Esc is the one way back.
            code if field.is_some() => {
                let text = field.unwrap_or_default();
                let (head, body) = match text.split_once('\n') {
                    Some((head, body)) => (head, Some(body)),
                    None => (text.as_str(), None),
                };
                let moved = edited(head, self.row_cursor, code, ctrl);
                if moved.is_some() {
                    self.row_left = Some(self.row_left.unwrap_or(0));
                }
                match moved {
                    Some((to, cursor)) if to != head => {
                        self.row_cursor = cursor;
                        let text = match body {
                            Some(body) => format!("{to}\n{body}"),
                            None => to,
                        };
                        return self.act_from(before, Action::Edited(text));
                    }
                    Some((_, cursor)) => self.row_cursor = cursor,
                    None => {}
                }
            }
            // On the line typing goes to the line, so the device is told what it now reads.
            code => match edited(&line, self.cursor, code, ctrl) {
                Some((to, cursor)) if to != line => {
                    let flow = self.act_from(before, Action::Type(to))?;
                    self.cursor = cursor;
                    return Ok(flow);
                }
                Some((_, cursor)) => self.cursor = cursor,
                None => {}
            },
        }
        Ok(Flow::Continue)
    }

    /// Does what a key asks for on a row in vim's normal mode.
    fn in_normal(&mut self, before: Before, asked: Normal) -> Result<Flow> {
        let (at, rows, line_empty, head, task, fresh) = {
            let view = self.device.view();
            let at = view.cursor.unwrap_or(0);
            let fresh = view.field.as_ref().and_then(|field| field.fresh);
            let head = view
                .field
                .as_ref()
                .map_or("", |field| head_and_body(field.text).0)
                .to_owned();
            (
                at,
                view.rows.len(),
                view.line.is_empty(),
                head,
                view.rows.get(at).is_some_and(|row| row.task.is_some()),
                fresh,
            )
        };
        match asked {
            Normal::Act(action) => self.act_from(before, action),
            Normal::Walk(by) => {
                let to = i64::try_from(at).unwrap_or(0) + by;
                let to = to.clamp(0, i64::try_from(rows.max(1)).unwrap_or(1) - 1);
                match to - i64::try_from(at).unwrap_or(0) {
                    0 => Ok(Flow::Continue),
                    by => self.act_from(before, Action::Move(by)),
                }
            }
            Normal::In => self.go_in_and_land(before),
            // A motion shows the head as it is written, scrolled with its caret, even where the
            // caret cannot move, as a key that moves the caret does without vim's keys.
            Normal::Caret(motion) => {
                let first = after_box(&head, task, usize::MAX);
                self.row_cursor = moved(&head, self.row_cursor, motion, first);
                self.row_left = Some(self.row_left.unwrap_or(0));
                Ok(Flow::Continue)
            }
            // A new row with only the labels it opened with is typed in where its caret belongs,
            // after a task's box and before the labels, so that Backspace there still takes it
            // away. A task's box, and a space after it, are not what is typed at the start of a
            // head; `[ ]Order` is a task too.
            Normal::Type(typing) => {
                let end = letters(&head);
                self.row_cursor = match (fresh, typing) {
                    (Some(at), _) => letters_in(&head, at),
                    (None, Typing::Before) => self.row_cursor.min(end),
                    (None, Typing::After) => (self.row_cursor + 1).min(end),
                    (None, Typing::Start) => after_box(&head, task, 1),
                    (None, Typing::End) => end,
                };
                self.row_left = Some(self.row_left.unwrap_or(0));
                self.normal = false;
                Ok(Flow::Continue)
            }
            // On the first row a new row above is a new note at the top, which is what Enter on an
            // empty line opens; with text on the line, that Enter would write the line instead.
            Normal::Above => match at.checked_sub(1) {
                Some(above) => {
                    self.act_from(before, Action::Select(Some(above)))?;
                    self.act(Action::Write)
                }
                None if line_empty => {
                    self.act_from(before, Action::Select(None))?;
                    self.act(Action::Write)
                }
                None => Ok(Flow::Continue),
            },
            Normal::ToLine => {
                let flow = self.act_from(before, Action::Select(None))?;
                self.cursor = letters(self.device.view().line);
                Ok(flow)
            }
        }
    }

    /// Goes into the note under the cursor and, with vim's keys, lands on the first row inside,
    /// in normal mode, as walking onto a row does. Nothing more happens if the session did not
    /// go in, as for a new row with nothing in it.
    fn go_in_and_land(&mut self, before: Before) -> Result<Flow> {
        let flow = self.act_from(before, Action::GoIn)?;
        if self.device.view().cursor.is_none() {
            return self.act(Action::Move(1));
        }
        Ok(flow)
    }

    /// Puts what was pasted into the text the cursor is in, the head of its row or the line, at
    /// the caret, as one change. A row in vim's normal mode takes nothing.
    fn paste(&mut self, text: &str) -> Result<Flow> {
        self.escaped = false;
        self.device.say("");
        let text = pasted(text);
        if text.is_empty() {
            return Ok(Flow::Continue);
        }
        let view = self.device.view();
        if self.vim && self.normal && view.field.is_some() {
            return Ok(Flow::Continue);
        }
        if let Some(field) = &view.field {
            let (head, body) = match field.text.split_once('\n') {
                Some((head, body)) => (head, Some(body)),
                None => (field.text, None),
            };
            let (to, cursor) = spliced(head, self.row_cursor, &text);
            let text = match body {
                Some(body) => format!("{to}\n{body}"),
                None => to,
            };
            self.row_cursor = cursor;
            self.row_left = Some(self.row_left.unwrap_or(0));
            self.act(Action::Edited(text))
        } else {
            let (to, cursor) = spliced(view.line, self.cursor, &text);
            let flow = self.act(Action::Type(to))?;
            self.cursor = cursor;
            Ok(flow)
        }
    }

    /// Where the cursor is, and whether the text it is in, the head of its row or the line, is
    /// empty. A new row with only the labels it opened
    /// with is empty while the caret is where it lands, after a task's box and before the rest.
    fn caret(&self, view: &View) -> (Place, bool) {
        let (place, text, caret, fresh) = match &view.field {
            Some(field) => {
                let head = head_and_body(field.text).0;
                (Place::Row, head, self.row_cursor, field.fresh)
            }
            None => (Place::Line, view.line, self.cursor, None),
        };
        let empty = text.is_empty() || fresh.map(|at| letters_in(text, at)) == Some(caret);
        (place, empty)
    }

    /// Asks the device for something, and carries out what it asks for in return.
    fn act(&mut self, action: Action) -> Result<Flow> {
        let before = Before::of(&self.device.view());
        self.act_from(before, action)
    }

    /// Acts as [`App::act`] does, with the view before it already taken.
    fn act_from(&mut self, before: Before, action: Action) -> Result<Flow> {
        let Before {
            line,
            on: was_on,
            fresh: was_fresh,
        } = before;
        let (writes, erases) = (action == Action::Write, action == Action::Erase);
        let todo = self.device.act(action);
        let flow = self.carry_out(todo)?;
        let view = self.device.view();
        // A line the device changed, as it does on the way into a note and out of one, has its
        // cursor at the end, and so does the head of a row just landed on. A head put back or
        // written in place keeps its caret where it was, if the head is still that long. A new
        // row with only the labels it opened with, landed on or put back to them, has its caret
        // where the device says, after a task's box and before the rest.
        if view.line != line {
            self.cursor = letters(view.line);
        }
        if let Some(field) = &view.field {
            let head = head_and_body(field.text).0;
            let end = letters(head);
            let stayed = under_cursor(&view) == was_on;
            self.row_cursor = match field.fresh {
                Some(start) if !(stayed && was_fresh) => letters_in(head, start),
                _ if stayed => self.row_cursor.min(end),
                _ => end,
            };
            // A row landed on reads from its start until a key writes in it or moves its caret.
            if !stayed {
                self.row_left = None;
            }
        } else {
            self.row_left = None;
        }
        // With vim's keys a row written in is in insert, and one landed on any other way is in
        // normal mode. Backspace taking an empty row away leaves the mode as it was on the row
        // above, as vim's Backspace at the start of a line in insert stays in insert.
        let now_on = under_cursor(&view);
        if writes && now_on.is_some() {
            self.normal = false;
        } else if !erases && now_on.is_some() && now_on != was_on {
            self.normal = true;
        }
        Ok(flow)
    }

    /// Does what the device asks: what it asks of the directory first, then what is to go to
    /// the relay, then the rest.
    fn carry_out(&mut self, todo: Todo) -> Result<Flow> {
        self.logs.keep(&todo, &mut self.slot)?;
        if let Some((_, link)) = &self.sync {
            // The relay is this window's only peer: its siblings read what it writes in the
            // directory, as it reads theirs.
            for said in todo.send.into_iter().filter(|said| said.to == RELAY) {
                // A thread that has gone is a relay that is not there, which the footer says.
                let _ = link.to_relay.send(said.text);
            }
        }
        if let Some(text) = todo.copy
            && !copy(&text)
        {
            self.device.say("could not write to the terminal");
        }
        if let Some(url) = todo.open
            && let Err(why) = open(&url)
        {
            self.device.say(why);
        }
        Ok(match todo.editor {
            Some(Editing { id, text }) => Flow::Editor { id, text },
            None => Flow::Continue,
        })
    }

    /// Takes in what the thread holding the socket has said since it was last asked, and says
    /// whether there was anything. Why a relay cannot be reached is kept to be said until it can.
    fn hear(&mut self) -> Result<bool> {
        let Some((relay, link)) = &self.sync else {
            return Ok(false);
        };
        let relay = relay.clone();
        let heard: Vec<sync::Event> = link.from_relay.try_iter().collect();
        let verified = self.device.key_verified();
        for event in &heard {
            match event {
                sync::Event::Connected => {
                    self.not_syncing = false;
                    self.unreachable = None;
                    let todo = self.device.connected(RELAY);
                    self.carry_out(todo)?;
                }
                sync::Event::Message(text) => {
                    let todo = self.device.received(RELAY, text);
                    self.carry_out(todo)?;
                }
                sync::Event::Disconnected => {
                    self.not_syncing = true;
                    self.device.disconnected(RELAY);
                }
                sync::Event::Unreachable(why) => {
                    self.not_syncing = true;
                    self.unreachable = Some(format!("{relay}: {why}"));
                }
            }
        }
        // Lines from the relay that this key does not open are said as soon as they come, in
        // the face's own words, which say what to do about it.
        if verified && !self.device.key_verified() {
            self.device.say("");
        }
        self.not_empty_now();
        Ok(!heard.is_empty())
    }

    /// Takes in what another writer on this machine, another window or `fzn tick` say, has added
    /// to the space's logs since they were last read, as lines from the relay are taken in, and
    /// says whether there was anything.
    fn look(&mut self) -> Result<bool> {
        let added = self.logs.load()?;
        let any = !added.is_empty();
        for (log, lines) in added {
            let todo = self.device.added(&log, lines);
            self.carry_out(todo)?;
        }
        self.not_empty_now();
        Ok(any)
    }

    /// Takes back what an empty space said once a note has come from elsewhere, since a first
    /// fill is not news and nothing else would say anything in its place.
    fn not_empty_now(&mut self) {
        let view = self.device.view();
        if view.notes > 0 && view.status == NO_NOTES {
            self.device.say("");
        }
    }

    /// What the status row says: what the last key did, or what happened since, and when
    /// nothing did, the trouble that lasts until it is put right: a passphrase that opens
    /// nothing, a relay that refused this device, until one takes it in, or a relay that cannot
    /// be reached. With none, there is no row.
    fn status_of(&self, view: &View) -> Option<String> {
        let said = view.status;
        if !said.is_empty() {
            Some(said.to_owned())
        } else if !self.device.key_verified() {
            Some(WRONG_KEY.to_owned())
        } else if let (Some(why), Some((relay, _))) = (self.device.refused(), &self.sync) {
            Some(format!("{relay}: {why}"))
        } else {
            self.unreachable.clone()
        }
    }

    /// Gives the terminal over to the editor, and saves what it leaves. An editor that cannot be
    /// run, or gives up, is said on the status row and the app stays.
    fn hand_to_editor(
        &mut self,
        terminal: &mut DefaultTerminal,
        id: String,
        text: &str,
    ) -> Result<()> {
        // Nothing is heard or looked at while the editor has the terminal, so a keeper lets the
        // relay go for as long, and another window takes it within a look; this one tries to
        // take it back at its next look after.
        self.let_relay_go();
        give_terminal_back(self.shape.take().is_some());
        let edited = edit_in(&self.editor, &id, text);
        // What the editor left is saved before the terminal is taken back, so that failing to
        // take it back loses nothing.
        self.edited(id, edited)?;
        take_terminal_back(terminal, self.keyboard.unwrap_or(Keyboard::Plain))
    }

    /// Saves what the editor left, or says why there is nothing to save. With vim's keys the row
    /// is walked back to, in normal mode, as after a body closes: its caret at the end of its
    /// head, which reads from its start.
    fn edited(&mut self, id: String, edited: Result<String, String>) -> Result<()> {
        self.normal = true;
        match edited {
            Ok(text) => {
                self.act(Action::Save { id, text })?;
            }
            Err(why) => self.device.say(why),
        }
        if self.vim
            && let Some(field) = &self.device.view().field
        {
            self.row_cursor = letters(head_and_body(field.text).0);
            self.row_left = None;
        }
        Ok(())
    }

    /// Paints the view from the top down: the path in a zoom, the line, the list, the body of what
    /// the cursor is on, and at the foot the status when there is one and the keys.
    fn draw(&mut self, frame: &mut Frame) {
        let view = self.device.view();
        let status = self.status_of(&view);
        let keys = self.keys_here(&view);
        let width = usize::from(frame.area().width);
        let height = usize::from(frame.area().height);
        let zoomed = !view.path.is_empty();
        let mut cursor = None;
        let mut y = 0;

        if zoomed {
            let crumb = breadcrumb(&self.space, &view.path, width.saturating_sub(1));
            paint(frame, y, 0, Line::from(crumb).dim());
            y += 1;
        }

        // The line, scrolled so that its cursor is in sight of whatever is said at its end.
        let archived = if view.archived_shown {
            "archived shown"
        } else {
            ""
        };
        let right = [dates_shown(view.dates), archived.to_owned()]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("  ");
        let said = right.width();
        let room = width.saturating_sub(2 + if said > 0 { said + 2 } else { 0 });
        let (shown, at, left) =
            sidescrolled(view.line, self.cursor, room, Some(self.line_left), BESIDE);
        self.line_left = left.unwrap_or(0);
        paint(frame, y, 0, Line::from(PROMPT).dim());
        paint(frame, y, 1, Line::from(shown));
        paint_right(frame, y, &right);
        if view.cursor.is_none() {
            cursor = Some((1 + at, y));
        }
        y += 1;

        // The row the cursor is on is drawn from the note as it is being typed, with the
        // terminal's cursor in its head, on one row. In vim's normal mode it is read as a row
        // landed on is until a motion moves its caret or Escape from insert leaves it written.
        let selected = view
            .cursor
            .and_then(|i| view.rows.get(i).map(|row| (i, row)))
            .map(|(i, row)| {
                let head = view
                    .field
                    .as_ref()
                    .map_or(row.head.as_str(), |f| head_and_body(f.text).0);
                let created = view.field.as_ref().and_then(|f| f.created);
                let painted = selected_row(
                    row,
                    head,
                    created,
                    (self.row_cursor, self.row_left),
                    view.scores_shown,
                    width,
                );
                (i, painted)
            });
        if let Some((_, painted)) = &selected {
            self.row_left = painted.left;
        }

        // The foot of the list is kept for the body of what the cursor is on, under a row of
        // nothing: on a row its note as it is being written, and on the line in a place the
        // place's note, if there is one. A head its row cuts is read there whole, above the
        // body. It is kept whatever the note holds, and in a place on the line too, so that
        // walking the list never changes how much of it there is; at the top the line has the
        // whole height. The status row takes the last of the rows kept, so that it does not move
        // the list either by coming or by going.
        let read = match &view.field {
            Some(field) => Some(field.text),
            None if zoomed => Some(view.here.map_or("", |here| here.text)),
            None => None,
        };
        let cut = selected.as_ref().is_some_and(|(_, painted)| painted.cut);
        let status_row = usize::from(status.is_some());
        let preview = read.map_or(0, |_| preview_rows(height.saturating_sub(1 + y)));
        let bottom = height.saturating_sub(1 + if preview > 0 { preview + 1 } else { status_row });
        if let Some(text) = read.filter(|_| preview > 0) {
            let (head, body) = head_and_body(text);
            let shown = match (cut, body.is_empty()) {
                (true, false) => format!("{head}\n{body}"),
                (true, true) => head.to_owned(),
                (false, _) => body.to_owned(),
            };
            let rows = body_rows(&shown, width, preview - status_row);
            for (k, row) in rows.into_iter().enumerate() {
                paint(frame, bottom + 1 + k, 0, row.dim());
            }
        }

        // In a place, what the line finds there is kept apart from what it only finds by a row
        // of nothing, where the view says the finds begin, counted with the row it comes before.
        let apart = |i: usize| view.finds_from == Some(i);
        let tall = |i: usize| usize::from(apart(i)) + 1;

        // The list moves only as far as it must to keep the cursor's row in sight, and only the
        // rows in sight are painted, however long the list is.
        let room = bottom.saturating_sub(y);
        match &selected {
            None => self.top = 0,
            Some((focus, _)) => {
                let focus = *focus;
                self.top = self.top.min(focus);
                let mut taken: usize = (self.top..=focus).map(tall).sum();
                while self.top < focus && taken > room {
                    taken -= tall(self.top);
                    self.top += 1;
                }
            }
        }
        let mut selected = selected;
        for (i, row) in view.rows.iter().enumerate().skip(self.top) {
            if y >= bottom {
                break;
            }
            if apart(i) {
                y += 1;
            }
            match selected.take_if(|(focus, _)| *focus == i) {
                Some((_, painted)) if y < bottom => {
                    cursor = Some((painted.x, y));
                    paint(frame, y, 0, painted.line);
                    y += 1;
                }
                Some(_) => {}
                None if y < bottom => {
                    paint(frame, y, 0, row_line(row, view.scores_shown, width));
                    y += 1;
                }
                None => {}
            }
        }

        if let Some(status) = status {
            let row = height.saturating_sub(2);
            paint(
                frame,
                row,
                1,
                Line::from(fit(&status, width.saturating_sub(2))),
            );
        }
        let foot = height.saturating_sub(1);
        let syncing = match (&self.sync, self.not_syncing) {
            (None, _) => None,
            (Some(_), true) => Some("not syncing".to_owned()),
            (Some(_), false) => self.device.clock(),
        }
        .unwrap_or_default();
        let corner = if syncing.is_empty() {
            0
        } else {
            syncing.width() + 2
        };
        let keys_room = width.saturating_sub(2 + corner);
        paint(
            frame,
            foot,
            1,
            Line::from(fit(&footer(view.hints, keys, keys_room), keys_room)).dim(),
        );
        paint_right(frame, foot, &syncing);

        if let Some((x, y)) = cursor
            && let (Ok(x), Ok(y)) = (u16::try_from(x), u16::try_from(y))
        {
            let area = frame.area();
            frame.set_cursor_position((area.x + x, area.y + y));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::tests::{set_writer, temp_dir};
    use fzn_engine::replica::{Header, Message, Out, Replica};
    use fzn_engine::{Moment, Record, Seal};
    use std::path::PathBuf;
    use std::sync::mpsc::{self, Receiver, Sender};

    const WRITER: &str = "3f9a01c2";

    fn key() -> Key {
        Key::from_bytes([7; 32])
    }

    /// The app opened on a space's directory with a key, as `fzn` opens it.
    fn open_with(space: &Path, key: Key) -> Result<App> {
        let slot = Slot::take(space, &space.with_file_name("config"))?;
        App::open(space, "main", slot, key, Weights::default(), "true".into())
    }

    /// The app on a new space under a scratch root, and the space's directory.
    fn app() -> (App, PathBuf) {
        let space = temp_dir().join("main");
        set_writer(&space, WRITER).unwrap();
        (open_with(&space, key()).unwrap(), space)
    }

    /// Takes the scratch root away.
    fn done(space: &Path) {
        fs::remove_dir_all(space.parent().unwrap()).unwrap();
    }

    /// What the logs in the directory add up to.
    fn on_disk(space: &Path) -> Store {
        Store::open(space, String::new(), Seal::new(key())).unwrap()
    }

    /// Presses a key, with Control held when `ctrl` is set.
    fn press(app: &mut App, code: KeyCode, ctrl: bool) -> Flow {
        let modifiers = if ctrl {
            KeyModifiers::CONTROL
        } else {
            KeyModifiers::NONE
        };
        press_with(app, code, modifiers)
    }

    /// Presses a key with these modifiers held.
    fn press_with(app: &mut App, code: KeyCode, modifiers: KeyModifiers) -> Flow {
        app.handle(KeyEvent::new(code, modifiers)).unwrap()
    }

    fn chord(app: &mut App, c: char) -> Flow {
        press(app, KeyCode::Char(c), true)
    }

    fn type_in(app: &mut App, text: &str) {
        for c in text.chars() {
            press(app, KeyCode::Char(c), false);
        }
    }

    /// Writes a note from the line and comes back to the line for the next: Enter takes the
    /// cursor to the note it wrote, and Esc brings it back, or with vim's keys two of them, the
    /// first to normal mode.
    fn write(app: &mut App, text: &str) {
        type_in(app, text);
        press(app, KeyCode::Enter, false);
        if on_note(app) {
            press(app, KeyCode::Esc, false);
        }
        if on_note(app) && app.vim {
            press(app, KeyCode::Esc, false);
        }
    }

    fn heads(app: &App) -> Vec<String> {
        let view = app.device.view();
        view.rows.iter().map(|r| r.head.clone()).collect()
    }

    fn line(app: &App) -> String {
        app.device.view().line.to_owned()
    }

    fn path(app: &App) -> Vec<String> {
        let view = app.device.view();
        view.path.iter().map(|&head| head.to_owned()).collect()
    }

    fn on_note(app: &App) -> bool {
        app.device.view().cursor.is_some()
    }

    /// What the field under the cursor reads, if the cursor is on a row.
    fn field(app: &App) -> Option<String> {
        app.device.view().field.map(|field| field.text.to_owned())
    }

    /// The screen as painted `width` by `height`: each row with its trailing blanks gone, which
    /// rows are dimmed and which bold from the second column, and where the terminal's cursor is.
    struct Screen {
        rows: Vec<String>,
        dim: Vec<bool>,
        bold: Vec<bool>,
        cursor: (u16, u16),
    }

    fn screen(app: &mut App, width: u16, height: u16) -> Screen {
        use ratatui::backend::TestBackend;
        use ratatui::style::Modifier;
        let mut terminal = ratatui::Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| app.draw(frame)).unwrap();
        let cursor = terminal.get_cursor_position().unwrap();
        let buffer = terminal.backend().buffer();
        let has = |y: u16, modifier: Modifier| buffer[(1, y)].modifier.contains(modifier);
        Screen {
            rows: (0..height)
                .map(|y| {
                    let row: String = (0..width).map(|x| buffer[(x, y)].symbol()).collect();
                    row.trim_end().to_owned()
                })
                .collect(),
            dim: (0..height).map(|y| has(y, Modifier::DIM)).collect(),
            bold: (0..height).map(|y| has(y, Modifier::BOLD)).collect(),
            cursor: (cursor.x, cursor.y),
        }
    }

    /// Writes a body under the note under the cursor, as the editor would.
    fn with_body(app: &mut App, text: &str) {
        let Flow::Editor { id, .. } = chord(app, 'o') else {
            panic!("the note under the cursor was not handed over");
        };
        app.act(Action::Save {
            id,
            text: text.into(),
        })
        .unwrap();
    }

    #[test]
    fn enter_writes_a_sealed_line_and_reopening_shows_the_note() {
        let (mut app, space) = app();
        assert!(app.device.view().status.starts_with("no notes yet"));
        write(&mut app, "kettlebell swings");
        assert_eq!(heads(&app), ["kettlebell swings"]);
        assert_eq!((line(&app).as_str(), app.cursor), ("", 0));
        let written = fs::read_to_string(space.join(format!("{WRITER}.jsonl"))).unwrap();
        assert_eq!(written.lines().count(), 1);
        assert!(written.starts_with(r#"{"s":1,"c":""#), "{written}");
        assert!(!written.contains("kettlebell"));

        drop(app);
        let reopened = open_with(&space, key()).unwrap();
        assert_eq!(heads(&reopened), ["kettlebell swings"]);
        assert_eq!(
            reopened.status_of(&reopened.device.view()),
            None,
            "a space with notes in it says nothing"
        );
        assert_eq!(reopened.device.device(), WRITER, "nothing to compact");
        done(&space);
    }

    #[test]
    fn a_passphrase_that_opens_nothing_here_is_refused_at_open() {
        let (mut app, space) = app();
        write(&mut app, "a garden");
        drop(app);
        let why = open_with(&space, Key::from_bytes([9; 32]))
            .err()
            .unwrap()
            .to_string();
        assert_eq!(
            why,
            "the passphrase does not open main: run fzn key to enter it again"
        );
        done(&space);
    }

    #[test]
    fn a_plain_log_from_before_sealing_is_compacted_at_open() {
        let space = temp_dir().join("main");
        set_writer(&space, WRITER).unwrap();
        let plain = |id: &str, text: &str, secs: i64| {
            Record {
                id: id.into(),
                text: text.into(),
                links: Vec::new(),
                created: Some(Moment::utc(secs)),
                updated: Some(Moment::utc(secs)),
                stamp: Some(Moment::utc(secs)),
                after: None,
                placed: None,
                archived: false,
                done: None,
                deleted: false,
                device: WRITER.into(),
            }
            .line()
            .unwrap()
        };
        let old = space.join(format!("{WRITER}.jsonl"));
        fs::write(
            &old,
            [
                plain("n1", "Boiler", 100),
                plain("n2", "House", 200),
                plain("n1", "Boiler serviced", 300),
            ]
            .concat(),
        )
        .unwrap();

        let app = open_with(&space, key()).unwrap();
        let mut seen = heads(&app);
        seen.sort();
        assert_eq!(seen, ["Boiler serviced", "House"]);
        assert_eq!(app.device.view().status, "compacted 3 versions into 2");
        let new = app.device.device().to_owned();
        assert_ne!(new, WRITER);
        assert_eq!(
            fs::read_to_string(space.join(".writer")).unwrap(),
            format!("{new}\n")
        );
        assert!(!old.exists(), "the plain log is gone");
        let logs: Vec<String> = fs::read_dir(&space)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".jsonl"))
            .collect();
        assert_eq!(logs, [format!("{new}.jsonl")]);
        let text = fs::read_to_string(space.join(&logs[0])).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            Header::read(lines[0]),
            Some(Header {
                compacts: vec![WRITER.into()],
                snapshot: 2
            })
        );
        for line in &lines[1..] {
            assert!(line.starts_with(r#"{"s":1,"#), "{line}");
            assert!(!line.contains("Boiler") && !line.contains("House"));
        }

        // Opened again, it is as it was left, and there is nothing more to do.
        drop(app);
        let again = open_with(&space, key()).unwrap();
        let mut seen = heads(&again);
        seen.sort();
        assert_eq!(seen, ["Boiler serviced", "House"]);
        assert_eq!(again.device.device(), new);
        done(&space);
    }

    #[test]
    fn a_chord_acts_on_the_line_or_on_the_note_under_the_cursor() {
        let (mut app, space) = app();
        write(&mut app, "kettlebell swings");
        write(&mut app, "boiler service");

        // On the line, Ctrl-D clears it. On a note, it deletes the note.
        type_in(&mut app, "boil");
        assert_eq!(heads(&app), ["boiler service"]);
        chord(&mut app, 'd');
        assert_eq!(line(&app), "");
        assert_eq!(heads(&app).len(), 2);
        type_in(&mut app, "boil");
        press(&mut app, KeyCode::Down, false);
        chord(&mut app, 'd');
        assert_eq!(app.device.view().notes, 1);
        assert_eq!(line(&app), "boil", "the line is left alone");
        chord(&mut app, 'd');
        assert_eq!(heads(&app), ["kettlebell swings"]);

        // On a note, Ctrl-V archives it or brings it back. On the line, it shows archived notes.
        press(&mut app, KeyCode::Down, false);
        chord(&mut app, 'v');
        assert!(heads(&app).is_empty());
        assert!(
            !on_note(&app),
            "nothing is listed, so the cursor is on the line"
        );
        chord(&mut app, 'v');
        assert_eq!(heads(&app), ["kettlebell swings"]);
        press(&mut app, KeyCode::Down, false);
        chord(&mut app, 'v');
        press(&mut app, KeyCode::Esc, false);
        chord(&mut app, 'v');
        assert_eq!(
            heads(&app),
            ["kettlebell swings"],
            "it is no longer archived"
        );

        // On a note, Ctrl-O hands it to the editor, and the cursor stays on it.
        press(&mut app, KeyCode::Down, false);
        let Flow::Editor { text, .. } = chord(&mut app, 'o') else {
            panic!("the note under the cursor was not handed over");
        };
        assert_eq!(text, "kettlebell swings");
        assert!(on_note(&app));

        // On a note, Tab goes in. On the line, Enter writes, and what is written there is linked.
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["kettlebell swings"]);
        write(&mut app, "two sets");
        assert_eq!(heads(&app), ["two sets"]);
        assert!(app.device.view().rows[0].linked);

        // Every change went to the log, and that is all there is to reopening.
        let written = fs::read_to_string(space.join(format!("{WRITER}.jsonl"))).unwrap();
        assert_eq!(written.lines().count(), 6);
        let reopened = on_disk(&space);
        let mut texts: Vec<&str> = reopened
            .notes()
            .records()
            .map(|r| r.text.as_str())
            .collect();
        texts.sort_unstable();
        assert_eq!(texts, ["kettlebell swings", "two sets"]);
        assert!(reopened.notes().records().all(|r| !r.archived));
        done(&space);
    }

    #[test]
    fn escape_comes_back_a_step_at_a_time_and_twice_quits() {
        let (mut app, space) = app();
        write(&mut app, "kettlebell swings");
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Tab, false);
        write(&mut app, "two sets");
        press(&mut app, KeyCode::Down, false);
        assert!(on_note(&app));

        press(&mut app, KeyCode::Esc, false);
        assert!(!on_note(&app), "off the list first");
        assert_eq!(path(&app).len(), 1);
        press(&mut app, KeyCode::Esc, false);
        assert!(path(&app).is_empty(), "then out of the note");

        assert!(matches!(
            press(&mut app, KeyCode::Esc, false),
            Flow::Continue
        ));
        assert_eq!(app.device.view().status, "Escape again to quit");
        assert!(matches!(press(&mut app, KeyCode::Esc, false), Flow::Quit));

        // Any other key in between, and it takes two again.
        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Left, false);
        assert!(matches!(
            press(&mut app, KeyCode::Esc, false),
            Flow::Continue
        ));
        assert!(matches!(press(&mut app, KeyCode::Esc, false), Flow::Quit));
        assert!(matches!(chord(&mut app, 'q'), Flow::Quit));
        done(&space);
    }

    #[test]
    fn tab_goes_into_what_the_line_names_wherever_the_caret_is_and_right_is_the_carets() {
        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        write(&mut app, "Fix the gutter #house");
        write(&mut app, "Cake recipe");

        type_in(&mut app, "#house");
        assert!(keys(&app).contains("Tab go in"), "{}", keys(&app));
        press(&mut app, KeyCode::Right, false);
        assert!(path(&app).is_empty(), "Right at the end does nothing");
        assert_eq!(app.cursor, 6);
        press(&mut app, KeyCode::Left, false);
        assert!(keys(&app).contains("Tab go in"), "{}", keys(&app));
        press(&mut app, KeyCode::Right, false);
        assert!(path(&app).is_empty(), "the caret moved");
        assert_eq!(app.cursor, 6);
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["#house"], "from short of the end too");
        assert_eq!(line(&app), "");
        let mut house = heads(&app)[..2].to_vec();
        house.sort();
        assert_eq!(house, ["Boiler service #house", "Fix the gutter #house"]);

        // What names nothing leaves Tab doing nothing, and types nothing.
        type_in(&mut app, "gutter and");
        assert!(!keys(&app).contains("go in"), "{}", keys(&app));
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["#house"]);
        assert_eq!(line(&app), "gutter and");
        done(&space);
    }

    #[test]
    fn escape_on_the_line_clears_it_then_steps_back_and_only_then_says_it_would_quit() {
        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Tab, false);

        type_in(&mut app, "boil");
        assert!(keys(&app).ends_with("Esc clear"), "{}", keys(&app));
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(line(&app), "", "cleared");
        assert_eq!(path(&app), ["#house"], "and still inside");
        assert!(keys(&app).ends_with("Esc back"), "{}", keys(&app));

        press(&mut app, KeyCode::Esc, false);
        assert!(path(&app).is_empty(), "out of the place");
        assert_eq!(line(&app), "#house", "with the search that found it");
        assert_eq!(app.cursor, 6);
        assert!(keys(&app).ends_with("Esc clear"), "{}", keys(&app));

        press(&mut app, KeyCode::Esc, false);
        assert_eq!(line(&app), "");
        assert_eq!(
            app.status_of(&app.device.view()),
            None,
            "a rung was taken, so nothing about quitting"
        );
        assert!(keys(&app).ends_with("Esc Esc quits"), "{}", keys(&app));
        assert!(matches!(
            press(&mut app, KeyCode::Esc, false),
            Flow::Continue
        ));
        assert_eq!(
            app.status_of(&app.device.view()).as_deref(),
            Some("Escape again to quit")
        );
        assert!(matches!(press(&mut app, KeyCode::Esc, false), Flow::Quit));

        // Ctrl-D on the line is a second key for clearing it.
        type_in(&mut app, "boil");
        chord(&mut app, 'd');
        assert_eq!(line(&app), "");
        done(&space);
    }

    #[test]
    fn in_a_place_what_the_line_only_finds_is_a_row_of_nothing_below_what_is_in_it() {
        let (mut app, space) = app();
        write(&mut app, "Boiler pressure gauge");
        write(&mut app, "Boiler");
        type_in(&mut app, "Boiler");
        press(&mut app, KeyCode::Tab, false);
        write(&mut app, "pressure low");
        assert_eq!(path(&app), ["Boiler"]);
        let shot = screen(&mut app, 60, 10);
        assert!(shot.rows[2].starts_with(" pressure low"), "{:?}", shot.rows);
        assert!(
            shot.rows[3..9].iter().all(String::is_empty),
            "only what is in it"
        );

        type_in(&mut app, "pressure");
        let shot = screen(&mut app, 60, 10);
        assert!(shot.rows[2].starts_with(" pressure low"), "{:?}", shot.rows);
        assert_eq!(shot.rows[3], "");
        assert!(
            shot.rows[4].starts_with(" Boiler pressure gauge"),
            "{:?}",
            shot.rows
        );
        done(&space);
    }

    #[test]
    fn a_new_row_inside_a_tag_opens_with_it_and_the_caret_before_it() {
        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Tab, false);
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(field(&app).as_deref(), Some(" #house"));
        assert_eq!(app.row_cursor, 0, "before the label");
        type_in(&mut app, "Order oil");
        assert_eq!(
            field(&app).as_deref(),
            Some("Order oil #house"),
            "as written"
        );

        // Put back, it is as it opened, with the caret before the label again.
        chord(&mut app, 'u');
        assert_eq!(field(&app).as_deref(), Some(" #house"));
        assert_eq!(app.row_cursor, 0);
        type_in(&mut app, "Order oil");
        press(&mut app, KeyCode::Esc, false);
        assert!(!on_note(&app));
        assert_eq!(heads(&app)[0], "Order oil #house");
        assert_eq!(path(&app), ["#house"]);
        done(&space);
    }

    #[test]
    fn backspace_on_a_new_row_inside_a_tag_with_only_the_tag_takes_it_away() {
        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Tab, false);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(heads(&app).len(), 2);
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(heads(&app), ["Boiler service #house"]);
        assert_eq!(field(&app).as_deref(), Some("Boiler service #house"));

        // With the caret moved off its start, Backspace is the caret's, and takes from the label.
        press(&mut app, KeyCode::Enter, false);
        press(&mut app, KeyCode::End, false);
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(field(&app).as_deref(), Some(" #hous"));
        assert_eq!(app.row_cursor, 6, "and the caret stays where it was");
        done(&space);
    }

    #[test]
    fn a_new_row_among_open_tasks_has_the_caret_after_its_box_and_ctrl_v_ticks_it() {
        let (mut app, space) = app();
        write(&mut app, "[ ] Order oil #house");
        type_in(&mut app, "[ ] #house");
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["[ ]", "#house"]);
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(field(&app).as_deref(), Some("[ ]  #house"));
        assert_eq!(app.row_cursor, 4, "after the box, before the label");
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(
            heads(&app),
            ["[ ] Order oil #house"],
            "and Backspace there takes it away"
        );

        press(&mut app, KeyCode::Enter, false);
        type_in(&mut app, "Bleed the radiators");
        assert_eq!(
            field(&app).as_deref(),
            Some("[ ] Bleed the radiators #house")
        );
        assert!(keys(&app).contains("^V tick"), "{}", keys(&app));
        chord(&mut app, 'v');
        assert_eq!(heads(&app), ["[ ] Order oil #house"]);
        let ticked = on_disk(&space);
        assert!(
            ticked
                .notes()
                .records()
                .any(|r| r.text == "[x] Bleed the radiators #house" && r.archived)
        );
        done(&space);
    }

    #[test]
    fn the_cursor_follows_the_line() {
        let (mut app, space) = app();
        type_in(&mut app, "héllo wörld");
        assert_eq!(app.cursor, 11);
        for _ in 0..5 {
            press(&mut app, KeyCode::Left, false);
        }
        type_in(&mut app, "X");
        assert_eq!((line(&app).as_str(), app.cursor), ("héllo Xwörld", 7));
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("héllo wörld", 6));
        press(&mut app, KeyCode::Delete, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("héllo örld", 6));
        chord(&mut app, 'a');
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("héllo örld", 0));
        chord(&mut app, 'e');
        press(&mut app, KeyCode::Delete, false);
        press(&mut app, KeyCode::Right, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("héllo örld", 10));
        chord(&mut app, 'w');
        assert_eq!((line(&app).as_str(), app.cursor), ("héllo ", 6));
        chord(&mut app, 'w');
        assert_eq!((line(&app).as_str(), app.cursor), ("", 0));
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(
            (line(&app).as_str(), app.cursor),
            ("", 0),
            "a tab is never typed"
        );

        // Off the list and back, the line's cursor is where it was left. A line the session puts
        // back has its cursor at the end, wherever it was left.
        write(&mut app, "trainingapp plan");
        type_in(&mut app, "trai");
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Down, false);
        assert!(on_note(&app));
        press(&mut app, KeyCode::Esc, false);
        type_in(&mut app, "n");
        assert!(!on_note(&app));
        assert_eq!((line(&app).as_str(), app.cursor), ("trani", 4));
        chord(&mut app, 'd');
        type_in(&mut app, "trai");
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Tab, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("", 0));
        press(&mut app, KeyCode::Esc, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("trai", 4));
        done(&space);
    }

    #[test]
    fn a_paste_is_one_change_with_its_tabs_and_line_breaks_made_spaces() {
        assert_eq!(pasted("one\ttwo\r\nthree\nfour\u{7}"), "one two three four");
        assert_eq!(
            pasted("https://example.com/boiler\n"),
            "https://example.com/boiler"
        );
        assert_eq!(pasted("\r\n Boiler \u{2028}House\t\n"), "Boiler  House");
        assert_eq!(
            pasted(" Boiler "),
            " Boiler ",
            "blanks with no break at an end stay"
        );
        let (mut app, space) = app();
        type_in(&mut app, "ab");
        press(&mut app, KeyCode::Left, false);
        app.paste("one\ttwo\nthree").unwrap();
        assert_eq!((line(&app).as_str(), app.cursor), ("aone two threeb", 14));
        assert!(path(&app).is_empty(), "a pasted tab does not go in");
        chord(&mut app, 'd');

        write(&mut app, "Boiler");
        let log = space.join(format!("{WRITER}.jsonl"));
        let count = || fs::read_to_string(&log).unwrap().lines().count();
        let before = count();
        press(&mut app, KeyCode::Down, false);
        with_body(&mut app, "Boiler\nThe combi.");
        let before_paste = count();
        assert_eq!(before_paste, before + 1);
        press(&mut app, KeyCode::Home, false);
        app.paste("Gas\tcombi ").unwrap();
        assert_eq!(
            field(&app).as_deref(),
            Some("Gas combi Boiler\nThe combi."),
            "into the head at the caret, the body kept"
        );
        assert_eq!(app.row_cursor, 10);
        assert_eq!(count(), before_paste + 1, "one version for the whole paste");
        done(&space);
    }

    #[test]
    fn keys_that_edit_the_line() {
        let key =
            |line: &str, cursor: usize, code: KeyCode, ctrl: bool| edited(line, cursor, code, ctrl);
        assert_eq!(
            key("ab", 1, KeyCode::Char('é'), false),
            Some(("aéb".into(), 2))
        );
        assert_eq!(
            key("aéb", 2, KeyCode::Backspace, false),
            Some(("ab".into(), 1))
        );
        assert_eq!(key("ab", 0, KeyCode::Backspace, false), None);
        assert_eq!(key("ab", 2, KeyCode::Delete, false), None);
        assert_eq!(
            key("one  two ", 9, KeyCode::Char('w'), true),
            Some(("one  ".into(), 5)),
            "back over the spaces, then the word"
        );
        assert_eq!(
            key("one two", 5, KeyCode::Char('w'), true),
            Some(("one wo".into(), 4)),
            "only what is before the cursor"
        );
        assert_eq!(key("ab", 9, KeyCode::Left, false), Some(("ab".into(), 1)));
        assert_eq!(key("ab", 2, KeyCode::Right, false), Some(("ab".into(), 2)));
        assert_eq!(key("ab", 1, KeyCode::Char('x'), true), None);
        assert_eq!(key("ab", 1, KeyCode::F(1), false), None);
    }

    /// The key a footer entry names.
    fn named(key: &str) -> (KeyCode, KeyModifiers) {
        let ctrl = KeyModifiers::CONTROL;
        match key {
            "Enter" => (KeyCode::Enter, KeyModifiers::NONE),
            "⇧Enter" => (KeyCode::Enter, KeyModifiers::SHIFT),
            "Esc" => (KeyCode::Esc, KeyModifiers::NONE),
            "↓" => (KeyCode::Down, KeyModifiers::NONE),
            "^↑" => (KeyCode::Up, ctrl),
            "^↓" => (KeyCode::Down, ctrl),
            "Tab" => (KeyCode::Tab, KeyModifiers::NONE),
            letter if letter.chars().count() == 1 => (
                KeyCode::Char(letter.chars().next().unwrap()),
                KeyModifiers::NONE,
            ),
            chord => {
                let mut chars = chord.chars();
                assert_eq!(chars.next(), Some('^'), "{chord}");
                let c = chars.next().unwrap().to_ascii_lowercase();
                (KeyCode::Char(c), ctrl)
            }
        }
    }

    /// The footer as it is now, having checked that every key it names asks for what it says. In
    /// insert with vim's keys Escape is vim's, and is tested on its own.
    fn keys(app: &App) -> String {
        let view = app.device.view();
        let zoomed = !view.path.is_empty();
        let (place, empty) = app.caret(&view);
        let now = app.keys_here(&view);
        for hint in keyed(view.hints, now) {
            let key = key_for(&hint.action, now);
            let (code, modifiers) = named(key);
            let asked = match now.mode {
                Mode::Insert if hint.action == Action::Select(None) => continue,
                Mode::Normal => match normal(code, modifiers, zoomed, empty) {
                    Some(Normal::Act(action)) => Some(action),
                    Some(Normal::In) => Some(Action::GoIn),
                    Some(Normal::ToLine) => Some(Action::Select(None)),
                    _ => None,
                },
                Mode::Plain | Mode::Insert => bound(code, modifiers, place, zoomed, empty),
            };
            assert_eq!(asked.as_ref(), Some(&hint.action), "{key} {}", hint.label);
        }
        footer(view.hints, now, usize::MAX)
    }

    #[test]
    fn the_footer_names_the_key_for_each_thing_on_offer() {
        let (mut app, space) = app();
        assert_eq!(
            keys(&app),
            "Enter new note  ^V show archived  Esc Esc quits",
            "nothing listed, so nothing to pick"
        );
        write(&mut app, "trainingapp");
        assert_eq!(
            keys(&app),
            "Enter new note  ↓ pick a note  ^V show archived  ^U undo  Esc Esc quits"
        );
        press(&mut app, KeyCode::Down, false);
        assert_eq!(
            keys(&app),
            "Enter new note  ^O editor  Tab go in  ^V archive  ^D delete  ^C copy  ^U undo  Esc up"
        );

        // Where the caret is in the head does not change what is on offer.
        press(&mut app, KeyCode::Left, false);
        assert_eq!(
            keys(&app),
            "Enter new note  ^O editor  Tab go in  ^V archive  ^D delete  ^C copy  ^U undo  Esc up"
        );
        press(&mut app, KeyCode::Right, false);
        assert!(path(&app).is_empty(), "the caret moved");

        // A changed head offers putting it back in place of undo.
        type_in(&mut app, "s");
        assert_eq!(
            keys(&app),
            "Enter new note  ^O editor  Tab go in  ^V archive  ^D delete  ^C copy  ^U put back  \
             Esc up"
        );
        chord(&mut app, 'u');
        assert_eq!(field(&app).as_deref(), Some("trainingapp"));
        assert_eq!(app.row_cursor, 11, "the caret is kept in the head put back");

        press(&mut app, KeyCode::Tab, false);
        write(&mut app, "squat form video https://example.com/squat");
        assert_eq!(
            keys(&app),
            "Enter new note here  ↓ pick a note  ^V show archived  ^U undo  Esc back"
        );
        press(&mut app, KeyCode::Down, false);
        assert_eq!(
            keys(&app),
            "Enter new note here  ^O editor  Tab go in  ^L unlink  ^V archive  ^D delete  \
             ^C copy  ^/ open URL  ^U undo  Esc up"
        );

        // The widest a row gets: a linked, archived note with a URL, with undo and redo to hand.
        press(&mut app, KeyCode::Esc, false);
        chord(&mut app, 'v');
        press(&mut app, KeyCode::Down, false);
        chord(&mut app, 'v');
        chord(&mut app, 'l');
        chord(&mut app, 'u');
        let widest = keys(&app);
        assert_eq!(
            widest,
            "Enter new note here  ^O editor  Tab go in  ^L unlink  ^V unarchive  ^D delete  \
             ^C copy  ^/ open URL  ^U undo  ^R redo  Esc up"
        );
        assert!(widest.chars().count() <= 126, "{}", widest.chars().count());
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            keys(&app),
            "Enter new note here  ↓ pick a note  ^V hide archived  ^U undo  ^R redo  Esc back"
        );

        // A new note offers the editor, and once it has text, everything a note offers, with
        // putting it back in place of undo.
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(keys(&app), "^O editor  Esc up");
        type_in(&mut app, "x");
        assert_eq!(
            keys(&app),
            "Enter new note here  ^O editor  Tab go in  ^L unlink  ^V archive  ^D delete  \
             ^C copy  ^↓ move down  ^U put back  Esc up"
        );
        done(&space);
    }

    #[test]
    fn ctrl_i_ctrl_m_and_ctrl_bracket_are_tab_enter_and_escape_and_ctrl_backspace_takes_a_word() {
        let ctrl = KeyModifiers::CONTROL;
        let none = KeyModifiers::NONE;
        let key = |code: KeyCode| meant(KeyEvent::new(code, ctrl));
        assert_eq!(key(KeyCode::Char('i')), (KeyCode::Tab, none));
        assert_eq!(key(KeyCode::Char('m')), (KeyCode::Enter, none));
        assert_eq!(key(KeyCode::Char('[')), (KeyCode::Esc, none));
        assert_eq!(key(KeyCode::Backspace), (KeyCode::Char('w'), ctrl));
        assert_eq!(
            key(KeyCode::Char('h')),
            (KeyCode::Char('h'), ctrl),
            "Ctrl-H is left alone, since a few terminals send it for Backspace"
        );

        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        type_in(&mut app, "#house");
        chord(&mut app, 'i');
        assert_eq!(path(&app), ["#house"], "Ctrl-I goes in");
        chord(&mut app, 'm');
        assert!(on_note(&app), "Ctrl-M opens a new row");
        chord(&mut app, '[');
        assert!(!on_note(&app), "Ctrl-[ comes back");
        type_in(&mut app, "oil tank");
        press_with(&mut app, KeyCode::Backspace, ctrl);
        assert_eq!(line(&app), "oil ", "Ctrl-Backspace takes a word");
        done(&space);
    }

    #[test]
    fn ctrl_slash_opens_a_url_and_so_do_ctrl_seven_and_ctrl_underscore_which_is_how_it_arrives() {
        let ctrl = KeyModifiers::CONTROL;
        for c in ['/', '7', '_'] {
            let on = |place| bound(KeyCode::Char(c), ctrl, place, false, false);
            assert_eq!(on(Place::Row), Some(Action::OpenUrl), "{c}");
            assert_eq!(on(Place::Line), None, "{c}");
        }
        let shifted = KeyModifiers::CONTROL | KeyModifiers::SHIFT;
        assert_eq!(
            bound(KeyCode::Char('-'), shifted, Place::Row, false, false),
            Some(Action::OpenUrl),
            "Ctrl-_ as the kitty flags send it"
        );
    }

    #[test]
    fn shift_enter_on_a_row_ticks_as_ctrl_v_does_and_on_the_line_is_enter() {
        let shift = KeyModifiers::SHIFT;
        let on = |place| bound(KeyCode::Enter, shift, place, false, false);
        assert_eq!(on(Place::Row), Some(Action::Archive));
        assert_eq!(on(Place::Line), Some(Action::Write));

        let (mut app, space) = app();
        write(&mut app, "[ ] Bleed the radiators");
        write(&mut app, "[ ] Order oil");
        press(&mut app, KeyCode::Down, false);
        press_with(&mut app, KeyCode::Enter, shift);
        let ticked = on_disk(&space);
        assert!(
            ticked
                .notes()
                .records()
                .any(|r| r.text == "[x] Order oil" && r.archived)
        );
        if on_note(&app) {
            press(&mut app, KeyCode::Esc, false);
        }
        type_in(&mut app, "Sweep the shed");
        press_with(&mut app, KeyCode::Enter, shift);
        assert_eq!(
            field(&app).as_deref(),
            Some("Sweep the shed"),
            "written, as Enter writes"
        );
        done(&space);
    }

    #[test]
    fn the_footer_says_shift_enter_ticks_where_the_terminal_tells_it_from_enter() {
        let (mut app, space) = app();
        write(&mut app, "[ ] Order oil");
        press(&mut app, KeyCode::Down, false);
        assert!(keys(&app).contains("^V tick"), "{}", keys(&app));
        for keyboard in [Keyboard::Kitty, Keyboard::Modified] {
            app.keyboard = Some(keyboard);
            assert!(keys(&app).contains("⇧Enter tick"), "{}", keys(&app));
            assert!(!keys(&app).contains("^V"), "{}", keys(&app));
        }
        press(&mut app, KeyCode::Esc, false);
        assert!(
            keys(&app).contains("^V show archived"),
            "showing archived is Ctrl-V's alone: {}",
            keys(&app)
        );
        app.keyboard = Some(Keyboard::Plain);
        press(&mut app, KeyCode::Down, false);
        assert!(keys(&app).contains("^V tick"), "{}", keys(&app));
        done(&space);
    }

    /// The app on a new space with vim's keys for the list.
    fn vim_app() -> (App, PathBuf) {
        let (mut app, space) = app();
        app.use_vim();
        (app, space)
    }

    /// The mode the keys are in where the cursor is.
    fn mode(app: &App) -> Mode {
        app.keys_here(&app.device.view()).mode
    }

    fn row_at(app: &App) -> Option<usize> {
        app.device.view().cursor
    }

    #[test]
    fn with_vim_a_row_walked_to_takes_vims_letters_and_j_and_k_stop_at_the_ends() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service");
        write(&mut app, "Fix the gutter");
        write(&mut app, "Sweep the shed");
        assert_eq!(mode(&app), Mode::Plain, "the line is always typing");
        press(&mut app, KeyCode::Down, false);
        assert_eq!(mode(&app), Mode::Normal);
        type_in(&mut app, "k");
        assert_eq!(row_at(&app), Some(0), "k never falls onto the line");
        type_in(&mut app, "j");
        assert_eq!(row_at(&app), Some(1));
        type_in(&mut app, "jj");
        assert_eq!(row_at(&app), Some(2), "nor past the last row");
        assert_eq!(field(&app).as_deref(), Some("Boiler service"));

        // Nothing is typed: not a letter vim's keys leave unbound, not a paste, and no key that
        // edits.
        type_in(&mut app, "zq3G");
        app.paste("pasted").unwrap();
        for code in [KeyCode::Delete, KeyCode::Backspace] {
            press(&mut app, code, false);
        }
        chord(&mut app, 'w');
        assert_eq!(field(&app).as_deref(), Some("Boiler service"));
        assert_eq!(row_at(&app), Some(2));
        assert_eq!(mode(&app), Mode::Normal);

        // The arrows up and down are as ever: up from the first row is the line.
        press(&mut app, KeyCode::Up, false);
        press(&mut app, KeyCode::Up, false);
        press(&mut app, KeyCode::Up, false);
        assert!(!on_note(&app));
        done(&space);
    }

    #[test]
    fn with_vim_shift_a_types_at_the_end_of_the_head_and_shift_i_after_a_tasks_box() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        write(&mut app, "[ ] Order oil");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "i");
        assert_eq!(mode(&app), Mode::Insert);
        assert_eq!(
            app.row_cursor, 13,
            "a row walked to has its caret at the end"
        );
        type_in(&mut app, " now");
        assert_eq!(field(&app).as_deref(), Some("[ ] Order oil now"));
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(mode(&app), Mode::Normal);

        type_in(&mut app, "I");
        assert_eq!(app.row_cursor, 4, "after the box");
        type_in(&mut app, "Gas ");
        assert_eq!(field(&app).as_deref(), Some("[ ] Gas Order oil now"));
        press(&mut app, KeyCode::Esc, false);
        type_in(&mut app, "A");
        assert_eq!((mode(&app), app.row_cursor), (Mode::Insert, 21));
        press(&mut app, KeyCode::Esc, false);

        type_in(&mut app, "j");
        type_in(&mut app, "I");
        assert_eq!(field(&app).as_deref(), Some("Boiler"));
        assert_eq!(app.row_cursor, 0, "at the start of a head with no box");
        done(&space);
    }

    #[test]
    fn with_vim_o_and_enter_open_a_row_under_in_insert_and_shift_o_one_above() {
        let (mut app, space) = vim_app();
        write(&mut app, "Fix the gutter");
        write(&mut app, "Sweep the shed");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "o");
        assert_eq!((row_at(&app), mode(&app)), (Some(1), Mode::Insert));
        type_in(&mut app, "Order oil");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            heads(&app),
            ["Sweep the shed", "Order oil", "Fix the gutter"]
        );

        type_in(&mut app, "O");
        assert_eq!((row_at(&app), mode(&app)), (Some(1), Mode::Insert));
        type_in(&mut app, "Bleed the radiators");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            heads(&app),
            [
                "Sweep the shed",
                "Bleed the radiators",
                "Order oil",
                "Fix the gutter"
            ]
        );

        // Enter is o, and a new row left empty goes, from normal mode as from insert.
        press(&mut app, KeyCode::Enter, false);
        assert_eq!((row_at(&app), mode(&app)), (Some(2), Mode::Insert));
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(mode(&app), Mode::Normal);
        type_in(&mut app, "j");
        assert_eq!(heads(&app).len(), 4);
        assert_eq!(field(&app).as_deref(), Some("Order oil"));

        // On the first row, O opens a new note at the top, when the line has nothing on it.
        type_in(&mut app, "kk");
        type_in(&mut app, "O");
        assert_eq!((row_at(&app), field(&app)), (Some(0), Some(String::new())));
        assert_eq!(mode(&app), Mode::Insert);
        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Up, false);
        type_in(&mut app, "the");
        press(&mut app, KeyCode::Down, false);
        let before = heads(&app);
        type_in(&mut app, "O");
        assert_eq!(heads(&app), before, "with text on the line, nothing");
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        done(&space);
    }

    #[test]
    fn with_vim_backspace_takes_an_empty_row_away_in_normal_mode_as_ever() {
        let (mut app, space) = vim_app();
        write(&mut app, "Fix the gutter");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "o");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!((row_at(&app), mode(&app)), (Some(1), Mode::Normal));
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(heads(&app), ["Fix the gutter"]);
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        done(&space);
    }

    #[test]
    fn with_vim_x_ticks_shift_d_deletes_and_u_undoes() {
        let (mut app, space) = vim_app();
        write(&mut app, "Sweep the shed");
        write(&mut app, "[ ] Order oil");
        write(&mut app, "Boiler service");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "j");
        type_in(&mut app, "x");
        let done_task = |space: &Path| {
            on_disk(space)
                .notes()
                .records()
                .any(|r| r.text == "[x] Order oil" && r.archived)
        };
        assert!(done_task(&space));
        assert_eq!(mode(&app), Mode::Normal);
        type_in(&mut app, "u");
        assert!(!done_task(&space), "undone");

        type_in(&mut app, "D");
        assert_eq!(app.device.view().notes, 2);
        assert!(on_note(&app));
        type_in(&mut app, "u");
        assert_eq!(app.device.view().notes, 3);
        done(&space);
    }

    #[test]
    fn with_vim_y_copies_and_the_chords_are_as_ever_in_normal_mode() {
        let ctrl = KeyModifiers::CONTROL;
        let none = KeyModifiers::NONE;
        let asked = |code, modifiers| normal(code, modifiers, false, false);
        assert_eq!(
            asked(KeyCode::Char('y'), none),
            Some(Normal::Act(Action::Copy))
        );
        assert_eq!(
            asked(KeyCode::Enter, KeyModifiers::SHIFT),
            Some(Normal::Act(Action::Archive))
        );
        for (c, action) in [
            ('v', Action::Archive),
            ('o', Action::Editor),
            ('l', Action::Link),
            ('c', Action::Copy),
            ('/', Action::OpenUrl),
            ('7', Action::OpenUrl),
            ('r', Action::Redo),
        ] {
            assert_eq!(
                asked(KeyCode::Char(c), ctrl),
                Some(Normal::Act(action)),
                "{c}"
            );
        }
        assert_eq!(
            asked(KeyCode::Up, ctrl),
            Some(Normal::Act(Action::Shift(-1)))
        );
        assert_eq!(asked(KeyCode::Char('G'), none), None);
        assert_eq!(asked(KeyCode::Char('d'), none), None, "so dd does nothing");
    }

    #[test]
    fn with_vim_ctrl_d_and_ctrl_u_page_by_ten_in_normal_mode_and_delete_and_undo_in_insert() {
        let (mut app, space) = vim_app();
        for n in 0..25 {
            write(&mut app, &format!("Seed packet {n}"));
        }
        press(&mut app, KeyCode::Down, false);
        for to in [10, 20, 24] {
            chord(&mut app, 'd');
            assert_eq!(row_at(&app), Some(to));
        }
        for to in [14, 4, 0] {
            chord(&mut app, 'u');
            assert_eq!(row_at(&app), Some(to));
        }
        assert_eq!(app.device.view().notes, 25, "nothing deleted");

        type_in(&mut app, "i");
        chord(&mut app, 'd');
        assert_eq!(app.device.view().notes, 24, "deleted from insert");
        assert_eq!(mode(&app), Mode::Normal);
        type_in(&mut app, "i");
        chord(&mut app, 'u');
        assert_eq!(app.device.view().notes, 25, "and undone from insert");
        done(&space);
    }

    #[test]
    fn with_vim_tab_goes_in_and_lands_on_the_first_row_inside_and_escape_comes_out_by_the_line() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        write(&mut app, "House");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "j");
        assert_eq!(field(&app).as_deref(), Some("Boiler"));
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["Boiler"]);
        assert!(!on_note(&app), "nothing inside, so the line");
        write(&mut app, "pressure low");
        write(&mut app, "gauge reads two bar");

        // l and h move the caret and never go in or out.
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "lhh");
        assert_eq!(path(&app), ["Boiler"]);
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));

        // Out of a place is the line's ladder: Escape to the line, then Escape on it.
        press(&mut app, KeyCode::Esc, false);
        assert!(!on_note(&app));
        assert_eq!(path(&app), ["Boiler"]);
        press(&mut app, KeyCode::Esc, false);
        assert!(path(&app).is_empty());

        // Tab from normal mode lands on the first row inside, in normal mode, and so does Tab
        // from insert.
        press(&mut app, KeyCode::Down, false);
        while field(&app).as_deref() != Some("Boiler") {
            type_in(&mut app, "j");
        }
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["Boiler"]);
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Down, false);
        while field(&app).as_deref() != Some("Boiler") {
            type_in(&mut app, "j");
        }
        type_in(&mut app, "i");
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["Boiler"], "Tab in insert goes in too");
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        done(&space);
    }

    #[test]
    fn with_vim_escape_in_a_row_written_in_keeps_it_and_goes_to_normal_mode() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        write(&mut app, "House");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "i");
        type_in(&mut app, " /today");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        let kept = field(&app).unwrap();
        assert!(
            !kept.contains("/today"),
            "its date words written out: {kept}"
        );
        assert_eq!(
            app.row_cursor,
            letters(head_and_body(&kept).0) - 1,
            "on the last letter of the head as kept"
        );

        // The arrows leave a row written in and land in normal mode.
        type_in(&mut app, "i");
        press(&mut app, KeyCode::Down, false);
        assert_eq!((row_at(&app), mode(&app)), (Some(1), Mode::Normal));
        type_in(&mut app, "a");
        press(&mut app, KeyCode::Up, false);
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        type_in(&mut app, "A");
        press(&mut app, KeyCode::Up, false);
        assert!(!on_note(&app));
        assert_eq!(mode(&app), Mode::Plain);
        done(&space);
    }

    #[test]
    fn with_vim_slash_goes_to_the_line_with_its_caret_at_the_end_and_the_list_as_it_is() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service");
        write(&mut app, "Boiler pressure");
        type_in(&mut app, "boil");
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Down, false);
        let listed = heads(&app);
        type_in(&mut app, "/");
        assert!(!on_note(&app));
        assert_eq!((line(&app).as_str(), app.cursor), ("boil", 4));
        assert_eq!(heads(&app), listed);
        type_in(&mut app, "er");
        assert_eq!(line(&app), "boiler", "the line types");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(
            field(&app).as_deref(),
            Some("boiler"),
            "and Enter writes it"
        );
        assert_eq!(mode(&app), Mode::Insert);
        done(&space);
    }

    #[test]
    fn with_vim_the_line_types_as_without_it_and_tab_there_stays_on_the_line() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service #house");
        type_in(&mut app, "jk");
        assert_eq!(line(&app), "jk");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(line(&app), "", "Escape on the line clears it");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["#house"]);
        assert!(
            !on_note(&app),
            "the cursor stays on the line, since you were typing"
        );
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(mode(&app), Mode::Insert, "a new row is written in");
        type_in(&mut app, "Order oil");
        assert_eq!(field(&app).as_deref(), Some("Order oil #house"));
        type_in(&mut app, "x");
        assert_eq!(field(&app).as_deref(), Some("Order oilx #house"));
        done(&space);
    }

    #[test]
    fn with_vim_the_footer_writes_vims_letters_tab_and_esc_in_normal_mode_and_esc_done_in_insert() {
        let (mut app, space) = vim_app();
        write(&mut app, "trainingapp");
        press(&mut app, KeyCode::Down, false);
        assert_eq!(
            keys(&app),
            "i type  o new note  ^O editor  Tab go in  x archive  D delete  y copy  u undo  Esc up"
        );
        app.keyboard = Some(Keyboard::Kitty);
        assert!(
            keys(&app).contains("x archive"),
            "x, as ever in normal mode"
        );
        type_in(&mut app, "i");
        assert_eq!(
            keys(&app),
            "Enter new note  ^O editor  Tab go in  ⇧Enter archive  ^D delete  ^C copy  ^U undo  \
             Esc done"
        );
        press(&mut app, KeyCode::Tab, false);
        write(&mut app, "squat form video https://example.com/squat");
        press(&mut app, KeyCode::Down, false);
        assert_eq!(
            keys(&app),
            "i type  o new note here  ^O editor  Tab go in  ^L unlink  x archive  D delete  \
             y copy  ^/ open URL  u undo  Esc up",
            "in a place, no h back"
        );
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            keys(&app),
            "Enter new note here  ↓ pick a note  ^V show archived  ^U undo  Esc back",
            "the line is as without vim"
        );
        done(&space);
    }

    #[test]
    fn with_vim_the_cursor_is_a_block_on_a_row_in_normal_mode_and_a_bar_where_letters_type() {
        let (mut app, space) = app();
        write(&mut app, "Boiler");
        let shape = |app: &App| app.shape_wanted(&app.device.view());
        assert_eq!(
            shape(&app),
            None,
            "without vim the cursor is the person's own"
        );
        press(&mut app, KeyCode::Down, false);
        assert_eq!(shape(&app), None);
        press(&mut app, KeyCode::Esc, false);

        app.use_vim();
        assert_eq!(shape(&app), Some(SetCursorStyle::SteadyBar));
        press(&mut app, KeyCode::Down, false);
        assert_eq!(shape(&app), Some(SetCursorStyle::SteadyBlock));
        type_in(&mut app, "i");
        assert_eq!(shape(&app), Some(SetCursorStyle::SteadyBar));
        done(&space);
    }

    #[test]
    fn tmux_is_asked_for_modify_other_keys_only_with_extended_keys_on_and_the_csi_u_format() {
        assert!(passes_csi_u("on|csi-u\n"));
        assert!(passes_csi_u("always|csi-u"));
        assert!(!passes_csi_u("off|csi-u\n"), "off passes nothing on");
        assert!(!passes_csi_u("on|xterm\n"), "crossterm drops xterm's form");
        assert!(!passes_csi_u("|"), "an older tmux knows neither");
        assert!(!passes_csi_u(""));
    }

    #[test]
    fn with_vim_the_editor_hands_the_row_back_in_normal_mode() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "i");
        let Flow::Editor { id, text } = chord(&mut app, 'o') else {
            panic!("the note under the cursor was not handed over");
        };
        app.edited(id.clone(), Ok(format!("{text}\nThe combi.")))
            .unwrap();
        assert_eq!(field(&app).as_deref(), Some("Boiler\nThe combi."));
        assert_eq!(mode(&app), Mode::Normal);
        assert_eq!((app.row_cursor, app.row_left), (6, None), "walked back to");

        type_in(&mut app, "0i");
        app.edited(id, Err("vi exited with 1; note unchanged".into()))
            .unwrap();
        assert_eq!(mode(&app), Mode::Normal, "however the editor finished");
        assert_eq!((app.row_cursor, app.row_left), (6, None));
        done(&space);
    }

    #[test]
    fn with_vim_ctrl_bracket_and_ctrl_i_in_a_row_written_in_are_escape_and_tab() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        write(&mut app, "House");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "i");
        chord(&mut app, '[');
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        type_in(&mut app, "ji");
        chord(&mut app, 'i');
        assert_eq!(path(&app), ["Boiler"]);
        assert!(!on_note(&app), "nothing inside, so the line");
        done(&space);
    }

    #[test]
    fn with_vim_a_new_row_is_typed_in_where_its_caret_belongs_and_backspace_takes_it_away() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service #house");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Tab, false);
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "O");
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Insert));
        assert_eq!(field(&app).as_deref(), Some(" #house"));
        assert_eq!(app.row_cursor, 0, "before the label");
        press(&mut app, KeyCode::Esc, false);
        for letter in ["i", "a", "A", "I"] {
            type_in(&mut app, letter);
            assert_eq!(app.row_cursor, 0, "{letter}");
            press(&mut app, KeyCode::Esc, false);
        }
        type_in(&mut app, "i");
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(heads(&app), ["Boiler service #house"]);
        assert!(!on_note(&app), "the first row went, so the line");

        write(&mut app, "[ ]Order oil");
        press(&mut app, KeyCode::Down, false);
        assert_eq!(field(&app).as_deref(), Some("[ ]Order oil #house"));
        type_in(&mut app, "I");
        assert_eq!(app.row_cursor, 3, "after a box with no space after it");
        done(&space);
    }

    #[test]
    fn with_vim_backspace_taking_a_row_away_in_insert_leaves_insert_on_the_row_above() {
        let (mut app, space) = vim_app();
        write(&mut app, "Fix the gutter");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "o");
        assert_eq!((row_at(&app), mode(&app)), (Some(1), Mode::Insert));
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(heads(&app), ["Fix the gutter"]);
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Insert));
        type_in(&mut app, "s");
        assert_eq!(field(&app).as_deref(), Some("Fix the gutters"));
        done(&space);
    }

    #[test]
    fn with_vim_a_paste_goes_into_a_row_written_in_and_onto_the_line() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "A");
        app.paste(" service").unwrap();
        assert_eq!(field(&app).as_deref(), Some("Boiler service"));
        press(&mut app, KeyCode::Esc, false);
        type_in(&mut app, "/");
        app.paste("boil").unwrap();
        assert_eq!(line(&app), "boil");
        done(&space);
    }

    #[test]
    fn with_vim_h_l_and_the_arrows_move_the_caret_a_letter_and_stop_at_the_ends() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        assert_eq!(app.row_cursor, 6);
        type_in(&mut app, "l");
        assert_eq!(app.row_cursor, 6, "l stops at the end");
        type_in(&mut app, "h");
        assert_eq!(app.row_cursor, 5);
        type_in(&mut app, "hhhhhhh");
        assert_eq!(app.row_cursor, 0, "h stops at the start");
        press(&mut app, KeyCode::Left, false);
        assert_eq!(app.row_cursor, 0);
        press(&mut app, KeyCode::Right, false);
        press(&mut app, KeyCode::Right, false);
        assert_eq!(app.row_cursor, 2);
        assert_eq!((row_at(&app), mode(&app)), (Some(0), Mode::Normal));
        assert_eq!(field(&app).as_deref(), Some("Boiler"), "nothing written");

        // With a modifier the arrows do nothing, so that nothing is selected in normal mode.
        for modifiers in [
            KeyModifiers::SHIFT,
            KeyModifiers::CONTROL,
            KeyModifiers::ALT,
        ] {
            press_with(&mut app, KeyCode::Left, modifiers);
            press_with(&mut app, KeyCode::Right, modifiers);
            press_with(&mut app, KeyCode::Home, modifiers);
            press_with(&mut app, KeyCode::End, modifiers);
            assert_eq!(app.row_cursor, 2, "{modifiers:?}");
        }
        done(&space);
    }

    #[test]
    fn with_vim_0_and_home_go_to_the_start_caret_after_a_tasks_box_and_dollar_and_end_to_the_end() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service");
        write(&mut app, "[ ] Order oil");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "0");
        assert_eq!(app.row_cursor, 0);
        type_in(&mut app, "^");
        assert_eq!(
            app.row_cursor, 4,
            "the first letter after the box and its space"
        );
        type_in(&mut app, "$");
        assert_eq!(app.row_cursor, 13, "after the last letter");
        press(&mut app, KeyCode::Home, false);
        assert_eq!(app.row_cursor, 0);
        press(&mut app, KeyCode::End, false);
        assert_eq!(app.row_cursor, 13);
        press_with(&mut app, KeyCode::Char('^'), KeyModifiers::SHIFT);
        assert_eq!(app.row_cursor, 4, "shifted, as some terminals send it");

        type_in(&mut app, "j$^");
        assert_eq!(field(&app).as_deref(), Some("Boiler service"));
        assert_eq!(app.row_cursor, 0, "the start, where there is no box");
        press_with(&mut app, KeyCode::Char('$'), KeyModifiers::SHIFT);
        assert_eq!(app.row_cursor, 14);
        assert_eq!(
            after_box("[x]   Sweep", true, usize::MAX),
            6,
            "every space after it"
        );
        done(&space);
    }

    #[test]
    fn w_b_and_e_go_by_vims_small_words_parting_letters_from_punctuation_at_blanks() {
        let head = "fix boiler-valve, now_ok  x";
        let walk = |motion, from: &[usize]| -> Vec<usize> {
            from.iter().map(|&at| moved(head, at, motion, 0)).collect()
        };
        assert_eq!(
            walk(Motion::Word, &[0, 4, 10, 11, 16, 18, 26, 27]),
            [4, 10, 11, 16, 18, 26, 27, 27]
        );
        assert_eq!(
            walk(Motion::WordBack, &[27, 26, 25, 18, 16, 13, 11, 10, 4, 0]),
            [26, 18, 18, 16, 11, 11, 10, 4, 0, 0]
        );
        assert_eq!(
            walk(Motion::WordEnd, &[0, 2, 5, 9, 10, 15, 16, 23, 26, 27]),
            [2, 9, 9, 10, 15, 16, 23, 26, 26, 27]
        );

        // A letter is as drawn: an accent goes with its letter, and a wide letter is one, so a
        // Chinese run is one word and a cup of coffee is punctuation.
        let head = "cafe\u{301} ☕ 火车 ok";
        let walk = |motion, at| moved(head, at, motion, 0);
        assert_eq!(walk(Motion::Word, 0), 5);
        assert_eq!(walk(Motion::Word, 5), 7);
        assert_eq!(walk(Motion::WordEnd, 0), 3);
        assert_eq!(walk(Motion::WordEnd, 5), 8);
        assert_eq!(walk(Motion::WordBack, 10), 7);
        assert_eq!(walk(Motion::On, 3), 4);
    }

    #[test]
    fn with_vim_i_types_before_the_letter_under_the_caret_and_a_after_it() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "b");
        assert_eq!(app.row_cursor, 7);
        type_in(&mut app, "i");
        type_in(&mut app, "gas ");
        assert_eq!(field(&app).as_deref(), Some("Boiler gas service"));
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            app.row_cursor, 10,
            "a letter back from where typing stopped"
        );
        type_in(&mut app, "0a");
        assert_eq!((mode(&app), app.row_cursor), (Mode::Insert, 1));
        type_in(&mut app, "-");
        assert_eq!(field(&app).as_deref(), Some("B-oiler gas service"));
        press(&mut app, KeyCode::Esc, false);
        for letter in ["i", "a"] {
            type_in(&mut app, "$");
            type_in(&mut app, letter);
            assert_eq!(
                app.row_cursor, 19,
                "at the end both type at the end: {letter}"
            );
            press(&mut app, KeyCode::Esc, false);
        }
        done(&space);
    }

    #[test]
    fn with_vim_escape_from_insert_moves_the_caret_a_letter_back_within_the_head_as_kept() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "i");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!((mode(&app), app.row_cursor), (Mode::Normal, 5));
        type_in(&mut app, "I");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!((mode(&app), app.row_cursor), (Mode::Normal, 0));

        // Keeping a head that writes out shorter puts the caret on its last letter.
        type_in(&mut app, "A");
        type_in(&mut app, " /task");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(field(&app).as_deref(), Some("[ ] Boiler"));
        assert_eq!(app.row_cursor, 9);
        done(&space);
    }

    #[test]
    fn with_vim_escape_in_normal_mode_goes_to_the_line_with_its_caret_at_the_end() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service");
        write(&mut app, "Boiler pressure");
        type_in(&mut app, "boil");
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Down, false);
        let listed = heads(&app);
        assert!(matches!(
            press(&mut app, KeyCode::Esc, false),
            Flow::Continue
        ));
        assert!(!on_note(&app));
        assert_eq!((line(&app).as_str(), app.cursor), ("boil", 4));
        assert_eq!(heads(&app), listed, "the list as it is");
        assert_eq!(app.device.view().status, "", "nothing about quitting");
        done(&space);
    }

    #[test]
    fn with_vim_two_escapes_from_insert_land_on_the_line_and_none_quits() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        type_in(&mut app, "boil");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "i");
        for _ in 0..2 {
            assert!(matches!(
                press(&mut app, KeyCode::Esc, false),
                Flow::Continue
            ));
        }
        assert!(!on_note(&app), "on the line");
        assert_eq!(line(&app), "boil");
        assert!(matches!(
            press(&mut app, KeyCode::Esc, false),
            Flow::Continue
        ));
        assert_eq!(line(&app), "", "the third clears the line");
        assert_eq!(app.device.view().status, "", "and says nothing of quitting");
        done(&space);
    }

    #[test]
    fn with_vim_e_goes_to_the_last_letter_of_the_word_through_the_keys() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler service due");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "0e");
        assert_eq!(app.row_cursor, 5);
        type_in(&mut app, "e");
        assert_eq!(app.row_cursor, 13);
        type_in(&mut app, "ee");
        assert_eq!(
            app.row_cursor, 17,
            "the last word's last letter, and there it stays"
        );
        assert_eq!(mode(&app), Mode::Normal);
        done(&space);
    }

    #[test]
    fn with_vim_escape_from_insert_in_a_row_written_from_the_line_stays_scrolled_with_its_caret() {
        let (mut app, space) = vim_app();
        type_in(
            &mut app,
            "the radiators in the house want bleeding before the winter",
        );
        press(&mut app, KeyCode::Enter, false);
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(mode(&app), Mode::Normal);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…leeding before the winter");
        let under = shot.rows[1].chars().nth(usize::from(shot.cursor.0));
        assert_eq!(
            (shot.cursor, under),
            ((26, 1), Some('r')),
            "on the last letter"
        );
        type_in(&mut app, "iZ");
        assert_eq!(
            field(&app).as_deref(),
            Some("the radiators in the house want bleeding before the winteZr")
        );
        done(&space);
    }

    #[test]
    fn with_vim_escape_in_a_new_task_row_keeps_the_caret_at_its_own_place() {
        let (mut app, space) = vim_app();
        write(&mut app, "[ ] Order oil #house");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Tab, false);
        type_in(&mut app, "[ ]");
        press(&mut app, KeyCode::Tab, false);
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "O");
        let fresh = field(&app).unwrap();
        let at = app.row_cursor;
        assert!(fresh.starts_with("[ ] ") && at == 4, "{fresh:?} {at}");
        press(&mut app, KeyCode::End, false);
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            (mode(&app), app.row_cursor),
            (Mode::Normal, 4),
            "not a letter back"
        );
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(heads(&app).len(), 1, "Backspace still takes it away");
        done(&space);
    }

    #[test]
    fn with_vim_escape_from_insert_follows_the_text_when_a_date_word_before_it_is_written_out() {
        let (mut app, space) = vim_app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "A");
        type_in(&mut app, " /today service");
        for _ in 0.."service".len() {
            press(&mut app, KeyCode::Left, false);
        }
        press(&mut app, KeyCode::Esc, false);
        let kept = field(&app).unwrap();
        assert!(!kept.contains("/today"), "{kept}");
        let head: Vec<&str> = head_and_body(&kept).0.graphemes(true).collect();
        assert_eq!(
            head[app.row_cursor..].concat(),
            " service",
            "a letter back from the s of service, where typing stopped, in {kept:?}"
        );
        done(&space);
    }

    /// The far ends of a link to a relay: what the window sent, and a way to tell it things.
    type Ends = (Receiver<String>, Sender<sync::Event>);

    /// Names a relay for a window, and gives back where the far ends of each link it dials go.
    fn dialler(app: &mut App) -> Receiver<Ends> {
        dialler_shown(app, "nas.local")
    }

    /// [`dialler`], with the relay shown as `host`.
    fn dialler_shown(app: &mut App, host: &str) -> Receiver<Ends> {
        let (ends, dialled) = mpsc::channel();
        app.relay(host.into(), move || {
            let (to_relay, sent) = mpsc::channel();
            let (tell, from_relay) = mpsc::channel();
            let _ = ends.send((sent, tell));
            Link {
                to_relay,
                from_relay,
            }
        })
        .unwrap();
        dialled
    }

    /// The far ends of a window's link to the relay, which it dials at once as the only window.
    fn link(app: &mut App) -> Ends {
        dialler(app)
            .try_recv()
            .expect("the only window holds the keeper's lease")
    }

    /// Connects the app to a relay, and passes what each says to the other until neither has
    /// more to say.
    fn connect(
        app: &mut App,
        relay: &mut Replica,
        sent: &Receiver<String>,
        tell: &Sender<sync::Event>,
    ) {
        relay.disconnect(0);
        tell.send(sync::Event::Connected).unwrap();
        let greeting = relay.connect(0);
        pass(greeting, tell);
        settle(app, relay, sent, tell);
    }

    /// Hands the app what the relay says to it.
    fn pass(out: Vec<Out>, tell: &Sender<sync::Event>) {
        for out in out {
            if let Out::Send(0, message) = out {
                let text = message.encode().unwrap();
                tell.send(sync::Event::Message(text)).unwrap();
            }
        }
    }

    fn settle(
        app: &mut App,
        relay: &mut Replica,
        sent: &Receiver<String>,
        tell: &Sender<sync::Event>,
    ) {
        for _ in 0..10 {
            app.hear().unwrap();
            for text in sent.try_iter() {
                let out = relay.receive(0, Message::decode(&text).unwrap());
                pass(out, tell);
            }
        }
    }

    /// Sealed lines another device with the same passphrase wrote.
    fn written_elsewhere(text: &str, key: Key) -> Vec<String> {
        let mut other = Device::new(Box::new(System), "dev-b", Weights::default(), key);
        other.act(Action::Type(text.into()));
        let todo = other.act(Action::Write);
        todo.keep.into_iter().flat_map(|kept| kept.lines).collect()
    }

    #[test]
    fn what_the_relay_sends_shows_up_and_is_kept() {
        let (mut app, space) = app();
        write(&mut app, "mine");
        let (sent, tell) = link(&mut app);
        assert!(!app.hear().unwrap(), "nothing until something is said");
        let mut relay = Replica::new();
        relay.append("dev-b", written_elsewhere("boiler service booked", key()));

        connect(&mut app, &mut relay, &sent, &tell);
        assert!(!app.not_syncing);
        let mut seen = heads(&app);
        seen.sort();
        assert_eq!(seen, ["boiler service booked", "mine"]);
        assert_eq!(app.device.view().status, "1 changed elsewhere");
        let kept = fs::read_to_string(space.join("dev-b.jsonl")).unwrap();
        assert_eq!(kept.lines().collect::<Vec<_>>(), relay.lines("dev-b"));
        assert_eq!(
            relay.lines(WRITER).len(),
            1,
            "and the relay has what was here"
        );

        // What is written while the relay is away reaches it when it is back.
        tell.send(sync::Event::Disconnected).unwrap();
        assert!(app.hear().unwrap());
        assert!(app.not_syncing);
        write(&mut app, "pressure dropped");
        assert!(sent.try_recv().is_err());
        connect(&mut app, &mut relay, &sent, &tell);
        assert_eq!(relay.lines(WRITER).len(), 2);

        // Reopened, the app has it all without the relay.
        drop(app);
        let reopened = open_with(&space, key()).unwrap();
        assert_eq!(heads(&reopened).len(), 3);
        done(&space);
    }

    #[test]
    fn what_a_command_writes_beside_the_open_app_is_listed_and_sent_on() {
        let (mut app, space) = app();
        write(&mut app, "[ ] Order oil #house");
        write(&mut app, "Boiler");
        let (sent, tell) = link(&mut app);
        let mut relay = Replica::new();
        connect(&mut app, &mut relay, &sent, &tell);
        let oil = on_disk(&space)
            .notes()
            .records()
            .find(|record| record.text.starts_with("[ ]"))
            .unwrap()
            .id
            .clone();
        let tick = |done| {
            let config = space.with_file_name("config");
            let (beside, _) = store::Beside::open(&space, "main", &config, key(), None).unwrap();
            let ids = std::slice::from_ref(&oil);
            beside.tick(ids, done, &mut Vec::new()).unwrap();
        };
        assert!(!app.look().unwrap(), "nothing yet");

        tick(true);
        assert!(app.look().unwrap());
        assert_eq!(heads(&app), ["Boiler"]);
        assert_eq!(app.device.view().status, "1 changed elsewhere");
        settle(&mut app, &mut relay, &sent, &tell);
        let commands = fs::read_to_string(space.join(".writer.1")).unwrap();
        let log = space.join(format!("{}.jsonl", commands.trim()));
        let kept = fs::read_to_string(&log).unwrap();
        assert_eq!(
            relay.lines(commands.trim()),
            kept.lines().collect::<Vec<_>>()
        );

        // A line still being written is taken once it is whole.
        tick(false);
        let whole = fs::read(&log).unwrap();
        fs::write(&log, &whole[..kept.len() + 40]).unwrap();
        assert!(!app.look().unwrap());
        assert_eq!(heads(&app), ["Boiler"]);
        fs::write(&log, &whole).unwrap();
        assert!(app.look().unwrap());
        assert_eq!(heads(&app), ["Boiler", "[ ] Order oil #house"]);

        // Reopened, the app reads both logs, and has nothing to say of them.
        drop(app);
        let reopened = open_with(&space, key()).unwrap();
        assert_eq!(heads(&reopened), ["Boiler", "[ ] Order oil #house"]);
        assert_eq!(reopened.status_of(&reopened.device.view()), None);
        done(&space);
    }

    #[test]
    fn two_windows_on_one_directory_list_each_others_notes_within_a_look() {
        let (mut first, space) = app();
        let mut second = open_with(&space, key()).unwrap();
        assert_ne!(first.device.device(), second.device.device());

        write(&mut first, "Boiler #house");
        assert!(second.look().unwrap());
        assert_eq!(heads(&second), ["Boiler #house"]);
        assert_eq!(
            second.status_of(&second.device.view()),
            None,
            "a first fill is no news"
        );

        // Each in its own place, and each writing a log of its own.
        type_in(&mut first, "#house");
        press(&mut first, KeyCode::Tab, false);
        assert_eq!(path(&first), ["#house"]);
        write(&mut second, "Order oil #house");
        assert!(path(&second).is_empty());
        assert!(first.look().unwrap());
        assert_eq!(heads(&first), ["Order oil #house", "Boiler #house"]);
        assert!(
            !first.look().unwrap(),
            "and nothing more until something is added"
        );
        let logs = |slot: &str| {
            let id = fs::read_to_string(space.join(slot)).unwrap();
            fs::read_to_string(space.join(format!("{}.jsonl", id.trim())))
                .unwrap()
                .lines()
                .count()
        };
        assert_eq!((logs(".writer"), logs(".writer.1")), (1, 1));

        // Opened again, a window has both.
        drop((first, second));
        assert_eq!(heads(&open_with(&space, key()).unwrap()).len(), 2);
        done(&space);
    }

    #[test]
    fn only_the_keeper_holds_the_socket_and_keeps_the_relays_lines_and_the_other_reads_them() {
        let (mut first, space) = app();
        let mut second = open_with(&space, key()).unwrap();
        let (sent, tell) = link(&mut first);
        let dialled = dialler(&mut second);
        assert!(
            dialled.try_recv().is_err(),
            "the second window dials nothing"
        );

        let mut relay = Replica::new();
        relay.append("dev-b", written_elsewhere("Pressure dropped", key()));
        connect(&mut first, &mut relay, &sent, &tell);
        assert_eq!(heads(&first), ["Pressure dropped"]);
        assert!(second.look().unwrap());
        assert_eq!(heads(&second), ["Pressure dropped"]);
        assert_eq!(
            second.status_of(&second.device.view()),
            None,
            "nor is it said to sync"
        );

        // What the second window writes reaches the relay through the first.
        write(&mut second, "Boiler #house");
        assert!(first.look().unwrap());
        settle(&mut first, &mut relay, &sent, &tell);
        assert_eq!(relay.lines(second.device.device()).len(), 1);

        // The keeper goes, and the other takes the lease at its next look and dials.
        relay.append("dev-b", written_elsewhere("Gate fixed", key()));
        drop(first);
        assert!(dialled.try_recv().is_err());
        second.keep_relay().unwrap();
        let (sent, tell) = dialled
            .try_recv()
            .expect("the second window dials once it keeps");
        connect(&mut second, &mut relay, &sent, &tell);
        let mut seen = heads(&second);
        seen.sort();
        assert_eq!(seen, ["Boiler #house", "Gate fixed", "Pressure dropped"]);
        let kept = fs::read_to_string(space.join("dev-b.jsonl")).unwrap();
        assert_eq!(
            kept.lines().collect::<Vec<_>>(),
            relay.lines("dev-b"),
            "no line twice"
        );
        done(&space);
    }

    #[test]
    fn a_keeper_lets_the_relay_go_while_its_editor_has_the_terminal() {
        let (mut first, space) = app();
        let mut second = open_with(&space, key()).unwrap();
        let _ends = link(&mut first);
        let dialled = dialler(&mut second);
        assert!(dialled.try_recv().is_err());

        // What hands the terminal to the editor lets the lease go first.
        first.let_relay_go();
        assert!(first.sync.is_none() && first.keeper.is_none());
        second.keep_relay().unwrap();
        assert!(
            dialled.try_recv().is_ok(),
            "the other window keeps the relay meanwhile"
        );
        first.keep_relay().unwrap();
        assert!(first.keeper.is_none(), "and keeps it until it goes");
        done(&space);
    }

    #[test]
    fn what_an_importer_writes_beside_an_open_window_is_listed_within_a_look() {
        let (mut app, space) = app();
        let _in_use = store::Lock::share(&space, "main").unwrap();
        let slot = Slot::take(&space, &space.with_file_name("config")).unwrap();
        let mut importing = Store::open(&space, slot.id().to_owned(), Seal::new(key())).unwrap();
        let mut record = Record::new("imported", "Prune the roses #garden");
        record.created = Some(Moment::utc(10));
        record.updated = record.created;
        importing.put(record).unwrap();
        assert!(app.look().unwrap());
        assert_eq!(heads(&app), ["Prune the roses #garden"]);
        done(&space);
    }

    #[test]
    fn a_device_given_lines_its_passphrase_does_not_open_writes_nothing() {
        let (mut app, space) = {
            let space = temp_dir().join("main");
            set_writer(&space, WRITER).unwrap();
            (open_with(&space, Key::from_bytes([9; 32])).unwrap(), space)
        };
        let (sent, tell) = link(&mut app);
        let mut relay = Replica::new();
        relay.append("dev-b", written_elsewhere("boiler service booked", key()));
        connect(&mut app, &mut relay, &sent, &tell);
        assert!(heads(&app).is_empty());
        assert_eq!(
            app.status_of(&app.device.view()).as_deref(),
            Some(WRONG_KEY),
            "said as soon as it comes"
        );
        write(&mut app, "a garden");
        let said = app.status_of(&app.device.view()).unwrap_or_default();
        assert!(said.contains("passphrase"), "{said}");
        press(&mut app, KeyCode::Left, false);
        assert_eq!(
            app.status_of(&app.device.view()).as_deref(),
            Some(WRONG_KEY),
            "and it is said until it is put right"
        );
        assert!(!space.join(format!("{WRITER}.jsonl")).exists());
        assert!(relay.lines(WRITER).is_empty());
        done(&space);
    }

    #[test]
    fn the_editor_is_handed_the_text_and_what_it_leaves_is_saved() {
        let (mut app, space) = app();
        write(&mut app, "the plan");
        assert!(
            matches!(chord(&mut app, 'o'), Flow::Continue),
            "on the line there is no note to hand over"
        );
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "s");
        let Flow::Editor { id, text } = chord(&mut app, 'o') else {
            panic!("the note under the cursor was not handed over");
        };
        assert_eq!(text, "the plans", "the head as it now reads");

        // An editor that changes nothing, and one that gives up.
        assert_eq!(edit_in("true", &id, &text), Ok("the plans".into()));
        let gave_up = edit_in("false", &id, &text).unwrap_err();
        assert_eq!(gave_up, "false exited with 1; note unchanged");

        // One that leaves something else behind.
        let left = space.join("left-by-the-editor.md");
        fs::write(&left, "The plan\nwith a body\n").unwrap();
        let editor = format!("cp {}", left.display());
        let edited = edit_in(&editor, &id, &text).unwrap();
        app.act(Action::Save { id, text: edited }).unwrap();
        let view = app.device.view();
        assert_eq!(view.status, "saved");
        assert_eq!(view.field.unwrap().text, "The plan\nwith a body");
        assert!(view.rows[0].has_body);
        let written = fs::read_to_string(space.join(format!("{WRITER}.jsonl"))).unwrap();
        assert_eq!(
            written.lines().count(),
            3,
            "the note, the letter typed, and what the editor left"
        );
        done(&space);
    }

    #[test]
    fn the_editors_file_is_the_owners_alone_and_goes_whatever_the_editor_did() {
        let dir = temp_dir();
        let seen = dir.join("seen");
        // The editor notes where the file is, then does as it is told.
        let editor = |then: &str| {
            let script = dir.join(format!("editor-{}", then.replace(' ', "-")));
            let note = format!("#!/bin/sh\necho \"$1\" > {}\n{then}\n", seen.display());
            fs::write(&script, note).unwrap();
            format!("sh {}", script.display())
        };
        for (then, ok) in [("exit 0", true), ("exit 1", false)] {
            assert_eq!(edit_in(&editor(then), "n1", "Boiler").is_ok(), ok);
            let path = fs::read_to_string(&seen).unwrap();
            assert!(!Path::new(path.trim()).exists(), "{path} was left behind");
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_editors_file_is_in_the_runtime_directory_or_one_of_its_own_and_only_the_owners() {
        #[cfg(unix)]
        let mode = |path: &Path| {
            use std::os::unix::fs::PermissionsExt as _;
            fs::metadata(path).unwrap().permissions().mode() & 0o777
        };
        let (runtime, temp) = (temp_dir(), temp_dir());

        let handed = Handed::within(Some(runtime.clone()), &temp, "n1", "Boiler").unwrap();
        assert_eq!(handed.path.parent(), Some(runtime.as_path()));
        assert_eq!(fs::read_to_string(&handed.path).unwrap(), "Boiler");
        #[cfg(unix)]
        assert_eq!(mode(&handed.path), 0o600);
        let path = handed.path.clone();
        drop(handed);
        assert!(!path.exists());

        // With no runtime directory, or one that does not take it, the note goes into a
        // directory of its own, which goes with it, whatever was left in it.
        let shut = runtime.join("shut");
        fs::create_dir(&shut).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&shut, fs::Permissions::from_mode(0o500)).unwrap();
        }
        // As root a shut directory takes the file all the same, and that case says nothing.
        let shut = fs::write(shut.join("probe"), "").is_err().then_some(shut);
        for runtime in [None, Some(runtime.join("gone")), shut] {
            let handed = Handed::within(runtime, &temp, "n1", "Boiler").unwrap();
            let own = handed.path.parent().unwrap().to_owned();
            assert_eq!(own.parent(), Some(temp.as_path()));
            #[cfg(unix)]
            assert_eq!((mode(&own), mode(&handed.path)), (0o700, 0o600));
            fs::write(own.join(".swp"), "Boiler").unwrap();
            drop(handed);
            assert!(!own.exists());
        }
        fs::remove_dir_all(runtime).unwrap();
        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    fn an_editor_that_cannot_run_is_said_and_the_app_stays() {
        let why = edit_in("no-such-editor-3f9a01c2", "n1", "Boiler").unwrap_err();
        assert_eq!(
            why,
            "could not run no-such-editor-3f9a01c2: No such file or directory"
        );
        assert_eq!(
            edit_in("  ", "n1", "Boiler").unwrap_err(),
            "no editor is set; note unchanged"
        );
        let gave_up = edit_in("false", "n1", "Boiler").unwrap_err();
        assert_eq!(gave_up, "false exited with 1; note unchanged");

        let (mut app, space) = app();
        app.editor = "no-such-editor-3f9a01c2".into();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        let Flow::Editor { id, text } = chord(&mut app, 'o') else {
            panic!("the note under the cursor was not handed over");
        };
        let edited = edit_in(&app.editor, &id, &text);
        app.edited(id, edited).unwrap();
        assert_eq!(
            app.status_of(&app.device.view()).as_deref(),
            Some("could not run no-such-editor-3f9a01c2: No such file or directory")
        );
        assert_eq!(
            field(&app).as_deref(),
            Some("Boiler"),
            "the note is as it was"
        );
        type_in(&mut app, "s");
        assert_eq!(
            field(&app).as_deref(),
            Some("Boilers"),
            "and the app goes on"
        );
        done(&space);
    }

    #[test]
    fn typing_into_a_row_edits_its_head_and_leaves_the_body() {
        let (mut app, space) = app();
        write(&mut app, "House");
        write(&mut app, "Boiler service");
        press(&mut app, KeyCode::Down, false);
        // Written in the same second, either may be first on the list.
        let (picked, other) = (heads(&app)[0].clone(), heads(&app)[1].clone());
        let end = picked.chars().count();
        assert_eq!(
            app.row_cursor, end,
            "the caret lands at the end of the head"
        );
        with_body(&mut app, &format!("{picked}\nCall two firms."));
        assert_eq!(heads(&app)[0], picked);
        assert_eq!(app.row_cursor, end, "and stays there over the editor");
        let log = space.join(format!("{WRITER}.jsonl"));
        let count = || fs::read_to_string(&log).unwrap().lines().count();
        let start = count();

        type_in(&mut app, " quote");
        for _ in 0..6 {
            press(&mut app, KeyCode::Left, false);
        }
        press(&mut app, KeyCode::Backspace, false);
        chord(&mut app, 'a');
        press(&mut app, KeyCode::Delete, false);
        type_in(&mut app, "b");
        let typed = format!("b{} quote\nCall two firms.", &picked[1..end - 1]);
        assert_eq!(field(&app), Some(typed.clone()));
        assert_eq!(app.row_cursor, 1);
        assert_eq!(line(&app), "", "the line is left alone");
        assert_eq!(
            count(),
            start + 9,
            "a version a change, the body kept under each"
        );

        press(&mut app, KeyCode::Down, false);
        assert_eq!(field(&app), Some(other.clone()));
        assert_eq!(
            app.row_cursor,
            other.chars().count(),
            "the caret lands at the end of the next head"
        );
        assert_eq!(app.device.view().status, "");
        assert_eq!(count(), start + 9, "and nothing more on leaving the row");
        let texts: Vec<String> = on_disk(&space)
            .notes()
            .records()
            .map(|r| r.text.clone())
            .collect();
        assert!(texts.contains(&typed), "{texts:?}");
        done(&space);
    }

    #[test]
    fn a_stray_letter_on_a_row_changes_its_head_and_is_written_at_once() {
        let (mut app, space) = app();
        write(&mut app, "House");
        write(&mut app, "Boiler");
        let log = space.join(format!("{WRITER}.jsonl"));
        let count = || fs::read_to_string(&log).unwrap().lines().count();
        press(&mut app, KeyCode::Down, false);
        let stray = format!("{}s", heads(&app)[0]);
        type_in(&mut app, "s");
        assert_eq!(field(&app), Some(stray.clone()));
        assert_eq!(
            app.status_of(&app.device.view()),
            None,
            "a letter says nothing"
        );
        assert_eq!(count(), 3, "and is written");

        press(&mut app, KeyCode::Down, false);
        assert_eq!(app.status_of(&app.device.view()), None);
        assert_eq!(count(), 3, "and down writes nothing more");
        assert_eq!(
            heads(&app)[0],
            stray,
            "the list holds still under the cursor"
        );

        // Walking the list without typing writes nothing.
        press(&mut app, KeyCode::Up, false);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Up, false);
        press(&mut app, KeyCode::Up, false);
        assert!(!on_note(&app));
        assert_eq!(count(), 3);
        done(&space);
    }

    #[test]
    fn tab_goes_into_the_note_under_the_cursor_wherever_the_caret_is() {
        let (mut app, space) = app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Left, false);
        assert_eq!(app.row_cursor, 5);
        press(&mut app, KeyCode::Right, false);
        assert_eq!(app.row_cursor, 6);
        press(&mut app, KeyCode::Right, false);
        assert_eq!(app.row_cursor, 6);
        assert!(
            path(&app).is_empty(),
            "Right is the caret's, and at the end does nothing"
        );

        // Left at the start of the head stays there.
        chord(&mut app, 'a');
        press(&mut app, KeyCode::Left, false);
        assert_eq!(app.row_cursor, 0);
        assert!(on_note(&app) && path(&app).is_empty());

        press(&mut app, KeyCode::Tab, false);
        assert_eq!(path(&app), ["Boiler"]);
        done(&space);
    }

    #[test]
    fn shift_tab_does_nothing() {
        let (mut app, space) = app();
        write(&mut app, "Boiler #house");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::BackTab, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("#house", 6));
        assert!(path(&app).is_empty());
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::BackTab, false);
        assert_eq!(field(&app).as_deref(), Some("Boiler #house"));
        assert!(path(&app).is_empty());
        done(&space);
    }

    #[test]
    fn a_tab_is_never_typed_into_a_head() {
        let (mut app, space) = app();
        write(&mut app, "House");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(field(&app).as_deref(), Some(""));
        press(&mut app, KeyCode::Tab, false);
        assert_eq!((field(&app).as_deref(), app.row_cursor), (Some(""), 0));
        assert!(path(&app).is_empty());
        done(&space);
    }

    /// A head too long for a row forty columns wide, where the head has twenty-six beside the
    /// date.
    const LONG: &str = "the boiler pressure keeps dropping after the radiators are bled";

    /// The head of a painted row, the columns from its second to the date, without the blank
    /// column the cursor stands in after it.
    fn head_of(row: &str) -> String {
        let head: String = row.chars().skip(1).take(26).collect();
        head.trim_end().to_owned()
    }

    #[test]
    fn a_long_head_landed_on_reads_from_its_start_with_the_cursor_on_its_cut() {
        let (mut app, space) = app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        let shot = screen(&mut app, 40, 8);
        let date = time::format_day(app.device.view().field.unwrap().created.unwrap().day());
        assert!(shot.rows[1].ends_with(&date), "{:?}", shot.rows[1]);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps…");
        assert_eq!(shot.cursor, (26, 1), "on the `…`, where the rest is");
        assert_eq!(
            app.row_cursor,
            letters(LONG),
            "the caret is at the end all the same"
        );

        // A letter typed goes at the end, and the head scrolls to show it there.
        type_in(&mut app, "!");
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "… the radiators are bled!");
        assert_eq!(shot.cursor, (26, 1));
        assert!(shot.rows[1].ends_with(&date), "{:?}", shot.rows[1]);
        done(&space);
    }

    #[test]
    fn a_key_in_a_long_head_scrolls_it_keeping_five_columns_and_marking_each_hidden_edge() {
        let (mut app, space) = app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::End, false);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        assert_eq!(shot.cursor, (26, 1), "in the column after the head");

        // Walking back, the text stays until the caret is five columns from the left edge's
        // `…`, and then moves a column a key, both edges hiding text.
        for _ in 0..19 {
            press(&mut app, KeyCode::Left, false);
            screen(&mut app, 40, 8);
        }
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        assert_eq!(shot.cursor, (7, 1), "five columns after the `…`");
        for _ in 0..11 {
            press(&mut app, KeyCode::Left, false);
            screen(&mut app, 40, 8);
        }
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…opping after the radiato…");
        assert_eq!(shot.cursor, (7, 1));

        // Right walks the caret back across what is shown, and moves the text only as it comes
        // within five columns of the right edge's `…`.
        for _ in 0..13 {
            press(&mut app, KeyCode::Right, false);
            screen(&mut app, 40, 8);
        }
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…opping after the radiato…");
        assert_eq!(
            shot.cursor,
            (20, 1),
            "five letters before the `…` after the caret's"
        );
        press(&mut app, KeyCode::Right, false);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…pping after the radiator…");
        assert_eq!(shot.cursor, (20, 1));

        // Home shows the start, the end hidden.
        press(&mut app, KeyCode::Home, false);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps…");
        assert_eq!(shot.cursor, (1, 1));
        done(&space);
    }

    #[test]
    fn moving_the_caret_within_what_is_shown_moves_no_text() {
        let (mut app, space) = app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Home, false);
        let at_home = screen(&mut app, 40, 8).rows[1].clone();
        for x in 2..20 {
            press(&mut app, KeyCode::Right, false);
            let shot = screen(&mut app, 40, 8);
            assert_eq!(shot.rows[1], at_home);
            assert_eq!(shot.cursor, (x, 1));
        }
        done(&space);
    }

    #[test]
    fn the_line_moves_its_text_only_when_its_caret_nears_an_edge() {
        let (mut app, space) = app();
        let text = "the garden shed roof wants felting before the rain";
        type_in(&mut app, text);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(shot.rows[0], ">…d roof wants felting before the rain");
        assert_eq!(shot.cursor, (38, 0));
        for x in (28..38).rev() {
            press(&mut app, KeyCode::Left, false);
            let shot = screen(&mut app, 40, 8);
            assert_eq!(shot.rows[0], ">…d roof wants felting before the rain");
            assert_eq!(shot.cursor, (x, 0));
        }
        // Five columns from the left edge's `…` it moves, a column a key, the caret staying.
        for x in (7..28).rev() {
            press(&mut app, KeyCode::Left, false);
            assert_eq!(screen(&mut app, 40, 8).cursor, (x, 0));
        }
        press(&mut app, KeyCode::Left, false);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(shot.rows[0], ">…ed roof wants felting before the rain");
        assert_eq!(shot.cursor, (7, 0));
        done(&space);
    }

    #[test]
    fn the_row_under_the_cursor_is_one_row_however_long_its_head_or_its_reasons() {
        let mut row = a_row("Bleed the radiators");
        row.links = vec!["Boiler".into(), "House".into()];
        row.reasons = vec![
            "#house".into(),
            "linked to the boiler service in spring".into(),
        ];
        let created = Moment::utc(0);
        let date = time::format_day(created.day());
        let head = (letters(&row.head), Some(0));
        let Selected { line, x, cut, .. } =
            selected_row(&row, &row.head, Some(created), head, false, 40);
        let painted = line.to_string();
        assert_eq!(
            painted,
            format!(" Bleed the radiators  → Bo…  {date}"),
            "the tail cut where the head leaves off, the date at the right"
        );
        assert_eq!((x, cut), (20, false));

        let mut row = a_row(LONG);
        row.reasons = vec!["#house".into()];
        let head = (letters(&row.head), None);
        let Selected { line, cut, .. } =
            selected_row(&row, &row.head, Some(created), head, false, 40);
        assert_eq!(
            line.to_string(),
            format!(" the boiler pressure keeps…  {date}"),
            "no room is left for the reasons by a head that is cut"
        );
        assert!(cut);

        // On the screen the row after it is the next row down.
        let (mut app, space) = app();
        write(&mut app, "House");
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        let shot = screen(&mut app, 40, 10);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps…");
        assert!(shot.rows[2].starts_with(" House"), "{:?}", shot.rows);
        done(&space);
    }

    #[test]
    fn a_note_with_a_body_shows_its_mark_before_the_date_on_the_cursor() {
        let mut row = a_row(LONG);
        row.has_body = true;
        let created = Moment::utc(0);
        let date = time::format_day(created.day());
        let head = (letters(&row.head), None);
        let Selected { line, x, .. } =
            selected_row(&row, &row.head, Some(created), head, false, 40);
        assert_eq!(
            line.to_string(),
            format!(" the boiler pressure kee…  ¶ {date}")
        );
        assert_eq!(x, 24, "the cut two columns earlier, for the mark");
    }

    #[test]
    fn the_foot_reads_a_cut_head_whole_above_the_body() {
        let (mut app, space) = app();
        write(&mut app, "House");
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        with_body(&mut app, &format!("{LONG}\n\nThe combi under the stairs."));
        let shot = screen(&mut app, 40, 12);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure kee…");
        assert_eq!(
            shot.rows[7..10],
            [
                "   the boiler pressure keeps dropping",
                "   after the radiators are bled",
                "   The combi under the stairs.",
            ],
            "the head, and the body without the blank line between"
        );

        // A head its row does not cut is not read twice.
        press(&mut app, KeyCode::Down, false);
        with_body(&mut app, "House\nThe shed key is on the hook.");
        let shot = screen(&mut app, 40, 12);
        assert!(shot.rows[2].starts_with(" House"), "{:?}", shot.rows);
        assert_eq!(shot.rows[7], "   The shed key is on the hook.");
        assert_eq!(shot.rows[8], "");
        done(&space);
    }

    #[test]
    fn a_head_left_and_landed_on_again_reads_from_its_start() {
        let (mut app, space) = app();
        write(&mut app, "House");
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::End, false);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        press(&mut app, KeyCode::Down, false);
        screen(&mut app, 40, 8);
        press(&mut app, KeyCode::Up, false);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps…");
        assert_eq!(shot.cursor, (26, 1));
        done(&space);
    }

    #[test]
    fn with_vim_a_long_head_reads_from_its_start_until_a_motion_and_stays_written_after_escape() {
        let (mut app, space) = vim_app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        assert_eq!(mode(&app), Mode::Normal);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps…");
        assert_eq!(shot.cursor, (26, 1), "the block on the `…`");

        // A motion scrolls the row with its caret, even where the caret cannot move.
        type_in(&mut app, "l");
        assert_eq!(mode(&app), Mode::Normal);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        assert_eq!(shot.cursor, (26, 1), "after the last letter");
        type_in(&mut app, "b");
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        assert_eq!(shot.cursor, (22, 1), "on the b of bled");
        type_in(&mut app, "0");
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps…");
        assert_eq!(shot.cursor, (1, 1));

        // Leaving the row and coming back reads it from its start again.
        press(&mut app, KeyCode::Up, false);
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "A");
        assert_eq!(mode(&app), Mode::Insert);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        assert_eq!(shot.cursor, (26, 1));

        // Escape is normal mode, with the caret a letter back, and the row stays as written.
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(mode(&app), Mode::Normal);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        assert_eq!(shot.cursor, (25, 1), "on the d of bled");

        type_in(&mut app, "I");
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps…");
        assert_eq!(shot.cursor, (1, 1), "at the start");
        done(&space);
    }

    #[test]
    fn with_vim_the_block_sits_on_the_letter_at_the_caret_two_columns_for_a_wide_one() {
        let (mut app, space) = vim_app();
        write(&mut app, "火车站 Boiler");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, "0l");
        assert_eq!(app.row_cursor, 1);
        assert_eq!(
            screen(&mut app, 40, 8).cursor,
            (3, 1),
            "on 车, after 火's two columns"
        );
        type_in(&mut app, "w");
        assert_eq!(app.row_cursor, 4);
        assert_eq!(screen(&mut app, 40, 8).cursor, (8, 1), "on the B");
        type_in(&mut app, "h");
        assert_eq!(screen(&mut app, 40, 8).cursor, (7, 1), "on the blank");
        type_in(&mut app, "$");
        assert_eq!(
            screen(&mut app, 40, 8).cursor,
            (14, 1),
            "after the last letter"
        );
        done(&space);
    }

    /// Says whether the painted row under the cursor fills its room with the end of its head:
    /// `…` and its last twenty-four letters, the caret in the column after them.
    fn full(app: &App, shot: &Screen) -> bool {
        let head = field(app).unwrap();
        let end: String = head.chars().skip(head.chars().count() - 24).collect();
        let painted: String = shot.rows[1].chars().skip(1).take(25).collect();
        painted == format!("…{end}") && shot.cursor == (26, 1)
    }

    #[test]
    fn deleting_at_the_end_of_a_long_head_keeps_the_field_full() {
        let (mut app, space) = app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::End, false);
        screen(&mut app, 40, 8);
        for n in 1..=30 {
            press(&mut app, KeyCode::Backspace, false);
            let shot = screen(&mut app, 40, 8);
            assert!(
                full(&app, &shot),
                "after {n}: {:?} {:?}",
                shot.rows[1],
                shot.cursor
            );
        }
        assert_eq!(
            head_of(&screen(&mut app, 40, 8).rows[1]),
            "…r pressure keeps droppin"
        );

        // Backspace on to where the head fits brings it back whole, from its start.
        for _ in 0..8 {
            press(&mut app, KeyCode::Backspace, false);
            screen(&mut app, 40, 8);
        }
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "the boiler pressure keeps");
        assert_eq!(shot.cursor, (26, 1));
        done(&space);
    }

    #[test]
    fn ctrl_w_and_delete_in_a_scrolled_head_keep_the_field_full() {
        let (mut app, space) = app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::End, false);
        screen(&mut app, 40, 8);
        for n in 1..=3 {
            chord(&mut app, 'w');
            let shot = screen(&mut app, 40, 8);
            assert!(
                full(&app, &shot),
                "after {n}: {:?} {:?}",
                shot.rows[1],
                shot.cursor
            );
        }
        assert_eq!(
            head_of(&screen(&mut app, 40, 8).rows[1]),
            "…eeps dropping after the"
        );

        // Delete takes what is after the caret, and what was hidden before comes back.
        for _ in 0..6 {
            press(&mut app, KeyCode::Left, false);
            screen(&mut app, 40, 8);
        }
        for _ in 0..6 {
            press(&mut app, KeyCode::Delete, false);
            screen(&mut app, 40, 8);
        }
        let shot = screen(&mut app, 40, 8);
        assert!(full(&app, &shot), "{:?} {:?}", shot.rows[1], shot.cursor);
        assert_eq!(head_of(&shot.rows[1]), "…sure keeps dropping afte");
        done(&space);
    }

    #[test]
    fn a_head_put_back_while_scrolled_reads_whole_again() {
        let (mut app, space) = app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, &format!(" {LONG}"));
        let shot = screen(&mut app, 40, 8);
        assert!(full(&app, &shot), "{:?} {:?}", shot.rows[1], shot.cursor);
        chord(&mut app, 'u');
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), "Boiler");
        assert_eq!(
            shot.cursor,
            (7, 1),
            "the caret kept as far as the head goes"
        );
        done(&space);
    }

    #[test]
    fn a_paste_into_a_head_landed_on_scrolls_it_to_the_caret() {
        let (mut app, space) = app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        app.paste(&format!(" {LONG}")).unwrap();
        let shot = screen(&mut app, 40, 8);
        assert!(full(&app, &shot), "{:?} {:?}", shot.rows[1], shot.cursor);
        assert_eq!(head_of(&shot.rows[1]), "…r the radiators are bled");
        done(&space);
    }

    #[test]
    fn a_head_exactly_as_wide_as_its_room_is_never_cut() {
        let (mut app, space) = app();
        let letters26 = "abcdefghijklmnopqrstuvwxyz";
        write(&mut app, letters26);
        press(&mut app, KeyCode::Down, false);
        let shot = screen(&mut app, 40, 8);
        assert_eq!(head_of(&shot.rows[1]), letters26);
        assert_eq!(shot.cursor, (27, 1), "in the gap after the head");
        assert!(
            shot.rows[3..7].iter().all(String::is_empty),
            "nothing at the foot"
        );
        for (code, x) in [
            (KeyCode::End, 27),
            (KeyCode::Left, 26),
            (KeyCode::Right, 27),
        ] {
            press(&mut app, code, false);
            let shot = screen(&mut app, 40, 8);
            assert_eq!(head_of(&shot.rows[1]), letters26, "{code:?}");
            assert_eq!(shot.cursor, (x, 1), "{code:?}");
            assert!(shot.rows[3..7].iter().all(String::is_empty), "{code:?}");
        }
        done(&space);
    }

    #[test]
    fn on_a_screen_too_narrow_for_the_date_the_row_under_the_cursor_goes_without_it() {
        let (mut app, space) = app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        let shot = screen(&mut app, 16, 8);
        assert_eq!(shot.rows[1], " the boiler pr…");
        assert_eq!(shot.cursor, (14, 1), "on the `…`");
        press(&mut app, KeyCode::End, false);
        let shot = screen(&mut app, 16, 8);
        assert_eq!(shot.rows[1], " …ors are bled");
        assert_eq!(shot.cursor, (14, 1));
        done(&space);
    }

    #[test]
    fn with_a_status_row_the_foot_reads_a_cut_head_in_the_rows_left() {
        let (mut app, space) = app();
        write(&mut app, LONG);
        press(&mut app, KeyCode::Down, false);
        with_body(&mut app, &format!("{LONG}\nThe combi under the stairs."));
        let shot = screen(&mut app, 40, 11);
        assert_eq!(
            shot.rows[7..10],
            ["   the boiler pressure keeps dropping", "   …", " saved"],
            "{:?}",
            shot.rows
        );
        done(&space);
    }

    #[test]
    fn enter_with_text_on_the_line_puts_the_cursor_at_the_end_of_the_note_it_wrote() {
        let (mut app, space) = app();
        type_in(&mut app, "House");
        press(&mut app, KeyCode::Enter, false);
        assert!(on_note(&app));
        assert_eq!(field(&app).as_deref(), Some("House"));
        assert_eq!(app.row_cursor, 5, "the caret at the end of its head");
        assert_eq!(line(&app), "");
        let shot = screen(&mut app, 40, 8);
        assert_eq!(shot.cursor, (6, 1), "the terminal's cursor in the row");

        // Enter there opens a row under it.
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(heads(&app), ["House", ""]);
        assert_eq!(app.device.view().cursor, Some(1));
        done(&space);
    }

    #[test]
    fn enter_on_an_empty_line_and_typing_makes_a_note() {
        let (mut app, space) = app();
        write(&mut app, "House");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(field(&app).as_deref(), Some(""));
        assert_eq!(app.row_cursor, 0);
        press(&mut app, KeyCode::Up, false);
        assert_eq!(
            heads(&app),
            ["House"],
            "a new row left empty was never anything"
        );

        press(&mut app, KeyCode::Enter, false);
        type_in(&mut app, "a garden");
        assert_eq!(line(&app), "", "typing went to the row");
        press(&mut app, KeyCode::Up, false);
        let mut seen = heads(&app);
        seen.sort();
        assert_eq!(seen, ["House", "a garden"]);
        assert!(
            !on_note(&app),
            "and the cursor is back on the line for the next"
        );
        assert_eq!(
            app.status_of(&app.device.view()),
            None,
            "kept as it was typed, so nothing is said"
        );
        let written = fs::read_to_string(space.join(format!("{WRITER}.jsonl"))).unwrap();
        assert_eq!(written.lines().count(), 1 + 8, "a line a letter");

        // Ctrl-O on a new note hands it to the editor as it is so far.
        press(&mut app, KeyCode::Enter, false);
        let Flow::Editor { text, .. } = chord(&mut app, 'o') else {
            panic!("the new note was not handed over");
        };
        assert_eq!(text, "");
        done(&space);
    }

    #[test]
    fn enter_on_a_row_opens_a_new_row_under_it_and_what_is_typed_there_is_kept() {
        let (mut app, space) = app();
        write(&mut app, "House");
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(heads(&app), ["House", ""]);
        assert_eq!(app.device.view().cursor, Some(1));
        assert_eq!((field(&app).as_deref(), app.row_cursor), (Some(""), 0));
        let shot = screen(&mut app, 60, 10);
        assert_eq!(shot.rows[2], "");
        assert_eq!(
            shot.cursor,
            (1, 2),
            "the cursor in the empty head under the row"
        );

        // What is typed is kept as it is typed, Enter opens another row, and Escape leaves the
        // last.
        type_in(&mut app, "Boiler");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(heads(&app), ["House", "Boiler", ""]);
        type_in(&mut app, "a garden");
        press(&mut app, KeyCode::Esc, false);
        assert!(!on_note(&app));
        assert_eq!(line(&app), "", "the line is left alone");
        let mut texts: Vec<String> = on_disk(&space)
            .notes()
            .records()
            .map(|r| r.text.clone())
            .collect();
        texts.sort();
        assert_eq!(texts, ["Boiler", "House", "a garden"]);
        done(&space);
    }

    #[test]
    fn control_and_an_arrow_on_a_row_moves_its_note_and_the_cursor_with_it() {
        let (mut app, space) = app();
        for text in ["Boiler", "House", "a garden"] {
            write(&mut app, text);
        }
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Down, true);
        assert_eq!(heads(&app), ["House", "a garden", "Boiler"]);
        assert_eq!(
            field(&app).as_deref(),
            Some("a garden"),
            "the cursor is still on it"
        );
        press(&mut app, KeyCode::Up, true);
        assert_eq!(heads(&app), ["a garden", "House", "Boiler"]);
        assert_eq!(app.device.view().cursor, Some(0));
        // On the line the arrows are the cursor's, held or not.
        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Down, true);
        assert_eq!(app.device.view().cursor, Some(0));
        done(&space);
    }

    #[test]
    fn backspace_on_an_empty_row_takes_it_away_and_goes_up() {
        let (mut app, space) = app();
        write(&mut app, "House");
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        let first = heads(&app)[0].clone();
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(heads(&app).len(), 3);
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(heads(&app).len(), 2);
        assert_eq!(app.device.view().cursor, Some(0));
        assert_eq!(field(&app), Some(first));
        done(&space);
    }

    #[test]
    fn backspace_at_the_start_of_a_head_with_text_leaves_it() {
        let (mut app, space) = app();
        write(&mut app, "House");
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Home, false);
        assert_eq!(app.row_cursor, 0);
        press(&mut app, KeyCode::Backspace, false);
        assert_eq!(heads(&app), ["House"]);
        assert_eq!(field(&app).as_deref(), Some("House"));
        assert_eq!(app.device.view().cursor, Some(0));
        done(&space);
    }

    #[test]
    fn a_head_typed_into_is_on_disk_when_the_app_quits() {
        let (mut app, space) = app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        type_in(&mut app, " pressure");
        assert!(matches!(chord(&mut app, 'q'), Flow::Quit));
        let texts: Vec<String> = on_disk(&space)
            .notes()
            .records()
            .map(|r| r.text.clone())
            .collect();
        assert_eq!(texts, ["Boiler pressure"]);
        done(&space);
    }

    #[test]
    fn the_cursors_row_has_the_cursor_in_its_head_its_head_bold_and_its_body_at_the_foot() {
        let (mut app, space) = app();
        write(&mut app, "House");
        write(&mut app, "Boiler");
        let shot = screen(&mut app, 60, 10);
        assert_eq!(shot.rows[0], ">", "the line, empty after its mark");
        assert_eq!(shot.cursor, (1, 0), "where the rows' text starts");
        assert!(
            shot.rows[1..9].iter().filter(|r| !r.is_empty()).count() == 2,
            "on the line nothing is kept for a body: {:?}",
            shot.rows
        );

        press(&mut app, KeyCode::Down, false);
        let picked = heads(&app)[0].clone();
        with_body(&mut app, &format!("{picked}\nThe combi under the stairs."));
        // Written last, the note with a body is first on the list, and the other under it.
        assert_eq!(heads(&app)[0], picked);
        let other = heads(&app)[1].clone();
        let created = app.device.view().field.unwrap().created.unwrap();
        let date = format!(" {}", time::format_day(created.day()));
        let shot = screen(&mut app, 60, 10);
        assert!(
            shot.rows[1].starts_with(&format!(" {picked}  "))
                && shot.rows[1].ends_with(&format!(" ¶{date}")),
            "{:?}",
            shot.rows[1]
        );
        assert!(shot.bold[1] && !shot.dim[1]);
        assert_eq!(shot.rows[2], format!(" {other}"), "nothing under the row");
        assert!(!shot.dim[2] && !shot.bold[2], "no other row is dimmed");
        let x = u16::try_from(1 + picked.len()).unwrap();
        assert_eq!(
            shot.cursor,
            (x, 1),
            "the terminal's own cursor, at the end of the head, right under the line"
        );
        // The body is at the foot of the list, set in and dimmed under a row of nothing, in the
        // three rows that leave the list half of its eight, the last of them the status's.
        assert_eq!(shot.rows[3..6], ["", "", ""]);
        assert_eq!(shot.rows[6], "   The combi under the stairs.");
        assert!(shot.dim[6]);
        assert_eq!(shot.rows[8], " saved", "what the last thing asked for did");
        assert!(
            shot.rows[9].starts_with(" Enter new note  ^O editor  Tab go in  ^V archive"),
            "{:?}",
            shot.rows[9]
        );
        assert!(shot.dim[9]);

        // A key that says nothing leaves no status row, and the keys stay at the foot.
        press(&mut app, KeyCode::Left, false);
        let shot = screen(&mut app, 60, 10);
        assert_eq!(shot.rows[8], "");
        assert_eq!(shot.cursor, (x - 1, 1), "one back from the end of the head");
        assert_eq!(shot.rows[6], "   The combi under the stairs.");

        // A note with no body keeps the room, and shows nothing in it.
        press(&mut app, KeyCode::Down, false);
        let shot = screen(&mut app, 60, 10);
        assert!(shot.rows[2].starts_with(&format!(" {other}")) && shot.bold[2]);
        assert!(
            shot.rows[3..9].iter().all(String::is_empty),
            "{:?}",
            shot.rows
        );

        // A new row opens at the top, empty, with the cursor in it.
        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Enter, false);
        let shot = screen(&mut app, 60, 10);
        assert_eq!(shot.rows[1], "");
        assert_eq!(shot.cursor, (1, 1));
        assert_eq!(shot.rows[2], format!(" {picked} ¶"));
        assert!(!shot.dim[2]);
        assert!(
            shot.rows[9].starts_with(" ^O editor  Esc up"),
            "{:?}",
            shot.rows[9]
        );
        done(&space);
    }

    #[test]
    fn the_list_moves_only_as_far_as_it_must_to_keep_the_cursor_in_sight() {
        let (mut app, space) = app();
        for n in 0..12 {
            write(&mut app, &format!("garden bed {n}"));
        }
        let listed = heads(&app);
        // The head a painted row shows, without the date the selected one has at its end.
        let head = |row: &str| row.trim().split("  ").next().unwrap().to_owned();
        // From the second row, as many rows of list as there are: seven from the line, where the
        // line and the footer take the rest, and four from a row, whose note's body takes three.
        let shown = |app: &mut App, rows: usize| -> Vec<String> {
            screen(app, 40, 9).rows[1..1 + rows]
                .iter()
                .map(|r| head(r))
                .collect()
        };
        assert_eq!(shown(&mut app, 7), listed[..7]);
        press(&mut app, KeyCode::Down, false);
        assert_eq!(shown(&mut app, 4), listed[..4]);
        assert_eq!(screen(&mut app, 40, 9).rows[5..8], ["", "", ""]);
        for _ in 0..4 {
            press(&mut app, KeyCode::Down, false);
        }
        assert_eq!(shown(&mut app, 4), listed[1..5]);
        let x = u16::try_from(1 + listed[4].len()).unwrap();
        assert_eq!(
            screen(&mut app, 40, 9).cursor,
            (x, 4),
            "the cursor's row is the last in sight"
        );
        press(&mut app, KeyCode::Up, false);
        assert_eq!(
            shown(&mut app, 4),
            listed[1..5],
            "moving back up within sight moves nothing"
        );
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            shown(&mut app, 7),
            listed[..7],
            "and from the line, the top, with the whole height"
        );
        done(&space);
    }

    #[test]
    fn with_no_relay_the_app_looks_four_times_a_second_however_fast_the_keys_come() {
        let (mut app, space) = app();
        app.looked = Instant::now() - Duration::from_millis(150);
        assert!(app.wait() <= Duration::from_millis(100), "{:?}", app.wait());
        app.looked = Instant::now() - LOOK;
        assert_eq!(app.wait(), Duration::ZERO, "it is time to look");
        let _link = link(&mut app);
        assert_eq!(
            app.wait(),
            TICK,
            "with a relay it listens a twentieth of a second at a time"
        );
        done(&space);
    }

    #[test]
    fn a_long_list_below_the_cursor_leaves_the_screen_as_a_short_one_does() {
        let (mut few, space) = app();
        for n in 0..12 {
            write(&mut few, &format!("garden bed {n}"));
        }
        for _ in 0..7 {
            press(&mut few, KeyCode::Down, false);
        }
        let short = screen(&mut few, 40, 9);
        drop(few);
        // The same notes, and three hundred more written before them, below them on the list.
        let (mut app, other) = app();
        for n in 0..300 {
            write(&mut app, &format!("compost turn {n}"));
        }
        for n in 0..12 {
            write(&mut app, &format!("garden bed {n}"));
        }
        for _ in 0..7 {
            press(&mut app, KeyCode::Down, false);
        }
        let long = screen(&mut app, 40, 9);
        assert_eq!(long.rows, short.rows);
        assert_eq!(long.dim, short.dim);
        assert_eq!(long.bold, short.bold);
        assert_eq!(long.cursor, short.cursor);
        done(&space);
        done(&other);
    }

    #[test]
    fn a_body_is_broken_between_words() {
        assert_eq!(
            wrap("the combi under the stairs", 10),
            ["the combi", "under the", "stairs"]
        );
        assert_eq!(wrap("one\n\ntwo", 10), ["one", "", "two"]);
        assert_eq!(wrap("abcdefghij", 4), ["abcd", "efgh", "ij"]);
        assert_eq!(
            head_and_body("Boiler\n\nThe combi.\n"),
            ("Boiler", "The combi.")
        );
        let long = (0..9)
            .map(|n| format!("line {n}"))
            .collect::<Vec<_>>()
            .join("\n");
        let rows = body_rows(&long, 40, 5);
        assert_eq!(rows.len(), 5, "cut at its height");
        assert_eq!(rows[3].to_string(), "   line 3");
        assert_eq!(rows[4].to_string(), "   …", "with a sign that it goes on");
        assert_eq!(body_rows(&long, 40, 9).len(), 9, "and whole when it fits");
        assert!(body_rows("", 40, 5).is_empty());
    }

    #[test]
    fn a_wide_letter_takes_the_two_columns_it_is_drawn_in() {
        assert_eq!(wrap("浴室的瓷砖", 6), ["浴室的", "瓷砖"]);
        assert_eq!(wrap("瓷砖 tiles", 5), ["瓷砖", "tiles"]);
        assert_eq!(wrap("浴室", 1), ["浴", "室"], "even where it cannot fit");
    }

    #[test]
    fn in_a_place_the_foot_reads_the_place_from_the_line_and_the_note_from_a_row() {
        let (mut app, space) = app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        with_body(&mut app, "Boiler\nThe combi under the stairs.");
        press(&mut app, KeyCode::Esc, false);
        type_in(&mut app, "Boiler");
        press(&mut app, KeyCode::Tab, false);
        for n in 0..6 {
            write(&mut app, &format!("valve {n}"));
        }
        assert_eq!(path(&app), ["Boiler"]);
        // Under the path and the line, five rows of list, a row of nothing, and three of body.
        let listed = |shot: &Screen| shot.rows[2..8].iter().filter(|r| !r.is_empty()).count();
        let shot = screen(&mut app, 40, 12);
        assert_eq!(shot.rows[1], ">");
        assert!(
            shot.rows[2].starts_with(" valve"),
            "nothing between the line and the list: {:?}",
            shot.rows
        );
        assert_eq!(listed(&shot), 5);
        assert_eq!(
            shot.rows[8], "   The combi under the stairs.",
            "the place's"
        );
        assert!(shot.dim[8]);

        press(&mut app, KeyCode::Down, false);
        let shot = screen(&mut app, 40, 12);
        assert_eq!(listed(&shot), 5, "the list keeps its height onto a row");
        assert!(
            shot.rows[8..11].iter().all(String::is_empty),
            "where the row's note, with no body, is read: {:?}",
            shot.rows
        );

        let picked = heads(&app)[0].clone();
        with_body(&mut app, &format!("{picked}\nA quarter turn."));
        let shot = screen(&mut app, 40, 12);
        assert_eq!(shot.rows[10], " saved");
        assert_eq!(listed(&shot), 5, "a status takes a row of the body's");
        assert_eq!(shot.rows[8], "   A quarter turn.");
        press(&mut app, KeyCode::Left, false);
        let shot = screen(&mut app, 40, 12);
        assert_eq!(shot.rows[10], "");
        assert_eq!(listed(&shot), 5, "and gives it back");
        assert_eq!(shot.rows[8], "   A quarter turn.");

        // At the top, on the line, nothing is kept and the list has the whole height.
        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Esc, false);
        assert!(path(&app).is_empty());
        let shot = screen(&mut app, 40, 12);
        assert_eq!(
            shot.rows[1..11].iter().filter(|r| !r.is_empty()).count(),
            7,
            "{:?}",
            shot.rows
        );
        assert!(!shot.rows.iter().any(|r| r.contains("combi")));
        done(&space);
    }

    #[test]
    fn the_body_under_the_cursor_is_given_the_foot_of_the_list_while_it_leaves_the_list_half() {
        assert_eq!(preview_rows(28), PREVIEW_ROWS);
        assert_eq!(preview_rows(10), 4);
        assert_eq!(preview_rows(6), 2);
        assert_eq!(preview_rows(5), 0, "too short for it, and it goes");
    }

    #[test]
    fn the_status_says_what_the_last_key_did_and_trouble_until_it_is_put_right() {
        let (mut app, space) = app();
        assert!(
            app.status_of(&app.device.view())
                .unwrap()
                .starts_with("no notes yet")
        );
        type_in(&mut app, "x");
        assert_eq!(
            app.status_of(&app.device.view()),
            None,
            "the first key clears what opening said"
        );
        chord(&mut app, 'd');
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            app.status_of(&app.device.view()).as_deref(),
            Some("Escape again to quit")
        );
        press(&mut app, KeyCode::Left, false);
        assert_eq!(
            app.status_of(&app.device.view()),
            None,
            "and so does every key after"
        );

        assert!(
            !app.hear().unwrap(),
            "with no relay there is nothing to hear"
        );
        let (_sent, tell) = link(&mut app);
        assert!(!app.hear().unwrap(), "nor until something is said");

        tell.send(sync::Event::Connected).unwrap();
        assert!(app.hear().unwrap());
        assert!(!app.not_syncing, "nothing is said while sync works");
        let foot = |app: &mut App| screen(app, 80, 12).rows.pop().unwrap();
        assert!(!foot(&mut app).contains("clock"), "{}", foot(&mut app));
        let behind = Message::Now(crate::system::now().plus_secs(600))
            .encode()
            .unwrap();
        tell.send(sync::Event::Message(behind)).unwrap();
        assert!(app.hear().unwrap());
        assert!(
            foot(&mut app).ends_with("this device's clock is 10 min behind"),
            "{}",
            foot(&mut app)
        );
        tell.send(sync::Event::Disconnected).unwrap();
        tell.send(sync::Event::Unreachable("connection refused".into()))
            .unwrap();
        assert!(app.hear().unwrap());
        assert!(app.not_syncing);
        press(&mut app, KeyCode::Left, false);
        assert_eq!(
            app.status_of(&app.device.view()).as_deref(),
            Some("nas.local: connection refused"),
            "why is said until the relay is back"
        );
        tell.send(sync::Event::Connected).unwrap();
        tell.send(sync::Event::Message(r#"{"forked":"dev-b"}"#.into()))
            .unwrap();
        assert!(app.hear().unwrap());
        assert!(!app.not_syncing, "every other log goes on");
        assert!(
            app.device
                .view()
                .status
                .starts_with("the log dev-b here is not the dev-b elsewhere"),
            "{}",
            app.device.view().status
        );

        // A fork of this device's own log is put right by a new log, and the new writer id is
        // kept with the space.
        tell.send(sync::Event::Message(format!(r#"{{"forked":"{WRITER}"}}"#)))
            .unwrap();
        assert!(app.hear().unwrap());
        let now = app.device.device().to_owned();
        assert_ne!(now, WRITER, "{}", app.device.view().status);
        assert_eq!(
            fs::read_to_string(space.join(".writer")).unwrap().trim(),
            now
        );
        assert!(
            app.device.view().status.starts_with("compacted"),
            "{}",
            app.device.view().status
        );
        done(&space);
    }

    #[test]
    fn a_refusal_is_said_by_the_relays_host_and_not_its_token_until_a_relay_takes_the_device_in() {
        let (mut app, space) = app();
        let token = "3f9a01c23f9a01c23f9a01c23f9a01c2";
        let dial = sync::dial_of(&format!("ws://joe:{token}@nas.local:4000")).unwrap();
        let (sent, tell) = dialler_shown(&mut app, &dial.shown).try_recv().unwrap();
        tell.send(sync::Event::Connected).unwrap();
        let refused = r#"{"refused":"joe needs the relay's token to make a space"}"#;
        tell.send(sync::Event::Message(refused.into())).unwrap();
        tell.send(sync::Event::Disconnected).unwrap();
        assert!(app.hear().unwrap());
        press(&mut app, KeyCode::Left, false);
        let said = "nas.local:4000: joe needs the relay's token to make a space";
        assert_eq!(app.status_of(&app.device.view()).as_deref(), Some(said));
        let painted = screen(&mut app, 80, 8);
        assert!(
            painted.rows.iter().any(|row| row.contains(said)),
            "{:?}",
            painted.rows
        );
        assert!(painted.rows.iter().all(|row| !row.contains(token)));

        // A relay that takes the device in puts it right.
        let mut relay = Replica::new();
        connect(&mut app, &mut relay, &sent, &tell);
        press(&mut app, KeyCode::Left, false);
        assert_eq!(app.status_of(&app.device.view()), None);
        done(&space);
    }

    #[test]
    fn what_the_line_stands_for_is_shown_after_an_arrow() {
        assert_eq!(dates_shown(&[]), "");
        assert_eq!(
            dates_shown(&[
                "Fri 2026-10-16".to_owned(),
                "Mon 2026-10-12 18:00 – now".to_owned()
            ]),
            "→ Fri 2026-10-16, Mon 2026-10-12 18:00 – now"
        );
    }

    #[test]
    fn the_path_starts_with_the_space_and_fits() {
        assert_eq!(breadcrumb("main", &[], 70), " main ");
        assert_eq!(
            breadcrumb("main", &["trainingapp", "Profile"], 70),
            " main › trainingapp › Profile "
        );
        let long = "a very long head that goes on and on about the training plan for the year";
        let crumb = breadcrumb("garden", &["trainingapp", long], 70);
        assert_eq!(crumb.chars().count(), 70);
        assert!(
            crumb.starts_with(" garden › trainingapp › a very long") && crumb.ends_with("… "),
            "a short head keeps its length and the long one takes what is left: {crumb}"
        );
        assert_eq!(
            breadcrumb("main", &["one", "two", "three", "four", "five"], 70),
            " main › one › … › four › five "
        );
        let crumb = breadcrumb("a-garden-space-with-a-long-name", &["Boiler"], 24);
        assert_eq!(
            crumb, " a-garden-spa… › Boiler ",
            "the place keeps its room"
        );
    }

    #[test]
    fn in_a_place_the_path_row_is_headed_by_the_space() {
        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        assert_eq!(screen(&mut app, 40, 6).rows[0], ">", "no path at the top");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Tab, false);
        let shot = screen(&mut app, 40, 6);
        assert_eq!(shot.rows[0], " main › #house");
        assert!(shot.dim[0]);
        done(&space);
    }

    #[test]
    fn base64_matches_the_standard() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"https://a.b/c?d=e"), "aHR0cHM6Ly9hLmIvYz9kPWU=");
    }

    #[test]
    fn byte_index_counts_letters_as_drawn() {
        assert_eq!(byte_index("héllo", 2), 3);
        assert_eq!(byte_index("héllo", 99), 6);
        assert_eq!(
            byte_index("e\u{301}tude", 1),
            3,
            "the accent is part of its letter"
        );
    }

    /// A row of the list as the app lists a note written with this head.
    fn a_row(head: &str) -> Row {
        let (mut app, space) = app();
        write(&mut app, head);
        let row = app.device.view().rows[0].clone();
        done(&space);
        row
    }

    #[test]
    fn a_head_is_cut_by_the_columns_it_takes() {
        assert_eq!(fit("浴室的瓷砖", 10), "浴室的瓷砖");
        assert_eq!(fit("浴室的瓷砖", 9), "浴室的瓷…");
        assert_eq!(fit("浴室的瓷砖", 8), "浴室的…");
        assert_eq!(fit("e\u{301}tude", 3), "e\u{301}t…");
        assert_eq!(fit("Boiler", 0), "");
        let row = a_row("浴室的瓷砖要换 #house");
        let line = row_line(&row, false, 12).to_string();
        assert_eq!(line, " 浴室的瓷…");
        assert!(line.width() <= 12);
    }

    #[test]
    fn a_head_scrolls_by_the_columns_its_letters_take() {
        let wide = "浴室的瓷砖";
        assert_eq!(
            sidescrolled(wide, 5, 20, Some(0), 1),
            (wide.into(), 10, Some(0))
        );
        assert_eq!(
            sidescrolled(wide, 5, 8, Some(0), 1),
            ("…的瓷砖".into(), 7, Some(2))
        );
        assert_eq!(
            sidescrolled(wide, 0, 8, Some(2), 1),
            ("浴室的…".into(), 0, Some(0))
        );
        assert_eq!(
            sidescrolled("e\u{301}tude", 1, 20, None, 5),
            ("e\u{301}tude".into(), 1, None)
        );
    }

    #[test]
    fn a_cut_leaves_no_blank_before_its_mark() {
        assert_eq!(
            sidescrolled("the boiler pressure", 19, 12, None, BESIDE),
            ("the boiler…".into(), 10, None),
            "the cursor on the `…`"
        );
    }

    #[test]
    fn a_joined_emoji_is_never_parted_across_rows_of_the_foot() {
        let family = "👨\u{200d}👩\u{200d}👧";
        let text = format!("{family}{family}{family}{family}");
        for width in 2..9 {
            let rows = wrap(&text, width);
            let letters: Vec<&str> = rows.iter().flat_map(|r| r.graphemes(true)).collect();
            assert_eq!(letters, [family; 4], "{width}: {rows:?}");
        }
    }

    #[test]
    fn a_wide_letter_at_an_edge_is_never_split_or_drawn_past_it() {
        let wide = "浴室的瓷砖要换浴室的瓷砖要换浴室的瓷砖要换";
        for room in 6..16 {
            for caret in 0..=letters(wide) {
                for left in [Some(0), Some(caret), Some(letters(wide))] {
                    let (shown, x, _) = sidescrolled(wide, caret, room, left, BESIDE);
                    assert!(shown.width() <= room, "{room} {caret} {left:?}: {shown}");
                    assert!(x <= room, "{room} {caret} {left:?}: {x}");
                    // The letter in the caret's column is the caret's.
                    let mut column = 0;
                    let under = shown.graphemes(true).find(|g| {
                        column += g.width();
                        column > x
                    });
                    let wanted = wide.graphemes(true).nth(caret);
                    assert_eq!(under, wanted, "{room} {caret} {left:?}: {shown} at {x}");
                }
            }
        }
        // The `…` at the left takes one column, and the letter after it starts in the next,
        // whole, though that leaves the row a column short at its right.
        let (shown, x, _) = sidescrolled(wide, 9, 11, Some(4), 1);
        assert_eq!(shown, "…浴室的瓷…");
        assert_eq!((shown.width(), x), (10, 5));
    }

    #[test]
    fn a_wide_head_under_the_cursor_has_its_date_at_the_edge_and_the_caret_at_its_end() {
        let row = a_row("浴室的瓷砖要换 #house");
        let created = Moment::utc(0);
        let date = time::format_day(created.day());
        let Selected { line, x, .. } = selected_row(
            &row,
            &row.head,
            Some(created),
            (letters(&row.head), None),
            false,
            40,
        );
        let painted = line.to_string();
        assert!(painted.ends_with(&date), "{painted:?}");
        assert_eq!(
            painted.width(),
            39,
            "the date ends a column short of the edge"
        );
        assert_eq!(x, 1 + 21, "after fourteen columns of letters and the tag");
    }

    #[test]
    fn a_combining_mark_is_part_of_its_letter() {
        let row = a_row("e\u{301} combi");
        let head = (letters(&row.head), None);
        let Selected { x, .. } = selected_row(&row, &row.head, None, head, false, 40);
        assert_eq!(x, 1 + 7);
        assert_eq!(
            edited("e\u{301}", 1, KeyCode::Backspace, false),
            Some((String::new(), 0)),
            "Backspace takes the accent with its letter"
        );
        assert_eq!(
            edited("e", 1, KeyCode::Char('\u{301}'), false),
            Some(("e\u{301}".into(), 1)),
            "an accent typed joins the letter before it"
        );
        assert_eq!(
            edited("ab e\u{301}", 4, KeyCode::Char('w'), true),
            Some(("ab ".into(), 3))
        );
    }
}
