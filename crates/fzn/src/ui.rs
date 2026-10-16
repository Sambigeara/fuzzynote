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
//! which is where a body is written. Right at the end of the head goes in, and at the end of the
//! line goes into the tag, person, day, box or note the line names; short of the end it is the
//! caret's, as Left is. A chord acts on the note where on the line it acts on the line, as it did
//! in fzn: Ctrl-D deletes or clears, Ctrl-V archives, which ticks a task, or shows archived notes,
//! and Ctrl-U puts a changed head back before it undoes anything else. Control with an arrow moves
//! the note under the cursor up or down a row, and the cursor with it. Escape comes back a rung at
//! a time: off the row, off the text on the line, out of the place, and on an empty line outside
//! any place two of them quit. The footer shows only the keys that apply where the cursor is.
//!
//! The screen is painted by hand and flat, in the terminal's own colours, because there is
//! nothing to frame: the terminal's cursor in a bold head is the row being written in, and dim is
//! what can be ignored. The line starts with a dim `>` in the column the rows leave empty, so that
//! it reads as where typing goes and its text lines up with theirs. Nothing is drawn between the
//! line and the list, nor under a row, where a body would open and close as the cursor walks past
//! and move every row below. The body read is that of what the cursor is on, the note under it or,
//! on the line in a place, the place's note, in rows kept at the foot of the list, the same rows
//! whatever the note holds. A status row is there only while there is something to say, and each
//! key clears what the one before it said.
//!
//! With a relay named, a thread holds the socket and the app hears from it between keys: what
//! the relay says goes to the device, and what the device wants kept or said comes back here to
//! be done, like anything else it asks for. Nothing is said while sync works, and `not syncing`
//! while it does not, in the corner of the foot that also says when this machine's clock is far
//! from the relay's. Nothing is reread from the directory while the app runs, since nothing
//! else writes it.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use fzn_engine::text::fit;
use fzn_engine::{Action, Device, Editing, Hint, Key, Row, Todo, View, Weights, time};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::Rect;
use ratatui::style::Stylize;
use ratatui::text::{Line, Span};
use ratatui::{DefaultTerminal, Frame};
use unicode_width::{UnicodeWidthChar as _, UnicodeWidthStr as _};

use crate::store::{self, Logs};
use crate::sync::{self, Link};
use crate::system::System;

/// How long the keyboard is waited on before the relay is listened to.
const TICK: Duration = Duration::from_millis(50);
/// The most rows the body of what the cursor is on takes at the foot of the list.
const PREVIEW_ROWS: usize = 5;
/// The mark before the text of the line, in the column the rows leave empty, so that the line
/// reads as where typing goes and the rows' text lines up under its own.
const PROMPT: &str = ">";
/// What the status row says for as long as the passphrase opens nothing the relay has sent.
const WRONG_KEY: &str =
    "the passphrase does not open this space: quit, and run fzn key to enter it again";

pub struct App {
    device: Device,
    logs: Logs,
    /// The space's directory, where the writer's id is kept.
    dir: PathBuf,
    editor: String,
    /// What the path row is headed with: the app's name, and the space when it is not the usual one.
    title: String,
    /// Cursor position in the line, in characters.
    cursor: usize,
    /// Cursor position in the head of the row the cursor is on, in characters.
    row_cursor: usize,
    /// The first row painted, kept so that the list moves only when the cursor would leave it.
    top: usize,
    /// Whether the previous key was Escape, which makes a second one quit.
    escaped: bool,
    /// The relay this space syncs with, as the status row names it, and the link to it.
    sync: Option<(String, Link)>,
    /// Whether the relay has been lost or could not be reached, and is not back yet.
    not_syncing: bool,
    /// Why the relay could not be reached, said until it is.
    unreachable: Option<String>,
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

/// What a key asks for. A chord acts on the line when the cursor is there and on the note under
/// the cursor otherwise. `at_end` and `empty` are of the text the cursor is in, the line or the
/// head. The arrow goes in only from its end, since short of it the arrow is the caret's: from a
/// head into the note, and from the line into what the line names, which the session decides.
/// Backspace takes a row away only when its head is empty, as a new row with only its labels is
/// while the caret is where it landed; with text in the head it is the caret's. Escape comes
/// back a rung: off the row, then off the text on the line, then out of the place.
fn bound(
    code: KeyCode,
    ctrl: bool,
    place: Place,
    zoomed: bool,
    at_end: bool,
    empty: bool,
) -> Option<Action> {
    let on_line = place == Place::Line;
    Some(match (code, ctrl) {
        (KeyCode::Enter, _) => Action::Write,
        (KeyCode::Backspace, _) if !on_line && empty => Action::Erase,
        (KeyCode::Char('o'), true) if !on_line => Action::Editor,
        (KeyCode::Right, _) if at_end => Action::GoIn,
        (KeyCode::Esc, _) if !on_line => Action::Select(None),
        (KeyCode::Esc, _) if !empty => Action::Type(String::new()),
        (KeyCode::Esc, _) if zoomed => Action::Back,
        (KeyCode::Char('d'), true) if on_line => Action::Type(String::new()),
        (KeyCode::Char('d'), true) => Action::Delete,
        (KeyCode::Char('v'), true) if on_line => Action::ShowArchived,
        (KeyCode::Char('v'), true) => Action::Archive,
        (KeyCode::Char('c'), true) if !on_line => Action::Copy,
        (KeyCode::Char('_' | '/'), true) if !on_line => Action::OpenUrl,
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

/// The key that asks for an action, as the footer names it.
fn key_for(action: &Action) -> &'static str {
    match action {
        Action::Write => "Enter",
        Action::Editor => "^O",
        Action::GoIn => "→",
        Action::Move(_) => "↓",
        Action::Shift(by) if *by < 0 => "^↑",
        Action::Shift(_) => "^↓",
        Action::Select(_) | Action::Back | Action::Done => "Esc",
        // Clearing the line is the rung Escape takes while it has text; Ctrl-D is its second key.
        Action::Type(text) if text.is_empty() => "Esc",
        Action::ShowArchived | Action::Archive => "^V",
        Action::Delete => "^D",
        Action::Link => "^L",
        Action::Copy => "^C",
        Action::OpenUrl => "^_",
        Action::Undo => "^U",
        Action::Redo => "^R",
        Action::ShowScores => "^B",
        // Typing and the editor ask for the rest, not a key. A body is written in the editor here,
        // so opening one in place has no key either. Backspace on an empty row asks for Erase,
        // and the footer does not name it, since taking an empty line away is what Backspace
        // does everywhere.
        Action::Edit
        | Action::Type(_)
        | Action::Edited(_)
        | Action::Save { .. }
        | Action::Erase => "",
    }
}

/// What is on offer that a key asks for where the caret is: not what no key asks for, and not
/// going in while the caret is short of the end of its text, where the arrow is the caret's.
fn keyed(hints: &[Hint], at_end: bool) -> Vec<&Hint> {
    hints
        .iter()
        .filter(|hint| !key_for(&hint.action).is_empty())
        .filter(|hint| at_end || hint.action != Action::GoIn)
        .collect()
}

/// The keys that do something where the cursor is, for the footer. Escape is always last, and
/// where the session has no way back for it to take, two of them quit.
fn footer(hints: &[Hint], at_end: bool, room: usize) -> String {
    let hints = keyed(hints, at_end);
    let mut keys: Vec<String> = hints
        .iter()
        .map(|hint| format!("{} {}", key_for(&hint.action), hint.label))
        .collect();
    if hints.last().is_none_or(|h| key_for(&h.action) != "Esc") {
        keys.push("Esc Esc quits".to_owned());
    }
    // A narrow terminal loses chords from the middle, never the first or the way back.
    let width = |keys: &[String]| keys.iter().map(|k| k.chars().count() + 2).sum::<usize>() - 2;
    while keys.len() > 2 && width(&keys) > room {
        keys.remove(keys.len() - 2);
    }
    keys.join("  ")
}

/// What a key does to a line of text, the line or a row's head, when it is one that edits
/// it or moves along it: the text as it then reads, and where its cursor then is, in characters.
fn edited(line: &str, cursor: usize, code: KeyCode, ctrl: bool) -> Option<(String, usize)> {
    let end = line.chars().count();
    let cursor = cursor.min(end);
    let without = |from: usize, to: usize| {
        let (from, to) = (byte_index(line, from), byte_index(line, to));
        format!("{}{}", &line[..from], &line[to..])
    };
    let with = |c: char| {
        let mut line = line.to_owned();
        line.insert(byte_index(&line, cursor), c);
        (line, cursor + 1)
    };
    Some(match (code, ctrl) {
        (KeyCode::Char('a'), true) | (KeyCode::Home, _) => (line.to_owned(), 0),
        (KeyCode::Char('e'), true) | (KeyCode::End, _) => (line.to_owned(), end),
        (KeyCode::Left, _) => (line.to_owned(), cursor.saturating_sub(1)),
        (KeyCode::Right, _) => (line.to_owned(), (cursor + 1).min(end)),
        (KeyCode::Backspace, _) if cursor > 0 => (without(cursor - 1, cursor), cursor - 1),
        (KeyCode::Delete, _) if cursor < end => (without(cursor, cursor + 1), cursor),
        (KeyCode::Char('w'), true) => {
            let chars: Vec<char> = line.chars().collect();
            let mut start = cursor;
            while start > 0 && chars[start - 1].is_whitespace() {
                start -= 1;
            }
            while start > 0 && !chars[start - 1].is_whitespace() {
                start -= 1;
            }
            (without(start, cursor), start)
        }
        (KeyCode::Tab, _) => with(' '),
        (KeyCode::Char(c), false) => with(c),
        _ => return None,
    })
}

/// Where you are: the app, then each place gone into, outermost first, in at most `max`
/// characters. Past three steps the middle ones give way to `…`. Every step gets an equal share
/// of the room, and what a short step does not use goes to the longer ones.
fn breadcrumb(title: &str, heads: &[&str], max: usize) -> String {
    let steps: Vec<&str> = match heads {
        [first, .., before, last] if heads.len() > 3 => vec![first, "…", before, last],
        _ => heads.to_vec(),
    };
    let mut room = max.saturating_sub(title.chars().count() + 2 + 3 * steps.len());
    let mut widths = vec![0; steps.len()];
    let mut order: Vec<usize> = (0..steps.len()).collect();
    order.sort_by_key(|&i| steps[i].chars().count());
    for (k, &i) in order.iter().enumerate() {
        widths[i] = steps[i].chars().count().min(room / (steps.len() - k));
        room -= widths[i];
    }
    let mut crumb = format!(" {title}");
    for (step, width) in steps.iter().zip(widths) {
        crumb.push_str(" › ");
        crumb.push_str(&fit(step, width));
    }
    crumb.push(' ');
    crumb
}

fn byte_index(s: &str, char_index: usize) -> usize {
    s.char_indices().nth(char_index).map_or(s.len(), |(b, _)| b)
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

fn run_editor(editor: &str, path: &Path) -> Result<ExitStatus> {
    let mut parts = editor.split_whitespace();
    let program = parts.next().context("empty editor command")?;
    Command::new(program)
        .args(parts)
        .arg(path)
        .status()
        .with_context(|| format!("run editor {editor:?}"))
}

/// Hands a note's text to the editor and waits for it. Gives back the text as it was left, or
/// what to say when the editor did not finish well.
fn edit_in(editor: &str, id: &str, text: &str) -> Result<Result<String, String>> {
    let path = std::env::temp_dir().join(format!("fzn-{}.md", sanitise(id)));
    fs::write(&path, text)?;
    let status = run_editor(editor, &path)?;
    if !status.success() {
        return Ok(Err(format!(
            "{editor} exited with {status}; note unchanged"
        )));
    }
    let edited = fs::read_to_string(&path)?;
    let _ = fs::remove_file(&path);
    Ok(Ok(edited))
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

/// What follows a row's head, to be dimmed: a mark if there is more to read, the notes it links
/// to, and why it is listed.
fn tail(row: &Row) -> String {
    let mut tail = String::new();
    if row.has_body {
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
/// across as many as it takes. Each line of the text starts a row of its own. A letter is as wide
/// as the terminal draws it, so that a row of wide letters, Chinese say, is broken where it meets
/// the edge rather than cut off there.
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
            for c in word.chars() {
                let n = c.width().unwrap_or(0);
                if len > 0 && len + n > width {
                    rows.push(std::mem::take(&mut row));
                    len = 0;
                }
                row.push(c);
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

/// A row of the list as it is when the cursor is not on it: the head, then as much of the tail
/// as there is room for. The head is cut only when it alone does not fit.
fn row_line(row: &Row, scores: bool, width: usize) -> Line<'static> {
    let score = score_of(row, scores);
    let room = width.saturating_sub(2 + score.chars().count());
    let head = fit(&row.head, room);
    let rest = room - head.chars().count();
    let tail = tail(row);
    let mut spans = vec![Span::raw(" "), Span::raw(score).dim(), Span::raw(head)];
    if !tail.is_empty() && rest > 3 {
        spans.push(Span::raw(fit(&tail, rest)).dim());
    }
    let line = Line::from(spans);
    if row.archived { line.dim() } else { line }
}

/// The row the cursor is on, `head` being its head as it is now typed: the head bold and its date
/// at the right, and its tail after the head or, when it does not fit there, on a row of its own
/// under it, since this is the row being asked about. A head too long for the room scrolls so
/// that the caret, `caret` characters in, stays in sight; the column it is then drawn at comes
/// back with the rows.
fn selected_rows(
    row: &Row,
    head: &str,
    caret: usize,
    scores: bool,
    width: usize,
) -> (Vec<Line<'static>>, usize) {
    let score = score_of(row, scores);
    let date = row
        .updated
        .map_or_else(String::new, |m| time::format_day(m.day()));
    let dated = if date.is_empty() { 0 } else { date.len() + 2 };
    let room = width
        .saturating_sub(2 + score.chars().count() + dated)
        .max(1);
    let caret = caret.min(head.chars().count());
    let scroll = caret.saturating_sub(room - 1);
    let head: String = head.chars().skip(scroll).take(room).collect();
    let x = 1 + score.chars().count() + caret - scroll;
    let tail = tail(row);
    let fits = tail.chars().count() <= room - head.chars().count();
    let mut used = 1 + score.chars().count() + head.chars().count();
    let mut spans = vec![
        Span::raw(" "),
        Span::raw(score).dim(),
        Span::raw(head).bold(),
    ];
    if fits {
        used += tail.chars().count();
        spans.push(Span::raw(tail.clone()).dim());
    }
    if !date.is_empty() {
        let pad = width.saturating_sub(used + date.len() + 1);
        spans.push(Span::raw(" ".repeat(pad)));
        spans.push(Span::raw(date).dim());
    }
    let mut rows = vec![Line::from(spans)];
    if !fits {
        let tail = fit(tail.trim_start(), width.saturating_sub(4));
        rows.push(Line::from(format!("   {tail}")).dim());
    }
    if row.archived {
        rows = rows.into_iter().map(Stylize::dim).collect();
    }
    (rows, x)
}

/// A row of the list as the rows of the screen it takes, and where in them the cursor is when it
/// is on that row: which of them, and how far along.
struct Painted {
    lines: Vec<Line<'static>>,
    cursor: Option<(usize, usize)>,
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
    let len = text.chars().count();
    if !text.is_empty() && len + 2 < width {
        paint(frame, y, width - len - 1, Line::from(text.to_owned()).dim());
    }
}

impl App {
    /// Opens a space's directory as a device writing under `writer`: every log is given to the
    /// device, what it asks for is done, and then whatever opening it is due, a compaction say.
    /// Fails if the key opens none of the sealed lines there, since nothing could be written.
    pub fn open(
        dir: &Path,
        space: &str,
        writer: String,
        key: Key,
        weights: Weights,
        editor: String,
        title: String,
    ) -> Result<Self> {
        let mut logs = Logs::open(dir)?;
        let loaded = logs.load(true)?;
        let mut app = Self {
            device: Device::new(Box::new(System), writer, weights, key),
            logs,
            dir: dir.to_owned(),
            editor,
            title,
            cursor: 0,
            row_cursor: 0,
            top: 0,
            escaped: false,
            sync: None,
            not_syncing: false,
            unreachable: None,
        };
        for (log, lines) in loaded {
            let todo = app.device.load(&log, lines);
            app.carry_out(todo)?;
        }
        if !app.device.key_verified() {
            bail!("the passphrase does not open {space}: run fzn key to enter it again");
        }
        let todo = app.device.opened();
        app.carry_out(todo)?;
        // An empty space says how to begin, until the first key.
        if app.device.view().status.is_empty() {
            match (app.device.unopened(), app.device.view().notes) {
                (0, 0) => app
                    .device
                    .say("no notes yet: type and press Enter, or see fzn --help to import"),
                (0, _) => {}
                (shut, _) => app.device.say(format!(
                    "{shut} lines here that this passphrase does not open"
                )),
            }
        }
        Ok(app)
    }

    /// Syncs with a relay over a link to the thread that holds the socket. `relay` is how the
    /// status row names it when it cannot be reached.
    pub fn sync(&mut self, relay: String, link: Link) {
        self.sync = Some((relay, link));
    }

    pub fn run(mut self) -> Result<()> {
        let mut terminal = ratatui::init();
        let outcome = self.event_loop(&mut terminal);
        ratatui::restore();
        outcome
    }

    fn event_loop(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        loop {
            terminal.draw(|frame| self.draw(frame))?;
            while !event::poll(TICK)? {
                if self.hear()? {
                    terminal.draw(|frame| self.draw(frame))?;
                }
            }
            if let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                match self.handle(key)? {
                    Flow::Continue => {}
                    Flow::Editor { id, text } => self.hand_to_editor(terminal, id, &text)?,
                    Flow::Quit => return Ok(()),
                }
            }
        }
    }

    fn handle(&mut self, key: KeyEvent) -> Result<Flow> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let escaped = std::mem::take(&mut self.escaped);
        let (zoomed, line, field, (place, at_end, empty)) = {
            let view = self.device.view();
            let field = view.field.as_ref().map(|field| field.text.to_owned());
            let caret = self.caret(&view);
            (!view.path.is_empty(), view.line.to_owned(), field, caret)
        };
        // Each key clears what the one before it said, so the status row says only what this
        // one did.
        self.device.say("");
        if (key.code, ctrl) == (KeyCode::Char('q'), true) {
            if place == Place::Row {
                self.act(Action::Done)?;
            }
            return Ok(Flow::Quit);
        }
        if let Some(action) = bound(key.code, ctrl, place, zoomed, at_end, empty) {
            return self.act(action);
        }
        match key.code {
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
                match edited(head, self.row_cursor, code, ctrl) {
                    Some((to, cursor)) if to != head => {
                        self.row_cursor = cursor;
                        let text = match body {
                            Some(body) => format!("{to}\n{body}"),
                            None => to,
                        };
                        return self.act(Action::Edited(text));
                    }
                    Some((_, cursor)) => self.row_cursor = cursor,
                    None => {}
                }
            }
            // On the line typing goes to the line, so the device is told what it now reads.
            code => match edited(&line, self.cursor, code, ctrl) {
                Some((to, cursor)) if to != line => {
                    let flow = self.act(Action::Type(to))?;
                    self.cursor = cursor;
                    return Ok(flow);
                }
                Some((_, cursor)) => self.cursor = cursor,
                None => {}
            },
        }
        Ok(Flow::Continue)
    }

    /// Where the cursor is, and of the text it is in, the head of its row or the line, whether
    /// the caret is at its end and whether it is empty. A new row with only the labels it opened
    /// with is empty while the caret is where it lands, after a task's box and before the rest.
    fn caret(&self, view: &View) -> (Place, bool, bool) {
        let (place, text, caret, fresh) = match &view.field {
            Some(field) => {
                let head = head_and_body(field.text).0;
                (Place::Row, head, self.row_cursor, field.fresh)
            }
            None => (Place::Line, view.line, self.cursor, None),
        };
        let empty = text.is_empty() || fresh == Some(caret);
        (place, caret >= text.chars().count(), empty)
    }

    /// Asks the device for something, and carries out what it asks for in return.
    fn act(&mut self, action: Action) -> Result<Flow> {
        let (line, was_on, was_fresh) = {
            let view = self.device.view();
            let fresh = view.field.as_ref().and_then(|field| field.fresh).is_some();
            (view.line.to_owned(), under_cursor(&view), fresh)
        };
        let todo = self.device.act(action);
        let flow = self.carry_out(todo)?;
        let view = self.device.view();
        // A line the device changed, as it does on the way into a note and out of one, has its
        // cursor at the end, and so does the head of a row just landed on. A head put back or
        // written in place keeps its caret where it was, if the head is still that long. A new
        // row with only the labels it opened with, landed on or put back to them, has its caret
        // where the device says, after a task's box and before the rest.
        if view.line != line {
            self.cursor = view.line.chars().count();
        }
        if let Some(field) = &view.field {
            let end = head_and_body(field.text).0.chars().count();
            let stayed = under_cursor(&view) == was_on;
            self.row_cursor = match field.fresh {
                Some(start) if !(stayed && was_fresh) => start,
                _ if stayed => self.row_cursor.min(end),
                _ => end,
            };
        }
        Ok(flow)
    }

    /// Does what the device asks: lines kept before retired logs are deleted, so that a
    /// compaction stopped half way leaves both logs and not neither, then the new writer id
    /// kept, then what is to go to the relay, then the rest.
    fn carry_out(&mut self, todo: Todo) -> Result<Flow> {
        for kept in &todo.keep {
            self.logs.append(&kept.log, &kept.lines)?;
        }
        for log in &todo.delete {
            self.logs.delete(log)?;
        }
        if let Some(writer) = &todo.device {
            store::set_writer(&self.dir, writer)?;
        }
        if let Some((_, link)) = &self.sync {
            for text in todo.send {
                // A thread that has gone is a relay that is not there, which the footer says.
                let _ = link.to_relay.send(text);
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
                    let todo = self.device.connected();
                    self.carry_out(todo)?;
                }
                sync::Event::Message(text) => {
                    let todo = self.device.received(text);
                    self.carry_out(todo)?;
                }
                sync::Event::Disconnected => {
                    self.not_syncing = true;
                    self.device.disconnected();
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
        Ok(!heard.is_empty())
    }

    /// What the status row says: what the last key did, or what happened since, and when
    /// nothing did, the trouble that lasts until it is put right. With none, there is no row.
    fn status(&self) -> Option<String> {
        let said = self.device.view().status;
        if !said.is_empty() {
            Some(said.to_owned())
        } else if !self.device.key_verified() {
            Some(WRONG_KEY.to_owned())
        } else {
            self.unreachable.clone()
        }
    }

    /// Gives the terminal over to the editor, and saves what it leaves.
    fn hand_to_editor(
        &mut self,
        terminal: &mut DefaultTerminal,
        id: String,
        text: &str,
    ) -> Result<()> {
        ratatui::restore();
        let edited = edit_in(&self.editor, &id, text);
        *terminal = ratatui::init();
        terminal.clear()?;
        match edited? {
            Ok(text) => {
                self.act(Action::Save { id, text })?;
            }
            Err(why) => self.device.say(why),
        }
        Ok(())
    }

    /// Paints the view from the top down: the path in a zoom, the line, the list, the body of what
    /// the cursor is on, and at the foot the status when there is one and the keys.
    fn draw(&mut self, frame: &mut Frame) {
        let status = self.status();
        let view = self.device.view();
        let (_, at_end, _) = self.caret(&view);
        let width = usize::from(frame.area().width);
        let height = usize::from(frame.area().height);
        let zoomed = !view.path.is_empty();
        let mut cursor = None;
        let mut y = 0;

        if zoomed {
            let crumb = breadcrumb(&self.title, &view.path, width.saturating_sub(1));
            paint(frame, y, 0, Line::from(crumb).dim());
            y += 1;
        }

        // The line, scrolled so that its cursor is in sight of whatever is said at its end.
        let archived = if view.archived_shown {
            "archived shown"
        } else {
            ""
        };
        let right = [dates_shown(&view.dates), archived.to_owned()]
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("  ");
        let said = right.chars().count();
        let room = width
            .saturating_sub(2 + if said > 0 { said + 2 } else { 0 })
            .max(1);
        let scroll = self.cursor.saturating_sub(room - 1);
        let shown: String = view.line.chars().skip(scroll).take(room).collect();
        paint(frame, y, 0, Line::from(PROMPT).dim());
        paint(frame, y, 1, Line::from(shown));
        paint_right(frame, y, &right);
        if view.cursor.is_none() {
            cursor = Some((1 + self.cursor - scroll, y));
        }
        y += 1;

        let mut blocks: Vec<Painted> = Vec::new();
        for (i, row) in view.rows.iter().enumerate() {
            let mut lines = Vec::new();
            // In a zoom, what the line finds in the place is kept apart from what it only finds
            // there. With nothing on the line the place lists only what is in it.
            if zoomed
                && !view.line.trim().is_empty()
                && i > 0
                && view.rows[i - 1].linked
                && !row.linked
            {
                lines.push(Line::default());
            }
            let at = lines.len();
            let mut at_cursor = None;
            // The row the cursor is on is drawn from the note as it is being typed, with the
            // terminal's cursor in its head.
            if view.cursor == Some(i) {
                let head = view
                    .field
                    .as_ref()
                    .map_or(row.head.as_str(), |f| head_and_body(f.text).0);
                let (rows, x) = selected_rows(row, head, self.row_cursor, view.scores_shown, width);
                lines.extend(rows);
                at_cursor = Some((at, x));
            } else {
                lines.push(row_line(row, view.scores_shown, width));
            }
            blocks.push(Painted {
                lines,
                cursor: at_cursor,
            });
        }

        // The foot of the list is kept for the body of what the cursor is on, under a row of
        // nothing: on a row its note as it is being written, and on the line in a place the
        // place's note, if there is one. It is kept whatever the note holds, and in a place on the
        // line too, so that walking the list never changes how much of it there is; at the top
        // the line has the whole height. The status row takes the last of the rows kept, so that
        // it does not move the list either by coming or by going.
        let read = match &view.field {
            Some(field) => Some(field.text),
            None if zoomed => Some(view.here.map_or("", |here| here.text.as_str())),
            None => None,
        };
        let status_row = usize::from(status.is_some());
        let preview = read.map_or(0, |_| preview_rows(height.saturating_sub(1 + y)));
        let bottom = height.saturating_sub(1 + if preview > 0 { preview + 1 } else { status_row });
        if let Some(text) = read.filter(|_| preview > 0) {
            let rows = body_rows(head_and_body(text).1, width, preview - status_row);
            for (k, row) in rows.into_iter().enumerate() {
                paint(frame, bottom + 1 + k, 0, row.dim());
            }
        }

        // The list moves only as far as it must to keep the cursor's row in sight.
        let room = bottom.saturating_sub(y);
        match view.cursor {
            None => self.top = 0,
            Some(focus) => {
                self.top = self.top.min(focus);
                while self.top < focus
                    && blocks[self.top..=focus]
                        .iter()
                        .map(|painted| painted.lines.len())
                        .sum::<usize>()
                        > room
                {
                    self.top += 1;
                }
            }
        }
        'list: for painted in blocks.into_iter().skip(self.top) {
            for (k, line) in painted.lines.into_iter().enumerate() {
                if y >= bottom {
                    break 'list;
                }
                if let Some((at, x)) = painted.cursor
                    && at == k
                {
                    cursor = Some((x, y));
                }
                paint(frame, y, 0, line);
                y += 1;
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
            Line::from(fit(&footer(&view.hints, at_end, keys_room), keys_room)).dim(),
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
    use crate::store::tests::temp_dir;
    use fzn_engine::replica::{Header, Message, Out, Replica};
    use fzn_engine::{Moment, Record, Seal};
    use std::sync::mpsc::{self, Receiver, Sender};

    const WRITER: &str = "3f9a01c2";

    fn key() -> Key {
        Key::from_bytes([7; 32])
    }

    /// The app opened on a space's directory with a key, as `fzn` opens it.
    fn open_with(space: &Path, key: Key) -> Result<App> {
        let writer = store::writer(space, &space.with_file_name("config"))?;
        App::open(
            space,
            "main",
            writer,
            key,
            Weights::default(),
            "true".into(),
            "fzn".into(),
        )
    }

    /// The app on a new space under a scratch root, and the space's directory.
    fn app() -> (App, PathBuf) {
        let space = temp_dir().join("main");
        store::set_writer(&space, WRITER).unwrap();
        (open_with(&space, key()).unwrap(), space)
    }

    /// Takes the scratch root away.
    fn done(space: &Path) {
        fs::remove_dir_all(space.parent().unwrap()).unwrap();
    }

    /// What the logs in the directory add up to.
    fn on_disk(space: &Path) -> Store {
        Store::open(space, String::new(), Seal::new(key()), false).unwrap()
    }

    /// Presses a key, with Control held when `ctrl` is set.
    fn press(app: &mut App, code: KeyCode, ctrl: bool) -> Flow {
        let modifiers = if ctrl {
            KeyModifiers::CONTROL
        } else {
            KeyModifiers::NONE
        };
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
    /// cursor to the note it wrote, and Esc brings it back.
    fn write(app: &mut App, text: &str) {
        type_in(app, text);
        press(app, KeyCode::Enter, false);
        if on_note(app) {
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
            reopened.status(),
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
        store::set_writer(&space, WRITER).unwrap();
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

        // On a note, the arrow goes in, since the caret landed at the end of its head. On the
        // line, Enter writes, and what is written there is linked.
        press(&mut app, KeyCode::Right, false);
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
        press(&mut app, KeyCode::Right, false);
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
    fn right_at_the_end_of_the_line_goes_into_what_it_names_and_short_of_it_moves_the_caret() {
        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        write(&mut app, "Fix the gutter #house");
        write(&mut app, "Cake recipe");

        type_in(&mut app, "#house");
        assert!(keys(&app).contains("→ go in"), "{}", keys(&app));
        press(&mut app, KeyCode::Left, false);
        assert!(!keys(&app).contains("→ go in"), "{}", keys(&app));
        press(&mut app, KeyCode::Right, false);
        assert!(path(&app).is_empty(), "the caret moved");
        assert_eq!(app.cursor, 6);
        press(&mut app, KeyCode::Right, false);
        assert_eq!(path(&app), ["#house"]);
        assert_eq!(line(&app), "");
        let mut house = heads(&app)[..2].to_vec();
        house.sort();
        assert_eq!(house, ["Boiler service #house", "Fix the gutter #house"]);

        // What names nothing leaves the arrow doing nothing.
        type_in(&mut app, "gutter and");
        assert!(!keys(&app).contains("go in"), "{}", keys(&app));
        press(&mut app, KeyCode::Right, false);
        assert_eq!(path(&app), ["#house"]);
        assert_eq!(line(&app), "gutter and");
        done(&space);
    }

    #[test]
    fn escape_on_the_line_clears_it_then_steps_back_and_only_then_says_it_would_quit() {
        let (mut app, space) = app();
        write(&mut app, "Boiler service #house");
        type_in(&mut app, "#house");
        press(&mut app, KeyCode::Right, false);

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
            app.status(),
            None,
            "a rung was taken, so nothing about quitting"
        );
        assert!(keys(&app).ends_with("Esc Esc quits"), "{}", keys(&app));
        assert!(matches!(
            press(&mut app, KeyCode::Esc, false),
            Flow::Continue
        ));
        assert_eq!(app.status().as_deref(), Some("Escape again to quit"));
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
        press(&mut app, KeyCode::Right, false);
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
        press(&mut app, KeyCode::Right, false);
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
        press(&mut app, KeyCode::Right, false);
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
        press(&mut app, KeyCode::Right, false);
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
        assert_eq!(line(&app), " ", "a tab is a space");
        chord(&mut app, 'd');

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
        press(&mut app, KeyCode::Right, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("", 0));
        press(&mut app, KeyCode::Esc, false);
        assert_eq!((line(&app).as_str(), app.cursor), ("trai", 4));
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
    fn named(key: &str) -> (KeyCode, bool) {
        match key {
            "Enter" => (KeyCode::Enter, false),
            "Esc" => (KeyCode::Esc, false),
            "↓" => (KeyCode::Down, false),
            "^↑" => (KeyCode::Up, true),
            "^↓" => (KeyCode::Down, true),
            "→" => (KeyCode::Right, false),
            chord => {
                let mut chars = chord.chars();
                assert_eq!(chars.next(), Some('^'), "{chord}");
                let c = chars.next().unwrap().to_ascii_lowercase();
                (KeyCode::Char(c), true)
            }
        }
    }

    /// The footer as it is now, having checked that every key it names asks for what it says.
    fn keys(app: &App) -> String {
        let view = app.device.view();
        let zoomed = !view.path.is_empty();
        let (place, at_end, empty) = app.caret(&view);
        for hint in keyed(&view.hints, at_end) {
            let (code, ctrl) = named(key_for(&hint.action));
            assert_eq!(
                bound(code, ctrl, place, zoomed, at_end, empty).as_ref(),
                Some(&hint.action),
                "{} {}",
                key_for(&hint.action),
                hint.label
            );
        }
        footer(&view.hints, at_end, usize::MAX)
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
            "Enter new note  ^O editor  → go in  ^V archive  ^D delete  ^C copy  ^U undo  Esc up"
        );

        // Short of the end of the head the arrow is the caret's, so going in is not named there.
        press(&mut app, KeyCode::Left, false);
        assert_eq!(
            keys(&app),
            "Enter new note  ^O editor  ^V archive  ^D delete  ^C copy  ^U undo  Esc up"
        );
        press(&mut app, KeyCode::Right, false);
        assert!(path(&app).is_empty(), "the caret moved");
        assert!(keys(&app).contains("→ go in"), "{}", keys(&app));

        // A changed head offers putting it back in place of undo.
        type_in(&mut app, "s");
        assert_eq!(
            keys(&app),
            "Enter new note  ^O editor  → go in  ^V archive  ^D delete  ^C copy  ^U put back  \
             Esc up"
        );
        chord(&mut app, 'u');
        assert_eq!(field(&app).as_deref(), Some("trainingapp"));
        assert_eq!(app.row_cursor, 11, "the caret is kept in the head put back");

        press(&mut app, KeyCode::Right, false);
        write(&mut app, "squat form video https://example.com/squat");
        assert_eq!(
            keys(&app),
            "Enter new note here  ↓ pick a note  ^V show archived  ^U undo  Esc back"
        );
        press(&mut app, KeyCode::Down, false);
        assert_eq!(
            keys(&app),
            "Enter new note here  ^O editor  → go in  ^L unlink  ^V archive  ^D delete  \
             ^C copy  ^_ open URL  ^U undo  Esc up"
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
            "Enter new note here  ^O editor  → go in  ^L unlink  ^V unarchive  ^D delete  \
             ^C copy  ^_ open URL  ^U undo  ^R redo  Esc up"
        );
        assert!(widest.chars().count() <= 124, "{}", widest.chars().count());
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
            "Enter new note here  ^O editor  → go in  ^L unlink  ^V archive  ^D delete  \
             ^C copy  ^↓ move down  ^U put back  Esc up"
        );
        done(&space);
    }

    /// The far ends of an app's link: what it sent, and a way to tell it things.
    fn link(app: &mut App) -> (Receiver<String>, Sender<sync::Event>) {
        let (to_relay, sent) = mpsc::channel();
        let (tell, from_relay) = mpsc::channel();
        app.sync(
            "nas.local".into(),
            Link {
                to_relay,
                from_relay,
            },
        );
        (sent, tell)
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
    fn a_device_given_lines_its_passphrase_does_not_open_writes_nothing() {
        let (mut app, space) = {
            let space = temp_dir().join("main");
            store::set_writer(&space, WRITER).unwrap();
            (open_with(&space, Key::from_bytes([9; 32])).unwrap(), space)
        };
        let (sent, tell) = link(&mut app);
        let mut relay = Replica::new();
        relay.append("dev-b", written_elsewhere("boiler service booked", key()));
        connect(&mut app, &mut relay, &sent, &tell);
        assert!(heads(&app).is_empty());
        assert_eq!(
            app.status().as_deref(),
            Some(WRONG_KEY),
            "said as soon as it comes"
        );
        write(&mut app, "a garden");
        let said = app.status().unwrap_or_default();
        assert!(said.contains("passphrase"), "{said}");
        press(&mut app, KeyCode::Left, false);
        assert_eq!(
            app.status().as_deref(),
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
        assert_eq!(edit_in("true", &id, &text).unwrap(), Ok("the plans".into()));
        let gave_up = edit_in("false", &id, &text).unwrap().unwrap_err();
        assert!(gave_up.starts_with("false exited with"), "{gave_up}");
        assert!(gave_up.ends_with("note unchanged"), "{gave_up}");

        // One that leaves something else behind.
        let left = space.join("left-by-the-editor.md");
        fs::write(&left, "The plan\nwith a body\n").unwrap();
        let editor = format!("cp {}", left.display());
        let edited = edit_in(&editor, &id, &text).unwrap().unwrap();
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
        assert_eq!(app.status(), None, "a letter says nothing");
        assert_eq!(count(), 3, "and is written");

        press(&mut app, KeyCode::Down, false);
        assert_eq!(app.status(), None);
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
    fn right_at_the_end_of_the_head_goes_in_and_before_it_moves_the_caret() {
        let (mut app, space) = app();
        write(&mut app, "Boiler");
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Left, false);
        assert_eq!(app.row_cursor, 5);
        press(&mut app, KeyCode::Right, false);
        assert_eq!(app.row_cursor, 6);
        assert!(
            path(&app).is_empty(),
            "short of the end the arrow is the caret's"
        );

        // Left at the start of the head stays there.
        chord(&mut app, 'a');
        press(&mut app, KeyCode::Left, false);
        assert_eq!(app.row_cursor, 0);
        assert!(on_note(&app) && path(&app).is_empty());

        chord(&mut app, 'e');
        press(&mut app, KeyCode::Right, false);
        assert_eq!(path(&app), ["Boiler"]);
        done(&space);
    }

    #[test]
    fn a_head_too_long_for_its_row_scrolls_to_keep_the_caret_in_sight() {
        let (mut app, space) = app();
        let long = "the boiler pressure keeps dropping after the radiators are bled";
        write(&mut app, long);
        press(&mut app, KeyCode::Down, false);
        let shot = screen(&mut app, 40, 8);
        let date = time::format_day(app.device.view().rows[0].updated.unwrap().day());
        assert!(shot.rows[1].ends_with(&date), "{:?}", shot.rows[1]);
        assert!(shot.rows[1].contains("are bled"), "{:?}", shot.rows[1]);
        assert!(shot.cursor.0 < 40 - 12, "{:?}", shot.cursor);
        assert_eq!(shot.cursor.1, 1);

        chord(&mut app, 'a');
        let shot = screen(&mut app, 40, 8);
        assert!(
            shot.rows[1].starts_with(" the boiler"),
            "{:?}",
            shot.rows[1]
        );
        assert_eq!(shot.cursor, (1, 1));
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
            app.status(),
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
        let updated = app.device.view().rows[0].updated.unwrap();
        let date = format!(" {}", time::format_day(updated.day()));
        let shot = screen(&mut app, 60, 10);
        assert!(
            shot.rows[1].starts_with(&format!(" {picked} ¶")) && shot.rows[1].ends_with(&date),
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
            shot.rows[9].starts_with(" Enter new note  ^O editor  → go in  ^V archive"),
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
        press(&mut app, KeyCode::Right, false);
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
        assert!(app.status().unwrap().starts_with("no notes yet"));
        type_in(&mut app, "x");
        assert_eq!(app.status(), None, "the first key clears what opening said");
        chord(&mut app, 'd');
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(app.status().as_deref(), Some("Escape again to quit"));
        press(&mut app, KeyCode::Left, false);
        assert_eq!(app.status(), None, "and so does every key after");

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
            app.status().as_deref(),
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
                .starts_with("the log dev-b here is not the dev-b on the relay"),
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
    fn the_title_says_where_you_are_and_fits() {
        assert_eq!(breadcrumb("fzn", &[], 70), " fzn ");
        assert_eq!(
            breadcrumb("fzn", &["trainingapp", "Profile"], 70),
            " fzn › trainingapp › Profile "
        );
        let long = "a very long head that goes on and on about the training plan for the year";
        let crumb = breadcrumb("fzn · fzn-2022", &["trainingapp", long], 70);
        assert_eq!(crumb.chars().count(), 70);
        assert!(
            crumb.starts_with(" fzn · fzn-2022 › trainingapp › a very long")
                && crumb.ends_with("… "),
            "a short head keeps its length and the long one takes what is left: {crumb}"
        );
        assert_eq!(
            breadcrumb("fzn", &["one", "two", "three", "four", "five"], 70),
            " fzn › one › … › four › five "
        );
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
    fn byte_index_handles_multibyte() {
        assert_eq!(byte_index("héllo", 2), 3);
        assert_eq!(byte_index("héllo", 99), 6);
    }
}
