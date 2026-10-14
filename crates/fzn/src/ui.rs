//! The terminal face: one input, one list, one preview, with the original fzn keys.
//!
//! The device is the app. This is what a terminal adds to it: which key asks for what, editing
//! the top line, painting the view, and carrying out what the device asks for in return, which
//! is keeping lines in the space's directory, saying things to the relay, running your editor,
//! the clipboard and opening a URL.
//!
//! The input is the top line. Down moves onto the list, Up and Escape come back to the top line,
//! and typing always goes to the top line. A chord acts on the top line when the cursor is there
//! and on the selected note otherwise, as it did in fzn: Enter writes or goes in, Ctrl-D clears
//! or deletes, Ctrl-V shows archived notes or archives. Escape steps back out of a note, and
//! outside any note two of them quit. Notes are edited in your editor. The footer shows only the
//! keys that apply where the cursor is.
//!
//! With a relay named, a thread holds the socket and the app hears from it between keys: what
//! the relay says goes to the device, and what the device wants kept or said comes back here to
//! be done, like anything else it asks for. The status row says how sync is going. Nothing is
//! reread from the directory while the app runs, since nothing else writes it.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use fzn_engine::text::fit;
use fzn_engine::{Action, Day, Device, Editing, Hint, Key, Moment, Todo, Weights, time};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use crate::store::{self, Logs};
use crate::sync::{self, Link};
use crate::system::System;

/// How long the keyboard is waited on before the relay is listened to.
const TICK: Duration = Duration::from_millis(50);
/// The most the breadcrumb on the top line takes up.
const CRUMB_MAX: usize = 70;

pub struct App {
    device: Device,
    logs: Logs,
    /// The space's directory, where the writer's id is kept.
    dir: PathBuf,
    editor: String,
    /// What the top line is headed with: the app's name, and the space when it is not the usual one.
    title: String,
    /// Cursor position in the top line, in characters.
    cursor: usize,
    /// Where the list is scrolled to.
    list: ListState,
    /// Whether the previous key was Escape, which makes a second one quit.
    escaped: bool,
    /// The relay this space syncs with, as the status row names it, and the link to it.
    sync: Option<(String, Link)>,
    /// How sync is going, for the status row.
    synced: String,
}

enum Flow {
    Continue,
    /// A note is to be opened in the editor, which needs the terminal.
    Edit {
        id: String,
        text: String,
    },
    Quit,
}

/// What a chord asks for. A chord acts on the top line when the cursor is there and on the
/// selected note otherwise. Escape comes back a step: off the list, then out of the note.
fn bound(code: KeyCode, ctrl: bool, on_top: bool, zoomed: bool) -> Option<Action> {
    Some(match (code, ctrl) {
        (KeyCode::Enter, _) if on_top => Action::Write,
        (KeyCode::Enter, _) => Action::GoIn,
        (KeyCode::Esc, _) if !on_top => Action::Select(None),
        (KeyCode::Esc, _) if zoomed => Action::Back,
        (KeyCode::Char('d'), true) if on_top => Action::Type(String::new()),
        (KeyCode::Char('d'), true) => Action::Delete,
        (KeyCode::Char('v'), true) if on_top => Action::ShowArchived,
        (KeyCode::Char('v'), true) => Action::Archive,
        (KeyCode::Char('o'), true) => Action::Edit,
        (KeyCode::Char('u'), true) => Action::Undo,
        (KeyCode::Char('r'), true) => Action::Redo,
        (KeyCode::Char('l'), true) => Action::Link,
        (KeyCode::Char('c'), true) => Action::Copy,
        (KeyCode::Char('_' | '/'), true) => Action::OpenUrl,
        (KeyCode::Char('b'), true) => Action::ShowScores,
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
        Action::Write | Action::GoIn => "Enter",
        Action::Move(_) => "↓",
        Action::Select(_) | Action::Back => "Esc",
        Action::ShowArchived | Action::Archive => "^V",
        Action::Delete => "^D",
        Action::Edit => "^O",
        Action::Link => "^L",
        Action::Copy => "^C",
        Action::OpenUrl => "^_",
        Action::Undo => "^U",
        Action::Redo => "^R",
        Action::ShowScores => "^B",
        // Typing and the editor ask for these, not a chord.
        Action::Type(_) | Action::Save { .. } => "",
    }
}

/// The keys that do something where the cursor is, for the second footer row. Escape is always
/// last, and where the session has no way back for it to take, two of them quit.
fn footer(hints: &[Hint]) -> String {
    let mut keys: Vec<String> = hints
        .iter()
        .map(|hint| format!("{} {}", key_for(&hint.action), hint.label))
        .collect();
    if hints.last().is_none_or(|h| key_for(&h.action) != "Esc") {
        keys.push("Esc Esc quits".to_owned());
    }
    keys.join(" · ")
}

/// What a key does to the top line, when it is one that edits it or moves along it: the line as
/// it then reads, and where its cursor then is, in characters.
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

/// Where you are: the app, then each note gone into, outermost first, in at most `max`
/// characters. Past three steps the middle ones give way to `…`. Every step gets an equal share
/// of the room, and what a short head does not use goes to the longer ones.
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

fn format_day_of(moment: Option<Moment>) -> String {
    moment.map_or_else(|| "?".to_owned(), |m| time::format_day(m.day()))
}

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/// The dates that the date words on the top line stand for, to show beside it.
fn dates_shown(dates: &[Day]) -> String {
    let dates: Vec<String> = dates
        .iter()
        .map(|&day| format!("{} {}", WEEKDAYS[time::weekday(day)], time::format_day(day)))
        .collect();
    if dates.is_empty() {
        String::new()
    } else {
        format!("  → {}", dates.join(", "))
    }
}

/// The first footer row, in at most `width` characters: what just happened, how many notes
/// there are, and how sync is going when there is a relay to sync with. What just happened is
/// the part that gives way when there is not room, so the rest is always there to be read.
fn status_row(status: &str, notes: usize, synced: &str, width: usize) -> String {
    let notes = match notes {
        1 => "1 note".to_owned(),
        n => format!("{n} notes"),
    };
    let standing = if synced.is_empty() {
        notes
    } else {
        format!("{notes} · {synced}")
    };
    if status.is_empty() {
        return standing;
    }
    let room = width.saturating_sub(standing.chars().count() + 3);
    format!("{} · {standing}", fit(status, room))
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
            list: ListState::default(),
            escaped: false,
            sync: None,
            synced: String::new(),
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
        if app.device.view().status.is_empty() {
            let status = match (app.device.unopened(), app.device.view().notes) {
                (0, 0) => {
                    "no notes yet: type and press Enter, or see fzn --help to import".to_owned()
                }
                (0, _) => dir.display().to_string(),
                (shut, _) => format!("{shut} lines here that this passphrase does not open"),
            };
            app.device.say(status);
        }
        Ok(app)
    }

    /// Syncs with a relay over a link to the thread that holds the socket. `relay` is how the
    /// status row names it.
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
                    Flow::Edit { id, text } => self.edit(terminal, id, &text)?,
                    Flow::Quit => return Ok(()),
                }
            }
        }
    }

    fn handle(&mut self, key: KeyEvent) -> Result<Flow> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let escaped = std::mem::take(&mut self.escaped);
        let (on_top, zoomed, line) = {
            let view = self.device.view();
            (
                view.cursor.is_none(),
                !view.path.is_empty(),
                view.line.to_owned(),
            )
        };
        if let Some(action) = bound(key.code, ctrl, on_top, zoomed) {
            return self.act(action);
        }
        match (key.code, ctrl) {
            (KeyCode::Esc, _) if escaped => return Ok(Flow::Quit),
            (KeyCode::Esc, _) => {
                self.escaped = true;
                self.device.say("Escape again to quit");
            }
            (KeyCode::Char('q'), true) => return Ok(Flow::Quit),
            (code, _) => match edited(&line, self.cursor, code, ctrl) {
                // Typing always goes to the top line, so the device is told what it now reads.
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

    /// Asks the device for something, and carries out what it asks for in return.
    fn act(&mut self, action: Action) -> Result<Flow> {
        let line = self.device.view().line.to_owned();
        let todo = self.device.act(action);
        let flow = self.carry_out(todo)?;
        // A line the device changed, as it does on the way into a note and out of one, has its
        // cursor at the end.
        let now = self.device.view().line;
        if now != line {
            self.cursor = now.chars().count();
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
                // A thread that has gone is a relay that is not there, which the row says.
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
        Ok(match todo.edit {
            Some(Editing { id, text }) => Flow::Edit { id, text },
            None => Flow::Continue,
        })
    }

    /// Takes in what the thread holding the socket has said since it was last asked, and says
    /// whether there was anything. How sync is going stays on the status row; why a relay
    /// cannot be reached is said once.
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
                    self.synced = format!("syncing with {relay}");
                    let todo = self.device.connected();
                    self.carry_out(todo)?;
                }
                sync::Event::Message(text) => {
                    let todo = self.device.received(text);
                    self.carry_out(todo)?;
                }
                sync::Event::Disconnected => {
                    self.synced = "not syncing".to_owned();
                    self.device.disconnected();
                }
                sync::Event::Unreachable(why) => {
                    self.synced = "not syncing".to_owned();
                    self.device.say(format!("{relay}: {why}"));
                }
            }
        }
        // Lines from the relay that this key does not open are said as soon as they come, and
        // not only when something is next written.
        if verified && !self.device.key_verified() {
            self.device.say(
                "the passphrase does not open this space: quit, and run fzn key to enter it again",
            );
        }
        Ok(!heard.is_empty())
    }

    /// Gives the terminal over to the editor, and saves what it leaves.
    fn edit(&mut self, terminal: &mut DefaultTerminal, id: String, text: &str) -> Result<()> {
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

    fn draw(&mut self, frame: &mut Frame) {
        let view = self.device.view();
        let [input_area, list_area, preview_area, status_area] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(9),
            Constraint::Length(2),
        ])
        .areas(frame.area());

        let inner_width = usize::from(input_area.width.saturating_sub(4)).max(1);
        let scroll = self.cursor.saturating_sub(inner_width - 1);
        let crumb_max = usize::from(input_area.width.saturating_sub(2)).min(CRUMB_MAX);
        let input = Paragraph::new(Line::from(vec![
            Span::raw("> ").dim(),
            Span::raw(view.line),
            Span::raw(dates_shown(&view.dates)).dim(),
        ]))
        .scroll((0, scroll as u16))
        .block(Block::bordered().title(breadcrumb(&self.title, &view.path, crumb_max)));
        frame.render_widget(input, input_area);
        frame.set_cursor_position((
            input_area.x + 3 + (self.cursor - scroll) as u16,
            input_area.y + 1,
        ));

        let searching = !view.line.trim().is_empty();
        let archived = if view.archived_shown {
            " · archived shown"
        } else {
            ""
        };
        let title = if let Some(head) = view.path.last() {
            let linked = view.rows.iter().filter(|r| r.linked).count();
            let counts = if searching {
                format!("{} matching", view.rows.len())
            } else {
                format!("{linked} linked, {} suggested", view.rows.len() - linked)
            };
            format!(" around {} · {counts}{archived} ", fit(head, 40))
        } else if searching {
            format!(" related · {}{archived} ", view.rows.len())
        } else {
            format!(" recent · {}{archived} ", view.rows.len())
        };
        let width = usize::from(list_area.width.saturating_sub(4));
        let items: Vec<ListItem> = view
            .rows
            .iter()
            .map(|row| {
                let date = row
                    .updated
                    .map_or_else(|| " ".repeat(10), |m| time::format_day(m.day()));
                let score = match (view.scores_shown, row.score) {
                    (true, Some(s)) => format!("{s:.2} "),
                    _ => String::new(),
                };
                let marker = if row.has_body { "▸ " } else { "  " };
                let head_width = width.saturating_sub(date.len() + 1 + score.len() + 2);
                let head = fit(&row.head, head_width);
                // What the note is linked to follows its head, dimmed, where there is room.
                let room = head_width - head.chars().count();
                let links = if row.links.is_empty() || room < 6 {
                    String::new()
                } else {
                    fit(&format!(" → {}", row.links.join(", ")), room)
                };
                let pad = room - links.chars().count();
                let mut lines = vec![Line::from(vec![
                    Span::raw(format!("{score}{marker}{head}")),
                    Span::raw(links).dim(),
                    Span::raw(format!("{:pad$} {date}", "")),
                ])];
                if !row.reasons.is_empty() {
                    lines.push(
                        Line::from(format!(
                            "      {}",
                            fit(&row.reasons.join(" · "), width.saturating_sub(6))
                        ))
                        .dim(),
                    );
                }
                let item = ListItem::new(Text::from(lines));
                if row.archived { item.dim() } else { item }
            })
            .collect();
        let list = List::new(items)
            .block(Block::bordered().title(title))
            .highlight_style(Style::new().reversed())
            .highlight_symbol("▶ ");
        self.list.select(view.cursor);
        frame.render_stateful_widget(list, list_area, &mut self.list);

        let shown = view
            .cursor
            .and_then(|i| view.rows.get(i))
            .or(view.rows.first());
        let (title, body) = match (shown, view.preview) {
            (Some(row), Some(record)) => (
                format!(
                    " {}created {} · updated {}{}{} ",
                    if view.cursor.is_none() {
                        "top of list · "
                    } else {
                        ""
                    },
                    format_day_of(record.created),
                    format_day_of(record.updated),
                    if record.archived { " · archived" } else { "" },
                    if row.links.is_empty() {
                        String::new()
                    } else {
                        format!(" · links {}", fit(&row.links.join(", "), 60))
                    }
                ),
                record.text.as_str(),
            ),
            _ => (" nothing to show ".to_owned(), ""),
        };
        frame.render_widget(
            Paragraph::new(body)
                .wrap(Wrap { trim: false })
                .block(Block::bordered().title(title)),
            preview_area,
        );

        // Two rows, each clipped at the edge: what happened, then the keys that apply.
        let footer = Text::from(vec![
            Line::from(status_row(
                view.status,
                view.notes,
                &self.synced,
                usize::from(status_area.width),
            )),
            Line::from(footer(&view.hints)).dim(),
        ]);
        frame.render_widget(Paragraph::new(footer), status_area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::store::tests::temp_dir;
    use fzn_engine::replica::{Header, Message, Out, Replica};
    use fzn_engine::{Record, Seal};
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

    fn write(app: &mut App, text: &str) {
        type_in(app, text);
        press(app, KeyCode::Enter, false);
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
        assert_eq!(reopened.device.view().status, space.display().to_string());
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

        // On a note, Enter goes in. On the line, it writes, and what is written there is linked.
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Enter, false);
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
        press(&mut app, KeyCode::Enter, false);
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
        type_in(&mut app, "x");
        assert!(matches!(
            press(&mut app, KeyCode::Esc, false),
            Flow::Continue
        ));
        assert!(matches!(press(&mut app, KeyCode::Esc, false), Flow::Quit));
        assert!(matches!(chord(&mut app, 'q'), Flow::Quit));
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

        // A line the session puts back has its cursor at the end, wherever it was left.
        write(&mut app, "trainingapp plan");
        type_in(&mut app, "trai");
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Down, false);
        assert!(on_note(&app));
        type_in(&mut app, "n");
        assert!(!on_note(&app), "typing always goes to the top line");
        assert_eq!((line(&app).as_str(), app.cursor), ("trani", 4));
        chord(&mut app, 'd');
        type_in(&mut app, "trai");
        press(&mut app, KeyCode::Left, false);
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Enter, false);
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
        let (on_top, zoomed) = (view.cursor.is_none(), !view.path.is_empty());
        for hint in &view.hints {
            let (code, ctrl) = named(key_for(&hint.action));
            assert_eq!(
                bound(code, ctrl, on_top, zoomed).as_ref(),
                Some(&hint.action),
                "{} {}",
                key_for(&hint.action),
                hint.label
            );
        }
        footer(&view.hints)
    }

    #[test]
    fn the_footer_names_the_key_for_each_thing_on_offer() {
        let (mut app, space) = app();
        assert_eq!(
            keys(&app),
            "Enter writes · ^V show archived · Esc Esc quits",
            "nothing listed, so nothing to pick"
        );
        write(&mut app, "trainingapp");
        assert_eq!(
            keys(&app),
            "Enter writes · ↓ picks a note · ^V show archived · ^U undo · Esc Esc quits"
        );
        press(&mut app, KeyCode::Down, false);
        assert_eq!(
            keys(&app),
            "Enter opens · ^O edit · ^V archive · ^D delete · ^C copy · ^U undo · Esc up"
        );

        press(&mut app, KeyCode::Enter, false);
        write(&mut app, "squat form video https://example.com/squat");
        assert_eq!(
            keys(&app),
            "Enter writes here · ↓ picks a note · ^V show archived · ^U undo · Esc back"
        );
        press(&mut app, KeyCode::Down, false);
        assert_eq!(
            keys(&app),
            "Enter opens · ^L unlink · ^O edit · ^V archive · ^D delete · ^C copy · ^_ open URL \
             · ^U undo · Esc up"
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
            "Enter opens · ^L unlink · ^O edit · ^V unarchive · ^D delete · ^C copy \
             · ^_ open URL · ^U undo · ^R redo · Esc up"
        );
        assert!(widest.chars().count() <= 113, "{}", widest.chars().count());
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(
            keys(&app),
            "Enter writes here · ↓ picks a note · ^V hide archived · ^U undo · ^R redo · Esc back"
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
        assert_eq!(app.synced, "syncing with nas.local");
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
        assert_eq!(app.synced, "not syncing");
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
            app.device.view().status,
            "the passphrase does not open this space: quit, and run fzn key to enter it again"
        );
        write(&mut app, "a garden");
        assert!(
            app.device.view().status.contains("passphrase"),
            "{}",
            app.device.view().status
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
            "on the top line there is no note to edit"
        );
        press(&mut app, KeyCode::Down, false);
        let Flow::Edit { id, text } = chord(&mut app, 'o') else {
            panic!("the note under the cursor was not handed over");
        };
        assert_eq!(text, "the plan");

        // An editor that changes nothing, and one that gives up.
        assert_eq!(edit_in("true", &id, &text).unwrap(), Ok("the plan".into()));
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
        assert_eq!(view.preview.unwrap().text, "The plan\nwith a body");
        assert!(view.rows[0].has_body);
        let written = fs::read_to_string(space.join(format!("{WRITER}.jsonl"))).unwrap();
        assert_eq!(written.lines().count(), 2);
        done(&space);
    }

    #[test]
    fn the_status_row_says_how_sync_is_going_when_there_is_a_relay() {
        assert_eq!(status_row("", 0, "", 80), "0 notes");
        assert_eq!(status_row("saved", 1, "", 80), "saved · 1 note");
        assert_eq!(
            status_row("saved", 4, "syncing with nas.local", 80),
            "saved · 4 notes · syncing with nas.local"
        );
        // What just happened gives way, so that the count and the sync are always there.
        let long = "/a/long/path/to/where/the/notes/are/kept/on/this/machine/main";
        let row = status_row(long, 152, "not syncing", 50);
        assert_eq!(row, "/a/long/path/to/where/t… · 152 notes · not syncing");
        assert_eq!(row.chars().count(), 50);

        let (mut app, space) = app();
        assert!(
            !app.hear().unwrap(),
            "with no relay there is nothing to hear"
        );
        let (_sent, tell) = link(&mut app);
        assert!(!app.hear().unwrap(), "nor until something is said");

        tell.send(sync::Event::Connected).unwrap();
        assert!(app.hear().unwrap());
        assert_eq!(app.synced, "syncing with nas.local");
        tell.send(sync::Event::Disconnected).unwrap();
        tell.send(sync::Event::Unreachable("connection refused".into()))
            .unwrap();
        assert!(app.hear().unwrap());
        assert_eq!(app.synced, "not syncing");
        assert_eq!(
            app.device.view().status,
            "nas.local: connection refused",
            "why is said once, and the row goes on saying that it is not"
        );
        tell.send(sync::Event::Connected).unwrap();
        tell.send(sync::Event::Message(r#"{"forked":"dev-b"}"#.into()))
            .unwrap();
        assert!(app.hear().unwrap());
        assert_eq!(
            app.synced, "syncing with nas.local",
            "every other log goes on"
        );
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
    fn date_words_are_shown_as_the_days_they_stand_for() {
        assert_eq!(dates_shown(&[]), "");
        assert_eq!(
            dates_shown(&[
                time::day_from_civil(2026, 10, 16),
                time::day_from_civil(2026, 10, 14)
            ]),
            "  → Fri 2026-10-16, Wed 2026-10-14"
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
