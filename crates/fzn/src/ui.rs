//! The terminal face: one input, one list, one preview, with the original fzn keys.
//!
//! The session is the app. This is what a terminal adds to it: which key asks for what, editing
//! the top line, painting the view, and carrying out what the session asks for in return, which
//! is keeping versions in this device's log, running your editor, the clipboard and opening a
//! URL.
//!
//! The input is the top line. Down moves onto the list, Up and Escape come back to the top line,
//! and typing always goes to the top line. A chord acts on the top line when the cursor is there
//! and on the selected note otherwise, as it did in fzn: Enter writes or goes in, Ctrl-D clears
//! or deletes, Ctrl-V shows archived notes or archives. Escape steps back out of a note, and
//! outside any note two of them quit. Notes are edited in your editor. The footer shows only the
//! keys that apply where the cursor is.

use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::time::Duration;

use anyhow::{Context as _, Result};
use fzn_engine::text::fit;
use fzn_engine::{Action, Day, Effect, Hint, Moment, Session, time};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use crate::store::Logs;

/// How long the keyboard has to be idle before the logs are checked for news.
const RELOAD_EVERY: Duration = Duration::from_secs(1);
/// The most the breadcrumb on the top line takes up.
const CRUMB_MAX: usize = 70;

pub struct App {
    session: Session,
    logs: Logs,
    editor: String,
    /// What the top line is headed with: the app's name, and the space when it is not the usual one.
    title: String,
    /// Cursor position in the top line, in characters.
    cursor: usize,
    /// Where the list is scrolled to.
    list: ListState,
    /// Whether the previous key was Escape, which makes a second one quit.
    escaped: bool,
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

impl App {
    pub fn new(session: Session, logs: Logs, editor: String, title: String) -> Self {
        Self {
            session,
            logs,
            editor,
            title,
            cursor: 0,
            list: ListState::default(),
            escaped: false,
        }
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
            while !event::poll(RELOAD_EVERY)? {
                if self.reload() {
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
            let view = self.session.view();
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
                self.session.say("Escape again to quit");
            }
            (KeyCode::Char('q'), true) => return Ok(Flow::Quit),
            (code, _) => match edited(&line, self.cursor, code, ctrl) {
                // Typing always goes to the top line, so the session is told what it now reads.
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

    /// Asks the session for something, and carries out what it asks for in return.
    fn act(&mut self, action: Action) -> Result<Flow> {
        let line = self.session.view().line.to_owned();
        let mut flow = Flow::Continue;
        for effect in self.session.act(action) {
            match effect {
                Effect::Keep(record) => self.logs.append(&record)?,
                Effect::Edit { id, text } => flow = Flow::Edit { id, text },
                Effect::Copy(text) => {
                    if !copy(&text) {
                        self.session.say("could not write to the terminal");
                    }
                }
                Effect::Open(url) => {
                    if let Err(why) = open(&url) {
                        self.session.say(why);
                    }
                }
            }
        }
        // A line the session changed, as it does on the way into a note and out of one, has its
        // cursor at the end.
        let now = self.session.view().line;
        if now != line {
            self.cursor = now.chars().count();
        }
        Ok(flow)
    }

    /// Takes in what other devices and other instances have written. Says whether anything
    /// on screen may have changed.
    fn reload(&mut self) -> bool {
        match self.logs.read() {
            Ok(records) => self.session.absorb(records),
            Err(e) => {
                self.session
                    .say(format!("could not reread the notes: {e:#}"));
                true
            }
        }
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
            Err(why) => self.session.say(why),
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame) {
        let view = self.session.view();
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
        let notes = match view.notes {
            1 => "1 note".to_owned(),
            n => format!("{n} notes"),
        };
        let status = if view.status.is_empty() {
            notes
        } else {
            format!("{} · {notes}", view.status)
        };
        let footer = Text::from(vec![
            Line::from(status),
            Line::from(footer(&view.hints)).dim(),
        ]);
        frame.render_widget(Paragraph::new(footer), status_area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::system::{self, System};
    use fzn_engine::{Record, Weights};
    use std::path::PathBuf;

    fn app() -> (App, PathBuf) {
        let dir = std::env::temp_dir().join(format!("fzn-ui-test-{:x}", system::random_u64()));
        let (notes, logs) = Store::open(&dir, "dev").unwrap().into_parts();
        let session = Session::new(Box::new(System), "dev", Weights::default(), notes);
        (App::new(session, logs, "true".into(), "fzn".into()), dir)
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
        let view = app.session.view();
        view.rows.iter().map(|r| r.head.clone()).collect()
    }

    fn line(app: &App) -> String {
        app.session.view().line.to_owned()
    }

    fn path(app: &App) -> Vec<String> {
        let view = app.session.view();
        view.path.iter().map(|&head| head.to_owned()).collect()
    }

    fn on_note(app: &App) -> bool {
        app.session.view().cursor.is_some()
    }

    #[test]
    fn enter_writes_the_line_into_this_devices_log() {
        let (mut app, dir) = app();
        write(&mut app, "kettlebell swings");
        assert_eq!(heads(&app), ["kettlebell swings"]);
        assert_eq!((line(&app).as_str(), app.cursor), ("", 0));
        let written = fs::read_to_string(dir.join("dev.jsonl")).unwrap();
        assert_eq!(written.lines().count(), 1);
        assert!(written.starts_with(r#"{"v":1,"id":""#), "{written}");
        assert!(written.contains(r#""text":"kettlebell swings""#));
        assert!(written.contains(r#""device":"dev""#));

        let reopened = Store::open(&dir, "dev").unwrap();
        assert_eq!(reopened.notes().len(), 1);
        assert_eq!(reopened.skipped(), 0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_chord_acts_on_the_line_or_on_the_note_under_the_cursor() {
        let (mut app, dir) = app();
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
        assert_eq!(app.session.view().notes, 1);
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
        assert!(app.session.view().rows[0].linked);

        // Every change went to the log, and that is all there is to reopening.
        let written = fs::read_to_string(dir.join("dev.jsonl")).unwrap();
        assert_eq!(written.lines().count(), 6);
        let reopened = Store::open(&dir, "dev").unwrap();
        let mut texts: Vec<&str> = reopened
            .notes()
            .records()
            .map(|r| r.text.as_str())
            .collect();
        texts.sort_unstable();
        assert_eq!(texts, ["kettlebell swings", "two sets"]);
        assert!(reopened.notes().records().all(|r| !r.archived));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn escape_comes_back_a_step_at_a_time_and_twice_quits() {
        let (mut app, dir) = app();
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
        assert_eq!(app.session.view().status, "Escape again to quit");
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
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_cursor_follows_the_line() {
        let (mut app, dir) = app();
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
        fs::remove_dir_all(&dir).unwrap();
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
        let view = app.session.view();
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
        let (mut app, dir) = app();
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
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn what_another_instance_writes_shows_up() {
        let (mut app, dir) = app();
        write(&mut app, "mine");
        assert!(!app.reload(), "its own line, read back, is not news");

        let now = system::now();
        let mut other = Logs::open(&dir, "other").unwrap();
        other
            .append(&Record {
                id: "theirs".into(),
                text: "written elsewhere".into(),
                links: Vec::new(),
                created: Some(now),
                updated: Some(now.plus(60)),
                stamp: Some(now),
                archived: false,
                deleted: false,
                device: "other".into(),
            })
            .unwrap();
        assert!(app.reload());
        assert_eq!(heads(&app), ["written elsewhere", "mine"]);
        assert_eq!(app.session.view().status, "1 changed elsewhere");
        assert!(!app.reload(), "and each line once");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_editor_is_handed_the_text_and_what_it_leaves_is_saved() {
        let (mut app, dir) = app();
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
        let left = dir.join("left-by-the-editor.md");
        fs::write(&left, "The plan\nwith a body\n").unwrap();
        let editor = format!("cp {}", left.display());
        let edited = edit_in(&editor, &id, &text).unwrap().unwrap();
        app.act(Action::Save { id, text: edited }).unwrap();
        let view = app.session.view();
        assert_eq!(view.status, "saved");
        assert_eq!(view.preview.unwrap().text, "The plan\nwith a body");
        assert!(view.rows[0].has_body);
        let written = fs::read_to_string(dir.join("dev.jsonl")).unwrap();
        assert_eq!(written.lines().count(), 2);
        fs::remove_dir_all(&dir).unwrap();
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
