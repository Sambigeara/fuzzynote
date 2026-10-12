//! The terminal face: one input, one list, one preview, with the original fzn keys.
//!
//! The input is the top line. Below it the list shows recent notes while the input is empty and
//! what the engine finds as you type, each with its reasons. Down moves onto the list, Up and
//! Escape come back to the top line, and typing always goes to the top line. A chord acts on the
//! top line when the cursor is there and on the selected note otherwise, as it did in fzn.
//! Notes are edited in your editor.

use std::collections::HashSet;
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, ExitStatus};

use anyhow::{Context as _, Result};
use fzn_engine::{Engine, Note, NoteId, SearchOptions, text, time};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use crate::store::{Record, Store, new_id, now_secs};

const RECENT_LIMIT: usize = 500;
const RELATED_LIMIT: usize = 60;
const AROUND_LIMIT: usize = 200;
const KEYS: &str = "Enter new, zoom · ^D delete · ^O edit · ^V archive · ^U undo · ^R redo · ^P link · ^C copy · ^_ open URL · ^B scores · Esc back, twice quits";

/// A note being looked around. The list shows its neighbourhood.
struct Pivot {
    id: String,
    head: String,
    /// The input as it was when the note was entered, restored on the way back out.
    input: String,
}

struct Row {
    id: NoteId,
    head: String,
    archived: bool,
    has_body: bool,
    reasons: String,
    score: Option<f32>,
    updated: Option<i64>,
}

pub struct App {
    engine: Engine,
    store: Store,
    editor: String,
    input: String,
    /// Cursor position in the input, in characters.
    cursor: usize,
    rows: Vec<Row>,
    list: ListState,
    show_archived: bool,
    show_scores: bool,
    status: String,
    /// Notes being looked around, innermost last.
    pivots: Vec<Pivot>,
    /// Whether the previous key was Escape, which makes a second one quit.
    escaped: bool,
    /// Versions to restore, newest last: what a note was before a change, and what it became.
    undo: Vec<(Option<Record>, Record)>,
    redo: Vec<(Option<Record>, Record)>,
}

enum Flow {
    Continue,
    Quit,
}

/// The engine's view of a stored note.
pub fn note_from(record: &Record) -> Note {
    let day = |secs: Option<i64>| secs.map(time::day_from_unix_secs);
    Note {
        id: NoteId::new(record.id.clone()),
        text: record.text.clone(),
        created: day(record.created),
        updated: day(record.updated),
        archived: record.archived,
    }
}

fn head_of(text: &str) -> &str {
    text.lines().next().unwrap_or("").trim()
}

fn has_body(text: &str) -> bool {
    text.split_once('\n')
        .is_some_and(|(_, body)| !body.trim().is_empty())
}

/// Truncates to `max` characters, marking the cut.
fn fit(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_owned();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    if max > 0 {
        out.push('…');
    }
    out
}

fn byte_index(s: &str, char_index: usize) -> usize {
    s.char_indices().nth(char_index).map_or(s.len(), |(b, _)| b)
}

/// Appends `[[head]]` to the input unless it is already there.
fn add_link(input: &str, head: &str) -> Option<String> {
    let link = format!("[[{head}]]");
    if input.to_lowercase().contains(&link.to_lowercase()) {
        return None;
    }
    let trimmed = input.trim_end();
    Some(if trimmed.is_empty() {
        link
    } else {
        format!("{trimmed} {link}")
    })
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

fn run_editor(editor: &str, path: &Path) -> Result<ExitStatus> {
    let mut parts = editor.split_whitespace();
    let program = parts.next().context("empty editor command")?;
    Command::new(program)
        .args(parts)
        .arg(path)
        .status()
        .with_context(|| format!("run editor {editor:?}"))
}

fn format_secs(secs: Option<i64>) -> String {
    secs.map_or_else(
        || "?".to_owned(),
        |s| time::format_day(time::day_from_unix_secs(s)),
    )
}

impl App {
    pub fn new(engine: Engine, store: Store, editor: String, status: String) -> Self {
        let mut app = Self {
            engine,
            store,
            editor,
            input: String::new(),
            cursor: 0,
            rows: Vec::new(),
            list: ListState::default(),
            show_archived: false,
            show_scores: false,
            status,
            pivots: Vec::new(),
            escaped: false,
            undo: Vec::new(),
            redo: Vec::new(),
        };
        app.refresh(true);
        app
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
            if let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
                && matches!(self.handle(key, terminal)?, Flow::Quit)
            {
                return Ok(());
            }
        }
    }

    fn handle(&mut self, key: KeyEvent, terminal: &mut DefaultTerminal) -> Result<Flow> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let escaped = std::mem::take(&mut self.escaped);
        let on_top = self.list.selected().is_none();
        let end = self.input.chars().count();
        match (key.code, ctrl) {
            (KeyCode::Esc, _) => {
                if !on_top {
                    self.list.select(None);
                } else if !self.pivots.is_empty() {
                    self.back();
                } else if escaped {
                    return Ok(Flow::Quit);
                } else {
                    self.escaped = true;
                    self.status = "Escape again to quit".into();
                }
            }
            (KeyCode::Char('q'), true) => return Ok(Flow::Quit),
            (KeyCode::Enter, _) => {
                if on_top {
                    self.create()?;
                } else {
                    self.look_around();
                }
            }
            (KeyCode::Char('d'), true) => {
                if on_top {
                    self.set_input(String::new());
                } else {
                    self.delete()?;
                }
            }
            (KeyCode::Char('v'), true) => {
                if on_top {
                    self.show_archived = !self.show_archived;
                    self.refresh(false);
                } else {
                    self.toggle_archive()?;
                }
            }
            (KeyCode::Char('o'), true) => self.edit(terminal)?,
            (KeyCode::Char('u'), true) => self.undo()?,
            (KeyCode::Char('r'), true) => self.redo()?,
            (KeyCode::Char('p'), true) => self.link(),
            (KeyCode::Char('c'), true) => self.copy(),
            (KeyCode::Char('_' | '/'), true) => self.open_url(),
            (KeyCode::Char('b'), true) => self.show_scores = !self.show_scores,
            (KeyCode::Up, _) => self.step(-1),
            (KeyCode::Down, _) => self.step(1),
            (KeyCode::PageUp, _) => self.step(-10),
            (KeyCode::PageDown, _) => self.step(10),
            (KeyCode::Char('a'), true) | (KeyCode::Home, _) => self.cursor = 0,
            (KeyCode::Char('e'), true) | (KeyCode::End, _) => self.cursor = end,
            (KeyCode::Char('w'), true) => self.delete_word(),
            (KeyCode::Left, _) => self.cursor = self.cursor.saturating_sub(1),
            (KeyCode::Right, _) => self.cursor = (self.cursor + 1).min(end),
            (KeyCode::Backspace, _) => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.remove_char(self.cursor);
                }
            }
            (KeyCode::Delete, _) => {
                if self.cursor < end {
                    self.remove_char(self.cursor);
                }
            }
            (KeyCode::Tab, _) => self.insert_char(' '),
            (KeyCode::Char(c), false) => self.insert_char(c),
            _ => {}
        }
        Ok(Flow::Continue)
    }

    /// Zooms on the selected note: the list becomes its neighbourhood.
    fn look_around(&mut self) {
        let Some(row) = self.selected() else {
            return;
        };
        if self.pivots.last().is_some_and(|p| p.id == row.id.as_str()) {
            return;
        }
        let pivot = Pivot {
            id: row.id.as_str().to_owned(),
            head: row.head.clone(),
            input: std::mem::take(&mut self.input),
        };
        self.pivots.push(pivot);
        self.set_input(String::new());
    }

    /// Steps back out to where the current context was entered from.
    fn back(&mut self) {
        if let Some(p) = self.pivots.pop() {
            self.set_input(p.input);
            self.reselect(&p.id);
        }
    }

    fn set_input(&mut self, input: String) {
        self.cursor = input.chars().count();
        self.input = input;
        self.refresh(true);
    }

    fn insert_char(&mut self, c: char) {
        let at = byte_index(&self.input, self.cursor);
        self.input.insert(at, c);
        self.cursor += 1;
        self.refresh(true);
    }

    fn remove_char(&mut self, char_index: usize) {
        let at = byte_index(&self.input, char_index);
        self.input.remove(at);
        self.refresh(true);
    }

    fn delete_word(&mut self) {
        let chars: Vec<char> = self.input.chars().collect();
        let mut start = self.cursor;
        while start > 0 && chars[start - 1].is_whitespace() {
            start -= 1;
        }
        while start > 0 && !chars[start - 1].is_whitespace() {
            start -= 1;
        }
        let kept: String = chars[..start].iter().chain(&chars[self.cursor..]).collect();
        self.input = kept;
        self.cursor = start;
        self.refresh(true);
    }

    /// Recomputes the list from the input and the context. `reset` moves the selection back to
    /// the top.
    fn refresh(&mut self, reset: bool) {
        let query = self.input.trim();
        let rows = match (self.pivots.last(), query.is_empty()) {
            (Some(p), _) => self.around(&p.id, query),
            (None, true) => self.recent(),
            (None, false) => self.related(query),
        };
        self.rows = rows;
        let selected = if reset { None } else { self.list.selected() };
        self.list.select(
            selected
                .filter(|_| !self.rows.is_empty())
                .map(|i| i.min(self.rows.len() - 1)),
        );
    }

    fn recent(&self) -> Vec<Row> {
        let mut records: Vec<&Record> = self
            .store
            .records()
            .filter(|r| self.show_archived || !r.archived)
            .collect();
        records.sort_by(|a, b| b.updated.cmp(&a.updated).then_with(|| a.id.cmp(&b.id)));
        records
            .into_iter()
            .take(RECENT_LIMIT)
            .map(|r| Row {
                id: NoteId::new(r.id.clone()),
                head: head_of(&r.text).to_owned(),
                archived: r.archived,
                has_body: has_body(&r.text),
                reasons: String::new(),
                score: None,
                updated: r.updated,
            })
            .collect()
    }

    fn related(&self, query: &str) -> Vec<Row> {
        let opts = SearchOptions {
            limit: RELATED_LIMIT,
            include_archived: true,
            today: Some(time::day_from_unix_secs(now_secs())),
        };
        self.engine
            .search(query, &opts)
            .into_iter()
            .map(|hit| self.row(&hit))
            .collect()
    }

    /// The neighbourhood of one note, narrowed by the input when there is one.
    fn around(&self, id: &str, query: &str) -> Vec<Row> {
        let opts = SearchOptions {
            limit: AROUND_LIMIT,
            include_archived: true,
            today: None,
        };
        let near = self.engine.related(&NoteId::new(id.to_owned()), &opts);
        if query.is_empty() {
            return near.iter().map(|hit| self.row(hit)).collect();
        }
        let ids: HashSet<&str> = near.iter().map(|hit| hit.note.id.as_str()).collect();
        let opts = SearchOptions {
            limit: RECENT_LIMIT,
            include_archived: true,
            today: Some(time::day_from_unix_secs(now_secs())),
        };
        self.engine
            .search(query, &opts)
            .iter()
            .filter(|hit| ids.contains(hit.note.id.as_str()))
            .map(|hit| self.row(hit))
            .collect()
    }

    fn row(&self, hit: &fzn_engine::Hit<'_>) -> Row {
        Row {
            id: hit.note.id.clone(),
            head: hit.note.head().to_owned(),
            archived: hit.note.archived,
            has_body: has_body(&hit.note.text),
            reasons: hit
                .reasons
                .iter()
                .map(|r| format!("{}: {}", r.scorer, r.text))
                .collect::<Vec<_>>()
                .join(" · "),
            score: Some(hit.score),
            updated: self.store.get(hit.note.id.as_str()).and_then(|r| r.updated),
        }
    }

    /// Moves the cursor between the top line (no selection) and the list.
    fn step(&mut self, delta: i64) {
        if self.rows.is_empty() {
            self.list.select(None);
            return;
        }
        let next = match self.list.selected() {
            None if delta > 0 => Some((delta - 1).min(self.rows.len() as i64 - 1)),
            None => None,
            Some(i) => {
                let n = i as i64 + delta;
                (n >= 0).then(|| n.min(self.rows.len() as i64 - 1))
            }
        };
        self.list.select(next.map(|n| n as usize));
    }

    fn selected(&self) -> Option<&Row> {
        self.list.selected().and_then(|i| self.rows.get(i))
    }

    fn reselect(&mut self, id: &str) {
        if let Some(i) = self.rows.iter().position(|r| r.id.as_str() == id) {
            self.list.select(Some(i));
        }
    }

    /// Writes a version to the store and the engine, remembering what it replaced for undo.
    fn commit(&mut self, record: Record) -> Result<()> {
        let before = self.store.version(record.id.as_str()).cloned();
        self.write(&record)?;
        self.undo.push((before, record));
        self.redo.clear();
        Ok(())
    }

    fn write(&mut self, record: &Record) -> Result<()> {
        if record.deleted {
            self.engine.remove(&NoteId::new(record.id.clone()));
        } else {
            self.engine.upsert(note_from(record));
        }
        self.store.put(record.clone())
    }

    /// Puts a note back to a version, stamped now so that it wins across devices.
    fn restore(&mut self, version: Option<&Record>, fallback: &Record) -> Result<String> {
        let record = match version {
            Some(v) => Record {
                updated: Some(now_secs()),
                ..v.clone()
            },
            None => Record {
                deleted: true,
                updated: Some(now_secs()),
                ..fallback.clone()
            },
        };
        let id = record.id.clone();
        self.write(&record)?;
        self.refresh(false);
        self.reselect(&id);
        Ok(id)
    }

    fn undo(&mut self) -> Result<()> {
        let Some((before, after)) = self.undo.pop() else {
            self.status = "nothing to undo".into();
            return Ok(());
        };
        self.restore(before.as_ref(), &after)?;
        self.status = format!("undid: {}", fit(head_of(&after.text), 40));
        self.redo.push((before, after));
        Ok(())
    }

    fn redo(&mut self) -> Result<()> {
        let Some((before, after)) = self.redo.pop() else {
            self.status = "nothing to redo".into();
            return Ok(());
        };
        self.restore(Some(&after), &after)?;
        self.status = format!("redid: {}", fit(head_of(&after.text), 40));
        self.undo.push((before, after));
        Ok(())
    }

    fn create(&mut self) -> Result<()> {
        let mut text = self.input.trim().to_owned();
        if text.is_empty() {
            return Ok(());
        }
        if let Some(p) = self.pivots.last()
            && let Some(linked) = add_link(&text, &p.head)
        {
            text = linked;
        }
        let now = now_secs();
        self.commit(Record {
            id: new_id(),
            text,
            created: Some(now),
            updated: Some(now),
            archived: false,
            deleted: false,
            device: String::new(),
        })?;
        self.set_input(String::new());
        self.status = "created".into();
        Ok(())
    }

    fn edit(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        let Some(id) = self.selected().map(|r| r.id.as_str().to_owned()) else {
            return Ok(());
        };
        let Some(record) = self.store.get(&id).cloned() else {
            return Ok(());
        };
        let path = std::env::temp_dir().join(format!("fzn-{}.md", sanitise(&id)));
        fs::write(&path, &record.text)?;

        ratatui::restore();
        let status = run_editor(&self.editor, &path);
        *terminal = ratatui::init();
        terminal.clear()?;

        let status = status?;
        if !status.success() {
            self.status = format!("{} exited with {status}; note unchanged", self.editor);
            return Ok(());
        }
        let edited = fs::read_to_string(&path)?;
        let _ = fs::remove_file(&path);
        let edited = edited.trim_end_matches(['\n', '\r']).to_owned();
        if edited == record.text {
            self.status = "unchanged".into();
        } else if edited.trim().is_empty() {
            self.status = "empty, so left unchanged; ^V archives".into();
        } else {
            self.commit(Record {
                text: edited,
                updated: Some(now_secs()),
                ..record
            })?;
            self.status = "saved".into();
            self.refresh(false);
            self.reselect(&id);
        }
        Ok(())
    }

    fn toggle_archive(&mut self) -> Result<()> {
        let Some(id) = self.selected().map(|r| r.id.as_str().to_owned()) else {
            return Ok(());
        };
        let Some(record) = self.store.get(&id).cloned() else {
            return Ok(());
        };
        let archived = !record.archived;
        self.commit(Record {
            archived,
            updated: Some(now_secs()),
            ..record
        })?;
        self.status = if archived { "archived" } else { "unarchived" }.into();
        self.refresh(false);
        self.reselect(&id);
        Ok(())
    }

    fn delete(&mut self) -> Result<()> {
        let Some(id) = self.selected().map(|r| r.id.as_str().to_owned()) else {
            return Ok(());
        };
        let Some(record) = self.store.get(&id).cloned() else {
            return Ok(());
        };
        self.commit(Record {
            deleted: true,
            updated: Some(now_secs()),
            ..record
        })?;
        self.status = "deleted · ^U undoes".into();
        self.refresh(false);
        Ok(())
    }

    /// Copies the selected note's first URL, or its head, to the clipboard via OSC 52.
    fn copy(&mut self) {
        let Some(record) = self.selected().and_then(|r| self.store.get(r.id.as_str())) else {
            return;
        };
        let (what, payload) = match text::facts(&record.text).urls.first() {
            Some(url) => ("URL", url.clone()),
            None => ("head", head_of(&record.text).to_owned()),
        };
        let mut out = std::io::stdout();
        let ok = write!(out, "\x1b]52;c;{}\x07", base64(payload.as_bytes())).is_ok()
            && out.flush().is_ok();
        self.status = if ok {
            format!("copied {what}: {}", fit(&payload, 50))
        } else {
            "could not write to the terminal".into()
        };
    }

    fn open_url(&mut self) {
        let Some(record) = self.selected().and_then(|r| self.store.get(r.id.as_str())) else {
            return;
        };
        let Some(url) = text::facts(&record.text).urls.first().cloned() else {
            self.status = "no URL in that note".into();
            return;
        };
        let opener = if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        };
        self.status = match Command::new(opener)
            .arg(&url)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(_) => format!("opened {}", fit(&url, 60)),
            Err(e) => format!("{opener}: {e}"),
        };
    }

    fn link(&mut self) {
        let Some(head) = self
            .selected()
            .map(|r| r.head.clone())
            .filter(|h| !h.is_empty())
        else {
            return;
        };
        match add_link(&self.input, &head) {
            Some(input) => {
                self.set_input(input);
                self.status = format!("linked [[{head}]]");
            }
            None => self.status = "already linked".into(),
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        let [input_area, list_area, preview_area, status_area] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(9),
            Constraint::Length(1),
        ])
        .areas(frame.area());

        let inner_width = usize::from(input_area.width.saturating_sub(4)).max(1);
        let scroll = self.cursor.saturating_sub(inner_width - 1);
        let input = Paragraph::new(Line::from(vec![
            Span::raw("> ").dim(),
            Span::raw(self.input.as_str()),
        ]))
        .scroll((0, scroll as u16))
        .block(Block::bordered().title(match self.pivots.last() {
            Some(p) => format!(" fzn · new notes link [[{}]] ", p.head),
            None => " fzn ".to_owned(),
        }));
        frame.render_widget(input, input_area);
        frame.set_cursor_position((
            input_area.x + 3 + (self.cursor - scroll) as u16,
            input_area.y + 1,
        ));

        let searching = !self.input.trim().is_empty();
        let title = if let Some(p) = self.pivots.last() {
            format!(
                " around {} · {}{} ",
                fit(&p.head, 40),
                self.rows.len(),
                if searching { " matching" } else { "" }
            )
        } else if searching {
            format!(" related · {} ", self.rows.len())
        } else {
            format!(
                " recent · {}{} ",
                self.rows.len(),
                if self.show_archived {
                    " · archived shown"
                } else {
                    ""
                }
            )
        };
        let width = usize::from(list_area.width.saturating_sub(4));
        let items: Vec<ListItem> = self
            .rows
            .iter()
            .map(|row| {
                let date = row.updated.map_or_else(
                    || " ".repeat(10),
                    |s| time::format_day(time::day_from_unix_secs(s)),
                );
                let score = match (self.show_scores, row.score) {
                    (true, Some(s)) => format!("{s:.2} "),
                    _ => String::new(),
                };
                let marker = if row.has_body { "▸ " } else { "  " };
                let head_width = width.saturating_sub(date.len() + 1 + score.len() + 2);
                let head = fit(&row.head, head_width);
                let mut lines = vec![Line::from(format!(
                    "{score}{marker}{head:<head_width$} {date}"
                ))];
                if !row.reasons.is_empty() {
                    lines.push(
                        Line::from(format!(
                            "      {}",
                            fit(&row.reasons, width.saturating_sub(6))
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
        frame.render_stateful_widget(list, list_area, &mut self.list);

        let shown = self.selected().or(self.rows.first());
        let (title, body) = match shown.and_then(|r| self.store.get(r.id.as_str())) {
            Some(record) => (
                format!(
                    " {}created {} · updated {}{} ",
                    if self.selected().is_none() {
                        "top of list · "
                    } else {
                        ""
                    },
                    format_secs(record.created),
                    format_secs(record.updated),
                    if record.archived { " · archived" } else { "" }
                ),
                record.text.clone(),
            ),
            None => (" nothing to show ".to_owned(), String::new()),
        };
        frame.render_widget(
            Paragraph::new(body)
                .wrap(Wrap { trim: false })
                .block(Block::bordered().title(title)),
            preview_area,
        );

        let status = Line::from(vec![
            Span::raw(format!("{} · {} notes · ", self.status, self.store.len())),
            Span::raw(KEYS).dim(),
        ]);
        frame.render_widget(Paragraph::new(status), status_area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn links_are_appended_once() {
        assert_eq!(
            add_link("", "Kettlebells").as_deref(),
            Some("[[Kettlebells]]")
        );
        assert_eq!(
            add_link("swings today  ", "Kettlebells").as_deref(),
            Some("swings today [[Kettlebells]]")
        );
        assert_eq!(add_link("see [[kettlebells]]", "Kettlebells"), None);
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
    fn fit_truncates_by_characters() {
        assert_eq!(fit("héllo", 10), "héllo");
        assert_eq!(fit("héllo world", 5), "héll…");
        assert_eq!(fit("x", 0), "");
    }

    #[test]
    fn byte_index_handles_multibyte() {
        assert_eq!(byte_index("héllo", 2), 3);
        assert_eq!(byte_index("héllo", 99), 6);
    }

    #[test]
    fn heads_and_bodies() {
        assert_eq!(head_of("  a head \nbody"), "a head");
        assert!(has_body("head\nbody"));
        assert!(!has_body("head\n  \n"));
        assert!(!has_body("head"));
    }
}
