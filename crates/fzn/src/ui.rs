//! The terminal face: one input, one list, one preview. Modal, in the vim sense.
//!
//! Insert mode is typing: the list shows recent notes while the input is empty and what the
//! engine finds as you type, each with its reasons; Enter creates a note from the input. Normal
//! mode moves and acts: pick, look around a note (its neighbourhood: links in and out, shared
//! tags and people, shared words), edit, archive, delete. Notes are edited in your editor.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::process::{Command, ExitStatus};

use anyhow::{Context as _, Result};
use fzn_engine::{Engine, Note, NoteId, SearchOptions, time};
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
const KEYS_INSERT: &str = "Enter new · Esc normal · ↑↓ pick · Tab look around";
const KEYS_NORMAL: &str = "j/k pick · Enter look around · h back · i type · e edit · y link · x archive · dd delete · ga archived · gs scores · q quit";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Insert,
    Normal,
}

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
    mode: Mode,
    /// Notes being looked around, innermost last.
    pivots: Vec<Pivot>,
    /// The first key of a two-key command such as `dd` or `gg`.
    pending: Option<char>,
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
            mode: Mode::Insert,
            pivots: Vec::new(),
            pending: None,
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
        let pending = self.pending.take();
        let end = self.input.chars().count();
        match (self.mode, key.code, ctrl) {
            (_, KeyCode::Char('c'), true) => return Ok(Flow::Quit),
            (_, KeyCode::Up, _) | (_, KeyCode::Char('p'), true) => self.step(-1),
            (_, KeyCode::Down, _) | (_, KeyCode::Char('n'), true) => self.step(1),
            (_, KeyCode::PageUp, _) => self.step(-10),
            (_, KeyCode::PageDown, _) => self.step(10),
            (_, KeyCode::Tab, _) => self.look_around(),
            (_, KeyCode::BackTab, _) => self.back(),

            (Mode::Insert, KeyCode::Esc, _) => self.mode = Mode::Normal,
            (Mode::Insert, KeyCode::Enter, _) => self.create()?,
            (Mode::Insert, KeyCode::Char('u'), true) => self.set_input(String::new()),
            (Mode::Insert, KeyCode::Char('w'), true) => self.delete_word(),
            (Mode::Insert, KeyCode::Home, _) => self.cursor = 0,
            (Mode::Insert, KeyCode::End, _) => self.cursor = end,
            (Mode::Insert, KeyCode::Left, _) => self.cursor = self.cursor.saturating_sub(1),
            (Mode::Insert, KeyCode::Right, _) => self.cursor = (self.cursor + 1).min(end),
            (Mode::Insert, KeyCode::Backspace, _) => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.remove_char(self.cursor);
                }
            }
            (Mode::Insert, KeyCode::Delete, _) => {
                if self.cursor < end {
                    self.remove_char(self.cursor);
                }
            }
            (Mode::Insert, KeyCode::Char(c), false) => self.insert_char(c),

            (Mode::Normal, KeyCode::Char('q'), false) => return Ok(Flow::Quit),
            (Mode::Normal, KeyCode::Esc, _) => {
                if !self.pivots.is_empty() {
                    self.back();
                } else if !self.input.is_empty() {
                    self.set_input(String::new());
                }
            }
            (Mode::Normal, KeyCode::Char('j'), false) => self.step(1),
            (Mode::Normal, KeyCode::Char('k'), false) => self.step(-1),
            (Mode::Normal, KeyCode::Char('d'), true) => self.step(10),
            (Mode::Normal, KeyCode::Char('u'), true) => self.step(-10),
            (Mode::Normal, KeyCode::Char('G'), false) => self.step(i64::MAX / 2),
            (Mode::Normal, KeyCode::Char('g'), false) if pending == Some('g') => {
                self.step(i64::MIN / 2);
            }
            (Mode::Normal, KeyCode::Char('a'), false) if pending == Some('g') => {
                self.show_archived = !self.show_archived;
                self.refresh(false);
            }
            (Mode::Normal, KeyCode::Char('s'), false) if pending == Some('g') => {
                self.show_scores = !self.show_scores;
            }
            (Mode::Normal, KeyCode::Char('d'), false) if pending == Some('d') => self.delete()?,
            (Mode::Normal, KeyCode::Char('c'), false) if pending == Some('c') => {
                self.set_input(String::new());
                self.mode = Mode::Insert;
            }
            (Mode::Normal, KeyCode::Char(c @ ('g' | 'd' | 'c')), false) => {
                self.pending = Some(c);
                self.status = match c {
                    'd' => "dd deletes the selected note",
                    'c' => "cc clears the input and types",
                    _ => "gg top · ga archived · gs scores",
                }
                .into();
            }
            (Mode::Normal, KeyCode::Char('i'), false) => self.mode = Mode::Insert,
            (Mode::Normal, KeyCode::Char('a'), false) => {
                self.cursor = end;
                self.mode = Mode::Insert;
            }
            (Mode::Normal, KeyCode::Char('/'), false) => {
                self.set_input(String::new());
                self.mode = Mode::Insert;
            }
            (Mode::Normal, KeyCode::Enter | KeyCode::Char('l'), _) => self.look_around(),
            (Mode::Normal, KeyCode::Char('h') | KeyCode::Backspace, _) => self.back(),
            (Mode::Normal, KeyCode::Char('e'), false) => self.edit(terminal)?,
            (Mode::Normal, KeyCode::Char('y'), false) => self.link(),
            (Mode::Normal, KeyCode::Char('x'), false) => self.toggle_archive()?,
            _ => {}
        }
        Ok(Flow::Continue)
    }

    /// Makes the selected note the context: the list becomes its neighbourhood.
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
        let selected = if reset {
            0
        } else {
            self.list.selected().unwrap_or(0)
        };
        self.list
            .select((!self.rows.is_empty()).then(|| selected.min(self.rows.len() - 1)));
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

    fn step(&mut self, delta: i64) {
        if self.rows.is_empty() {
            return;
        }
        let current = self.list.selected().unwrap_or(0) as i64;
        let next = (current + delta).clamp(0, self.rows.len() as i64 - 1);
        self.list.select(Some(next as usize));
    }

    fn selected(&self) -> Option<&Row> {
        self.list.selected().and_then(|i| self.rows.get(i))
    }

    fn reselect(&mut self, id: &str) {
        if let Some(i) = self.rows.iter().position(|r| r.id.as_str() == id) {
            self.list.select(Some(i));
        }
    }

    /// Writes a version to the store and the engine.
    fn commit(&mut self, record: Record) -> Result<()> {
        self.engine.upsert(note_from(&record));
        self.store.put(record)
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
            self.status = "empty, so left unchanged; x archives".into();
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
        self.engine.remove(&NoteId::new(id));
        self.store.put(Record {
            deleted: true,
            updated: Some(now_secs()),
            ..record
        })?;
        self.status = "deleted".into();
        self.refresh(false);
        Ok(())
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
        if self.mode == Mode::Insert {
            frame.set_cursor_position((
                input_area.x + 3 + (self.cursor - scroll) as u16,
                input_area.y + 1,
            ));
        }

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

        let (title, body) = match self.selected().and_then(|r| self.store.get(r.id.as_str())) {
            Some(record) => (
                format!(
                    " created {} · updated {}{} ",
                    format_secs(record.created),
                    format_secs(record.updated),
                    if record.archived { " · archived" } else { "" }
                ),
                record.text.clone(),
            ),
            None => (" nothing selected ".to_owned(), String::new()),
        };
        frame.render_widget(
            Paragraph::new(body)
                .wrap(Wrap { trim: false })
                .block(Block::bordered().title(title)),
            preview_area,
        );

        let (mode, keys) = match self.mode {
            Mode::Insert => ("INSERT", KEYS_INSERT),
            Mode::Normal => ("NORMAL", KEYS_NORMAL),
        };
        let status = Line::from(vec![
            Span::raw(format!(" {mode} ")).bold().reversed(),
            Span::raw(format!(" {} · {} notes · ", self.status, self.store.len())),
            Span::raw(keys).dim(),
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
