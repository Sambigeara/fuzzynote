//! The terminal face: one input, one list, one preview.
//!
//! With the input empty the list shows recent notes. As you type it shows what the engine finds,
//! each with its reasons. Enter creates a note from the input. Notes are edited in your editor.

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
const KEYS: &str = "Enter new · ↑↓ pick · ^O edit · ^V archive · ^D delete · ^L link · ^R archived · ^B scores · Esc clear, twice to quit";

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
    /// A key that needs a second press to act.
    armed: Option<KeyCode>,
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
            armed: None,
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
        let armed = self.armed.take() == Some(key.code);
        match (key.code, ctrl) {
            (KeyCode::Char('c' | 'q'), true) => return Ok(Flow::Quit),
            (KeyCode::Esc, _) => {
                if !self.input.is_empty() {
                    self.set_input(String::new());
                } else if armed {
                    return Ok(Flow::Quit);
                } else {
                    self.armed = Some(KeyCode::Esc);
                    self.status = "Escape again to quit".into();
                }
            }
            (KeyCode::Enter, _) => self.create()?,
            (KeyCode::Char('d'), true) => {
                if armed {
                    self.delete()?;
                } else if self.selected().is_some() {
                    self.armed = Some(key.code);
                    self.status = "Ctrl-D again to delete the selected note".into();
                }
            }
            (KeyCode::Up, _) => self.step(-1),
            (KeyCode::Down, _) => self.step(1),
            (KeyCode::PageUp, _) => self.step(-10),
            (KeyCode::PageDown, _) => self.step(10),
            (KeyCode::Char('o'), true) => self.edit(terminal)?,
            (KeyCode::Char('v'), true) => self.toggle_archive()?,
            (KeyCode::Char('l'), true) => self.link(),
            (KeyCode::Char('r'), true) => {
                self.show_archived = !self.show_archived;
                self.refresh(false);
            }
            (KeyCode::Char('b'), true) => self.show_scores = !self.show_scores,
            (KeyCode::Char('a'), true) | (KeyCode::Home, _) => self.cursor = 0,
            (KeyCode::Char('e'), true) | (KeyCode::End, _) => {
                self.cursor = self.input.chars().count()
            }
            (KeyCode::Char('u'), true) => self.set_input(String::new()),
            (KeyCode::Char('w'), true) => self.delete_word(),
            (KeyCode::Left, _) => self.cursor = self.cursor.saturating_sub(1),
            (KeyCode::Right, _) => self.cursor = (self.cursor + 1).min(self.input.chars().count()),
            (KeyCode::Backspace, _) => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                    self.remove_char(self.cursor);
                }
            }
            (KeyCode::Delete, _) => {
                if self.cursor < self.input.chars().count() {
                    self.remove_char(self.cursor);
                }
            }
            (KeyCode::Char(c), false) => self.insert_char(c),
            _ => {}
        }
        Ok(Flow::Continue)
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

    /// Recomputes the list from the input. `reset` moves the selection back to the top.
    fn refresh(&mut self, reset: bool) {
        let query = self.input.trim();
        let rows = if query.is_empty() {
            self.recent()
        } else {
            self.related(query)
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
            .map(|hit| Row {
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
            })
            .collect()
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
        let text = self.input.trim().to_owned();
        if text.is_empty() {
            return Ok(());
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
        .block(Block::bordered().title(" fzn "));
        frame.render_widget(input, input_area);
        frame.set_cursor_position((
            input_area.x + 3 + (self.cursor - scroll) as u16,
            input_area.y + 1,
        ));

        let searching = !self.input.trim().is_empty();
        let title = if searching {
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
