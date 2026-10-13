//! The terminal face: one input, one list, one preview, with the original fzn keys.
//!
//! The input is the top line. Below it the list shows recent notes while the input is empty and
//! what the engine finds as you type, each with its reasons. A note is found by its own words and
//! by the head of a note it links to, so the name of a note brings what was written in it, as
//! going into the note does. Down moves onto the list, Up and
//! Escape come back to the top line, and typing always goes to the top line. A chord acts on the
//! top line when the cursor is there and on the selected note otherwise, as it did in fzn.
//! Archived notes stay out of every list until Ctrl-V on the top line asks for them. Notes are
//! edited in your editor. The footer shows only the keys that apply where the cursor is.
//!
//! Enter on a note goes into it, and the title grows by one step: what is linked to or from it
//! comes first, newest on top, and what the engine finds besides follows, best first. Where you
//! are is what you are talking about, so a note written there is linked to it. The line comes
//! with you under one rule: if the note contains what was typed, or its head and the heads of the
//! notes it links to have every word of it, that was a search, so the line clears, and Escape
//! brings it back to an empty line; otherwise it was a draft, and it stays on the line to be
//! written there. Escape never throws away what is on the line. Enter on a note
//! already on the path goes back to it, so the path never repeats. Ctrl-L on a note in a zoom
//! links or unlinks it, as a correction. A `[[head]]` typed into a note is one more way of making
//! a link: saving the note links it to the note so headed. However it was made, a link is the
//! target's id held on the note.
//!
//! A date is plain text, written `2026-10-16`. A word such as `/fri` or `/tomorrow` is a quicker
//! way of typing one: the top line shows the date it stands for and searches with it, and saving
//! a note writes the date in its place, from the top line or from the editor.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::time::Duration;

use anyhow::{Context as _, Result};
use fzn_engine::{Day, Engine, Note, NoteId, SearchOptions, text, time};
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Style, Stylize};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::{DefaultTerminal, Frame};

use crate::store::{Moment, Record, Store, new_id};

const RECENT_LIMIT: usize = 500;
const RELATED_LIMIT: usize = 60;
const AROUND_LIMIT: usize = 200;
/// How long the keyboard has to be idle before the logs are checked for news.
const RELOAD_EVERY: Duration = Duration::from_secs(1);
/// The most the breadcrumb on the top line takes up.
const CRUMB_MAX: usize = 70;

/// A note being looked around. The list shows its neighbourhood.
struct Pivot {
    id: String,
    head: String,
    /// The search the note was found with, put back on the way out if the line is empty by then.
    /// A draft carried in has nothing to put back: it is still on the line.
    restore: Option<String>,
}

struct Row {
    id: NoteId,
    head: String,
    archived: bool,
    has_body: bool,
    /// Whether the note is linked to or from the one being looked around.
    linked: bool,
    /// The heads, as they are now, of the notes this one is linked to by id.
    links: String,
    reasons: String,
    score: Option<f32>,
    updated: Option<Moment>,
}

pub struct App {
    engine: Engine,
    store: Store,
    editor: String,
    /// What the top line is headed with: the app's name, and the space when it is not the usual one.
    title: String,
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
    /// The selected note is to be opened in the editor, which needs the terminal.
    Edit,
    Quit,
}

/// The engine's view of a stored note.
pub fn note_from(record: &Record) -> Note {
    Note {
        id: NoteId::new(record.id.clone()),
        text: record.text.clone(),
        links: record.links.iter().cloned().map(NoteId::new).collect(),
        created: record.created.map(Moment::day),
        updated: record.updated.map(Moment::day),
        archived: record.archived,
    }
}

/// The links a record holds once its text is saved: what it held, what the new text names in
/// brackets, and no longer what only the old text named.
pub fn links_on_save(
    engine: &Engine,
    id: &str,
    old: &str,
    new: &str,
    links: &[String],
) -> Vec<String> {
    let held: Vec<NoteId> = links.iter().cloned().map(NoteId::new).collect();
    engine
        .links_on_save(&NoteId::new(id.to_owned()), old, new, &held)
        .into_iter()
        .map(|l| l.0)
        .collect()
}

fn head_of(text: &str) -> &str {
    text.lines().next().unwrap_or("").trim()
}

/// Whether there is more to read than the head. A line of nothing but tags is a label, not
/// reading.
fn has_body(text: &str) -> bool {
    text.lines()
        .skip(1)
        .any(|line| !line.trim().is_empty() && !text::only_tags(line))
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

fn run_editor(editor: &str, path: &Path) -> Result<ExitStatus> {
    let mut parts = editor.split_whitespace();
    let program = parts.next().context("empty editor command")?;
    Command::new(program)
        .args(parts)
        .arg(path)
        .status()
        .with_context(|| format!("run editor {editor:?}"))
}

fn format_day_of(moment: Option<Moment>) -> String {
    moment.map_or_else(|| "?".to_owned(), |m| time::format_day(m.day()))
}

/// Today, by this machine's clock.
fn today() -> Day {
    Moment::now().day()
}

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/// The dates that the date words on the top line stand for, to show beside it.
fn dates_shown(input: &str, today: Day) -> String {
    let dates: Vec<String> = text::date_words(input, today)
        .iter()
        .map(|w| {
            format!(
                "{} {}",
                WEEKDAYS[time::weekday(w.day)],
                time::format_day(w.day)
            )
        })
        .collect();
    if dates.is_empty() {
        String::new()
    } else {
        format!("  → {}", dates.join(", "))
    }
}

impl App {
    pub fn new(
        engine: Engine,
        store: Store,
        editor: String,
        title: String,
        status: String,
    ) -> Self {
        let mut app = Self {
            engine,
            store,
            editor,
            title,
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
                    Flow::Edit => self.edit(terminal)?,
                    Flow::Quit => return Ok(()),
                }
            }
        }
    }

    fn handle(&mut self, key: KeyEvent) -> Result<Flow> {
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
            (KeyCode::Char('o'), true) => return Ok(Flow::Edit),
            (KeyCode::Char('u'), true) => self.undo()?,
            (KeyCode::Char('r'), true) => self.redo()?,
            (KeyCode::Char('l'), true) => self.link()?,
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

    /// Zooms on the selected note: the list becomes its neighbourhood. If the note contains what
    /// was typed, or is named by it, in its head and in the heads of the notes it links to, that
    /// was a search for it, so the line clears and is kept for the way back. Otherwise it was a
    /// draft, and it comes along to be written here. A note already on the
    /// path is gone back to rather than added again, so the path never repeats.
    fn look_around(&mut self) {
        let Some(row) = self.selected() else {
            return;
        };
        let (id, head) = (row.id.as_str().to_owned(), row.head.clone());
        if let Some(at) = self.pivots.iter().position(|p| p.id == id) {
            self.back_to(at + 1);
            return;
        }
        let typed = text::expand_date_words(self.input.trim(), today());
        let searched = typed.is_empty()
            || self
                .store
                .get(&id)
                .is_some_and(|r| r.text.to_lowercase().contains(&typed.to_lowercase()))
            || self.engine.named_by(&NoteId::new(id.clone()), &typed);
        let restore = searched.then(|| std::mem::take(&mut self.input));
        self.pivots.push(Pivot { id, head, restore });
        if searched {
            self.set_input(String::new());
        } else {
            self.refresh(true);
        }
    }

    /// Steps back out to where the current context was entered from.
    fn back(&mut self) {
        self.back_to(self.pivots.len().saturating_sub(1));
    }

    /// Steps back out to `depth` notes on the path, onto the top line, so that the next chord
    /// acts on the input and not on a note. What is on the line stays as it is; only an empty
    /// line gets back the search that the first note left behind was found with, which is what
    /// the line held at that depth.
    fn back_to(&mut self, depth: usize) {
        if depth >= self.pivots.len() {
            return;
        }
        let left = self.pivots.split_off(depth);
        match left.into_iter().next().and_then(|p| p.restore) {
            Some(line) if self.input.trim().is_empty() => self.set_input(line),
            _ => self.refresh(true),
        }
    }

    /// Takes in what other devices and other instances have written. Says whether anything
    /// on screen may have changed.
    fn reload(&mut self) -> bool {
        let changed = match self.store.sync() {
            Ok(changed) => changed,
            Err(e) => {
                self.status = format!("could not reread the notes: {e:#}");
                return true;
            }
        };
        if changed.is_empty() {
            return false;
        }
        for id in &changed {
            match self.store.get(id) {
                Some(record) => self.engine.upsert(note_from(record)),
                None => {
                    self.engine.remove(&NoteId::new(id.clone()));
                }
            }
        }
        let selected = self.selected().map(|r| r.id.as_str().to_owned());
        self.refresh(false);
        if let Some(id) = selected {
            self.reselect(&id);
        }
        self.status = format!("{} changed elsewhere", changed.len());
        true
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
        // A date word searches as the date it stands for.
        let query = text::expand_date_words(self.input.trim(), today());
        let rows = match (self.pivots.last(), query.is_empty()) {
            (Some(p), _) => self.around(&p.id, &query),
            (None, true) => self.recent(),
            (None, false) => self.related(&query),
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
                linked: false,
                links: self.link_heads(r),
                reasons: String::new(),
                score: None,
                updated: r.updated,
            })
            .collect()
    }

    fn related(&self, query: &str) -> Vec<Row> {
        let opts = SearchOptions {
            limit: RELATED_LIMIT,
            include_archived: self.show_archived,
            today: Some(today()),
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
            include_archived: self.show_archived,
            today: None,
        };
        let near = self.engine.related(&NoteId::new(id.to_owned()), &opts);
        if query.is_empty() {
            // What is linked is a list you keep, so it comes first with the newest on top. What
            // the engine suggests follows in the order it gave, best first.
            let mut rows: Vec<Row> = near.iter().map(|hit| self.row(hit)).collect();
            rows.sort_by(|a, b| match (a.linked, b.linked) {
                (true, true) => b.updated.cmp(&a.updated).then_with(|| b.id.cmp(&a.id)),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            });
            return rows;
        }
        let ids: HashSet<&str> = near.iter().map(|hit| hit.note.id.as_str()).collect();
        let linked: HashSet<&str> = near
            .iter()
            .filter(|hit| hit.linked)
            .map(|hit| hit.note.id.as_str())
            .collect();
        let opts = SearchOptions {
            limit: RECENT_LIMIT,
            include_archived: self.show_archived,
            today: Some(today()),
        };
        self.engine
            .search(query, &opts)
            .iter()
            .filter(|hit| ids.contains(hit.note.id.as_str()))
            .map(|hit| Row {
                linked: linked.contains(hit.note.id.as_str()),
                ..self.row(hit)
            })
            .collect()
    }

    /// The heads, as they are now, of the notes a record is linked to by id. The note being
    /// looked around is left out, since everything on the list is about it.
    fn link_heads(&self, record: &Record) -> String {
        let around = self.pivots.last().map(|p| p.id.as_str());
        record
            .links
            .iter()
            .filter(|id| Some(id.as_str()) != around)
            .filter_map(|id| self.store.get(id))
            .map(|target| head_of(&target.text))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn row(&self, hit: &fzn_engine::Hit<'_>) -> Row {
        let record = self.store.get(hit.note.id.as_str());
        Row {
            id: hit.note.id.clone(),
            head: hit.note.head().to_owned(),
            archived: hit.note.archived,
            has_body: has_body(&hit.note.text),
            linked: hit.linked,
            links: record.map(|r| self.link_heads(r)).unwrap_or_default(),
            reasons: hit
                .reasons
                .iter()
                .map(|r| match r.scorer {
                    // A link is a fact and reads as one, with no scorer to credit. So is a name.
                    "link" | "name" => r.text.clone(),
                    scorer => format!("{scorer}: {}", r.text),
                })
                .collect::<Vec<_>>()
                .join(" · "),
            score: Some(hit.score),
            updated: record.and_then(|r| r.updated),
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
        let after = self.write(record)?;
        self.undo.push((before, after));
        self.redo.clear();
        Ok(())
    }

    /// Stamps a version so that it supersedes the one held, here and on every other device, and
    /// writes it.
    fn write(&mut self, mut record: Record) -> Result<Record> {
        record.stamp = Some(self.store.next_stamp(&record.id));
        if record.deleted {
            self.engine.remove(&NoteId::new(record.id.clone()));
        } else {
            self.engine.upsert(note_from(&record));
        }
        self.store.put(record.clone())?;
        Ok(record)
    }

    /// Puts a note back to a version.
    fn restore(&mut self, version: Option<&Record>, fallback: &Record) -> Result<String> {
        let record = match version {
            Some(v) => v.clone(),
            None => Record {
                deleted: true,
                ..fallback.clone()
            },
        };
        let id = record.id.clone();
        self.write(record)?;
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

    /// Turns the input into a note, linked to the note being looked around and to whatever it
    /// names in brackets, with its date words written out as dates.
    fn create(&mut self) -> Result<()> {
        let text = text::expand_date_words(self.input.trim(), today());
        if text.is_empty() {
            return Ok(());
        }
        let around: Vec<String> = self
            .pivots
            .last()
            .map(|p| p.id.clone())
            .into_iter()
            .collect();
        let id = new_id();
        let links = links_on_save(&self.engine, &id, "", &text, &around);
        let now = Moment::now();
        self.commit(Record {
            id,
            text,
            links,
            created: Some(now),
            updated: Some(now),
            stamp: None,
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
        let edited = text::expand_date_words(edited.trim_end_matches(['\n', '\r']), today());
        if edited == record.text {
            self.status = "unchanged".into();
        } else if edited.trim().is_empty() {
            self.status = "empty, so left unchanged; ^V archives".into();
        } else {
            let links = links_on_save(&self.engine, &id, &record.text, &edited, &record.links);
            self.commit(Record {
                text: edited,
                links,
                updated: Some(Moment::now()),
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
        self.commit(Record { archived, ..record })?;
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

    /// Links the selected note to the one being looked around, or takes that link away. It is a
    /// correction: what is written in a zoom is linked already, and outside one there is nothing
    /// to link to.
    fn link(&mut self) -> Result<()> {
        let Some(around) = self.pivots.last().map(|p| p.id.clone()) else {
            self.status = "go into a note first; what you write there links to it".into();
            return Ok(());
        };
        let Some(row) = self.selected() else {
            self.status =
                "pick a note to link or unlink; what you write here is linked already".into();
            return Ok(());
        };
        let id = row.id.as_str().to_owned();
        let head = fit(&row.head, 40);
        let (Some(note), Some(pivot)) = (
            self.store.get(&id).cloned(),
            self.store.get(&around).cloned(),
        ) else {
            return Ok(());
        };
        let without = |links: &[String], gone: &str| -> Vec<String> {
            links.iter().filter(|l| *l != gone).cloned().collect()
        };
        // A text that still names the other note in brackets links it again when next saved.
        let named = |text: &str, other: &str| {
            let names = self.engine.named_in(text);
            if names.iter().any(|n| n.as_str() == other) {
                " · its [[brackets]] are still in the text, so an edit links it again"
            } else {
                ""
            }
        };
        if note.links.contains(&around) {
            self.status = format!("unlinked {head}{}", named(&note.text, &around));
            self.commit(Record {
                links: without(&note.links, &around),
                ..note
            })?;
        } else if pivot.links.contains(&id) {
            self.status = format!("unlinked {head}{}", named(&pivot.text, &id));
            self.commit(Record {
                links: without(&pivot.links, &id),
                ..pivot
            })?;
        } else {
            let mut links = note.links.clone();
            links.push(around);
            self.commit(Record { links, ..note })?;
            self.status = format!("linked {head}");
        }
        self.refresh(false);
        self.reselect(&id);
        Ok(())
    }

    /// The first footer row: what just happened, and how many notes there are.
    fn status_row(&self) -> String {
        let notes = match self.store.len() {
            1 => "1 note".to_owned(),
            n => format!("{n} notes"),
        };
        if self.status.is_empty() {
            notes
        } else {
            format!("{} · {notes}", self.status)
        }
    }

    /// The keys that do something where the cursor is, for the second footer row. Escape is
    /// always last.
    fn keys(&self) -> String {
        let zoomed = !self.pivots.is_empty();
        let (mut keys, esc) = match self.selected() {
            None => {
                let mut keys = vec![if zoomed {
                    "Enter writes here"
                } else {
                    "Enter writes"
                }];
                if !self.rows.is_empty() {
                    keys.push("↓ picks a note");
                }
                keys.push(if self.show_archived {
                    "^V hide archived"
                } else {
                    "^V show archived"
                });
                (keys, if zoomed { "Esc back" } else { "Esc Esc quits" })
            }
            Some(row) => {
                let mut keys = vec!["Enter opens"];
                if zoomed {
                    keys.push(if row.linked { "^L unlink" } else { "^L link" });
                }
                keys.extend([
                    "^O edit",
                    if row.archived {
                        "^V unarchive"
                    } else {
                        "^V archive"
                    },
                    "^D delete",
                    "^C copy",
                ]);
                // Only the one note under the cursor is read for a URL, once per draw.
                if self
                    .store
                    .get(row.id.as_str())
                    .is_some_and(|r| !text::facts(&r.text).urls.is_empty())
                {
                    keys.push("^_ open URL");
                }
                (keys, "Esc up")
            }
        };
        if !self.undo.is_empty() {
            keys.push("^U undo");
        }
        if !self.redo.is_empty() {
            keys.push("^R redo");
        }
        keys.push(esc);
        keys.join(" · ")
    }

    fn draw(&mut self, frame: &mut Frame) {
        let [input_area, list_area, preview_area, status_area] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(9),
            Constraint::Length(2),
        ])
        .areas(frame.area());

        let inner_width = usize::from(input_area.width.saturating_sub(4)).max(1);
        let scroll = self.cursor.saturating_sub(inner_width - 1);
        let heads: Vec<&str> = self.pivots.iter().map(|p| p.head.as_str()).collect();
        let crumb_max = usize::from(input_area.width.saturating_sub(2)).min(CRUMB_MAX);
        let input = Paragraph::new(Line::from(vec![
            Span::raw("> ").dim(),
            Span::raw(self.input.as_str()),
            Span::raw(dates_shown(&self.input, today())).dim(),
        ]))
        .scroll((0, scroll as u16))
        .block(Block::bordered().title(breadcrumb(&self.title, &heads, crumb_max)));
        frame.render_widget(input, input_area);
        frame.set_cursor_position((
            input_area.x + 3 + (self.cursor - scroll) as u16,
            input_area.y + 1,
        ));

        let searching = !self.input.trim().is_empty();
        let archived = if self.show_archived {
            " · archived shown"
        } else {
            ""
        };
        let title = if let Some(p) = self.pivots.last() {
            let linked = self.rows.iter().filter(|r| r.linked).count();
            let counts = if searching {
                format!("{} matching", self.rows.len())
            } else {
                format!("{linked} linked, {} suggested", self.rows.len() - linked)
            };
            format!(" around {} · {counts}{archived} ", fit(&p.head, 40))
        } else if searching {
            format!(" related · {}{archived} ", self.rows.len())
        } else {
            format!(" recent · {}{archived} ", self.rows.len())
        };
        let width = usize::from(list_area.width.saturating_sub(4));
        let items: Vec<ListItem> = self
            .rows
            .iter()
            .map(|row| {
                let date = row
                    .updated
                    .map_or_else(|| " ".repeat(10), |m| time::format_day(m.day()));
                let score = match (self.show_scores, row.score) {
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
                    fit(&format!(" → {}", row.links), room)
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
                    " {}created {} · updated {}{}{} ",
                    if self.selected().is_none() {
                        "top of list · "
                    } else {
                        ""
                    },
                    format_day_of(record.created),
                    format_day_of(record.updated),
                    if record.archived { " · archived" } else { "" },
                    match shown.map(|r| r.links.as_str()) {
                        Some(links) if !links.is_empty() => format!(" · links {}", fit(links, 60)),
                        _ => String::new(),
                    }
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

        // Two rows, each clipped at the edge: what happened, then the keys that apply.
        let footer = Text::from(vec![
            Line::from(self.status_row()),
            Line::from(self.keys()).dim(),
        ]);
        frame.render_widget(Paragraph::new(footer), status_area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn app() -> (App, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("fzn-ui-test-{:x}", crate::store::random_u64()));
        let store = Store::open(&dir, "dev").unwrap();
        let app = App::new(
            Engine::new(),
            store,
            "true".into(),
            "fzn".into(),
            String::new(),
        );
        (app, dir)
    }

    fn add(app: &mut App, text: &str) {
        app.set_input(text.to_owned());
        app.create().unwrap();
    }

    fn heads(app: &App) -> Vec<&str> {
        app.rows.iter().map(|r| r.head.as_str()).collect()
    }

    #[test]
    fn archiving_keeps_the_date_and_hides_the_note_until_asked_for() {
        let (mut app, dir) = app();
        app.commit(Record {
            id: "old".into(),
            text: "kettlebell swings".into(),
            links: Vec::new(),
            created: Some(Moment::utc(1_000_000)),
            updated: Some(Moment::utc(1_000_000)),
            stamp: None,
            archived: false,
            deleted: false,
            device: String::new(),
        })
        .unwrap();
        app.refresh(true);
        app.step(1);
        app.toggle_archive().unwrap();
        let held = app.store.get("old").unwrap();
        assert!(held.archived);
        assert_eq!(
            held.updated,
            Some(Moment::utc(1_000_000)),
            "archiving is not an edit"
        );

        assert!(heads(&app).is_empty());
        app.set_input("kettlebell".into());
        assert!(heads(&app).is_empty(), "nor does typing find it");
        app.show_archived = true;
        app.refresh(false);
        assert_eq!(heads(&app), ["kettlebell swings"]);

        // Undo lands in the same second as the change it reverses, and still has to win.
        app.undo().unwrap();
        assert!(!app.store.get("old").unwrap().archived);
        drop(app);
        let reopened = Store::open(&dir, "dev").unwrap();
        assert!(!reopened.get("old").unwrap().archived);
        assert_eq!(
            reopened.get("old").unwrap().updated,
            Some(Moment::utc(1_000_000))
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn stepping_back_out_of_a_note_lands_on_the_top_line() {
        let (mut app, dir) = app();
        add(&mut app, "kettlebell swings");
        add(&mut app, "kettlebell press");
        app.set_input("kettlebell".into());
        app.step(1);
        app.look_around();
        assert_eq!(app.pivots.len(), 1);
        assert_eq!(app.input, "");
        app.back();
        assert!(app.pivots.is_empty());
        assert_eq!(app.input, "kettlebell");
        assert_eq!(app.list.selected(), None, "so that ^D clears the input");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn going_into_a_note_already_on_the_path_goes_back_to_it() {
        let (mut app, dir) = app();
        put(
            &mut app,
            "squats",
            "Squat log\ntrainingapp sets",
            &[],
            1_000,
        );
        put(
            &mut app,
            "profile",
            "Profile\ntrainingapp numbers",
            &["squats"],
            2_000,
        );
        put(
            &mut app,
            "hub",
            "trainingapp\nthe plan",
            &["profile"],
            3_000,
        );
        let path =
            |app: &App| -> Vec<String> { app.pivots.iter().map(|p| p.head.clone()).collect() };

        type_in(&mut app, "trai");
        look_around(&mut app, "trainingapp");
        type_in(&mut app, "pro");
        look_around(&mut app, "Profile");
        type_in(&mut app, "squat");
        look_around(&mut app, "Squat log");
        assert_eq!(path(&app), ["trainingapp", "Profile", "Squat log"]);
        assert!(
            heads(&app).contains(&"trainingapp"),
            "the hub is a neighbour of the log, so it is listed again"
        );

        look_around(&mut app, "trainingapp");
        assert_eq!(path(&app), ["trainingapp"], "back to it, not a fourth step");
        assert_eq!(app.input, "pro", "the line as it was at that depth");
        assert_eq!(app.list.selected(), None);

        // With something on the line, the line stays as it is.
        app.set_input(String::new());
        look_around(&mut app, "Profile");
        type_in(&mut app, "trai");
        look_around(&mut app, "trainingapp");
        assert_eq!(path(&app), ["trainingapp"]);
        assert_eq!(app.input, "trai");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn what_another_instance_writes_shows_up() {
        let (mut app, dir) = app();
        add(&mut app, "mine");
        assert!(!app.reload(), "nothing new");

        let mut other = Store::open(&dir, "dev").unwrap();
        let theirs = Record {
            id: "theirs".into(),
            text: "written elsewhere".into(),
            links: Vec::new(),
            created: Some(Moment::now()),
            updated: Some(Moment::now().plus(60)),
            stamp: Some(Moment::now()),
            archived: false,
            deleted: false,
            device: String::new(),
        };
        other.put(theirs.clone()).unwrap();
        assert!(app.reload());
        assert_eq!(heads(&app), ["written elsewhere", "mine"]);
        app.set_input("elsewhere".into());
        assert_eq!(
            heads(&app),
            ["written elsewhere"],
            "and the engine knows it"
        );

        other
            .put(Record {
                deleted: true,
                stamp: Some(other.next_stamp("theirs")),
                ..theirs
            })
            .unwrap();
        assert!(app.reload());
        assert!(heads(&app).is_empty());
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A note with its own time, so that the order of a list does not hang on the clock.
    fn put(app: &mut App, id: &str, text: &str, links: &[&str], updated: i64) {
        app.commit(Record {
            id: id.into(),
            text: text.into(),
            links: links.iter().map(|&l| l.to_owned()).collect(),
            created: Some(Moment::utc(updated)),
            updated: Some(Moment::utc(updated)),
            stamp: None,
            archived: false,
            deleted: false,
            device: String::new(),
        })
        .unwrap();
        app.refresh(true);
    }

    fn select(app: &mut App, head: &str) {
        let i = app.rows.iter().position(|r| r.head == head).unwrap();
        app.list.select(Some(i));
    }

    fn look_around(app: &mut App, head: &str) {
        select(app, head);
        app.look_around();
    }

    /// Presses a key, with Control held when `ctrl` is set.
    fn press(app: &mut App, code: KeyCode, ctrl: bool) {
        let modifiers = if ctrl {
            KeyModifiers::CONTROL
        } else {
            KeyModifiers::NONE
        };
        app.handle(KeyEvent::new(code, modifiers)).unwrap();
    }

    fn type_in(app: &mut App, text: &str) {
        for c in text.chars() {
            press(app, KeyCode::Char(c), false);
        }
    }

    #[test]
    fn a_note_written_around_another_is_linked_by_id_and_listed_first() {
        let (mut app, dir) = app();
        put(
            &mut app,
            "profile",
            "Profile\ntrainingapp numbers and plan",
            &[],
            2_000,
        );
        put(
            &mut app,
            "hub",
            "trainingapp\nthe plan",
            &["profile"],
            3_000,
        );
        put(
            &mut app,
            "near",
            "trainingapp plan ideas",
            &[],
            9_000_000_000,
        );
        look_around(&mut app, "trainingapp");
        assert_eq!(heads(&app), ["Profile", "trainingapp plan ideas"]);

        add(&mut app, "some new note");
        assert_eq!(
            heads(&app),
            ["some new note", "Profile", "trainingapp plan ideas"],
            "linked first and newest on top, then what the engine suggests"
        );
        assert_eq!(
            app.rows.iter().map(|r| r.linked).collect::<Vec<_>>(),
            [true, true, false]
        );
        assert!(app.rows[0].reasons.starts_with("links here"));
        assert!(app.rows[1].reasons.starts_with("linked from here"));
        let new = app.store.get(app.rows[0].id.as_str()).unwrap().clone();
        assert_eq!(new.text, "some new note", "the text is what was typed");
        assert_eq!(new.links, ["hub"]);

        // Renaming the hub breaks nothing, and from outside the note says what it is linked to.
        let hub = app.store.get("hub").unwrap().clone();
        app.commit(Record {
            text: "Training App\nthe plan".into(),
            ..hub
        })
        .unwrap();
        app.refresh(false);
        assert_eq!(heads(&app)[..2], ["some new note", "Profile"]);
        app.back();
        let row = app.rows.iter().find(|r| r.head == "some new note").unwrap();
        assert_eq!(row.links, "Training App");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn ctrl_l_links_and_unlinks_the_selected_note() {
        let (mut app, dir) = app();
        put(
            &mut app,
            "profile",
            "Profile\ntrainingapp numbers",
            &[],
            2_000,
        );
        let hub = "trainingapp\nthe plan, see [[Profile]]";
        put(&mut app, "hub", hub, &["profile"], 3_000);
        put(&mut app, "near", "trainingapp plan ideas", &[], 1_000);
        look_around(&mut app, "trainingapp");
        assert_eq!(heads(&app), ["Profile", "trainingapp plan ideas"]);

        select(&mut app, "trainingapp plan ideas");
        press(&mut app, KeyCode::Char('l'), true);
        assert_eq!(app.store.get("near").unwrap().links, ["hub"]);
        assert_eq!(
            app.store.get("near").unwrap().updated,
            Some(Moment::utc(1_000))
        );
        assert_eq!(heads(&app), ["Profile", "trainingapp plan ideas"]);
        assert!(app.rows[1].linked, "locked in, and listed by its own date");
        assert_eq!(app.selected().unwrap().head, "trainingapp plan ideas");

        press(&mut app, KeyCode::Char('l'), true);
        assert!(app.store.get("near").unwrap().links.is_empty());
        assert!(!app.selected().unwrap().linked);
        assert_eq!(app.status, "unlinked trainingapp plan ideas");

        // Any link comes off the same way, whichever note holds it and however it was made.
        select(&mut app, "Profile");
        press(&mut app, KeyCode::Char('l'), true);
        assert!(app.store.get("hub").unwrap().links.is_empty());
        assert!(!app.selected().unwrap().linked);
        assert!(app.status.contains("still in the text"), "{}", app.status);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn brackets_typed_into_a_note_link_it_by_id() {
        let (mut app, dir) = app();
        put(
            &mut app,
            "prog",
            "Kettlebell Programme\nweek one",
            &[],
            2_000,
        );
        let typed = "follow [[kettlebell programme]] and [[Nowhere]] today";
        add(&mut app, typed);
        let new = app.rows[0].id.as_str().to_owned();
        let held = app.store.get(&new).unwrap();
        assert_eq!(held.links, ["prog"]);
        assert_eq!(held.text, typed, "the text is left as typed");
        look_around(&mut app, "Kettlebell Programme");
        assert!(app.rows[0].linked && app.rows[0].reasons.starts_with("links here"));

        // Renamed, the target is still linked, and the note shows its new head.
        let prog = app.store.get("prog").unwrap().clone();
        app.commit(Record {
            text: "Strength Programme\nweek one".into(),
            ..prog
        })
        .unwrap();
        app.back();
        let row = app.rows.iter().find(|r| r.id.as_str() == new).unwrap();
        assert_eq!(row.links, "Strength Programme");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn ctrl_l_outside_a_zoom_does_nothing_but_say_so() {
        let (mut app, dir) = app();
        put(&mut app, "a", "kettlebell swings", &[], 2_000);
        put(&mut app, "b", "kettlebell press", &[], 1_000);
        type_in(&mut app, "kettlebell day");
        select(&mut app, "kettlebell swings");
        press(&mut app, KeyCode::Char('l'), true);
        assert!(app.store.records().all(|r| r.links.is_empty()));
        assert_eq!(app.undo.len(), 2, "nothing was written");
        assert!(
            app.status.starts_with("go into a note first"),
            "{}",
            app.status
        );
        assert_eq!(app.input, "kettlebell day", "the input is left alone");

        press(&mut app, KeyCode::Esc, false);
        press(&mut app, KeyCode::Enter, false);
        let new = app
            .store
            .records()
            .find(|r| r.text == "kettlebell day")
            .unwrap();
        assert!(
            new.links.is_empty(),
            "nor is anything kept for the note being written"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn esc_never_throws_away_what_you_typed() {
        let (mut app, dir) = app();
        put(&mut app, "hub", "trainingapp\nthe plan", &[], 3_000);
        put(&mut app, "near", "trainingapp plan ideas", &[], 1_000);

        // Gone into from an empty line.
        select(&mut app, "trainingapp");
        press(&mut app, KeyCode::Enter, false);
        type_in(&mut app, "deload next week");
        press(&mut app, KeyCode::Esc, false);
        assert!(app.pivots.is_empty());
        assert_eq!(app.input, "deload next week");

        // Gone into by a search.
        press(&mut app, KeyCode::Char('d'), true);
        type_in(&mut app, "trai");
        select(&mut app, "trainingapp");
        press(&mut app, KeyCode::Enter, false);
        type_in(&mut app, "deload next week");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(app.input, "deload next week", "the draft, not the search");
        assert_eq!(app.list.selected(), None);

        // With the line emptied, the search comes back.
        press(&mut app, KeyCode::Char('d'), true);
        type_in(&mut app, "trai");
        select(&mut app, "trainingapp");
        press(&mut app, KeyCode::Enter, false);
        type_in(&mut app, "oops");
        press(&mut app, KeyCode::Char('d'), true);
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(app.input, "trai");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_search_clears_on_the_way_in_and_comes_back_on_the_way_out() {
        let (mut app, dir) = app();
        put(&mut app, "hub", "trainingapp\nthe plan", &[], 3_000);
        put(&mut app, "near", "trainingapp plan ideas", &[], 1_000);
        type_in(&mut app, "trai");
        select(&mut app, "trainingapp");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(app.pivots.len(), 1);
        assert_eq!(app.input, "");
        assert_eq!(heads(&app), ["trainingapp plan ideas"]);

        press(&mut app, KeyCode::Esc, false);
        assert!(app.pivots.is_empty());
        assert_eq!(app.input, "trai");
        assert_eq!(app.cursor, 4);
        assert_eq!(app.list.selected(), None, "on the top line");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_name_of_a_note_finds_what_was_written_in_it() {
        let (mut app, dir) = app();
        put(&mut app, "boiler", "Boiler", &[], 1_000);
        put(&mut app, "quote", "engineer quoted 240", &["boiler"], 2_000);
        put(
            &mut app,
            "pressure",
            "pressure dropped again",
            &["boiler"],
            3_000,
        );
        put(&mut app, "far", "cake recipe", &[], 4_000);

        type_in(&mut app, "boiler");
        assert_eq!(heads(&app)[0], "Boiler", "the note so named comes first");
        assert!(
            app.rows[0].reasons.starts_with("its name · "),
            "{}",
            app.rows[0].reasons
        );
        assert_eq!(heads(&app).len(), 3);
        let row = app.rows.iter().find(|r| r.id.as_str() == "quote").unwrap();
        assert!(row.reasons.contains("about Boiler"), "{}", row.reasons);
        assert_eq!(row.links, "Boiler", "and the row says so too");

        // Going into a note found that way was a search for it, though it does not say the word.
        select(&mut app, "pressure dropped again");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(app.pivots.len(), 1);
        assert_eq!(app.input, "");
        press(&mut app, KeyCode::Esc, false);
        assert_eq!(app.input, "boiler");

        // So was one found by a word of its own and the name of what it is about.
        press(&mut app, KeyCode::Char('d'), true);
        type_in(&mut app, "boiler quoted");
        assert_eq!(heads(&app)[0], "engineer quoted 240");
        press(&mut app, KeyCode::Down, false);
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(app.input, "");
        press(&mut app, KeyCode::Esc, false);

        // Something new said about the boiler is a draft, and goes in with you.
        press(&mut app, KeyCode::Char('d'), true);
        type_in(&mut app, "boiler needs a service");
        select(&mut app, "Boiler");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(app.input, "boiler needs a service");
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A hub with a neighbourhood of three: two notes that share words with the draft below, and
    /// one that shares nothing with it.
    fn hub_with_neighbours(app: &mut App) {
        put(
            app,
            "profile",
            "Profile\ntrainingapp numbers and plan",
            &[],
            2_000,
        );
        put(
            app,
            "squats",
            "Squat log\ntrainingapp sets",
            &["hub"],
            2_500,
        );
        put(app, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
        put(app, "near", "trainingapp plan ideas", &[], 1_000);
    }

    const DRAFT: &str = "the plan needs a deload week";

    #[test]
    fn a_draft_comes_with_you_and_is_written_where_you_land() {
        let (mut app, dir) = app();
        hub_with_neighbours(&mut app);
        look_around(&mut app, "trainingapp");
        assert_eq!(
            heads(&app),
            ["Squat log", "Profile", "trainingapp plan ideas"]
        );
        app.back();

        type_in(&mut app, DRAFT);
        select(&mut app, "trainingapp");
        press(&mut app, KeyCode::Enter, false);
        assert_eq!(app.pivots.len(), 1);
        assert_eq!(app.input, DRAFT, "the draft came along");
        assert_eq!(app.cursor, DRAFT.chars().count());
        assert_eq!(app.list.selected(), None);
        assert_eq!(
            heads(&app),
            ["trainingapp plan ideas", "Profile"],
            "the neighbourhood, narrowed by the draft"
        );

        press(&mut app, KeyCode::Enter, false);
        let new = app.store.records().find(|r| r.text == DRAFT).unwrap();
        assert_eq!(new.links, ["hub"]);
        assert_eq!(app.input, "");
        assert_eq!(heads(&app)[0], DRAFT, "at the top of the linked part");

        press(&mut app, KeyCode::Esc, false);
        assert!(app.pivots.is_empty());
        assert_eq!(
            app.input, "",
            "the draft was written, so it is not put back"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_draft_taken_into_a_note_and_back_out_is_still_on_the_line() {
        let (mut app, dir) = app();
        hub_with_neighbours(&mut app);
        type_in(&mut app, DRAFT);
        select(&mut app, "trainingapp");
        press(&mut app, KeyCode::Enter, false);
        press(&mut app, KeyCode::Char('!'), false);
        press(&mut app, KeyCode::Esc, false);
        assert!(app.pivots.is_empty());
        assert_eq!(
            app.input,
            format!("{DRAFT}!"),
            "as it is now, not as it was"
        );
        assert_eq!(app.list.selected(), None);
        assert!(
            heads(&app).contains(&"trainingapp"),
            "and what it finds is listed"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_footer_shows_only_what_applies() {
        let (mut app, dir) = app();
        assert_eq!(app.status_row(), "0 notes");
        assert_eq!(
            app.keys(),
            "Enter writes · ^V show archived · Esc Esc quits",
            "nothing listed, so nothing to pick"
        );
        put(
            &mut app,
            "profile",
            "Profile\ntrainingapp numbers",
            &[],
            2_000,
        );
        assert_eq!(app.status_row(), "1 note");
        put(
            &mut app,
            "video",
            "Squat form video\nhttps://example.com/squat for trainingapp",
            &["hub"],
            2_500,
        );
        put(
            &mut app,
            "hub",
            "trainingapp\nthe plan",
            &["profile"],
            3_000,
        );
        put(&mut app, "near", "trainingapp plan ideas", &[], 1_000);
        app.status = "saved".into();
        assert_eq!(app.status_row(), "saved · 4 notes");
        assert_eq!(
            app.keys(),
            "Enter writes · ↓ picks a note · ^V show archived · ^U undo · Esc Esc quits"
        );
        select(&mut app, "trainingapp");
        assert_eq!(
            app.keys(),
            "Enter opens · ^O edit · ^V archive · ^D delete · ^C copy · ^U undo · Esc up"
        );

        app.look_around();
        assert_eq!(
            app.keys(),
            "Enter writes here · ↓ picks a note · ^V show archived · ^U undo · Esc back"
        );
        select(&mut app, "Profile");
        assert_eq!(
            app.keys(),
            "Enter opens · ^L unlink · ^O edit · ^V archive · ^D delete · ^C copy · ^U undo \
             · Esc up"
        );
        select(&mut app, "Squat form video");
        assert_eq!(
            app.keys(),
            "Enter opens · ^L unlink · ^O edit · ^V archive · ^D delete · ^C copy · ^_ open URL \
             · ^U undo · Esc up"
        );
        select(&mut app, "trainingapp plan ideas");
        assert!(
            app.keys().starts_with("Enter opens · ^L link ·"),
            "{}",
            app.keys()
        );

        // The widest a row gets: a linked, archived note with a URL, with undo and redo to hand.
        app.show_archived = true;
        app.refresh(false);
        select(&mut app, "Squat form video");
        app.toggle_archive().unwrap();
        select(&mut app, "trainingapp plan ideas");
        press(&mut app, KeyCode::Char('l'), true);
        app.undo().unwrap();
        app.list.select(None);
        assert_eq!(
            app.keys(),
            "Enter writes here · ↓ picks a note · ^V hide archived · ^U undo · ^R redo · Esc back"
        );
        select(&mut app, "Squat form video");
        let widest = app.keys();
        assert_eq!(
            widest,
            "Enter opens · ^L unlink · ^O edit · ^V unarchive · ^D delete · ^C copy \
             · ^_ open URL · ^U undo · ^R redo · Esc up"
        );
        assert!(widest.chars().count() <= 113, "{}", widest.chars().count());
        fs::remove_dir_all(&dir).unwrap();
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
    fn a_date_word_searches_as_its_date_and_is_written_as_one() {
        let (mut app, dir) = app();
        let friday = text::expand_date_words("/fri", today());
        let tomorrow = text::expand_date_words("/tom", today());
        add(&mut app, &format!("dentist {friday}"));
        add(&mut app, "something else entirely");

        // On the line the word stays as typed, the date is shown beside it, and the search
        // already uses the date.
        type_in(&mut app, "/fri");
        assert_eq!(app.input, "/fri");
        assert!(dates_shown(&app.input, today()).ends_with(&friday));
        assert_eq!(heads(&app), [format!("dentist {friday}")]);

        // Going into the note it found is a search, so the line clears.
        app.step(1);
        app.look_around();
        assert_eq!(app.input, "");
        app.back();
        assert_eq!(app.input, "/fri");

        // Writing the line writes the date.
        app.set_input("book the car in /tom, not /today's".into());
        app.create().unwrap();
        assert!(
            heads(&app).contains(&format!("book the car in {tomorrow}, not /today's").as_str())
        );
        assert_eq!(dates_shown("no dates here /home/sam", today()), "");
        fs::remove_dir_all(&dir).unwrap();
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
        assert!(!has_body("head\n#house #plan\n"));
        assert!(has_body("head\n#house\n- and more"));
        assert!(has_body("head\n# A heading"));
        assert!(!has_body("head"));
    }
}
