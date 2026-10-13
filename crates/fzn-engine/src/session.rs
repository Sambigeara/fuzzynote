//! The session: the app itself, with no screen and no keyboard.
//!
//! There is one line and one list. The list shows recent notes while the line is empty and what
//! the engine finds as it is typed, each with its reasons. A note is found by its own words and
//! by the head of a note it links to, so the name of a note brings what was written in it, as
//! going into the note does. The cursor is on the line or on a note of the list, and typing
//! always goes to the line. Archived notes stay out of every list until they are asked for.
//!
//! Going into a note makes it the place, and the path grows by one step: what is linked to or
//! from it comes first, newest on top, and what the engine finds besides follows, best first.
//! Where you are is what you are talking about, so a note written there is linked to it. The
//! line comes with you under one rule: if the note contains what was typed, or its head and the
//! heads of the notes it links to have every word of it, that was a search, so the line clears,
//! and coming back puts it on an empty line again; otherwise it was a draft, and it stays on the
//! line to be written there. Coming back never throws away what is on the line. Going into a
//! note already on the path goes back to it, so the path never repeats. Linking or unlinking a
//! note of the list is a correction. A `[[head]]` typed into a note is one more way of making a
//! link: saving the note links it to the note so headed. However it was made, a link is the
//! target's id held on the note.
//!
//! A date is plain text, written `2026-10-16`. A word such as `/fri` or `/tomorrow` is a quicker
//! way of typing one: the view says the date it stands for, the search uses it, and saving a
//! note writes the date in its place.
//!
//! A face owns none of this. It says what was asked for, as an [`Action`], carries out the
//! [`Effect`]s that come back, and paints the [`View`]. Which key or gesture asks for what is the
//! face's to decide, and so is editing the line: a terminal has to do that itself and a browser
//! is given it, so the session is only ever told what the line now reads.

use std::cmp::Ordering;
use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::engine::{Engine, Hit, SearchOptions};
use crate::note::NoteId;
use crate::record::{Notes, Record, links_on_save};
use crate::score::Weights;
use crate::text;
use crate::time::{Day, Moment};

const RECENT_LIMIT: usize = 500;
const RELATED_LIMIT: usize = 60;
const AROUND_LIMIT: usize = 200;

/// What a session needs from the machine it runs on.
pub trait Host {
    /// The time, with this machine's offset from UTC.
    fn now(&self) -> Moment;

    /// An id for a new note: unique across devices, and sorting after the ids made before it.
    fn new_id(&self) -> String;
}

/// Everything that can be asked of a session. One that does not apply where the cursor is does
/// nothing. As JSON an action is its name in snake case, with what it carries where it carries
/// something: `"write"`, `{"type":"a line"}`, `{"select":null}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// The line now reads this. The cursor goes to the line.
    Type(String),
    /// Move the cursor down the list by this many notes, or up it. Up from the first note is
    /// the line.
    Move(i64),
    /// Put the cursor on this row of the list, or on the line.
    Select(Option<usize>),
    /// Turn the line into a note, linked to the note being looked around.
    Write,
    /// Go into the note under the cursor.
    GoIn,
    /// Step back out of the note being looked around, onto the line.
    Back,
    /// Open the note under the cursor for editing. Comes back as [`Effect::Edit`].
    Edit,
    /// The text of a note as it was edited.
    Save {
        id: String,
        text: String,
    },
    /// Archive the note under the cursor, or bring it back.
    Archive,
    /// Delete the note under the cursor.
    Delete,
    /// Link the note under the cursor to the one being looked around, or take that link away.
    Link,
    /// Copy the first URL in the note under the cursor, or its head.
    Copy,
    /// Open the first URL in the note under the cursor.
    OpenUrl,
    Undo,
    Redo,
    /// List archived notes too, or stop.
    ShowArchived,
    /// Say what each listed note scored, or stop.
    ShowScores,
}

/// What a face has to do about an action. The session has already done its part.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// A version of a note to keep, as one more line of this device's log.
    Keep(Record),
    /// A note to put in front of the person for editing. What they make of it comes back as
    /// [`Action::Save`].
    Edit { id: String, text: String },
    /// Text for the clipboard.
    Copy(String),
    /// A URL to open.
    Open(String),
}

/// Something that can be asked for where the cursor is, and what to call it there. A face puts
/// its own key or control beside the label.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Hint {
    pub action: Action,
    pub label: &'static str,
}

/// A note on the list.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Row {
    pub id: String,
    pub head: String,
    pub archived: bool,
    /// Whether there is more to read than the head.
    pub has_body: bool,
    /// Whether the note is linked to or from the one being looked around.
    pub linked: bool,
    /// The heads, as they are now, of the notes this one is linked to by id. The note being
    /// looked around is left out, since everything on the list is about it.
    pub links: Vec<String>,
    /// Why the note is listed.
    pub reasons: Vec<String>,
    pub score: Option<f32>,
    pub updated: Option<Moment>,
}

/// What to show.
#[derive(Debug, Serialize)]
pub struct View<'a> {
    /// Where you are: the heads of the notes gone into, outermost first.
    pub path: Vec<&'a str>,
    pub line: &'a str,
    /// The dates that the date words on the line stand for.
    pub dates: Vec<Day>,
    /// The list: what is around the last note on the path, or what the line finds, or what is
    /// recent when there is neither.
    pub rows: &'a [Row],
    /// The row the cursor is on. With none, it is on the line.
    pub cursor: Option<usize>,
    /// The note to read beside the list: the one under the cursor, or the first on the list.
    pub preview: Option<&'a Record>,
    /// What just happened.
    pub status: &'a str,
    /// How many notes there are.
    pub notes: usize,
    /// What can be asked for where the cursor is. The way back is last.
    pub hints: Vec<Hint>,
    pub archived_shown: bool,
    pub scores_shown: bool,
}

/// A note being looked around. The list shows its neighbourhood.
struct Pivot {
    id: String,
    head: String,
    /// The search the note was found with, put back on the way out if the line is empty by then.
    /// A draft carried in has nothing to put back: it is still on the line.
    restore: Option<String>,
}

/// One person at one space. Create one per face.
pub struct Session {
    host: Box<dyn Host>,
    device: String,
    engine: Engine,
    notes: Notes,
    line: String,
    rows: Vec<Row>,
    /// The row the cursor is on. With none, it is on the line.
    cursor: Option<usize>,
    show_archived: bool,
    show_scores: bool,
    status: String,
    /// Notes being looked around, innermost last.
    pivots: Vec<Pivot>,
    /// Versions to restore, newest last: what a note was before a change, and what it became.
    undo: Vec<(Option<Record>, Record)>,
    redo: Vec<(Option<Record>, Record)>,
    /// What the action being carried out asks of the face.
    out: Vec<Effect>,
}

/// Whether there is more to read than the head. A line of nothing but tags is a label, not
/// reading.
fn has_body(text: &str) -> bool {
    text.lines()
        .skip(1)
        .any(|line| !line.trim().is_empty() && !text::only_tags(line))
}

impl Session {
    /// Opens a space. `device` names this device in the versions it writes, and breaks ties
    /// between versions written in the same second.
    pub fn new(
        host: Box<dyn Host>,
        device: impl Into<String>,
        weights: Weights,
        notes: Notes,
    ) -> Self {
        let mut engine = Engine::with_weights(weights);
        for record in notes.records() {
            engine.upsert(record.note());
        }
        let mut session = Self {
            host,
            device: device.into(),
            engine,
            notes,
            line: String::new(),
            rows: Vec::new(),
            cursor: None,
            show_archived: false,
            show_scores: false,
            status: String::new(),
            pivots: Vec::new(),
            undo: Vec::new(),
            redo: Vec::new(),
            out: Vec::new(),
        };
        session.refresh(true);
        session
    }

    /// Does what was asked, and returns what the face has to do about it.
    pub fn act(&mut self, action: Action) -> Vec<Effect> {
        match action {
            Action::Type(line) => {
                self.line = line;
                self.refresh(true);
            }
            Action::Move(by) => self.step(by),
            Action::Select(None) => self.cursor = None,
            Action::Select(Some(row)) => {
                if row < self.rows.len() {
                    self.cursor = Some(row);
                }
            }
            Action::Write => self.write_line(),
            Action::GoIn => self.go_in(),
            Action::Back => self.back_to(self.pivots.len().saturating_sub(1)),
            Action::Edit => self.edit(),
            Action::Save { id, text } => self.save(&id, &text),
            Action::Archive => self.archive(),
            Action::Delete => self.delete(),
            Action::Link => self.link(),
            Action::Copy => self.copy(),
            Action::OpenUrl => self.open_url(),
            Action::Undo => self.undo(),
            Action::Redo => self.redo(),
            Action::ShowArchived => {
                self.show_archived = !self.show_archived;
                self.refresh(false);
            }
            Action::ShowScores => self.show_scores = !self.show_scores,
        }
        std::mem::take(&mut self.out)
    }

    /// Takes in versions written elsewhere: by other devices, or by other faces on this one.
    /// Says whether anything on show may have changed. A session's own versions, read back from
    /// where they were kept, are not news.
    pub fn absorb(&mut self, records: impl IntoIterator<Item = Record>) -> bool {
        let mut changed = Vec::new();
        for record in records {
            let id = record.id.clone();
            if self.notes.apply(record) {
                changed.push(id);
            }
        }
        changed.sort_unstable();
        changed.dedup();
        if changed.is_empty() {
            return false;
        }
        for id in &changed {
            match self.notes.get(id) {
                Some(record) => self.engine.upsert(record.note()),
                None => {
                    self.engine.remove(&NoteId::new(id.clone()));
                }
            }
        }
        let selected = self.selected().map(|r| r.id.clone());
        self.refresh(false);
        if let Some(id) = selected {
            self.reselect(&id);
        }
        self.status = format!("{} changed elsewhere", changed.len());
        true
    }

    /// Puts a message where the session puts its own, for what only the face can know: that an
    /// effect could not be carried out, say.
    pub fn say(&mut self, status: impl Into<String>) {
        self.status = status.into();
    }

    pub fn view(&self) -> View<'_> {
        let shown = self.selected().or(self.rows.first());
        View {
            path: self.pivots.iter().map(|p| p.head.as_str()).collect(),
            line: &self.line,
            dates: text::date_words(&self.line, self.today())
                .iter()
                .map(|w| w.day)
                .collect(),
            rows: &self.rows,
            cursor: self.cursor,
            preview: shown.and_then(|r| self.notes.get(&r.id)),
            status: &self.status,
            notes: self.notes.len(),
            hints: self.hints(),
            archived_shown: self.show_archived,
            scores_shown: self.show_scores,
        }
    }

    /// Today, by the host's clock.
    fn today(&self) -> Day {
        self.host.now().day()
    }

    /// What can be asked for where the cursor is. The way back is always last.
    fn hints(&self) -> Vec<Hint> {
        let zoomed = !self.pivots.is_empty();
        let mut hints = Vec::new();
        let mut offer = |action, label| hints.push(Hint { action, label });
        match self.selected() {
            None => {
                offer(Action::Write, if zoomed { "writes here" } else { "writes" });
                if !self.rows.is_empty() {
                    offer(Action::Move(1), "picks a note");
                }
                offer(
                    Action::ShowArchived,
                    if self.show_archived {
                        "hide archived"
                    } else {
                        "show archived"
                    },
                );
            }
            Some(row) => {
                offer(Action::GoIn, "opens");
                if zoomed {
                    offer(Action::Link, if row.linked { "unlink" } else { "link" });
                }
                offer(Action::Edit, "edit");
                offer(
                    Action::Archive,
                    if row.archived { "unarchive" } else { "archive" },
                );
                offer(Action::Delete, "delete");
                offer(Action::Copy, "copy");
                // Only the one note under the cursor is read for a URL, once per view.
                if self
                    .notes
                    .get(&row.id)
                    .is_some_and(|r| !text::facts(&r.text).urls.is_empty())
                {
                    offer(Action::OpenUrl, "open URL");
                }
            }
        }
        if !self.undo.is_empty() {
            offer(Action::Undo, "undo");
        }
        if !self.redo.is_empty() {
            offer(Action::Redo, "redo");
        }
        match self.cursor {
            Some(_) => offer(Action::Select(None), "up"),
            None if zoomed => offer(Action::Back, "back"),
            None => {}
        }
        hints
    }

    /// Zooms on the note under the cursor: the list becomes its neighbourhood. If the note
    /// contains what was typed, or is named by it, in its head and in the heads of the notes it
    /// links to, that was a search for it, so the line clears and is kept for the way back.
    /// Otherwise it was a draft, and it comes along to be written here. A note already on the
    /// path is gone back to rather than added again, so the path never repeats.
    fn go_in(&mut self) {
        let Some(row) = self.selected() else {
            return;
        };
        let (id, head) = (row.id.clone(), row.head.clone());
        if let Some(at) = self.pivots.iter().position(|p| p.id == id) {
            self.back_to(at + 1);
            return;
        }
        let typed = text::expand_date_words(self.line.trim(), self.today());
        let searched = typed.is_empty()
            || self
                .notes
                .get(&id)
                .is_some_and(|r| r.text.to_lowercase().contains(&typed.to_lowercase()))
            || self.engine.named_by(&NoteId::new(id.clone()), &typed);
        let restore = searched.then(|| std::mem::take(&mut self.line));
        self.pivots.push(Pivot { id, head, restore });
        self.refresh(true);
    }

    /// Steps back out to `depth` notes on the path, onto the line, so that what is asked next is
    /// asked of the line and not of a note. What is on the line stays as it is; only an empty
    /// line gets back the search that the first note left behind was found with, which is what
    /// the line held at that depth.
    fn back_to(&mut self, depth: usize) {
        if depth >= self.pivots.len() {
            return;
        }
        let left = self.pivots.split_off(depth);
        if let Some(line) = left.into_iter().next().and_then(|p| p.restore)
            && self.line.trim().is_empty()
        {
            self.line = line;
        }
        self.refresh(true);
    }

    /// Recomputes the list from the line and the path. `reset` moves the cursor back to the
    /// line.
    fn refresh(&mut self, reset: bool) {
        // A date word searches as the date it stands for.
        let query = text::expand_date_words(self.line.trim(), self.today());
        let rows = match (self.pivots.last(), query.is_empty()) {
            (Some(p), _) => self.around(&p.id, &query),
            (None, true) => self.recent(),
            (None, false) => self.related(&query),
        };
        self.rows = rows;
        let cursor = if reset { None } else { self.cursor };
        self.cursor = cursor
            .filter(|_| !self.rows.is_empty())
            .map(|i| i.min(self.rows.len() - 1));
    }

    fn recent(&self) -> Vec<Row> {
        let mut records: Vec<&Record> = self
            .notes
            .records()
            .filter(|r| self.show_archived || !r.archived)
            .collect();
        records.sort_by(|a, b| b.updated.cmp(&a.updated).then_with(|| a.id.cmp(&b.id)));
        records
            .into_iter()
            .take(RECENT_LIMIT)
            .map(|r| Row {
                id: r.id.clone(),
                head: r.head().to_owned(),
                archived: r.archived,
                has_body: has_body(&r.text),
                linked: false,
                links: self.link_heads(r),
                reasons: Vec::new(),
                score: None,
                updated: r.updated,
            })
            .collect()
    }

    fn related(&self, query: &str) -> Vec<Row> {
        let opts = SearchOptions {
            limit: RELATED_LIMIT,
            include_archived: self.show_archived,
            today: Some(self.today()),
        };
        self.engine
            .search(query, &opts)
            .into_iter()
            .map(|hit| self.row(&hit))
            .collect()
    }

    /// The neighbourhood of one note, narrowed by the line when there is something on it.
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
            today: Some(self.today()),
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
    fn link_heads(&self, record: &Record) -> Vec<String> {
        let around = self.pivots.last().map(|p| p.id.as_str());
        record
            .links
            .iter()
            .filter(|id| Some(id.as_str()) != around)
            .filter_map(|id| self.notes.get(id))
            .map(|target| target.head().to_owned())
            .collect()
    }

    fn row(&self, hit: &Hit<'_>) -> Row {
        let record = self.notes.get(hit.note.id.as_str());
        Row {
            id: hit.note.id.as_str().to_owned(),
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
                .collect(),
            score: Some(hit.score),
            updated: record.and_then(|r| r.updated),
        }
    }

    /// Moves the cursor between the line and the list.
    fn step(&mut self, delta: i64) {
        if self.rows.is_empty() {
            self.cursor = None;
            return;
        }
        let last = self.rows.len() as i64 - 1;
        let next = match self.cursor {
            None if delta > 0 => Some((delta - 1).min(last)),
            None => None,
            Some(i) => {
                let n = i as i64 + delta;
                (n >= 0).then(|| n.min(last))
            }
        };
        self.cursor = next.map(|n| n as usize);
    }

    fn selected(&self) -> Option<&Row> {
        self.cursor.and_then(|i| self.rows.get(i))
    }

    /// The note under the cursor, as it is held.
    fn selected_record(&self) -> Option<&Record> {
        self.selected().and_then(|r| self.notes.get(&r.id))
    }

    fn reselect(&mut self, id: &str) {
        if let Some(i) = self.rows.iter().position(|r| r.id == id) {
            self.cursor = Some(i);
        }
    }

    /// Writes a version, remembering what it replaced for undo.
    fn commit(&mut self, record: Record) {
        let before = self.notes.version(&record.id).cloned();
        let after = self.write(record);
        self.undo.push((before, after));
        self.redo.clear();
    }

    /// Stamps a version so that it supersedes the one held, here and on every other device,
    /// takes it in, and asks for it to be kept.
    fn write(&mut self, mut record: Record) -> Record {
        record.stamp = Some(self.notes.next_stamp(&record.id, self.host.now()));
        record.device = self.device.clone();
        if record.deleted {
            self.engine.remove(&NoteId::new(record.id.clone()));
        } else {
            self.engine.upsert(record.note());
        }
        self.notes.apply(record.clone());
        self.out.push(Effect::Keep(record.clone()));
        record
    }

    /// Puts a note back to a version.
    fn restore(&mut self, version: Option<&Record>, fallback: &Record) {
        let record = match version {
            Some(v) => v.clone(),
            None => Record {
                deleted: true,
                ..fallback.clone()
            },
        };
        let id = record.id.clone();
        self.write(record);
        self.refresh(false);
        self.reselect(&id);
    }

    fn undo(&mut self) {
        let Some((before, after)) = self.undo.pop() else {
            self.status = "nothing to undo".into();
            return;
        };
        self.restore(before.as_ref(), &after);
        self.status = format!("undid: {}", text::fit(after.head(), 40));
        self.redo.push((before, after));
    }

    fn redo(&mut self) {
        let Some((before, after)) = self.redo.pop() else {
            self.status = "nothing to redo".into();
            return;
        };
        self.restore(Some(&after), &after);
        self.status = format!("redid: {}", text::fit(after.head(), 40));
        self.undo.push((before, after));
    }

    /// Turns the line into a note, linked to the note being looked around and to whatever it
    /// names in brackets, with its date words written out as dates.
    fn write_line(&mut self) {
        let text = text::expand_date_words(self.line.trim(), self.today());
        if text.is_empty() {
            return;
        }
        let around: Vec<String> = self
            .pivots
            .last()
            .map(|p| p.id.clone())
            .into_iter()
            .collect();
        let id = self.host.new_id();
        let links = links_on_save(&self.engine, &id, "", &text, &around);
        let now = self.host.now();
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
        });
        self.line.clear();
        self.refresh(true);
        self.status = "created".into();
    }

    fn edit(&mut self) {
        if let Some(record) = self.selected_record() {
            let (id, text) = (record.id.clone(), record.text.clone());
            self.out.push(Effect::Edit { id, text });
        }
    }

    /// Takes the text of a note as it was edited. What it names in brackets is linked, what it
    /// no longer names is not, and its date words are written out as dates.
    fn save(&mut self, id: &str, edited: &str) {
        let Some(record) = self.notes.get(id).cloned() else {
            self.status = "that note is gone, so nothing was saved".into();
            return;
        };
        let edited = text::expand_date_words(edited.trim_end_matches(['\n', '\r']), self.today());
        if edited == record.text {
            self.status = "unchanged".into();
        } else if edited.trim().is_empty() {
            self.status = "empty, so left unchanged · archive or delete it instead".into();
        } else {
            let links = links_on_save(&self.engine, id, &record.text, &edited, &record.links);
            self.commit(Record {
                text: edited,
                links,
                updated: Some(self.host.now()),
                ..record
            });
            self.status = "saved".into();
            self.refresh(false);
            self.reselect(id);
        }
    }

    fn archive(&mut self) {
        let Some(record) = self.selected_record().cloned() else {
            return;
        };
        let (id, archived) = (record.id.clone(), !record.archived);
        self.commit(Record { archived, ..record });
        self.status = if archived { "archived" } else { "unarchived" }.into();
        self.refresh(false);
        self.reselect(&id);
    }

    fn delete(&mut self) {
        let Some(record) = self.selected_record().cloned() else {
            return;
        };
        self.commit(Record {
            deleted: true,
            ..record
        });
        self.status = "deleted · undo brings it back".into();
        self.refresh(false);
    }

    /// Asks for the first URL of the note under the cursor, or its head, to be copied.
    fn copy(&mut self) {
        let Some(record) = self.selected_record() else {
            return;
        };
        let (what, payload) = match text::facts(&record.text).urls.first() {
            Some(url) => ("URL", url.clone()),
            None => ("head", record.head().to_owned()),
        };
        self.status = format!("copied {what}: {}", text::fit(&payload, 50));
        self.out.push(Effect::Copy(payload));
    }

    fn open_url(&mut self) {
        let Some(record) = self.selected_record() else {
            return;
        };
        let Some(url) = text::facts(&record.text).urls.first().cloned() else {
            self.status = "no URL in that note".into();
            return;
        };
        self.status = format!("opened {}", text::fit(&url, 60));
        self.out.push(Effect::Open(url));
    }

    /// Links the note under the cursor to the one being looked around, or takes that link away.
    /// It is a correction: what is written in a zoom is linked already, and outside one there is
    /// nothing to link to.
    fn link(&mut self) {
        let Some(around) = self.pivots.last().map(|p| p.id.clone()) else {
            self.status = "go into a note first; what you write there links to it".into();
            return;
        };
        let Some(row) = self.selected() else {
            self.status =
                "pick a note to link or unlink; what you write here is linked already".into();
            return;
        };
        let id = row.id.clone();
        let head = text::fit(&row.head, 40);
        let (Some(note), Some(pivot)) = (
            self.notes.get(&id).cloned(),
            self.notes.get(&around).cloned(),
        ) else {
            return;
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
            });
        } else if pivot.links.contains(&id) {
            self.status = format!("unlinked {head}{}", named(&pivot.text, &id));
            self.commit(Record {
                links: without(&pivot.links, &id),
                ..pivot
            });
        } else {
            let mut links = note.links.clone();
            links.push(around);
            self.commit(Record { links, ..note });
            self.status = format!("linked {head}");
        }
        self.refresh(false);
        self.reselect(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time;
    use std::cell::Cell;

    /// Midday on Tuesday 2026-10-13, in UTC.
    const NOON: Moment = Moment {
        secs: 1_791_892_800,
        offset: 0,
    };

    /// A machine whose clock has stopped, and whose ids count up.
    #[derive(Default)]
    struct Stopped {
        ids: Cell<u32>,
    }

    impl Host for Stopped {
        fn now(&self) -> Moment {
            NOON
        }

        fn new_id(&self) -> String {
            self.ids.set(self.ids.get() + 1);
            format!("new-{:04}", self.ids.get())
        }
    }

    fn session() -> Session {
        Session::new(
            Box::new(Stopped::default()),
            "dev",
            Weights::default(),
            Notes::new(),
        )
    }

    fn add(s: &mut Session, text: &str) -> Vec<Effect> {
        s.act(Action::Type(text.to_owned()));
        s.act(Action::Write)
    }

    /// A note with its own time, so that the order of a list does not hang on the clock. The
    /// version to keep goes out with whatever is asked next.
    fn put(s: &mut Session, id: &str, text: &str, links: &[&str], updated: i64) {
        s.commit(Record {
            id: id.into(),
            text: text.into(),
            links: links.iter().map(|&l| l.to_owned()).collect(),
            created: Some(Moment::utc(updated)),
            updated: Some(Moment::utc(updated)),
            stamp: None,
            archived: false,
            deleted: false,
            device: String::new(),
        });
        s.refresh(true);
    }

    fn heads(s: &Session) -> Vec<&str> {
        s.rows.iter().map(|r| r.head.as_str()).collect()
    }

    fn select(s: &mut Session, head: &str) {
        let i = s.rows.iter().position(|r| r.head == head).unwrap();
        s.act(Action::Select(Some(i)));
    }

    fn go_into(s: &mut Session, head: &str) {
        select(s, head);
        s.act(Action::GoIn);
    }

    /// Types onto the end of the line.
    fn type_in(s: &mut Session, text: &str) {
        s.act(Action::Type(format!("{}{text}", s.line)));
    }

    fn clear(s: &mut Session) {
        s.act(Action::Type(String::new()));
    }

    fn labels(s: &Session) -> Vec<&'static str> {
        s.view().hints.iter().map(|h| h.label).collect()
    }

    fn path(s: &Session) -> Vec<&str> {
        s.view().path
    }

    /// The versions asked to be kept, put through the lines of a log and read back.
    fn reopened<'a>(effects: impl IntoIterator<Item = &'a Effect>) -> Notes {
        let mut notes = Notes::new();
        for effect in effects {
            if let Effect::Keep(record) = effect {
                let line = record.line().unwrap();
                notes.apply(Record::read(line.as_bytes()).unwrap());
            }
        }
        notes
    }

    #[test]
    fn midday_is_when_the_tests_say_it_is() {
        assert_eq!(NOON.to_string(), "2026-10-13T12:00:00Z");
        assert_eq!(time::weekday(NOON.day()), 1);
    }

    #[test]
    fn archiving_keeps_the_date_and_hides_the_note_until_asked_for() {
        let mut s = session();
        let mut log = Vec::new();
        put(&mut s, "old", "kettlebell swings", &[], 1_000_000);
        log.extend(s.act(Action::Move(1)));
        log.extend(s.act(Action::Archive));
        let held = s.notes.get("old").unwrap();
        assert!(held.archived);
        assert_eq!(
            held.updated,
            Some(Moment::utc(1_000_000)),
            "archiving is not an edit"
        );
        assert_eq!(held.device, "dev");

        assert!(heads(&s).is_empty());
        s.act(Action::Type("kettlebell".into()));
        assert!(heads(&s).is_empty(), "nor does typing find it");
        s.act(Action::ShowArchived);
        assert_eq!(heads(&s), ["kettlebell swings"]);
        assert!(s.view().archived_shown);

        // Undo lands in the same second as the change it reverses, and still has to win,
        // whatever order the three versions are read back in.
        log.extend(s.act(Action::Undo));
        assert!(!s.notes.get("old").unwrap().archived);
        assert_eq!(log.len(), 3, "each change asked for one version to be kept");
        for notes in [reopened(&log), reopened(log.iter().rev())] {
            assert!(!notes.get("old").unwrap().archived);
            assert_eq!(
                notes.get("old").unwrap().updated,
                Some(Moment::utc(1_000_000))
            );
        }
    }

    #[test]
    fn the_cursor_moves_between_the_line_and_the_list() {
        let mut s = session();
        s.act(Action::Move(1));
        assert_eq!(s.cursor, None, "nothing listed, so nowhere to go");
        put(&mut s, "a", "kettlebell swings", &[], 3_000);
        put(&mut s, "b", "kettlebell press", &[], 2_000);
        put(&mut s, "c", "kettlebell halo", &[], 1_000);
        s.act(Action::Move(1));
        assert_eq!(s.cursor, Some(0));
        assert_eq!(s.view().preview.unwrap().id, "a");
        s.act(Action::Move(10));
        assert_eq!(s.cursor, Some(2), "no further than the last note");
        s.act(Action::Move(-1));
        assert_eq!(s.cursor, Some(1));
        s.act(Action::Move(-10));
        assert_eq!(s.cursor, None, "up from the first note is the line");
        assert_eq!(
            s.view().preview.unwrap().id,
            "a",
            "from the line, the first note is the one to read"
        );

        s.act(Action::Select(Some(2)));
        assert_eq!(s.cursor, Some(2));
        s.act(Action::Select(Some(3)));
        assert_eq!(s.cursor, Some(2), "a row that is not there is not gone to");
        s.act(Action::Type("press".into()));
        assert_eq!(s.cursor, None, "typing always goes to the line");
        assert_eq!(heads(&s), ["kettlebell press"]);
    }

    #[test]
    fn stepping_back_out_of_a_note_lands_on_the_line() {
        let mut s = session();
        add(&mut s, "kettlebell swings");
        add(&mut s, "kettlebell press");
        s.act(Action::Type("kettlebell".into()));
        s.act(Action::Move(1));
        s.act(Action::GoIn);
        assert_eq!(s.pivots.len(), 1);
        assert_eq!(s.line, "");
        s.act(Action::Back);
        assert!(s.pivots.is_empty());
        assert_eq!(s.line, "kettlebell");
        assert_eq!(
            s.cursor, None,
            "so that what is asked next is asked of the line"
        );
        s.act(Action::Back);
        assert_eq!(s.line, "kettlebell", "with nowhere to step back to");
    }

    #[test]
    fn going_into_a_note_already_on_the_path_goes_back_to_it() {
        let mut s = session();
        put(&mut s, "squats", "Squat log\ntrainingapp sets", &[], 1_000);
        put(
            &mut s,
            "profile",
            "Profile\ntrainingapp numbers",
            &["squats"],
            2_000,
        );
        put(&mut s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);

        type_in(&mut s, "trai");
        go_into(&mut s, "trainingapp");
        type_in(&mut s, "pro");
        go_into(&mut s, "Profile");
        type_in(&mut s, "squat");
        go_into(&mut s, "Squat log");
        assert_eq!(path(&s), ["trainingapp", "Profile", "Squat log"]);
        assert!(
            heads(&s).contains(&"trainingapp"),
            "the hub is a neighbour of the log, so it is listed again"
        );

        go_into(&mut s, "trainingapp");
        assert_eq!(path(&s), ["trainingapp"], "back to it, not a fourth step");
        assert_eq!(s.line, "pro", "the line as it was at that depth");
        assert_eq!(s.cursor, None);

        // With something on the line, the line stays as it is.
        clear(&mut s);
        go_into(&mut s, "Profile");
        type_in(&mut s, "trai");
        go_into(&mut s, "trainingapp");
        assert_eq!(path(&s), ["trainingapp"]);
        assert_eq!(s.line, "trai");
    }

    #[test]
    fn what_is_written_elsewhere_shows_up() {
        let mut s = session();
        let mine = add(&mut s, "mine");
        assert!(!s.absorb(Vec::new()), "nothing new");
        let kept: Vec<Record> = reopened(&mine).records().cloned().collect();
        assert_eq!(kept.len(), 1);
        assert!(
            !s.absorb(kept),
            "its own versions, read back from the log, are not news"
        );

        let theirs = Record {
            id: "theirs".into(),
            text: "written elsewhere".into(),
            links: Vec::new(),
            created: Some(NOON),
            updated: Some(NOON.plus(60)),
            stamp: Some(NOON),
            archived: false,
            deleted: false,
            device: "other".into(),
        };
        s.act(Action::Move(1));
        assert!(s.absorb([theirs.clone()]));
        assert_eq!(heads(&s), ["written elsewhere", "mine"]);
        assert_eq!(s.status, "1 changed elsewhere");
        assert_eq!(
            s.selected().unwrap().head,
            "mine",
            "the cursor stays on its note"
        );
        s.act(Action::Type("elsewhere".into()));
        assert_eq!(heads(&s), ["written elsewhere"], "and the engine knows it");

        assert!(s.absorb([Record {
            deleted: true,
            stamp: Some(NOON.plus(1)),
            ..theirs
        }]));
        assert!(heads(&s).is_empty());
    }

    #[test]
    fn a_note_written_around_another_is_linked_by_id_and_listed_first() {
        let mut s = session();
        put(
            &mut s,
            "profile",
            "Profile\ntrainingapp numbers and plan",
            &[],
            2_000,
        );
        put(&mut s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
        put(&mut s, "near", "trainingapp plan ideas", &[], 9_000_000_000);
        go_into(&mut s, "trainingapp");
        assert_eq!(heads(&s), ["Profile", "trainingapp plan ideas"]);

        let kept = add(&mut s, "some new note");
        assert_eq!(
            heads(&s),
            ["some new note", "Profile", "trainingapp plan ideas"],
            "linked first and newest on top, then what the engine suggests"
        );
        assert_eq!(
            s.rows.iter().map(|r| r.linked).collect::<Vec<_>>(),
            [true, true, false]
        );
        assert!(s.rows[0].reasons[0].starts_with("links here"));
        assert!(s.rows[1].reasons[0].starts_with("linked from here"));
        let new = s.notes.get(&s.rows[0].id).unwrap().clone();
        assert_eq!(new.text, "some new note", "the text is what was typed");
        assert_eq!(new.links, ["hub"]);
        assert_eq!((new.created, new.updated), (Some(NOON), Some(NOON)));
        assert_eq!(
            kept.last(),
            Some(&Effect::Keep(new.clone())),
            "and that is the version asked to be kept"
        );
        assert_eq!(s.line, "");
        assert_eq!(s.status, "created");

        // Renaming the hub breaks nothing, and from outside the note says what it is linked to.
        let hub = s.notes.get("hub").unwrap().clone();
        s.commit(Record {
            text: "Training App\nthe plan".into(),
            ..hub
        });
        s.refresh(false);
        assert_eq!(heads(&s)[..2], ["some new note", "Profile"]);
        s.act(Action::Back);
        let row = s.rows.iter().find(|r| r.head == "some new note").unwrap();
        assert_eq!(row.links, ["Training App"]);
    }

    #[test]
    fn linking_and_unlinking_the_note_under_the_cursor() {
        let mut s = session();
        put(
            &mut s,
            "profile",
            "Profile\ntrainingapp numbers",
            &[],
            2_000,
        );
        let hub = "trainingapp\nthe plan, see [[Profile]]";
        put(&mut s, "hub", hub, &["profile"], 3_000);
        put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);
        go_into(&mut s, "trainingapp");
        assert_eq!(heads(&s), ["Profile", "trainingapp plan ideas"]);

        select(&mut s, "trainingapp plan ideas");
        s.act(Action::Link);
        assert_eq!(s.notes.get("near").unwrap().links, ["hub"]);
        assert_eq!(
            s.notes.get("near").unwrap().updated,
            Some(Moment::utc(1_000))
        );
        assert_eq!(heads(&s), ["Profile", "trainingapp plan ideas"]);
        assert!(s.rows[1].linked, "locked in, and listed by its own date");
        assert_eq!(s.selected().unwrap().head, "trainingapp plan ideas");

        s.act(Action::Link);
        assert!(s.notes.get("near").unwrap().links.is_empty());
        assert!(!s.selected().unwrap().linked);
        assert_eq!(s.status, "unlinked trainingapp plan ideas");

        // Any link comes off the same way, whichever note holds it and however it was made.
        select(&mut s, "Profile");
        s.act(Action::Link);
        assert!(s.notes.get("hub").unwrap().links.is_empty());
        assert!(!s.selected().unwrap().linked);
        assert!(s.status.contains("still in the text"), "{}", s.status);
    }

    #[test]
    fn brackets_typed_into_a_note_link_it_by_id() {
        let mut s = session();
        put(&mut s, "prog", "Kettlebell Programme\nweek one", &[], 2_000);
        let typed = "follow [[kettlebell programme]] and [[Nowhere]] today";
        add(&mut s, typed);
        let new = s.rows[0].id.clone();
        let held = s.notes.get(&new).unwrap();
        assert_eq!(held.links, ["prog"]);
        assert_eq!(held.text, typed, "the text is left as typed");
        go_into(&mut s, "Kettlebell Programme");
        assert!(s.rows[0].linked && s.rows[0].reasons[0].starts_with("links here"));

        // Renamed, the target is still linked, and the note shows its new head.
        let prog = s.notes.get("prog").unwrap().clone();
        s.commit(Record {
            text: "Strength Programme\nweek one".into(),
            ..prog
        });
        s.act(Action::Back);
        let row = s.rows.iter().find(|r| r.id == new).unwrap();
        assert_eq!(row.links, ["Strength Programme"]);
    }

    #[test]
    fn linking_outside_a_note_does_nothing_but_say_so() {
        let mut s = session();
        put(&mut s, "a", "kettlebell swings", &[], 2_000);
        put(&mut s, "b", "kettlebell press", &[], 1_000);
        type_in(&mut s, "kettlebell day");
        select(&mut s, "kettlebell swings");
        s.act(Action::Link);
        assert!(s.notes.records().all(|r| r.links.is_empty()));
        assert_eq!(s.undo.len(), 2, "nothing was written");
        assert!(s.status.starts_with("go into a note first"), "{}", s.status);
        assert_eq!(s.line, "kettlebell day", "the line is left alone");

        s.act(Action::Select(None));
        s.act(Action::Write);
        let new = s
            .notes
            .records()
            .find(|r| r.text == "kettlebell day")
            .unwrap();
        assert!(
            new.links.is_empty(),
            "nor is anything kept for the note being written"
        );
    }

    #[test]
    fn coming_back_never_throws_away_what_you_typed() {
        let mut s = session();
        put(&mut s, "hub", "trainingapp\nthe plan", &[], 3_000);
        put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);

        // Gone into from an empty line.
        go_into(&mut s, "trainingapp");
        type_in(&mut s, "deload next week");
        s.act(Action::Back);
        assert!(s.pivots.is_empty());
        assert_eq!(s.line, "deload next week");

        // Gone into by a search.
        clear(&mut s);
        type_in(&mut s, "trai");
        go_into(&mut s, "trainingapp");
        type_in(&mut s, "deload next week");
        s.act(Action::Back);
        assert_eq!(s.line, "deload next week", "the draft, not the search");
        assert_eq!(s.cursor, None);

        // With the line emptied, the search comes back.
        clear(&mut s);
        type_in(&mut s, "trai");
        go_into(&mut s, "trainingapp");
        type_in(&mut s, "oops");
        clear(&mut s);
        s.act(Action::Back);
        assert_eq!(s.line, "trai");
    }

    #[test]
    fn a_search_clears_on_the_way_in_and_comes_back_on_the_way_out() {
        let mut s = session();
        put(&mut s, "hub", "trainingapp\nthe plan", &[], 3_000);
        put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);
        type_in(&mut s, "trai");
        go_into(&mut s, "trainingapp");
        assert_eq!(s.pivots.len(), 1);
        assert_eq!(s.line, "");
        assert_eq!(heads(&s), ["trainingapp plan ideas"]);

        s.act(Action::Back);
        assert!(s.pivots.is_empty());
        assert_eq!(s.line, "trai");
        assert_eq!(s.cursor, None, "on the line");
    }

    #[test]
    fn the_name_of_a_note_finds_what_was_written_in_it() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        put(&mut s, "quote", "engineer quoted 240", &["boiler"], 2_000);
        put(
            &mut s,
            "pressure",
            "pressure dropped again",
            &["boiler"],
            3_000,
        );
        put(&mut s, "far", "cake recipe", &[], 4_000);

        type_in(&mut s, "boiler");
        assert_eq!(heads(&s)[0], "Boiler", "the note so named comes first");
        assert_eq!(s.rows[0].reasons[0], "its name", "{:?}", s.rows[0].reasons);
        assert!(s.rows[0].reasons.len() > 1, "and then what it scored for");
        assert_eq!(heads(&s).len(), 3);
        let row = s.rows.iter().find(|r| r.id == "quote").unwrap();
        assert!(
            row.reasons.iter().any(|r| r.contains("about Boiler")),
            "{:?}",
            row.reasons
        );
        assert_eq!(row.links, ["Boiler"], "and the row says so too");

        // Going into a note found that way was a search for it, though it does not say the word.
        go_into(&mut s, "pressure dropped again");
        assert_eq!(s.pivots.len(), 1);
        assert_eq!(s.line, "");
        s.act(Action::Back);
        assert_eq!(s.line, "boiler");

        // So was one found by a word of its own and the name of what it is about.
        clear(&mut s);
        type_in(&mut s, "boiler quoted");
        assert_eq!(heads(&s)[0], "engineer quoted 240");
        s.act(Action::Move(1));
        s.act(Action::GoIn);
        assert_eq!(s.line, "");
        s.act(Action::Back);

        // Something new said about the boiler is a draft, and goes in with you.
        clear(&mut s);
        type_in(&mut s, "boiler needs a service");
        go_into(&mut s, "Boiler");
        assert_eq!(s.line, "boiler needs a service");
    }

    /// A hub with a neighbourhood of three: two notes that share words with the draft below, and
    /// one that shares nothing with it.
    fn hub_with_neighbours(s: &mut Session) {
        put(
            s,
            "profile",
            "Profile\ntrainingapp numbers and plan",
            &[],
            2_000,
        );
        put(s, "squats", "Squat log\ntrainingapp sets", &["hub"], 2_500);
        put(s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
        put(s, "near", "trainingapp plan ideas", &[], 1_000);
    }

    const DRAFT: &str = "the plan needs a deload week";

    #[test]
    fn a_draft_comes_with_you_and_is_written_where_you_land() {
        let mut s = session();
        hub_with_neighbours(&mut s);
        go_into(&mut s, "trainingapp");
        assert_eq!(
            heads(&s),
            ["Squat log", "Profile", "trainingapp plan ideas"]
        );
        s.act(Action::Back);

        type_in(&mut s, DRAFT);
        go_into(&mut s, "trainingapp");
        assert_eq!(s.pivots.len(), 1);
        assert_eq!(s.line, DRAFT, "the draft came along");
        assert_eq!(s.cursor, None);
        assert_eq!(
            heads(&s),
            ["trainingapp plan ideas", "Profile"],
            "the neighbourhood, narrowed by the draft"
        );

        s.act(Action::Write);
        let new = s.notes.records().find(|r| r.text == DRAFT).unwrap();
        assert_eq!(new.links, ["hub"]);
        assert_eq!(s.line, "");
        assert_eq!(heads(&s)[0], DRAFT, "at the top of the linked part");

        s.act(Action::Back);
        assert!(s.pivots.is_empty());
        assert_eq!(s.line, "", "the draft was written, so it is not put back");
    }

    #[test]
    fn a_draft_taken_into_a_note_and_back_out_is_still_on_the_line() {
        let mut s = session();
        hub_with_neighbours(&mut s);
        type_in(&mut s, DRAFT);
        go_into(&mut s, "trainingapp");
        type_in(&mut s, "!");
        s.act(Action::Back);
        assert!(s.pivots.is_empty());
        assert_eq!(s.line, format!("{DRAFT}!"), "as it is now, not as it was");
        assert_eq!(s.cursor, None);
        assert!(
            heads(&s).contains(&"trainingapp"),
            "and what it finds is listed"
        );
    }

    #[test]
    fn only_what_applies_is_offered() {
        let mut s = session();
        assert_eq!(s.view().notes, 0);
        assert_eq!(
            labels(&s),
            ["writes", "show archived"],
            "nothing listed, so nothing to pick"
        );
        put(
            &mut s,
            "profile",
            "Profile\ntrainingapp numbers",
            &[],
            2_000,
        );
        assert_eq!(s.view().notes, 1);
        put(
            &mut s,
            "video",
            "Squat form video\nhttps://example.com/squat for trainingapp",
            &["hub"],
            2_500,
        );
        put(&mut s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
        put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);
        s.say("saved");
        assert_eq!((s.view().status, s.view().notes), ("saved", 4));
        assert_eq!(
            labels(&s),
            ["writes", "picks a note", "show archived", "undo"]
        );
        assert_eq!(s.view().hints[1].action, Action::Move(1));
        select(&mut s, "trainingapp");
        assert_eq!(
            labels(&s),
            ["opens", "edit", "archive", "delete", "copy", "undo", "up"]
        );
        assert_eq!(
            s.view().hints.last().unwrap().action,
            Action::Select(None),
            "the way back is last"
        );

        s.act(Action::GoIn);
        assert_eq!(
            labels(&s),
            [
                "writes here",
                "picks a note",
                "show archived",
                "undo",
                "back"
            ]
        );
        assert_eq!(s.view().hints.last().unwrap().action, Action::Back);
        select(&mut s, "Profile");
        assert_eq!(
            labels(&s),
            [
                "opens", "unlink", "edit", "archive", "delete", "copy", "undo", "up"
            ]
        );
        select(&mut s, "Squat form video");
        assert_eq!(
            labels(&s),
            [
                "opens", "unlink", "edit", "archive", "delete", "copy", "open URL", "undo", "up"
            ]
        );
        select(&mut s, "trainingapp plan ideas");
        assert_eq!(labels(&s)[..2], ["opens", "link"]);

        // The most there is to offer: a linked, archived note with a URL, with undo and redo to
        // hand.
        s.act(Action::ShowArchived);
        select(&mut s, "Squat form video");
        s.act(Action::Archive);
        select(&mut s, "trainingapp plan ideas");
        s.act(Action::Link);
        s.act(Action::Undo);
        s.act(Action::Select(None));
        assert_eq!(
            labels(&s),
            [
                "writes here",
                "picks a note",
                "hide archived",
                "undo",
                "redo",
                "back"
            ]
        );
        select(&mut s, "Squat form video");
        assert_eq!(
            labels(&s),
            [
                "opens",
                "unlink",
                "edit",
                "unarchive",
                "delete",
                "copy",
                "open URL",
                "undo",
                "redo",
                "up"
            ]
        );
    }

    #[test]
    fn a_date_word_searches_as_its_date_and_is_written_as_one() {
        let mut s = session();
        add(&mut s, "dentist 2026-10-16");
        add(&mut s, "something else entirely");

        // On the line the word stays as typed, the view says the date it stands for, and the
        // search already uses the date.
        type_in(&mut s, "/fri");
        assert_eq!(s.line, "/fri");
        assert_eq!(s.view().dates, [time::day_from_civil(2026, 10, 16)]);
        assert_eq!(heads(&s), ["dentist 2026-10-16"]);

        // Going into the note it found is a search, so the line clears.
        s.act(Action::Move(1));
        s.act(Action::GoIn);
        assert_eq!(s.line, "");
        s.act(Action::Back);
        assert_eq!(s.line, "/fri");

        // Writing the line writes the date.
        add(&mut s, "book the car in /tom, not /today's");
        assert!(heads(&s).contains(&"book the car in 2026-10-14, not /today's"));
        s.act(Action::Type("no dates here /home/sam".into()));
        assert!(s.view().dates.is_empty());
    }

    #[test]
    fn an_edit_goes_out_as_a_request_and_comes_back_as_a_save() {
        let mut s = session();
        put(&mut s, "prog", "Kettlebell Programme\nweek one", &[], 2_000);
        put(&mut s, "plan", "the plan", &[], 3_000);
        assert!(
            s.act(Action::Edit)
                .iter()
                .all(|e| matches!(e, Effect::Keep(_))),
            "on the line there is no note to edit"
        );
        select(&mut s, "the plan");
        assert_eq!(
            s.act(Action::Edit),
            [Effect::Edit {
                id: "plan".into(),
                text: "the plan".into()
            }]
        );
        let save = |s: &mut Session, text: &str| {
            s.act(Action::Save {
                id: "plan".into(),
                text: text.into(),
            })
        };

        assert!(save(&mut s, "the plan\n").is_empty());
        assert_eq!(
            s.status, "unchanged",
            "an editor's last newline is not a change"
        );
        assert!(save(&mut s, " \n").is_empty());
        assert!(s.status.starts_with("empty, so left unchanged"));
        assert_eq!(s.notes.get("plan").unwrap().text, "the plan");

        let kept = save(
            &mut s,
            "The plan\nfollow [[kettlebell programme]] from /tom\n",
        );
        let held = s.notes.get("plan").unwrap().clone();
        assert_eq!(kept, [Effect::Keep(held.clone())]);
        assert_eq!(
            held.text,
            "The plan\nfollow [[kettlebell programme]] from 2026-10-14"
        );
        assert_eq!(held.links, ["prog"]);
        assert_eq!(held.updated, Some(NOON), "an edit is dated");
        assert_eq!(held.created, Some(Moment::utc(3_000)));
        assert_eq!(s.status, "saved");
        assert_eq!(
            s.selected().unwrap().head,
            "The plan",
            "the cursor stays on the note"
        );

        // Taking the brackets out takes the link away, and undo puts both back.
        save(&mut s, "The plan");
        assert!(s.notes.get("plan").unwrap().links.is_empty());
        s.act(Action::Undo);
        assert_eq!(s.notes.get("plan").unwrap().links, ["prog"]);

        s.act(Action::Delete);
        assert!(save(&mut s, "too late").is_empty());
        assert!(s.status.starts_with("that note is gone"), "{}", s.status);
    }

    #[test]
    fn copying_and_opening_are_left_to_the_face() {
        let mut s = session();
        put(
            &mut s,
            "video",
            "Squat form video\nhttps://example.com/squat",
            &[],
            2_000,
        );
        put(&mut s, "plan", "the plan", &[], 3_000);
        assert!(
            s.act(Action::Copy)
                .iter()
                .all(|e| matches!(e, Effect::Keep(_))),
            "on the line there is no note to copy from"
        );

        select(&mut s, "the plan");
        assert_eq!(s.act(Action::Copy), [Effect::Copy("the plan".into())]);
        assert_eq!(s.status, "copied head: the plan");
        assert!(s.act(Action::OpenUrl).is_empty());
        assert_eq!(s.status, "no URL in that note");

        select(&mut s, "Squat form video");
        let url = "https://example.com/squat";
        assert_eq!(s.act(Action::Copy), [Effect::Copy(url.into())]);
        assert_eq!(s.status, format!("copied URL: {url}"));
        assert_eq!(s.act(Action::OpenUrl), [Effect::Open(url.into())]);
        assert_eq!(s.status, format!("opened {url}"));
    }

    #[test]
    fn deleting_and_undoing_ask_for_a_version_each() {
        let mut s = session();
        let written = add(&mut s, "kettlebell swings");
        s.act(Action::Move(1));
        let deleted = s.act(Action::Delete);
        assert!(heads(&s).is_empty());
        assert_eq!(s.view().notes, 0);
        assert!(matches!(&deleted[..], [Effect::Keep(r)] if r.deleted));

        let undone = s.act(Action::Undo);
        assert_eq!(heads(&s), ["kettlebell swings"]);
        assert_eq!(s.status, "undid: kettlebell swings");
        let redone = s.act(Action::Redo);
        assert!(heads(&s).is_empty());
        assert!(s.act(Action::Redo).is_empty());
        assert_eq!(s.status, "nothing to redo");

        let log: Vec<Effect> = [written, deleted, undone, redone].concat();
        assert_eq!(log.len(), 4);
        assert!(reopened(&log).is_empty());
        assert!(reopened(log.iter().rev()).is_empty());
    }

    #[test]
    fn a_space_opens_with_the_notes_it_is_given() {
        let mut notes = Notes::new();
        for (id, text, updated, archived) in [
            ("a", "kettlebell swings", 2_000, false),
            ("b", "kettlebell press", 3_000, false),
            ("c", "kettlebell halo", 4_000, true),
        ] {
            notes.apply(Record {
                id: id.into(),
                text: text.into(),
                links: Vec::new(),
                created: None,
                updated: Some(Moment::utc(updated)),
                stamp: None,
                archived,
                deleted: false,
                device: "other".into(),
            });
        }
        let mut s = Session::new(
            Box::new(Stopped::default()),
            "dev",
            Weights::default(),
            notes,
        );
        assert_eq!(heads(&s), ["kettlebell press", "kettlebell swings"]);
        assert_eq!(s.view().notes, 3, "archived notes are still notes");
        s.act(Action::Type("swi".into()));
        assert_eq!(heads(&s), ["kettlebell swings"], "and they are indexed");
    }

    #[test]
    fn actions_and_the_view_go_over_a_wire_as_json() {
        for (action, json) in [
            (Action::Write, r#""write""#),
            (Action::GoIn, r#""go_in""#),
            (Action::ShowArchived, r#""show_archived""#),
            (Action::Type("a line".into()), r#"{"type":"a line"}"#),
            (Action::Move(-1), r#"{"move":-1}"#),
            (Action::Select(None), r#"{"select":null}"#),
            (Action::Select(Some(3)), r#"{"select":3}"#),
            (
                Action::Save {
                    id: "n1".into(),
                    text: "a\nb".into(),
                },
                r#"{"save":{"id":"n1","text":"a\nb"}}"#,
            ),
        ] {
            assert_eq!(serde_json::to_string(&action).unwrap(), json);
            assert_eq!(serde_json::from_str::<Action>(json).unwrap(), action);
        }

        let mut s = session();
        put(&mut s, "hub", "Boiler\nthe plan", &[], 3_000);
        s.act(Action::Move(1));
        let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
        assert_eq!(view["line"], "");
        assert_eq!(view["cursor"], 0);
        assert_eq!(view["rows"][0]["head"], "Boiler");
        assert_eq!(view["rows"][0]["has_body"], true);
        assert_eq!(view["rows"][0]["updated"], "1970-01-01T00:50:00Z");
        assert_eq!(view["preview"]["text"], "Boiler\nthe plan");
        assert_eq!(view["notes"], 1);
        // A hint's action is what to send back to ask for it.
        assert_eq!(view["hints"][0]["label"], "opens");
        assert_eq!(view["hints"][0]["action"], "go_in");
        let last = view["hints"].as_array().unwrap().last().unwrap();
        assert_eq!(last["action"], serde_json::json!({"select": null}));

        let effect = Effect::Edit {
            id: "hub".into(),
            text: "Boiler".into(),
        };
        assert_eq!(
            serde_json::to_string(&effect).unwrap(),
            r#"{"edit":{"id":"hub","text":"Boiler"}}"#
        );
    }

    #[test]
    fn heads_and_bodies() {
        assert!(has_body("head\nbody"));
        assert!(!has_body("head\n  \n"));
        assert!(!has_body("head\n#house #plan\n"));
        assert!(has_body("head\n#house\n- and more"));
        assert!(has_body("head\n# A heading"));
        assert!(!has_body("head"));
    }
}
