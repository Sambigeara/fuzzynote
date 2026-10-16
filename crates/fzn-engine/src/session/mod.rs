//! The session: the app itself, with no screen and no keyboard.
//!
//! There is one line and one list. The list shows the top of the space while the line is empty,
//! in the order its notes are kept in, and what the engine finds as it is typed, each with its
//! reasons. On the line words find and everything else holds (see `query`): a phrase, a tag, a
//! person, a day and a span narrow what is listed and never how it ranks, and a line of nothing
//! but those lists exactly what holds them, in the order the top of the space is in. A note is
//! found by its own words and by the head of a note it links to, so the name of a note brings
//! what was written in it, as going into the note does. The cursor is on the line or on a note of
//! the list, and what is typed goes where the cursor is. Archived notes stay out of every list
//! until they are asked for.
//!
//! Going into a note makes it the place, and the path grows by one step. A place is a set and is
//! listed as the top of the space is: with nothing on the line, what is in it, which for a note is
//! what is linked to or from it, in the order the space keeps; with something on the line, what
//! that finds, the place's own notes first and then the rest of its neighbourhood, each best first
//! with its reasons. Where you are is what you are talking about, so a note written there is linked
//! to it. The line comes with you under one rule: if the note contains what was typed, or holds
//! what the line holds and its head and the heads of the notes it links to have every word of it
//! outside its phrases, that was a search, so the line clears, and coming back puts it on an
//! empty line again; otherwise it was a draft, and it stays on the line to be written there.
//! Stepping back never throws away what is on the line; clearing it is asked for apart, and
//! offered as the way back while there is something on it. Going into a place already on the
//! path goes back to it, so the path never repeats. Linking or unlinking a note of the list is a
//! correction. A `[[head]]` typed into a note is one more way of making a link: saving the note
//! links it to the note so headed. However it was made, a link is the target's id held on the
//! note.
//!
//! A tag, a person or a day is a place too, gone into from the line when the line is labels and
//! nothing else, each in turn, as a note is gone into from the line when the line is its name:
//! either way the line was a search for the place, so it clears and comes back on the way out. A
//! span is not a place, since a note written now could not be written in a span gone by. What is
//! in a label is every note carrying it, in the space's order, and typing finds among those first
//! and then among what the label's words find. A note written there ends its head with the
//! label, as it would have been typed, so that it reads as one written with the label on the line
//! always has. A new row opens with the label already in its head and the caret before it, so
//! that what the writer sees is what is written, and the note is in its place on every device
//! from its first letter. Only a note written in the place inherits it. Since the place says its
//! labels, a row there that is not under the cursor shows its head without them where they end
//! it, when the note is a head only, and the path names a note as its row read where it was gone
//! into from. A head is named without the labels that end it too, so the words a row reads go
//! into it.
//!
//! Labels gone into one after another are one place, so that going into today from inside a
//! tag is today's notes in that tag: what is in it is the notes carrying every one of them, and
//! a note written there ends its head with each it does not carry already, in the order of the
//! path. The place is the last note on the path, if there is one, together with the labels gone
//! into after it, so that `Boiler › today` is Boiler's notes from today and a note written there
//! links to Boiler and is of today. Labels before the last note are only how it was reached, and
//! a note gone into starts a new place.
//!
//! A task is a note whose head starts with a box, `[ ]` while it is open and `[x]` once it is
//! done. The box is a label too, of the open tasks, gone into as a tag is, and a note written
//! there starts with it, since that is where a box is written. Archiving a task ticks it and
//! bringing it back unticks it, in one version, so that no device sees the tick without the
//! archiving.
//!
//! A date is plain text, written `2026-10-16`. A word such as `/fri` or `/tomorrow` is a quicker
//! way of typing one: the view says the date it stands for, on the line it holds the notes of
//! that day as the date written out does, and saving a note writes the date in its place.
//!
//! The row the cursor is on is a field: its head is there to be typed into the moment the cursor
//! lands, and its body is opened for the rest. The face says what the note now reads as it is
//! typed, and every change to a head is written at once as a version, so that there is nothing
//! to press to keep a thing and the note is on every device as it is typed. Date words are
//! written out when the cursor leaves the row, so that a word on its way to being one is not
//! made a date, and a head cleared to be typed again is not written while it is empty. The body
//! is written when it closes, since a version is the whole note. The versions written in one
//! visit of the cursor to a row are gathered into one entry, so that undo takes back a visit
//! and not a keystroke. A new note, opened at the top from an empty line or under the row the
//! cursor is on, is written at its first letter, and left with nothing in it but the label it
//! opened with, its row goes. The list holds still while the cursor walks it, so that the arrow
//! that leaves a head lands where it was aimed, and is put in order when the cursor comes back
//! to the line.
//!
//! Where a note sits is kept, as it was in the original: a note opened under a row is placed
//! after it, one opened or written from the line at the top, and it stays there on every device.
//! An edit never moves a note; moving one is asked for, a row at a time, and what was written
//! under it stays where it was.
//!
//! A face owns none of this. It says what was asked for, as an [`Action`], carries out the
//! [`Effect`]s that come back, and paints the [`View`]. Which key or gesture asks for what is the
//! face's to decide, and so is editing the line: a terminal has to do that itself and a browser
//! is given it, so the session is only ever told what the line now reads. A face that is
//! reloaded under the person keeps where they were as the path's steps, and asks to be put back
//! there, so the engine gives the path as steps and takes it back as an action.

use serde::{Deserialize, Serialize};

use crate::engine::Engine;
use crate::query::Query;
use crate::record::{Notes, Record};
use crate::score::Weights;
use crate::text;
use crate::time::{Day, Moment};

mod field;
mod list;
mod path;
mod undo;

use field::Writing;
use path::Step;
use undo::Change;

const TOP_LIMIT: usize = 500;
const FOUND_LIMIT: usize = 60;
const PLACE_LIMIT: usize = 200;

/// What a session needs from the machine it runs on.
pub trait Host {
    /// The time, with this machine's offset from UTC.
    fn now(&self) -> Moment;

    /// An id for a new note: unique across devices, and sorting after the ids made before it.
    fn new_id(&self) -> String;

    /// Fills `buf` from the machine's source of randomness. For nonces and new writer ids.
    fn random(&self, buf: &mut [u8]);
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
    /// Move the note under the cursor down the list by this many rows, or up it, and keep the
    /// cursor on it. Only where the list is in the order notes are kept in: with nothing on the
    /// line, at the top or in a place.
    Shift(i64),
    /// Turn the line into a note, linked to the note being looked around, and put the cursor on
    /// it. With nothing on the line, open a new note at the top of the list to write in place.
    /// On a row, write out its date words and open a new note under it to write in place.
    Write,
    /// Go into the note under the cursor. On the line, go into what the line names: each tag,
    /// person or day of a line of nothing else, one after another, or else the one note it is the
    /// name of.
    GoIn,
    /// Step back out of the place being looked around, onto the line.
    Back,
    /// Open the body of the note under the cursor, so that the field holds the whole note.
    Edit,
    /// The note under the cursor now reads this. The cursor stays on it. A head is written at
    /// once, as typed, unless it is empty; an open body is written when it closes.
    Edited(String),
    /// Write the note under the cursor as it is to be kept, with its date words written out,
    /// and close its body. The cursor stays: a note emptied is deleted, and a new note with
    /// nothing in it stays as an empty row, what was kept of it as it was typed taken back.
    Done,
    /// Write the note under the cursor as it stands, with its date words written out and its
    /// body, if it is open, kept as a version, and stay: the body stays open and the cursor
    /// where it is. What a face asks before it is hidden or reloaded, and vim's `:w`.
    Keep,
    /// Hand the note under the cursor to the face's editor, as it now reads. Comes back as
    /// [`Effect::Editor`], and what the editor leaves comes back as [`Action::Save`].
    Editor,
    /// The text of a note as the editor left it.
    Save {
        id: String,
        text: String,
    },
    /// Archive the note under the cursor, or bring it back. A task is ticked as it is archived
    /// and unticked as it is brought back.
    Archive,
    /// Delete the note under the cursor.
    Delete,
    /// Take away the note under the cursor if it has no text, and go up to the row above or
    /// onto the line: Backspace at the start of an empty row, which takes the row away as it
    /// would an empty line. A note with a body stays.
    Erase,
    /// Link the note under the cursor to the one being looked around, or take that link away.
    /// In labels with no note before them there is nothing to link to.
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
    /// Go back to where a face kept you: these steps from the top, each gone into as it would be
    /// from the line, passing over a note that has gone, a label that is not one, and a place
    /// already on the path. The line clears, and the cursor is on it.
    Restore(Vec<KeptStep>),
}

/// A step of the path as a face keeps it across a reload: the note gone into, by its id, or the
/// label as it was typed, with the search the step was found with, if it was one. As JSON,
/// `{"note":"3f9a01c2-0001","search":"boi"}` or `{"label":"#house"}`. It is apart from the
/// path's own step, which holds what the session reads off the note and the label as well.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KeptStep {
    Note {
        note: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        search: Option<String>,
    },
    Label {
        label: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        search: Option<String>,
    },
}

impl Action {
    /// Whether this action can change a note, and so needs the key verified. Typing into a
    /// head or opening a body begins a change, so it is refused with the writing of it: nothing
    /// is changed that could not then be written.
    pub fn writes(&self) -> bool {
        match self {
            Action::Write
            | Action::Edit
            | Action::Edited(_)
            | Action::Editor
            | Action::Done
            | Action::Keep
            | Action::Save { .. }
            | Action::Archive
            | Action::Delete
            | Action::Erase
            | Action::Link
            | Action::Shift(_)
            | Action::Undo
            | Action::Redo => true,
            Action::Type(_)
            | Action::Move(_)
            | Action::Select(_)
            | Action::GoIn
            | Action::Back
            | Action::Copy
            | Action::OpenUrl
            | Action::ShowArchived
            | Action::ShowScores
            | Action::Restore(_) => false,
        }
    }
}

/// What a face has to do about an action. The session has already done its part.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// A version of a note to keep, as one more line of this device's log.
    Keep(Record),
    /// A note to put in front of the person in the face's editor. What they make of it comes
    /// back as [`Action::Save`].
    Editor { id: String, text: String },
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
    /// The head as the row shows it when the cursor is not on it, where that differs from the
    /// head: in a place, with the place's labels taken off its end, when the note is a head only
    /// and they end it. None when it reads as it is. The row under the cursor shows its text as
    /// it is, whatever this says.
    pub shown: Option<String>,
    pub archived: bool,
    /// Whether there is more to read than the head.
    pub has_body: bool,
    /// Whether the note is a fact of the place being looked around: linked to or from its note
    /// and carrying its labels. With nothing on the line a place lists only its facts; with
    /// something on it they come before what is only found there.
    pub linked: bool,
    /// The heads, as they are now, of the notes this one is linked to by id. The note being
    /// looked around is left out, since everything on the list is about it.
    pub links: Vec<String>,
    /// Why the note is listed.
    pub reasons: Vec<String>,
    pub score: Option<f32>,
    pub updated: Option<Moment>,
}

/// The note under the cursor as it now reads. There is one whenever the cursor is on a row.
#[derive(Debug, Serialize)]
pub struct Field<'a> {
    pub text: &'a str,
    /// Where the caret belongs, in characters, in a new note with nothing in it but the labels it
    /// opened with, whose row goes if the cursor leaves it so: after a task's box and before the
    /// labels at the end. Backspace there takes the row away. None for any other note.
    pub fresh: Option<usize>,
    /// The body is open: the text is to be shown and written whole, not only its head.
    pub whole: bool,
}

/// What to show.
#[derive(Debug, Serialize)]
pub struct View<'a> {
    /// Where you are, outermost first: the heads of the notes gone into, and the labels as they
    /// were typed, `#house`, `@joe` or `2026-10-16`.
    pub path: Vec<&'a str>,
    pub line: &'a str,
    /// What the date words and spans on the line stand for, in the order they were typed:
    /// `Fri 2026-10-16`, `Mon 2026-10-12 18:00 – now`.
    pub dates: Vec<String>,
    /// The list: what is in the place on the path, or what the line finds there or at the top,
    /// or the top of the space when there is neither.
    pub rows: &'a [Row],
    /// The row the cursor is on. With none, it is on the line.
    pub cursor: Option<usize>,
    /// What just happened.
    pub status: &'a str,
    /// How many notes there are.
    pub notes: usize,
    /// What can be asked for where the cursor is. The way back is last.
    pub hints: Vec<Hint>,
    pub archived_shown: bool,
    pub scores_shown: bool,
    /// The note being looked around, the last note on the path, so that a face can show what it
    /// says. None when there is no note on the path: labels alone have nothing to say.
    pub here: Option<&'a Record>,
    /// The note under the cursor as it is being written, if the cursor is on a row.
    pub field: Option<Field<'a>>,
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
    /// The path, the last step gone into last.
    path: Vec<Step>,
    /// Changes to take back, newest last.
    undo: Vec<Change>,
    redo: Vec<Change>,
    /// The note under the cursor as it is being written. There is one if and only if the cursor
    /// is on a row, and it is for that row.
    field: Option<Writing>,
    /// The list has something to catch up on: a head written from a row, or versions arrived
    /// while the cursor was on the list. Put right when the cursor returns to the line, or when
    /// a chord rebuilds the list.
    stale: bool,
    /// What the action being carried out asks of the face.
    out: Vec<Effect>,
}

impl Session {
    /// Opens a space. `device` names this device in the versions it writes, and breaks ties
    /// between versions written in the same millisecond.
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
            path: Vec::new(),
            undo: Vec::new(),
            redo: Vec::new(),
            field: None,
            stale: false,
            out: Vec::new(),
        };
        session.refresh(true);
        session
    }

    /// Versions written from now on name this writer.
    pub fn writes_as(&mut self, device: impl Into<String>) {
        self.device = device.into();
    }

    /// The writer the versions this session writes name.
    pub fn device(&self) -> &str {
        &self.device
    }

    /// Does what was asked, and returns what the face has to do about it.
    pub fn act(&mut self, action: Action) -> Vec<Effect> {
        if let Some(action) = self.on_field(action) {
            self.carry_out(action);
        }
        self.land();
        std::mem::take(&mut self.out)
    }

    fn carry_out(&mut self, action: Action) {
        match action {
            Action::Type(line) => {
                self.line = line;
                self.refresh(true);
            }
            Action::Move(by) => self.step(by),
            Action::Select(None) => self.onto_line(),
            Action::Select(Some(row)) => {
                if row < self.rows.len() {
                    self.cursor = Some(row);
                }
            }
            Action::Write => self.write_line(),
            Action::GoIn => self.go_in(),
            Action::Back => self.back_to(self.path.len().saturating_sub(1)),
            // On the line there is no note to open, change, write, hand to the editor or take
            // away.
            Action::Edit
            | Action::Edited(_)
            | Action::Done
            | Action::Keep
            | Action::Editor
            | Action::Erase => {}
            Action::Save { id, text } => self.save(&id, &text),
            Action::Archive => self.archive(),
            Action::Delete => self.delete(),
            Action::Link => self.link(),
            Action::Shift(by) => self.shift(by),
            Action::Copy => self.copy(),
            Action::OpenUrl => self.open_url(),
            Action::Undo => self.undo(),
            Action::Redo => self.redo(),
            Action::ShowArchived => {
                self.show_archived = !self.show_archived;
                self.refresh(false);
            }
            Action::ShowScores => self.show_scores = !self.show_scores,
            Action::Restore(steps) => self.restore_path(steps),
        }
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
                    self.engine.remove(id);
                }
            }
        }
        // The list holds still while the cursor walks it, so that a row does not move or vanish
        // under it; what arrived is listed when the cursor comes back to the line. A version of
        // the note under the cursor shows at once, if its head has not been changed here.
        if self.cursor.is_some() {
            self.stale = true;
            self.land();
        } else {
            self.refresh(false);
        }
        self.status = format!("{} changed elsewhere", changed.len());
        true
    }

    /// Puts a message where the session puts its own, for what only the face can know: that an
    /// effect could not be carried out, say.
    pub fn say(&mut self, status: impl Into<String>) {
        self.status = status.into();
    }

    /// Where you are, as a face keeps it across a reload: each step of the path, outermost
    /// first, to give back as [`Action::Restore`].
    pub fn steps(&self) -> Vec<KeptStep> {
        self.path.iter().map(Step::kept).collect()
    }

    pub fn view(&self) -> View<'_> {
        View {
            path: self.path.iter().map(Step::name).collect(),
            line: &self.line,
            dates: Query::read(&self.line, self.today()).shown(),
            rows: &self.rows,
            cursor: self.cursor,
            status: &self.status,
            notes: self.notes.len(),
            hints: self.hints(),
            archived_shown: self.show_archived,
            scores_shown: self.show_scores,
            here: self.place_note().and_then(|id| self.notes.get(id)),
            field: self.field.as_ref().map(|field| Field {
                text: &field.text,
                fresh: field.blank().then(|| field.start()),
                whole: field.whole,
            }),
        }
    }

    /// Today, by the host's clock.
    fn today(&self) -> Day {
        self.host.now().day()
    }

    /// What can be asked for where the cursor is. The way back is always last.
    fn hints(&self) -> Vec<Hint> {
        let zoomed = !self.path.is_empty();
        let mut hints = Vec::new();
        let mut offer = |action, label| hints.push(Hint { action, label });
        let field = self.field.as_ref();
        let changed = field.is_some_and(Writing::differs);
        let blank = field.is_some_and(Writing::blank);
        // An open body offers only what is about the note, and closing it is the way back.
        if field.is_some_and(|f| f.whole) {
            if changed {
                offer(Action::Undo, "put back");
            }
            if !blank {
                offer(Action::Delete, "delete");
            }
            offer(Action::Done, "done");
            return hints;
        }
        match (self.selected(), field) {
            (Some(row), Some(field)) => {
                // A new note with nothing in it is already an empty row to write in.
                if !blank {
                    offer(
                        Action::Write,
                        if zoomed { "new note here" } else { "new note" },
                    );
                }
                offer(Action::Edit, "body");
                offer(Action::Editor, "editor");
                // A new note with nothing in it has nowhere to go in to and nothing to act on.
                if !blank {
                    offer(Action::GoIn, "go in");
                    // Labels alone have nothing to link to: what is written there carries them.
                    // Nor has the place's own note, listed when the line names it.
                    if let Some(around) = self.place_note().filter(|&around| *around != row.id) {
                        let holds = |from: &String, to: &String| {
                            self.notes.get(from).is_some_and(|n| n.links.contains(to))
                        };
                        let linked = holds(&row.id, around) || holds(around, &row.id);
                        offer(Action::Link, if linked { "unlink" } else { "link" });
                    }
                    // A task is ticked by archiving it, and says so.
                    offer(
                        Action::Archive,
                        match (text::task(&field.text).is_some(), row.archived) {
                            (true, false) => "tick",
                            (true, true) => "untick",
                            (false, false) => "archive",
                            (false, true) => "unarchive",
                        },
                    );
                    offer(Action::Delete, "delete");
                    offer(Action::Copy, "copy");
                    // Only the one note under the cursor is read for a URL, once per view.
                    if !text::facts(&field.text).urls.is_empty() {
                        offer(Action::OpenUrl, "open URL");
                    }
                    let rows = self.in_order();
                    if let Some(i) = self.cursor.filter(|&i| i < rows) {
                        if i > 0 {
                            offer(Action::Shift(-1), "move up");
                        }
                        if i + 1 < rows {
                            offer(Action::Shift(1), "move down");
                        }
                    }
                }
            }
            _ => {
                let label = match (self.line.trim().is_empty(), zoomed) {
                    (true, false) => "new note",
                    (true, true) => "new note here",
                    (false, false) => "write",
                    (false, true) => "write here",
                };
                offer(Action::Write, label);
                if !self.places_named().is_empty() {
                    offer(Action::GoIn, "go in");
                }
                if !self.rows.is_empty() {
                    offer(Action::Move(1), "pick a note");
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
        }
        // Undo puts back a head changed in this visit, and otherwise takes back the last change,
        // and says which it will be. Redo is not offered over a change, since the change cleared
        // what there was to redo. A new note is the change, so only putting it back is offered
        // there.
        if changed {
            offer(Action::Undo, "put back");
        } else if !blank && !self.undo.is_empty() {
            offer(Action::Undo, "undo");
        }
        if !changed && !blank && !self.redo.is_empty() {
            offer(Action::Redo, "redo");
        }
        // Off the row, then off the text, then out of the place.
        match self.cursor {
            Some(_) => offer(Action::Select(None), "up"),
            None if !self.line.is_empty() => offer(Action::Type(String::new()), "clear"),
            None if zoomed => offer(Action::Back, "back"),
            None => {}
        }
        hints
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
}

#[cfg(test)]
mod tests;
