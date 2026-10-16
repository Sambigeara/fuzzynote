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
//! from its first letter. Only a note written in the place inherits it.
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
//! is given it, so the session is only ever told what the line now reads.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::engine::{self, Engine, Hit, SearchOptions};
use crate::note::NoteId;
use crate::query::{Label, Query};
use crate::record::{Notes, Record, links_on_save};
use crate::score::Weights;
use crate::text;
use crate::time::{Day, Moment};

const TOP_LIMIT: usize = 500;
const RELATED_LIMIT: usize = 60;
const AROUND_LIMIT: usize = 200;

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
            | Action::ShowScores => false,
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

/// The note under the cursor as it is being written: what it now reads, and what it was. One
/// lasts as long as one visit of the cursor to a row, and everything it writes in that time is
/// one entry to undo.
struct Writing {
    id: String,
    text: String,
    /// What the note read when the cursor landed, or when the body last closed or the editor
    /// last came back; for a new note, what it opened with. Putting back goes back to this.
    original: String,
    /// The row was opened as a new note in this visit, so that left with nothing in it but its
    /// label it goes rather than being deleted.
    new: bool,
    /// The body is open: the field holds the whole note and not only its head.
    whole: bool,
    /// When the cursor landed, which is `created` for a new note.
    opened: Moment,
    /// What a new note's head opened with: the labels of the place it was opened in, the box
    /// first and the rest each after a space, or nothing. It is not the writer's, so a row with
    /// only this in it is empty.
    label: String,
    /// The entry on the undo stack that this visit's versions are gathered into, once one has
    /// been written.
    entry: Option<usize>,
}

impl Writing {
    /// Whether the text has changed in this visit: from what it read when the cursor landed, or
    /// was last written out on closing its body, which for a new note is what it opened with at
    /// first. A new note with nothing in it has nothing to put back.
    fn differs(&self) -> bool {
        if self.blank() {
            return false;
        }
        // A newline at the end is dropped on writing, so it is not a change.
        let ends = ['\n', '\r'];
        self.text.trim_end_matches(ends) != self.original.trim_end_matches(ends)
    }

    /// A new note with nothing in it but the label it opened with, which goes if it is left so.
    fn blank(&self) -> bool {
        self.new && self.bare(&self.text)
    }

    /// Nothing in the head but the label a new note opened with. Such a head is not written
    /// while the cursor is on it.
    fn head_empty(&self) -> bool {
        self.bare(self.text.lines().next().unwrap_or(""))
    }

    /// Whether a text holds nothing but the labels the note opened with, or the start of them,
    /// however it is spaced, as is left when they are taken out a letter at a time from the end,
    /// or a space is typed or taken out beside the box: none of it is the writer's, so that
    /// taking the labels out writes nothing.
    fn bare(&self, text: &str) -> bool {
        text::normalise_head(&self.label).starts_with(&text::normalise_head(text))
    }

    /// Where the writer's text goes in what a new note opened with: after the box and its space,
    /// before the labels at the end.
    fn start(&self) -> usize {
        match text::task(&self.label) {
            Some(_) => text::OPEN.len() + 1,
            None => 0,
        }
    }
}

/// A step of the path. The last note on it and the labels after it are the place looked around.
struct Pivot {
    place: Place,
    /// The search the place was found with, put back on the way out if the line is empty by
    /// then. A draft carried in has nothing to put back: it is still on the line.
    restore: Option<String>,
}

/// What can be gone into: a note, or a label typed on the line. A note written in a place
/// inherits it, as a link to the note or as the label at the end of its head. Labels gone into
/// one after another are one place, with the last note before them if there is one, so a note
/// written there inherits each of them.
enum Place {
    Note {
        id: String,
        head: String,
    },
    /// `written` is the label as it was typed, and as it is added to a note: `#house`, `@joe`,
    /// `2026-10-16`.
    Label {
        label: Label,
        written: String,
    },
}

impl Place {
    /// What the path calls it.
    fn name(&self) -> &str {
        match self {
            Place::Note { head, .. } => head,
            Place::Label { written, .. } => written,
        }
    }

    /// The note it is, if it is one.
    fn note(&self) -> Option<&String> {
        match self {
            Place::Note { id, .. } => Some(id),
            Place::Label { .. } => None,
        }
    }

    /// Whether it is the same place as another: the same note, or the same label in whatever
    /// case it was typed.
    fn is(&self, other: &Place) -> bool {
        match (self, other) {
            (Place::Note { id: a, .. }, Place::Note { id: b, .. }) => a == b,
            (Place::Label { label: a, .. }, Place::Label { label: b, .. }) => a == b,
            _ => false,
        }
    }
}

/// A text with a label written into its head, as it would have been typed there: the box at its
/// start, and anything else at its end.
fn with_label(text: &str, label: &Label, written: &str) -> String {
    if *label == Label::Open {
        return text::tick(text, false);
    }
    let (head, body) = text
        .split_once('\n')
        .map_or((text, None), |(h, b)| (h, Some(b)));
    let mut out = format!("{head} {written}");
    if let Some(body) = body {
        out.push('\n');
        out.push_str(body);
    }
    out
}

/// The versions one change wrote, in the order it wrote them: what each note was before, and
/// what it became. Most changes are to one note; a move is to the note and what was placed after
/// it.
type Change = Vec<(Option<Record>, Record)>;

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
    pivots: Vec<Pivot>,
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

/// Whether there is more to read than the head. A line of nothing but tags is a label, not
/// reading.
fn has_body(text: &str) -> bool {
    text.lines()
        .skip(1)
        .any(|line| !line.trim().is_empty() && !text::only_tags(line))
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
            pivots: Vec::new(),
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

    /// Whether the note under the cursor holds something not yet written, which leaving it
    /// would write: an open body typed into, a date word not yet written out, a head emptied, or a
    /// new note left empty after a version of it was written.
    pub fn unwritten(&self) -> bool {
        let Some(field) = self.field.as_ref() else {
            return false;
        };
        let held = self.notes.get(&field.id);
        if field.blank() {
            return held.is_some();
        }
        let text = self.normalise(&field.text);
        match held {
            Some(held) => held.text != text,
            None => field.new,
        }
    }

    /// Does what was asked, and returns what the face has to do about it.
    pub fn act(&mut self, action: Action) -> Vec<Effect> {
        if let Some(action) = self.on_field(action) {
            self.carry_out(action);
        }
        self.land();
        std::mem::take(&mut self.out)
    }

    /// What an action does to the note under the cursor. The few that belong to the field are
    /// done here; every other leaves it first, writing out what is left to write, and is given
    /// back to be done as it would be. One that asks for the note under the cursor is dropped
    /// when leaving took that note away, so that it does not fall on a neighbour.
    fn on_field(&mut self, action: Action) -> Option<Action> {
        let Some(field) = self.field.as_mut() else {
            return Some(action);
        };
        match action {
            // A head is kept as it is typed; an open body waits for it to close.
            Action::Edited(text) => {
                field.text = text;
                if !field.whole {
                    self.keep();
                }
            }
            Action::Undo if field.differs() => {
                self.put_back();
                self.status = "put back as it was".into();
            }
            // A new note with nothing in it has no version to take back or bring back, and
            // leaving it for one would drop it unasked.
            Action::Undo | Action::Redo if field.blank() => {}
            // Typing clears what there was to redo, so on a row typed into there is nothing, and
            // leaving the row to say so would split its visit into two entries to undo.
            Action::Redo if self.redo.is_empty() => self.status = "nothing to redo".into(),
            Action::Edit => field.whole = true,
            // The editor is handed the note as it now reads, and what it leaves is written with
            // what was typed here and what was written there.
            Action::Editor => {
                let (id, text) = (field.id.clone(), field.text.clone());
                self.out.push(Effect::Editor { id, text });
            }
            Action::Save { id, text } if id == field.id => {
                field.text = text;
                if field.blank() {
                    // An editor that leaves nothing, or only the label the row opened with,
                    // leaves no note.
                    self.leave();
                } else if !self.flush() && self.notes.get(&id).is_some() {
                    self.status = "unchanged".into();
                }
            }
            // A new note cleared with its body open is an empty row again, and what was kept of
            // it as it was typed is taken off every device, as leaving it would; the row stays,
            // with the cursor in it, to be written in or left.
            Action::Done if field.blank() => {
                field.whole = false;
                self.unwrite_new();
            }
            Action::Done => {
                self.flush();
                if let Some(field) = self.field.as_mut() {
                    field.whole = false;
                }
            }
            Action::ShowScores => self.show_scores = !self.show_scores,
            // A new note with nothing in it deleted goes as if it was never anything.
            Action::Delete if field.blank() => {
                self.abandon();
                if self.cursor.is_none() {
                    self.onto_line();
                }
            }
            // With the body open Backspace is the editor's.
            Action::Erase if field.whole => {}
            // Backspace on an empty row takes it away and steps up, as it would an empty line. A
            // new note goes; a note emptied is deleted, and undo brings it back. One with a body
            // stays, since the body is not in the head to see.
            Action::Erase => {
                // A head with text is not an empty row, whatever the face sent.
                if !field.head_empty() {
                    return None;
                }
                let (id, new) = (field.id.clone(), field.new);
                if !field.bare(&field.text) {
                    self.status = "it has a body, so it stays".into();
                    return None;
                }
                if new {
                    self.abandon();
                } else {
                    let at = self.rows.iter().position(|r| r.id == id);
                    match self.revise(&id, String::new()) {
                        Some(record) => {
                            self.gather(record);
                            self.status = "deleted · undo brings it back".into();
                        }
                        None => self.status = "that note is gone, so nothing was saved".into(),
                    }
                    self.field = None;
                    self.cursor = at.and_then(|i| {
                        self.rows.remove(i);
                        i.checked_sub(1)
                    });
                    self.stale = true;
                }
                if self.cursor.is_none() {
                    self.onto_line();
                }
            }
            // A new note with nothing in it is already an empty row to write in, and has nowhere
            // to go in to and nothing to act on; leaving it for a chord would take the row away.
            Action::Write
            | Action::GoIn
            | Action::Archive
            | Action::Link
            | Action::Shift(_)
            | Action::Copy
            | Action::OpenUrl
                if field.blank() => {}
            // The row is written out, and a new note opens under it, where the row now is. A
            // head emptied takes its note and row away, and the new note opens where it was.
            Action::Write => {
                let id = field.id.clone();
                let was = self.cursor.unwrap_or(0);
                self.flush();
                let at = match self.rows.iter().position(|r| r.id == id) {
                    Some(i) => i + 1,
                    None => was.min(self.rows.len()),
                };
                self.draft_at(at);
            }
            Action::Select(Some(row)) if self.cursor == Some(row) => {}
            action => {
                let id = field.id.clone();
                // A tap lands on the note it was aimed at, however the rows move when the field
                // is left: a new note left empty takes its row with it, and a list put in order
                // puts the rows elsewhere.
                let aimed = match &action {
                    Action::Select(Some(row)) => self.rows.get(*row).map(|r| r.id.clone()),
                    _ => None,
                };
                // An arrow off a new note left empty steps from where its row was, which is gone
                // by the time it lands: up to the row above it, down to the row that was under it.
                let past = match action {
                    Action::Move(by) if field.blank() => {
                        self.rows.iter().position(|r| r.id == id).map(|i| {
                            let n = i as i64 + by;
                            let left = self.rows.len() as i64 - 1;
                            let to = if by > 0 { n - 1 } else { n };
                            (n >= 0 && left > 0).then(|| to.min(left - 1) as usize)
                        })
                    }
                    _ => None,
                };
                if let Some(at) = past {
                    // The row goes and the list is left as it is: it is put in order only if
                    // the cursor reaches the line, which the landing decides and not the row.
                    self.abandon();
                    return Some(Action::Select(at));
                }
                self.leave();
                let on_note = matches!(
                    action,
                    Action::GoIn
                        | Action::Archive
                        | Action::Delete
                        | Action::Link
                        | Action::Shift(_)
                        | Action::Copy
                        | Action::OpenUrl
                );
                if on_note && self.selected().is_none_or(|r| r.id != id) {
                    return None;
                }
                if let Action::Select(Some(row)) = action {
                    let at = aimed
                        .and_then(|id| self.rows.iter().position(|r| r.id == id))
                        .or_else(|| (!self.rows.is_empty()).then(|| row.min(self.rows.len() - 1)));
                    return Some(Action::Select(at));
                }
                return Some(action);
            }
        }
        None
    }

    /// Writes the head under the cursor as it is typed: the text as it reads, with no date
    /// words written out, since a word on its way to being one would be made a date mid-word.
    /// An empty head is not written, so that a head cleared to be typed again does not delete
    /// its note under the hands. The row is put right where it stands and the list holds still.
    fn keep(&mut self) {
        let Some(field) = self.field.as_ref() else {
            return;
        };
        if field.whole || field.head_empty() {
            return;
        }
        let id = field.id.clone();
        let text = field.text.trim_end_matches(['\n', '\r']).to_owned();
        let record = match self.notes.get(&id) {
            Some(held) if held.text == text => return,
            Some(_) => self.revise(&id, text),
            None if field.new => Some(self.created(field, text)),
            // A note gone elsewhere is not written again; leaving it says so.
            None => None,
        };
        let Some(record) = record else {
            return;
        };
        self.gather(record);
        self.status.clear();
        self.mend_row(&id);
        self.stale = true;
    }

    /// The first version of the new note being written in the field, linked to the note being
    /// looked around and placed after the row above its own, or at the top. That is where its
    /// row was opened, since whatever moves out of a place leaves the row first, and the list
    /// holds still while the cursor is on it.
    fn created(&self, field: &Writing, text: String) -> Record {
        let around = self.around_note();
        let links = links_on_save(
            &self.engine,
            &field.id,
            "",
            &text,
            around.map_or(&[], std::slice::from_ref),
        );
        Record {
            id: field.id.clone(),
            text,
            links,
            created: Some(field.opened),
            updated: Some(self.host.now()),
            stamp: None,
            after: self
                .rows
                .iter()
                .position(|r| r.id == field.id)
                .and_then(|i| i.checked_sub(1))
                .map(|i| self.rows[i].id.clone()),
            placed: None,
            archived: false,
            deleted: false,
            device: String::new(),
        }
    }

    /// Puts the field back as it read when the cursor landed. What was written in this visit is
    /// written back to that, through the visit's own entry, which then changes nothing and goes:
    /// the visit leaves nothing to undo. A new note goes back to its row as it opened, empty or
    /// with only its label, and its version, if it had one, is deleted.
    fn put_back(&mut self) {
        let Some(field) = self.field.as_mut() else {
            return;
        };
        field.text = field.original.clone();
        let (id, original, blank) = (field.id.clone(), field.original.clone(), field.blank());
        let ours = field.entry.filter(|&i| i + 1 == self.undo.len());
        let Some(held) = self.notes.get(&id).cloned() else {
            return;
        };
        if held.text == original {
            return;
        }
        let theirs = held.device != self.device;
        let record = match ours.map(|i| self.undo[i][0].0.clone()) {
            // What the version the cursor landed on said, dates and links and all, wherever the
            // note has been moved since.
            Some(Some(before)) if before.text == original && !before.deleted => Record {
                after: held.after,
                placed: held.placed,
                ..before
            },
            _ if blank => Record {
                deleted: true,
                ..held
            },
            _ => match self.revise(&id, original) {
                Some(record) => record,
                None => return,
            },
        };
        // A version that arrived from elsewhere since the head was typed is theirs, not this
        // visit's, so putting it back is a change of its own that undo can take back.
        if theirs {
            self.commit(record);
        } else {
            self.gather(record);
        }
        if blank && let Some(row) = self.rows.iter_mut().find(|r| r.id == id) {
            row.head = String::new();
            row.has_body = false;
            row.links = Vec::new();
            row.updated = None;
        }
        self.mend_row(&id);
        self.stale = true;
    }

    /// Keeps the field with the cursor: wherever the cursor has landed, the note under it is the
    /// field. A field for the note already under the cursor stays as it is, except that while it
    /// is unchanged in this visit a version of that note arrived from elsewhere shows in it. One
    /// typed into is left as it reads, and is written past theirs at the next keystroke or when
    /// the cursor leaves.
    fn land(&mut self) {
        let Some((id, head)) = self.selected().map(|r| (r.id.clone(), r.head.clone())) else {
            self.field = None;
            return;
        };
        let held = self.notes.get(&id).map(|r| r.text.as_str());
        match self.field.as_mut() {
            Some(field) if field.id == id => {
                if let Some(held) = held
                    && !field.new
                    && field.text == field.original
                    && held != field.original
                {
                    field.text = held.to_owned();
                    field.original = held.to_owned();
                }
            }
            // A note gone elsewhere while its row is still listed is read from the row, so that
            // the head does not go blank under the cursor.
            _ => {
                let text = held.map_or(head, str::to_owned);
                self.field = Some(Writing {
                    id,
                    original: text.clone(),
                    text,
                    new: false,
                    whole: false,
                    opened: self.host.now(),
                    label: String::new(),
                    entry: None,
                });
            }
        }
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
            Action::Back => self.back_to(self.pivots.len().saturating_sub(1)),
            // On the line there is no note to open, change, write, hand to the editor or take
            // away.
            Action::Edit | Action::Edited(_) | Action::Done | Action::Editor | Action::Erase => {}
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
                    self.engine.remove(&NoteId::new(id.clone()));
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

    pub fn view(&self) -> View<'_> {
        View {
            path: self.pivots.iter().map(|p| p.place.name()).collect(),
            line: &self.line,
            dates: Query::read(&self.line, self.today()).shown(),
            rows: &self.rows,
            cursor: self.cursor,
            status: &self.status,
            notes: self.notes.len(),
            hints: self.hints(),
            archived_shown: self.show_archived,
            scores_shown: self.show_scores,
            here: self.around_note().and_then(|id| self.notes.get(id)),
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
        let zoomed = !self.pivots.is_empty();
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
                    if let Some(around) = self.around_note().filter(|&around| *around != row.id) {
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

    /// Zooms on the note under the cursor, or from the line on what the line names: the list
    /// becomes what is in it. If the note contains what was typed, or holds what the line holds
    /// and is named by its words outside phrases, in its head and in the heads of the notes it
    /// links to, or by its letters, that was a search for it, so the line clears and is kept for
    /// the way back. Otherwise it was a draft, and it comes along to be written here. A place
    /// already on the path is gone back to rather than added again, so the path never repeats.
    fn go_in(&mut self) {
        let Some(row) = self.selected() else {
            return self.go_in_from_line();
        };
        let id = row.id.clone();
        let today = self.today();
        let typed = text::expand_date_words(self.line.trim(), today);
        let searched = typed.is_empty()
            || self
                .notes
                .get(&id)
                .is_some_and(|r| r.text.to_lowercase().contains(&typed.to_lowercase()))
            || self
                .engine
                .searched(&NoteId::new(id.clone()), &Query::read(&self.line, today));
        let place = Place::Note {
            id,
            head: row.head.clone(),
        };
        if let Some(at) = self.pivots.iter().position(|p| p.place.is(&place)) {
            self.back_to(at + 1);
            return;
        }
        let restore = searched.then(|| std::mem::take(&mut self.line));
        self.pivots.push(Pivot { place, restore });
        self.refresh(true);
    }

    /// Goes into what the line names, one place after another, as if the arrow had been pressed
    /// after each: a place already on the path is gone back to, and any other is gone into. The
    /// line was a search for where it leads, so it clears, and comes back on the way out of the
    /// first place it went into; one that only went back stays as it is.
    fn go_in_from_line(&mut self) {
        let places = self.places_named();
        if places.is_empty() {
            return;
        }
        let mut went = false;
        for place in places {
            match self.pivots.iter().position(|p| p.place.is(&place)) {
                Some(at) => self.pivots.truncate(at + 1),
                None => {
                    let restore = (!went).then(|| std::mem::take(&mut self.line));
                    self.pivots.push(Pivot { place, restore });
                    went = true;
                }
            }
        }
        self.refresh(true);
    }

    /// What the line names to go into: each label of a line of labels and nothing else, in the
    /// order typed, or else the one note in the space the line is the name of and that holds what
    /// the line holds. That note is first on the list in a zoom as at the top, saying `its name`,
    /// so this is where Down and then Right would go too. One place is named unless it is where
    /// you are, so that one on the path is gone back to; of several labels, those the place has
    /// already are passed over, so that the rest are added to it.
    fn places_named(&self) -> Vec<Place> {
        let query = Query::read(&self.line, self.today());
        let mut places: Vec<Place> = Vec::new();
        if query.only_labels() {
            for typed in query.labels {
                let place = Place::Label {
                    label: typed.label,
                    written: typed.written,
                };
                if !places.iter().any(|p| p.is(&place)) {
                    places.push(place);
                }
            }
            if places.len() > 1 {
                let had: Vec<&Label> = self.labels_here().map(|(label, _)| label).collect();
                places.retain(
                    |place| !matches!(place, Place::Label { label, .. } if had.contains(&label)),
                );
                return places;
            }
        } else if let Some(note) = self.engine.the_note_named(&query, self.show_archived) {
            places.push(Place::Note {
                id: note.id.as_str().to_owned(),
                head: note.head().to_owned(),
            });
        }
        places.retain(|place| !self.pivots.last().is_some_and(|p| p.place.is(place)));
        places
    }

    /// The note being looked around: the last note on the path, if there is one. It is the
    /// place together with the labels gone into after it.
    fn around_note(&self) -> Option<&String> {
        self.pivots.iter().rev().find_map(|p| p.place.note())
    }

    /// The labels being looked around, as they were typed, outermost first: those gone into one
    /// after another since the last note on the path, or since its start. They are one place
    /// with that note. Labels before it are only how it was reached.
    fn labels_here(&self) -> impl Iterator<Item = (&Label, &str)> {
        let from = self
            .pivots
            .iter()
            .rposition(|p| p.place.note().is_some())
            .map_or(0, |at| at + 1);
        self.pivots[from..].iter().filter_map(|p| match &p.place {
            Place::Label { label, written } => Some((label, written.as_str())),
            Place::Note { .. } => None,
        })
    }

    /// Steps back out to `depth` places on the path, onto the line, so that what is asked next is
    /// asked of the line and not of a note. What is on the line stays as it is; only an empty
    /// line gets back the search that the first place left behind was found with, which is what
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
        let query = Query::read(&self.line, self.today());
        self.rows = match (self.pivots.is_empty(), query.ranks()) {
            (false, _) => self.around(&query),
            (true, true) => self.found(&query, self.show_archived, RELATED_LIMIT),
            (true, false) => self.found(&query, self.show_archived, TOP_LIMIT),
        };
        self.stale = false;
        let cursor = if reset { None } else { self.cursor };
        self.cursor = cursor
            .filter(|_| !self.rows.is_empty())
            .map(|i| i.min(self.rows.len() - 1));
    }

    /// The notes to list, in the order the space keeps them in, the archived ones too if asked.
    fn kept(&self, archived: bool) -> impl Iterator<Item = &Record> {
        self.notes
            .order()
            .into_iter()
            .filter(move |r| !r.deleted && (archived || !r.archived))
    }

    /// A note as the empty line lists it, with nothing to say why.
    fn listed(&self, r: &Record) -> Row {
        Row {
            id: r.id.clone(),
            head: r.head().to_owned(),
            archived: r.archived,
            has_body: has_body(&r.text),
            linked: false,
            links: self.link_heads(r),
            reasons: Vec::new(),
            score: None,
            updated: r.updated,
        }
    }

    /// What the line lists at the top of the space, at most `limit`. With nothing to rank by it
    /// is the notes that hold what the line holds, in the order the space keeps them in and with
    /// nothing to say why, as a place lists what is in it: on an empty line, the top of the
    /// space. Otherwise it is what the line finds among them.
    fn found(&self, query: &Query, archived: bool, limit: usize) -> Vec<Row> {
        if !query.ranks() {
            let held: Option<HashSet<&str>> = (!query.is_empty()).then(|| {
                self.engine
                    .holding(query)
                    .into_iter()
                    .map(|note| note.id.as_str())
                    .collect()
            });
            return self
                .kept(archived)
                .filter(|r| {
                    held.as_ref()
                        .is_none_or(|held| held.contains(r.id.as_str()))
                })
                .take(limit)
                .map(|r| self.listed(r))
                .collect();
        }
        let opts = SearchOptions {
            limit,
            include_archived: archived,
            today: Some(self.today()),
        };
        let mut found = self.engine.find(query, &opts);
        if query.letters {
            found = self.by_letters(found);
            found.truncate(limit);
        }
        found.iter().map(|hit| self.row(hit)).collect()
    }

    /// Notes found by their letters, best first: by what the letters scored, then the shorter
    /// head, then the order the space keeps them in.
    fn by_letters<'a>(&self, mut hits: Vec<Hit<'a>>) -> Vec<Hit<'a>> {
        let mut rank: HashMap<&str, usize> = hits
            .iter()
            .map(|hit| (hit.note.id.as_str(), usize::MAX))
            .collect();
        for (at, record) in self.notes.order().into_iter().enumerate() {
            if let Some(held) = rank.get_mut(record.id.as_str()) {
                *held = at;
            }
        }
        hits.sort_by_cached_key(|hit| {
            (
                Reverse(hit.score as i32),
                hit.note.head().chars().count(),
                rank[hit.note.id.as_str()],
            )
        });
        hits
    }

    /// Every note the line holds at the top of the space, archived ones too, in the order the
    /// line lists them and however many there are: what `fzn export` writes for a query.
    pub fn export(&self, line: &str) -> Vec<&Record> {
        let query = Query::read(line, self.today());
        self.found(&query, true, usize::MAX)
            .iter()
            .filter_map(|row| self.notes.get(&row.id))
            .collect()
    }

    /// The place being looked around. With nothing on the line to rank by it is what is in it
    /// and holds what the line holds, its facts in the order the space keeps them in and nothing
    /// the engine suggests, as the top of the space is: what is linked to or from its note and
    /// carries all of its labels. With something to rank by it is what the line finds in its
    /// neighbourhood: the note the line names, then the facts that match, then what else the
    /// engine finds there that matches, all of them holding what the line holds.
    fn around(&self, query: &Query) -> Vec<Row> {
        let note = self.around_note().map(|id| NoteId::new(id.clone()));
        let (labels, written): (Vec<_>, Vec<_>) = self
            .labels_here()
            .map(|(label, written)| (label.indexed(), written))
            .unzip();
        let written = written.join(" ");
        let place = engine::Place {
            note: note.as_ref(),
            labels: &labels,
            written: &written,
        };
        if !query.ranks() {
            let facts: HashMap<&str, Option<&str>> = self
                .engine
                .facts(&place, query)
                .into_iter()
                .map(|(note, link)| (note.id.as_str(), link))
                .collect();
            return self
                .kept(self.show_archived)
                .filter_map(|r| {
                    // How it is linked is a fact too, and the one thing said about it.
                    let link = facts.get(r.id.as_str())?;
                    Some(Row {
                        linked: true,
                        reasons: link.iter().map(|&text| text.to_owned()).collect(),
                        ..self.listed(r)
                    })
                })
                .collect();
        }
        let opts = SearchOptions {
            limit: AROUND_LIMIT,
            include_archived: self.show_archived,
            today: Some(self.today()),
        };
        let near = self.engine.around(&place, &opts);
        let ids: HashSet<&str> = near.iter().map(|hit| hit.note.id.as_str()).collect();
        let linked: HashSet<&str> = near
            .iter()
            .filter(|hit| hit.linked)
            .map(|hit| hit.note.id.as_str())
            .collect();
        // Every match, since it is cut to the neighbourhood: a limit here would let the rest of
        // the space crowd out a fact that matches.
        let opts = SearchOptions {
            limit: usize::MAX,
            ..opts
        };
        let mut found = self.engine.find(query, &opts);
        if query.letters {
            found = self.by_letters(found);
        }
        let fact = |hit: &Hit| linked.contains(hit.note.id.as_str());
        let here = self.pivots.last().and_then(|p| p.place.note());
        let mut found: Vec<&Hit> = found
            .iter()
            .filter(|hit| ids.contains(hit.note.id.as_str()) || hit.named)
            .filter(|hit| Some(hit.note.id.as_str()) != here.map(String::as_str))
            .collect();
        // The note the line names is listed first wherever it is, as at the top, so that the
        // list shows where Right at the end of the line goes: the place's note too, with labels
        // gone into after it, since Right goes back to it, but not where you are, from which
        // Right goes nowhere. Then what is in the place, and then what is only found there, each
        // in the order it was found in.
        found.sort_by_key(|&hit| (!hit.named, !fact(hit)));
        found
            .into_iter()
            .map(|hit| Row {
                linked: fact(hit),
                ..self.row(hit)
            })
            .collect()
    }

    /// The heads, as they are now, of the notes a record is linked to by id. The note being
    /// looked around is left out, since everything on the list is about it.
    fn link_heads(&self, record: &Record) -> Vec<String> {
        let around = self.around_note();
        record
            .links
            .iter()
            .filter(|&id| Some(id) != around)
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
        match next {
            Some(n) => self.cursor = Some(n as usize),
            None => self.onto_line(),
        }
    }

    /// Puts the cursor on the line, and the list in order if it has fallen behind while the
    /// cursor was on it.
    fn onto_line(&mut self) {
        self.cursor = None;
        if self.stale {
            self.refresh(true);
        }
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
        self.undo.push(vec![(before, after)]);
        self.redo.clear();
    }

    /// Writes a version from the field, gathering it into the one entry its visit has on the
    /// undo stack, so that undo takes back the visit and not a keystroke. The first write of a
    /// visit pushes the entry, keeping what the note was before; each later one changes only
    /// what it became. An entry that comes to change nothing, as when a head is typed and put
    /// back or a new note is written and taken away, is removed.
    fn gather(&mut self, record: Record) {
        let before = self.notes.version(&record.id).cloned();
        let after = self.write(record);
        self.redo.clear();
        let top = self.undo.len().checked_sub(1);
        let i = match self
            .field
            .as_ref()
            .and_then(|f| f.entry)
            .filter(|&i| Some(i) == top)
        {
            Some(i) => {
                self.undo[i][0].1 = after;
                i
            }
            None => {
                self.undo.push(vec![(before, after)]);
                self.undo.len() - 1
            }
        };
        let (before, after) = &self.undo[i][0];
        let nothing = match before {
            None => after.deleted,
            Some(b) => {
                b.text == after.text
                    && b.links == after.links
                    && b.archived == after.archived
                    && b.deleted == after.deleted
            }
        };
        let entry = if nothing {
            self.undo.pop();
            None
        } else {
            Some(i)
        };
        if let Some(field) = self.field.as_mut() {
            field.entry = entry;
        }
    }

    /// Stamps a version so that it supersedes the one held, here and on every other device,
    /// takes it in, and asks for it to be kept. Each side is stamped only where it changes what
    /// is held, so that an edit does not move the note and a move is not an edit: what the note
    /// says, unchanged, keeps the stamp and the writer it was written with. A place that is the
    /// one held keeps the later of its two clocks: a move to where the note already is stamps
    /// its own, to be put before what was placed there since.
    fn write(&mut self, mut record: Record) -> Record {
        let now = self.host.now();
        let held = self.notes.version(&record.id);
        (record.stamp, record.device) = match held {
            Some(held) if held.says_the_same(&record) => (held.stamp, held.device.clone()),
            _ => (
                Some(self.notes.next_stamp(&record.id, now)),
                self.device.clone(),
            ),
        };
        record.placed = match held {
            Some(held) if held.after == record.after => record.placed.max(held.placed),
            _ => Some(
                self.notes
                    .next_placed(&record.id, record.after.as_deref(), now),
            ),
        };
        if record.deleted {
            self.engine.remove(&NoteId::new(record.id.clone()));
        } else {
            self.engine.upsert(record.note());
        }
        self.notes.apply(record.clone());
        self.out.push(Effect::Keep(record.clone()));
        record
    }

    /// Puts back what each note was before a change, or after it, and the cursor on the first
    /// note the change was to. Only the side the change made is put back, so that what has been
    /// done to the other since, here or elsewhere, holds: taking back a move leaves what was
    /// written since, and taking back an edit leaves where the note has been moved. The notes
    /// are written in the order the change wrote them, not the reverse, since what was placed
    /// after the same note is put in order by when it was placed: a run put back after its note
    /// last to first would come out the wrong way round.
    fn restore(&mut self, change: &Change, before: bool) {
        for (was, became) in change {
            let Some(held) = self.notes.version(&became.id).cloned() else {
                continue;
            };
            let record = match was {
                None if before => Record {
                    deleted: true,
                    ..held
                },
                None => became.clone(),
                Some(was) => {
                    let to = if before { was } else { became };
                    if was.says_the_same(became) {
                        Record {
                            after: to.after.clone(),
                            placed: to.placed,
                            ..held
                        }
                    } else if (&was.after, was.placed) == (&became.after, became.placed) {
                        Record {
                            after: held.after,
                            placed: held.placed,
                            ..to.clone()
                        }
                    } else {
                        to.clone()
                    }
                }
            };
            self.write(record);
        }
        self.refresh(false);
        if let Some((_, first)) = change.first() {
            self.reselect(&first.id);
        }
    }

    fn undo(&mut self) {
        let Some(change) = self.undo.pop() else {
            self.status = "nothing to undo".into();
            return;
        };
        self.restore(&change, true);
        self.status = format!("undid: {}", text::fit(change[0].1.head(), 40));
        self.redo.push(change);
    }

    fn redo(&mut self) {
        let Some(change) = self.redo.pop() else {
            self.status = "nothing to redo".into();
            return;
        };
        self.restore(&change, false);
        self.status = format!("redid: {}", text::fit(change[0].1.head(), 40));
        self.undo.push(change);
    }

    /// Turns the line into a note, linked to the note being looked around and to whatever it
    /// names in brackets, with each label being looked around that it does not carry already
    /// written in, with its date words written out as dates, and puts the cursor on it: it is the
    /// note most likely to be gone on with, and Enter there opens a row under it. With nothing on
    /// the line, opens a new note at the top of the list to be written in place. A note's link is
    /// not in its text, and a new row has the labels in its head from the start.
    fn write_line(&mut self) {
        let today = self.today();
        let text = text::expand_date_words(self.line.trim(), today);
        if text.is_empty() {
            self.draft_at(0);
            return;
        }
        let text = self.inherit(text, today);
        let around = self.around_note().map_or(&[][..], std::slice::from_ref);
        let id = self.host.new_id();
        let links = links_on_save(&self.engine, &id, "", &text, around);
        let now = self.host.now();
        self.commit(Record {
            id: id.clone(),
            text,
            links,
            created: Some(now),
            updated: Some(now),
            stamp: None,
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: String::new(),
        });
        self.line.clear();
        self.refresh(true);
        self.reselect(&id);
        self.status = "created".into();
    }

    /// A text with each label being looked around that it does not carry already written into
    /// its head, as it would have been typed: the box at the start and the rest at the end, in the
    /// order of the path. A day is carried by a note written on it, since that is its `created`.
    fn inherit(&self, text: String, written_on: Day) -> String {
        self.labels_here().fold(text, |text, (label, written)| {
            if label.carried_by(&text, written_on) {
                text
            } else {
                with_label(&text, label, written)
            }
        })
    }

    /// Opens a new note at this row of the list, with the cursor in it. It has its id from now,
    /// and its time, and it is written at its first letter. Inside labels its head opens with
    /// each of them, but for one the note carries already, as one written on the day it is does.
    fn draft_at(&mut self, at: usize) {
        let id = self.host.new_id();
        let opened = self.host.now();
        let label = self.inherit(String::new(), opened.day());
        let at = at.min(self.rows.len());
        self.rows.insert(
            at,
            Row {
                id: id.clone(),
                head: String::new(),
                archived: false,
                has_body: false,
                // Written here, it will be linked to the note being looked around, or carry the
                // labels.
                linked: !self.pivots.is_empty(),
                links: Vec::new(),
                reasons: Vec::new(),
                score: None,
                updated: None,
            },
        );
        self.cursor = Some(at);
        self.field = Some(Writing {
            id,
            text: label.clone(),
            original: label.clone(),
            new: true,
            whole: false,
            opened,
            label,
            entry: None,
        });
    }

    /// A note's text as it is to be kept: date words written as dates, no newline at the end and
    /// no blank line at the start, since a note's head is its first line. Nothing but blank
    /// lines is nothing.
    fn normalise(&self, edited: &str) -> String {
        let mut rest = edited.trim_end_matches(['\n', '\r']);
        while let Some((first, after)) = rest.split_once('\n')
            && first.trim().is_empty()
        {
            rest = after;
        }
        if rest.trim().is_empty() {
            return String::new();
        }
        text::expand_date_words(rest, self.today())
    }

    /// Writes the note under the cursor as it is to be kept, with its date words written out, and
    /// leaves the cursor where it is. Says whether it wrote anything. What it is compared with is
    /// what the note holds, since the visit may have written it already as it was typed. A new
    /// note is written if it has text and was not written as typed, as when its body was typed
    /// into. A note emptied is deleted, which takes its row away now; a new one emptied is left
    /// for the caller to take away. Otherwise the row is put right where it stands and the list
    /// waits for the cursor to come back to the line, so that the arrow that left the head lands
    /// on the row it was aimed at.
    fn flush(&mut self) -> bool {
        let Some(field) = self.field.as_ref() else {
            return false;
        };
        if field.blank() {
            return false;
        }
        let (id, new, original) = (field.id.clone(), field.new, field.original.clone());
        let text = self.normalise(&field.text);
        let wrote = match self.notes.get(&id) {
            Some(held) if held.text == text => false,
            Some(_) => {
                let Some(record) = self.revise(&id, text.clone()) else {
                    return false;
                };
                self.gather(record);
                if text.is_empty() {
                    self.status = "deleted · undo brings it back".into();
                    self.refresh(false);
                    return true;
                }
                self.status = "saved".into();
                true
            }
            None if new => {
                let record = self.created(field, text.clone());
                self.gather(record);
                self.status = "created".into();
                true
            }
            None => {
                if text != original {
                    self.status = "that note is gone, so nothing was saved".into();
                }
                if let Some(field) = self.field.as_mut() {
                    field.text = original;
                }
                return false;
            }
        };
        if let Some(field) = self.field.as_mut() {
            field.original = text.clone();
            field.text = text;
        }
        self.mend_row(&id);
        self.stale = true;
        wrote
    }

    /// Writes out the note under the cursor, because the cursor is about to move. A new note
    /// left with nothing in it goes, and its row with it.
    fn leave(&mut self) {
        if self.field.as_ref().is_some_and(Writing::blank) {
            self.abandon();
            if self.cursor.is_none() {
                self.onto_line();
            }
        } else {
            self.flush();
            self.field = None;
        }
    }

    /// Takes away a new note with nothing in it. If a version of it was written as it was
    /// typed, it was on every device, so its deletion is written too; the visit's entry then
    /// changes nothing and goes, so there is nothing to undo.
    fn abandon(&mut self) {
        let held = self
            .field
            .as_ref()
            .and_then(|f| self.notes.get(&f.id))
            .cloned();
        if let Some(held) = held {
            self.gather(Record {
                deleted: true,
                ..held
            });
        }
        self.drop_draft();
    }

    /// Takes back what was kept of a new note that has been cleared, leaving its row where it
    /// is, as it opened, with the cursor in it: a later letter writes it again.
    fn unwrite_new(&mut self) {
        let Some(field) = self.field.as_mut() else {
            return;
        };
        field.original = field.label.clone();
        let id = field.id.clone();
        let Some(held) = self.notes.get(&id).cloned() else {
            return;
        };
        self.gather(Record {
            deleted: true,
            ..held
        });
        if let Some(row) = self.rows.iter_mut().find(|r| r.id == id) {
            row.head = String::new();
            row.has_body = false;
            row.links = Vec::new();
            row.updated = None;
        }
        self.stale = true;
    }

    /// Takes away the field of a new note, and its row, wherever the row is. The cursor goes
    /// back to the row above it, which is where the note was opened from, or off the list if it
    /// was the top row. The list is not put in order here, since the cursor may be on its way
    /// to another row: the caller that leaves it on the line does that.
    fn drop_draft(&mut self) {
        let Some(field) = self.field.take() else {
            return;
        };
        self.cursor = match self.rows.iter().position(|r| r.id == field.id) {
            Some(i) => {
                self.rows.remove(i);
                i.checked_sub(1)
            }
            None => None,
        };
    }

    /// Puts a row right where it stands, from the note as it is now held.
    fn mend_row(&mut self, id: &str) {
        let Some(record) = self.notes.get(id) else {
            return;
        };
        let (head, body, links) = (
            record.head().to_owned(),
            has_body(&record.text),
            self.link_heads(record),
        );
        let updated = record.updated;
        if let Some(row) = self.rows.iter_mut().find(|r| r.id == id) {
            row.head = head;
            row.has_body = body;
            row.links = links;
            row.updated = updated;
        }
    }

    /// Takes the text of a note as the editor left it, for a note that is not under the cursor.
    fn save(&mut self, id: &str, edited: &str) {
        let Some(record) = self.notes.get(id) else {
            self.status = "that note is gone, so nothing was saved".into();
            return;
        };
        let edited = self.normalise(edited);
        if edited == record.text {
            self.status = "unchanged".into();
        } else {
            self.put(id, edited);
            self.refresh(false);
            self.reselect(id);
        }
    }

    /// Writes a note's new text, as one change of its own.
    fn put(&mut self, id: &str, text: String) {
        let Some(record) = self.revise(id, text) else {
            self.status = "that note is gone, so nothing was saved".into();
            return;
        };
        self.status = if record.deleted {
            "deleted · undo brings it back"
        } else {
            "saved"
        }
        .into();
        self.commit(record);
    }

    /// A held note with new text, as a version to write. What it names in brackets is linked
    /// and what it no longer names is not. Emptied, it is deleted, and undo brings it back.
    fn revise(&self, id: &str, text: String) -> Option<Record> {
        let record = self.notes.get(id)?.clone();
        if text.is_empty() {
            return Some(Record {
                deleted: true,
                ..record
            });
        }
        let links = links_on_save(&self.engine, id, &record.text, &text, &record.links);
        Some(Record {
            text,
            links,
            updated: Some(self.host.now()),
            ..record
        })
    }

    /// Archives the note under the cursor, or brings it back. A task's box says the same in its
    /// text, ticked or not, and is written in the same version, so that no device sees one
    /// without the other and undo puts back both.
    fn archive(&mut self) {
        let Some(record) = self.selected_record().cloned() else {
            return;
        };
        let (id, archived) = (record.id.clone(), !record.archived);
        let task = text::task(&record.text).is_some();
        let record = match task.then(|| text::tick(&record.text, archived)) {
            Some(ticked) if ticked != record.text => self.revise(&id, ticked).unwrap_or(record),
            _ => record,
        };
        self.commit(Record { archived, ..record });
        self.status = match (task, archived) {
            (true, true) => "ticked",
            (true, false) => "unticked",
            (false, true) => "archived",
            (false, false) => "unarchived",
        }
        .into();
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

    /// How many rows at the top of the list are in the order the space keeps its notes in, and
    /// so can be moved among: all of them with nothing on the line to rank by, at the top as in
    /// a place, and none of what the line finds, which is in the order of its scores.
    fn in_order(&self) -> usize {
        if Query::read(&self.line, self.today()).ranks() {
            0
        } else {
            self.rows.len()
        }
    }

    /// Moves the note under the cursor `by` rows of the list as it is shown, as the original's
    /// move up and move down did, one note alone. Down, it is placed after the row it passes;
    /// up, after the note that row follows in the space's order, or at the top. Either way it
    /// lands beside that row, in any view, filtered or not, and goes past nothing else. What was
    /// placed after it is placed instead after the note it followed, so that a run written under
    /// it stays where it was, in its order, and no cycle is made. The whole is one change to
    /// undo, which puts the note back after the note it followed.
    fn shift(&mut self, by: i64) {
        let rows = self.in_order();
        let Some(from) = self.cursor.filter(|&i| i < rows) else {
            return;
        };
        let to = (from as i64 + by).clamp(0, rows as i64 - 1) as usize;
        if to == from {
            return;
        }
        let Some(note) = self.selected_record().cloned() else {
            return;
        };
        let id = note.id.clone();
        let order = self.notes.order();
        let at = |id: &str| order.iter().position(|r| r.id == id);
        let ahead = |at: usize| at.checked_sub(1).map(|i| order[i].id.clone());
        let (Some(at), Some(past)) = (at(&id), at(&self.rows[to].id)) else {
            return;
        };
        let after = if to < from {
            ahead(past)
        } else {
            Some(order[past].id.clone())
        };
        let followed = ahead(at);
        // Oldest placed first, so that each placed after the one before it keeps its order.
        let hanging: Vec<Record> = order
            .iter()
            .rev()
            .filter(|r| r.after.as_ref() == Some(&id))
            .map(|&r| r.clone())
            .collect();
        let was = Record {
            after: followed.clone(),
            ..note.clone()
        };
        // Its own moment, since it may be placed after the note it is after already, to go before
        // what was placed there since.
        let placed = Some(
            self.notes
                .next_placed(&id, after.as_deref(), self.host.now()),
        );
        let moved = Record {
            after,
            placed,
            ..note
        };
        let mut change = vec![(Some(was), self.write(moved))];
        for record in hanging {
            let moved = Record {
                after: followed.clone(),
                ..record.clone()
            };
            change.push((Some(record), self.write(moved)));
        }
        self.undo.push(change);
        self.redo.clear();
        self.status = "moved".into();
        self.refresh(false);
        self.reselect(&id);
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
    /// It is a correction: what is written in a zoom is linked already, and outside one, or in
    /// labels with no note before them, there is nothing to link to.
    fn link(&mut self) {
        let Some(around) = self.around_note().cloned() else {
            let written: Vec<&str> = self.labels_here().map(|(_, written)| written).collect();
            self.status = if written.is_empty() {
                "go into a note first; what you write there links to it".into()
            } else {
                let it = if written.len() == 1 { "it" } else { "them" };
                format!(
                    "nothing links to {}; what you write here carries {it}",
                    written.join(" ")
                )
            };
            return;
        };
        let Some(row) = self.selected() else {
            self.status =
                "pick a note to link or unlink; what you write here is linked already".into();
            return;
        };
        let id = row.id.clone();
        if id == around {
            self.status = "a note does not link to itself".into();
            return;
        }
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
        ms: 1_791_892_800_000,
        offset: 0,
    };

    /// A machine whose clock has stopped, and whose ids and randomness count up.
    #[derive(Default)]
    struct Stopped {
        ids: Cell<u32>,
        random: Cell<u8>,
    }

    impl Host for Stopped {
        fn now(&self) -> Moment {
            NOON
        }

        fn new_id(&self) -> String {
            self.ids.set(self.ids.get() + 1);
            format!("new-{:04}", self.ids.get())
        }

        fn random(&self, buf: &mut [u8]) {
            for byte in buf {
                self.random.set(self.random.get().wrapping_add(1));
                *byte = self.random.get();
            }
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

    /// A note with its own time, placed at the top: the list is in the order the notes were put,
    /// the last first, and their dates do not hang on the clock. The version to keep goes out
    /// with whatever is asked next.
    fn put(s: &mut Session, id: &str, text: &str, links: &[&str], updated: i64) {
        s.commit(Record {
            id: id.into(),
            text: text.into(),
            links: links.iter().map(|&l| l.to_owned()).collect(),
            created: Some(Moment::utc(updated)),
            updated: Some(Moment::utc(updated)),
            stamp: None,
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: String::new(),
        });
        s.refresh(true);
        s.land();
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
        assert_eq!(NOON.to_string(), "2026-10-13T12:00:00.000Z");
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
        put(&mut s, "c", "kettlebell halo", &[], 1_000);
        put(&mut s, "b", "kettlebell press", &[], 2_000);
        put(&mut s, "a", "kettlebell swings", &[], 3_000);
        s.act(Action::Move(1));
        assert_eq!(s.cursor, Some(0));
        s.act(Action::Move(10));
        assert_eq!(s.cursor, Some(2), "no further than the last note");
        s.act(Action::Move(-1));
        assert_eq!(s.cursor, Some(1));
        s.act(Action::Move(-10));
        assert_eq!(s.cursor, None, "up from the first note is the line");

        s.act(Action::Select(Some(2)));
        assert_eq!(s.cursor, Some(2));
        s.act(Action::Select(Some(3)));
        assert_eq!(s.cursor, Some(2), "a row that is not there is not gone to");
        s.act(Action::Type("press".into()));
        assert_eq!(s.cursor, None, "the line's action goes to the line");
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
        put(
            &mut s,
            "squats",
            "Squat log\ntrainingapp sets",
            &["hub"],
            1_000,
        );
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
            "the log links to the hub, so it is listed again"
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
            updated: Some(NOON.plus_secs(60)),
            stamp: Some(NOON),
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: "other".into(),
        };
        s.act(Action::Move(1));
        assert!(s.absorb([theirs.clone()]));
        assert_eq!(heads(&s), ["mine"], "the list holds still under the cursor");
        assert_eq!(s.status, "1 changed elsewhere");
        s.act(Action::Select(None));
        assert_eq!(heads(&s), ["written elsewhere", "mine"]);
        s.act(Action::Type("elsewhere".into()));
        assert_eq!(heads(&s), ["written elsewhere"], "and the engine knows it");

        assert!(s.absorb([Record {
            deleted: true,
            stamp: Some(NOON.plus_secs(1)),
            ..theirs
        }]));
        assert!(heads(&s).is_empty(), "from the line, at once");
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
        assert_eq!(heads(&s), ["Profile"]);

        let kept = add(&mut s, "some new note");
        assert_eq!(
            heads(&s),
            ["some new note", "Profile"],
            "what is linked, the last written on top, and nothing the engine suggests"
        );
        assert!(s.rows.iter().all(|r| r.linked));
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
        put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);
        put(
            &mut s,
            "profile",
            "Profile\ntrainingapp numbers",
            &[],
            2_000,
        );
        let hub = "trainingapp\nthe plan, see [[Profile]]";
        put(&mut s, "hub", hub, &["profile"], 3_000);
        go_into(&mut s, "trainingapp");
        assert_eq!(heads(&s), ["Profile"]);
        type_in(&mut s, "trainingapp ");
        assert_eq!(heads(&s), ["Profile", "trainingapp plan ideas"]);
        assert_eq!(
            s.rows.iter().map(|r| r.linked).collect::<Vec<_>>(),
            [true, false]
        );

        select(&mut s, "trainingapp plan ideas");
        s.act(Action::Link);
        assert_eq!(s.notes.get("near").unwrap().links, ["hub"]);
        assert_eq!(
            s.notes.get("near").unwrap().updated,
            Some(Moment::utc(1_000))
        );
        assert!(s.rows.iter().all(|r| r.linked), "now in the place");
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
        s.act(Action::Select(None));
        clear(&mut s);
        assert!(heads(&s).is_empty(), "and nothing is in the place now");
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
        put(&mut s, "near", "trainingapp plan ideas", &["hub"], 1_000);
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
        assert_eq!(heads(&s), ["Squat log", "Profile"]);
        s.act(Action::Back);

        type_in(&mut s, DRAFT);
        go_into(&mut s, "trainingapp");
        assert_eq!(s.pivots.len(), 1);
        assert_eq!(s.line, DRAFT, "the draft came along");
        assert_eq!(s.cursor, None);
        assert_eq!(
            heads(&s),
            ["Profile", "trainingapp plan ideas"],
            "the neighbourhood narrowed by the draft, what is in the place first"
        );

        s.act(Action::Write);
        let new = s.notes.records().find(|r| r.text == DRAFT).unwrap();
        assert_eq!(new.links, ["hub"]);
        assert_eq!(s.line, "");
        assert_eq!(heads(&s)[0], DRAFT, "at the top of the place");

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
            ["new note", "show archived"],
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
        put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);
        put(&mut s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
        s.say("saved");
        assert_eq!((s.view().status, s.view().notes), ("saved", 4));
        assert_eq!(
            labels(&s),
            ["new note", "pick a note", "show archived", "undo"]
        );
        assert_eq!(s.view().hints[1].action, Action::Move(1));
        type_in(&mut s, "squat");
        assert_eq!(labels(&s)[0], "write", "with something on the line");
        clear(&mut s);
        select(&mut s, "trainingapp");
        assert_eq!(
            labels(&s),
            [
                "new note",
                "body",
                "editor",
                "go in",
                "archive",
                "delete",
                "copy",
                "move down",
                "undo",
                "up"
            ]
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
                "new note here",
                "pick a note",
                "show archived",
                "undo",
                "back"
            ]
        );
        assert_eq!(s.view().hints.last().unwrap().action, Action::Back);
        type_in(&mut s, "squat");
        assert_eq!(labels(&s)[0], "write here");
        clear(&mut s);
        select(&mut s, "Profile");
        assert_eq!(
            labels(&s),
            [
                "new note here",
                "body",
                "editor",
                "go in",
                "unlink",
                "archive",
                "delete",
                "copy",
                "move up",
                "undo",
                "up"
            ]
        );
        select(&mut s, "Squat form video");
        assert_eq!(
            labels(&s),
            [
                "new note here",
                "body",
                "editor",
                "go in",
                "unlink",
                "archive",
                "delete",
                "copy",
                "open URL",
                "move down",
                "undo",
                "up"
            ]
        );
        // What is not in the place is found by typing, and can be linked there.
        s.act(Action::Select(None));
        type_in(&mut s, "plan");
        select(&mut s, "trainingapp plan ideas");
        assert_eq!(
            labels(&s)[..5],
            ["new note here", "body", "editor", "go in", "link"]
        );

        // The most there is to offer: a linked, archived note with a URL, with undo and redo to
        // hand. Under the top of the facts it could be moved up as well.
        s.act(Action::Select(None));
        clear(&mut s);
        s.act(Action::ShowArchived);
        select(&mut s, "Squat form video");
        s.act(Action::Archive);
        s.act(Action::Select(None));
        type_in(&mut s, "plan");
        select(&mut s, "trainingapp plan ideas");
        s.act(Action::Link);
        s.act(Action::Undo);
        s.act(Action::Select(None));
        clear(&mut s);
        assert_eq!(
            labels(&s),
            [
                "new note here",
                "pick a note",
                "hide archived",
                "undo",
                "redo",
                "back"
            ]
        );
        select(&mut s, "Squat form video");
        let all = [
            "new note here",
            "body",
            "editor",
            "go in",
            "unlink",
            "unarchive",
            "delete",
            "copy",
            "open URL",
            "move down",
            "undo",
            "redo",
            "up",
        ];
        assert_eq!(labels(&s), all);

        // A changed head is put back rather than undone. It is written as it is typed, which
        // leaves nothing to redo.
        s.act(Action::Edited(
            "Squat form video, slowed down\nhttps://example.com/squat for trainingapp".into(),
        ));
        assert_eq!(
            labels(&s),
            [
                "new note here",
                "body",
                "editor",
                "go in",
                "unlink",
                "unarchive",
                "delete",
                "copy",
                "open URL",
                "move down",
                "put back",
                "up"
            ]
        );
        s.act(Action::Undo);
        let without_redo: Vec<&str> = all.into_iter().filter(|&l| l != "redo").collect();
        assert_eq!(labels(&s), without_redo);

        // An open body offers what is about the note, and closing it is the way back. Putting
        // back is offered only once there is something to put back.
        s.act(Action::Edit);
        assert_eq!(labels(&s), ["delete", "done"]);
        assert_eq!(s.view().hints.last().unwrap().action, Action::Done);
        s.act(Action::Edited("Squat form video\nslowed down".into()));
        assert_eq!(labels(&s), ["put back", "delete", "done"]);
        s.act(Action::Undo);
        assert_eq!(labels(&s), ["delete", "done"]);
        s.act(Action::Done);
        s.act(Action::Select(None));
        s.act(Action::Write);
        assert_eq!(labels(&s), ["body", "editor", "up"], "a new note");
        s.act(Action::Edited("deload".into()));
        assert_eq!(
            labels(&s),
            [
                "new note here",
                "body",
                "editor",
                "go in",
                "unlink",
                "archive",
                "delete",
                "copy",
                "move down",
                "put back",
                "up"
            ],
            "a note, once it has text"
        );
    }

    #[test]
    fn a_date_word_searches_as_its_date_and_is_written_as_one() {
        let mut s = session();
        add(&mut s, "dentist 2026-10-16");
        add(&mut s, "something else entirely");

        // On the line the word stays as typed, the view says the date it stands for, and it
        // already holds the notes of that day.
        type_in(&mut s, "/fri");
        assert_eq!(s.line, "/fri");
        assert_eq!(s.view().dates, ["Fri 2026-10-16"]);
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
            s.act(Action::Editor)
                .iter()
                .all(|e| matches!(e, Effect::Keep(_))),
            "on the line there is no note to edit"
        );
        select(&mut s, "the plan");
        assert_eq!(
            s.act(Action::Editor),
            [Effect::Editor {
                id: "plan".into(),
                text: "the plan".into()
            }]
        );
        assert!(!s.view().field.unwrap().whole, "the editor is not the body");
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

        // Taking the brackets out takes the link away, and undo puts both back. Undo takes back
        // a visit, so this is a visit of its own.
        s.act(Action::Select(None));
        select(&mut s, "The plan");
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
                after: None,
                placed: None,
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
            (Action::Edit, r#""edit""#),
            (Action::Done, r#""done""#),
            (Action::Editor, r#""editor""#),
            (Action::Erase, r#""erase""#),
            (Action::Edited("a\nb".into()), r#"{"edited":"a\nb"}"#),
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
        assert_eq!(view["rows"][0]["updated"], "1970-01-01T00:50:00.000Z");
        assert_eq!(view["notes"], 1);
        assert_eq!(view["here"], serde_json::Value::Null);
        assert_eq!(
            view["field"],
            serde_json::json!({"text": "Boiler\nthe plan", "fresh": null, "whole": false})
        );
        assert!(view.get("open").is_none());
        // A hint's action is what to send back to ask for it.
        assert_eq!(view["hints"][0]["label"], "new note");
        assert_eq!(view["hints"][0]["action"], "write");
        assert_eq!(view["hints"][1]["action"], "edit");
        let last = view["hints"].as_array().unwrap().last().unwrap();
        assert_eq!(last["action"], serde_json::json!({"select": null}));

        s.act(Action::Edit);
        let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
        assert_eq!(view["field"]["whole"], true);
        s.act(Action::GoIn);
        let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
        assert_eq!(view["here"]["text"], "Boiler\nthe plan");
        assert_eq!(view["field"], serde_json::Value::Null);

        let effect = Effect::Editor {
            id: "hub".into(),
            text: "Boiler".into(),
        };
        assert_eq!(
            serde_json::to_string(&effect).unwrap(),
            r#"{"editor":{"id":"hub","text":"Boiler"}}"#
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

    #[test]
    fn only_the_actions_that_can_change_a_note_write() {
        let writes = [
            Action::Write,
            Action::Edit,
            Action::Edited("Boiler: bleed the radiators".into()),
            Action::Editor,
            Action::Done,
            Action::Save {
                id: "n1".into(),
                text: "Boiler: bleed the radiators".into(),
            },
            Action::Archive,
            Action::Delete,
            Action::Link,
            Action::Undo,
            Action::Redo,
        ];
        let reads = [
            Action::Type("boiler".into()),
            Action::Move(1),
            Action::Select(None),
            Action::GoIn,
            Action::Back,
            Action::Copy,
            Action::OpenUrl,
            Action::ShowArchived,
            Action::ShowScores,
        ];
        for action in writes {
            assert!(action.writes(), "{action:?}");
        }
        for action in reads {
            assert!(!action.writes(), "{action:?}");
        }
    }

    #[test]
    fn a_new_writer_names_the_versions_written_after_it() {
        let devices = |effects: Vec<Effect>| -> Vec<String> {
            effects
                .into_iter()
                .filter_map(|e| match e {
                    Effect::Keep(r) => Some(r.device),
                    _ => None,
                })
                .collect()
        };
        let mut s = session();
        assert_eq!(s.device(), "dev");
        assert_eq!(devices(add(&mut s, "plant the garlic")), ["dev"]);
        s.writes_as("3f9a01c2");
        assert_eq!(s.device(), "3f9a01c2");
        assert_eq!(devices(add(&mut s, "net the brassicas")), ["3f9a01c2"]);
    }

    /// The versions an action asked to be kept.
    fn kept(effects: &[Effect]) -> Vec<&Record> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::Keep(r) => Some(r),
                _ => None,
            })
            .collect()
    }

    /// The field, as the view gives it: what it reads, whether it is new, and whether its body
    /// is open.
    fn field(s: &Session) -> Option<(String, bool, bool)> {
        s.view()
            .field
            .map(|field| (field.text.to_owned(), field.fresh.is_some(), field.whole))
    }

    /// A version written on another device, a minute or two after midday.
    fn theirs(id: &str, text: &str, at: i64) -> Record {
        Record {
            id: id.into(),
            text: text.into(),
            links: Vec::new(),
            created: Some(NOON),
            updated: Some(NOON.plus_secs(at)),
            stamp: Some(NOON.plus_secs(at)),
            after: None,
            placed: Some(NOON.plus_secs(at)),
            archived: false,
            deleted: false,
            device: "3f9a01c2".into(),
        }
    }

    fn edited(s: &mut Session, text: &str) -> Vec<Effect> {
        s.act(Action::Edited(text.to_owned()))
    }

    #[test]
    fn enter_on_an_empty_line_opens_a_note_to_write_in_place_linked_to_where_you_are() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        put(&mut s, "house", "House", &["boiler"], 2_000);
        go_into(&mut s, "Boiler");
        assert_eq!(heads(&s), ["House"]);

        assert!(s.act(Action::Write).is_empty(), "nothing is written yet");
        assert_eq!(field(&s), Some((String::new(), true, false)));
        assert_eq!(heads(&s), ["", "House"], "a new row at the top");
        assert!(s.rows[0].linked, "it will be linked to where you are");
        assert_eq!(s.cursor, Some(0));
        assert_eq!(s.line, "");
        assert_eq!(s.view().here.unwrap().text, "Boiler");

        let typed = edited(&mut s, "pressure dropped to 0.8\nbleed the radiators /fri");
        let [new] = kept(&typed)[..] else {
            panic!("{typed:?}");
        };
        assert_eq!(new.id, "new-0001", "the id it was given when it opened");
        assert_eq!(
            new.text, "pressure dropped to 0.8\nbleed the radiators /fri",
            "as it was typed"
        );
        assert_eq!(new.links, ["boiler"]);
        assert_eq!((new.created, new.updated), (Some(NOON), Some(NOON)));
        let done = s.act(Action::Done);
        let [new] = kept(&done)[..] else {
            panic!("{done:?}");
        };
        assert_eq!(
            new.text, "pressure dropped to 0.8\nbleed the radiators 2026-10-16",
            "and its date word written out"
        );
        assert_eq!(
            field(&s),
            Some((new.text.clone(), false, false)),
            "the cursor stays on it, a note now"
        );
        assert_eq!(heads(&s), ["pressure dropped to 0.8", "House"]);
        assert!(s.rows[0].linked);
        assert!(s.act(Action::Move(-1)).is_empty(), "nothing more to write");

        // Outside a note there is nothing to link to.
        s.act(Action::Back);
        s.act(Action::Write);
        assert!(!s.rows[0].linked);
    }

    #[test]
    fn a_new_row_typed_into_and_cleared_goes_when_left_and_leaves_nothing_to_undo() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        s.say("");
        let undo = s.undo.len();

        s.act(Action::Write);
        let id = s.rows[2].id.clone();
        assert_eq!(kept(&edited(&mut s, "a gar")).len(), 1);
        assert!(
            edited(&mut s, " \n").is_empty(),
            "an empty head is not written"
        );
        assert_eq!(s.notes.get(&id).unwrap().text, "a gar");

        let left = s.act(Action::Move(1));
        assert!(
            matches!(&kept(&left)[..], [r] if r.id == id && r.deleted),
            "it was on every device, so it is taken off them: {left:?}"
        );
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"], "no row");
        assert_eq!(
            s.selected().unwrap().head,
            "a garden",
            "the row that was under it"
        );
        assert_eq!(s.status, "");
        assert_eq!(s.undo.len(), undo, "nothing to undo");
        assert_eq!(s.notes.len(), 3);

        // One never typed into writes nothing, and is not left behind by going anywhere else.
        s.act(Action::Select(None));
        s.act(Action::Write);
        assert!(s.act(Action::Move(1)).is_empty());
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
        assert_eq!(s.selected().unwrap().head, "Boiler");
    }

    #[test]
    fn a_chord_on_a_new_row_with_nothing_in_it_does_nothing_and_leaves_the_row() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        s.act(Action::Write);
        let id = s.rows[2].id.clone();
        for action in [
            Action::GoIn,
            Action::Archive,
            Action::Link,
            Action::Copy,
            Action::OpenUrl,
        ] {
            let name = format!("{action:?}");
            assert!(s.act(action).is_empty(), "{name} asked nothing of the face");
            assert_eq!(s.cursor, Some(2), "{name} left the cursor where it was");
            assert_eq!(s.rows[2].id, id, "{name} left the row");
            assert_eq!(s.field.as_ref().map(|f| f.id.as_str()), Some(id.as_str()));
            assert!(path(&s).is_empty(), "{name} went nowhere");
        }
        assert_eq!(s.notes.len(), 3, "nothing was written");
    }

    #[test]
    fn a_note_edited_in_place_is_written_at_each_change_and_done_writes_out_its_date_words() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler service", &[], 1_000);
        s.act(Action::Move(1));
        assert_eq!(field(&s), Some(("Boiler service".into(), false, false)));
        for text in [
            "Boiler service b",
            "Boiler service booked",
            "Boiler service booked /fri",
        ] {
            assert_eq!(kept(&edited(&mut s, text)).len(), 1, "a version a change");
        }
        assert_eq!(
            s.notes.get("boiler").unwrap().text,
            "Boiler service booked /fri",
            "as it was typed"
        );

        let done = s.act(Action::Done);
        let [held] = kept(&done)[..] else {
            panic!("{done:?}");
        };
        assert_eq!(held.text, "Boiler service booked 2026-10-16");
        assert_eq!(held.updated, Some(NOON));
        assert_eq!(s.status, "saved");
        assert_eq!(
            field(&s),
            Some(("Boiler service booked 2026-10-16".into(), false, false)),
            "written, and the cursor stays"
        );
        assert_eq!(
            s.selected().unwrap().head,
            "Boiler service booked 2026-10-16"
        );

        // Leaving writes nothing more and says nothing.
        s.say("");
        assert!(s.act(Action::Move(-1)).is_empty());
        assert_eq!(s.status, "");

        // Undo takes back the visit, all of it.
        s.act(Action::Undo);
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler service");
    }

    #[test]
    fn escape_keeps_what_was_typed_and_undo_puts_it_back() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler service", &[], 1_000);
        s.act(Action::Move(1));
        assert_eq!(kept(&edited(&mut s, "Boiler serviced")).len(), 1);
        let undone = s.act(Action::Undo);
        assert_eq!(
            kept(&undone)[0].text,
            "Boiler service",
            "putting back writes the head as it was"
        );
        assert_eq!(s.status, "put back as it was");
        assert_eq!(field(&s), Some(("Boiler service".into(), false, false)));
        assert!(s.act(Action::Select(None)).is_empty());
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler service");

        s.act(Action::Move(1));
        edited(&mut s, "Boiler serviced");
        let up = s.act(Action::Select(None));
        assert!(kept(&up).is_empty(), "kept as it was typed");
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
    }

    #[test]
    fn moving_off_a_row_keeps_what_was_typed_and_writes_nothing_more() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 2_000);
        put(&mut s, "boiler", "Boiler", &[], 3_000);
        s.act(Action::Move(1));
        assert_eq!(
            kept(&edited(&mut s, "Boiler serviced"))[0].text,
            "Boiler serviced"
        );
        assert!(kept(&s.act(Action::Move(1))).is_empty());
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
        assert_eq!(field(&s), Some(("House".into(), false, false)));
        assert_eq!(s.selected().unwrap().head, "House");

        // The line's action goes to the line.
        edited(&mut s, "House insurance");
        assert!(kept(&s.act(Action::Type("garden".into()))).is_empty());
        assert_eq!(s.notes.get("house").unwrap().text, "House insurance");
        assert_eq!((s.line.as_str(), s.cursor), ("garden", None));
        assert_eq!(field(&s), None);
    }

    #[test]
    fn a_new_note_is_written_as_it_is_typed_and_the_next_is_an_enter_away() {
        let mut s = session();
        for text in ["plant the garlic", "net the brassicas"] {
            s.act(Action::Write);
            assert_eq!(kept(&edited(&mut s, text)).len(), 1);
            assert!(kept(&s.act(Action::Move(-1))).is_empty());
            assert_eq!(
                (s.cursor, s.line.as_str()),
                (None, ""),
                "on the line, for the next"
            );
            assert!(heads(&s).contains(&text));
        }
        assert_eq!(s.notes.len(), 2);
    }

    #[test]
    fn a_burst_of_keystrokes_on_one_head_in_one_second_keeps_its_stamp_within_that_many_milliseconds_of_the_clock()
     {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 3_000);
        select(&mut s, "Boiler");
        // A key at a time with the clock stopped: each is a version, stamped a millisecond past
        // the last, where a second a key put the head as many seconds ahead.
        let head = "Boiler: service due, book it before win";
        let mut stamps = Vec::new();
        for end in "Boiler".len()..=head.len() {
            for version in kept(&edited(&mut s, &head[..end])) {
                stamps.extend(version.stamp);
            }
        }
        assert_eq!(stamps.len(), head.len() - "Boiler".len());
        assert!(stamps.windows(2).all(|two| two[0] < two[1]));
        let ahead = stamps.last().unwrap().ms - NOON.ms;
        assert!((0..=stamps.len() as i64).contains(&ahead), "{ahead} ms");
    }

    #[test]
    fn a_note_emptied_in_place_is_deleted_and_undo_brings_it_back() {
        let mut s = session();
        put(&mut s, "house", "House\nthe deeds", &[], 2_000);
        put(&mut s, "boiler", "Boiler", &[], 3_000);
        select(&mut s, "House");
        edited(&mut s, "");
        let done = s.act(Action::Done);
        assert!(matches!(&kept(&done)[..], [r] if r.deleted && r.text == "House\nthe deeds"));
        assert_eq!(s.status, "deleted · undo brings it back");
        assert_eq!(heads(&s), ["Boiler"]);

        s.act(Action::Undo);
        assert_eq!(heads(&s), ["Boiler", "House"]);

        // Deleting from the emptied row deletes it once, and nothing next to it goes with it.
        select(&mut s, "House");
        edited(&mut s, "");
        let deleted = s.act(Action::Delete);
        assert_eq!(kept(&deleted).len(), 1);
        assert_eq!(heads(&s), ["Boiler"]);
    }

    #[test]
    fn a_note_gone_elsewhere_keeps_its_head_under_the_cursor_and_leaves_without_a_word() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 1_000);
        put(&mut s, "boiler", "Boiler", &[], 2_000);
        assert_eq!(heads(&s), ["Boiler", "House"]);
        s.act(Action::Move(2));
        assert_eq!(s.selected().unwrap().head, "House");

        // Boiler is deleted elsewhere while the cursor walks the list, which holds still.
        let mut gone = s.notes.get("boiler").unwrap().clone();
        gone.deleted = true;
        gone.stamp = Some(NOON.plus_secs(120));
        gone.device = "other".into();
        assert!(s.absorb([gone]));
        assert_eq!(heads(&s), ["Boiler", "House"], "the list holds still");

        // Landing on the row that is left shows its head, not nothing; leaving it again, with
        // nothing typed, says nothing, since nothing was lost.
        s.say("");
        s.act(Action::Move(-1));
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
        assert!(s.act(Action::Move(-1)).is_empty());
        assert_eq!(s.status, "");
        assert_eq!((s.cursor, heads(&s)), (None, vec!["House"]));
    }

    #[test]
    fn undo_and_redo_on_an_empty_new_note_do_nothing_and_undo_on_one_typed_into_empties_it() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 1_000);
        s.say("");
        let undo = s.undo.len();
        s.act(Action::Write);
        assert!(s.act(Action::Undo).is_empty());
        assert!(s.act(Action::Redo).is_empty());
        assert_eq!(
            field(&s),
            Some((String::new(), true, false)),
            "still a new note"
        );
        assert_eq!(heads(&s), ["", "House"]);
        assert_eq!(s.status, "");

        // One typed into is written, and putting it back takes the version off again and
        // leaves the row empty to write in.
        edited(&mut s, "a garden");
        assert_eq!(s.notes.len(), 2);
        let undone = s.act(Action::Undo);
        assert!(matches!(&kept(&undone)[..], [r] if r.deleted));
        assert_eq!(field(&s), Some((String::new(), true, false)));
        assert_eq!(heads(&s), ["", "House"]);
        assert_eq!(s.rows[0].updated, None);
        assert_eq!(s.notes.len(), 1);
        assert_eq!(s.undo.len(), undo);
    }

    #[test]
    fn what_arrives_while_the_cursor_is_on_the_list_is_listed_when_it_comes_back_to_the_line() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        s.act(Action::Move(1));
        assert!(s.absorb([theirs("house", "House", 60)]));
        assert_eq!(s.status, "1 changed elsewhere");
        assert_eq!(heads(&s), ["Boiler"], "the list waits");
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
        assert!(s.act(Action::Select(None)).is_empty());
        assert_eq!(heads(&s), ["House", "Boiler"], "and comes with the line");

        select(&mut s, "Boiler");
        assert!(s.absorb([theirs("garden", "a garden", 120)]));
        assert_eq!(kept(&edited(&mut s, "Boiler serviced")).len(), 1);
        assert_eq!(
            heads(&s),
            ["House", "Boiler serviced"],
            "the list still waits"
        );
        s.act(Action::Select(None));
        assert_eq!(heads(&s), ["a garden", "House", "Boiler serviced"]);
    }

    #[test]
    fn the_editor_writes_a_draft() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        go_into(&mut s, "Boiler");
        s.act(Action::Write);
        let id = s.rows[0].id.clone();
        assert_eq!(
            s.act(Action::Editor),
            [Effect::Editor {
                id: id.clone(),
                text: String::new()
            }]
        );
        assert!(field(&s).is_some(), "the draft waits for the editor");
        let saved = s.act(Action::Save {
            id: id.clone(),
            text: "bleed the radiators\n".into(),
        });
        let [new] = kept(&saved)[..] else {
            panic!("{saved:?}");
        };
        assert_eq!(
            (new.id.as_str(), new.text.as_str()),
            (id.as_str(), "bleed the radiators")
        );
        assert_eq!(new.links, ["boiler"]);
        assert_eq!(s.status, "created");
        assert_eq!(
            field(&s),
            Some(("bleed the radiators".into(), false, false))
        );
        assert_eq!(s.cursor, Some(0));

        // An editor that leaves nothing leaves no note, and the cursor goes back to the row the
        // new note was opened under.
        s.act(Action::Write);
        let id = s.rows[1].id.clone();
        s.act(Action::Editor);
        assert!(
            s.act(Action::Save {
                id,
                text: "\n".into()
            })
            .is_empty()
        );
        assert_eq!(heads(&s), ["bleed the radiators"]);
        assert_eq!(s.cursor, Some(0));
        assert_eq!(
            field(&s),
            Some(("bleed the radiators".into(), false, false))
        );

        // From the line, the cursor goes back to the line.
        s.act(Action::Select(None));
        s.act(Action::Write);
        let id = s.rows[0].id.clone();
        s.act(Action::Editor);
        s.act(Action::Save {
            id,
            text: String::new(),
        });
        assert_eq!((s.cursor, field(&s)), (None, None));
    }

    #[test]
    fn an_empty_file_back_from_the_editor_deletes_the_note() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
        s.act(Action::Move(1));
        s.act(Action::Editor);
        let saved = s.act(Action::Save {
            id: "boiler".into(),
            text: " \n\n".into(),
        });
        assert!(matches!(&kept(&saved)[..], [r] if r.deleted));
        assert_eq!(s.status, "deleted · undo brings it back");
        assert!(heads(&s).is_empty());
        s.act(Action::Undo);
        assert_eq!(heads(&s), ["Boiler"]);
    }

    #[test]
    fn the_head_emptied_under_a_body_gives_the_bodys_first_line_as_the_head() {
        let mut s = session();
        put(
            &mut s,
            "boiler",
            "Boiler\nthe combi under the stairs\nserviced in spring",
            &[],
            1_000,
        );
        s.act(Action::Move(1));
        edited(
            &mut s,
            "\n  \nthe combi under the stairs\nserviced in spring\n",
        );
        let done = s.act(Action::Done);
        assert_eq!(
            kept(&done)[0].text,
            "the combi under the stairs\nserviced in spring"
        );
        assert_eq!(heads(&s), ["the combi under the stairs"]);
        assert!(s.rows[0].has_body);
    }

    /// Three notes, the last put first, with what writing them asked for already gone out.
    fn three(s: &mut Session) {
        put(s, "garden", "a garden", &[], 1_000);
        put(s, "house", "House", &[], 2_000);
        put(s, "boiler", "Boiler", &[], 3_000);
        s.act(Action::Select(None));
    }

    #[test]
    fn typing_into_a_head_writes_each_change_and_leaving_writes_nothing_more() {
        let mut s = session();
        three(&mut s);
        let undo = s.undo.len();
        assert!(s.act(Action::Move(1)).is_empty());
        assert_eq!(
            field(&s),
            Some(("Boiler".into(), false, false)),
            "nothing pressed to begin"
        );
        assert!(!s.unwritten());
        for text in ["Boiler s", "Boiler serviced"] {
            let typed = edited(&mut s, text);
            let [held] = kept(&typed)[..] else {
                panic!("{typed:?}");
            };
            assert_eq!(held.text, text);
            assert_eq!(s.notes.get("boiler").unwrap().text, text);
            assert_eq!(s.selected().unwrap().head, text, "the row is put right");
            assert!(!s.unwritten());
        }
        assert!(kept(&s.act(Action::Move(1))).is_empty());
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
        assert!(!s.unwritten());
        assert_eq!(s.undo.len(), undo + 1, "one entry for the visit");
    }

    #[test]
    fn down_through_a_list_of_unchanged_heads_writes_nothing() {
        let mut s = session();
        three(&mut s);
        let undo = s.undo.len();
        for by in [1, 1, 1, 1, -1, -1, -1, -1] {
            assert!(s.act(Action::Move(by)).is_empty());
        }
        assert_eq!(s.cursor, None);
        assert_eq!(s.undo.len(), undo);
    }

    #[test]
    fn a_head_written_mid_list_stays_where_it_is_and_down_lands_on_the_row_below_it() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        assert_eq!(kept(&edited(&mut s, "House insurance")).len(), 1);
        assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
        s.act(Action::Move(1));
        assert_eq!(s.selected().unwrap().head, "a garden");
        assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
    }

    #[test]
    fn an_old_note_edited_stays_where_it_is_kept_when_the_cursor_comes_back_to_the_line() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "a garden");
        let place = s.notes.get("garden").map(|r| (r.after.clone(), r.placed));
        let typed = edited(&mut s, "a garden, dug over");
        let [edit] = kept(&typed)[..] else {
            panic!("{typed:?}");
        };
        assert_eq!(
            Some((edit.after.clone(), edit.placed)),
            place,
            "an edit is not a move"
        );
        s.act(Action::Move(-1));
        assert_eq!(heads(&s), ["Boiler", "House", "a garden, dug over"]);
        s.act(Action::Move(-1));
        s.act(Action::Move(-1));
        assert_eq!(s.cursor, None);
        assert_eq!(heads(&s), ["Boiler", "House", "a garden, dug over"]);
    }

    #[test]
    fn undo_on_a_changed_head_puts_it_back_and_on_an_unchanged_one_takes_back_the_last_version() {
        let mut s = session();
        three(&mut s);
        s.act(Action::Move(1));
        edited(&mut s, "Boiler serviced");
        let put_back = s.act(Action::Undo);
        assert_eq!(kept(&put_back)[0].text, "Boiler");
        assert_eq!(s.status, "put back as it was");
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));

        edited(&mut s, "Boiler serviced");
        s.act(Action::Done);
        let undone = s.act(Action::Undo);
        assert_eq!(kept(&undone)[0].text, "Boiler");
        assert_eq!(s.status, "undid: Boiler serviced");
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
    }

    #[test]
    fn a_chord_on_a_head_typed_into_acts_on_the_note_as_it_reads() {
        let mut s = session();
        three(&mut s);
        let undo = s.undo.len();
        s.act(Action::Move(1));
        assert_eq!(kept(&edited(&mut s, "Boiler serviced")).len(), 1);
        let archived = s.act(Action::Archive);
        let [archive] = kept(&archived)[..] else {
            panic!("{archived:?}");
        };
        assert_eq!(
            (archive.text.as_str(), archive.archived),
            ("Boiler serviced", true)
        );
        assert_eq!(s.undo.len(), undo + 2);
        assert_eq!(heads(&s), ["House", "a garden"]);
    }

    #[test]
    fn the_body_opens_on_the_row_and_closes_onto_its_head() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
        s.act(Action::Move(1));
        assert!(s.act(Action::Edit).is_empty());
        assert_eq!(field(&s), Some(("Boiler\nthe combi".into(), false, true)));
        assert!(edited(&mut s, "Boiler\nthe combi under the stairs").is_empty());
        let done = s.act(Action::Done);
        assert_eq!(kept(&done).len(), 1);
        assert_eq!(
            field(&s),
            Some(("Boiler\nthe combi under the stairs".into(), false, false))
        );
        assert_eq!(s.cursor, Some(0), "the cursor stays on the row");
        assert!(s.act(Action::Done).is_empty(), "written once");
    }

    #[test]
    fn the_editor_is_handed_the_head_as_it_now_reads_and_what_it_leaves_is_one_version() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
        s.act(Action::Move(1));
        let undo = s.undo.len();
        edited(&mut s, "Boiler serviced\nthe combi");
        assert_eq!(
            s.act(Action::Editor),
            [Effect::Editor {
                id: "boiler".into(),
                text: "Boiler serviced\nthe combi".into()
            }]
        );
        let saved = s.act(Action::Save {
            id: "boiler".into(),
            text: "Boiler serviced\nthe combi\nin spring\n".into(),
        });
        let [held] = kept(&saved)[..] else {
            panic!("{saved:?}");
        };
        assert_eq!(held.text, "Boiler serviced\nthe combi\nin spring");
        assert_eq!(s.undo.len(), undo + 1);
        assert_eq!(s.status, "saved");
        assert!(!s.unwritten());
    }

    #[test]
    fn deleting_a_new_note_with_text_deletes_it_and_an_empty_one_goes_with_nothing_written() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 2_000);
        s.act(Action::Select(None));
        s.act(Action::Write);
        edited(&mut s, "a garden");
        let deleted = s.act(Action::Delete);
        assert!(matches!(&kept(&deleted)[..], [r] if r.deleted && r.text == "a garden"));
        assert_eq!(heads(&s), ["House"]);
        assert_eq!(s.status, "deleted · undo brings it back");
        s.act(Action::Undo);
        assert_eq!(heads(&s), ["a garden", "House"], "a note like any other");

        s.act(Action::Select(None));
        s.say("");
        let undo = s.undo.len();
        s.act(Action::Write);
        assert!(s.act(Action::Delete).is_empty());
        assert_eq!(heads(&s), ["a garden", "House"]);
        assert_eq!((s.cursor, field(&s)), (None, None));
        assert_eq!(s.notes.len(), 2);
        assert_eq!(s.undo.len(), undo);
        assert_eq!(s.status, "");
    }

    #[test]
    fn going_into_a_new_note_with_text_goes_into_the_note_it_is() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 2_000);
        s.act(Action::Select(None));
        s.act(Action::Write);
        assert_eq!(kept(&edited(&mut s, "a garden")).len(), 1);
        assert!(kept(&s.act(Action::GoIn)).is_empty(), "written already");
        assert_eq!(path(&s), ["a garden"]);

        // One with no text has nowhere to go in to, and the row stays to be written in.
        s.act(Action::Back);
        s.act(Action::Write);
        assert!(s.act(Action::GoIn).is_empty());
        assert!(path(&s).is_empty());
        assert_eq!(s.cursor, Some(0));
    }

    #[test]
    fn a_version_of_the_note_under_the_cursor_shows_while_its_head_is_unchanged_and_waits_while_it_is_not()
     {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        s.act(Action::Move(1));
        assert!(s.absorb([theirs("boiler", "Boiler\nbled the radiators", 60)]));
        assert_eq!(
            field(&s),
            Some(("Boiler\nbled the radiators".into(), false, false))
        );

        edited(&mut s, "Boiler serviced");
        assert!(s.absorb([theirs("boiler", "Boiler\nbled them twice", 120)]));
        assert_eq!(field(&s), Some(("Boiler serviced".into(), false, false)));
        let up = s.act(Action::Select(None));
        assert_eq!(kept(&up)[0].text, "Boiler serviced", "written past theirs");
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
    }

    #[test]
    fn enter_on_a_row_opens_a_new_note_under_it() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        put(&mut s, "swings", "kettlebell swings", &["boiler"], 2_000);
        put(&mut s, "garden", "a garden", &["boiler"], 3_000);
        put(&mut s, "house", "House", &["boiler"], 4_000);
        go_into(&mut s, "Boiler");
        assert_eq!(heads(&s), ["House", "a garden", "kettlebell swings"]);
        type_in(&mut s, "");
        select(&mut s, "a garden");
        s.say("");

        assert!(s.act(Action::Write).is_empty(), "nothing is written yet");
        assert_eq!(heads(&s), ["House", "a garden", "", "kettlebell swings"]);
        assert_eq!(s.cursor, Some(2));
        assert_eq!(field(&s), Some((String::new(), true, false)));
        assert!(s.rows[2].linked, "it will be linked to where you are");

        let typed = edited(&mut s, "bleed the radiators");
        let [new] = kept(&typed)[..] else {
            panic!("{typed:?}");
        };
        assert_eq!(new.text, "bleed the radiators");
        assert_eq!(new.links, ["boiler"]);
        assert_eq!(new.created, Some(NOON), "when Enter was pressed");
        assert_eq!(s.status, "");
        assert!(s.act(Action::Select(None)).is_empty());
        assert_eq!(
            (s.cursor, s.line.as_str()),
            (None, ""),
            "the line is untouched"
        );
    }

    #[test]
    fn enter_on_a_row_typed_into_writes_nothing_more_and_on_one_emptied_deletes_it() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        assert_eq!(kept(&edited(&mut s, "House insurance")).len(), 1);
        assert!(s.act(Action::Write).is_empty());
        assert_eq!(heads(&s), ["Boiler", "House insurance", "", "a garden"]);
        assert_eq!(s.cursor, Some(2));
        assert_eq!(field(&s), Some((String::new(), true, false)));

        // A head emptied deletes its note, and the new note opens where its row was.
        select(&mut s, "Boiler");
        assert!(edited(&mut s, "").is_empty());
        let entered = s.act(Action::Write);
        assert!(matches!(&kept(&entered)[..], [r] if r.deleted));
        assert_eq!(s.cursor, Some(0));
        assert_eq!(field(&s), Some((String::new(), true, false)));
    }

    #[test]
    fn enter_on_a_new_note_with_text_opens_another_under_it() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 1_000);
        s.act(Action::Write);
        assert_eq!(kept(&edited(&mut s, "plant the garlic")).len(), 1);
        assert!(s.act(Action::Write).is_empty());
        assert_eq!(heads(&s), ["plant the garlic", "", "House"]);
        assert_eq!(s.cursor, Some(1));
        assert_eq!(kept(&edited(&mut s, "net the brassicas")).len(), 1);
        assert!(s.act(Action::Select(None)).is_empty());
        assert_eq!(s.notes.len(), 3);
        assert_eq!(s.cursor, None);
    }

    #[test]
    fn enter_on_an_empty_new_note_does_nothing() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        s.act(Action::Write);
        assert!(s.act(Action::Write).is_empty());
        assert_eq!(heads(&s), ["Boiler", "House", "", "a garden"]);
        assert_eq!(s.cursor, Some(2));
        assert_eq!(field(&s), Some((String::new(), true, false)));
    }

    #[test]
    fn an_empty_new_note_left_by_an_arrow_goes_and_the_arrow_lands_past_it() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        edited(&mut s, "House insurance");
        s.act(Action::Write);
        assert!(s.act(Action::Move(-1)).is_empty());
        assert_eq!(s.selected().unwrap().head, "House insurance");
        assert_eq!(
            heads(&s),
            ["Boiler", "House insurance", "a garden"],
            "the list holds still"
        );

        s.act(Action::Write);
        assert!(s.act(Action::Move(1)).is_empty());
        assert_eq!(
            s.selected().unwrap().head,
            "a garden",
            "the row that was under it"
        );
        assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);

        // Under the last row, down has nowhere further to go than the last row.
        s.act(Action::Write);
        s.act(Action::Move(1));
        assert_eq!(s.selected().unwrap().head, "a garden");

        // At the top, up is the line, and there the list is put in order, where an edit has
        // not moved anything.
        s.act(Action::Select(None));
        s.act(Action::Write);
        assert_eq!(s.cursor, Some(0));
        s.act(Action::Move(-1));
        assert_eq!((s.cursor, field(&s)), (None, None));
        assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
    }

    #[test]
    fn down_off_an_empty_new_note_at_the_top_lands_on_the_first_row_even_when_the_list_is_stale() {
        let mut s = session();
        three(&mut s);
        s.act(Action::Write);
        assert_eq!(s.cursor, Some(0));
        // A version arriving while the cursor is on the list waits for the line.
        assert!(s.absorb([Record {
            id: "theirs".into(),
            text: "written elsewhere".into(),
            links: Vec::new(),
            created: Some(NOON),
            updated: Some(NOON),
            stamp: Some(NOON),
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: "other".into(),
        }]));
        s.act(Action::Move(1));
        assert_eq!(
            s.selected().unwrap().head,
            "Boiler",
            "the row that was under it"
        );
        assert_eq!(
            heads(&s),
            ["Boiler", "House", "a garden"],
            "and the list holds still"
        );
        s.act(Action::Select(None));
        assert!(
            heads(&s).contains(&"written elsewhere"),
            "put in order once the cursor is on the line"
        );
    }

    #[test]
    fn backspace_on_an_empty_new_row_takes_it_away_and_lands_on_the_row_above() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 1_000);
        put(&mut s, "boiler", "Boiler", &[], 2_000);
        s.act(Action::Select(Some(0)));
        let undo = s.undo.len();
        s.act(Action::Write);
        assert_eq!(s.cursor, Some(1));
        assert!(s.act(Action::Erase).is_empty(), "nothing is written");
        assert_eq!(s.cursor, Some(0));
        assert_eq!(heads(&s), ["Boiler", "House"]);
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
        assert_eq!(s.notes.len(), 2);
        assert_eq!(s.undo.len(), undo);
    }

    #[test]
    fn backspace_on_an_empty_new_row_at_the_top_lands_on_the_line() {
        let mut s = session();
        three(&mut s);
        s.act(Action::Write);
        assert_eq!(s.cursor, Some(0));
        assert!(s.act(Action::Erase).is_empty());
        assert_eq!((s.cursor, field(&s)), (None, None));
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
    }

    #[test]
    fn backspace_on_a_head_emptied_deletes_the_note_and_steps_up() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 1_000);
        put(&mut s, "boiler", "Boiler", &[], 2_000);
        select(&mut s, "House");
        edited(&mut s, "");
        let erased = s.act(Action::Erase);
        assert!(matches!(&kept(&erased)[..], [r] if r.id == "house" && r.deleted));
        assert_eq!(s.status, "deleted · undo brings it back");
        assert_eq!(s.cursor, Some(0));
        assert_eq!(heads(&s), ["Boiler"]);
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));

        s.act(Action::Undo);
        s.act(Action::Select(None));
        assert_eq!(heads(&s), ["Boiler", "House"], "undo brings it back");
    }

    #[test]
    fn backspace_on_an_empty_head_over_a_body_leaves_the_note() {
        let mut s = session();
        put(&mut s, "house", "House\nthe plan", &[], 1_000);
        select(&mut s, "House");
        edited(&mut s, "\nthe plan");
        assert!(s.act(Action::Erase).is_empty());
        assert_eq!(s.notes.get("house").unwrap().text, "House\nthe plan");
        assert_eq!(s.cursor, Some(0));
        assert_eq!(s.status, "it has a body, so it stays");
    }

    #[test]
    fn backspace_with_the_body_open_is_not_the_engines() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        s.act(Action::Edit);
        edited(&mut s, "");
        assert!(s.act(Action::Erase).is_empty());
        assert_eq!(field(&s), Some((String::new(), false, true)));
        assert_eq!(s.cursor, Some(1));
        assert!(!s.notes.get("house").unwrap().deleted);
    }

    #[test]
    fn backspace_at_the_start_of_a_head_with_text_leaves_it() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        assert!(s.act(Action::Erase).is_empty());
        assert_eq!(s.cursor, Some(1));
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
    }

    #[test]
    fn backspace_on_the_line_does_nothing() {
        let mut s = session();
        three(&mut s);
        assert!(s.act(Action::Erase).is_empty());
        assert_eq!((s.cursor, field(&s)), (None, None));
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
        assert_eq!(s.notes.len(), 3);
    }

    #[test]
    fn backspace_on_an_empty_new_row_at_the_top_puts_a_stale_list_in_order() {
        let mut s = session();
        three(&mut s);
        s.act(Action::Write);
        // A version arriving while the cursor is on the list waits for the line.
        assert!(s.absorb([theirs("theirs", "written elsewhere", 0)]));
        assert!(!heads(&s).contains(&"written elsewhere"));
        s.act(Action::Erase);
        assert_eq!(s.cursor, None);
        assert!(
            heads(&s).contains(&"written elsewhere"),
            "put in order once the cursor is on the line"
        );
    }

    #[test]
    fn discarding_a_new_note_cleared_under_a_row_lands_on_the_row_above_and_leaves_nothing_to_undo()
    {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        let undo = s.undo.len();
        s.act(Action::Write);
        edited(&mut s, "a garden shed");
        edited(&mut s, "");
        let discarded = s.act(Action::Delete);
        assert!(matches!(&kept(&discarded)[..], [r] if r.deleted));
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
        assert_eq!(s.selected().unwrap().head, "House");
        assert_eq!(field(&s), Some(("House".into(), false, false)));
        assert_eq!(s.undo.len(), undo);

        // Leaving one empty for the line goes to the line.
        s.act(Action::Write);
        s.act(Action::Select(None));
        assert_eq!(
            (s.cursor, heads(&s)),
            (None, vec!["Boiler", "House", "a garden"])
        );
    }

    #[test]
    fn a_row_offers_a_new_note_first_and_an_empty_new_note_does_not() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        assert_eq!(s.view().hints[0].action, Action::Write);
        assert_eq!(labels(&s)[0], "new note");
        s.act(Action::Write);
        assert_eq!(labels(&s), ["body", "editor", "up"]);
        edited(&mut s, "a garden shed");
        assert_eq!(labels(&s)[0], "new note");
    }

    #[test]
    fn typing_into_a_new_row_writes_the_note_at_its_first_letter_and_each_letter_after() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 1_000);
        s.act(Action::Select(None));
        s.say("");
        let undo = s.undo.len();
        s.act(Action::Write);
        let id = s.rows[0].id.clone();
        assert_eq!(s.rows[0].updated, None, "no date until it is written");

        let mut versions = Vec::new();
        for text in ["B", "Bo"] {
            versions.extend(edited(&mut s, text));
        }
        let texts: Vec<&str> = kept(&versions).iter().map(|r| r.text.as_str()).collect();
        assert_eq!(texts, ["B", "Bo"], "a version a letter");
        let held = s.notes.get(&id).unwrap();
        assert_eq!(held.text, "Bo");
        assert_eq!(held.created, Some(NOON), "when Enter was pressed");
        assert_eq!(s.rows[0].head, "Bo");
        assert_eq!(s.rows[0].updated, Some(NOON), "the row has its date");
        assert_eq!(
            s.status, "",
            "the date is the sign; the status says nothing"
        );
        assert_eq!(field(&s), Some(("Bo".into(), false, false)), "a note now");
        assert_eq!(s.undo.len(), undo + 1, "one entry for the visit");

        // Once left, one undo takes the note away entirely.
        assert!(s.act(Action::Select(None)).is_empty());
        s.act(Action::Undo);
        assert!(s.notes.get(&id).is_none());
        assert_eq!(heads(&s), ["House"]);
        assert_eq!(s.undo.len(), undo);
    }

    #[test]
    fn a_head_cleared_to_be_typed_again_is_not_deleted_while_the_cursor_is_on_it() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "Boiler");
        assert!(edited(&mut s, "").is_empty(), "nothing is written");
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler");
        assert_eq!(
            s.selected().unwrap().head,
            "Boiler",
            "the row reads as it did"
        );
        assert!(s.unwritten(), "leaving now would delete it");
        let typed = edited(&mut s, "House");
        assert!(matches!(&kept(&typed)[..], [r] if r.id == "boiler" && r.text == "House"));
        assert!(!s.unwritten());
    }

    #[test]
    fn a_date_word_is_kept_as_typed_and_written_out_when_the_cursor_leaves() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        let typed = edited(&mut s, "House viewing /tom");
        assert_eq!(
            kept(&typed)[0].text,
            "House viewing /tom",
            "not made a date mid-word"
        );
        assert!(s.unwritten());
        let left = s.act(Action::Move(1));
        assert_eq!(kept(&left)[0].text, "House viewing 2026-10-14");
        assert_eq!(
            s.notes.get("house").unwrap().text,
            "House viewing 2026-10-14"
        );

        // One undo afterwards brings back the head as it was before the visit.
        s.act(Action::Select(None));
        s.act(Action::Undo);
        assert_eq!(s.notes.get("house").unwrap().text, "House");
    }

    #[test]
    fn put_back_writes_the_head_as_it_was_when_the_cursor_landed_and_leaves_nothing_to_undo() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        put(&mut s, "house", "House", &[], 2_000);
        let undo = s.undo.len();
        select(&mut s, "Boiler");
        edited(&mut s, "Boiler s");
        edited(&mut s, "Boiler serviced");
        let put_back = s.act(Action::Undo);
        let [back] = kept(&put_back)[..] else {
            panic!("{put_back:?}");
        };
        assert_eq!(
            (back.text.as_str(), back.updated),
            ("Boiler", Some(Moment::utc(1_000))),
            "as it was, date and all"
        );
        assert_eq!(s.status, "put back as it was");
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
        assert_eq!(heads(&s), ["House", "Boiler"], "the list does not move");
        assert_eq!(s.undo.len(), undo, "the visit left nothing to undo");

        // Undo after leaving takes back what was done before the visit.
        s.act(Action::Select(None));
        s.act(Action::Undo);
        assert!(s.notes.get("house").is_none());
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler");
    }

    #[test]
    fn putting_back_over_a_version_from_elsewhere_is_a_change_that_undo_takes_back() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        select(&mut s, "Boiler");
        edited(&mut s, "Boiler s");
        s.absorb([theirs("boiler", "Boiler serviced in spring", 60)]);
        s.act(Action::Undo);
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler");

        s.act(Action::Select(None));
        s.act(Action::Undo);
        assert_eq!(
            s.notes.get("boiler").unwrap().text,
            "Boiler serviced in spring",
            "what was written elsewhere is not lost to the put back"
        );
    }

    #[test]
    fn a_new_row_cleared_with_its_body_open_is_taken_off_every_device_when_the_body_closes() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        let undo = s.undo.len();
        s.act(Action::Write);
        let id = s.rows[2].id.clone();
        assert_eq!(kept(&edited(&mut s, "a gar")).len(), 1);
        s.act(Action::Edit);
        assert!(
            edited(&mut s, "").is_empty(),
            "a body is written when it closes"
        );

        let done = s.act(Action::Done);
        assert!(
            matches!(&kept(&done)[..], [r] if r.id == id && r.deleted),
            "it was on every device, so it is taken off them: {done:?}"
        );
        assert_eq!(
            field(&s),
            Some((String::new(), true, false)),
            "an empty row again"
        );
        let row = s.selected().unwrap();
        assert_eq!((row.head.as_str(), row.updated), ("", None));
        assert_eq!(s.undo.len(), undo, "nothing to undo");

        // A letter typed there writes it again, and leaving it empty writes nothing more.
        assert_eq!(kept(&edited(&mut s, "B")).len(), 1);
        edited(&mut s, "");
        s.act(Action::Done);
        assert!(s.notes.get(&id).is_none());
        assert!(s.act(Action::Move(1)).is_empty());
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
    }

    #[test]
    fn undo_on_a_new_note_after_its_body_closes_takes_the_note_away() {
        let mut s = session();
        s.act(Action::Write);
        let id = s.rows[0].id.clone();
        edited(&mut s, "Boiler");
        s.act(Action::Edit);
        edited(&mut s, "Boiler\nthe combi under the stairs");
        s.act(Action::Done);
        assert!(
            !s.hints().iter().any(|h| h.label == "put back"),
            "nothing to put back once the body is written"
        );

        let undone = s.act(Action::Undo);
        assert!(matches!(&kept(&undone)[..], [r] if r.id == id && r.deleted));
        assert!(s.notes.get(&id).is_none());
    }

    #[test]
    fn redo_with_nothing_to_redo_leaves_a_visit_one_entry_to_undo() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        let undo = s.undo.len();
        edited(&mut s, "House plan");
        s.act(Action::Redo);
        assert_eq!(s.status, "nothing to redo");
        assert_eq!(
            field(&s),
            Some(("House plan".into(), false, false)),
            "still on the row"
        );
        edited(&mut s, "House plans");
        s.act(Action::Select(None));
        assert_eq!(s.undo.len(), undo + 1);

        s.act(Action::Undo);
        assert_eq!(s.notes.get("house").unwrap().text, "House");
    }

    #[test]
    fn the_body_is_not_written_as_it_is_typed_but_when_it_closes() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
        s.act(Action::Move(1));
        s.act(Action::Edit);
        for text in ["Boiler\nthe combi u", "Boiler\nthe combi under the stairs"] {
            assert!(edited(&mut s, text).is_empty());
        }
        assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler\nthe combi");
        assert!(s.unwritten());
        let done = s.act(Action::Done);
        assert_eq!(kept(&done)[0].text, "Boiler\nthe combi under the stairs");
        assert_eq!(s.status, "saved");
        assert!(!s.unwritten());
    }

    #[test]
    fn a_keystroke_that_leaves_the_text_as_it_is_held_writes_nothing() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        assert!(edited(&mut s, "House").is_empty());
        assert!(
            edited(&mut s, "House\n").is_empty(),
            "a newline at the end is not a change"
        );
        assert_eq!(kept(&edited(&mut s, "Houses")).len(), 1);
        assert!(edited(&mut s, "Houses").is_empty());
    }

    #[test]
    fn enter_with_text_on_the_line_takes_the_cursor_to_the_note_it_wrote() {
        let mut s = session();
        three(&mut s);
        type_in(&mut s, "a garden shed");
        s.act(Action::Write);
        let id = s
            .notes
            .records()
            .find(|r| r.text == "a garden shed")
            .unwrap()
            .id
            .clone();
        assert_eq!(s.selected().unwrap().id, id);
        assert_eq!(field(&s), Some(("a garden shed".into(), false, false)));
        assert_eq!(s.line, "");
        assert_eq!(s.status, "created");

        // Enter there opens a row under it, so a run of notes starts from the line.
        s.act(Action::Write);
        assert_eq!(s.cursor, Some(1));
        assert_eq!(field(&s), Some((String::new(), true, false)));
    }

    #[test]
    fn enter_with_text_on_the_line_takes_the_cursor_to_the_note_it_wrote_in_a_zoom_too() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        put(&mut s, "house", "House", &["boiler"], 2_000);
        go_into(&mut s, "Boiler");
        type_in(&mut s, "bleed the radiators");
        s.act(Action::Write);
        let new = s.selected().unwrap();
        assert_eq!(new.head, "bleed the radiators");
        assert!(new.linked);
        assert_eq!(s.notes.get(&new.id).unwrap().links, ["boiler"]);
        assert_eq!(s.line, "");
    }

    /// Three notes tagged `#house`, one of them by a line under its head as the importer files
    /// tags, one that says the word without the tag, and one that has nothing to do with it.
    fn house(s: &mut Session) {
        put(s, "boiler", "Boiler\n#house", &[], 1_000);
        put(s, "gutter", "clear the gutter #house", &[], 2_000);
        put(s, "roof", "fix the roof tiles #house", &[], 3_000);
        put(s, "plan", "house move plan", &[], 4_000);
        put(s, "cake", "cake recipe", &[], 5_000);
    }

    fn linked(s: &Session) -> Vec<bool> {
        s.rows.iter().map(|r| r.linked).collect()
    }

    #[test]
    fn a_tag_on_the_line_is_gone_into_and_lists_every_note_carrying_it() {
        let mut s = session();
        house(&mut s);
        let mut old = s.notes.get("roof").unwrap().clone();
        old.id = "fence".into();
        old.text = "paint the fence #house".into();
        old.archived = true;
        s.commit(old);

        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house"]);
        assert_eq!(s.line, "", "it was a search for the place");
        assert!(s.view().here.is_none(), "a label has no note to read");
        assert_eq!(
            heads(&s),
            [
                "fix the roof tiles #house",
                "clear the gutter #house",
                "Boiler"
            ],
            "every note carrying it, last written on top, and nothing else that says it"
        );
        assert_eq!(linked(&s), [true, true, true]);
        assert!(
            s.rows.iter().all(|r| r.reasons.is_empty()),
            "what is in a place needs no reason"
        );

        s.act(Action::ShowArchived);
        assert!(heads(&s).contains(&"paint the fence #house"));
        assert!(s.rows.iter().find(|r| r.id == "fence").unwrap().linked);

        s.act(Action::Back);
        assert!(path(&s).is_empty());
        assert_eq!(s.line, "#house", "and the search comes back on the way out");
    }

    #[test]
    fn typing_inside_a_tag_narrows_its_neighbourhood() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        type_in(&mut s, "roof");
        assert_eq!(heads(&s), ["fix the roof tiles #house"]);
        assert_eq!(linked(&s), [true]);
        clear(&mut s);
        type_in(&mut s, "move");
        assert_eq!(heads(&s), ["house move plan"]);
        assert_eq!(linked(&s), [false]);
        clear(&mut s);
        type_in(&mut s, "cake");
        assert!(heads(&s).is_empty(), "nothing outside the neighbourhood");
    }

    #[test]
    fn every_note_carrying_a_tag_is_listed_whatever_the_limit_and_typing_keeps_those_that_match() {
        let mut s = session();
        put(&mut s, "tyres", "pump the tyres #house", &[], 1_000);
        s.absorb((0..=AROUND_LIMIT).map(|i| theirs(&format!("leaves-{i:04}"), "rake #house", 1)));
        s.absorb((0..=TOP_LIMIT).map(|i| theirs(&format!("pump-{i:04}"), "pump it", 1)));
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        assert_eq!(linked(&s), [true; AROUND_LIMIT + 2]);

        // However many notes elsewhere say it better, a note carrying the tag that says it stays.
        type_in(&mut s, "pump");
        assert_eq!(heads(&s), ["pump the tyres #house"]);
        assert_eq!(linked(&s), [true]);
    }

    #[test]
    fn a_note_written_from_the_line_inside_a_tag_ends_with_it_and_never_twice() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);

        type_in(&mut s, "bleed the radiators");
        s.act(Action::Write);
        let new = s.selected().unwrap().clone();
        let held = s.notes.get(&new.id).unwrap();
        assert_eq!(held.text, "bleed the radiators #house");
        assert!(held.links.is_empty(), "a label is not a note to link to");
        assert!(new.linked);
        assert_eq!(
            heads(&s)[0],
            "bleed the radiators #house",
            "first, as newest"
        );

        for typed in ["#house call the plumber", "call the roofer #House"] {
            s.act(Action::Select(None));
            type_in(&mut s, typed);
            s.act(Action::Write);
            let new = s.selected().unwrap().id.clone();
            assert_eq!(
                s.notes.get(&new).unwrap().text,
                typed,
                "it carries it already"
            );
        }
    }

    #[test]
    fn a_new_row_inside_a_tag_opens_with_it_and_its_first_letter_writes_it_there() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);

        assert!(s.act(Action::Write).is_empty(), "nothing is written yet");
        assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
        assert!(!s.unwritten(), "the label alone is nothing to write");
        let id = s.field.as_ref().unwrap().id.clone();

        // The first letter goes before the label, where the caret is, and is written with it.
        let typed = edited(&mut s, "B #house");
        let [Effect::Keep(kept)] = &typed[..] else {
            panic!("one version: {typed:?}");
        };
        assert_eq!(kept.text, "B #house");
        assert_eq!(field(&s), Some(("B #house".to_owned(), false, false)));

        edited(&mut s, "Bleed the radiators /fri #house");
        let undo = s.undo.len();
        s.act(Action::Move(1));
        assert_eq!(
            s.notes.get(&id).unwrap().text,
            "Bleed the radiators 2026-10-16 #house",
            "date words are written out on leaving, and nothing is added"
        );
        assert_eq!(s.undo.len(), undo, "still one change to undo");
        s.act(Action::Select(None));
        assert_eq!(heads(&s)[0], "Bleed the radiators 2026-10-16 #house");
        assert!(s.rows[0].linked);

        // Only a note made here inherits: one edited here is left as it was written.
        type_in(&mut s, "move");
        select(&mut s, "house move plan");
        edited(&mut s, "house move plan, after the winter");
        s.act(Action::Select(None));
        assert_eq!(
            s.notes.get("plan").unwrap().text,
            "house move plan, after the winter"
        );
    }

    #[test]
    fn a_new_row_inside_a_tag_with_only_the_tag_in_it_is_empty() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        let listed = heads(&s).len();

        // Left as it opened, it goes, and nothing was ever written.
        s.act(Action::Write);
        let offered = labels(&s);
        assert!(!offered.contains(&"go in") && !offered.contains(&"new note here"));
        for action in [Action::Write, Action::GoIn, Action::Archive, Action::Copy] {
            assert!(s.act(action).is_empty());
        }
        assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
        assert!(s.act(Action::Select(None)).is_empty());
        assert_eq!(heads(&s).len(), listed, "no row remains");

        // Backspace at its start takes it away, as it would an empty row.
        s.act(Action::Move(1));
        s.act(Action::Write);
        assert_eq!(heads(&s).len(), listed + 1);
        assert!(s.act(Action::Erase).is_empty());
        assert_eq!(heads(&s).len(), listed);
        assert_eq!(s.cursor, Some(0), "up to the row it was opened under");

        // The editor leaving only the label leaves no note either.
        s.act(Action::Write);
        let id = s.field.as_ref().unwrap().id.clone();
        assert!(
            s.act(Action::Save {
                id,
                text: " #house\n".into()
            })
            .is_empty()
        );
        assert_eq!(heads(&s).len(), listed);

        // Nor does a body that pushes the label under an empty head, as Enter at its start does.
        s.act(Action::Write);
        s.act(Action::Edit);
        edited(&mut s, "\n #house");
        assert!(s.act(Action::Done).is_empty());
        assert!(s.act(Action::Select(None)).is_empty());
        assert_eq!(heads(&s).len(), listed);
    }

    #[test]
    fn a_new_row_inside_a_tag_put_back_or_cleared_is_as_it_opened() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);

        s.act(Action::Write);
        edited(&mut s, "order oil #house");
        let id = s.field.as_ref().unwrap().id.clone();
        assert!(s.notes.get(&id).is_some());
        s.act(Action::Undo);
        assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
        assert!(s.notes.get(&id).is_none(), "its version is taken back");

        // Written out, then cleared back to its label and written out, it is as it opened.
        edited(&mut s, "order oil #house");
        s.act(Action::Done);
        assert_eq!(s.notes.get(&id).unwrap().text, "order oil #house");
        edited(&mut s, " #house");
        s.act(Action::Done);
        assert!(s.notes.get(&id).is_none());
        edited(&mut s, "order logs #house");
        s.act(Action::Move(1));
        assert_eq!(s.notes.get(&id).unwrap().text, "order logs #house");

        // The label taken out is out: what is seen is what is written. Taken out a letter at a
        // time from its end, what is left of it is no more the writer's than the whole of it.
        s.act(Action::Select(None));
        s.act(Action::Write);
        for left in (0..7).rev() {
            assert!(edited(&mut s, &" #house"[..left]).is_empty(), "{left}");
        }
        edited(&mut s, "sweep the chimney");
        let id = s.field.as_ref().unwrap().id.clone();
        s.act(Action::Select(None));
        assert_eq!(s.notes.get(&id).unwrap().text, "sweep the chimney");
    }

    #[test]
    fn inside_a_tag_and_then_a_note_a_new_note_links_to_the_note_and_gets_no_tag() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        type_in(&mut s, "boil");
        assert!(labels(&s).contains(&"go in"), "the line names one note");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house", "Boiler"]);
        assert_eq!(s.line, "");
        assert_eq!(s.view().here.unwrap().id, "boiler");

        type_in(&mut s, "bleed the radiators");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        let held = s.notes.get(&new).unwrap();
        assert_eq!(held.text, "bleed the radiators");
        assert_eq!(held.links, ["boiler"]);

        s.act(Action::Write);
        edited(&mut s, "top up the pressure");
        let id = s.field.as_ref().unwrap().id.clone();
        s.act(Action::Select(None));
        assert_eq!(s.notes.get(&id).unwrap().text, "top up the pressure");
        assert_eq!(s.notes.get(&id).unwrap().links, ["boiler"]);

        s.act(Action::Back);
        assert_eq!(path(&s), ["#house"]);
        assert_eq!(s.line, "boil", "the search that found the note");
    }

    #[test]
    fn a_label_already_on_the_path_is_gone_back_to() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        type_in(&mut s, "#house");
        assert!(
            !labels(&s).contains(&"go in"),
            "the place you are in is not offered"
        );
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house"], "and is not gone into again");
        assert_eq!(s.line, "#house");

        clear(&mut s);
        type_in(&mut s, "boiler");
        s.act(Action::GoIn);
        type_in(&mut s, "#HOUSE");
        s.act(Action::GoIn);
        assert_eq!(
            path(&s),
            ["#house"],
            "back to it, in whatever case it is typed"
        );
        assert_eq!(
            s.line, "#HOUSE",
            "with something on the line, as for a note"
        );

        // Alone on the line it is gone back to though the place has it, being one with the
        // labels gone into after it.
        go_to(&mut s, "/today");
        assert_eq!(path(&s), ["#house", "2026-10-13"]);
        type_in(&mut s, "#house");
        assert!(labels(&s).contains(&"go in"));
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house"], "back to it");
    }

    #[test]
    fn going_in_is_offered_on_the_line_only_when_it_names_a_place_and_clear_comes_before_back() {
        let mut s = session();
        house(&mut s);
        let last = |s: &Session| s.view().hints.last().map(|h| (h.label, h.action.clone()));
        assert!(!labels(&s).contains(&"go in"));
        assert_ne!(labels(&s).last(), Some(&"clear"), "nothing to clear");

        for (typed, names) in [
            ("cake rec", true),
            ("cake recipe and", false),
            ("cake", false),
            ("#house", true),
            ("#house roof", false),
            ("@joe", true),
            ("/fri", true),
            ("/fri dentist", false),
        ] {
            clear(&mut s);
            type_in(&mut s, typed);
            assert_eq!(labels(&s).contains(&"go in"), names, "{typed}");
            assert_eq!(
                last(&s),
                Some(("clear", Action::Type(String::new()))),
                "{typed}"
            );
        }

        // What does not name one place is not gone into.
        clear(&mut s);
        type_in(&mut s, "cake recipe and");
        s.act(Action::GoIn);
        assert!(path(&s).is_empty());
        assert_eq!(s.line, "cake recipe and");
        put(&mut s, "cake2", "Cake Recipe", &[], 6_000);
        clear(&mut s);
        type_in(&mut s, "cake rec");
        assert!(!labels(&s).contains(&"go in"), "two notes share the name");
        s.act(Action::GoIn);
        assert!(path(&s).is_empty());

        clear(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        assert_eq!(last(&s), Some(("back", Action::Back)));
        type_in(&mut s, "roof");
        assert_eq!(last(&s), Some(("clear", Action::Type(String::new()))));
        assert!(!labels(&s).contains(&"back"));
        s.act(Action::Type(String::new()));
        assert_eq!(path(&s), ["#house"], "clearing stays where it is");
        assert_eq!(last(&s), Some(("back", Action::Back)));
    }

    #[test]
    fn a_person_and_a_day_are_places_too() {
        let mut s = session();
        let friday = NOON.plus_secs(3 * 86_400).secs();
        put(&mut s, "ladder", "@Joe has the ladder", &[], 1_000);
        put(&mut s, "call", "call @joe about the boiler", &[], 2_000);
        put(&mut s, "dentist", "dentist 2026-10-16", &[], 3_000);
        put(&mut s, "keys", "pick up the keys", &[], friday);
        put(&mut s, "cake", "cake recipe", &[], 4_000);

        type_in(&mut s, "@joe");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["@joe"]);
        assert_eq!(
            heads(&s),
            ["call @joe about the boiler", "@Joe has the ladder"]
        );
        type_in(&mut s, "fix the gate");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(s.notes.get(&new).unwrap().text, "fix the gate @joe");
        s.act(Action::Back);
        assert_eq!(s.line, "@joe");

        clear(&mut s);
        type_in(&mut s, "/fri");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["2026-10-16"], "a day as it is written");
        assert_eq!(
            heads(&s),
            ["pick up the keys", "dentist 2026-10-16"],
            "written on the day, or mentioning it"
        );
        type_in(&mut s, "post the parcel");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(
            s.notes.get(&new).unwrap().text,
            "post the parcel 2026-10-16"
        );
        type_in(&mut s, "dentist again /fri");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(s.notes.get(&new).unwrap().text, "dentist again 2026-10-16");
        s.act(Action::Select(None));
        s.act(Action::Back);
        assert_eq!(s.line, "/fri", "the line as it was typed");
    }

    #[test]
    fn inside_a_place_the_note_the_line_names_is_listed_first_and_gone_into() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);

        type_in(&mut s, "cake rec");
        assert_eq!(heads(&s), ["cake recipe"], "outside the tag, but named");
        assert_eq!(s.rows[0].reasons[0], "its name");
        assert!(!s.rows[0].linked, "named, not a fact of the place");
        assert!(labels(&s).contains(&"go in"));
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house", "cake recipe"], "where the list said");

        s.act(Action::Back);
        for (typed, listed) in [
            ("roof", &["fix the roof tiles #house"][..]),
            ("cake", &[][..]),
        ] {
            clear(&mut s);
            type_in(&mut s, typed);
            assert_eq!(heads(&s), listed, "{typed} names nothing");
            assert!(!labels(&s).contains(&"go in"), "{typed}");
        }
    }

    #[test]
    fn a_note_written_on_the_day_carries_it_and_one_in_another_day_has_it_written_in() {
        let mut s = session();
        let friday = NOON.plus_secs(3 * 86_400).secs();
        put(&mut s, "keys", "pick up the keys", &[], friday);

        type_in(&mut s, "/today");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["2026-10-13"]);
        type_in(&mut s, "water the plants");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(s.notes.get(&new).unwrap().text, "water the plants");
        s.act(Action::Select(None));
        s.act(Action::Write);
        assert_eq!(
            field(&s),
            Some((String::new(), true, false)),
            "nothing to add"
        );
        edited(&mut s, "feed the cat");
        let id = s.field.as_ref().unwrap().id.clone();
        s.act(Action::Select(None));
        assert_eq!(s.notes.get(&id).unwrap().text, "feed the cat");
        assert_eq!(
            heads(&s)[..2],
            ["feed the cat", "water the plants"],
            "both written on it, so both carry it"
        );

        s.act(Action::Back);
        clear(&mut s);
        type_in(&mut s, "/fri");
        s.act(Action::GoIn);
        let keys = s.rows.iter().find(|r| r.id == "keys").unwrap();
        assert!(
            keys.reasons.iter().all(|r| !r.contains("apart")),
            "a note of the day is near it, not near today: {:?}",
            keys.reasons
        );
        type_in(&mut s, "post the parcel");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(
            s.notes.get(&new).unwrap().text,
            "post the parcel 2026-10-16"
        );
        s.act(Action::Write);
        assert_eq!(field(&s), Some((" 2026-10-16".to_owned(), true, false)));
        edited(&mut s, "order oil 2026-10-16");
        let id = s.field.as_ref().unwrap().id.clone();
        s.act(Action::Select(None));
        assert_eq!(s.notes.get(&id).unwrap().text, "order oil 2026-10-16");
    }

    #[test]
    fn today_gone_into_from_a_tag_is_todays_notes_in_it_and_what_is_written_there_carries_both() {
        let mut s = session();
        house(&mut s);
        put(&mut s, "boots", "dry the boots #house", &[], NOON.secs());
        put(&mut s, "plants", "water the plants", &[], NOON.secs());

        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        type_in(&mut s, "/today");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house", "2026-10-13"]);
        assert_eq!(
            heads(&s),
            ["dry the boots #house"],
            "not the tag's notes of other days, nor today's without the tag"
        );
        for (typed, listed, fact) in [
            ("boots", "dry the boots #house", true),
            ("plants", "water the plants", false),
            ("gutter", "clear the gutter #house", false),
        ] {
            type_in(&mut s, typed);
            assert_eq!(heads(&s), [listed], "the line narrows the two as one");
            assert_eq!(linked(&s), [fact], "{typed}");
            clear(&mut s);
        }

        type_in(&mut s, "Order oil");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(
            s.notes.get(&new).unwrap().text,
            "Order oil #house",
            "the tag written in, and the day carried by being written on it"
        );
        assert!(s.rows[0].linked, "and it is a fact of the place");

        s.act(Action::Select(None));
        s.act(Action::Write);
        assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
        edited(&mut s, "Bleed radiators #house");
        let id = s.field.as_ref().unwrap().id.clone();
        s.act(Action::Select(None));
        assert_eq!(s.notes.get(&id).unwrap().text, "Bleed radiators #house");

        s.act(Action::Back);
        assert_eq!(path(&s), ["#house"], "back one rung at a time");
        assert_eq!(s.line, "/today");
    }

    #[test]
    fn labels_gone_into_one_after_another_open_a_new_row_with_each_in_the_order_of_the_path() {
        let mut s = session();
        house(&mut s);
        put(&mut s, "ladder", "@joe has the ladder #house", &[], 1_500);
        put(&mut s, "gate", "@joe fixed the gate", &[], 1_600);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        type_in(&mut s, "@joe");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house", "@joe"]);
        assert_eq!(heads(&s), ["@joe has the ladder #house"]);

        s.act(Action::Write);
        assert_eq!(field(&s), Some((" #house @joe".to_owned(), true, false)));
        for left in (0..12).rev() {
            assert!(
                edited(&mut s, &" #house @joe"[..left]).is_empty(),
                "what is left of the labels is not the writer's: {left}"
            );
        }
        s.act(Action::Select(None));
        for (typed, written) in [
            ("Order oil", "Order oil #house @joe"),
            ("ask @Joe about the oil", "ask @Joe about the oil #house"),
        ] {
            s.act(Action::Select(None));
            type_in(&mut s, typed);
            s.act(Action::Write);
            let new = s.selected().unwrap().id.clone();
            assert_eq!(s.notes.get(&new).unwrap().text, written);
        }

        select(&mut s, "@joe has the ladder #house");
        s.act(Action::Link);
        assert_eq!(
            s.status,
            "nothing links to #house @joe; what you write here carries them"
        );

        s.act(Action::Select(None));
        s.act(Action::Back);
        clear(&mut s);
        type_in(&mut s, "/fri");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house", "2026-10-16"]);
        s.act(Action::Write);
        assert_eq!(
            field(&s),
            Some((" #house 2026-10-16".to_owned(), true, false))
        );
    }

    /// A boiler tagged `#house`, with a note written in it yesterday and one today, and today
    /// two notes that are not about it, one of them in `#house` too.
    fn boiler(s: &mut Session) {
        let today = NOON.secs() - 3_600;
        put(s, "boiler", "Boiler\n#house", &[], NOON.secs() - 9 * 86_400);
        put(
            s,
            "service",
            "Service booked",
            &["boiler"],
            NOON.secs() - 86_400,
        );
        put(s, "pressure", "Pressure dropped #house", &["boiler"], today);
        put(s, "oil", "Order oil #house", &[], today);
        put(s, "plants", "Water the plants", &[], today);
    }

    /// Goes into what the line names, typed from an empty line.
    fn go_to(s: &mut Session, typed: &str) {
        s.act(Action::Select(None));
        clear(s);
        type_in(s, typed);
        s.act(Action::GoIn);
    }

    #[test]
    fn with_nothing_on_the_line_a_place_lists_only_its_facts_in_the_order_kept_and_no_scores() {
        let mut s = session();
        boiler(&mut s);
        put(&mut s, "gauge", "Boiler pressure gauge", &[], NOON.secs());
        go_to(&mut s, "Boiler");
        assert_eq!(path(&s), ["Boiler"]);
        assert_eq!(
            heads(&s),
            ["Pressure dropped #house", "Service booked"],
            "what is linked, and not the notes that share its words or its tag"
        );
        for row in &s.rows {
            assert!(row.linked && row.score.is_none());
            assert_eq!(
                row.reasons,
                ["links here"],
                "how it is linked, and nothing scored"
            );
        }
        s.act(Action::Select(Some(0)));
        s.act(Action::Shift(1));
        s.act(Action::Select(None));
        assert_eq!(heads(&s), ["Service booked", "Pressure dropped #house"]);

        go_to(&mut s, "#house");
        assert_eq!(path(&s), ["Boiler", "#house"]);
        go_to(&mut s, "Pressure dropped #house");
        assert_eq!(heads(&s), ["Boiler"]);
        assert_eq!(s.rows[0].reasons, ["linked from here"]);
    }

    #[test]
    fn typing_in_a_place_lists_the_facts_that_match_before_what_it_finds() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler\nthe old one", &[], 1_000);
        put(
            &mut s,
            "visit",
            "Engineer visit\npressure low",
            &["boiler"],
            2_000,
        );
        put(&mut s, "gauge", "Boiler pressure gauge", &[], 3_000);
        put(&mut s, "cake", "cake recipe", &[], 4_000);
        go_to(&mut s, "Boiler");
        assert_eq!(heads(&s), ["Engineer visit"]);

        type_in(&mut s, "pressure");
        assert_eq!(
            heads(&s),
            ["Engineer visit", "Boiler pressure gauge"],
            "the fact first, though the find has the word in its head"
        );
        assert_eq!(linked(&s), [true, false]);
        assert!(
            s.rows
                .iter()
                .all(|r| r.score.is_some() && !r.reasons.is_empty())
        );
    }

    #[test]
    fn a_note_and_the_labels_gone_into_after_it_are_one_place() {
        let mut s = session();
        boiler(&mut s);
        go_to(&mut s, "Boiler");
        go_to(&mut s, "/today");
        assert_eq!(path(&s), ["Boiler", "2026-10-13"]);
        assert_eq!(
            heads(&s),
            ["Pressure dropped #house"],
            "Boiler's notes from today"
        );
        assert_eq!(
            s.view().here.unwrap().id,
            "boiler",
            "its note is read there"
        );

        // What is written there links to the note and is of the day by being written on it.
        s.act(Action::Write);
        assert_eq!(field(&s), Some((String::new(), true, false)));
        let id = s.field.as_ref().unwrap().id.clone();
        edited(&mut s, "Bleed the radiators");
        s.act(Action::Select(None));
        assert_eq!(s.notes.get(&id).unwrap().links, ["boiler"]);
        assert_eq!(
            heads(&s),
            ["Bleed the radiators", "Pressure dropped #house"]
        );

        // Ctrl-L there is the note's, as in its own zoom.
        select(&mut s, "Bleed the radiators");
        assert!(labels(&s).contains(&"unlink"));
        s.act(Action::Link);
        assert!(s.notes.get(&id).unwrap().links.is_empty());
        assert_eq!(heads(&s), ["Pressure dropped #house"], "out of the place");
        s.act(Action::Select(None));
        type_in(&mut s, "oil");
        select(&mut s, "Order oil #house");
        assert!(labels(&s).contains(&"link"));

        // A note linked to it on another day is found there, and is not of the place.
        go_to(&mut s, "Boiler");
        go_to(&mut s, "/today");
        type_in(&mut s, "booked");
        assert_eq!(heads(&s), ["Service booked"]);
        assert_eq!(linked(&s), [false]);
        select(&mut s, "Service booked");
        assert!(labels(&s).contains(&"unlink"));

        // With a tag after it, what is in it carries the tag, and a new note carries it too.
        go_to(&mut s, "Boiler");
        go_to(&mut s, "#house");
        assert_eq!(path(&s), ["Boiler", "#house"]);
        assert_eq!(heads(&s), ["Pressure dropped #house"]);
        s.act(Action::Write);
        assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
        s.act(Action::Select(None));
        type_in(&mut s, "Check the valve");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        let held = s.notes.get(&new).unwrap();
        assert_eq!(held.text, "Check the valve #house");
        assert_eq!(held.links, ["boiler"]);

        // A note gone into from there starts a new place.
        go_to(&mut s, "Pressure dropped #house");
        assert_eq!(path(&s), ["Boiler", "#house", "Pressure dropped #house"]);
        s.act(Action::Write);
        assert_eq!(field(&s), Some((String::new(), true, false)));
        assert_eq!(heads(&s)[1..], ["Boiler"]);
    }

    #[test]
    fn the_note_of_a_place_with_labels_after_it_is_listed_when_named_and_gone_back_to() {
        let mut s = session();
        boiler(&mut s);
        go_to(&mut s, "Boiler");
        type_in(&mut s, "Boiler");
        assert!(
            !labels(&s).contains(&"go in") && !heads(&s).contains(&"Boiler"),
            "in its own zoom it names where you are"
        );

        go_to(&mut s, "/today");
        type_in(&mut s, "Boiler");
        assert_eq!(heads(&s)[0], "Boiler", "where Right goes");
        assert_eq!(s.rows[0].reasons[0], "its name");
        assert!(!s.rows[0].linked, "named, not a fact of the place");
        assert!(labels(&s).contains(&"go in"));
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["Boiler"], "back to it");

        // Its row has nothing to link to.
        go_to(&mut s, "/today");
        type_in(&mut s, "Boiler");
        select(&mut s, "Boiler");
        assert!(!labels(&s).contains(&"link") && !labels(&s).contains(&"unlink"));
        let undo = s.undo.len();
        s.act(Action::Link);
        assert_eq!(s.undo.len(), undo, "nothing was written");
        assert!(s.notes.get("boiler").unwrap().links.is_empty());
    }

    #[test]
    fn the_labels_before_a_note_are_only_how_it_was_reached() {
        let mut s = session();
        boiler(&mut s);
        go_to(&mut s, "#house");
        go_to(&mut s, "Boiler");
        assert_eq!(path(&s), ["#house", "Boiler"]);
        assert_eq!(
            heads(&s),
            ["Pressure dropped #house", "Service booked"],
            "Boiler's notes, with the tag or without"
        );
        s.act(Action::Write);
        assert_eq!(field(&s), Some((String::new(), true, false)), "no tag");
        s.act(Action::Select(None));
        type_in(&mut s, "Call the engineer");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        let held = s.notes.get(&new).unwrap();
        assert_eq!(held.text, "Call the engineer");
        assert_eq!(held.links, ["boiler"]);

        // Today after the note is the place Boiler then today is.
        go_to(&mut s, "/today");
        assert_eq!(path(&s), ["#house", "Boiler", "2026-10-13"]);
        let here = heads(&s).join("|");
        s.act(Action::Back);
        s.act(Action::Back);
        s.act(Action::Back);
        go_to(&mut s, "Boiler");
        go_to(&mut s, "/today");
        assert_eq!(heads(&s).join("|"), here);
        assert_eq!(here, "Call the engineer|Pressure dropped #house");
    }

    #[test]
    fn what_carries_a_label_is_listed_whatever_the_weights_give_it() {
        let nothing = Weights {
            lexical: 0.0,
            explicit: 0.0,
            temporal: 0.0,
            ..Weights::default()
        };
        let mut s = Session::new(Box::new(Stopped::default()), "dev", nothing, Notes::new());
        put(&mut s, "dentist", "dentist 2026-10-16", &[], 3_000);
        put(&mut s, "gutter", "clear the gutter #house", &[], 2_000);
        type_in(&mut s, "/fri");
        s.act(Action::GoIn);
        assert_eq!(heads(&s), ["dentist 2026-10-16"]);
        s.act(Action::Back);
        clear(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        assert_eq!(heads(&s), ["clear the gutter #house"]);
        assert_eq!(linked(&s), [true]);
    }

    #[test]
    fn linking_inside_a_label_does_nothing_but_say_so() {
        let mut s = session();
        house(&mut s);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        for (typed, head) in [("", "Boiler"), ("move", "house move plan")] {
            s.act(Action::Select(None));
            clear(&mut s);
            type_in(&mut s, typed);
            select(&mut s, head);
            assert!(!labels(&s).contains(&"link") && !labels(&s).contains(&"unlink"));
            let undo = s.undo.len();
            s.act(Action::Link);
            assert_eq!(s.undo.len(), undo, "nothing was written");
            assert!(
                s.status.starts_with("nothing links to #house"),
                "{}",
                s.status
            );
        }
    }

    /// Writes a note in a new row under the one the cursor is on, or at the top from the line,
    /// and comes back to the line. Gives back what was asked to be kept.
    fn under(s: &mut Session, text: &str) -> Vec<Effect> {
        let mut log = s.act(Action::Write);
        log.extend(edited(s, text));
        log.extend(s.act(Action::Select(None)));
        log
    }

    /// Another device's session, given the versions this one asked to be kept.
    fn elsewhere<'a>(effects: impl IntoIterator<Item = &'a Effect>) -> Session {
        Session::new(
            Box::new(Stopped::default()),
            "3f9a01c2",
            Weights::default(),
            reopened(effects),
        )
    }

    #[test]
    fn a_note_written_under_a_row_stays_under_it_here_and_on_another_device() {
        let mut s = session();
        let mut log = Vec::new();
        for text in ["Boiler", "House", "a garden"] {
            log.extend(add(&mut s, text));
            log.extend(s.act(Action::Select(None)));
        }
        assert_eq!(heads(&s), ["a garden", "House", "Boiler"]);
        log.extend(s.act(Action::Move(2)));
        log.extend(under(&mut s, "Order oil"));
        let placed = ["a garden", "House", "Order oil", "Boiler"];
        assert_eq!(heads(&s), placed, "back on the line, it has not moved");
        let oil = s.rows[2].id.clone();
        assert_eq!(s.notes.get(&oil).unwrap().after, Some(s.rows[1].id.clone()));
        assert_eq!(heads(&elsewhere(&log)), placed);
    }

    #[test]
    fn a_note_opened_or_written_on_the_line_goes_to_the_top() {
        let mut s = session();
        three(&mut s);
        let opened = under(&mut s, "Order oil");
        assert_eq!(heads(&s), ["Order oil", "Boiler", "House", "a garden"]);
        let [first, ..] = kept(&opened)[..] else {
            panic!("{opened:?}");
        };
        assert!(
            first.after.is_none() && first.placed.is_some(),
            "placed from its first version, so placed at once everywhere"
        );
        add(&mut s, "Bleed the radiators");
        s.act(Action::Select(None));
        assert_eq!(heads(&s)[..2], ["Bleed the radiators", "Order oil"]);
    }

    #[test]
    fn a_note_moves_a_row_down_or_up_with_the_cursor_on_it() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "Boiler");
        assert!(!labels(&s).contains(&"move up"), "nowhere up to go");
        s.act(Action::Shift(1));
        assert_eq!(heads(&s), ["House", "Boiler", "a garden"]);
        assert_eq!(s.selected().unwrap().head, "Boiler");
        assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
        s.act(Action::Shift(-1));
        assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);

        select(&mut s, "a garden");
        assert!(!labels(&s).contains(&"move down"), "nowhere down to go");
        s.act(Action::Shift(-1));
        assert_eq!(heads(&s), ["Boiler", "a garden", "House"]);
        s.act(Action::Shift(-1));
        assert_eq!(heads(&s), ["a garden", "Boiler", "House"]);
        assert_eq!(s.notes.get("garden").unwrap().after, None, "at the top");

        let undo = s.undo.len();
        assert!(
            s.act(Action::Shift(-1)).is_empty(),
            "the top moved up writes nothing"
        );
        select(&mut s, "House");
        assert!(s.act(Action::Shift(1)).is_empty());
        assert_eq!(s.undo.len(), undo);
        s.act(Action::Select(None));
        assert_eq!(heads(&s), ["a garden", "Boiler", "House"]);

        // What the line finds is in the order of its scores, so nothing there is moved.
        type_in(&mut s, "garden");
        s.act(Action::Move(1));
        assert!(!labels(&s).iter().any(|l| l.starts_with("move")));
        assert!(s.act(Action::Shift(1)).is_empty());
    }

    #[test]
    fn a_note_moves_among_the_facts_of_a_place() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        put(&mut s, "house", "House", &["boiler"], 2_000);
        put(&mut s, "garden", "a garden", &["boiler"], 3_000);
        put(&mut s, "oil", "Order oil", &["boiler"], 4_000);
        go_into(&mut s, "Boiler");
        assert_eq!(heads(&s), ["Order oil", "a garden", "House"]);
        select(&mut s, "House");
        s.act(Action::Shift(-1));
        assert_eq!(heads(&s), ["Order oil", "House", "a garden"]);
        s.act(Action::Shift(-1));
        assert_eq!(heads(&s), ["House", "Order oil", "a garden"]);
        assert_eq!(s.selected().unwrap().head, "House");
        select(&mut s, "a garden");
        assert!(!labels(&s).contains(&"move down"), "the facts end here");
        s.act(Action::Back);
        assert_eq!(heads(&s), ["House", "Order oil", "a garden", "Boiler"]);
    }

    #[test]
    fn a_note_moves_alone_and_undo_puts_back_what_was_written_under_it_too() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        under(&mut s, "Order oil");
        select(&mut s, "House");
        under(&mut s, "Bleed the radiators");
        let written = [
            "Boiler",
            "House",
            "Bleed the radiators",
            "Order oil",
            "a garden",
        ];
        assert_eq!(heads(&s), written);

        select(&mut s, "House");
        let undo = s.undo.len();
        let moved = s.act(Action::Shift(-1));
        assert_eq!(
            kept(&moved).len(),
            3,
            "the note and the two placed after it"
        );
        assert_eq!(s.undo.len(), undo + 1, "one change");
        s.act(Action::Select(None));
        assert_eq!(
            heads(&s),
            [
                "House",
                "Boiler",
                "Bleed the radiators",
                "Order oil",
                "a garden"
            ],
            "the run under it stays where it was, in its order"
        );

        select(&mut s, "House");
        s.act(Action::Undo);
        assert_eq!(heads(&s), written);
        assert_eq!(s.selected().unwrap().head, "House");
        assert_eq!(s.undo.len(), undo);
        s.act(Action::Redo);
        assert_eq!(
            heads(&s),
            [
                "House",
                "Boiler",
                "Bleed the radiators",
                "Order oil",
                "a garden"
            ]
        );

        // Down past what was written under it, it swaps with the row below and no more.
        s.act(Action::Undo);
        s.act(Action::Select(None));
        select(&mut s, "House");
        s.act(Action::Shift(1));
        assert_eq!(
            heads(&s),
            [
                "Boiler",
                "Bleed the radiators",
                "House",
                "Order oil",
                "a garden"
            ]
        );
    }

    #[test]
    fn a_note_placed_after_one_deleted_or_archived_keeps_its_place() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        under(&mut s, "Order oil");
        select(&mut s, "Boiler");
        under(&mut s, "Bleed the radiators");
        assert_eq!(
            heads(&s),
            [
                "Boiler",
                "Bleed the radiators",
                "House",
                "Order oil",
                "a garden"
            ]
        );
        select(&mut s, "House");
        s.act(Action::Delete);
        select(&mut s, "Boiler");
        s.act(Action::Archive);
        s.act(Action::Select(None));
        assert_eq!(heads(&s), ["Bleed the radiators", "Order oil", "a garden"]);
        s.act(Action::ShowArchived);
        assert_eq!(
            heads(&s),
            ["Boiler", "Bleed the radiators", "Order oil", "a garden"]
        );
    }

    #[test]
    fn a_move_or_an_edit_carries_the_other_side_of_the_note_as_it_was_written() {
        // Two other devices wrote the note and placed it in the same second, and this device has
        // heard from only one of them.
        let first = Record {
            after: Some("house".into()),
            ..theirs("oil", "Order oil", 60)
        };
        let second = Record {
            text: "Order oil by Friday".into(),
            after: Some("garden".into()),
            device: "3f9a01c3".into(),
            ..first.clone()
        };
        let merged = |versions: &[&Record]| {
            let mut notes = Notes::new();
            for version in versions {
                notes.apply((*version).clone());
            }
            notes.get("oil").cloned().unwrap()
        };
        let both = merged(&[&first, &second]);
        for change in [Action::Shift(-1), Action::Edited("Order oil, paid".into())] {
            let mut s = session();
            three(&mut s);
            s.absorb([first.clone()]);
            select(&mut s, "Order oil");
            let effects = s.act(change.clone());
            let [version] = kept(&effects)[..] else {
                panic!("{effects:?}");
            };
            let all = merged(&[&first, &second, version]);
            match change {
                Action::Shift(_) => assert_eq!(all.text, both.text, "a move is not an edit"),
                _ => assert_eq!(all.after, both.after, "an edit is not a move"),
            }
        }
    }

    #[test]
    fn a_fact_moved_up_goes_past_the_fact_above_it_and_nothing_else() {
        let mut s = session();
        put(&mut s, "boiler", "Boiler", &[], 1_000);
        put(&mut s, "oil", "Order oil", &["boiler"], 2_000);
        put(&mut s, "garden", "a garden", &["boiler"], 3_000);
        put(&mut s, "house", "House", &[], 4_000);
        put(
            &mut s,
            "radiators",
            "Bleed the radiators",
            &["boiler"],
            5_000,
        );
        go_into(&mut s, "Boiler");
        assert_eq!(
            heads(&s)[..3],
            ["Bleed the radiators", "a garden", "Order oil"]
        );
        select(&mut s, "Order oil");
        s.act(Action::Shift(-1));
        assert_eq!(
            heads(&s)[..3],
            ["Bleed the radiators", "Order oil", "a garden"]
        );
        s.act(Action::Back);
        assert_eq!(
            heads(&s),
            [
                "Bleed the radiators",
                "House",
                "Order oil",
                "a garden",
                "Boiler"
            ],
            "it has not jumped the note between the two facts"
        );
    }

    #[test]
    fn a_note_gone_elsewhere_is_not_moved() {
        let mut s = session();
        put(&mut s, "house", "House", &[], 1_000);
        put(&mut s, "boiler", "Boiler", &[], 2_000);
        s.act(Action::Move(1));
        let gone = Record {
            deleted: true,
            stamp: Some(NOON.plus_secs(120)),
            device: "3f9a01c2".into(),
            ..s.notes.get("boiler").unwrap().clone()
        };
        s.absorb([gone]);
        assert!(kept(&s.act(Action::Shift(1))).is_empty());
    }

    #[test]
    fn undoing_a_move_leaves_what_another_device_has_written_since() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "House");
        s.act(Action::Shift(1));
        s.act(Action::Select(None));
        let edit = Record {
            text: "House insurance".into(),
            stamp: Some(NOON.plus_secs(60)),
            device: "3f9a01c2".into(),
            ..s.notes.get("house").unwrap().clone()
        };
        s.absorb([edit]);
        s.act(Action::Undo);
        assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
    }

    #[test]
    fn putting_back_a_head_leaves_where_another_device_has_moved_its_note() {
        let mut s = session();
        three(&mut s);
        select(&mut s, "a garden");
        edited(&mut s, "a garden, dug over");
        let moved = Record {
            after: Some("boiler".into()),
            placed: Some(NOON.plus_secs(60)),
            ..s.notes.get("garden").unwrap().clone()
        };
        s.absorb([moved]);
        s.act(Action::Undo);
        assert_eq!(field(&s).unwrap().0, "a garden");
        s.act(Action::Select(None));
        assert_eq!(heads(&s), ["Boiler", "a garden", "House"]);
    }

    #[test]
    fn a_line_of_nothing_but_what_holds_lists_exactly_that_in_the_spaces_order() {
        let mut s = session();
        house(&mut s);
        let carrying = [
            "fix the roof tiles #house",
            "clear the gutter #house",
            "Boiler",
        ];
        for typed in ["#house", "#ho", "#house a"] {
            clear(&mut s);
            type_in(&mut s, typed);
            assert_eq!(
                heads(&s),
                carrying,
                "{typed}: what carries it in the order kept, and not what says its word"
            );
            assert!(
                s.rows
                    .iter()
                    .all(|r| r.reasons.is_empty() && r.score.is_none())
            );
        }
        select(&mut s, "clear the gutter #house");
        assert!(labels(&s).contains(&"move up") && labels(&s).contains(&"move down"));

        s.act(Action::Select(None));
        clear(&mut s);
        type_in(&mut s, "#house roof");
        assert_eq!(
            heads(&s),
            ["fix the roof tiles #house"],
            "words rank among them"
        );
        assert!(!s.rows[0].reasons.is_empty());
        clear(&mut s);
        type_in(&mut s, "#house cake");
        assert!(heads(&s).is_empty(), "and find nothing that does not hold");
    }

    #[test]
    fn a_line_of_labels_is_gone_into_one_after_another() {
        let mut s = session();
        house(&mut s);
        put(&mut s, "boots", "dry the boots #house", &[], NOON.secs());
        type_in(&mut s, "#house /today");
        assert!(labels(&s).contains(&"go in"));
        s.act(Action::GoIn);
        assert_eq!(
            path(&s),
            ["#house", "2026-10-13"],
            "one place, as if Right after each"
        );
        assert_eq!(s.line, "");
        assert_eq!(heads(&s), ["dry the boots #house"]);

        type_in(&mut s, "#house @joe");
        s.act(Action::GoIn);
        assert_eq!(
            path(&s),
            ["#house", "2026-10-13", "@joe"],
            "of several, a label the place has already is passed over"
        );

        // Out a step at a time, each line coming back on the way out of the first place it
        // went into.
        s.act(Action::Back);
        assert_eq!(path(&s), ["#house", "2026-10-13"]);
        assert_eq!(s.line, "#house @joe");
        clear(&mut s);
        s.act(Action::Back);
        assert_eq!((path(&s), s.line.as_str()), (vec!["#house"], ""));
        s.act(Action::Back);
        assert!(path(&s).is_empty());
        assert_eq!(s.line, "#house /today");
    }

    #[test]
    fn a_span_is_said_beside_the_line_and_is_not_a_place() {
        let mut s = session();
        // Monday 16:00 and 20:00, the day before the tests' noon.
        put(
            &mut s,
            "early",
            "bled the radiators",
            &[],
            NOON.secs() - 20 * 3_600,
        );
        put(&mut s, "late", "ordered oil", &[], NOON.secs() - 16 * 3_600);
        type_in(&mut s, "/-mon 18:00..");
        assert_eq!(s.view().dates, ["Mon 2026-10-12 18:00 – now"]);
        assert_eq!(heads(&s), ["ordered oil"], "written since then");
        clear(&mut s);
        type_in(&mut s, "#house /-mon..");
        assert!(!labels(&s).contains(&"go in"), "a span is not a place");
        s.act(Action::GoIn);
        assert!(path(&s).is_empty());
        assert_eq!(s.line, "#house /-mon..");

        // Enter writes the line as it reads, its date words written out.
        clear(&mut s);
        type_in(&mut s, "ask about “the leak” /-mon 18:00..");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(
            s.notes.get(&new).unwrap().text,
            "ask about “the leak” 2026-10-12 18:00.."
        );
    }

    #[test]
    fn inside_a_place_what_the_line_holds_narrows_its_facts_and_its_finds_alike() {
        let mut s = session();
        boiler(&mut s);
        go_to(&mut s, "Boiler");
        for (typed, listed, facts) in [
            ("#house", &["Pressure dropped #house"][..], &[true][..]),
            ("/yesterday", &["Service booked"], &[true]),
            ("oil /today", &["Order oil #house"], &[false]),
            ("oil /yesterday", &[], &[]),
        ] {
            clear(&mut s);
            type_in(&mut s, typed);
            assert_eq!(heads(&s), listed, "{typed}");
            assert_eq!(linked(&s), facts, "{typed}");
        }
    }

    #[test]
    fn letters_rank_by_what_they_score_then_the_shorter_head_then_the_spaces_order() {
        let mut s = session();
        put(&mut s, "list", "bs list", &[], 500);
        put(&mut s, "shed", "Boiler shed", &[], 1_000);
        put(&mut s, "sheds", "Boiler sheds", &[], 2_000);
        put(&mut s, "shop", "Bottle shop!", &[], 3_000);
        put(&mut s, "body", "Notes\nboiler shed in the body", &[], 4_000);
        type_in(&mut s, "~bs");
        assert_eq!(
            heads(&s),
            ["bs list", "Boiler shed", "Bottle shop!", "Boiler sheds"]
        );
        assert_eq!(s.rows[0].reasons, ["letters: bs"]);

        clear(&mut s);
        select(&mut s, "Boiler sheds");
        s.act(Action::Shift(-1));
        s.act(Action::Select(None));
        type_in(&mut s, "~bs");
        assert_eq!(
            heads(&s)[2..],
            ["Boiler sheds", "Bottle shop!"],
            "a tie goes by the space's order"
        );
    }

    /// Two open tasks in `#house` and one outside it, one done and one archived without its tick,
    /// and a note with a task in its body, which is not one.
    fn tasks(s: &mut Session) {
        put(s, "oil", "[ ] Order oil #house", &[], 1_000);
        put(s, "radiators", "[ ] Bleed the radiators #house", &[], 2_000);
        put(s, "chimney", "[x] Sweep the chimney #house", &[], 3_000);
        put(s, "shed", "[ ] Paint the shed #house", &[], 4_000);
        put(s, "stamps", "[ ] Buy stamps", &[], 5_000);
        put(
            s,
            "boiler",
            "Boiler #house\n- [ ] get three quotes",
            &[],
            6_000,
        );
        for id in ["chimney", "shed"] {
            let held = s.notes.get(id).unwrap().clone();
            s.commit(Record {
                archived: true,
                ..held
            });
        }
        s.refresh(true);
    }

    #[test]
    fn the_box_on_the_line_holds_the_open_tasks_and_the_rest_of_the_line_still_applies() {
        let mut s = session();
        tasks(&mut s);
        let open = [
            "[ ] Buy stamps",
            "[ ] Bleed the radiators #house",
            "[ ] Order oil #house",
        ];
        for typed in ["[ ]", "[ ] a"] {
            clear(&mut s);
            type_in(&mut s, typed);
            assert_eq!(heads(&s), open, "{typed:?}");
        }
        s.act(Action::ShowArchived);
        assert_eq!(heads(&s), open, "a task archived is not open");
        s.act(Action::ShowArchived);
        clear(&mut s);
        type_in(&mut s, "[ ] #house");
        assert_eq!(heads(&s), open[1..]);
        clear(&mut s);
        type_in(&mut s, "[ ] oil");
        assert_eq!(heads(&s), ["[ ] Order oil #house"]);
        let exported: Vec<&str> = s.export("[ ]").iter().map(|r| r.id.as_str()).collect();
        assert_eq!(exported, ["stamps", "radiators", "oil"]);
    }

    #[test]
    fn the_box_is_a_place_and_a_new_row_there_opens_with_it_before_the_caret() {
        let mut s = session();
        tasks(&mut s);
        type_in(&mut s, "#house [ ]");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["#house", "[ ]"]);
        assert_eq!(
            heads(&s),
            ["[ ] Bleed the radiators #house", "[ ] Order oil #house"]
        );

        // The box first, then the caret, then the other labels.
        assert!(s.act(Action::Write).is_empty());
        let fresh = |s: &Session| s.view().field.map(|f| (f.text.to_owned(), f.fresh));
        assert_eq!(fresh(&s), Some(("[ ]  #house".to_owned(), Some(4))));
        for left in ["[ ] ", "[ ] #house", "[ ]   #house", "[ ]  #ho", "[", ""] {
            assert!(
                edited(&mut s, left).is_empty(),
                "nothing of the writer's: {left:?}"
            );
        }
        let typed = edited(&mut s, "[ ] B #house");
        let [Effect::Keep(kept)] = &typed[..] else {
            panic!("one version: {typed:?}");
        };
        assert_eq!(kept.text, "[ ] B #house");
        s.act(Action::Undo);
        assert_eq!(fresh(&s), Some(("[ ]  #house".to_owned(), Some(4))));
        s.act(Action::Erase);
        assert_eq!(s.cursor, None, "Backspace where it opened takes it away");

        type_in(&mut s, "Fix the gate");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(s.notes.get(&new).unwrap().text, "[ ] Fix the gate #house");
        s.act(Action::Select(None));
        type_in(&mut s, "[ ] Prune the roses");
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(
            s.notes.get(&new).unwrap().text,
            "[ ] Prune the roses #house",
            "never two boxes"
        );

        // Gone into the other way round, it is one place all the same.
        s.act(Action::Select(None));
        s.act(Action::Back);
        s.act(Action::Back);
        clear(&mut s);
        type_in(&mut s, "[ ]");
        s.act(Action::GoIn);
        type_in(&mut s, "#house");
        s.act(Action::GoIn);
        assert_eq!(path(&s), ["[ ]", "#house"]);
        s.act(Action::Write);
        assert_eq!(fresh(&s), Some(("[ ]  #house".to_owned(), Some(4))));
    }

    #[test]
    fn ticking_a_task_writes_its_box_and_archives_it_in_one_version_and_undo_puts_back_both() {
        let mut s = session();
        tasks(&mut s);
        let mut log = Vec::new();
        select(&mut s, "[ ] Order oil #house");
        assert!(labels(&s).contains(&"tick"));
        let ticked = s.act(Action::Archive);
        let [Effect::Keep(kept)] = &ticked[..] else {
            panic!("one version: {ticked:?}");
        };
        assert_eq!(
            (kept.text.as_str(), kept.archived),
            ("[x] Order oil #house", true)
        );
        assert_eq!(
            kept.updated,
            Some(NOON),
            "the box is in the text, so it is an edit"
        );
        assert_eq!(s.status, "ticked");
        assert!(!heads(&s).contains(&"[x] Order oil #house"));
        log.extend(ticked);

        let undone = s.act(Action::Undo);
        let [Effect::Keep(kept)] = &undone[..] else {
            panic!("one version: {undone:?}");
        };
        assert_eq!(
            (kept.text.as_str(), kept.archived),
            ("[ ] Order oil #house", false)
        );
        assert_eq!(kept.updated, Some(Moment::utc(1_000)));
        log.extend(undone);
        log.extend(s.act(Action::Redo));
        for notes in [reopened(&log), reopened(log.iter().rev())] {
            let oil = notes.get("oil").unwrap();
            assert_eq!(
                (oil.text.as_str(), oil.archived),
                ("[x] Order oil #house", true)
            );
        }

        // Shown, a done task is unticked as it is brought back, and so is one archived without
        // its tick.
        s.act(Action::Select(None));
        s.act(Action::ShowArchived);
        for (head, unticked) in [
            ("[x] Order oil #house", "[ ] Order oil #house"),
            ("[ ] Paint the shed #house", "[ ] Paint the shed #house"),
        ] {
            select(&mut s, head);
            assert!(labels(&s).contains(&"untick"));
            s.act(Action::Archive);
            assert_eq!(s.status, "unticked");
            let id = s.selected().unwrap().id.clone();
            let held = s.notes.get(&id).unwrap();
            assert_eq!((held.text.as_str(), held.archived), (unticked, false));
            s.act(Action::Select(None));
        }

        // Anything else is archived as it always was, a box in its body or not.
        select(&mut s, "Boiler #house");
        assert!(labels(&s).contains(&"archive"));
        s.act(Action::Archive);
        assert_eq!(s.status, "archived");
        assert_eq!(
            s.notes.get("boiler").unwrap().text,
            "Boiler #house\n- [ ] get three quotes"
        );
    }

    #[test]
    fn going_into_a_note_the_line_holds_was_a_search_and_a_draft_carrying_a_tag_is_not() {
        let mut s = session();
        house(&mut s);
        for typed in ["~fr", "\"roof tiles\"", "#house roof"] {
            clear(&mut s);
            type_in(&mut s, typed);
            go_into(&mut s, "fix the roof tiles #house");
            assert_eq!(s.line, "", "{typed} was a search");
            s.act(Action::Back);
            assert_eq!(s.line, typed);
        }
        clear(&mut s);
        type_in(&mut s, "boiler needs bleeding #house");
        go_into(&mut s, "Boiler");
        assert_eq!(
            s.line, "boiler needs bleeding #house",
            "a draft, carried in to be written"
        );
    }
}
