//! The field: the row under the cursor, written in place. It is a part of its own because what
//! one visit to a row writes, and when, is the most delicate rule the session keeps: a head is
//! written as it is typed, a body when it closes, a new note at its first letter and taken away if
//! it is left empty, and all a visit writes is one entry to undo. Kept together, those rules are
//! not bent by the rest.

use crate::record::Record;
use crate::text;
use crate::time::{Day, Moment};

use super::{Action, Effect, Row, Session};

/// The note under the cursor as it is being written: what it now reads, and what it was. One
/// lasts as long as one visit of the cursor to a row, and everything it writes in that time is
/// one entry to undo.
pub(super) struct Writing {
    pub(super) id: String,
    pub(super) text: String,
    /// What the note read when the cursor landed, or when the body last closed or the editor
    /// last came back; for a new note, what it opened with. Putting back goes back to this.
    original: String,
    /// The row was opened as a new note in this visit, so that left with nothing in it but its
    /// label it goes rather than being deleted.
    new: bool,
    /// The body is open: the field holds the whole note and not only its head.
    pub(super) whole: bool,
    /// Typed into in this visit, whether or not what was typed has been written out, and not
    /// put back since.
    touched: bool,
    /// When the cursor landed, which is `created` for a new note.
    opened: Moment,
    /// What a new note's head opened with: the labels of the place it was opened in, the box
    /// first and the rest each after a space, or nothing. It is not the writer's, so a row with
    /// only this in it is empty.
    label: String,
    /// The entry on the undo stack that this visit's versions are gathered into, once one has
    /// been written.
    pub(super) entry: Option<usize>,
    /// The note you are in, its body opened from the line: closing it goes back to the line.
    pub(super) from_line: bool,
}

impl Writing {
    /// A visit to a note that reads `text`, begun at `opened`, with nothing typed yet.
    fn new(id: String, text: String, opened: Moment) -> Self {
        Writing {
            id,
            original: text.clone(),
            text,
            new: false,
            whole: false,
            touched: false,
            opened,
            label: String::new(),
            entry: None,
            from_line: false,
        }
    }

    /// Whether the text has changed in this visit: from what it read when the cursor landed, or
    /// was last written out on closing its body, which for a new note is what it opened with at
    /// first. A new note with nothing in it has nothing to put back.
    pub(super) fn differs(&self) -> bool {
        if self.blank() {
            return false;
        }
        // A newline at the end is dropped on writing, so it is not a change.
        let ends = ['\n', '\r'];
        self.text.trim_end_matches(ends) != self.original.trim_end_matches(ends)
    }

    /// Whether the person is writing in it: a new note, a body open, or a head typed into in
    /// this visit. Such a field's row is not taken from under the hands.
    pub(super) fn in_hand(&self) -> bool {
        self.new || self.whole || self.touched
    }

    /// A new note with nothing in it but the label it opened with, which goes if it is left so.
    pub(super) fn blank(&self) -> bool {
        self.new && self.bare(&self.text)
    }

    /// A new note that leaving would leave with nothing of the writer's: blank, or with nothing
    /// but what it opened with, with or without a box before it, once its task word is written
    /// out, as `/task` typed in a new row in `#house` leaves `[ ] #house`. The task word is a box
    /// and not the writer's text, so such a row goes as a blank one does rather than keep an
    /// empty task.
    pub(super) fn empty(&self) -> bool {
        self.blank()
            || (self.new && {
                let written = text::expand_task_word(&self.text);
                written != self.text
                    && (self.bare(&written)
                        || self.bare(written.strip_prefix(text::OPEN).unwrap_or(&written)))
            })
    }

    /// Where the session puts the caret in the head: at its end, or in a new note with nothing in
    /// it but its labels, where the writer's text goes.
    pub(super) fn home(&self) -> usize {
        if self.blank() {
            self.start()
        } else {
            self.head().chars().count()
        }
    }

    /// The head, the text's first line.
    pub(super) fn head(&self) -> &str {
        self.text.lines().next().unwrap_or("")
    }

    /// Whether the head holds a word that saving it writes out as something else: `/task` as
    /// the box, or a date word as its date.
    pub(super) fn has_words(&self, today: Day) -> bool {
        let head = self.head();
        text::expand_words(head, today) != head
    }

    /// Nothing in the head but the label a new note opened with. Such a head is not written
    /// while the cursor is on it.
    fn head_empty(&self) -> bool {
        self.bare(self.head())
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
    pub(super) fn start(&self) -> usize {
        match text::task(&self.label) {
            Some(_) => text::OPEN.len() + 1,
            None => 0,
        }
    }
}

/// A version of a note written in the visit that opened it, whose archived flag follows its
/// box until the cursor leaves: a new note headed `[x]` is something done being logged, done as
/// it is written, and one whose box is taken out before then is a note like any other. A note
/// written before is ticked only by the key, so typing a box into it changes nothing else.
fn first_visit(new: bool, record: Record) -> Record {
    if !new || record.deleted {
        return record;
    }
    Record {
        archived: text::task(&record.text) == Some(true),
        ..record
    }
}

impl Session {
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

    /// What an action does to the note under the cursor. The few that belong to the field are
    /// done here; every other leaves it first, writing out what is left to write, and is given
    /// back to be done as it would be. One that asks for the note under the cursor is dropped
    /// when leaving took that note away, so that it does not fall on a neighbour.
    pub(super) fn on_field(&mut self, action: Action) -> Option<Action> {
        let today = self.today();
        let Some(field) = self.field.as_mut() else {
            return Some(action);
        };
        match action {
            // A head is kept as it is typed; an open body waits for it to close.
            Action::Edited(text) => {
                field.touched |= text != field.text;
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
            Action::Open => field.whole = true,
            // The editor is handed the note as it now reads, and what it leaves is written with
            // what was typed here and what was written there.
            Action::Editor => {
                let (id, text) = (field.id.clone(), field.text.clone());
                self.out.push(Effect::Editor { id, text });
            }
            Action::Save { id, text } if id == field.id => {
                field.text = text;
                if field.empty() {
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
            Action::Close if field.empty() => {
                field.whole = false;
                field.text = field.label.clone();
                self.unwrite_new();
            }
            // The note you are in, opened from the line, closes back onto the line, and its row,
            // put there for it, goes with the cursor.
            Action::Close => {
                self.flush();
                match self.field.as_mut() {
                    Some(field) if field.from_line => {
                        self.field = None;
                        self.onto_line();
                    }
                    Some(field) => field.whole = false,
                    None => {}
                }
            }
            // Kept as it stands, and nothing else changes. A new note with nothing in it has
            // nothing to keep, and a head emptied, or cleared to be typed again, is not kept,
            // since only leaving the row writes that: keeping it would delete the note, or make
            // its body's first line its head, under the hands.
            Action::Keep if field.empty() || field.head_empty() => {}
            Action::Keep => {
                self.flush();
            }
            Action::ShowScores => self.show_scores = !self.show_scores,
            // A new note with nothing in it deleted goes as if it was never anything.
            Action::Delete if field.empty() => {
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
                    match self.revise(&id, String::new()) {
                        Some(record) => {
                            self.gather(record);
                            self.status = "deleted · undo brings it back".into();
                        }
                        None => self.status = "that note is gone, so nothing was saved".into(),
                    }
                    self.field = None;
                    self.cursor = self.drop_row(&id).and_then(|i| i.checked_sub(1));
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
                if field.empty() => {}
            // A head with a word in it still to be written out, `/task` or a date word, has it
            // written out, and the cursor stays on the row: Enter makes what was typed what it
            // stands for, and the next Enter opens a row under.
            Action::Write if !field.whole && field.has_words(today) => {
                self.flush();
            }
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
                let (id, blank) = (field.id.clone(), field.empty());
                // A note emptied is deleted when the cursor leaves it, and its row goes as a new
                // note's left empty does.
                let emptied = !field.new && self.notes.get(&id).is_some() && {
                    let text = field.text.clone();
                    self.normalise(&text).is_empty()
                };
                // A tap lands on the note it was aimed at, however the rows move when the field
                // is left: a new note left empty takes its row with it, and a list put in order
                // puts the rows elsewhere.
                let aimed = match &action {
                    Action::Select(Some(row)) => self.rows.get(*row).map(|r| r.id.clone()),
                    _ => None,
                };
                // An arrow off a new note left empty, or a note emptied, steps from where its row
                // was, which is gone by the time it lands: up to the row above it, down to the row
                // that was under it.
                let past = match action {
                    Action::Move(by) if blank || emptied => {
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
                    if blank {
                        self.abandon();
                    } else {
                        self.flush();
                        self.field = None;
                    }
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
        let (id, new) = (field.id.clone(), field.new);
        let text = field.text.trim_end_matches(['\n', '\r']).to_owned();
        let record = match self.notes.get(&id) {
            Some(held) if held.text == text => return,
            Some(_) => self
                .revise(&id, text)
                .map(|record| first_visit(new, record)),
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
        let around = self.place_note();
        let links = self.engine.links_on_save(
            &field.id,
            "",
            &text,
            around.map_or(&[], std::slice::from_ref),
        );
        Record {
            links,
            created: Some(field.opened),
            updated: Some(self.host.now()),
            archived: text::task(&text) == Some(true),
            after: self
                .rows
                .iter()
                .position(|r| r.id == field.id)
                .and_then(|i| i.checked_sub(1))
                .map(|i| self.rows[i].id.clone()),
            ..Record::new(field.id.clone(), text)
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
        // Put back, it is no longer being written in.
        field.touched = false;
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
        if blank {
            self.blank_row(&id);
        }
        self.mend_row(&id);
        self.stale = true;
    }

    /// Keeps the field with the cursor: wherever the cursor has landed, the note under it is the
    /// field. A field for the note already under the cursor stays as it is, except that while it
    /// is unchanged in this visit a version of that note arrived from elsewhere shows in it. One
    /// typed into is left as it reads, and is written past theirs at the next keystroke or when
    /// the cursor leaves.
    pub(super) fn land(&mut self) {
        // A row kept under the hands while its note went goes once the cursor is off it, unless
        // what was written there since, a head put back say, has brought the note back.
        if let Some(going) = self.going.clone()
            && self.selected().is_none_or(|r| r.id != going)
        {
            let goes = self.goes(&going, &mut None);
            self.going = None;
            if goes && let Some(at) = self.drop_row(&going) {
                self.cursor = self.cursor.map(|i| if at < i { i - 1 } else { i });
            }
        }
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
                self.field = Some(Writing::new(id, text, self.host.now()));
            }
        }
    }

    /// Opens the body of the note you are in from the line, as a row's opens: the note is put
    /// under the cursor, at the top of the list, where the note the line names is listed, unless
    /// the line names it and it is listed there already. A row put there for it is going, so that
    /// it goes once the cursor leaves it, as closing the body takes it back to the line, and the
    /// list is as it was. With no note on the path there is nothing to open.
    pub(super) fn open_here(&mut self) {
        let Some(record) = self.place_note().and_then(|id| self.notes.get(id)) else {
            return;
        };
        let at = match self.rows.iter().position(|r| r.id == record.id) {
            Some(at) => at,
            None => {
                let row = self.listed(record);
                self.going = Some(row.id.clone());
                self.rows.insert(0, row);
                0
            }
        };
        self.cursor = Some(at);
        self.land();
        if let Some(field) = self.field.as_mut() {
            field.whole = true;
            field.from_line = true;
        }
    }

    /// Hands the note you are in to the face's editor from the line, as a row's is handed: what
    /// the editor leaves comes back as [`Action::Save`], and the cursor stays on the line.
    pub(super) fn edit_here(&mut self) {
        let Some(record) = self.place_note().and_then(|id| self.notes.get(id)) else {
            return;
        };
        let (id, text) = (record.id.clone(), record.text.clone());
        self.out.push(Effect::Editor { id, text });
    }

    /// Opens a new note at this row of the list, with the cursor in it. It has its id from now,
    /// and its time, and it is written at its first letter. Inside labels its head opens with
    /// each of them, but for one the note carries already, as one written on the day it is does.
    pub(super) fn draft_at(&mut self, at: usize) {
        let id = self.host.new_id();
        let opened = self.host.now();
        let label = self.inherit(String::new(), opened.day());
        let at = at.min(self.rows.len());
        self.rows.insert(
            at,
            Row {
                id: id.clone(),
                head: String::new(),
                shown: None,
                task: None,
                archived: false,
                has_body: false,
                // Written here, it will be linked to the note being looked around, or carry the
                // labels.
                linked: !self.path.is_empty(),
                links: Vec::new(),
                reasons: Vec::new(),
                score: None,
            },
        );
        self.cursor = Some(at);
        self.field = Some(Writing {
            new: true,
            label: label.clone(),
            ..Writing::new(id, label, opened)
        });
    }

    /// A note's text as it is to be kept: date words written as dates, the task word in its head
    /// as the box at its start, no newline at the end and no blank line at the start, since a
    /// note's head is its first line. Nothing but blank lines is nothing.
    pub(super) fn normalise(&self, edited: &str) -> String {
        let mut rest = edited.trim_end_matches(['\n', '\r']);
        while let Some((first, after)) = rest.split_once('\n')
            && first.trim().is_empty()
        {
            rest = after;
        }
        if rest.trim().is_empty() {
            return String::new();
        }
        text::expand_words(rest, self.today())
    }

    /// Writes the note under the cursor as it is to be kept, with its date words written out, and
    /// leaves the cursor where it is. Says whether it wrote anything. What it is compared with is
    /// what the note holds, since the visit may have written it already as it was typed. A new
    /// note is written if it has text and was not written as typed, as when its body was typed
    /// into. A note emptied is deleted, which takes its row away now and leaves the rest of the
    /// list where it was, the cursor on the row that took its place; a new one emptied is left
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
                let record = first_visit(new, record);
                self.gather(record);
                if text.is_empty() {
                    self.status = "deleted · undo brings it back".into();
                    self.take_row(&id);
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
        // A new note headed `[x]` is archived as it is written, and its row says so once it is
        // written out, so that a version arriving later is not taken for a tick.
        if let (Some(row), Some(record)) = (
            self.rows.iter_mut().find(|r| r.id == id),
            self.notes.get(&id),
        ) {
            row.archived = record.archived;
        }
        self.stale = true;
        wrote
    }

    /// Writes out the note under the cursor, because the cursor is about to move. A new note
    /// left with nothing in it goes, and its row with it.
    fn leave(&mut self) {
        if self.field.as_ref().is_some_and(Writing::empty) {
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
        self.blank_row(&id);
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

    /// Puts a new note's row back as it opened: no head, no body and no links.
    fn blank_row(&mut self, id: &str) {
        if let Some(row) = self.rows.iter_mut().find(|r| r.id == id) {
            row.head = String::new();
            row.shown = None;
            row.task = None;
            row.has_body = false;
            row.links = Vec::new();
        }
    }

    /// Puts a row right where it stands, from the note as it is now held.
    pub(super) fn mend_row(&mut self, id: &str) {
        let Some(record) = self.notes.get(id) else {
            return;
        };
        let body = text::has_body(&record.text);
        let (head, shown, links) = (
            record.head().to_owned(),
            self.shown(record.head()),
            self.link_heads(record),
        );
        let task = text::task(&record.text);
        if let Some(row) = self.rows.iter_mut().find(|r| r.id == id) {
            row.head = head;
            row.shown = shown;
            row.task = task;
            row.has_body = body;
            row.links = links;
        }
    }
}
