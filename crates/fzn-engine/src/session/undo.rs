//! Changes: every version the session writes goes through `write` here, and every change an
//! action makes is an entry to undo. It is a part of its own so that there is one way a note is
//! changed, and undo sees every change. The actions that change a note from the line or the list
//! live here too: writing the line, saving, archiving and ticking, deleting and moving.

use crate::note::NoteId;
use crate::record::{Record, links_on_save};
use crate::text;

use super::{Effect, Session};

/// The versions one change wrote, in the order it wrote them: what each note was before, and
/// what it became. Most changes are to one note; a move is to the note and what was placed after
/// it.
pub(super) type Change = Vec<(Option<Record>, Record)>;

impl Session {
    /// Writes a version, remembering what it replaced for undo.
    pub(super) fn commit(&mut self, record: Record) {
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
    pub(super) fn gather(&mut self, record: Record) {
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

    pub(super) fn undo(&mut self) {
        let Some(change) = self.undo.pop() else {
            self.status = "nothing to undo".into();
            return;
        };
        self.restore(&change, true);
        self.status = format!("undid: {}", text::fit(change[0].1.head(), 40));
        self.redo.push(change);
    }

    pub(super) fn redo(&mut self) {
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
    pub(super) fn write_line(&mut self) {
        let today = self.today();
        let text = text::expand_date_words(self.line.trim(), today);
        if text.is_empty() {
            self.draft_at(0);
            return;
        }
        let text = self.inherit(text, today);
        let around = self.place_note().map_or(&[][..], std::slice::from_ref);
        let id = self.host.new_id();
        let links = links_on_save(&self.engine, &id, "", &text, around);
        let now = self.host.now();
        self.commit(Record {
            links,
            created: Some(now),
            updated: Some(now),
            ..Record::new(id.clone(), text)
        });
        self.line.clear();
        self.refresh(true);
        self.reselect(&id);
        self.status = "created".into();
    }

    /// Takes the text of a note as the editor left it, for a note that is not under the cursor.
    pub(super) fn save(&mut self, id: &str, edited: &str) {
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
    pub(super) fn revise(&self, id: &str, text: String) -> Option<Record> {
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

    /// Archives the note under the cursor, or brings it back.
    pub(super) fn archive(&mut self) {
        let Some(record) = self.selected_record().cloned() else {
            return;
        };
        let id = record.id.clone();
        let (record, ticked) = self.archiving(record);
        self.status = match (ticked, record.archived) {
            (Some(true), _) => "ticked",
            (Some(false), _) => "unticked",
            (None, true) => "archived",
            (None, false) => "unarchived",
        }
        .into();
        self.commit(record);
        self.refresh(false);
        self.reselect(&id);
    }

    /// A note archived, or brought back if it is archived, as the version to write, and for a
    /// task whether it is now done. A task's box says the same in its text, ticked or not, and is
    /// written in the same version, so that no device sees one without the other and undo puts
    /// back both.
    fn archiving(&self, record: Record) -> (Record, Option<bool>) {
        let archived = !record.archived;
        let ticked = text::task(&record.text).map(|_| archived);
        let record = match ticked.map(|done| text::tick(&record.text, done)) {
            Some(text) if text != record.text => self.revise(&record.id, text).unwrap_or(record),
            _ => record,
        };
        (Record { archived, ..record }, ticked)
    }

    /// Ticks these tasks, or unticks them, each as archiving it under the cursor would: one
    /// version a note. All or none: an id that names no note, or a note that archiving would not
    /// tick, or untick, is said by its id, and nothing is written. Asked of a session that is
    /// writing no note, as the command line's is, and gives back what is to be kept.
    pub fn tick(&mut self, ids: &[String], done: bool) -> Result<Vec<Effect>, String> {
        let mut versions: Vec<Record> = Vec::new();
        for id in ids {
            if versions.iter().any(|version| &version.id == id) {
                continue;
            }
            let Some(record) = self.notes.get(id).cloned() else {
                return Err(format!("no note has the id {id}"));
            };
            match self.archiving(record) {
                (version, Some(ticked)) if ticked == done => versions.push(version),
                _ => {
                    let task = if done { "an open" } else { "a done" };
                    return Err(format!("{id} is not {task} task"));
                }
            }
        }
        for version in versions {
            self.write(version);
        }
        self.refresh(false);
        Ok(std::mem::take(&mut self.out))
    }

    pub(super) fn delete(&mut self) {
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

    /// Moves the note under the cursor `by` rows of the list as it is shown, as the original's
    /// move up and move down did, one note alone. Down, it is placed after the row it passes;
    /// up, after the note that row follows in the space's order, or at the top. Either way it
    /// lands beside that row, in any view, filtered or not, and goes past nothing else. What was
    /// placed after it is placed instead after the note it followed, so that a run written under
    /// it stays where it was, in its order, and no cycle is made. The whole is one change to
    /// undo, which puts the note back after the note it followed.
    pub(super) fn shift(&mut self, by: i64) {
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
}
