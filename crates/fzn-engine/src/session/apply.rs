//! Notes written from outside, as `fzn apply` gives them: the lines `fzn query` writes, changed
//! by a script or a person, or written by hand. It is a part of its own because a line carries
//! more than its writer means to change, since a snapshot names every field, and each field has
//! to be read by a rule of its own: what the app would do with it, what is fzn's alone, and what
//! cannot be done from outside yet. Everything is checked before anything is written, so that a
//! refactor either lands whole or not at all.

use std::collections::HashSet;

use serde::{Deserialize, Deserializer};

use crate::record::Record;
use crate::text;
use crate::time::Moment;

use super::{Effect, Session};

/// A note as it is given from outside. A line of `fzn query` reads as one, and so does a line
/// with an id and one field. What is not given is as held. What fzn keeps for itself is not
/// taken from the line: `created`, `updated`, `done` and `v` are not read at all, and `device`
/// and `placed` only to tell which version was read.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct Given {
    /// The note it is, or none for a new note.
    pub id: Option<String>,
    pub text: Option<String>,
    pub links: Option<Vec<String>>,
    pub archived: Option<bool>,
    pub deleted: Option<bool>,
    /// The version that was read, by when it was written and by whom, which has to be the one
    /// held still: a note changed since it was read is not written over unseen.
    pub stamp: Option<Moment>,
    pub device: Option<String>,
    /// The note it sits after, and `Some(None)` for the top, which has to be where it sits
    /// unless the note has been moved since it was read.
    #[serde(deserialize_with = "given")]
    pub after: Option<Option<String>>,
    /// When the note was put where it sits, as read, which tells a move made since it was read
    /// from a move asked for.
    #[serde(deserialize_with = "given")]
    pub placed: Option<Option<Moment>>,
}

/// A field that is there, even as null, as distinct from one that is not.
fn given<'de, D: Deserializer<'de>, T: Deserialize<'de>>(d: D) -> Result<Option<T>, D::Error> {
    T::deserialize(d).map(Some)
}

impl Session {
    /// Writes notes as they are given from outside, by the app's own rules: the text normalised
    /// as the editor's is and its brackets made links, a task's box and its archived flag kept
    /// together whichever was changed, and a line with no id a new note at the top. A line that
    /// changes nothing writes nothing. All or none: a line that cannot be written is said by its
    /// id, or by its place in the input for a new note, and nothing is written. Gives back each
    /// note as it now stands, in the order given, and what is to be kept. Asked of a session that
    /// is writing no note, as the command line's is.
    pub fn apply(&mut self, given: Vec<Given>) -> Result<(Vec<Record>, Vec<Effect>), String> {
        let mut ids: Vec<String> = Vec::with_capacity(given.len());
        let mut seen: HashSet<String> = HashSet::with_capacity(given.len());
        let mut versions = Vec::new();
        for (n, line) in given.into_iter().enumerate() {
            let id = line.id.clone().filter(|id| !id.is_empty());
            let (id, version) = match id {
                Some(id) => {
                    if !seen.insert(id.clone()) {
                        return Err(format!("{id} is given twice"));
                    }
                    let version = self.changed(&id, line)?;
                    (id, version)
                }
                None => {
                    let version = self.created_from(n + 1, line)?;
                    (version.id.clone(), Some(version))
                }
            };
            ids.push(id);
            versions.extend(version);
        }
        for version in versions {
            self.write(version);
        }
        self.refresh(false);
        self.derive();
        let notes = ids
            .iter()
            .filter_map(|id| self.notes.version(id).cloned())
            .collect();
        Ok((notes, std::mem::take(&mut self.out)))
    }

    /// The version a line makes of a held note, or none when it changes nothing.
    fn changed(&self, id: &str, line: Given) -> Result<Option<Record>, String> {
        let Some(held) = self.notes.version(id) else {
            return Err(format!("no note has the id {id}"));
        };
        // The same moment written in another zone is the same version; the writer tells two
        // apart within a millisecond.
        let stamped = line
            .stamp
            .is_some_and(|stamp| held.stamp.map(|h| h.ms) != Some(stamp.ms));
        let written = line
            .device
            .as_ref()
            .is_some_and(|device| *device != held.device);
        if line.stamp.is_some() && (stamped || written) {
            return Err(format!("{id} has changed since it was read"));
        }
        // A note moved since it was read keeps its stamp, since a move is not an edit, and is
        // told by its place's own clock; its new place stands, and what the line says of its text
        // still applies, as an edit and a move merge apart.
        let moved_since = line.placed.is_some_and(|placed| placed != held.placed);
        if !moved_since && line.after.is_some_and(|after| after != held.after) {
            return Err(format!("moving {id} is not done from outside yet"));
        }
        if held.deleted {
            return match line.deleted {
                Some(true) => Ok(None),
                _ => Err(format!("{id} was deleted")),
            };
        }
        if line.deleted == Some(true) {
            return Ok(Some(Record {
                deleted: true,
                ..held.clone()
            }));
        }
        let text = match line.text {
            // Compared as given first, so that a note read out and given back unchanged is not
            // changed by a rule applied since it was written.
            Some(text) if text != held.text => self.normalise(&text),
            _ => held.text.clone(),
        };
        if text.is_empty() {
            return Err(format!("{id} has no text; delete it instead"));
        }
        let archived = line.archived.unwrap_or(held.archived);
        let (text, archived) = boxed(&held.text, held.archived, text, archived)
            .ok_or_else(|| format!("{id}'s box and archived flag disagree"))?;
        let links = self.linked(id, line.links, &held.links)?;
        // Brackets are followed as saving follows them, when the text changes, so that a text
        // given as held leaves its links as given.
        let links = if text == held.text {
            links
        } else {
            self.engine.links_on_save(id, &held.text, &text, &links)
        };
        let version = Record {
            updated: if text == held.text {
                held.updated
            } else {
                Some(self.host.now())
            },
            text,
            links,
            archived,
            ..held.clone()
        };
        Ok((!version.says_the_same(held)).then_some(version))
    }

    /// A new note from a line with no id, at the top, written now. Its box decides whether it is
    /// archived.
    fn created_from(&self, n: usize, line: Given) -> Result<Record, String> {
        if line.deleted == Some(true) {
            return Err(format!("line {n} is a new note, and cannot be deleted"));
        }
        if line.after.is_some_and(|after| after.is_some()) {
            return Err(format!(
                "line {n} places a new note, which is not done from outside yet"
            ));
        }
        let text = self.normalise(line.text.as_deref().unwrap_or(""));
        if text.is_empty() {
            return Err(format!("line {n} is a new note with no text"));
        }
        let archived = match (text::task(&text), line.archived) {
            (Some(done), Some(archived)) if done != archived => {
                return Err(format!("line {n}'s box and archived flag disagree"));
            }
            (Some(done), _) => done,
            (None, archived) => archived.unwrap_or(false),
        };
        let id = self.host.new_id();
        let links = self.linked(&id, line.links, &[])?;
        let links = self.engine.links_on_save(&id, "", &text, &links);
        let now = self.host.now();
        Ok(Record {
            links,
            created: Some(now),
            updated: Some(now),
            archived,
            ..Record::new(id, text)
        })
    }

    /// The links given, or those held when none are. A link held already stays whatever has
    /// become of the note it names, since deleting a note leaves the links to it; one added has
    /// to name a note here.
    fn linked(
        &self,
        id: &str,
        given: Option<Vec<String>>,
        held: &[String],
    ) -> Result<Vec<String>, String> {
        let Some(given) = given else {
            return Ok(held.to_vec());
        };
        let mut links: Vec<String> = Vec::with_capacity(given.len());
        for link in given {
            if !held.contains(&link) && self.notes.get(&link).is_none() {
                return Err(format!("{id} links to {link}, which is no note here"));
            }
            if link != id && !links.contains(&link) {
                links.push(link);
            }
        }
        Ok(links)
    }
}

/// A task's box and its archived flag as they are to be written, given what is held and what
/// was asked for: the side that was changed wins and the other follows, as the key that ticks a
/// task keeps them. Both changed and disagreeing is none. Neither changed leaves them as they
/// were, even disagreeing, since putting that right is the key's. A note that is no task is
/// archived as asked.
fn boxed(held: &str, was: bool, text: String, archived: bool) -> Option<(String, bool)> {
    let Some(done) = text::task(&text) else {
        return Some((text, archived));
    };
    let ticked = text::task(held) != Some(done);
    let flagged = archived != was;
    match (ticked, flagged) {
        (true, true) if done != archived => None,
        (true, _) => Some((text, done)),
        (false, true) => Some((text::tick(&text, archived), archived)),
        (false, false) => Some((text, archived)),
    }
}
