//! The path: the places gone into, and what a note written in a place inherits from it. It is a
//! part of its own because the place is what every list and every new note is relative to, and
//! what it is, the last note on the path with the labels gone into after it, is best read in one
//! place.

use crate::query::{Label, Query};
use crate::record::Record;
use crate::text;
use crate::time::Day;

use super::Session;

/// A step of the path: what was gone into, a note or a label typed on the line, with the search
/// it was found with. A note written in a place inherits it, as a link to the note or as the label
/// at the end of its head. Labels gone into one after another are one place, with the last note
/// before them if there is one, so a note written there inherits each of them: the last note on
/// the path and the labels after it are the place looked around.
///
/// `search` is the search the step was found with, put back on the way out if the line is empty
/// by then. A draft carried in has nothing to put back: it is still on the line.
pub(super) enum Step {
    Note {
        id: String,
        head: String,
        search: Option<String>,
    },
    /// `written` is the label as it was typed, and as it is added to a note: `#house`, `@joe`,
    /// `2026-10-16`.
    Label {
        label: Label,
        written: String,
        search: Option<String>,
    },
}

impl Step {
    /// What the path calls it.
    pub(super) fn name(&self) -> &str {
        match self {
            Step::Note { head, .. } => head,
            Step::Label { written, .. } => written,
        }
    }

    /// The note it is, if it is one.
    pub(super) fn note(&self) -> Option<&String> {
        match self {
            Step::Note { id, .. } => Some(id),
            Step::Label { .. } => None,
        }
    }

    /// Whether it is the same place as another: the same note, or the same label in whatever
    /// case it was typed.
    fn is(&self, other: &Step) -> bool {
        match (self, other) {
            (Step::Note { id: a, .. }, Step::Note { id: b, .. }) => a == b,
            (Step::Label { label: a, .. }, Step::Label { label: b, .. }) => a == b,
            _ => false,
        }
    }

    /// The same step, found with this search.
    fn found_with(mut self, search: Option<String>) -> Step {
        match &mut self {
            Step::Note { search: s, .. } | Step::Label { search: s, .. } => *s = search,
        }
        self
    }

    /// The search it was found with, to put back on the way out.
    fn into_search(self) -> Option<String> {
        match self {
            Step::Note { search, .. } | Step::Label { search, .. } => search,
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

impl Session {
    /// Zooms on the note under the cursor, or from the line on what the line names: the list
    /// becomes what is in it. If the note contains what was typed, or holds what the line holds
    /// and is named by its words outside phrases, in its head and in the heads of the notes it
    /// links to, or by its letters, that was a search for it, so the line clears and is kept for
    /// the way back. Otherwise it was a draft, and it comes along to be written here. A place
    /// already on the path is gone back to rather than added again, so the path never repeats.
    pub(super) fn go_in(&mut self) {
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
            || self.engine.searched(&id, &Query::read(&self.line, today));
        let step = Step::Note {
            id,
            head: row.head.clone(),
            search: None,
        };
        if let Some(at) = self.path.iter().position(|s| s.is(&step)) {
            self.back_to(at + 1);
            return;
        }
        let search = searched.then(|| std::mem::take(&mut self.line));
        self.path.push(step.found_with(search));
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
        for step in places {
            match self.path.iter().position(|s| s.is(&step)) {
                Some(at) => self.path.truncate(at + 1),
                None => {
                    let search = (!went).then(|| std::mem::take(&mut self.line));
                    self.path.push(step.found_with(search));
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
    pub(super) fn places_named(&self) -> Vec<Step> {
        let query = Query::read(&self.line, self.today());
        let mut places: Vec<Step> = Vec::new();
        if query.only_labels() {
            for typed in query.labels {
                let place = Step::Label {
                    label: typed.label,
                    written: typed.written,
                    search: None,
                };
                if !places.iter().any(|p| p.is(&place)) {
                    places.push(place);
                }
            }
            if places.len() > 1 {
                let had: Vec<&Label> = self.labels_here().map(|(label, _)| label).collect();
                places.retain(
                    |place| !matches!(place, Step::Label { label, .. } if had.contains(&label)),
                );
                return places;
            }
        } else if let Some(note) = self.engine.the_note_named(&query, self.show_archived) {
            places.push(Step::Note {
                id: note.id.clone(),
                head: note.head().to_owned(),
                search: None,
            });
        }
        places.retain(|place| !self.path.last().is_some_and(|s| s.is(place)));
        places
    }

    /// The note being looked around: the last note on the path, if there is one. It is the
    /// place together with the labels gone into after it.
    pub(super) fn place_note(&self) -> Option<&String> {
        self.path.iter().rev().find_map(Step::note)
    }

    /// The labels being looked around, as they were typed, outermost first: those gone into one
    /// after another since the last note on the path, or since its start. They are one place
    /// with that note. Labels before it are only how it was reached.
    pub(super) fn labels_here(&self) -> impl Iterator<Item = (&Label, &str)> {
        let from = self
            .path
            .iter()
            .rposition(|s| s.note().is_some())
            .map_or(0, |at| at + 1);
        self.path[from..].iter().filter_map(|s| match s {
            Step::Label { label, written, .. } => Some((label, written.as_str())),
            Step::Note { .. } => None,
        })
    }

    /// Steps back out to `depth` places on the path, onto the line, so that what is asked next is
    /// asked of the line and not of a note. What is on the line stays as it is; only an empty
    /// line gets back the search that the first place left behind was found with, which is what
    /// the line held at that depth.
    pub(super) fn back_to(&mut self, depth: usize) {
        if depth >= self.path.len() {
            return;
        }
        let left = self.path.split_off(depth);
        if let Some(line) = left.into_iter().next().and_then(Step::into_search)
            && self.line.trim().is_empty()
        {
            self.line = line;
        }
        self.refresh(true);
    }

    /// A text with each label being looked around that it does not carry already written into
    /// its head, as it would have been typed: the box at the start and the rest at the end, in the
    /// order of the path. A day is carried by a note written on it, since that is its `created`.
    pub(super) fn inherit(&self, text: String, written_on: Day) -> String {
        self.labels_here().fold(text, |text, (label, written)| {
            if label.carried_by(&text, written_on) {
                text
            } else {
                with_label(&text, label, written)
            }
        })
    }

    /// Links the note under the cursor to the one being looked around, or takes that link away.
    /// It is a correction: what is written in a zoom is linked already, and outside one, or in
    /// labels with no note before them, there is nothing to link to.
    pub(super) fn link(&mut self) {
        let Some(around) = self.place_note().cloned() else {
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
        let (Some(note), Some(place)) = (
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
        } else if place.links.contains(&id) {
            self.status = format!("unlinked {head}{}", named(&place.text, &id));
            self.commit(Record {
                links: without(&place.links, &id),
                ..place
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
