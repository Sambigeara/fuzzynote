//! The path: the places gone into, and what a note written at its end inherits from it. It is a
//! part of its own because the path is what every list and every new note is relative to, and it
//! is read two ways that are best kept side by side: a note written there inherits all of it, and
//! the place it lists is only the last note on it with the labels gone into after it.

use crate::engine;
use crate::note::Note;
use crate::query::{Label, Query};
use crate::record::Record;
use crate::text;
use crate::time::{Day, Moment};

use super::{KeptStep, Session};

/// A step of the path: what was gone into, a note or a label typed on the line, with the search
/// it was found with. A note written at the end of the path inherits every label on it, at the end
/// of its head or the box at its start, and links to the last note on it. Labels gone into one
/// after another are one place, with the last note before them if there is one: the last note on
/// the path and the labels after it are the place looked around.
///
/// `search` is the search the step was found with, put back on the way out if the line is empty
/// by then. A label gone into from a line of several was found with itself as it is written, as
/// if typed alone, so that a path steps back the same however it was made. A draft carried in has
/// nothing to put back: it is still on the line.
pub(super) enum Step {
    /// `from` is the note whose row the cursor was on when it was gone into, for the cursor to
    /// come back to on the way out.
    Note {
        id: String,
        head: String,
        search: Option<String>,
        from: Option<String>,
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

    /// The same step, found with this search unless it was found with one of its own.
    fn found_with_or(mut self, search: String) -> Step {
        match &mut self {
            Step::Note { search: s, .. } | Step::Label { search: s, .. } => {
                s.get_or_insert(search);
            }
        }
        self
    }

    /// The step as a face keeps it.
    pub(super) fn kept(&self) -> KeptStep {
        match self {
            Step::Note { id, search, .. } => KeptStep::Note {
                note: id.clone(),
                search: search.clone(),
            },
            Step::Label {
                written, search, ..
            } => KeptStep::Label {
                label: written.clone(),
                search: search.clone(),
            },
        }
    }

    /// The search it was found with, to put back on the way out, and the note whose row the
    /// cursor goes back to.
    fn into_return(self) -> (Option<String>, Option<String>) {
        match self {
            Step::Note { search, from, .. } => (search, from),
            Step::Label { search, .. } => (search, None),
        }
    }
}

/// Whether a word of a head, read as a label, is this one of the path's: `#house` or `#House`
/// for the tag, `@joe` for the person, `2026-10-16` for the day. The box is never at the end of a
/// head.
fn is_label(word: &text::Labelled, label: &Label) -> bool {
    match (word, label) {
        (text::Labelled::Tag(tag), Label::Tag(of)) => tag == of,
        (text::Labelled::Mention(person), Label::Mention(of)) => person == of,
        (text::Labelled::Day(day), Label::Day(of)) => day == of,
        _ => false,
    }
}

/// A head as a row that is not under the cursor shows it on a path whose labels are these:
/// with them taken off its end, in any order and any case, when they end it, whether the note
/// has a body or not. The path already says them. The box at the start stays, since it says
/// what the row is; and a head of nothing but the labels, or the box and the labels, shows as it
/// is, since a row of nothing says less than a row of its label, as does a heading mark with
/// nothing after it. None when nothing is taken off.
pub(super) fn head_shown<'a>(
    head: &str,
    labels: impl Iterator<Item = &'a Label> + Clone,
) -> Option<String> {
    head_without(head, |word| labels.clone().any(|l| is_label(word, l)))
}

/// A head with the labels that end it taken off while `said` says each is said already, by the
/// rules of [`head_shown`].
fn head_without(head: &str, said: impl Fn(&text::Labelled) -> bool) -> Option<String> {
    let rest = text::without_end(head, |word| {
        text::label_word(word).is_some_and(|w| said(&w))
    });
    if rest.len() == head.trim_end().len() {
        return None;
    }
    let bare = match text::task(rest) {
        Some(_) => rest[text::OPEN.len()..].trim(),
        None => rest,
    };
    // A heading mark with nothing after it is nothing too, since it reads as an empty heading.
    let bare = unmarked(bare);
    let marks_only = bare.chars().all(|c| c == '#');
    (!marks_only).then(|| rest.to_owned())
}

/// A head with the heading mark it starts with taken off, `# Boiler` read as `Boiler`, as a
/// rendered heading reads.
fn unmarked(head: &str) -> &str {
    let marks = head.len() - head.trim_start_matches('#').len();
    match head[marks..].strip_prefix(' ') {
        Some(rest) if (1..=6).contains(&marks) && !rest.trim().is_empty() => rest.trim_start(),
        _ => head,
    }
}

/// A text with a label written into its head, as it would have been typed there: the box at its
/// start, and anything else at its end.
fn with_label(text: &str, label: &Label, written: &str) -> String {
    match label {
        Label::Open => return text::tick(text, false),
        Label::Done => return text::tick(text, true),
        _ => {}
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
    /// Goes into the note under the cursor, or from the line into what the line names: the list
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
        let typed = text::expand_words(self.line.trim(), today);
        let searched = typed.is_empty()
            || self.engine.contains(&id, &typed)
            || self.engine.searched(&id, &self.parsed);
        let head = self.step_name(&id).unwrap_or_else(|| row.head.clone());
        let from = Some(id.clone());
        self.go_into(id, head, searched, from);
    }

    /// Goes into the one note a name in brackets names, as Tab on its row would, the line kept
    /// for the way back: following a link is picking its target.
    pub(super) fn follow_link(&mut self, name: &str) {
        let Ok(id) = self.named_one(name) else {
            return;
        };
        let from = self.selected().map(|row| row.id.clone());
        if let Some(head) = self.step_name(&id) {
            self.go_into(id, head, true, from);
        }
    }

    /// The one note a name in brackets names, or why it names none to follow: it names no note,
    /// or several.
    pub(super) fn named_one(&self, name: &str) -> Result<String, String> {
        match &self.engine.named_in(&format!("[[{name}]]"))[..] {
            [id] => Ok(id.clone()),
            [] => Err(format!("no note is called {name}")),
            named => Err(format!("{} notes are called {name}", named.len())),
        }
    }

    fn go_into(&mut self, id: String, head: String, searched: bool, from: Option<String>) {
        let step = Step::Note {
            id,
            head,
            search: None,
            from,
        };
        if let Some(at) = self.path.iter().position(|s| s.is(&step)) {
            self.back_to(at + 1);
            return;
        }
        let search = searched.then(|| std::mem::take(&mut self.line));
        self.path.push(step.found_with(search));
        self.refresh(true);
    }

    /// Goes into what the line names, one place after another, as if each had been typed and Tab
    /// pressed after it: a place already on the path is gone back to, and any other is gone into.
    /// The line was a search for where it leads, so it clears, and comes back on the way out of a
    /// place it names alone; a label of several comes back as it is written, as `places_named`
    /// gives it. A line that only went back stays as it is.
    pub(super) fn go_in_from_line(&mut self) {
        let places = self.places_named();
        if places.is_empty() {
            return;
        }
        for step in places {
            match self.path.iter().position(|s| s.is(&step)) {
                Some(at) => self.path.truncate(at + 1),
                None => {
                    let line = std::mem::take(&mut self.line);
                    self.path.push(step.found_with_or(line));
                }
            }
        }
        self.refresh(true);
    }

    /// What the line names to go into: each label of a line of labels and nothing else, in the
    /// order typed, or else the one note in the space the line is the name of and that holds what
    /// the line holds, by [`Session::named_here`]. That note is first on the list in a place as at
    /// the top, saying `its name`, so this is where Down and then Tab would go too. One place is
    /// named unless it is where you are, so that one on the path is gone back to; of several
    /// labels, those the place has already are passed over, so that the rest are added to it, and
    /// each is found with itself as it is written, as if typed alone, so that a path made in one
    /// Tab steps back as one made a label at a time does. A label typed twice is one place.
    pub(super) fn places_named(&self) -> Vec<Step> {
        let query = &self.parsed;
        let mut places: Vec<Step> = Vec::new();
        // A line of both boxes names nowhere to go: with archived hidden no note is both, and
        // with it shown the line is `[x]` with a box too many.
        let both = query.asks_done() && query.labels.iter().any(|t| t.label == Label::Open);
        if both {
            return places;
        }
        if query.only_labels() {
            for typed in &query.labels {
                let place = Step::Label {
                    label: typed.label.clone(),
                    written: typed.written.clone(),
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
                return places
                    .into_iter()
                    .map(|place| {
                        let written = place.name().to_owned();
                        place.found_with(Some(written))
                    })
                    .collect();
            }
        } else if let [note] = self.named_here(query)[..] {
            places.push(Step::Note {
                id: note.id.clone(),
                head: self.step_name(&note.id).unwrap_or_default(),
                search: None,
                from: None,
            });
        }
        places.retain(|place| !self.path.last().is_some_and(|s| s.is(place)));
        places
    }

    /// The notes the line names here, by the one rule the list and going in share, so that a
    /// row saying `its name` is always where Tab goes. A note named only without the labels
    /// that end it is named here if its row reads so here: if it carries the path's labels.
    pub(super) fn named_here(&self, query: &Query) -> Vec<&Note> {
        let archived = self.archived_listed(query);
        let (mut named, called) = self.engine.notes_named(query, archived);
        if called {
            named.retain(|note| {
                let day = note.written().unwrap_or(Day::MIN);
                self.path_labels()
                    .all(|(label, _)| label.carried_by(&note.text, day, query.every_task))
            });
        }
        named
    }

    /// A head as a row that is not under the cursor shows it here, if that differs from the
    /// head: with the path's labels taken off its end, by [`head_shown`].
    pub(super) fn shown(&self, head: &str) -> Option<String> {
        head_shown(head, self.path_labels().map(|(label, _)| label))
    }

    /// A head as a row here reads it, which is how anything here names a note: its arrow, a
    /// reason, a status.
    pub(super) fn row_head(&self, head: &str) -> String {
        self.shown(head).unwrap_or_else(|| head.to_owned())
    }

    /// The head of a note a row links to as the row's arrow reads it: as a row here reads it,
    /// and without the labels that end the row's own head too, since the row says them.
    pub(super) fn arrow(&self, own: &str, target: &str) -> String {
        // The labels that end the row's own head, gathered as they are taken off it: what is
        // left of the head is not wanted, only what was taken.
        let mut ends = Vec::new();
        text::without_end(own, |word| match text::label_word(word) {
            Some(label) => {
                ends.push(label);
                true
            }
            None => false,
        });
        let path: Vec<&Label> = self.path_labels().map(|(label, _)| label).collect();
        head_without(target, |word| {
            ends.contains(word) || path.iter().any(|&label| is_label(word, label))
        })
        .unwrap_or_else(|| target.to_owned())
    }

    /// Goes back to where a face kept you: each step from the top, as going in from the line
    /// would, passing over a note that has gone, a label that is not one, and a place already on
    /// the path. The line clears, and the cursor is on it.
    pub(super) fn restore_path(&mut self, steps: Vec<KeptStep>) {
        self.path.clear();
        self.line.clear();
        let today = self.today();
        for kept in steps {
            let step = match kept {
                KeptStep::Note { note, search } => {
                    let Some(head) = self.step_name(&note) else {
                        continue;
                    };
                    let id = note;
                    Step::Note {
                        id,
                        head,
                        search,
                        from: None,
                    }
                }
                KeptStep::Label { label, search } => {
                    let query = Query::read(&label, today);
                    let ([typed], true) = (&query.labels[..], query.only_labels()) else {
                        continue;
                    };
                    Step::Label {
                        label: typed.label.clone(),
                        written: typed.written.clone(),
                        search,
                    }
                }
            };
            if !self.path.iter().any(|s| s.is(&step)) {
                self.path.push(step);
            }
        }
        self.refresh(true);
    }

    /// What the path calls a note gone into from here: its head as its row reads where it was
    /// gone into from, its heading mark and the path's labels at its end taken off.
    pub(super) fn step_name(&self, id: &str) -> Option<String> {
        let record = self.notes.get(id)?;
        let head = unmarked(record.head());
        let labels = self.path_labels().map(|(label, _)| label);
        Some(head_shown(head, labels).unwrap_or_else(|| head.to_owned()))
    }

    /// The note being looked around: the last note on the path, if there is one. It is the
    /// place together with the labels gone into after it.
    pub(super) fn place_note(&self) -> Option<&String> {
        self.path.iter().rev().find_map(Step::note)
    }

    /// The labels the place narrows by, as they were typed, outermost first: those gone into one
    /// after another since the last note on the path, or since its start. They are one place
    /// with that note. Labels before it narrow nothing, so a note lists the same however it was
    /// reached.
    pub(super) fn labels_here(&self) -> impl Iterator<Item = (&Label, &str)> + Clone {
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

    /// The labels a note written here carries and a row here hides, as they were typed: every
    /// label on the path, outermost first, before its last note and after. Of two boxes on it, the
    /// one nearer the end is the path's, since a note has one box.
    pub(super) fn path_labels(&self) -> impl Iterator<Item = (&Label, &str)> + Clone {
        let boxed = |s: &Step| {
            matches!(
                s,
                Step::Label {
                    label: Label::Open | Label::Done,
                    ..
                }
            )
        };
        let last_box = self.path.iter().rposition(boxed);
        self.path
            .iter()
            .enumerate()
            .filter_map(move |(at, s)| match s {
                Step::Label { .. } if boxed(s) && Some(at) != last_box => None,
                Step::Label { label, written, .. } => Some((label, written.as_str())),
                Step::Note { .. } => None,
            })
    }

    /// Steps back out to `depth` places on the path, onto the line, so that what is asked next is
    /// asked of the line and not of a note.
    /// Steps back to `depth` places, the cursor going back to the row it was on when the first
    /// place left behind was gone into, if that note is still listed, and otherwise onto the line.
    pub(super) fn back_to(&mut self, depth: usize) {
        if depth < self.path.len() {
            let from = self.cut_path(depth);
            self.refresh(true);
            self.cursor = from.and_then(|id| self.rows.iter().position(|r| r.id == id));
        }
    }

    /// Cuts the path to `depth` places. What is on the line stays as it is; only an empty line
    /// gets back the search that the first place left behind was found with, which is what the
    /// line held at that depth. A line that still reads as the last step out left it is as good
    /// as empty, since nothing of it is the writer's: so two steps out of a path made in one Tab
    /// end with the outer search, as two out of one made a step at a time do. Says the note whose
    /// row the cursor was on when that place was gone into, if it was gone into from a row and the
    /// line is not the writer's, whose next letter is for the line.
    pub(super) fn cut_path(&mut self, depth: usize) -> Option<String> {
        let left = self.path.split_off(depth);
        let untouched = self.line.trim().is_empty() || self.returned == self.line;
        let (search, from) = left.into_iter().next().map(Step::into_return)?;
        if let Some(search) = search
            && untouched
        {
            self.returned.clone_from(&search);
            self.line = search;
        }
        from.filter(|_| untouched)
    }

    /// How deep the path goes before the first note on it that has gone, deleted here or on
    /// another device, if one has.
    pub(super) fn gone_from(&self) -> Option<usize> {
        self.path
            .iter()
            .position(|s| s.note().is_some_and(|id| self.notes.get(id).is_none()))
    }

    /// A text with each label on the path that it does not carry already written into its head,
    /// as it would have been typed: the box at the start and the rest at the end, in the order of
    /// the path. A day is carried by a note written on it, since that is its `created`.
    /// A box the writer typed is theirs, whichever box the path has, so a text headed by one is
    /// given neither; a text with none carries no box, whatever is shown.
    pub(super) fn inherit(&self, text: String, written_on: Day) -> String {
        self.path_labels().fold(text, |text, (label, written)| {
            let boxed = matches!(label, Label::Open | Label::Done) && text::task(&text).is_some();
            if boxed || label.carried_by(&text, written_on, false) {
                text
            } else {
                with_label(&text, label, written)
            }
        })
    }

    /// Links the note under the cursor to the one being looked around, or takes that link away,
    /// and with it all that is under the note. It is a correction: what is written in a note's
    /// place is linked already, and outside one, or in labels with no note before them, there is
    /// nothing to link to. A note under the place through another note is in it already, and is
    /// unlinked from that one, so nothing is written for it here.
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
        let head = text::fit(&self.row_head(&row.head), 40);
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
        } else if let Some(above) = self.under_through(&id) {
            self.status = format!("it is under {above}; unlink it there");
            return;
        } else {
            let mut links = note.links.clone();
            links.push(around);
            self.commit(Record { links, ..note });
            self.status = format!("linked {head}");
        }
        self.refresh(false);
        self.reselect(&id);
        // Unlinked from the place's note, a note still under it through another stays listed,
        // and says why.
        if let Some(above) = self.under_through(&id) {
            self.status.push_str(&format!(" · still under {above}"));
        }
    }

    /// What a note is under the place through, named as its row's arrow names it, if it is in the
    /// place now and not linked to the place's note either way: the first note it links to that
    /// is under the place itself. It is asked of the notes as they are now and not of the note's
    /// row, which holds still with the list, so that the foot and the link chord say what a list
    /// made now would.
    pub(super) fn under_through(&self, id: &str) -> Option<String> {
        let around = self.place_note()?;
        let note = self.notes.get(id)?;
        let links = |from: &str, to: &str| {
            self.notes
                .get(from)
                .is_some_and(|n| n.links.iter().any(|l| l == to))
        };
        if id == around || links(id, around) || links(around, id) {
            return None;
        }
        // One walk, whatever the notes carry: it answers whether this note is under the place
        // and which of what it links to is too. The place's labels are then asked of this note
        // alone, as the list narrows by them.
        let place = engine::Place {
            note: Some(around),
            labels: &[],
            written: "",
        };
        let under = self.engine.facts(&place, &Query::default());
        let day = note.created.map_or(Day::MIN, Moment::day);
        let narrowed = self
            .labels_here()
            .all(|(label, _)| label.carried_by(&note.text, day, self.show_archived));
        if !narrowed || !under.iter().any(|(fact, _)| fact.id == id) {
            return None;
        }
        // A note the place's note only links to is not one it is under.
        let above = note.links.iter().find(|&target| {
            under
                .iter()
                .any(|(fact, how)| &fact.id == target && *how != Some("linked from here"))
        })?;
        let above = self.notes.get(above)?;
        Some(text::fit(&self.arrow(note.head(), above.head()), 40))
    }

    /// The labels the place narrows by, as the index holds them, the open box as every task if
    /// `every_task`.
    pub(super) fn narrowing(&self, every_task: bool) -> Vec<crate::score::Label> {
        self.labels_here()
            .map(|(label, _)| label.indexed(every_task))
            .collect()
    }
}
