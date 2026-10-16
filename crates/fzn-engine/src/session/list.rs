//! The list: what is shown under the line, and the cursor's walk over it. It is a part of its own
//! because making the list is where a keystroke spends its time, and it reads the notes and the
//! engine without changing a note: the rest of the session changes things and asks for the list
//! to be made again.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use crate::engine::{self, Hit, SearchOptions};
use crate::query::Query;
use crate::record::Record;
use crate::score::Scorer;
use crate::text;

use super::path::Step;
use super::{FOUND_LIMIT, PLACE_LIMIT, Row, Session, TOP_LIMIT};

impl Session {
    /// Recomputes the list from the line and the path. `reset` moves the cursor back to the
    /// line.
    pub(super) fn refresh(&mut self, reset: bool) {
        let query = Query::read(&self.line, self.today());
        (self.rows, self.finds_from) = match (self.path.is_empty(), query.ranks()) {
            (false, _) => self.in_place(&query),
            (true, true) => (self.found(&query, self.show_archived, FOUND_LIMIT), None),
            (true, false) => (self.found(&query, self.show_archived, TOP_LIMIT), None),
        };
        self.parsed = query;
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
        let has_body = text::has_body(&r.text);
        Row {
            id: r.id.clone(),
            head: r.head().to_owned(),
            shown: self.shown(r.head(), has_body),
            task: text::task(&r.text),
            archived: r.archived,
            has_body,
            linked: false,
            links: self.link_heads(r),
            reasons: Vec::new(),
            score: None,
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
        // There are few hits and many notes, so each note in the order is looked for among the
        // hits, and by where it is held rather than by its id, which would be a read of every id.
        let mut rank: Vec<(*const Record, usize)> = hits
            .iter()
            .filter_map(|hit| Some((self.notes.get(&hit.note.id)? as *const Record, usize::MAX)))
            .collect();
        rank.sort_unstable();
        for (at, record) in self.notes.order().into_iter().enumerate() {
            let held = record as *const Record;
            if let Ok(i) = rank.binary_search_by_key(&held, |&(r, _)| r) {
                rank[i].1 = at;
            }
        }
        hits.sort_by_cached_key(|hit| {
            let at = self.notes.get(&hit.note.id).and_then(|r| {
                let i = rank.binary_search_by_key(&(r as *const Record), |&(r, _)| r);
                i.ok().map(|i| rank[i].1)
            });
            (
                Reverse(hit.score as i32),
                hit.note.head().chars().count(),
                at.unwrap_or(usize::MAX),
            )
        });
        hits
    }

    /// Every note the line holds at the top of the space, archived ones too, in the order the
    /// line lists them and however many there are: what `fzn export` writes for a query.
    pub fn query(&self, line: &str) -> Vec<&Record> {
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
    /// engine finds there that matches, all of them holding what the line holds. With it comes
    /// the first of what is only found there, if a fact comes before it, where a face sets the
    /// two apart.
    fn in_place(&self, query: &Query) -> (Vec<Row>, Option<String>) {
        let note = self.place_note().map(String::as_str);
        let (labels, written): (Vec<_>, Vec<_>) = self
            .labels_here()
            .map(|(label, written)| (label.indexed(), written))
            .unzip();
        let written = written.join(" ");
        let place = engine::Place {
            note,
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
            let rows = self
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
            return (rows, None);
        }
        let opts = SearchOptions {
            limit: PLACE_LIMIT,
            include_archived: self.show_archived,
            today: Some(self.today()),
        };
        let near = self.engine.neighbourhood(&place, &opts);
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
        // The engine names by the whole space. Here a note is named only as going in names it,
        // so one named without the labels that end it and not carrying the place's is only found.
        let named: HashSet<&str> = self
            .named_here(query)
            .into_iter()
            .map(|note| note.id.as_str())
            .collect();
        for hit in &mut found {
            if hit.named && !named.contains(hit.note.id.as_str()) {
                hit.named = false;
                hit.reasons.retain(|reason| reason.scorer != Scorer::Name);
            }
        }
        let fact = |hit: &Hit| linked.contains(hit.note.id.as_str());
        let here = self.path.last().and_then(Step::note);
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
        let first = found.iter().position(|&hit| !hit.named && !fact(hit));
        let finds_from = first
            .filter(|&at| found[..at].iter().any(|&hit| fact(hit)))
            .map(|at| found[at].note.id.clone());
        let rows = found
            .into_iter()
            .map(|hit| Row {
                linked: fact(hit),
                ..self.row(hit)
            })
            .collect();
        (rows, finds_from)
    }

    /// The heads, as they are now, of the notes a record is linked to by id. The note being
    /// looked around is left out, since everything on the list is about it.
    pub(super) fn link_heads(&self, record: &Record) -> Vec<String> {
        let around = self.place_note();
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
        let has_body = text::has_body(&hit.note.text);
        Row {
            id: hit.note.id.clone(),
            head: hit.note.head().to_owned(),
            shown: self.shown(hit.note.head(), has_body),
            task: text::task(&hit.note.text),
            archived: hit.note.archived,
            has_body,
            linked: hit.linked,
            links: record.map(|r| self.link_heads(r)).unwrap_or_default(),
            reasons: hit
                .reasons
                .iter()
                .map(|r| match r.scorer {
                    // A link is a fact and reads as one, with no scorer to credit. So is a name.
                    Scorer::Link | Scorer::Name => r.text.clone(),
                    scorer => format!("{scorer}: {}", r.text),
                })
                .collect(),
            score: Some(hit.score),
        }
    }

    /// Moves the cursor between the line and the list.
    pub(super) fn step(&mut self, delta: i64) {
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
    pub(super) fn onto_line(&mut self) {
        self.cursor = None;
        if self.stale {
            self.refresh(true);
        }
    }

    pub(super) fn selected(&self) -> Option<&Row> {
        self.cursor.and_then(|i| self.rows.get(i))
    }

    /// The note under the cursor, as it is held.
    pub(super) fn selected_record(&self) -> Option<&Record> {
        self.selected().and_then(|r| self.notes.get(&r.id))
    }

    pub(super) fn reselect(&mut self, id: &str) {
        if let Some(i) = self.rows.iter().position(|r| r.id == id) {
            self.cursor = Some(i);
        }
    }

    /// How many rows at the top of the list are in the order the space keeps its notes in, and
    /// so can be moved among: all of them with nothing on the line to rank by, at the top as in
    /// a place, and none of what the line finds, which is in the order of its scores.
    pub(super) fn in_order(&self) -> usize {
        if self.parsed.ranks() {
            0
        } else {
            self.rows.len()
        }
    }
}
