//! The list: what is shown under the line, and the cursor's walk over it. It is a part of its own
//! because making the list is where a keystroke spends its time, and it reads the notes and the
//! engine without changing a note: the rest of the session changes things and asks for the list
//! to be made again.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use crate::engine::{self, Hit, SearchOptions};
use crate::query::{Label, Query};
use crate::record::{Record, Walk};
use crate::score::Scorer;
use crate::text;

use super::path::Step;
use super::{DEEPEST, FOUND_LIMIT, PLACE_LIMIT, Row, Session, TOP_LIMIT};

impl Session {
    /// Recomputes the list from the line and the path. `reset` moves the cursor back to the
    /// line. A note on the path that has gone, by whatever route, takes the path back to just
    /// before it, as stepping back would, and the cursor to the line: there is nothing left to be
    /// in.
    pub(super) fn refresh(&mut self, reset: bool) {
        let reset = match self.gone_from() {
            Some(depth) => {
                self.cut_path(depth);
                true
            }
            None => reset,
        };
        let (query, rows, finds_from) = self.made();
        (self.rows, self.finds_from, self.parsed) = (rows, finds_from, query);
        self.stale = false;
        self.going = None;
        let cursor = if reset { None } else { self.cursor };
        self.cursor = cursor
            .filter(|_| !self.rows.is_empty())
            .map(|i| i.min(self.rows.len() - 1));
    }

    /// The list as it would be made now from the line and the path: the line as read, the rows,
    /// and the first row that only the line finds in a place.
    fn made(&self) -> (Query, Vec<Row>, Option<String>) {
        let query = self.read_line();
        let archived = self.archived_listed(&query);
        let (rows, finds_from) = match (self.path.is_empty(), query.ranks()) {
            (false, _) => self.in_place(&query),
            (true, true) => (self.found(&query, archived, FOUND_LIMIT), None),
            (true, false) => (self.found(&query, archived, TOP_LIMIT), None),
        };
        (query, rows, finds_from)
    }

    /// The line as it is read here: with the done tasks on the path a span on it measures when
    /// notes were done, as it does with `[x]` on the line, since what is written there is done,
    /// and while archived notes are shown the open box holds every task. Only the toggle does
    /// that, since `[x]` asks for the done tasks and not for `[ ]` to change.
    pub(super) fn read_line(&self) -> Query {
        let mut query = Query::read(&self.line, self.today());
        query.every_task = self.show_archived;
        if self.done_here() {
            query.measure_done();
        }
        query
    }

    /// Whether archived notes are listed: when they are shown, and when the line or the path
    /// asks for the done tasks, which are archived.
    pub(super) fn archived_listed(&self, query: &Query) -> bool {
        self.show_archived || query.asks_done() || self.done_here()
    }

    /// Whether the path's box is the done tasks', anywhere on it: what is written here is done,
    /// so what is done is listed, though only the labels after the last note narrow.
    fn done_here(&self) -> bool {
        self.path_labels().any(|(label, _)| *label == Label::Done)
    }

    /// The notes to list, in the order the space keeps them in, the archived ones too if asked.
    fn kept(&self, archived: bool) -> impl Iterator<Item = &Record> {
        self.notes
            .order()
            .into_iter()
            .filter(move |r| kept(r, archived))
    }

    /// A note as the empty line lists it, with nothing to say why.
    pub(super) fn listed(&self, r: &Record) -> Row {
        self.drawn((r, 0, None, Vec::new()))
    }

    /// A note as a list in the space's order draws it: `level` deep, under the listed note
    /// `under`, which its arrow then leaves out, since the drawing says it, and under it
    /// `through` the notes not listed it is walked through. Past the deepest a row is drawn, the
    /// drawing no longer says it, and the arrow does.
    fn drawn(&self, (r, level, under, through): Drawn) -> Row {
        let under = under.map(|u| u.id.as_str());
        let has_body = text::has_body(&r.text);
        Row {
            id: r.id.clone(),
            head: r.head().to_owned(),
            shown: self.shown(r.head()),
            task: text::task(&r.text),
            archived: r.archived,
            has_body,
            linked: false,
            links: self.link_heads_besides(r, unsaid(under, level)),
            reasons: Vec::new(),
            score: None,
            depth: level.min(DEEPEST),
            level,
            under: under.map(str::to_owned),
            through: through.iter().map(|t| t.id.clone()).collect(),
        }
    }

    /// The notes of a list in the space's order as it draws them, at most `limit`, each with how
    /// deep it is and the listed note it is under. `listed` says which notes the list has, and
    /// `root` is the note you are in, which is the top. A note is under the first note it links
    /// to that is listed, and failing one, under the nearest listed note above the first note it
    /// links to, walked through what is not listed; with nothing listed above it, or the note you
    /// are in, it is at the top. The top is in the space's order, and each note is followed by
    /// what is under it, at any depth, the notes under one note in the space's order too. A ring
    /// of notes each under the next, which nothing at the top reaches, is drawn from the note of
    /// it that comes first, where that one comes.
    ///
    /// A list that is cut, as the top is, reads only as many notes as it draws, since drawing the
    /// whole space to keep the first few hundred rows costs ten times what they do: the notes
    /// under a note are found by what links to it, and put in order by where each is in the
    /// walk, which is kept beside it. A list that is not cut, a place's or a command's, is drawn
    /// whole in one pass over it, which costs less than a search from each of its notes.
    pub(super) fn tree<'a>(
        &'a self,
        listed: &dyn Fn(&Record) -> bool,
        root: Option<&str>,
        limit: usize,
    ) -> Vec<Drawn<'a>> {
        let walk = self.notes.walk();
        let lists = Lists {
            session: self,
            listed,
            root,
        };
        if limit == usize::MAX {
            let held = walk.records().enumerate().filter(|(_, r)| lists.has(r));
            return lists.whole(&walk, held.collect());
        }
        let mut out = Vec::new();
        let mut drawn: HashSet<*const Record> = HashSet::new();
        // The notes found to be under something at the top, so that the notes under one are not
        // each walked up past it again.
        let mut topped: HashSet<*const Record> = HashSet::new();
        let mut stack = Vec::new();
        for r in walk.records() {
            if out.len() >= limit {
                break;
            }
            if !lists.has(r) || drawn.contains(&std::ptr::from_ref(r)) {
                continue;
            }
            if let Some(above) = lists.above(r)
                && !lists.heads_ring(r, above, &walk, &mut topped)
            {
                continue;
            }
            stack.push((r, 0, None, Vec::new()));
            while let Some((r, level, under, through)) = stack.pop() {
                if out.len() >= limit {
                    break;
                }
                if !drawn.insert(std::ptr::from_ref(r)) {
                    continue;
                }
                out.push((r, level, under, through));
                let below = lists.below(r, &walk);
                let below = below.into_iter().rev();
                stack.extend(below.map(|(c, through)| (c, level + 1, Some(r), through)));
            }
            stack.clear();
        }
        out
    }

    /// The notes that hold what the line holds, at most `limit`, in the order the line lists them
    /// when there is nothing on it to rank by: the space's order, or on a line holding a span the
    /// order of the moment the span reads, oldest first, notes of the same moment in the space's
    /// order, or with a `-` before the span exactly that list turned over, newest first. A list
    /// of time is cut from its old end either way, so that it still reaches the span's end.
    fn ordered<'a>(
        &'a self,
        query: &Query,
        archived: bool,
        holds: impl Fn(&Record) -> bool,
        limit: usize,
    ) -> Vec<&'a Record> {
        let held = self.kept(archived).filter(|r| holds(r));
        let Some(timed) = query.timed() else {
            return held.take(limit).collect();
        };
        let mut held: Vec<&Record> = held.collect();
        held.sort_by_cached_key(|r| match timed.done {
            true => r.finished().map(|at| at.ms),
            false => r.created.map(|at| at.ms),
        });
        held.drain(..held.len().saturating_sub(limit));
        if timed.newest {
            held.reverse();
        }
        held
    }

    /// What the line lists at the top of the space, at most `limit`. With nothing to rank by it
    /// is the notes that hold what the line holds, with nothing to say why, as a place lists what
    /// is in it: in the order the space keeps them in, drawn as a tree, which on an empty line is
    /// the top of the space, or on a line of time flat in time order. Otherwise it is what the
    /// line finds among them.
    fn found(&self, query: &Query, archived: bool, limit: usize) -> Vec<Row> {
        if !query.ranks() {
            let held: Option<HashSet<&str>> = (!query.is_empty()).then(|| {
                self.engine
                    .holding(query)
                    .into_iter()
                    .map(|note| note.id.as_str())
                    .collect()
            });
            let holds = |r: &Record| {
                held.as_ref()
                    .is_none_or(|held| held.contains(r.id.as_str()))
            };
            if query.timed().is_some() {
                return self
                    .ordered(query, archived, holds, limit)
                    .into_iter()
                    .map(|r| self.listed(r))
                    .collect();
            }
            let listed = |r: &Record| kept(r, archived) && holds(r);
            return self
                .tree(&listed, None, limit)
                .into_iter()
                .map(|drawn| self.drawn(drawn))
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
    /// line lists them and however many there are: what `fzn query` writes.
    pub fn query(&self, line: &str) -> Vec<&Record> {
        let mut query = Query::read(line, self.today());
        // A command's line is finished, so a `!` form at its end leaves out what it names as it
        // would with a space after it. Only that form can change, being the last on the line.
        if query.leaving {
            query = Query::read(&format!("{line} "), self.today());
        }
        self.found(&query, true, usize::MAX)
            .iter()
            .filter_map(|row| self.notes.get(&row.id))
            .collect()
    }

    /// The place being looked around. With nothing on the line to rank by it is what is in it
    /// and holds what the line holds, its facts in the order the space keeps them in, drawn as a
    /// tree from its note, and nothing the engine suggests, as the top of the space is: what is
    /// under its note at any depth or linked from it, and carries all of its labels. With
    /// something to rank by it is what the line finds in its neighbourhood: the note the line
    /// names, then the facts that match, then what else the engine finds there that matches, all
    /// of them holding what the line holds. With it comes the first of what is only found there,
    /// if a fact comes before it, where a face sets the two apart.
    fn in_place(&self, query: &Query) -> (Vec<Row>, Option<String>) {
        let note = self.place_note().map(String::as_str);
        let labels = self.narrowing(query.every_task);
        let written: Vec<&str> = self.labels_here().map(|(_, written)| written).collect();
        let written = written.join(" ");
        let place = engine::Place {
            note,
            labels: &labels,
            written: &written,
        };
        let archived = self.archived_listed(query);
        if !query.ranks() {
            let facts: HashMap<&str, Option<&str>> = self
                .engine
                .facts(&place, query)
                .into_iter()
                .map(|(note, link)| (note.id.as_str(), link))
                .collect();
            let held: Vec<Drawn> = match query.timed() {
                Some(_) => self
                    .ordered(
                        query,
                        archived,
                        |r| facts.contains_key(r.id.as_str()),
                        usize::MAX,
                    )
                    .into_iter()
                    .map(|r| (r, 0, None, Vec::new()))
                    .collect(),
                None => {
                    let listed =
                        |r: &Record| kept(r, archived) && facts.contains_key(r.id.as_str());
                    self.tree(&listed, note, usize::MAX)
                }
            };
            let rows = held
                .into_iter()
                .filter_map(|drawn| {
                    let r = drawn.0;
                    // How it is linked is a fact too, and the one thing said about it, but that
                    // it links here, which being in the place says already. A note under the
                    // place through another note is drawn under it, or has its arrow to say which.
                    let link = facts.get(r.id.as_str())?;
                    Some(Row {
                        linked: true,
                        reasons: link
                            .iter()
                            .filter(|&&how| how != "links here")
                            .map(|&how| how.to_owned())
                            .collect(),
                        ..self.drawn(drawn)
                    })
                })
                .collect();
            return (rows, None);
        }
        let opts = SearchOptions {
            limit: PLACE_LIMIT,
            include_archived: archived,
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
        // list shows where Tab on the line goes: the place's note too, with labels gone into
        // after it, since Tab goes back to it, but not where you are, from which Tab goes
        // nowhere. Then what is in the place, and then what is only found there, each
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

    /// The heads, as they are now and as the record's arrow reads them, of the notes it is
    /// linked to by id. The note being looked around is left out, since everything on the list
    /// is about it.
    pub(super) fn link_heads(&self, record: &Record) -> Vec<String> {
        self.link_heads_besides(record, None)
    }

    /// The same, leaving out too the note its row is drawn under, which the drawing names.
    pub(super) fn link_heads_besides(&self, record: &Record, under: Option<&str>) -> Vec<String> {
        let around = self.place_note();
        record
            .links
            .iter()
            .filter(|&id| Some(id) != around && Some(id.as_str()) != under)
            .filter_map(|id| self.notes.get(id))
            .map(|target| self.arrow(record.head(), target.head()))
            .collect()
    }

    /// Draws again the arrows of the listed notes that link to this one, as it now reads, so
    /// that no arrow names a note as it was before a change, or one that has gone.
    pub(super) fn redraw_arrows(&mut self, id: &str) {
        let redrawn: Vec<(usize, Vec<String>)> = self
            .rows
            .iter()
            .enumerate()
            .filter_map(|(at, row)| {
                let record = self.notes.get(&row.id)?;
                let links_it = record.links.iter().any(|l| l == id);
                let under = unsaid(row.under.as_deref(), row.level);
                links_it.then(|| (at, self.link_heads_besides(record, under)))
            })
            .collect();
        for (at, links) in redrawn {
            self.rows[at].links = links;
        }
    }

    /// Takes away where they stand the rows of the place's facts that a list made now would not
    /// have, as when what they were under has gone, and leaves every other row as it was. What
    /// is only found, by a name that has gone, say, stays listed until the list is made again.
    pub(super) fn leave_facts(&mut self) {
        let mut listed = None;
        let gone: Vec<String> = self
            .rows
            .iter()
            .filter(|row| row.linked && self.notes.get(&row.id).is_some())
            .filter(|row| {
                !listed
                    .get_or_insert_with(|| self.listed_ids())
                    .contains(&row.id)
            })
            .map(|row| row.id.clone())
            .collect();
        for id in gone {
            self.take_away(&id);
        }
    }

    /// The notes a list made now would have.
    fn listed_ids(&self) -> HashSet<String> {
        self.made().1.into_iter().map(|r| r.id).collect()
    }

    fn row(&self, hit: &Hit<'_>) -> Row {
        let record = self.notes.get(hit.note.id.as_str());
        let has_body = text::has_body(&hit.note.text);
        Row {
            id: hit.note.id.clone(),
            head: hit.note.head().to_owned(),
            shown: self.shown(hit.note.head()),
            task: text::task(&hit.note.text),
            archived: hit.note.archived,
            has_body,
            linked: hit.linked,
            links: record.map(|r| self.link_heads(r)).unwrap_or_default(),
            reasons: hit
                .reasons
                .iter()
                .map(|r| {
                    // A note it was found about is named as the row's arrow reads it.
                    let text = r.read(|id| {
                        let note = self.notes.get(id)?;
                        Some(self.arrow(hit.note.head(), note.head()))
                    });
                    match r.scorer {
                        // A name is a fact and reads as one, with no scorer to credit.
                        Scorer::Name => text,
                        scorer => format!("{scorer}: {text}"),
                    }
                })
                .collect(),
            score: Some(hit.score),
            depth: 0,
            level: 0,
            under: None,
            through: Vec::new(),
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

    /// Takes a note's row off the list where it stands and leaves every other row as it was,
    /// for a note taken away under the cursor: the list is made again only when it would be for
    /// another reason, so that what was listed beside the note does not go with it. Says where
    /// the row was.
    pub(super) fn drop_row(&mut self, id: &str) -> Option<usize> {
        self.stale = true;
        let at = self.rows.iter().position(|r| r.id == id)?;
        let gone = self.rows.remove(at);
        // What was drawn under it comes up a level where it stands, as the lines under a heading
        // taken out of a document close up, and what was right under it is under what it was
        // under. Each says again what the drawing no longer does.
        let below = self.rows[at..]
            .iter()
            .take_while(|r| r.level > gone.level)
            .count();
        for i in at..at + below {
            let row = &mut self.rows[i];
            row.level -= 1;
            row.depth = row.level.min(DEEPEST);
            // What was right under it is now under what it was under through it, as the list
            // made again has it when it was walked through, and stands where it stood.
            if row.under.as_deref() == Some(id) {
                row.under.clone_from(&gone.under);
                let inner = std::mem::take(&mut row.through);
                row.through = gone.through.to_vec();
                row.through.push(gone.id.clone());
                row.through.extend(inner);
            }
            let under = unsaid(row.under.as_deref(), row.level).map(str::to_owned);
            if let Some(record) = self.notes.get(&self.rows[i].id) {
                self.rows[i].links = self.link_heads_besides(record, under.as_deref());
            }
        }
        // A new row being written under it is now written under what it was under.
        if let Some(field) = self.field.as_mut()
            && field.under.as_deref() == Some(id)
        {
            field.under.clone_from(&gone.under);
        }
        // What the line only finds begins where it did, at the row that took this one's place,
        // and is set apart only after a fact, as when the list is made.
        if self.finds_from.as_deref() == Some(id) {
            self.finds_from = self.rows.get(at).map(|r| r.id.clone());
        }
        let from = self
            .finds_from
            .as_ref()
            .and_then(|from| self.rows.iter().position(|r| &r.id == from));
        if from.is_some_and(|from| !self.rows[..from].iter().any(|r| r.linked)) {
            self.finds_from = None;
        }
        Some(at)
    }

    /// Takes the row of a note deleted under the cursor off the list, as a tick's row goes but
    /// with the rest of the list left where it was, and puts the cursor on the row that took
    /// its place: on the row above it if it was the last, and on the line if none is left.
    pub(super) fn take_row(&mut self, id: &str) {
        let Some(at) = self.drop_row(id) else {
            return;
        };
        self.cursor = self.rows.len().checked_sub(1).map(|last| at.min(last));
        if self.cursor.is_none() {
            self.onto_line();
        }
    }

    /// Follows notes changed by something other than the typing under the cursor: a tick or a
    /// delete here, a change undone or done again, or a version from elsewhere. A note's row goes
    /// where it stands, as a deleted note's does, when the note is deleted, or when it was
    /// archived or brought back and the list made now would not have it; the rest of the list
    /// stays as it was, rows and order. Any other row is drawn again as its note now reads, since
    /// an edit never moves a row. The row under the cursor goes with the cursor going to the row
    /// that took its place, unless it is being written in, when it goes once the cursor leaves
    /// it. Says whether a row went or is going.
    pub(super) fn follow(&mut self, ids: &[String]) -> bool {
        let mut listed = None;
        let mut went = false;
        for id in ids {
            let goes = self.goes(id, &mut listed);
            let under = self.selected().is_some_and(|r| &r.id == id);
            // The row under the hands reads as its field does until the cursor leaves it.
            let in_hand = under && self.field.as_ref().is_some_and(|f| f.in_hand());
            if !in_hand {
                self.mend_row(id);
                if let (Some(row), Some(record)) = (
                    self.rows.iter_mut().find(|r| &r.id == id),
                    self.notes.get(id),
                ) {
                    row.archived = record.archived;
                }
            }
            if !goes {
                if self.going.as_ref() == Some(id) {
                    self.going = None;
                }
                continue;
            }
            went = true;
            self.take_away(id);
        }
        went
    }

    /// Takes a listed note's row away where it stands. The row under the cursor goes with the
    /// cursor going to the row that took its place, unless it is being written in, when it goes
    /// once the cursor leaves it; any other goes with the cursor staying on its own row.
    fn take_away(&mut self, id: &str) {
        let under = self.selected().is_some_and(|r| r.id == id);
        let in_hand = under && self.field.as_ref().is_some_and(|f| f.in_hand());
        if in_hand {
            self.going = Some(id.to_owned());
        } else if under {
            self.field = None;
            self.take_row(id);
        } else if let Some(at) = self.drop_row(id) {
            self.cursor = self.cursor.map(|i| if at < i { i - 1 } else { i });
        }
    }

    /// Whether a listed note's row is to go: the note is deleted, or it was archived or brought
    /// back, or its row is going already, and the list made now would not have it. What the list
    /// would have is made once, into `listed`, however many notes ask.
    pub(super) fn goes(&self, id: &str, listed: &mut Option<HashSet<String>>) -> bool {
        let Some(row) = self.rows.iter().find(|r| r.id == id) else {
            return false;
        };
        let Some(record) = self.notes.get(id) else {
            return true;
        };
        (record.archived != row.archived || self.going.as_deref() == Some(id))
            && !listed.get_or_insert_with(|| self.listed_ids()).contains(id)
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
    /// a place, and none of what the line finds, which is in the order of its scores, nor of a
    /// line of time, which is in time order.
    pub(super) fn in_order(&self) -> usize {
        if self.nests() { self.rows.len() } else { 0 }
    }

    /// The row a move takes the row at `at` past: the note `by` places along among the notes
    /// beside it, under the note it is drawn under, or at the top, stopping at the first and the
    /// last. None where the row cannot be moved, or there is no note beside it that way.
    pub(super) fn sibling(&self, at: usize, by: i64) -> Option<usize> {
        // A row under its note through a note not listed stands where that one does, with all
        // else under it through that note, so it is not moved alone, and moves with none of them.
        if at >= self.in_order() || !self.rows[at].through.is_empty() {
            return None;
        }
        let under = &self.rows[at].under;
        let beside: Vec<usize> = (0..self.rows.len())
            .filter(|&i| &self.rows[i].under == under)
            .collect();
        let from = beside.iter().position(|&i| i == at)?;
        let to = (from as i64 + by).clamp(0, beside.len() as i64 - 1) as usize;
        (to != from).then(|| beside[to])
    }

    /// Whether the list is in the order the space keeps its notes in, and so is drawn as a
    /// tree: whether the line, as it was read when the list was made, neither ranks nor is a
    /// line of time.
    pub(super) fn nests(&self) -> bool {
        !self.parsed.ranks() && self.parsed.timed().is_none()
    }
}

/// A note as a list drawn as a tree has it: how deep it is, the listed note it is under, and the
/// notes not listed it is under that one through, outermost first, which it stands among those
/// beside it by.
pub(super) type Drawn<'a> = (&'a Record, usize, Option<&'a Record>, Vec<&'a Record>);

/// Where a note stands among those beside it: where each note not listed that it is under them
/// through stands in the space's order, outermost first, and then where it does.
fn stand(walk: &Walk, through: &[&Record], place: usize) -> Vec<usize> {
    let mut at: Vec<usize> = through
        .iter()
        .map(|t| walk.place(&t.id).unwrap_or(usize::MAX))
        .collect();
    at.push(place);
    at
}

/// Whether a note is one to list, the archived ones too if asked.
fn kept(r: &Record, archived: bool) -> bool {
    !r.deleted && (archived || !r.archived)
}

/// The note a row's arrow leaves out because the drawing says it: the one it is drawn under,
/// unless it is deeper than it is drawn.
pub(super) fn unsaid(under: Option<&str>, level: usize) -> Option<&str> {
    under.filter(|_| level <= DEEPEST)
}

/// A list in the space's order, as its tree is drawn: which notes it has, and the note you are
/// in, if any, which is the top.
struct Lists<'s, 'f> {
    session: &'s Session,
    listed: &'f dyn Fn(&Record) -> bool,
    root: Option<&'f str>,
}

impl<'s> Lists<'s, '_> {
    fn has(&self, r: &Record) -> bool {
        (self.listed)(r) && Some(r.id.as_str()) != self.root
    }

    /// The listed note a note is drawn under, or none at the top.
    fn above(&self, r: &'s Record) -> Option<&'s Record> {
        self.above_via(r).map(|(above, _)| above)
    }

    /// The same, with the notes not listed it is under that one through, outermost first: where
    /// the note stands among those beside it, as it did while they were listed.
    fn above_via(&self, r: &'s Record) -> Option<(&'s Record, Vec<&'s Record>)> {
        let notes = &self.session.notes;
        self.above_by(r, |id| notes.get(id).filter(|target| self.has(target)))
    }

    /// The listed note a note is drawn under, as `find` finds a listed note by its id, or none at
    /// the top: the first it links to that is listed, and failing one, what is above the first
    /// it links to, found the same way. A link to the note you are in is the top.
    fn above_by<T>(
        &self,
        r: &'s Record,
        find: impl Fn(&str) -> Option<T>,
    ) -> Option<(T, Vec<&'s Record>)> {
        let mut at = r;
        let mut passed: Vec<&'s Record> = Vec::new();
        loop {
            let mut through = None;
            for link in &at.links {
                let link = link.as_str();
                if Some(link) == self.root {
                    return None;
                }
                if link == r.id {
                    continue;
                }
                if let Some(found) = find(link) {
                    passed.reverse();
                    return Some((found, passed));
                }
                if through.is_none() {
                    through = self.session.notes.get(link);
                }
            }
            let next = through?;
            if passed.iter().any(|&p| std::ptr::eq(p, next)) {
                return None;
            }
            passed.push(next);
            at = next;
        }
    }

    /// The tree of every listed note, drawn in one pass over them: `held` in the space's order,
    /// each with where it is in the walk.
    fn whole(&self, walk: &Walk, held: Vec<(usize, &'s Record)>) -> Vec<Drawn<'s>> {
        let n = held.len();
        let at: HashMap<&str, usize> = held
            .iter()
            .enumerate()
            .map(|(i, (_, r))| (r.id.as_str(), i))
            .collect();
        let mut above = Vec::with_capacity(n);
        let mut through = Vec::with_capacity(n);
        let mut below = vec![Vec::new(); n];
        let mut stands = false;
        for (i, &(_, r)) in held.iter().enumerate() {
            let (p, chain) = self
                .above_by(r, |id| at.get(id).copied())
                .map_or((None, Vec::new()), |(p, chain)| (Some(p), chain));
            if let Some(p) = p {
                stands |= !chain.is_empty();
                below[p].push(i);
            }
            above.push(p);
            through.push(chain);
        }
        // What is under a note through notes not listed stands where they do, from the top down.
        if stands {
            for notes in &mut below {
                notes.sort_by_cached_key(|&i| stand(walk, &through[i], held[i].0));
            }
        }
        let held: Vec<&Record> = held.into_iter().map(|(_, r)| r).collect();
        // What the top reaches, so that a ring, which it does not, is drawn where it comes.
        let mut reached = vec![false; n];
        let mut stack: Vec<usize> = (0..n).filter(|&i| above[i].is_none()).collect();
        while let Some(i) = stack.pop() {
            if !std::mem::replace(&mut reached[i], true) {
                stack.extend(&below[i]);
            }
        }
        let heads_ring = |i: usize| {
            let mut climbed = vec![i];
            while let Some(next) = above[climbed[climbed.len() - 1]] {
                if let Some(from) = climbed.iter().position(|&c| c == next) {
                    return climbed[from..].iter().min() == Some(&i);
                }
                climbed.push(next);
            }
            false
        };
        let mut drawn = vec![false; n];
        let mut out = Vec::with_capacity(n);
        let mut stack = Vec::new();
        for top in 0..n {
            if drawn[top] || (above[top].is_some() && (reached[top] || !heads_ring(top))) {
                continue;
            }
            stack.push((top, 0));
            while let Some((i, level)) = stack.pop() {
                if std::mem::replace(&mut drawn[i], true) {
                    continue;
                }
                let under = above[i].filter(|_| level > 0).map(|p| held[p]);
                let chain = match under {
                    Some(_) => std::mem::take(&mut through[i]),
                    None => Vec::new(),
                };
                out.push((held[i], level, under, chain));
                stack.extend(below[i].iter().rev().map(|&c| (c, level + 1)));
            }
        }
        out
    }

    /// Whether a note under `above` is where a ring is drawn from: nothing at the top is above
    /// it, and it comes first in the space's order of the ring above it. What is found to be
    /// under something at the top is put in `topped`.
    fn heads_ring(
        &self,
        r: &'s Record,
        above: &'s Record,
        walk: &Walk,
        topped: &mut HashSet<*const Record>,
    ) -> bool {
        if topped.contains(&std::ptr::from_ref(above)) {
            topped.insert(std::ptr::from_ref(r));
            return false;
        }
        let mut climbed = vec![r];
        let mut next = Some(above);
        while let Some(at) = next {
            if topped.contains(&std::ptr::from_ref(at)) {
                break;
            }
            if let Some(from) = climbed.iter().position(|&c| std::ptr::eq(c, at)) {
                let head = climbed[from..]
                    .iter()
                    .min_by_key(|c| walk.place(&c.id).unwrap_or(usize::MAX));
                return head.is_some_and(|&head| std::ptr::eq(head, r));
            }
            climbed.push(at);
            next = self.above(at);
        }
        topped.extend(climbed.into_iter().map(std::ptr::from_ref));
        false
    }

    /// The listed notes drawn right under a note, in the space's order, each with the note it
    /// stands at: those linking to it, and those linking to what is not listed and links to it in
    /// turn, however deep. Every note not listed that links there is walked through, since a note
    /// is under what its own links put it under, whatever the note walked through is under.
    fn below(&self, r: &'s Record, walk: &Walk) -> Vec<(&'s Record, Vec<&'s Record>)> {
        let notes = &self.session.notes;
        let mut below = Vec::new();
        // What is walked through, each note of it once, so that a ring of notes not listed ends
        // the walk.
        let mut through = vec![r];
        let mut passed: HashSet<*const Record> = HashSet::from([std::ptr::from_ref(r)]);
        while let Some(at) = through.pop() {
            for id in self.session.engine.linking_to(&at.id) {
                let Some(c) = notes.get(id) else {
                    continue;
                };
                if Some(id) == self.root || !passed.insert(std::ptr::from_ref(c)) {
                    continue;
                }
                if !self.has(c) {
                    through.push(c);
                    continue;
                }
                let Some((above, chain)) = self.above_via(c) else {
                    continue;
                };
                if std::ptr::eq(above, r) {
                    let place = walk.place(&c.id).unwrap_or(usize::MAX);
                    below.push((stand(walk, &chain, place), c, chain));
                }
            }
        }
        // What is under it through notes not listed stands where they do, from the top down.
        below.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        below.into_iter().map(|(_, c, chain)| (c, chain)).collect()
    }
}
