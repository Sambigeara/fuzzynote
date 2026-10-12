//! The engine: owns the notes and the index, answers queries with reasons.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::index::{DocId, Index, TermId};
use crate::note::{Note, NoteId};
use crate::score::{self, Candidates, Context, Label, QueryLabel, Reason, Weights};
use crate::text::{self, Facts};
use crate::time::Day;

struct Doc {
    note: Note,
    facts: Facts,
}

/// Notes plus the index over them. Create one per space.
#[derive(Default)]
pub struct Engine {
    docs: Vec<Option<Doc>>,
    by_id: HashMap<NoteId, DocId>,
    free: Vec<DocId>,
    index: Index,
    weights: Weights,
}

/// How to answer a query.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchOptions {
    pub limit: usize,
    pub include_archived: bool,
    /// Today, for recency. When absent, proximity in time is not scored for typed queries.
    pub today: Option<Day>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            limit: 10,
            include_archived: true,
            today: None,
        }
    }
}

/// A suggested note, its score, and why.
#[derive(Clone, Debug)]
pub struct Hit<'a> {
    pub note: &'a Note,
    pub score: f32,
    pub reasons: Vec<Reason>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub notes: usize,
    pub archived: usize,
    pub terms: usize,
    pub tokens: u64,
}

impl Engine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_weights(weights: Weights) -> Self {
        Self {
            weights,
            ..Self::default()
        }
    }

    pub fn weights(&self) -> &Weights {
        &self.weights
    }

    pub fn weights_mut(&mut self) -> &mut Weights {
        &mut self.weights
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn get(&self, id: &NoteId) -> Option<&Note> {
        let &d = self.by_id.get(id)?;
        self.docs[d as usize].as_ref().map(|doc| &doc.note)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Note> {
        self.docs.iter().flatten().map(|d| &d.note)
    }

    /// Adds a note, or replaces the note with the same id.
    pub fn upsert(&mut self, note: Note) {
        let facts = text::facts(&note.text);
        let tokens = text::tokens(&note.text);
        let labels = doc_labels(&note, &facts);
        let doc = match self.by_id.get(&note.id) {
            Some(&d) => {
                self.index.remove(d);
                d
            }
            None => {
                let d = self.free.pop().unwrap_or_else(|| {
                    self.docs.push(None);
                    (self.docs.len() - 1) as DocId
                });
                self.by_id.insert(note.id.clone(), d);
                d
            }
        };
        self.index.add(doc, &tokens, &labels);
        self.docs[doc as usize] = Some(Doc { note, facts });
    }

    /// Removes a note and returns it.
    pub fn remove(&mut self, id: &NoteId) -> Option<Note> {
        let d = self.by_id.remove(id)?;
        self.index.remove(d);
        self.free.push(d);
        self.docs[d as usize].take().map(|doc| doc.note)
    }

    /// What the input relates to, as it is being typed.
    pub fn search(&self, text: &str, opts: &SearchOptions) -> Vec<Hit<'_>> {
        let terms = text::tokens(text);
        let facts = text::facts(text);
        let labels = query_labels(&terms, &facts, None);
        let ctx = Context {
            terms: &terms,
            labels: &labels,
            dates: &facts.dates,
            day: opts.today,
            exclude: None,
        };
        self.run(&ctx, opts)
    }

    /// The neighbourhood of an existing note.
    pub fn related(&self, id: &NoteId, opts: &SearchOptions) -> Vec<Hit<'_>> {
        let Some(&d) = self.by_id.get(id) else {
            return Vec::new();
        };
        let Some(doc) = self.docs[d as usize].as_ref() else {
            return Vec::new();
        };
        let terms = self.top_terms(d, 20);
        let labels = query_labels(&terms, &doc.facts, Some(doc.note.head()));
        let ctx = Context {
            terms: &terms,
            labels: &labels,
            dates: &doc.facts.dates,
            day: doc.note.day(),
            exclude: Some(d),
        };
        self.run(&ctx, opts)
    }

    pub fn stats(&self) -> Stats {
        Stats {
            notes: self.index.live_docs(),
            archived: self
                .docs
                .iter()
                .flatten()
                .filter(|d| d.note.archived)
                .count(),
            terms: self.index.term_count(),
            tokens: self.index.total_tokens(),
        }
    }

    fn run(&self, ctx: &Context, opts: &SearchOptions) -> Vec<Hit<'_>> {
        let mut c = Candidates::default();
        score::lexical(&self.index, ctx, &self.weights, &mut c);
        score::explicit(&self.index, ctx, &mut c);
        let day_of = |d: DocId| {
            self.docs
                .get(d as usize)
                .and_then(Option::as_ref)
                .and_then(|doc| doc.note.day())
        };
        score::temporal(&self.index, &day_of, ctx, &self.weights, &mut c);
        score::combine(c, &self.weights)
            .into_iter()
            .filter_map(|(d, score, reasons)| {
                let doc = self.docs.get(d as usize)?.as_ref()?;
                if !opts.include_archived && doc.note.archived {
                    return None;
                }
                Some(Hit {
                    note: &doc.note,
                    score,
                    reasons,
                })
            })
            .take(opts.limit)
            .collect()
    }

    /// The note's most distinctive terms, ignoring ones no other note shares.
    fn top_terms(&self, doc: DocId, n: usize) -> Vec<String> {
        let mut scored: Vec<(f32, TermId)> = self
            .index
            .doc_terms(doc)
            .iter()
            .filter(|&&(t, _)| self.index.df(t) > 1)
            .map(|&(t, tf)| ((tf as f32).ln_1p() * self.index.idf(t), t))
            .collect();
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        scored
            .into_iter()
            .take(n)
            .map(|(_, t)| self.index.term_name(t).to_owned())
            .collect()
    }
}

fn doc_labels(note: &Note, facts: &Facts) -> Vec<Label> {
    let mut labels: Vec<Label> = Vec::new();
    labels.extend(facts.tags.iter().cloned().map(Label::Tag));
    labels.extend(facts.mentions.iter().cloned().map(Label::Mention));
    labels.extend(facts.links.iter().cloned().map(Label::Link));
    let head = text::normalise_head(note.head());
    if !head.is_empty() {
        labels.push(Label::Head(head));
    }
    let mut head_terms = text::tokens(note.head());
    head_terms.sort_unstable();
    head_terms.dedup();
    labels.extend(head_terms.into_iter().map(Label::HeadTerm));
    labels.extend(facts.dates.iter().copied().map(Label::Date));
    if let Some(d) = note.created
        && !facts.dates.contains(&d)
    {
        labels.push(Label::Date(d));
    }
    labels
}

fn query_labels(terms: &[String], facts: &Facts, backlinks_to: Option<&str>) -> Vec<QueryLabel> {
    let mut labels: Vec<QueryLabel> = Vec::new();
    let mut distinct = terms.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    labels.extend(distinct.into_iter().map(QueryLabel::HeadTerm));
    labels.extend(facts.tags.iter().cloned().map(QueryLabel::Tag));
    labels.extend(facts.mentions.iter().cloned().map(QueryLabel::Mention));
    for l in &facts.links {
        labels.push(QueryLabel::LinkTarget(l.clone()));
        labels.push(QueryLabel::LinksTo(l.clone()));
    }
    if let Some(head) = backlinks_to {
        let head = text::normalise_head(head);
        if !head.is_empty() {
            labels.push(QueryLabel::LinksTo(head));
        }
    }
    labels
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time;

    fn engine(notes: &[(&str, &str)]) -> Engine {
        let mut e = Engine::new();
        for (id, text) in notes {
            e.upsert(Note::new(*id, *text));
        }
        e
    }

    fn ids<'a>(hits: &[Hit<'a>]) -> Vec<&'a str> {
        hits.iter().map(|h| h.note.id.as_str()).collect()
    }

    fn has_reason(hit: &Hit, scorer: &str, contains: &str) -> bool {
        hit.reasons
            .iter()
            .any(|r| r.scorer == scorer && r.text.contains(contains))
    }

    #[test]
    fn ranks_notes_sharing_rare_words_first() {
        let e = engine(&[
            ("a", "kettlebell swings 20kg"),
            ("b", "kettlebell press"),
            ("c", "swings and roundabouts at the park"),
            ("d", "the park at night"),
            ("e", "the the the"),
        ]);
        let hits = e.search("kettlebell swings", &SearchOptions::default());
        assert_eq!(ids(&hits)[0], "a");
        assert!(ids(&hits).contains(&"b") && ids(&hits).contains(&"c"));
        assert!(!ids(&hits).contains(&"e"));
        assert!(has_reason(&hits[0], "words", "kettlebell"));
    }

    #[test]
    fn fuzzy_matches_misspellings_and_says_so() {
        let e = engine(&[("a", "kettlebells log"), ("b", "shopping")]);
        let hits = e.search("kettlebels", &SearchOptions::default());
        assert_eq!(ids(&hits), ["a"]);
        assert!(has_reason(&hits[0], "words", "kettlebels≈kettlebells"));
    }

    #[test]
    fn explicit_labels_connect_notes_without_shared_words() {
        let e = engine(&[
            ("a", "buy milk #errand"),
            ("b", "post letters #errand"),
            ("c", "ping @joe about the roof"),
        ]);
        let hits = e.search("#errand", &SearchOptions::default());
        assert_eq!(ids(&hits).len(), 2);
        assert!(hits.iter().all(|h| has_reason(h, "explicit", "#errand")));
        let hits = e.search("@Joe", &SearchOptions::default());
        assert_eq!(ids(&hits), ["c"]);
    }

    #[test]
    fn wikilinks_resolve_to_heads_and_back() {
        let e = engine(&[
            ("prog", "Kettlebell Programme\nweek one: swings"),
            ("mon", "Monday\nfollow [[kettlebell programme]]"),
            ("tue", "Tuesday\nrest"),
        ]);
        let hits = e.search("[[Kettlebell Programme]]", &SearchOptions::default());
        assert_eq!(ids(&hits)[0], "prog");
        assert!(has_reason(
            &hits[0],
            "explicit",
            "is [[kettlebell programme]]"
        ));
        let related = e.related(&NoteId::new("prog"), &SearchOptions::default());
        assert_eq!(ids(&related), ["mon"]);
        assert!(has_reason(
            &related[0],
            "explicit",
            "links [[kettlebell programme]]"
        ));
    }

    #[test]
    fn head_words_outrank_length_normalisation() {
        let long_body = "lorem ipsum dolor sit amet ".repeat(400);
        let e = engine(&[
            ("page", &format!("Horticultural Trials\n{long_body}")),
            ("journal", "Journal\nread about horticultural trials today"),
            ("other", "the the the"),
        ]);
        let hits = e.search("horticultural trials", &SearchOptions::default());
        assert_eq!(ids(&hits)[0], "page");
        assert!(has_reason(
            &hits[0],
            "explicit",
            "in head: horticultural, trials"
        ));
    }

    #[test]
    fn related_excludes_itself() {
        let e = engine(&[("a", "alpha beta"), ("b", "alpha gamma"), ("c", "delta")]);
        let hits = e.related(&NoteId::new("a"), &SearchOptions::default());
        assert_eq!(ids(&hits), ["b"]);
        assert!(
            e.related(&NoteId::new("missing"), &SearchOptions::default())
                .is_empty()
        );
    }

    #[test]
    fn dates_connect_notes() {
        let mut e = engine(&[("a", "dentist on 2026-09-05"), ("b", "nothing here")]);
        let mut c = Note::new("c", "written that day");
        c.created = Some(time::day_from_civil(2026, 9, 5));
        e.upsert(c);
        let hits = e.search("2026-09-05", &SearchOptions::default());
        assert_eq!(ids(&hits).len(), 2);
        assert!(
            hits.iter()
                .all(|h| has_reason(h, "time", "same date 2026-09-05"))
        );
    }

    #[test]
    fn recency_reranks_candidates_only() {
        let mut e = Engine::new();
        let today = time::day_from_civil(2026, 10, 12);
        let mut old = Note::new("old", "kettlebell swings");
        old.updated = Some(today - 400);
        let mut new = Note::new("new", "kettlebell swings");
        new.updated = Some(today - 1);
        let mut unrelated = Note::new("x", "cake recipe");
        unrelated.updated = Some(today);
        e.upsert(old);
        e.upsert(new);
        e.upsert(unrelated);
        let hits = e.search(
            "kettlebell",
            &SearchOptions {
                today: Some(today),
                ..Default::default()
            },
        );
        assert_eq!(ids(&hits), ["new", "old"]);
        assert!(has_reason(&hits[0], "time", "a day apart"));
    }

    #[test]
    fn upsert_replaces_and_remove_forgets() {
        let mut e = engine(&[("a", "kettlebell swings"), ("b", "shopping")]);
        e.upsert(Note::new("a", "cake recipe"));
        assert!(e.search("kettlebell", &SearchOptions::default()).is_empty());
        assert_eq!(ids(&e.search("cake", &SearchOptions::default())), ["a"]);
        assert_eq!(e.len(), 2);
        assert!(e.remove(&NoteId::new("a")).is_some());
        assert!(e.remove(&NoteId::new("a")).is_none());
        assert!(e.search("cake", &SearchOptions::default()).is_empty());
        assert_eq!(e.stats().notes, 1);
        e.upsert(Note::new("c", "cake again"));
        assert_eq!(ids(&e.search("cake", &SearchOptions::default())), ["c"]);
    }

    #[test]
    fn archived_notes_are_included_unless_asked_otherwise() {
        let mut e = Engine::new();
        let mut n = Note::new("a", "kettlebell swings");
        n.archived = true;
        e.upsert(n);
        assert_eq!(e.search("kettlebell", &SearchOptions::default()).len(), 1);
        assert!(
            e.search(
                "kettlebell",
                &SearchOptions {
                    include_archived: false,
                    ..Default::default()
                }
            )
            .is_empty()
        );
        assert_eq!(e.stats().archived, 1);
    }
}
