//! The engine: owns the notes and the index, answers queries with reasons.

use std::cmp::{Ordering, Reverse};
use std::collections::HashMap;

use crate::index::{DocId, Index, TermId};
use crate::note::Note;
use crate::query::{Hold, Query};
use crate::score::{self, Candidates, Context, Label, QueryLabel, Reason, Scorer, Weights};
use crate::text::{self, Facts};
use crate::time::Day;

struct Doc {
    note: Note,
    facts: Facts,
    /// The text lowercased, made once here rather than for each note a phrase or letters ask of
    /// on each keystroke.
    lower: String,
}

impl Doc {
    /// The head, lowercased.
    fn lower_head(&self) -> &str {
        text::head(&self.lower)
    }
}

/// Notes plus the index over them. Create one per space.
#[derive(Default)]
pub struct Engine {
    docs: Vec<Option<Doc>>,
    by_id: HashMap<String, DocId>,
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

/// A place being looked around: the last note on the path, if there is one, and the labels
/// gone into after it, which `written` is as they were typed. The labels before the note are
/// only how it was reached, and are not here.
pub(crate) struct Place<'a> {
    pub note: Option<&'a str>,
    pub labels: &'a [Label],
    pub written: &'a str,
}

/// A place as the index has it: its note, the notes linking to it and those it links to, each
/// sorted and without it, and its facts, sorted.
struct Ties {
    note: Option<DocId>,
    links_in: Vec<DocId>,
    links_out: Vec<DocId>,
    facts: Vec<DocId>,
}

/// What a line holds, made ready to ask of each note: the notes carrying its labels, which the
/// index lists and which are worked out once, and the rest, which is asked of each note, a
/// phrase last since it reads the whole text.
#[derive(Default)]
struct Held<'q> {
    /// The notes carrying every label, sorted, or none when the line holds no label.
    carrying: Option<Vec<DocId>>,
    asked: Vec<&'q Hold>,
}

impl Held<'_> {
    fn holds(&self, d: DocId, doc: &Doc) -> bool {
        self.carrying
            .as_ref()
            .is_none_or(|docs| docs.binary_search(&d).is_ok())
            && self
                .asked
                .iter()
                .all(|hold| hold.held_by(&doc.note, &doc.lower))
    }

    /// The notes that may hold it, of `docs`: those carrying its labels, or all of them.
    fn within(&self, docs: usize) -> impl Iterator<Item = DocId> + '_ {
        let all = self.carrying.is_none().then_some(0..docs as DocId);
        self.carrying
            .iter()
            .flatten()
            .copied()
            .chain(all.into_iter().flatten())
    }
}

/// A suggested note, its score, and why.
#[derive(Clone, Debug)]
pub struct Hit<'a> {
    pub note: &'a Note,
    pub score: f32,
    pub reasons: Vec<Reason>,
    /// Whether the note is a fact of the place being looked around: linked to or from its note
    /// and carrying its labels. That is a fact, where the rest is a guess, and such notes are
    /// returned whatever they score and whatever the limit.
    pub linked: bool,
    /// Whether the note's head is what was typed. Such notes come first, whatever they score
    /// and whatever the limit.
    pub named: bool,
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

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn get(&self, id: &str) -> Option<&Note> {
        let &d = self.by_id.get(id)?;
        self.docs[d as usize].as_ref().map(|doc| &doc.note)
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
        let lower = text::lowercase(&note.text);
        self.docs[doc as usize] = Some(Doc { note, facts, lower });
    }

    /// Removes a note and returns it.
    pub fn remove(&mut self, id: &str) -> Option<Note> {
        let d = self.by_id.remove(id)?;
        self.index.remove(d);
        self.free.push(d);
        self.docs[d as usize].take().map(|doc| doc.note)
    }

    /// What the line finds among the notes holding everything else on it, best first with the
    /// reasons: its words, ranked as words always are, the note they name first; or with `~` the
    /// notes whose heads have its letters, which the session puts in order, since a tie goes by
    /// the space's.
    pub(crate) fn find(&self, query: &Query, opts: &SearchOptions) -> Vec<Hit<'_>> {
        let held = self.held(query);
        if query.letters {
            return self.letters(query, &held, opts);
        }
        let (named, _) = self.named_among(&query.name(), &held, opts.include_archived);
        self.typed(&query.words, &named, opts, &[], &held)
    }

    /// Every note holding what the line holds, for a line with nothing to rank by.
    pub(crate) fn holding(&self, query: &Query) -> Vec<&Note> {
        let held = self.held(query);
        held.within(self.docs.len())
            .filter_map(|d| {
                let doc = self.docs[d as usize].as_ref()?;
                held.holds(d, doc).then_some(&doc.note)
            })
            .collect()
    }

    /// What a line holds, as the index has it.
    fn held<'q>(&self, query: &'q Query) -> Held<'q> {
        let carrying = query
            .labels
            .iter()
            .map(|typed| {
                let label = typed.label.indexed();
                if typed.whole {
                    return self.index.docs_with(&label).to_vec();
                }
                let mut docs: Vec<DocId> = self
                    .index
                    .docs_starting(&label)
                    .flatten()
                    .copied()
                    .collect();
                docs.sort_unstable();
                docs.dedup();
                docs
            })
            .reduce(|mut all, docs| {
                all.retain(|d| docs.binary_search(d).is_ok());
                all
            });
        let mut asked: Vec<&Hold> = query.holds.iter().collect();
        asked.sort_by_key(|hold| matches!(hold, Hold::Phrase(_)));
        Held { carrying, asked }
    }

    /// The notes whose heads have every word of the line's letters in order, among those holding
    /// the rest of the line, best first and then the shorter head. Past the limit only what ties
    /// with the last listed is kept, since the space's order, which the session knows, decides
    /// between those.
    fn letters(&self, query: &Query, held: &Held, opts: &SearchOptions) -> Vec<Hit<'_>> {
        let mut letters = score::Letters::new(&query.free);
        let mut found: Vec<(i32, usize, DocId)> = held
            .within(self.docs.len())
            .filter_map(|d| {
                let doc = self.docs[d as usize].as_ref()?;
                if !opts.include_archived && doc.note.archived {
                    return None;
                }
                let head = doc.lower_head();
                let score = letters.score(head)?;
                held.holds(d, doc).then(|| (score, head.chars().count(), d))
            })
            .collect();
        found.sort_unstable_by_key(|&(score, length, d)| (Reverse(score), length, d));
        if let Some(&(score, length, _)) = opts.limit.checked_sub(1).and_then(|n| found.get(n)) {
            let end =
                found.partition_point(|&(s, l, _)| (Reverse(s), l) <= (Reverse(score), length));
            found.truncate(end);
        }
        let reason = Reason {
            scorer: Scorer::Letters,
            text: query.free.split_whitespace().collect::<Vec<_>>().join(", "),
        };
        found
            .into_iter()
            .filter_map(|(score, _, d)| {
                Some(Hit {
                    note: &self.docs[d as usize].as_ref()?.note,
                    score: score as f32,
                    reasons: vec![reason.clone()],
                    linked: false,
                    named: false,
                })
            })
            .collect()
    }

    /// What a text finds by its words, its tags, people and dates, scored, and only the notes
    /// that hold what is held. The last word counts as a prefix unless the text ends in
    /// whitespace. The notes in `first`, sorted, are the notes the text names, and come first
    /// whatever they score and whatever the limit; the notes in `place`, sorted, are listed as
    /// the facts of a place whatever they score and whatever the limit.
    fn typed(
        &self,
        text: &str,
        first: &[DocId],
        opts: &SearchOptions,
        place: &[DocId],
        held: &Held,
    ) -> Vec<Hit<'_>> {
        let (terms, prefix) = typed_words(text);
        let facts = text::facts(text);
        let mut labels = query_labels(&terms, &facts);
        if let Some(p) = &prefix {
            labels.push(QueryLabel::HeadPrefix(p.clone()));
        }
        // A note is found by the name of a note it links to, as it is by its own words.
        let named = |d: DocId| self.named(d);
        let ctx = Context {
            terms: &terms,
            prefix: prefix.as_deref(),
            labels: &labels,
            dates: &facts.dates,
            day: opts.today,
            exclude: None,
            links_in: &[],
            links_out: &[],
            named: Some(&named),
        };
        self.run(&ctx, opts, first, place, held)
    }

    /// The notes whose head is what was typed, sorted: every word of the head was typed and no
    /// other word was. The last word typed stands for the one head word it starts, unless
    /// whitespace follows it. Only when no head is, the notes whose head is what was typed with
    /// the labels that end it left off, so that a name never stops naming the note it named for
    /// a note that only ends the same way.
    fn headed(&self, text: &str) -> Vec<DocId> {
        let full = self.headed_by(text, Label::Name);
        if full.is_empty() {
            self.headed_by(text, Label::Called)
        } else {
            full
        }
    }

    /// The notes one tier of names gives for what was typed, sorted.
    fn headed_by(&self, text: &str, tier: fn(String) -> Label) -> Vec<DocId> {
        let mut words = text::tokens(text);
        let last = if text.ends_with(char::is_whitespace) {
            None
        } else {
            words.pop()
        };
        words.sort_unstable();
        words.dedup();
        let called = |words: &[String]| self.index.docs_with(&tier(words.join(" ")));
        let mut found: Vec<DocId> = match &last {
            None => called(&words).to_vec(),
            // One name for each word the unfinished one may be.
            Some(start) => self
                .index
                .terms_starting(start)
                .flat_map(|word| {
                    let mut all = words.clone();
                    if let Err(at) = all.binary_search_by(|w| w.as_str().cmp(word)) {
                        all.insert(at, word.to_owned());
                    }
                    called(&all)
                })
                .copied()
                .collect(),
        };
        found.sort_unstable();
        found.dedup();
        found
    }

    /// Whether going into a note from the list was a search for it, and not taking a draft there
    /// to be written in it: the note holds everything the line holds, and is named by the words
    /// outside its phrases, every one of them in its head or in the head of a note it links to,
    /// the word still being typed as the start of one; or with `~` it has their letters in its
    /// head. A line with nothing on it to rank by or to hold searched for nothing.
    pub(crate) fn searched(&self, id: &str, query: &Query) -> bool {
        let Some((d, doc)) = self
            .by_id
            .get(id)
            .and_then(|&d| Some((d, self.docs[d as usize].as_ref()?)))
        else {
            return false;
        };
        if query.is_empty() || !self.held(query).holds(d, doc) {
            return false;
        }
        if query.letters {
            return score::Letters::new(&query.free)
                .score(doc.lower_head())
                .is_some();
        }
        let (terms, prefix) = typed_words(&query.free);
        let mut names = text::tokens(doc.note.head());
        for target in doc.note.links.iter().filter_map(|l| self.get(l)) {
            names.extend(text::tokens(target.head()));
        }
        terms.iter().all(|t| names.contains(t))
            && prefix.is_none_or(|p| names.iter().any(|n| n.starts_with(&p)))
    }

    /// Whether a note's text has this in it, in any case.
    pub(crate) fn contains(&self, id: &str, typed: &str) -> bool {
        self.by_id
            .get(id)
            .and_then(|&d| self.docs[d as usize].as_ref())
            .is_some_and(|doc| doc.lower.contains(&text::lowercase(typed)))
    }

    /// A note's head and the notes that link to it.
    fn named(&self, d: DocId) -> Option<(&str, &[DocId])> {
        let doc = self.docs.get(d as usize)?.as_ref()?;
        let linked = self.index.docs_with(&Label::Link(doc.note.id.clone()));
        Some((doc.note.head(), linked))
    }

    /// The facts of a place that hold what the line holds, each with how it is linked to the
    /// place's note when it has one: the notes linked to or from the note that carry every
    /// label, or with no note the notes carrying them all. They are a set, worked out from the
    /// links and the index and in no order; nothing is scored.
    pub(crate) fn facts(&self, place: &Place, query: &Query) -> Vec<(&Note, Option<&'static str>)> {
        let Some(ties) = self.ties(place) else {
            return Vec::new();
        };
        let held = self.held(query);
        ties.facts
            .iter()
            .filter_map(|&d| {
                let doc = self.docs[d as usize].as_ref()?;
                held.holds(d, doc)
                    .then(|| (&doc.note, link_reason(&ties.links_in, &ties.links_out, d)))
            })
            .collect()
    }

    /// The neighbourhood of a place: its facts, marked as such and listed whatever they score
    /// and whatever the limit, and what the engine finds besides, best first. Around a note that
    /// is what shares its tags, people, words and dates, and what links to the same things; a
    /// note linked to it but not carrying the labels is one of those. Around labels alone it is
    /// what their words find, and the last day among them is the day nearness in time is
    /// measured from, as a note's own day is, so that a note written on it says the same date
    /// and not how far it is from today.
    pub(crate) fn neighbourhood(&self, place: &Place, opts: &SearchOptions) -> Vec<Hit<'_>> {
        let Some(ties) = self.ties(place) else {
            return Vec::new();
        };
        let Some(d) = ties.note else {
            let opts = SearchOptions {
                today: place
                    .labels
                    .iter()
                    .rev()
                    .find_map(|label| match label {
                        Label::Date(day) => Some(*day),
                        _ => None,
                    })
                    .or(opts.today),
                ..opts.clone()
            };
            // Whitespace after it, so that the last label's word is a whole word and not the
            // start of one.
            let written = format!("{} ", place.written);
            return self.typed(
                &written,
                &self.headed(&written),
                &opts,
                &ties.facts,
                &Held::default(),
            );
        };
        let Some(doc) = self.docs[d as usize].as_ref() else {
            return Vec::new();
        };
        let terms = self.top_terms(d, 20);
        let mut labels = query_labels(&terms, &doc.facts);
        // Notes linked to the same things this one is are its siblings: a suggestion.
        labels.extend(
            ties.links_out
                .iter()
                .filter_map(|&t| self.docs[t as usize].as_ref())
                .map(|target| QueryLabel::LinksTo {
                    id: target.note.id.clone(),
                    head: target.note.head().to_owned(),
                }),
        );
        let ctx = Context {
            terms: &terms,
            prefix: None,
            labels: &labels,
            dates: &doc.facts.dates,
            day: doc.note.day(),
            exclude: Some(d),
            links_in: &ties.links_in,
            links_out: &ties.links_out,
            named: None,
        };
        self.run(&ctx, opts, &[], &ties.facts, &Held::default())
    }

    /// A place as the index has it, or nothing if its note is gone. Its facts are the notes
    /// linked either way that carry every label, the links alone with no labels, or the carriers
    /// alone with no note.
    fn ties(&self, place: &Place) -> Option<Ties> {
        let carrying = place.labels.split_first().map(|(first, rest)| {
            let mut carrying = self.index.docs_with(first).to_vec();
            for label in rest {
                let with = self.index.docs_with(label);
                carrying.retain(|d| with.binary_search(d).is_ok());
            }
            carrying
        });
        let Some(id) = place.note else {
            return Some(Ties {
                note: None,
                links_in: Vec::new(),
                links_out: Vec::new(),
                facts: carrying.unwrap_or_default(),
            });
        };
        let &d = self.by_id.get(id)?;
        let doc = self.docs[d as usize].as_ref()?;
        let mut links_in = self.index.docs_with(&Label::Link(id.to_owned())).to_vec();
        let mut links_out: Vec<DocId> = doc
            .note
            .links
            .iter()
            .filter_map(|l| self.by_id.get(l).copied())
            .collect();
        for docs in [&mut links_in, &mut links_out] {
            docs.sort_unstable();
            docs.dedup();
            docs.retain(|&other| other != d);
        }
        let mut facts: Vec<DocId> = links_in.iter().chain(&links_out).copied().collect();
        facts.sort_unstable();
        facts.dedup();
        if let Some(carrying) = carrying {
            facts.retain(|d| carrying.binary_search(d).is_ok());
        }
        Some(Ties {
            note: Some(d),
            links_in,
            links_out,
            facts,
        })
    }

    /// The notes the line names, by the rule that lists a name first in `find`, that hold what
    /// the line holds: those whose head is what was typed, or only when there are none, those
    /// whose head is with the labels that end it left off, which says `true` beside them. The
    /// app goes into the one there is. Letters name nothing.
    pub(crate) fn notes_named(&self, query: &Query, include_archived: bool) -> (Vec<&Note>, bool) {
        if query.letters {
            return (Vec::new(), false);
        }
        let held = self.held(query);
        let (named, called) = self.named_among(&query.name(), &held, include_archived);
        let notes = named
            .into_iter()
            .filter_map(|d| Some(&self.docs[d as usize].as_ref()?.note))
            .collect();
        (notes, called)
    }

    /// The notes a name names among those that can be listed, sorted, and whether they are of the
    /// second tier. The tier is chosen among what is left once archived notes and notes that do
    /// not hold the line are left out, so that the list and going in agree: a head that would
    /// name a note no one can see does not keep the name from the notes that can be.
    fn named_among(&self, name: &str, held: &Held, include_archived: bool) -> (Vec<DocId>, bool) {
        let tier = |tier: fn(String) -> Label| -> Vec<DocId> {
            self.headed_by(name, tier)
                .into_iter()
                .filter(|&d| {
                    self.docs[d as usize].as_ref().is_some_and(|doc| {
                        (include_archived || !doc.note.archived) && held.holds(d, doc)
                    })
                })
                .collect()
        };
        let full = tier(Label::Name);
        if full.is_empty() {
            (tier(Label::Called), true)
        } else {
            (full, false)
        }
    }

    /// The notes a text names in `[[brackets]]`, by their heads. Brackets are a way of typing a
    /// link and nothing more: the link itself is the id, held on the note that was saved.
    pub fn named_in(&self, text: &str) -> Vec<String> {
        let mut named = Vec::new();
        for head in text::facts(text).links {
            for &d in self.index.docs_with(&Label::Head(head.clone())) {
                if let Some(doc) = self.docs[d as usize].as_ref()
                    && !named.contains(&doc.note.id)
                {
                    named.push(doc.note.id.clone());
                }
            }
        }
        named
    }

    /// The links a note holds once its text is saved. Every note the new text names in brackets
    /// is linked, a link whose brackets were taken out of the text is dropped, and links made
    /// any other way are kept. A note does not link to itself.
    pub fn links_on_save(&self, id: &str, old: &str, new: &str, links: &[String]) -> Vec<String> {
        let (before, after) = (self.named_in(old), self.named_in(new));
        let mut links: Vec<String> = links
            .iter()
            .filter(|l| !before.contains(l) || after.contains(l))
            .cloned()
            .collect();
        for l in after {
            if l != id && !links.contains(&l) {
                links.push(l);
            }
        }
        links
    }

    /// Scores the candidates and lists them best first, after the notes in `first`, which is
    /// sorted: those are the notes being named, and they are listed whatever they score. So are
    /// the notes in `facts`, also sorted, which are the facts of the place being looked around.
    /// Only the notes that hold what is held are listed at all.
    fn run<'a: 'c, 'c>(
        &'a self,
        ctx: &Context<'c>,
        opts: &SearchOptions,
        first: &[DocId],
        facts: &[DocId],
        held: &Held,
    ) -> Vec<Hit<'a>> {
        let mut c = Candidates::default();
        for &d in first.iter().chain(facts) {
            c.include(d);
        }
        score::lexical(&self.index, ctx, &self.weights, &mut c);
        score::explicit(&self.index, ctx, &self.weights, &mut c);
        let day_of = |d: DocId| {
            self.docs
                .get(d as usize)
                .and_then(Option::as_ref)
                .and_then(|doc| doc.note.day())
        };
        score::temporal(&self.index, &day_of, ctx, &self.weights, &mut c);
        let named = |d: DocId| first.binary_search(&d).is_ok();
        let fact = |d: DocId| facts.binary_search(&d).is_ok();
        let mut ranked = score::combine(c, &self.weights, &|d| fact(d) || named(d));
        // A stable sort, so the notes being named and the rest each keep their order.
        ranked.sort_by_key(|&(d, ..)| !named(d));
        let mut suggestions = 0;
        ranked
            .into_iter()
            .filter_map(|(d, score, mut reasons)| {
                let doc = self.docs.get(d as usize)?.as_ref()?;
                if !opts.include_archived && doc.note.archived {
                    return None;
                }
                // The limit is on suggestions. What is a fact or named is always there. A
                // suggestion past the limit is not asked whether it holds, since a phrase reads
                // the whole text.
                let suggested = !named(d) && !fact(d);
                if suggested && suggestions == opts.limit || !held.holds(d, doc) {
                    return None;
                }
                suggestions += usize::from(suggested);
                if let Some(text) = link_reason(ctx.links_in, ctx.links_out, d) {
                    reasons.insert(
                        0,
                        Reason {
                            scorer: Scorer::Link,
                            text: text.to_owned(),
                        },
                    );
                }
                if named(d) {
                    reasons.insert(
                        0,
                        Reason {
                            scorer: Scorer::Name,
                            text: "its name".to_owned(),
                        },
                    );
                }
                Some(Hit {
                    note: &doc.note,
                    score,
                    reasons,
                    linked: fact(d),
                    named: named(d),
                })
            })
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
    labels.extend(note.links.iter().map(|l| Label::Link(l.clone())));
    let head = text::normalise_head(note.head());
    if !head.is_empty() {
        labels.push(Label::Head(head));
    }
    let mut head_terms = text::tokens(note.head());
    head_terms.sort_unstable();
    head_terms.dedup();
    if !head_terms.is_empty() {
        labels.push(Label::Name(head_terms.join(" ")));
    }
    // A head is called without the labels that end it too, since a row in their place reads so.
    let mut called = text::tokens(text::name_of(note.head()));
    called.sort_unstable();
    called.dedup();
    if !called.is_empty() && called != head_terms {
        labels.push(Label::Called(called.join(" ")));
    }
    labels.extend(head_terms.into_iter().map(Label::HeadTerm));
    labels.extend(facts.dates.iter().copied().map(Label::Date));
    if let Some(d) = note.written()
        && !facts.dates.contains(&d)
    {
        labels.push(Label::Date(d));
    }
    if !note.archived && text::task(&note.text) == Some(false) {
        labels.push(Label::Open);
    }
    labels
}

/// How a note is linked to the note being looked around, from the notes that link to it and the
/// notes it links to, each sorted. It is a fact, and reads as one.
fn link_reason(links_in: &[DocId], links_out: &[DocId], d: DocId) -> Option<&'static str> {
    match (
        links_in.binary_search(&d).is_ok(),
        links_out.binary_search(&d).is_ok(),
    ) {
        (true, true) => Some("linked both ways"),
        (true, false) => Some("links here"),
        (false, true) => Some("linked from here"),
        (false, false) => None,
    }
}

/// What was typed, as the words that are complete and the word still being typed. The last word
/// is still being typed unless whitespace follows it. A date counts as a date, which the time
/// scorer reads, and not as the numbers in it, which would find every note with a 16 in it.
fn typed_words(text: &str) -> (Vec<String>, Option<String>) {
    let words = text::without_dates(text);
    let mut terms = text::tokens(&words);
    let prefix = if words.ends_with(char::is_whitespace) {
        None
    } else {
        terms.pop()
    };
    (terms, prefix)
}

/// The head words, tags and mentions a query asks for.
fn query_labels(terms: &[String], facts: &Facts) -> Vec<QueryLabel> {
    let mut labels: Vec<QueryLabel> = Vec::new();
    let mut distinct = terms.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    labels.extend(distinct.into_iter().map(QueryLabel::HeadTerm));
    labels.extend(facts.tags.iter().cloned().map(QueryLabel::Tag));
    labels.extend(facts.mentions.iter().cloned().map(QueryLabel::Mention));
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

    /// What a text finds by its words, its tags, people and dates, as the neighbourhood of
    /// labels asks the scorers, the last word a prefix unless whitespace follows it.
    fn search<'a>(e: &'a Engine, text: &str, opts: &SearchOptions) -> Vec<Hit<'a>> {
        e.typed(text, &e.headed(text), opts, &[], &Held::default())
    }

    /// Written at midnight UTC on the day.
    fn on(day: Day) -> Option<time::Moment> {
        Some(time::Moment::utc(i64::from(day) * 86_400))
    }

    fn ids<'a>(hits: &[Hit<'a>]) -> Vec<&'a str> {
        hits.iter().map(|h| h.note.id.as_str()).collect()
    }

    /// The neighbourhood of a note alone.
    fn near<'a>(e: &'a Engine, id: &str, opts: &SearchOptions) -> Vec<Hit<'a>> {
        let place = Place {
            note: Some(id),
            labels: &[],
            written: "",
        };
        e.neighbourhood(&place, opts)
    }

    fn has_reason(hit: &Hit, scorer: &str, contains: &str) -> bool {
        hit.reasons
            .iter()
            .any(|r| r.scorer.to_string() == scorer && r.text.contains(contains))
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
        let hits = search(&e, "kettlebell swings", &SearchOptions::default());
        assert_eq!(ids(&hits)[0], "a");
        assert!(ids(&hits).contains(&"b") && ids(&hits).contains(&"c"));
        assert!(!ids(&hits).contains(&"e"));
        assert!(has_reason(&hits[0], "words", "kettlebell"));
    }

    #[test]
    fn fuzzy_matches_misspellings_and_says_so() {
        let e = engine(&[("a", "kettlebells log"), ("b", "shopping")]);
        let hits = search(&e, "kettlebels", &SearchOptions::default());
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
        let hits = search(&e, "#errand", &SearchOptions::default());
        assert_eq!(ids(&hits).len(), 2);
        assert!(hits.iter().all(|h| has_reason(h, "explicit", "#errand")));
        let hits = search(&e, "@Joe", &SearchOptions::default());
        assert_eq!(ids(&hits), ["c"]);
    }

    #[test]
    fn brackets_are_a_way_of_typing_a_link() {
        let mut e = engine(&[
            ("prog", "Kettlebell Programme\nweek one: swings"),
            (
                "mon",
                "Monday\nfollow [[kettlebell programme]] and [[Nowhere]]",
            ),
            ("tue", "Tuesday\nrest"),
        ]);
        let id = str::to_owned;
        assert_eq!(
            e.named_in("see [[Kettlebell Programme|the plan]]"),
            [id("prog")]
        );
        assert!(e.named_in("[[Nowhere]] and no brackets").is_empty());
        assert!(
            near(&e, "prog", &SearchOptions::default())
                .iter()
                .all(|h| !h.linked),
            "nothing is linked until a note is saved with its links"
        );

        // Saving links what the text names and keeps what was linked some other way.
        let text = "Monday\nfollow [[kettlebell programme]] and [[Nowhere]]";
        let links = e.links_on_save(&id("mon"), "", text, &[id("tue")]);
        assert_eq!(links, [id("tue"), id("prog")]);
        // Taking the brackets out takes that link away, and no other.
        let plain = "Monday\nfollow the programme";
        assert_eq!(
            e.links_on_save(&id("mon"), text, plain, &links),
            [id("tue")]
        );
        // A link whose target was since renamed is no longer named by the text, so it stays.
        e.upsert(Note::new("prog", "Strength Programme\nweek one: swings"));
        assert_eq!(e.links_on_save(&id("mon"), text, text, &links), links);
        // A note does not link to itself.
        assert!(
            e.links_on_save(&id("tue"), "", "[[Tuesday]]", &[])
                .is_empty()
        );
    }

    fn linked_to(id: &str, text: &str, target: &str) -> Note {
        let mut n = Note::new(id, text);
        n.links.push(target.to_owned());
        n
    }

    #[test]
    fn links_by_id_are_facts_that_survive_a_rename() {
        let mut e = engine(&[
            ("profile", "Profile\ntrainingapp numbers"),
            ("near", "trainingapp ideas and plan"),
            ("far", "cake recipe"),
        ]);
        e.upsert(linked_to("hub", "trainingapp\nthe plan", "profile"));
        e.upsert(linked_to("child", "some new note", "hub"));
        e.upsert(linked_to("sibling", "another one", "hub"));
        let no_suggestions = SearchOptions {
            limit: 0,
            ..Default::default()
        };
        let linked = |e: &Engine, id: &str| -> Vec<String> {
            let mut ids: Vec<String> = near(e, id, &no_suggestions)
                .iter()
                .inspect(|h| assert!(h.linked))
                .map(|h| h.note.id.to_string())
                .collect();
            ids.sort();
            ids
        };
        assert_eq!(
            linked(&e, "hub"),
            ["child", "profile", "sibling"],
            "linked notes are there whatever the limit, and share no words with the hub"
        );
        let around = near(&e, "hub", &SearchOptions::default());
        let hit = |id: &str| around.iter().find(|h| h.note.id.as_str() == id).unwrap();
        assert!(has_reason(hit("child"), "link", "links here"));
        assert!(has_reason(hit("profile"), "link", "linked from here"));
        assert!(!hit("near").linked, "shared words are a suggestion");
        assert!(around.iter().all(|h| h.note.id.as_str() != "far"));

        // From a child, the hub is linked and the other child is a suggestion with its reason.
        let around = near(&e, "child", &SearchOptions::default());
        assert_eq!(ids(&around), ["hub", "sibling"]);
        assert!(around[0].linked && !around[1].linked);
        assert!(has_reason(&around[1], "explicit", "also links trainingapp"));
        assert!(
            around.iter().all(|h| !has_reason(h, "explicit", "about ")),
            "in a neighbourhood what is linked is listed already"
        );

        e.upsert(linked_to("hub", "Training App\nrenamed", "profile"));
        assert_eq!(linked(&e, "hub"), ["child", "profile", "sibling"]);
        e.upsert(Note::new("child", "some new note"));
        assert_eq!(linked(&e, "hub"), ["profile", "sibling"], "unlinked");
    }

    #[test]
    fn a_note_is_found_by_the_name_of_a_note_it_links_to() {
        let mut e = engine(&[
            ("house", "House"),
            ("says", "Called the plumber about the boiler"),
            ("far", "cake recipe"),
        ]);
        e.upsert(linked_to("boiler", "Boiler", "house"));
        e.upsert(linked_to("pressure", "pressure dropped again", "boiler"));
        e.upsert(linked_to("quote", "engineer quoted 240", "boiler"));
        let opts = SearchOptions::default();

        // The name finds the note itself first, and then what was written about it.
        let found = search(&e, "boiler ", &opts);
        assert_eq!(found[0].note.id.as_str(), "boiler");
        let mut all = ids(&found);
        all.sort_unstable();
        assert_eq!(all, ["boiler", "pressure", "quote", "says"]);
        let hit = |id: &str| found.iter().find(|h| h.note.id.as_str() == id).unwrap();
        assert!(has_reason(hit("pressure"), "explicit", "about Boiler"));
        assert!(has_reason(hit("boiler"), "explicit", "in head: boiler"));
        assert!(!has_reason(hit("says"), "explicit", "about"));

        // A word of the note and the name of what it is about find it together.
        let found = search(&e, "boiler quoted ", &opts);
        assert_eq!(found[0].note.id.as_str(), "quote");
        assert!(has_reason(
            &found[0],
            "explicit",
            "in head: quoted, about Boiler"
        ));

        // The word still being typed stands for the name it starts.
        assert!(ids(&search(&e, "boi", &opts)).contains(&"pressure"));

        // One hop: the house finds the boiler, and not what was written about the boiler.
        assert_eq!(ids(&search(&e, "house ", &opts)), ["house", "boiler"]);

        // The name is read when it is asked for, so a rename is followed.
        e.upsert(linked_to("boiler", "Heating", "house"));
        assert!(ids(&search(&e, "heating ", &opts)).contains(&"pressure"));
        assert_eq!(ids(&search(&e, "boiler ", &opts)), ["says"]);
    }

    #[test]
    fn the_note_headed_with_what_was_typed_comes_first() {
        let long_body = "what was checked and what to watch before next winter ".repeat(300);
        let mut e = engine(&[
            ("doc", &format!("Boiler Service\n{long_body}")),
            ("ideas", "ideas"),
            ("other", "identity papers for the service"),
        ]);
        e.upsert(linked_to("a", "continuing with [[Boiler Service]]", "doc"));
        e.upsert(linked_to("b", "boiler service follow up", "doc"));
        let opts = SearchOptions::default();
        let first = |typed: &str| search(&e, typed, &opts)[0].note.id.to_string();
        let named = |typed: &str| -> Vec<String> {
            search(&e, typed, &opts)
                .iter()
                .filter(|h| has_reason(h, "name", "its name"))
                .map(|h| h.note.id.to_string())
                .collect()
        };

        let found = search(&e, "boiler service", &opts);
        assert_eq!(ids(&found)[0], "doc");
        assert!(
            found[0].score < found[1].score,
            "first for its name, where its score would not put it"
        );
        assert_eq!(found[0].reasons[0].scorer, Scorer::Name);
        assert_eq!(named("boiler service"), ["doc"]);

        // However it is typed, and with its last word unfinished.
        assert_eq!(first("Service  BOILER "), "doc");
        assert_eq!(first("boiler se"), "doc");
        assert_eq!(named("id"), ["ideas"], "a head of one word, from its start");

        // Part of the name is not the name, and nor is more than the name.
        assert!(named("boiler").is_empty());
        assert!(named("boiler service follow").is_empty());
        assert!(named("boiler service winter").is_empty());
        assert_eq!(named("boiler service follow up"), ["b"]);

        // It is there whatever the limit, and hidden with the archived like any other note.
        let none = SearchOptions {
            limit: 0,
            ..Default::default()
        };
        assert_eq!(ids(&search(&e, "boiler service", &none)), ["doc"]);
        let mut doc = e.get("doc").unwrap().clone();
        doc.archived = true;
        e.upsert(doc);
        let live = SearchOptions {
            include_archived: false,
            ..Default::default()
        };
        assert!(!ids(&search(&e, "boiler service", &live)).contains(&"doc"));

        // A neighbourhood has no name in it to go first.
        let around = near(&e, "a", &opts);
        assert!(around.iter().all(|h| !has_reason(h, "name", "")));
    }

    #[test]
    fn going_into_a_note_was_a_search_when_it_holds_the_line_and_its_words_name_it() {
        let mut e = engine(&[("house", "House"), ("far", "cake recipe")]);
        e.upsert(linked_to(
            "boiler",
            "Boiler\nserviced in the spring",
            "house",
        ));
        let mut quote = linked_to(
            "quote",
            "engineer quoted 240 #heating\nsaid the valve sticks",
            "boiler",
        );
        quote.created = on(time::day_from_civil(2026, 10, 13));
        e.upsert(quote);
        let today = time::day_from_civil(2026, 10, 15);
        let searched = |id: &str, typed: &str| e.searched(id, &Query::read(typed, today));

        assert!(searched("quote", "boiler"));
        assert!(searched("quote", "boi"), "the word still being typed");
        assert!(searched("quote", "boiler quoted"));
        assert!(searched("quote", "Quoted BOILER"));
        assert!(!searched("quote", "boi quoted"), "a finished word is whole");
        assert!(!searched("quote", "boiler valve"));
        assert!(!searched("quote", "house"), "one hop");
        assert!(!searched("boiler", "quoted"), "the name goes one way");
        assert!(!searched("boiler", "spring"), "the head, and not the body");
        assert!(searched("quote", "boiler 2026-10-13"), "written that day");
        assert!(!searched("quote", "boiler 2026-10-14"));
        assert!(searched("quote", "/-tue boiler"), "a date word is a day");
        assert!(searched("quote", "#heating boiler"));
        assert!(
            !searched("quote", "#garden boiler"),
            "it holds what is held"
        );
        assert!(searched("quote", "\"valve sticks\""), "a phrase is held");
        assert!(searched("quote", "boiler “valve sticks”"));
        assert!(
            !searched("quote", "spring \"valve sticks\""),
            "and the words outside it name the note"
        );
        assert!(searched("quote", "~eq 24"), "letters in its head");
        assert!(!searched("quote", "~bq"));
        assert!(searched("quote", "/-tue"), "nothing but what is held");
        assert!(
            !searched("quote", "e"),
            "nothing to name it by and nothing held"
        );
        assert!(!searched("quote", "~"));
        assert!(!searched("gone", "boiler"));
    }

    #[test]
    fn the_line_holds_what_it_holds_and_its_words_rank_only_among_those() {
        let today = time::day_from_civil(2026, 10, 15);
        let mut e = engine(&[
            ("tank", "Oil tank #house\ncheck the level"),
            ("gauge", "oil gauge #garden"),
            ("doc", "Heating\nthe oil tank is behind the shed"),
            ("hosting", "oil for the mower #hosting"),
            ("far", "cake recipe"),
        ]);
        let mut monday = Note::new("monday", "topped up the oil");
        monday.created = Some(time::Moment {
            ms: (i64::from(today - 3) * 86_400 + 18 * 3_600 + 30 * 60) * 1_000,
            offset: 0,
        });
        e.upsert(monday);
        let opts = SearchOptions::default();
        let find = |typed: &str| ids(&e.find(&Query::read(typed, today), &opts));
        let held = |typed: &str| {
            let mut held: Vec<&str> = e
                .holding(&Query::read(typed, today))
                .iter()
                .map(|n| n.id.as_str())
                .collect();
            held.sort_unstable();
            held
        };

        assert_eq!(find("oil #house"), ["tank"], "a tag holds");
        assert_eq!(
            held("#ho"),
            ["hosting", "tank"],
            "the tag still being typed"
        );
        assert_eq!(held("#ho "), Vec::<&str>::new(), "and once it is finished");
        assert_eq!(
            find("\"oil tank\""),
            ["tank", "doc"],
            "a phrase holds, and its words rank it in a head first"
        );
        assert_eq!(
            find("“OIL TANK"),
            ["tank", "doc"],
            "unclosed and in any case"
        );
        assert_eq!(held("\"oil tank\" #garden"), Vec::<&str>::new());
        assert_eq!(
            held("/-mon 18:00.."),
            ["monday"],
            "written since Monday 18:00"
        );
        assert_eq!(held("/-mon 18:31.."), Vec::<&str>::new());
        assert_eq!(held("..2026-10-12"), ["monday"], "to the end of the day");
        assert_eq!(
            held("..2026-10-12 18:30"),
            ["monday"],
            "to the end of the minute"
        );
        assert_eq!(held("..2026-10-12 18:29"), Vec::<&str>::new());
        assert_eq!(
            held("/-tue.."),
            Vec::<&str>::new(),
            "nor is a note with no time"
        );
        assert_eq!(
            find("topped /-mon"),
            ["monday"],
            "a day typed holds the notes of it"
        );

        // The note the line names is listed first only if it holds what the line holds.
        assert_eq!(find("oil tank")[0], "tank");
        assert_eq!(
            find("oil tank #garden"),
            ["gauge"],
            "the oil tank does not hold #garden, so it is not there to name"
        );
        let query = Query::read("oil tank #house", today);
        let named = |query: &Query| -> Vec<&str> {
            let (notes, _) = e.notes_named(query, true);
            notes.into_iter().map(|n| n.id.as_str()).collect()
        };
        assert_eq!(named(&query), ["tank"], "a head says its tag as a word");
        let query = Query::read("oil tank #garden", today);
        assert!(named(&query).is_empty());
    }

    #[test]
    fn letters_after_a_tilde_find_heads_with_them_in_order_and_say_so() {
        let today = time::day_from_civil(2026, 10, 15);
        let e = engine(&[
            ("service", "Boiler service"),
            ("absolve", "absolve the vase"),
            ("garden", "Garden shed #house"),
            ("ser", "Some easy rope"),
            ("body", "Notes\nboiler service in the body"),
        ]);
        let opts = SearchOptions::default();
        let found = e.find(&Query::read("~bsv", today), &opts);
        let mut found: Vec<(&str, f32)> = found
            .iter()
            .map(|h| (h.note.id.as_str(), h.score))
            .collect();
        found.sort_by(|a, b| b.1.total_cmp(&a.1));
        assert_eq!(found[0].0, "service", "{found:?}");
        assert!(found.iter().all(|&(id, _)| id != "body"), "heads only");
        assert!(found.iter().any(|&(id, _)| id == "absolve"));
        let mut ser = score::Letters::new("ser");
        assert!(
            ser.score("Boiler service") > ser.score("Some easy rope"),
            "a run from the start of a word outranks letters scattered over words"
        );
        let hit = &e.find(&Query::read("~bo se", today), &opts)[0];
        assert_eq!(hit.reasons[0].text, "bo, se", "every word must be there");
        assert!(
            ids(&e.find(&Query::read("~gs #house", today), &opts)) == ["garden"],
            "what is held still holds"
        );
    }

    #[test]
    fn head_words_outrank_length_normalisation() {
        let long_body = "lorem ipsum dolor sit amet ".repeat(400);
        let e = engine(&[
            ("page", &format!("Horticultural Trials\n{long_body}")),
            ("journal", "Journal\nread about horticultural trials today"),
            ("other", "the the the"),
        ]);
        let hits = search(&e, "horticultural trials", &SearchOptions::default());
        assert_eq!(ids(&hits)[0], "page");
        assert!(has_reason(
            &hits[0],
            "explicit",
            "in head: horticultural, trials"
        ));
    }

    #[test]
    fn the_word_being_typed_matches_as_a_prefix() {
        let long_body = "lorem ipsum dolor sit amet ".repeat(400);
        let e = engine(&[
            ("page", &format!("House/Event Publication\n{long_body}")),
            ("other", "events are fun"),
            ("kb", "kettlebells log"),
        ]);
        let hits = search(&e, "event publicatio", &SearchOptions::default());
        assert_eq!(ids(&hits)[0], "page");
        assert!(
            has_reason(&hits[0], "explicit", "in head: event, publication"),
            "{:?}",
            hits[0].reasons
        );
        assert!(has_reason(&hits[0], "words", "publication"));
        let hits = search(&e, "kettl", &SearchOptions::default());
        assert_eq!(ids(&hits), ["kb"]);
        assert!(
            search(&e, "kettl ", &SearchOptions::default()).is_empty(),
            "a trailing space makes it a whole word"
        );
    }

    #[test]
    fn a_notes_neighbourhood_leaves_it_out() {
        let e = engine(&[("a", "alpha beta"), ("b", "alpha gamma"), ("c", "delta")]);
        let hits = near(&e, "a", &SearchOptions::default());
        assert_eq!(ids(&hits), ["b"]);
        assert!(near(&e, "missing", &SearchOptions::default()).is_empty());
    }

    #[test]
    fn dates_connect_notes() {
        let mut e = engine(&[("a", "dentist on 2026-09-05"), ("b", "nothing here")]);
        let mut c = Note::new("c", "written that day");
        c.created = on(time::day_from_civil(2026, 9, 5));
        e.upsert(c);
        e.upsert(Note::new("d", "05 things to do in 2026, on the 09 bus"));
        let hits = search(&e, "2026-09-05", &SearchOptions::default());
        assert_eq!(ids(&hits).len(), 2, "the numbers in a date are not words");
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
        let hits = search(
            &e,
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
        assert!(search(&e, "kettlebell", &SearchOptions::default()).is_empty());
        assert_eq!(ids(&search(&e, "cake", &SearchOptions::default())), ["a"]);
        assert_eq!(e.len(), 2);
        assert!(e.remove("a").is_some());
        assert!(e.remove("a").is_none());
        assert!(search(&e, "cake", &SearchOptions::default()).is_empty());
        assert_eq!(e.len(), 1);
        e.upsert(Note::new("c", "cake again"));
        assert_eq!(ids(&search(&e, "cake", &SearchOptions::default())), ["c"]);
    }

    #[test]
    fn archived_notes_are_included_unless_asked_otherwise() {
        let mut e = Engine::new();
        let mut n = Note::new("a", "kettlebell swings");
        n.archived = true;
        e.upsert(n);
        assert_eq!(search(&e, "kettlebell", &SearchOptions::default()).len(), 1);
        assert!(
            search(
                &e,
                "kettlebell",
                &SearchOptions {
                    include_archived: false,
                    ..Default::default()
                }
            )
            .is_empty()
        );
    }
}
