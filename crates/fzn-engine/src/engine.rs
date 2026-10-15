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
    /// Whether the note is linked to or from the one being looked around, or carries the labels
    /// being looked around. That is a fact, where the rest is a guess, and such notes are
    /// returned whatever they score and whatever the limit.
    pub linked: bool,
    /// Whether the note's head is what was typed. Such notes come first, whatever they score
    /// and whatever the limit.
    pub named: bool,
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

    /// What the input relates to, as it is being typed. The last word counts as a prefix unless
    /// the input ends in whitespace. A note headed with what was typed is the note being named,
    /// and comes first whatever it scores and whatever the limit.
    pub fn search(&self, text: &str, opts: &SearchOptions) -> Vec<Hit<'_>> {
        self.typed(text, opts, &[])
    }

    /// `search`, with the notes in `carrying`, which is sorted, listed as facts whatever they
    /// score and whatever the limit.
    fn typed(&self, text: &str, opts: &SearchOptions, carrying: &[DocId]) -> Vec<Hit<'_>> {
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
        self.run(&ctx, opts, &self.headed(text), carrying)
    }

    /// The notes whose head is what was typed, sorted: every word of the head was typed and no
    /// other word was. The last word typed stands for the one head word it starts, unless
    /// whitespace follows it.
    fn headed(&self, text: &str) -> Vec<DocId> {
        let mut words = text::tokens(text);
        let last = if text.ends_with(char::is_whitespace) {
            None
        } else {
            words.pop()
        };
        words.sort_unstable();
        words.dedup();
        let called = |words: &[String]| self.index.docs_with(&Label::Name(words.join(" ")));
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

    /// Whether a note is named by what was typed: every word of it is in the note's head or in
    /// the head of a note it links to, the word still being typed as the start of one, and every
    /// date is a day the note mentions or was written on. The app asks this to tell a search
    /// that found the note from something to be written in it.
    pub fn named_by(&self, id: &NoteId, text: &str) -> bool {
        let Some(doc) = self
            .by_id
            .get(id)
            .and_then(|&d| self.docs[d as usize].as_ref())
        else {
            return false;
        };
        let (terms, prefix) = typed_words(text);
        if terms.is_empty() && prefix.is_none() {
            return false;
        }
        let mut names = text::tokens(doc.note.head());
        for target in doc.note.links.iter().filter_map(|l| self.get(l)) {
            names.extend(text::tokens(target.head()));
        }
        terms.iter().all(|t| names.contains(t))
            && prefix.is_none_or(|p| names.iter().any(|n| n.starts_with(&p)))
            && text::facts(text)
                .dates
                .iter()
                .all(|day| doc.facts.dates.contains(day) || doc.note.created == Some(*day))
    }

    /// A note's head and the notes that link to it.
    fn named(&self, d: DocId) -> Option<(&str, &[DocId])> {
        let doc = self.docs.get(d as usize)?.as_ref()?;
        let linked = self
            .index
            .docs_with(&Label::Link(doc.note.id.as_str().to_owned()));
        Some((doc.note.head(), linked))
    }

    /// The neighbourhood of an existing note: what is linked to or from it, marked as such, and
    /// what the engine finds besides.
    pub fn related(&self, id: &NoteId, opts: &SearchOptions) -> Vec<Hit<'_>> {
        let Some(&d) = self.by_id.get(id) else {
            return Vec::new();
        };
        let Some(doc) = self.docs[d as usize].as_ref() else {
            return Vec::new();
        };
        let mut links_in = self
            .index
            .docs_with(&Label::Link(id.as_str().to_owned()))
            .to_vec();
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

        let terms = self.top_terms(d, 20);
        let mut labels = query_labels(&terms, &doc.facts);
        // Notes linked to the same things this one is are its siblings: a suggestion.
        labels.extend(
            links_out
                .iter()
                .filter_map(|&t| self.docs[t as usize].as_ref())
                .map(|target| QueryLabel::LinksTo {
                    id: target.note.id.as_str().to_owned(),
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
            links_in: &links_in,
            links_out: &links_out,
            named: None,
        };
        self.run(&ctx, opts, &[], &[])
    }

    /// The neighbourhood of tags, people and days taken together, which `written` is as they
    /// would be typed: every note carrying all of them, marked as linked and listed whatever it
    /// scores and whatever the limit, and what the engine finds besides for those words. The
    /// last day among them is also the day nearness in time is measured from, as a note's own
    /// day is in `related`, so that a note written on it says the same date and not how far it
    /// is from today.
    pub(crate) fn labelled(
        &self,
        labels: &[Label],
        written: &str,
        opts: &SearchOptions,
    ) -> Vec<Hit<'_>> {
        let Some((first, rest)) = labels.split_first() else {
            return Vec::new();
        };
        let mut carrying = self.index.docs_with(first).to_vec();
        for label in rest {
            let with = self.index.docs_with(label);
            carrying.retain(|d| with.binary_search(d).is_ok());
        }
        let opts = SearchOptions {
            today: labels
                .iter()
                .rev()
                .find_map(|label| match label {
                    Label::Date(day) => Some(*day),
                    _ => None,
                })
                .or(opts.today),
            ..opts.clone()
        };
        // Whitespace after it, so that the last label's word is a whole word and not the start
        // of one.
        self.typed(&format!("{written} "), &opts, &carrying)
    }

    /// The one note whose head is what was typed, by the rule that lists a name first in
    /// `search`, if exactly one is. The app goes into it from the line.
    pub(crate) fn the_note_named(&self, text: &str, include_archived: bool) -> Option<&Note> {
        let mut named = self
            .headed(text)
            .into_iter()
            .filter_map(|d| self.docs[d as usize].as_ref())
            .map(|doc| &doc.note)
            .filter(|note| include_archived || !note.archived);
        let one = named.next()?;
        named.next().is_none().then_some(one)
    }

    /// The notes a text names in `[[brackets]]`, by their heads. Brackets are a way of typing a
    /// link and nothing more: the link itself is the id, held on the note that was saved.
    pub fn named_in(&self, text: &str) -> Vec<NoteId> {
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
    pub fn links_on_save(
        &self,
        id: &NoteId,
        old: &str,
        new: &str,
        links: &[NoteId],
    ) -> Vec<NoteId> {
        let (before, after) = (self.named_in(old), self.named_in(new));
        let mut links: Vec<NoteId> = links
            .iter()
            .filter(|l| !before.contains(l) || after.contains(l))
            .cloned()
            .collect();
        for l in after {
            if l != *id && !links.contains(&l) {
                links.push(l);
            }
        }
        links
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

    /// Scores the candidates and lists them best first, after the notes in `first`, which is
    /// sorted: those are the notes being named, and they are listed whatever they score. So are
    /// the notes in `carrying`, also sorted, which carry the labels being looked around and are
    /// facts of it, as what is linked is of a note.
    fn run<'a: 'c, 'c>(
        &'a self,
        ctx: &Context<'c>,
        opts: &SearchOptions,
        first: &[DocId],
        carrying: &[DocId],
    ) -> Vec<Hit<'a>> {
        let mut c = Candidates::default();
        for &d in first.iter().chain(carrying) {
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
        let link = |d: DocId| {
            let (to_here, from_here) = (
                ctx.links_in.binary_search(&d).is_ok(),
                ctx.links_out.binary_search(&d).is_ok(),
            );
            match (to_here, from_here) {
                (true, true) => Some("linked both ways"),
                (true, false) => Some("links here"),
                (false, true) => Some("linked from here"),
                (false, false) => None,
            }
        };
        let named = |d: DocId| first.binary_search(&d).is_ok();
        let fact = |d: DocId| link(d).is_some() || carrying.binary_search(&d).is_ok();
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
                match link(d) {
                    Some(text) => reasons.insert(
                        0,
                        Reason {
                            scorer: "link",
                            text: text.to_owned(),
                        },
                    ),
                    None if named(d) || fact(d) => {}
                    // The limit is on suggestions. What is a fact or named is always there.
                    None => {
                        suggestions += 1;
                        if suggestions > opts.limit {
                            return None;
                        }
                    }
                }
                if named(d) {
                    reasons.insert(
                        0,
                        Reason {
                            scorer: "name",
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
    labels.extend(
        note.links
            .iter()
            .map(|l| Label::Link(l.as_str().to_owned())),
    );
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
    labels.extend(head_terms.into_iter().map(Label::HeadTerm));
    labels.extend(facts.dates.iter().copied().map(Label::Date));
    if let Some(d) = note.created
        && !facts.dates.contains(&d)
    {
        labels.push(Label::Date(d));
    }
    labels
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
    fn brackets_are_a_way_of_typing_a_link() {
        let mut e = engine(&[
            ("prog", "Kettlebell Programme\nweek one: swings"),
            (
                "mon",
                "Monday\nfollow [[kettlebell programme]] and [[Nowhere]]",
            ),
            ("tue", "Tuesday\nrest"),
        ]);
        let id = NoteId::new;
        assert_eq!(
            e.named_in("see [[Kettlebell Programme|the plan]]"),
            [id("prog")]
        );
        assert!(e.named_in("[[Nowhere]] and no brackets").is_empty());
        assert!(
            e.related(&id("prog"), &SearchOptions::default())
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
        n.links.push(NoteId::new(target));
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
            let mut ids: Vec<String> = e
                .related(&NoteId::new(id), &no_suggestions)
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
        let around = e.related(&NoteId::new("hub"), &SearchOptions::default());
        let hit = |id: &str| around.iter().find(|h| h.note.id.as_str() == id).unwrap();
        assert!(has_reason(hit("child"), "link", "links here"));
        assert!(has_reason(hit("profile"), "link", "linked from here"));
        assert!(!hit("near").linked, "shared words are a suggestion");
        assert!(around.iter().all(|h| h.note.id.as_str() != "far"));

        // From a child, the hub is linked and the other child is a suggestion with its reason.
        let around = e.related(&NoteId::new("child"), &SearchOptions::default());
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
        let found = e.search("boiler ", &opts);
        assert_eq!(found[0].note.id.as_str(), "boiler");
        let mut all = ids(&found);
        all.sort_unstable();
        assert_eq!(all, ["boiler", "pressure", "quote", "says"]);
        let hit = |id: &str| found.iter().find(|h| h.note.id.as_str() == id).unwrap();
        assert!(has_reason(hit("pressure"), "explicit", "about Boiler"));
        assert!(has_reason(hit("boiler"), "explicit", "in head: boiler"));
        assert!(!has_reason(hit("says"), "explicit", "about"));

        // A word of the note and the name of what it is about find it together.
        let found = e.search("boiler quoted ", &opts);
        assert_eq!(found[0].note.id.as_str(), "quote");
        assert!(has_reason(
            &found[0],
            "explicit",
            "in head: quoted, about Boiler"
        ));

        // The word still being typed stands for the name it starts.
        assert!(ids(&e.search("boi", &opts)).contains(&"pressure"));

        // One hop: the house finds the boiler, and not what was written about the boiler.
        assert_eq!(ids(&e.search("house ", &opts)), ["house", "boiler"]);

        // The name is read when it is asked for, so a rename is followed.
        e.upsert(linked_to("boiler", "Heating", "house"));
        assert!(ids(&e.search("heating ", &opts)).contains(&"pressure"));
        assert_eq!(ids(&e.search("boiler ", &opts)), ["says"]);
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
        let first = |typed: &str| e.search(typed, &opts)[0].note.id.to_string();
        let named = |typed: &str| -> Vec<String> {
            e.search(typed, &opts)
                .iter()
                .filter(|h| has_reason(h, "name", "its name"))
                .map(|h| h.note.id.to_string())
                .collect()
        };

        let found = e.search("boiler service", &opts);
        assert_eq!(ids(&found)[0], "doc");
        assert!(
            found[0].score < found[1].score,
            "first for its name, where its score would not put it"
        );
        assert_eq!(found[0].reasons[0].scorer, "name");
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
        assert_eq!(ids(&e.search("boiler service", &none)), ["doc"]);
        let mut doc = e.get(&NoteId::new("doc")).unwrap().clone();
        doc.archived = true;
        e.upsert(doc);
        let live = SearchOptions {
            include_archived: false,
            ..Default::default()
        };
        assert!(!ids(&e.search("boiler service", &live)).contains(&"doc"));

        // A neighbourhood has no name in it to go first.
        let around = e.related(&NoteId::new("a"), &opts);
        assert!(around.iter().all(|h| !has_reason(h, "name", "")));
    }

    #[test]
    fn what_was_typed_names_a_note_by_its_head_or_by_what_it_links_to() {
        let mut e = engine(&[("house", "House"), ("far", "cake recipe")]);
        e.upsert(linked_to(
            "boiler",
            "Boiler\nserviced in the spring",
            "house",
        ));
        let mut quote = linked_to("quote", "engineer quoted 240", "boiler");
        quote.created = Some(time::day_from_civil(2026, 10, 13));
        e.upsert(quote);
        let named = |id: &str, typed: &str| e.named_by(&NoteId::new(id), typed);

        assert!(named("quote", "boiler"));
        assert!(named("quote", "boi"), "the word still being typed");
        assert!(named("quote", "boiler quoted"));
        assert!(named("quote", "Quoted BOILER"));
        assert!(!named("quote", "boi quoted"), "a finished word is whole");
        assert!(!named("quote", "boiler valve"));
        assert!(!named("quote", "house"), "one hop");
        assert!(!named("boiler", "quoted"), "the name goes one way");
        assert!(!named("boiler", "spring"), "the head, and not the body");
        assert!(named("quote", "boiler 2026-10-13"), "written that day");
        assert!(!named("quote", "boiler 2026-10-14"));
        assert!(!named("quote", ""));
        assert!(!named("gone", "boiler"));
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
    fn the_word_being_typed_matches_as_a_prefix() {
        let long_body = "lorem ipsum dolor sit amet ".repeat(400);
        let e = engine(&[
            ("page", &format!("House/Event Publication\n{long_body}")),
            ("other", "events are fun"),
            ("kb", "kettlebells log"),
        ]);
        let hits = e.search("event publicatio", &SearchOptions::default());
        assert_eq!(ids(&hits)[0], "page");
        assert!(
            has_reason(&hits[0], "explicit", "in head: event, publication"),
            "{:?}",
            hits[0].reasons
        );
        assert!(has_reason(&hits[0], "words", "publication"));
        let hits = e.search("kettl", &SearchOptions::default());
        assert_eq!(ids(&hits), ["kb"]);
        assert!(
            e.search("kettl ", &SearchOptions::default()).is_empty(),
            "a trailing space makes it a whole word"
        );
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
        e.upsert(Note::new("d", "05 things to do in 2026, on the 09 bus"));
        let hits = e.search("2026-09-05", &SearchOptions::default());
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
