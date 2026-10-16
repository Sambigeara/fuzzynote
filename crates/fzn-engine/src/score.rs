//! Scorers and their reasons. Each scorer contributes roughly `[0, 1]` plus a short reason, and
//! `Weights` decides how the contributions combine.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::index::{DocId, Index, TermId};
use crate::time::{self, Day};

/// Explicit structure a note carries, indexed for exact lookup, and in order for a tag or a
/// person still being typed.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Label {
    Tag(String),
    Mention(String),
    /// The id of a note this one is linked to.
    Link(String),
    /// The note's own head, normalised, so that a `[[head]]` typed into a text can be resolved
    /// to it when the text is saved.
    Head(String),
    /// A word in the note's head.
    HeadTerm(String),
    /// The words of the note's head, in alphabetical order with a space between: what the note
    /// is called, in whatever order and case that is typed.
    Name(String),
    /// The same, for a head that ends with labels, of its words without them: what its row
    /// reads in a place that says them. A name of the second tier, asked for only when no head
    /// is what was typed.
    Called(String),
    /// A day the note was created on or mentions.
    Date(Day),
    /// The note is an open task: its head starts with `[ ]`, and it is not archived.
    Open,
}

impl Label {
    /// Whether this label is one that `start`, typed so far, could be: a tag or a person whose
    /// name starts with its name. Any other label starts only itself.
    pub fn starts_with(&self, start: &Label) -> bool {
        match (self, start) {
            (Label::Tag(name), Label::Tag(start))
            | (Label::Mention(name), Label::Mention(start)) => name.starts_with(start.as_str()),
            _ => self == start,
        }
    }
}

/// What a query asks for explicitly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QueryLabel {
    Tag(String),
    Mention(String),
    /// Notes linked to the note with this id. The head says which one in the reason.
    LinksTo {
        id: String,
        head: String,
    },
    /// Notes with this word in their head.
    HeadTerm(String),
    /// Notes whose head has a word starting with this, the word still being typed.
    HeadPrefix(String),
}

/// Tunable weights. The defaults are a starting point, not a conclusion.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Weights {
    /// Weight of the lexical (BM25) scorer.
    pub lexical: f32,
    /// Weight of the explicit scorer: tags, mentions, links.
    pub explicit: f32,
    /// Weight of the temporal scorer: shared dates and proximity in time.
    pub temporal: f32,
    /// Multiplier on a fuzzy term expansion relative to an exact term.
    pub fuzzy: f32,
    /// Multiplier on a word found in the head of a note that a note links to, relative to one
    /// found in its own head. Below 1, so that a name finds the note it belongs to first.
    pub about: f32,
    /// Minimum trigram similarity for a vocabulary term to count as a fuzzy match.
    pub fuzzy_min_similarity: f32,
    pub bm25_k1: f32,
    pub bm25_b: f32,
    /// Days over which the temporal proximity bonus halves.
    pub halflife_days: f32,
}

impl Default for Weights {
    fn default() -> Self {
        Self {
            lexical: 1.0,
            explicit: 0.8,
            temporal: 0.3,
            fuzzy: 0.7,
            about: 0.8,
            fuzzy_min_similarity: 0.5,
            bm25_k1: 1.2,
            bm25_b: 0.75,
            halflife_days: 30.0,
        }
    }
}

/// Why a note was suggested.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reason {
    pub scorer: Scorer,
    pub text: String,
}

/// What gave a reason: a scorer, or a fact. `Link`, for a note linked to or from the one being
/// looked around, and `Name`, for a note headed with what was typed, are facts and not scores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scorer {
    Words,
    Explicit,
    Time,
    Letters,
    Link,
    Name,
}

impl std::fmt::Display for Scorer {
    /// The scorer's name as a reason credits it: `words`, `explicit`, `time` or `letters`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Scorer::Words => "words",
            Scorer::Explicit => "explicit",
            Scorer::Time => "time",
            Scorer::Letters => "letters",
            Scorer::Link => "link",
            Scorer::Name => "name",
        })
    }
}

#[derive(Default)]
pub(crate) struct Partial {
    lexical: f32,
    explicit: f32,
    temporal: f32,
    reasons: Vec<Reason>,
}

#[derive(Default)]
pub(crate) struct Candidates(HashMap<DocId, Partial>);

impl Candidates {
    fn entry(&mut self, doc: DocId) -> &mut Partial {
        self.0.entry(doc).or_default()
    }

    /// Makes a note a candidate whatever it goes on to score.
    pub(crate) fn include(&mut self, doc: DocId) {
        self.entry(doc);
    }
}

pub(crate) struct Context<'a> {
    /// Complete query words.
    pub terms: &'a [String],
    /// The word still being typed, if any. It matches every vocabulary word it starts.
    pub prefix: Option<&'a str>,
    pub labels: &'a [QueryLabel],
    pub dates: &'a [Day],
    /// The day to measure proximity from: today when typing, the note's own day around a note.
    pub day: Option<Day>,
    pub exclude: Option<DocId>,
    /// Notes that link to the note being looked around, sorted.
    pub links_in: &'a [DocId],
    /// Notes it links to, sorted.
    pub links_out: &'a [DocId],
    /// Where this is given, a note is found by the head of a note it links to as it is by its
    /// own. It is given for what is typed, and not for a neighbourhood, where what is linked is
    /// listed already.
    pub named: Option<&'a Named<'a>>,
}

/// For a note, its head and the notes that link to it.
pub(crate) type Named<'a> = dyn Fn(DocId) -> Option<(&'a str, &'a [DocId])> + 'a;

/// How many completions a prefix may expand to. Rare completions carry the most information,
/// so the cap is generous; a note earns credit for its best completion only.
pub(crate) const PREFIX_EXPANSIONS: usize = 512;

/// A vocabulary term standing in for a query word, with the credit it earns relative to an
/// exact match and the text that explains it.
struct Member {
    term: TermId,
    weight: f32,
    label: String,
}

/// One query word and everything it may stand for: itself, its completions if it is still
/// being typed, and words that look like it. A note earns credit for the best member only.
fn group(index: &Index, word: &str, is_prefix: bool, w: &Weights) -> Vec<Member> {
    let mut members = Vec::new();
    let exact = index.term_id(word);
    if let Some(t) = exact {
        members.push(Member {
            term: t,
            weight: 1.0,
            label: word.to_owned(),
        });
    }
    if is_prefix && word.chars().count() >= 2 {
        for t in index.prefix_terms(word, PREFIX_EXPANSIONS) {
            if exact != Some(t) {
                members.push(Member {
                    term: t,
                    weight: 1.0,
                    label: index.term_name(t).to_owned(),
                });
            }
        }
    }
    if word.chars().count() >= 4 {
        for (t, sim) in index.fuzzy_terms(word, 3, w.fuzzy_min_similarity) {
            if exact != Some(t) && !members.iter().any(|m| m.term == t) {
                members.push(Member {
                    term: t,
                    weight: w.fuzzy * sim,
                    label: format!("{word}≈{}", index.term_name(t)),
                });
            }
        }
    }
    members
}

/// BM25 over query words, each standing for its exact term, completions and look-alikes,
/// normalised so that a note matching every word strongly scores about 1.
pub(crate) fn lexical(index: &Index, ctx: &Context, w: &Weights, c: &mut Candidates) {
    let mut seen: Vec<&str> = Vec::new();
    let mut groups: Vec<Vec<Member>> = Vec::new();
    let words = ctx
        .terms
        .iter()
        .map(|t| (t.as_str(), false))
        .chain(ctx.prefix.map(|p| (p, true)));
    for (word, is_prefix) in words {
        if seen.contains(&word) {
            continue;
        }
        seen.push(word);
        let members = group(index, word, is_prefix, w);
        if !members.is_empty() {
            groups.push(members);
        }
    }
    let (k1, b) = (w.bm25_k1, w.bm25_b);
    let avg = index.avg_doc_len().max(1.0);
    let max_possible: f32 = groups
        .iter()
        .map(|g| {
            g.iter()
                .map(|m| m.weight * index.idf(m.term) * (k1 + 1.0))
                .fold(0.0, f32::max)
        })
        .sum();
    if max_possible <= 0.0 {
        return;
    }
    let mut matched: HashMap<DocId, Vec<(f32, String)>> = HashMap::new();
    for members in &groups {
        let mut best: HashMap<DocId, (f32, usize)> = HashMap::new();
        for (mi, m) in members.iter().enumerate() {
            let idf = index.idf(m.term);
            for &(doc, tf) in index.postings(m.term) {
                if Some(doc) == ctx.exclude {
                    continue;
                }
                let tf = tf as f32;
                let dl = index.doc_len(doc) as f32;
                let s = m.weight * idf * tf * (k1 + 1.0) / (tf + k1 * (1.0 - b + b * dl / avg));
                let e = best.entry(doc).or_insert((0.0, mi));
                if s > e.0 {
                    *e = (s, mi);
                }
            }
        }
        for (doc, (s, mi)) in best {
            let contribution = s / max_possible;
            c.entry(doc).lexical += contribution;
            matched
                .entry(doc)
                .or_default()
                .push((contribution, members[mi].label.clone()));
        }
    }
    for (doc, mut m) in matched {
        m.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        let names: Vec<&str> = m.iter().take(3).map(|(_, label)| label.as_str()).collect();
        let more = m.len().saturating_sub(3);
        let text = if more > 0 {
            format!("{} +{more}", names.join(", "))
        } else {
            names.join(", ")
        };
        c.entry(doc).reasons.push(Reason {
            scorer: Scorer::Words,
            text,
        });
    }
}

fn add_explicit(
    hits: &mut HashMap<DocId, (f32, Vec<String>)>,
    exclude: Option<DocId>,
    docs: &[DocId],
    weight: f32,
    text: &str,
) {
    for &d in docs {
        if Some(d) == exclude {
            continue;
        }
        let e = hits.entry(d).or_insert_with(|| (0.0, Vec::new()));
        e.0 += weight;
        e.1.push(text.to_owned());
    }
}

/// Where a query word was found for a note.
#[derive(Clone, Copy)]
enum InHead<'a> {
    /// In its own head: the word.
    Own(&'a str),
    /// In the head of a note it links to: that head.
    Of(&'a str),
}

/// Offers the credit for one query word to every note with `word` in its head and, where the
/// query asks for it, a share of it to every note linked to one of those. A note keeps the best
/// credit it is offered for a query word.
fn offer_head<'a>(
    index: &Index,
    ctx: &Context<'a>,
    word: &'a str,
    weight: f32,
    about: f32,
    best: &mut HashMap<DocId, (f32, InHead<'a>)>,
) {
    let mut offer = |doc: DocId, weight: f32, found: InHead<'a>| {
        let held = best.get(&doc).map_or(0.0, |&(held, _)| held);
        if Some(doc) != ctx.exclude && weight > held {
            best.insert(doc, (weight, found));
        }
    };
    for &d in index.docs_with(&Label::HeadTerm(word.to_owned())) {
        offer(d, weight, InHead::Own(word));
        if let Some((head, linked)) = ctx.named.and_then(|named| named(d)) {
            for &other in linked {
                if other != d {
                    offer(other, weight * about, InHead::Of(head));
                }
            }
        }
    }
}

/// Shared tags, mentions, link targets and head words. Three shared labels saturate the score. Head
/// words count as the fraction of the query, by rarity, that appears in the head, scaled by how
/// specific the query is, so a note titled with the query outranks a one-liner that mentions it.
/// Where the query asks for it, the head of a note that a note links to counts nearly as its own
/// does: a note written about another is found by that note's name without having to say it.
pub(crate) fn explicit<'a>(index: &'a Index, ctx: &Context<'a>, w: &Weights, c: &mut Candidates) {
    let mut hits: HashMap<DocId, (f32, Vec<String>)> = HashMap::new();
    let mut head_hits: HashMap<DocId, (f32, Vec<InHead>)> = HashMap::new();
    let idf_max = ((index.live_docs() as f32) + 1.0).ln().max(f32::EPSILON);
    let mut query_weight = 0.0;
    for l in ctx.labels {
        // What each note earns for this query word, if it is one.
        let mut best: HashMap<DocId, (f32, InHead)> = HashMap::new();
        match l {
            QueryLabel::HeadTerm(t) => {
                let Some(term) = index.term_id(t) else {
                    continue;
                };
                let weight = index.idf(term);
                query_weight += weight;
                offer_head(index, ctx, t, weight, w.about, &mut best);
            }
            QueryLabel::HeadPrefix(p) => {
                // One credit for the word being typed: the best completion found for each note.
                let completions = index.prefix_terms(p, PREFIX_EXPANSIONS);
                let Some(group_weight) = completions.iter().map(|&t| index.idf(t)).reduce(f32::max)
                else {
                    continue;
                };
                query_weight += group_weight;
                for &t in &completions {
                    offer_head(
                        index,
                        ctx,
                        index.term_name(t),
                        index.idf(t),
                        w.about,
                        &mut best,
                    );
                }
            }
            QueryLabel::Tag(t) => add_explicit(
                &mut hits,
                ctx.exclude,
                index.docs_with(&Label::Tag(t.clone())),
                1.0,
                &format!("#{t}"),
            ),
            QueryLabel::Mention(m) => add_explicit(
                &mut hits,
                ctx.exclude,
                index.docs_with(&Label::Mention(m.clone())),
                1.0,
                &format!("@{m}"),
            ),
            QueryLabel::LinksTo { id, head } => add_explicit(
                &mut hits,
                ctx.exclude,
                index.docs_with(&Label::Link(id.clone())),
                1.0,
                &format!("also links {head}"),
            ),
        }
        for (d, (weight, found)) in best {
            let e = head_hits.entry(d).or_insert_with(|| (0.0, Vec::new()));
            e.0 += weight;
            e.1.push(found);
        }
    }
    // What is linked to or from the note being looked around. The engine states it as a
    // reason of its own, so it carries weight here and no text.
    for (docs, weight) in [(ctx.links_out, 2.0), (ctx.links_in, 1.0)] {
        for &d in docs {
            if Some(d) != ctx.exclude {
                hits.entry(d).or_insert_with(|| (0.0, Vec::new())).0 += weight;
            }
        }
    }
    let specificity = (query_weight / idf_max).min(1.0);
    for (doc, (sum, found)) in head_hits {
        let fraction = if query_weight > 0.0 {
            sum / query_weight
        } else {
            0.0
        };
        let e = hits.entry(doc).or_insert_with(|| (0.0, Vec::new()));
        e.0 += 3.0 * fraction * specificity;
        let (mut own, mut of): (Vec<&str>, Vec<&str>) = (Vec::new(), Vec::new());
        for f in found {
            match f {
                InHead::Own(word) => own.push(word),
                InHead::Of(head) if !of.contains(&head) => of.push(head),
                InHead::Of(_) => {}
            }
        }
        if !own.is_empty() {
            e.1.push(format!("in head: {}", own.join(", ")));
        }
        e.1.extend(of.iter().map(|head| format!("about {head}")));
    }
    for (doc, (sum, texts)) in hits {
        let p = c.entry(doc);
        p.explicit += (sum / 3.0).min(1.0);
        if !texts.is_empty() {
            p.reasons.push(Reason {
                scorer: Scorer::Explicit,
                text: texts.join(", "),
            });
        }
    }
}

fn describe_gap(days: i32) -> String {
    match days {
        0 => "same day".into(),
        1 => "a day apart".into(),
        d if d < 31 => format!("{d} days apart"),
        d if d < 60 => "a month apart".into(),
        d if d < 365 => format!("{} months apart", d / 30),
        d if d < 730 => "a year apart".into(),
        d => format!("{} years apart", d / 365),
    }
}

/// Shared dates add candidates; proximity in time re-ranks what is already there.
pub(crate) fn temporal(
    index: &Index,
    day_of: &dyn Fn(DocId) -> Option<Day>,
    ctx: &Context,
    w: &Weights,
    c: &mut Candidates,
) {
    // A date shared with the query that is also the query's own day is one coincidence, not two:
    // such a note gets the shared-date credit and not the proximity bonus as well.
    let mut same_day: Vec<DocId> = Vec::new();
    for &d in ctx.dates {
        for &doc in index.docs_with(&Label::Date(d)) {
            if Some(doc) == ctx.exclude {
                continue;
            }
            if Some(d) == ctx.day && day_of(doc) == Some(d) {
                same_day.push(doc);
            }
            let p = c.entry(doc);
            p.temporal += 0.5;
            p.reasons.push(Reason {
                scorer: Scorer::Time,
                text: format!("same date {}", time::format_day(d)),
            });
        }
    }
    if let Some(day) = ctx.day {
        same_day.sort_unstable();
        for (&doc, p) in c.0.iter_mut() {
            if same_day.binary_search(&doc).is_ok() {
                continue;
            }
            let Some(d) = day_of(doc) else { continue };
            let delta = (d - day).abs();
            let bonus = 0.5 * 0.5f32.powf(delta as f32 / w.halflife_days.max(1.0));
            if bonus < 0.02 {
                continue;
            }
            p.temporal += bonus;
            p.reasons.push(Reason {
                scorer: Scorer::Time,
                text: describe_gap(delta),
            });
        }
    }
    for p in c.0.values_mut() {
        p.temporal = p.temporal.min(1.0);
    }
}

/// What letters typed after `~` earn in a head, as fzf scores them: a letter found, one at the
/// start of a word of the head, a letter running straight on from the one before, and what a
/// gap between two letters costs, its first letter skipped and each one after.
const LETTER: i32 = 16;
const WORD_START: i32 = 8;
const RUN: i32 = 4;
const GAP: i32 = 3;
const GAP_ON: i32 = 1;

/// The letters typed after `~`, a word of them at a time, lowercased, and room to score them.
pub(crate) struct Letters {
    words: Vec<Vec<char>>,
    /// For each place in a head, the best score with the letters so far ending there, and what
    /// a letter running on from it earns. Kept between heads, so that a head is scored without
    /// allocating.
    best: Vec<Option<(i32, i32)>>,
}

impl Letters {
    pub(crate) fn new(typed: &str) -> Self {
        Self {
            words: typed
                .split_whitespace()
                .map(|word| word.chars().map(lower).collect())
                .collect(),
            best: Vec::new(),
        }
    }

    /// What the letters score in a head, every word of them, or None when one is not there.
    pub(crate) fn score(&mut self, head: &str) -> Option<i32> {
        let mut score = 0;
        for word in &self.words {
            score += letters(head, word, &mut self.best)?;
        }
        Some(score)
    }
}

/// What the letters of `word` score in `head`, or None when they are not all in it in order, as
/// the original fzn's fuzzy found them. They rank as fzf ranks them: a letter at the start of a
/// word earns more, twice as much for the first letter typed; a letter running on from the one
/// before earns at least what the first letter of its run did; and a gap costs more the longer
/// it is. The best placing of the letters is the one scored, so that `bsv` in "Boiler service" is
/// the `b` of Boiler and the `s` of service.
fn letters(head: &str, word: &[char], best: &mut Vec<Option<(i32, i32)>>) -> Option<i32> {
    // Most heads have not got the letters at all, and saying so takes one pass.
    let mut wanted = word.iter().peekable();
    for c in head.chars() {
        wanted.next_if(|&&w| w == lower(c));
    }
    if wanted.peek().is_some() {
        return None;
    }
    let (&first, rest) = word.split_first()?;
    let Some((&last, between)) = rest.split_last() else {
        return places(head)
            .filter(|&(c, _)| c == first)
            .map(|(_, start)| LETTER + 2 * start)
            .max();
    };
    best.clear();
    best.extend(
        places(head).map(|(c, start)| (c == first).then(|| (LETTER + 2 * start, start.max(RUN)))),
    );
    for &w in between {
        next_letter(head, w, best, |at, score| *at = score);
    }
    let mut top = None;
    next_letter(head, last, best, |_, score| {
        top = top.max(score.map(|(s, _)| s))
    });
    top
}

/// Places the next letter after those in `best`, giving each place in the head what it scores
/// there to `keep`, along with that place's cell in `best`.
fn next_letter(
    head: &str,
    w: char,
    best: &mut [Option<(i32, i32)>],
    mut keep: impl FnMut(&mut Option<(i32, i32)>, Option<(i32, i32)>),
) {
    // The best from two or more places back, less what the gap from there costs, and what the
    // places one and two back held for the letters before this one.
    let (mut before, mut last, mut skipped) = (None::<i32>, None, None);
    for ((c, start), at) in places(head).zip(best.iter_mut()) {
        let held = *at;
        before = before
            .map(|s| s - GAP_ON)
            .max(skipped.map(|(s, _): (i32, i32)| s - GAP));
        let here = (c == w)
            .then(|| {
                let run = last.map(|(s, on): (i32, i32)| (s + LETTER + on.max(start), on));
                let gap = before.map(|s| (s + LETTER + start, start.max(RUN)));
                run.max(gap)
            })
            .flatten();
        keep(at, here);
        skipped = last;
        last = held;
    }
}

/// Each letter of a head, lowercased, with what it earns for starting a word.
fn places(head: &str) -> impl Iterator<Item = (char, i32)> + '_ {
    let mut in_word = false;
    head.chars().map(move |c| {
        let start = if in_word { 0 } else { WORD_START };
        in_word = c.is_alphanumeric();
        (lower(c), start)
    })
}

fn lower(c: char) -> char {
    if c.is_ascii() {
        c.to_ascii_lowercase()
    } else {
        c.to_lowercase().next().unwrap_or(c)
    }
}

/// Weighted sum, best first, ties broken by doc id for determinism. A note that scores nothing
/// is dropped unless `keep` vouches for it.
pub(crate) fn combine(
    c: Candidates,
    w: &Weights,
    keep: &dyn Fn(DocId) -> bool,
) -> Vec<(DocId, f32, Vec<Reason>)> {
    let mut out: Vec<(DocId, f32, Vec<Reason>)> =
        c.0.into_iter()
            .map(|(d, p)| {
                (
                    d,
                    w.lexical * p.lexical + w.explicit * p.explicit + w.temporal * p.temporal,
                    p.reasons,
                )
            })
            .filter(|x| x.1 > 0.0 || keep(x.0))
            .collect();
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gaps_read_naturally() {
        assert_eq!(describe_gap(0), "same day");
        assert_eq!(describe_gap(1), "a day apart");
        assert_eq!(describe_gap(12), "12 days apart");
        assert_eq!(describe_gap(40), "a month apart");
        assert_eq!(describe_gap(100), "3 months apart");
        assert_eq!(describe_gap(400), "a year apart");
        assert_eq!(describe_gap(800), "2 years apart");
    }
}
