//! Scorers and their reasons. Each scorer contributes roughly `[0, 1]` plus a short reason, and
//! `Weights` decides how the contributions combine.

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::index::{DocId, Index, TermId};
use crate::time::{self, Day};

/// Explicit structure a note carries, indexed for exact lookup.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Label {
    Tag(String),
    Mention(String),
    /// A wikilink target the note points at, normalised.
    Link(String),
    /// The note's own head, normalised, so wikilinks can resolve to it.
    Head(String),
    /// A word in the note's head.
    HeadTerm(String),
    /// A day the note was created on or mentions.
    Date(Day),
}

/// What a query asks for explicitly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum QueryLabel {
    Tag(String),
    Mention(String),
    /// Notes whose head is this.
    LinkTarget(String),
    /// Notes that link to this.
    LinksTo(String),
    /// Notes with this word in their head.
    HeadTerm(String),
}

/// Tunable weights. The defaults are a starting point, not a conclusion.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(default)
)]
pub struct Weights {
    /// Weight of the lexical (BM25) scorer.
    pub lexical: f32,
    /// Weight of the explicit scorer: tags, mentions, links.
    pub explicit: f32,
    /// Weight of the temporal scorer: shared dates and proximity in time.
    pub temporal: f32,
    /// Multiplier on a fuzzy term expansion relative to an exact term.
    pub fuzzy: f32,
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
    /// The scorer that produced it: `words`, `explicit` or `time`.
    pub scorer: &'static str,
    pub text: String,
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
}

pub(crate) struct Context<'a> {
    pub terms: &'a [String],
    pub labels: &'a [QueryLabel],
    pub dates: &'a [Day],
    /// The day to measure proximity from: today when typing, the note's own day for `related`.
    pub day: Option<Day>,
    pub exclude: Option<DocId>,
}

/// BM25 over exact query terms plus fuzzy expansions, normalised so that a note matching every
/// term strongly scores about 1.
pub(crate) fn lexical(index: &Index, ctx: &Context, w: &Weights, c: &mut Candidates) {
    let mut seen: Vec<&str> = Vec::new();
    let mut qterms: Vec<(TermId, f32, String)> = Vec::new();
    for q in ctx.terms {
        if seen.contains(&q.as_str()) {
            continue;
        }
        seen.push(q);
        let exact = index.term_id(q);
        if let Some(t) = exact {
            qterms.push((t, 1.0, q.clone()));
        }
        if q.chars().count() >= 4 {
            for (t, sim) in index.fuzzy_terms(q, 3, w.fuzzy_min_similarity) {
                if exact != Some(t) {
                    qterms.push((t, w.fuzzy * sim, format!("{q}≈{}", index.term_name(t))));
                }
            }
        }
    }
    let (k1, b) = (w.bm25_k1, w.bm25_b);
    let avg = index.avg_doc_len().max(1.0);
    let max_possible: f32 = qterms
        .iter()
        .map(|(t, wt, _)| wt * index.idf(*t) * (k1 + 1.0))
        .sum();
    if max_possible <= 0.0 {
        return;
    }
    let mut matched: HashMap<DocId, Vec<(f32, usize)>> = HashMap::new();
    for (qi, (t, wt, _)) in qterms.iter().enumerate() {
        let idf = index.idf(*t);
        for &(doc, tf) in index.postings(*t) {
            if Some(doc) == ctx.exclude {
                continue;
            }
            let tf = tf as f32;
            let dl = index.doc_len(doc) as f32;
            let s = idf * tf * (k1 + 1.0) / (tf + k1 * (1.0 - b + b * dl / avg));
            let contribution = wt * s / max_possible;
            c.entry(doc).lexical += contribution;
            matched.entry(doc).or_default().push((contribution, qi));
        }
    }
    for (doc, mut m) in matched {
        m.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        let names: Vec<&str> = m
            .iter()
            .take(3)
            .map(|&(_, qi)| qterms[qi].2.as_str())
            .collect();
        let more = m.len().saturating_sub(3);
        let text = if more > 0 {
            format!("{} +{more}", names.join(", "))
        } else {
            names.join(", ")
        };
        c.entry(doc).reasons.push(Reason {
            scorer: "words",
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

/// Shared tags, mentions, wikilinks and head words. Three shared labels saturate the score. Head
/// words count as the fraction of the query, by rarity, that appears in the head, scaled by how
/// specific the query is, so a note titled with the query outranks a one-liner that mentions it.
pub(crate) fn explicit(index: &Index, ctx: &Context, c: &mut Candidates) {
    let mut hits: HashMap<DocId, (f32, Vec<String>)> = HashMap::new();
    let mut head_hits: HashMap<DocId, (f32, Vec<String>)> = HashMap::new();
    let idf_max = ((index.live_docs() as f32) + 1.0).ln().max(f32::EPSILON);
    let mut query_weight = 0.0;
    for l in ctx.labels {
        match l {
            QueryLabel::HeadTerm(t) => {
                let Some(term) = index.term_id(t) else {
                    continue;
                };
                let weight = index.idf(term);
                query_weight += weight;
                for &d in index.docs_with(&Label::HeadTerm(t.clone())) {
                    if Some(d) == ctx.exclude {
                        continue;
                    }
                    let e = head_hits.entry(d).or_insert_with(|| (0.0, Vec::new()));
                    e.0 += weight;
                    e.1.push(t.clone());
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
            QueryLabel::LinkTarget(x) => add_explicit(
                &mut hits,
                ctx.exclude,
                index.docs_with(&Label::Head(x.clone())),
                2.0,
                &format!("is [[{x}]]"),
            ),
            QueryLabel::LinksTo(x) => add_explicit(
                &mut hits,
                ctx.exclude,
                index.docs_with(&Label::Link(x.clone())),
                1.0,
                &format!("links [[{x}]]"),
            ),
        }
    }
    let specificity = (query_weight / idf_max).min(1.0);
    for (doc, (sum, words)) in head_hits {
        let fraction = if query_weight > 0.0 {
            sum / query_weight
        } else {
            0.0
        };
        let e = hits.entry(doc).or_insert_with(|| (0.0, Vec::new()));
        e.0 += 3.0 * fraction * specificity;
        e.1.push(format!("in head: {}", words.join(", ")));
    }
    for (doc, (sum, texts)) in hits {
        let p = c.entry(doc);
        p.explicit += (sum / 3.0).min(1.0);
        p.reasons.push(Reason {
            scorer: "explicit",
            text: texts.join(", "),
        });
    }
}

fn describe_gap(days: i32) -> String {
    match days {
        0 => "same day".into(),
        1 => "a day apart".into(),
        d if d < 31 => format!("{d} days apart"),
        d if d < 365 => format!("{} months apart", d / 30),
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
    for &d in ctx.dates {
        for &doc in index.docs_with(&Label::Date(d)) {
            if Some(doc) == ctx.exclude {
                continue;
            }
            let p = c.entry(doc);
            p.temporal += 0.5;
            p.reasons.push(Reason {
                scorer: "time",
                text: format!("same date {}", time::format_day(d)),
            });
        }
    }
    if let Some(day) = ctx.day {
        for (&doc, p) in c.0.iter_mut() {
            let Some(d) = day_of(doc) else { continue };
            let delta = (d - day).abs();
            let bonus = 0.5 * 0.5f32.powf(delta as f32 / w.halflife_days.max(1.0));
            if bonus < 0.02 {
                continue;
            }
            p.temporal += bonus;
            p.reasons.push(Reason {
                scorer: "time",
                text: describe_gap(delta),
            });
        }
    }
    for p in c.0.values_mut() {
        p.temporal = p.temporal.min(1.0);
    }
}

/// Weighted sum, best first, ties broken by doc id for determinism.
pub(crate) fn combine(c: Candidates, w: &Weights) -> Vec<(DocId, f32, Vec<Reason>)> {
    let mut out: Vec<(DocId, f32, Vec<Reason>)> =
        c.0.into_iter()
            .map(|(d, p)| {
                (
                    d,
                    w.lexical * p.lexical + w.explicit * p.explicit + w.temporal * p.temporal,
                    p.reasons,
                )
            })
            .filter(|x| x.1 > 0.0)
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
        assert_eq!(describe_gap(100), "3 months apart");
        assert_eq!(describe_gap(800), "2 years apart");
    }
}
