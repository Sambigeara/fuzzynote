//! An incrementally maintained inverted index, a trigram index over its vocabulary for fuzzy
//! matching, and a label index for explicit structure.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use crate::score::Label;
use crate::text;

pub type DocId = u32;
pub type TermId = u32;

struct Term {
    name: String,
    trigram_count: u32,
    /// `(doc, term frequency)`, sorted by doc.
    postings: Vec<(DocId, u32)>,
}

#[derive(Default)]
pub struct Index {
    term_ids: HashMap<String, TermId>,
    /// The vocabulary in order, for prefix lookups.
    sorted_terms: BTreeMap<String, TermId>,
    terms: Vec<Term>,
    trigram_terms: HashMap<String, Vec<TermId>>,
    /// Per doc: `(term, tf)`, sorted by term. Needed for removal and for "more like this".
    doc_terms: Vec<Vec<(TermId, u32)>>,
    doc_len: Vec<u32>,
    live: Vec<bool>,
    live_docs: usize,
    total_len: u64,
    /// In order, so that a tag or a person still being typed finds every one it starts.
    labels: BTreeMap<Label, Vec<DocId>>,
    doc_labels: Vec<Vec<Label>>,
}

impl Index {
    fn ensure_doc(&mut self, doc: DocId) {
        let n = doc as usize + 1;
        if self.doc_terms.len() < n {
            self.doc_terms.resize_with(n, Vec::new);
            self.doc_len.resize(n, 0);
            self.live.resize(n, false);
            self.doc_labels.resize_with(n, Vec::new);
        }
    }

    fn intern(&mut self, name: &str) -> TermId {
        if let Some(&t) = self.term_ids.get(name) {
            return t;
        }
        let t = self.terms.len() as TermId;
        let grams = text::trigrams(name);
        self.terms.push(Term {
            name: name.to_owned(),
            trigram_count: grams.len() as u32,
            postings: Vec::new(),
        });
        self.term_ids.insert(name.to_owned(), t);
        self.sorted_terms.insert(name.to_owned(), t);
        for g in grams {
            self.trigram_terms.entry(g).or_default().push(t);
        }
        t
    }

    /// Adds a document. A document that is already present must be removed first.
    pub fn add(&mut self, doc: DocId, tokens: &[String], labels: &[Label]) {
        self.ensure_doc(doc);
        assert!(!self.live[doc as usize], "doc {doc} added twice");
        let mut tf: HashMap<&str, u32> = HashMap::new();
        for t in tokens {
            *tf.entry(t.as_str()).or_insert(0) += 1;
        }
        let mut entries: Vec<(TermId, u32)> = tf
            .into_iter()
            .map(|(name, c)| (self.intern(name), c))
            .collect();
        entries.sort_unstable();
        for &(t, c) in &entries {
            let p = &mut self.terms[t as usize].postings;
            let pos = p.partition_point(|&(d, _)| d < doc);
            p.insert(pos, (doc, c));
        }
        let i = doc as usize;
        let len = tokens.len() as u32;
        self.doc_terms[i] = entries;
        self.doc_len[i] = len;
        self.total_len += u64::from(len);
        self.live[i] = true;
        self.live_docs += 1;
        let mut doc_labels = labels.to_vec();
        doc_labels.dedup();
        for l in &doc_labels {
            let v = self.labels.entry(l.clone()).or_default();
            let pos = v.partition_point(|&d| d < doc);
            if v.get(pos) != Some(&doc) {
                v.insert(pos, doc);
            }
        }
        self.doc_labels[i] = doc_labels;
    }

    /// Removes a document. Removing an absent document is a no-op.
    pub fn remove(&mut self, doc: DocId) {
        let i = doc as usize;
        if !self.is_live(doc) {
            return;
        }
        for (t, _) in std::mem::take(&mut self.doc_terms[i]) {
            let p = &mut self.terms[t as usize].postings;
            if let Ok(pos) = p.binary_search_by_key(&doc, |&(d, _)| d) {
                p.remove(pos);
            }
        }
        for l in std::mem::take(&mut self.doc_labels[i]) {
            if let Some(v) = self.labels.get_mut(&l) {
                if let Ok(pos) = v.binary_search(&doc) {
                    v.remove(pos);
                }
                if v.is_empty() {
                    self.labels.remove(&l);
                }
            }
        }
        self.total_len -= u64::from(self.doc_len[i]);
        self.doc_len[i] = 0;
        self.live[i] = false;
        self.live_docs -= 1;
    }

    pub fn is_live(&self, doc: DocId) -> bool {
        self.live.get(doc as usize).copied().unwrap_or(false)
    }

    pub fn live_docs(&self) -> usize {
        self.live_docs
    }

    pub fn doc_len(&self, doc: DocId) -> u32 {
        self.doc_len.get(doc as usize).copied().unwrap_or(0)
    }

    pub fn avg_doc_len(&self) -> f32 {
        if self.live_docs == 0 {
            0.0
        } else {
            self.total_len as f32 / self.live_docs as f32
        }
    }

    pub fn term_id(&self, name: &str) -> Option<TermId> {
        self.term_ids.get(name).copied()
    }

    pub fn term_name(&self, t: TermId) -> &str {
        &self.terms[t as usize].name
    }

    pub fn postings(&self, t: TermId) -> &[(DocId, u32)] {
        &self.terms[t as usize].postings
    }

    pub fn doc_terms(&self, doc: DocId) -> &[(TermId, u32)] {
        self.doc_terms.get(doc as usize).map_or(&[], Vec::as_slice)
    }

    pub fn df(&self, t: TermId) -> usize {
        self.postings(t).len()
    }

    /// BM25 inverse document frequency; always positive.
    pub fn idf(&self, t: TermId) -> f32 {
        let n = self.live_docs as f32;
        let df = self.df(t) as f32;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    pub fn docs_with(&self, label: &Label) -> &[DocId] {
        self.labels.get(label).map_or(&[], Vec::as_slice)
    }

    /// The notes carrying a label that `start` starts, each list sorted: every tag or person
    /// whose name starts with its name, or for any other label the label itself.
    pub fn docs_starting<'a>(&'a self, start: &'a Label) -> impl Iterator<Item = &'a [DocId]> {
        self.labels
            .range(start..)
            .take_while(move |(label, _)| label.starts_with(start))
            .map(|(_, docs)| docs.as_slice())
    }

    /// Every vocabulary term starting with `prefix`, in alphabetical order.
    pub fn terms_starting<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a str> {
        self.sorted_terms
            .range(prefix.to_owned()..)
            .map(|(name, _)| name.as_str())
            .take_while(move |name| name.starts_with(prefix))
    }

    /// Vocabulary terms starting with `prefix`, most common first, at most `limit`.
    pub fn prefix_terms(&self, prefix: &str, limit: usize) -> Vec<TermId> {
        let mut out: Vec<TermId> = self
            .sorted_terms
            .range(prefix.to_owned()..)
            .take_while(|(name, _)| name.starts_with(prefix))
            .map(|(_, &t)| t)
            .filter(|&t| !self.terms[t as usize].postings.is_empty())
            .collect();
        out.sort_by(|&a, &b| {
            self.df(b)
                .cmp(&self.df(a))
                .then_with(|| self.term_name(a).cmp(self.term_name(b)))
        });
        out.truncate(limit);
        out
    }

    /// Vocabulary terms that look like `query`, by trigram Jaccard similarity, best first.
    pub fn fuzzy_terms(
        &self,
        query: &str,
        limit: usize,
        min_similarity: f32,
    ) -> Vec<(TermId, f32)> {
        let grams = text::trigrams(query);
        let mut overlap: HashMap<TermId, u32> = HashMap::new();
        for g in &grams {
            if let Some(ts) = self.trigram_terms.get(g) {
                for &t in ts {
                    *overlap.entry(t).or_insert(0) += 1;
                }
            }
        }
        let qn = grams.len() as f32;
        let mut out: Vec<(TermId, f32)> = overlap
            .into_iter()
            .filter_map(|(t, o)| {
                let term = &self.terms[t as usize];
                if term.name == query || term.postings.is_empty() {
                    return None;
                }
                let sim = o as f32 / (qn + term.trigram_count as f32 - o as f32);
                (sim >= min_similarity).then_some((t, sim))
            })
            .collect();
        out.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        out.truncate(limit);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<String> {
        text::tokens(s)
    }

    #[test]
    fn add_query_remove() {
        let mut ix = Index::default();
        ix.add(
            0,
            &toks("kettlebells swing swing"),
            &[Label::Tag("gym".into())],
        );
        ix.add(1, &toks("kettlebells press"), &[]);
        ix.add(2, &toks("shopping milk"), &[Label::Tag("gym".into())]);
        let kb = ix.term_id("kettlebells").unwrap();
        assert_eq!(ix.df(kb), 2);
        assert_eq!(ix.postings(ix.term_id("swing").unwrap()), &[(0, 2)]);
        assert_eq!(ix.docs_with(&Label::Tag("gym".into())), &[0, 2]);
        assert!(
            ix.idf(kb) < ix.idf(ix.term_id("milk").unwrap()),
            "rarer terms weigh more"
        );
        assert_eq!(ix.live_docs(), 3);
        assert_eq!(ix.total_len, 7);

        ix.remove(0);
        ix.remove(0);
        assert_eq!(ix.df(kb), 1);
        assert_eq!(ix.docs_with(&Label::Tag("gym".into())), &[2]);
        assert_eq!(ix.live_docs(), 2);
        assert_eq!(ix.total_len, 4);
        assert!(ix.doc_terms(0).is_empty());
    }

    #[test]
    fn prefix_terms_complete_the_word_being_typed() {
        let mut ix = Index::default();
        ix.add(0, &toks("kettlebells kettle kettlebell"), &[]);
        ix.add(1, &toks("kettlebells keg"), &[]);
        let names: Vec<&str> = ix
            .prefix_terms("kett", 10)
            .into_iter()
            .map(|t| ix.term_name(t))
            .collect();
        assert_eq!(
            names,
            ["kettlebells", "kettle", "kettlebell"],
            "most common first"
        );
        assert_eq!(ix.prefix_terms("kett", 1).len(), 1);
        assert!(ix.prefix_terms("zzz", 10).is_empty());
        ix.remove(0);
        let names: Vec<&str> = ix
            .prefix_terms("kett", 10)
            .into_iter()
            .map(|t| ix.term_name(t))
            .collect();
        assert_eq!(names, ["kettlebells"], "dead terms are not completions");
    }

    #[test]
    fn fuzzy_terms_find_misspellings() {
        let mut ix = Index::default();
        ix.add(0, &toks("kettlebells workout"), &[]);
        ix.add(1, &toks("kettle on the hob"), &[]);
        let found = ix.fuzzy_terms("kettlebels", 3, 0.5);
        assert_eq!(
            found.first().map(|&(t, _)| ix.term_name(t)),
            Some("kettlebells")
        );
        assert!(
            found.iter().all(|&(t, _)| ix.term_name(t) != "kettle"),
            "{found:?}"
        );
        assert!(
            ix.fuzzy_terms("kettlebells", 3, 0.5)
                .iter()
                .all(|&(t, _)| ix.term_name(t) != "kettlebells")
        );
    }
}
