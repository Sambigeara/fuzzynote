//! The grammar of the line: words find, and everything else holds.
//!
//! The words on the line rank, as they always have, each note with its reasons. Every other form
//! on the line is something a note has to hold to be listed at all: a phrase in quotes, a tag, a
//! person, a day, the box of an open task or of a done one, a span of when notes were written,
//! or with the done box on the line of when they were done, and with `!` before it what a note
//! has to be without. `/task` is the open box typed another way, as a date word is a day typed
//! another way. A form never changes how the words rank, so a new kind of filter is one more
//! form here and one more way for a note to hold it, and the words are left alone. A line of
//! nothing but forms has nothing to rank by, and lists exactly what holds them in the space's
//! order, as a place lists what is in it, or with a span in time order, since a span is a
//! question about time: oldest first, or newest first with a `-` before a span. `~` at the start
//! of the line makes its words letters, found in order in heads, as the original fzn's fuzzy
//! found them.
//!
//! Labels are what the index lists, so the engine looks them up; a span and a phrase are asked
//! of each note, and only of the notes that are left once the rest has narrowed them.

use crate::note::Note;
use crate::score;
use crate::text;
use crate::time::{self, Day, MS_PER_DAY, MS_PER_MINUTE, Moment};

/// The marks that open and close a phrase. A phone's keyboard curls a quote as it is typed, so
/// any of them closes what any of them opened.
const QUOTES: [char; 4] = ['"', '“', '”', '„'];

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

/// What the line asks for.
#[derive(Default)]
pub(crate) struct Query {
    /// The words that rank, where they were typed, with every form that only holds blanked out.
    /// A phrase's words are words too, so that a phrase in a head outranks one in a body.
    pub words: String,
    /// The words outside phrases: what has to name a note for going into it to have been a
    /// search for it, and with `~` the letters to find.
    pub free: String,
    /// `~` began the line, so the words are letters to find in order in heads.
    pub letters: bool,
    /// The tags, people, days and box a note has to carry.
    pub labels: Vec<Typed>,
    /// What else a note has to hold.
    pub holds: Vec<Hold>,
    /// The tags and people a note must not carry, lowercased as the index holds them.
    pub without: Vec<Label>,
    /// A `!` form is still being typed at the end of the line: it holds nothing yet, and the line
    /// is still not a line of labels.
    pub leaving: bool,
    /// The days the date words stand for, the spans, and the box the task word stands for, in
    /// the order they were typed.
    shown: Vec<Shown>,
}

/// A tag, a person, a day or the box of an open task typed on the line.
pub(crate) struct Typed {
    pub label: Label,
    /// As it was typed, `#House`, or for a day as it is written, `2026-10-16`.
    pub written: String,
    /// Something follows it. Until something does, the last thing on the line is still being
    /// typed, and holds every tag or person it is the start of, so that the list does not empty
    /// mid-word.
    pub whole: bool,
}

/// A label that is a place: a tag or a person, lowercased as the index holds them, a day, or the
/// open tasks.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum Label {
    Tag(String),
    Mention(String),
    Day(Day),
    /// The box `[ ]`: the notes whose heads start with it and that are not archived.
    Open,
    /// The box `[x]`: the notes whose heads start with it, archived or not. Asking for them is
    /// asking for archived notes, since that is where done tasks are.
    Done,
}

impl Label {
    /// The label as the index holds it.
    pub(crate) fn indexed(&self) -> score::Label {
        match self {
            Label::Tag(tag) => score::Label::Tag(tag.clone()),
            Label::Mention(person) => score::Label::Mention(person.clone()),
            Label::Day(day) => score::Label::Date(*day),
            Label::Open => score::Label::Open,
            Label::Done => score::Label::Done,
        }
    }

    /// Whether a note already carries it: anywhere in its text, for a day by having been written
    /// on it, and for the box at the start of its head.
    pub(crate) fn carried_by(&self, text: &str, created: Day) -> bool {
        match self {
            Label::Tag(tag) => text::facts(text).tags.contains(tag),
            Label::Mention(person) => text::facts(text).mentions.contains(person),
            // The day a note was written is not typed: it is the note's `created`.
            Label::Day(day) => *day == created || text::facts(text).dates.contains(day),
            Label::Open => text::task(text) == Some(false),
            Label::Done => text::task(text) == Some(true),
        }
    }
}

/// What a note has to hold that the index does not list.
pub(crate) enum Hold {
    /// Its text has this in it, both lowercased as `text::lowercase` does.
    Phrase(String),
    /// Its text does not have this in it, both lowercased as `text::lowercase` does.
    Without(String),
    /// It was written in this span, or done in it.
    Span(Span),
}

impl Hold {
    /// Whether a note holds it, given the note and its text lowercased.
    pub(crate) fn held_by(&self, note: &Note, lower: &str) -> bool {
        match self {
            Hold::Phrase(phrase) => lower.contains(phrase.as_str()),
            Hold::Without(phrase) => !lower.contains(phrase.as_str()),
            Hold::Span(span) => span.read(note).is_some_and(|at| span.holds(at)),
        }
    }
}

/// When notes were written, or done: from the start of a day, or of a minute of it, to the end
/// of one, either end open, or both, which is all time. It is read by the clock of the place
/// each note was written in, as a note's day is.
#[derive(Clone, Copy)]
pub(crate) struct Span {
    from: Option<End>,
    to: Option<End>,
    /// It measures when notes were done and not when they were written, since the line asks
    /// about the done tasks.
    pub done: bool,
    /// A `-` was typed before it, so a line of time it orders lists newest first.
    pub newest: bool,
}

impl Span {
    /// The moment of a note the span measures: when it was written, or when it was done.
    pub(crate) fn read(&self, note: &Note) -> Option<Moment> {
        if self.done { note.done } else { note.created }
    }

    fn holds(&self, written: Moment) -> bool {
        let wall = written.wall();
        self.from.is_none_or(|from| wall >= from.start())
            && self.to.is_none_or(|to| wall < to.end())
    }
}

/// One end of a span: a day, and the minute of it if a time was typed.
#[derive(Clone, Copy)]
struct End {
    day: Day,
    minute: Option<i64>,
}

impl End {
    fn start(self) -> i64 {
        i64::from(self.day) * MS_PER_DAY + self.minute.unwrap_or(0) * MS_PER_MINUTE
    }

    /// The end of its minute, or of its day.
    fn end(self) -> i64 {
        self.start()
            + match self.minute {
                Some(_) => MS_PER_MINUTE,
                None => MS_PER_DAY,
            }
    }

    fn shown(self) -> String {
        let day = day_shown(self.day);
        match self.minute {
            Some(minute) => format!("{day} {:02}:{:02}", minute / 60, minute % 60),
            None => day,
        }
    }
}

/// Something on the line that stands for something else, to be said beside it: a time, or the
/// box the task word is a way of typing.
enum Shown {
    Day(Day),
    Span(Span),
    Task,
}

fn day_shown(day: Day) -> String {
    format!("{} {}", WEEKDAYS[time::weekday(day)], time::format_day(day))
}

impl Query {
    /// Reads the line. The words are read as they always have been, from the line without the
    /// space at either end, so that the last word is the one still being typed.
    pub(crate) fn read(line: &str, today: Day) -> Self {
        let finished = line.ends_with(char::is_whitespace);
        let trailing = &line[line.trim_end().len()..];
        let line = line.trim();
        let (letters, line) = match line.strip_prefix('~') {
            Some(rest) => (true, rest),
            None => (false, line),
        };
        let chars: Vec<char> = line.chars().collect();
        let mut query = Query {
            letters,
            ..Query::default()
        };
        let code = text::code(&chars);
        let mut i = 0;
        while i < chars.len() {
            // Code is words, as in a note, and nothing in it holds.
            if let Some(r) = code.iter().find(|r| r.contains(&i)) {
                query.words.extend(&chars[i..r.end]);
                query.free.extend(&chars[i..r.end]);
                i = r.end;
                continue;
            }
            let c = chars[i];
            let boundary = i == 0 || !text::is_word_char(chars[i - 1]);
            // A `!` starting a word holds the notes without what follows it: a phrase, a tag, a
            // person, or else plain text to the next space. It never ranks, and it holds only
            // once something follows it, so that the list does not empty on the way to a word.
            // A mark after it that starts no tag or person leaves it words.
            let bang = c == '!'
                && (i == 0 || chars[i - 1].is_whitespace())
                && chars.get(i + 1).is_some_and(|n| !n.is_whitespace());
            let label = match chars.get(i + 1).filter(|_| bang) {
                Some('#') => text::take_tag(&chars, i + 2).map(|(t, end)| (Label::Tag(t), end)),
                Some('@') => {
                    text::take_mention(&chars, i + 2).map(|(m, end)| (Label::Mention(m), end))
                }
                _ => None,
            };
            let unmarked = matches!(chars.get(i + 1), Some('#' | '@')) && label.is_none();
            if bang && !unmarked {
                let follows = |end: usize| end < chars.len() || finished;
                let next = chars[i + 1];
                query.blank();
                if QUOTES.contains(&next) {
                    let close = chars[i + 2..].iter().position(|c| QUOTES.contains(c));
                    let end = close.map_or(chars.len(), |n| i + 2 + n);
                    let phrase: String = chars[i + 2..end].iter().collect();
                    if close.is_none() {
                        query.leaving = true;
                    } else if !phrase.trim().is_empty() {
                        query.holds.push(Hold::Without(text::lowercase(&phrase)));
                    }
                    i = end + 1;
                    continue;
                }
                i = match label {
                    Some((label, end)) => {
                        if follows(end) {
                            query.without.push(label);
                        } else {
                            query.leaving = true;
                        }
                        end
                    }
                    None => {
                        let end = chars[i + 1..]
                            .iter()
                            .position(|c| c.is_whitespace())
                            .map_or(chars.len(), |n| i + 1 + n);
                        if follows(end) {
                            let plain: String = chars[i + 1..end].iter().collect();
                            query.holds.push(Hold::Without(text::lowercase(&plain)));
                        } else {
                            query.leaving = true;
                        }
                        end
                    }
                };
                continue;
            }
            if QUOTES.contains(&c) {
                // A phrase runs to the next quote, or while it is being typed to the end of the
                // line, the space typed last included.
                let close = chars[i + 1..].iter().position(|c| QUOTES.contains(c));
                let end = close.map_or(chars.len(), |n| i + 1 + n);
                let mut phrase: String = chars[i + 1..end].iter().collect();
                query.words.push(' ');
                query.words.push_str(&phrase);
                query.free.push(' ');
                match close {
                    Some(_) => query.blank(),
                    None => phrase.push_str(trailing),
                }
                if !phrase.trim().is_empty() {
                    query.holds.push(Hold::Phrase(text::lowercase(&phrase)));
                }
                i = end + 1;
                continue;
            }
            // A box starts a word, as a date word does, and is read only whole: `[` and `[ ` hold
            // nothing, so that a `[[` on its way to a link lists no tasks.
            let boxed = [(text::OPEN, Label::Open), (text::DONE, Label::Done)]
                .into_iter()
                .find(|(mark, _)| chars[i..].iter().copied().take(3).eq(mark.chars()));
            if let Some((mark, label)) = boxed.filter(|_| text::at_word_start(&chars, i)) {
                query.labels.push(Typed {
                    label,
                    written: mark.to_owned(),
                    whole: true,
                });
                query.blank();
                i += 3;
                continue;
            }
            // The task word is a way of typing the open box, and is read as the box is, said
            // beside the line as a date word's date is.
            if text::at_word_start(&chars, i)
                && let Some(end) = text::take_task_word(&chars, i)
            {
                query.labels.push(Typed {
                    label: Label::Open,
                    written: text::OPEN.to_owned(),
                    whole: true,
                });
                query.shown.push(Shown::Task);
                query.blank();
                i = end;
                continue;
            }
            // A URL is words, and a `#` in it is not a tag.
            if boundary
                && c.is_ascii_alphabetic()
                && let Some((_, end)) = text::take_url(&chars, i)
            {
                query.words.extend(&chars[i..end]);
                query.free.extend(&chars[i..end]);
                i = end;
                continue;
            }
            // A `-` starting a word and straight before a span lists it newest first; before
            // anything else it is words, as it always was. What follows is read as it would be
            // without the `-`, as the start of a word, so that `-/-mon..` is `/-mon..` reversed.
            if c == '-'
                && (i == 0 || chars[i - 1].is_whitespace())
                && let Some((span, end)) = newest_span(&chars[i + 1..], today)
            {
                query.holds.push(Hold::Span(span));
                query.shown.push(Shown::Span(span));
                query.blank();
                i += 1 + end;
                continue;
            }
            let label = match c {
                '#' if boundary => {
                    text::take_tag(&chars, i + 1).map(|(t, end)| (Label::Tag(t), end))
                }
                '@' if boundary => {
                    text::take_mention(&chars, i + 1).map(|(m, end)| (Label::Mention(m), end))
                }
                _ => None,
            };
            let taken = match label {
                Some((label, end)) => {
                    query.labels.push(Typed {
                        label,
                        written: chars[i..end].iter().collect(),
                        whole: end < chars.len() || finished,
                    });
                    Some(end)
                }
                None => query.dated(&chars, i, boundary, today),
            };
            match taken {
                Some(end) => {
                    query.blank();
                    i = end;
                }
                None => {
                    query.words.push(c);
                    query.free.push(c);
                    i += 1;
                }
            }
        }
        if query.asks_done() {
            query.measure_done();
        }
        query
    }

    /// The span the line holds, if it holds one: a line of time is listed in the order of the
    /// moment its first span reads, newest first if any span on the line has a `-` before it,
    /// so that the newest end said first beside it is the end the list starts at.
    pub(crate) fn timed(&self) -> Option<Span> {
        let spans = || {
            self.holds.iter().filter_map(|hold| match hold {
                Hold::Span(span) => Some(*span),
                Hold::Phrase(_) | Hold::Without(_) => None,
            })
        };
        let newest = spans().any(|span| span.newest);
        spans().next().map(|span| Span { newest, ..span })
    }

    /// Whether the line asks for the done tasks, with `[x]`.
    pub(crate) fn asks_done(&self) -> bool {
        self.labels.iter().any(|typed| typed.label == Label::Done)
    }

    /// Makes every span on the line measure when notes were done, as it does with `[x]` on the
    /// line or in the place it is typed in.
    pub(crate) fn measure_done(&mut self) {
        for hold in &mut self.holds {
            if let Hold::Span(span) = hold {
                span.done = true;
            }
        }
    }

    /// Reads a day or a span at `i`, if one starts there, and says where it ends: `START..END`,
    /// `START..` to now, `..END`, or a day alone. A day is a date written out, in the old form
    /// too, or a date word; an end of a span may have a time after it. A time anywhere else is
    /// words.
    fn dated(&mut self, chars: &[char], i: usize, boundary: bool, today: Day) -> Option<usize> {
        if boundary && text::dots(chars, i) {
            // Two dots alone are a span with neither end: all time. Alone means a word of their
            // own, so that the dots after `wait...` are not one.
            let alone = |at: Option<&char>| at.is_none_or(|c| c.is_whitespace());
            if alone(i.checked_sub(1).map(|at| &chars[at])) && alone(chars.get(i + 2)) {
                self.span(None, None);
                return Some(i + 2);
            }
            let (to, end) = span_end(chars, i + 2, today)?;
            self.span(None, Some(to));
            return Some(end);
        }
        let (day, end, word) = if boundary && chars[i].is_ascii_digit() {
            text::take_iso_date(chars, i).map(|(day, end)| (day, end, false))?
        } else if text::at_word_start(chars, i) && chars[i] == '/' {
            text::take_date_word(chars, i, today).map(|(day, end)| (day, end, true))?
        } else if boundary && chars[i].is_ascii_alphabetic() {
            text::take_fzn_date(chars, i).map(|(day, end)| (day, end, false))?
        } else {
            return None;
        };
        let (from, at) = timed(chars, day, end);
        if text::dots(chars, at) {
            let (to, end) = match span_end(chars, at + 2, today) {
                Some((to, end)) => (Some(to), end),
                None => (None, at + 2),
            };
            self.span(Some(from), to);
            return Some(end);
        }
        self.labels.push(Typed {
            label: Label::Day(day),
            written: time::format_day(day),
            whole: true,
        });
        // A date word is said as the date it stands for; a date written out says itself.
        if word {
            self.shown.push(Shown::Day(day));
        }
        Some(end)
    }

    fn span(&mut self, from: Option<End>, to: Option<End>) {
        let span = Span {
            from,
            to,
            done: false,
            newest: false,
        };
        self.holds.push(Hold::Span(span));
        self.shown.push(Shown::Span(span));
    }

    /// A form taken out of the words leaves a space, so that the words either side stay apart.
    fn blank(&mut self) {
        self.words.push(' ');
        self.free.push(' ');
    }

    /// What the line names a note by, as the line is read for "A name is a fact too": its words,
    /// and the labels typed as words, since a head says its tags as words. Whatever was typed last
    /// is still the start of a word.
    pub(crate) fn name(&self) -> String {
        let (whole, typing): (Vec<&Typed>, Vec<&Typed>) =
            self.labels.iter().partition(|typed| typed.whole);
        let mut name: String = whole
            .iter()
            .map(|typed| format!("{} ", typed.written))
            .collect();
        name.push_str(&self.words);
        for typed in typing {
            name.push(' ');
            name.push_str(&typed.written);
        }
        name
    }

    /// Whether there is anything to rank by: a word, or with `~` a letter.
    pub(crate) fn ranks(&self) -> bool {
        if self.letters {
            !self.free.trim().is_empty()
        } else {
            !text::tokens(&self.words).is_empty()
        }
    }

    /// Nothing to rank by and nothing to hold: the line lists what it would if it were empty.
    pub(crate) fn is_empty(&self) -> bool {
        !self.ranks() && self.labels.is_empty() && self.holds.is_empty() && self.without.is_empty()
    }

    /// Whether the line is labels and nothing else, which Tab goes into one after another.
    pub(crate) fn only_labels(&self) -> bool {
        !self.letters
            && self.holds.is_empty()
            && self.without.is_empty()
            && !self.leaving
            && self.words.trim().is_empty()
            && !self.labels.is_empty()
    }

    /// What the date words, spans and task word on the line stand for, to be said beside it:
    /// `Fri 2026-10-16`, `Mon 2026-10-12 18:00 – now`, `[ ]`.
    pub(crate) fn shown(&self) -> Vec<String> {
        self.shown
            .iter()
            .map(|shown| match shown {
                Shown::Day(day) => day_shown(*day),
                Shown::Span(span) => {
                    let from = span.from.map_or("…".to_owned(), End::shown);
                    let to = span.to.map_or("now".to_owned(), End::shown);
                    // Newest first, it is said from the end the list starts at.
                    match span.newest {
                        true => format!("{to} – {from}"),
                        false => format!("{from} – {to}"),
                    }
                }
                Shown::Task => text::OPEN.to_owned(),
            })
            .collect()
    }
}

/// The span at the start of `chars`, read as the start of a word, made newest first, and where it
/// ends; nothing if a span does not start there, a day alone included.
fn newest_span(chars: &[char], today: Day) -> Option<(Span, usize)> {
    if chars.is_empty() {
        return None;
    }
    let mut read = Query::default();
    let end = read.dated(chars, 0, true, today)?;
    let Some(Hold::Span(span)) = read.holds.pop() else {
        return None;
    };
    Some((
        Span {
            newest: true,
            ..span
        },
        end,
    ))
}

/// A time of day after a date, ` 18:00`, if there is one: the end it makes, and where it ends.
fn timed(chars: &[char], day: Day, at: usize) -> (End, usize) {
    let minute = (chars.get(at) == Some(&' ')
        && chars.get(at + 3) == Some(&':')
        && !text::followed_by_digit(chars, at + 6))
    .then(|| text::digits(chars, at + 1, 2).zip(text::digits(chars, at + 4, 2)))
    .flatten()
    .filter(|&(hour, minute)| hour < 24 && minute < 60);
    match minute {
        Some((hour, minute)) => (
            End {
                day,
                minute: Some(i64::from(hour * 60 + minute)),
            },
            at + 6,
        ),
        None => (End { day, minute: None }, at),
    }
}

/// The end of a span after its `..`, if a date is there: a date written out or a date word,
/// and a time after it if there is one.
fn span_end(chars: &[char], at: usize, today: Day) -> Option<(End, usize)> {
    let (day, end) =
        text::take_iso_date(chars, at).or_else(|| text::take_date_word(chars, at, today))?;
    Some(timed(chars, day, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Thursday 2026-10-15.
    fn read(line: &str) -> Query {
        Query::read(line, time::day_from_civil(2026, 10, 15))
    }

    fn held(query: &Query) -> Vec<String> {
        let labels = query.labels.iter().map(|typed| {
            let open = if typed.whole { "" } else { "…" };
            format!("{}{open}", typed.written)
        });
        let holds = query.holds.iter().map(|hold| match hold {
            Hold::Phrase(phrase) => format!("{phrase:?}"),
            Hold::Without(phrase) => format!("!{phrase:?}"),
            Hold::Span(_) => "span".to_owned(),
        });
        let without = query.without.iter().map(|label| match label {
            Label::Tag(tag) => format!("!#{tag}"),
            Label::Mention(person) => format!("!@{person}"),
            _ => "!?".to_owned(),
        });
        labels.chain(holds).chain(without).collect()
    }

    #[test]
    fn words_find_and_everything_else_holds() {
        let query = read("boiler #House @joe /fri “oil tank” 2026-10-01");
        assert_eq!(text::tokens(&query.words), ["boiler", "oil", "tank"]);
        assert_eq!(text::tokens(&query.free), ["boiler"]);
        assert_eq!(
            held(&query),
            ["#House", "@joe", "2026-10-16", "2026-10-01", "\"oil tank\""]
        );
        assert_eq!(
            query.shown(),
            ["Fri 2026-10-16"],
            "a date written out says itself"
        );
        assert!(query.ranks() && !query.letters && !query.only_labels());
    }

    #[test]
    fn code_on_the_line_is_words_and_holds_nothing() {
        let query = read("paint `#fff` @joe");
        assert_eq!(
            held(&query),
            ["@joe…"],
            "the last word is still being typed"
        );
        assert!(query.holds.is_empty());
        let words = text::tokens(&query.words);
        assert!(words.contains(&"paint".to_owned()) && words.contains(&"fff".to_owned()));
    }

    #[test]
    fn a_bang_holds_what_does_not_have_what_follows_it() {
        let query = read("boiler !oil !\"Tank lid\" !#House !@joe ");
        assert_eq!(text::tokens(&query.words), ["boiler"]);
        assert_eq!(text::tokens(&query.free), ["boiler"]);
        assert_eq!(
            held(&query),
            ["!\"oil\"", "!\"tank lid\"", "!#house", "!@joe"]
        );
        assert!(!query.only_labels());
        assert!(!read("#house !#garden ").only_labels());
    }

    #[test]
    fn a_bang_still_being_typed_holds_nothing() {
        let withouts = |query: &Query| {
            query
                .holds
                .iter()
                .filter(|hold| matches!(hold, Hold::Without(_)))
                .count()
        };
        for line in ["!oi", "#house !#gar", "!\"tank l", "!\"\" "] {
            let query = read(line);
            assert!(query.without.is_empty(), "{line:?}");
            assert_eq!(withouts(&query), 0, "{line:?}");
        }
        assert_eq!(withouts(&read("!oil ")), 1);
        assert_eq!(withouts(&read("!\"oil\"")), 1);
    }

    #[test]
    fn a_bang_still_being_typed_makes_the_line_no_line_of_labels() {
        assert!(!read("#house !roo").only_labels());
        assert!(read("#house").only_labels());
    }

    #[test]
    fn a_bang_before_a_mark_that_starts_nothing_is_words() {
        for line in ["!# ", "!@ "] {
            let query = read(line);
            assert!(query.without.is_empty() && !query.leaving, "{line:?}");
            assert!(
                !query
                    .holds
                    .iter()
                    .any(|hold| matches!(hold, Hold::Without(_))),
                "{line:?}"
            );
        }
    }

    #[test]
    fn a_bang_that_does_not_start_a_word_is_words() {
        for (line, words) in [("wow!oil", &["wow", "oil"][..]), ("! oil", &["oil"])] {
            let query = read(line);
            assert!(query.without.is_empty(), "{line:?}");
            assert!(
                !query
                    .holds
                    .iter()
                    .any(|hold| matches!(hold, Hold::Without(_))),
                "{line:?}"
            );
            assert_eq!(text::tokens(&query.words), words, "{line:?}");
        }
    }

    #[test]
    fn what_is_still_being_typed_holds_every_label_it_starts() {
        assert_eq!(held(&read("boiler #ho")), ["#ho…"]);
        assert_eq!(held(&read("boiler #ho ")), ["#ho"]);
        assert_eq!(held(&read("#ho, boiler")), ["#ho"]);
        for (line, words) in [
            ("boiler #ho", ["boiler", "ho"]),
            ("#house boi", ["house", "boi"]),
        ] {
            let name = read(line).name();
            assert_eq!(text::tokens(&name), words, "{line}");
            assert!(!name.ends_with(' '), "the last still a start: {line}");
        }
    }

    #[test]
    fn a_phrase_runs_to_the_next_quote_of_any_kind_or_to_the_end_of_the_line() {
        for line in ["\"oil tank\"", "“oil tank”", "„oil tank“", "\"Oil Tank"] {
            assert_eq!(held(&read(line)), ["\"oil tank\""], "{line}");
        }
        assert_eq!(
            held(&read("\"oil ")),
            ["\"oil \""],
            "the space typed last is the phrase's while it is open"
        );
        assert!(
            read("\"\" “ ”").holds.is_empty(),
            "an empty phrase holds nothing"
        );
        assert_eq!(
            text::tokens(&read("\"#house plan\"").words),
            ["house", "plan"],
            "inside quotes a tag is text"
        );
    }

    #[test]
    fn spans_hold_when_notes_were_written() {
        for (line, shown) in [
            ("/-mon 18:00..", "Mon 2026-10-12 18:00 – now"),
            ("2026-10-01..2026-10-05", "Thu 2026-10-01 – Mon 2026-10-05"),
            ("..2026-10-05 09:30", "… – Mon 2026-10-05 09:30"),
            ("/-mon../fri", "Mon 2026-10-12 – Fri 2026-10-16"),
            ("/yesterday..x", "Wed 2026-10-14 – now"),
        ] {
            let query = read(line);
            assert_eq!(held(&query), ["span"], "{line}");
            assert_eq!(query.shown(), [shown], "{line}");
        }
        let query = read("/-mon 18:00 boiler");
        assert_eq!(held(&query), ["2026-10-12"]);
        assert_eq!(query.shown(), ["Mon 2026-10-12"]);
        assert_eq!(
            text::tokens(&query.words),
            ["18", "00", "boiler"],
            "a time outside a span is words"
        );
        for line in ["..", "#house .. ", "[x] .."] {
            let query = read(line);
            assert!(held(&query).ends_with(&["span".to_owned()]), "{line}");
            assert_eq!(
                query.shown(),
                ["… – now"],
                "two dots alone are all time: {line}"
            );
        }
        for line in ["...", "x..", "wait...", "(..)"] {
            assert!(held(&read(line)).is_empty(), "{line}");
        }
        assert_eq!(
            held(&read("x..2026-10-05")),
            ["2026-10-05"],
            "a span starts a word"
        );
        assert_eq!(
            held(&read("2026-10-01 25:00..")),
            ["2026-10-01"],
            "no such time, so no span"
        );
    }

    #[test]
    fn a_dash_before_a_span_lists_it_newest_first_and_before_anything_else_is_words() {
        for (line, shown) in [
            ("-..", "now – …"),
            ("-/-mon..", "now – Mon 2026-10-12"),
            ("-/-mon 18:00..", "now – Mon 2026-10-12 18:00"),
            ("-2026-10-01..2026-10-05", "Mon 2026-10-05 – Thu 2026-10-01"),
            ("#house -..2026-10-05 ", "Mon 2026-10-05 – …"),
        ] {
            let query = read(line);
            assert!(held(&query).ends_with(&["span".to_owned()]), "{line}");
            assert_eq!(query.shown(), [shown], "{line}");
            assert!(query.timed().is_some_and(|span| span.newest), "{line}");
            assert!(!query.ranks(), "the dash is not a word: {line}");
        }
        for line in ["/-mon..", "- ..", "-x .."] {
            let query = read(line);
            assert!(query.timed().is_some_and(|span| !span.newest), "{line}");
        }
        let query = read("[x] -/-mon.. #work ");
        assert_eq!(held(&query), ["[x]", "#work", "span"]);
        assert!(query.timed().is_some_and(|span| span.newest && span.done));
        for line in ["-", "-boiler", "-/-mon", "x -#house", "-...", "a-.."] {
            assert!(read(line).timed().is_none(), "{line}");
        }
        assert_eq!(text::tokens(&read("-boiler").words), ["boiler"]);
        assert!(
            held(&read("-/-mon")).is_empty(),
            "a dash before a date word alone leaves it words, as it was"
        );
        assert_eq!(
            held(&read("-2026-10-01")),
            ["2026-10-01"],
            "a date written out after a dash is still a day, the dash a word, as it was"
        );
    }

    #[test]
    fn one_dashed_span_turns_the_line_newest_first_and_each_span_says_its_own_way() {
        let query = read("/-mon.. -..2026-10-05");
        assert_eq!(held(&query), ["span", "span"]);
        assert_eq!(
            query.shown(),
            ["Mon 2026-10-12 – now", "Mon 2026-10-05 – …"]
        );
        assert!(query.timed().is_some_and(|span| span.newest));
    }

    #[test]
    fn a_span_runs_from_the_start_of_its_first_minute_or_day_to_the_end_of_its_last() {
        let day = time::day_from_civil(2026, 10, 12);
        let at = |hour: i64, minute: i64, offset: i32| Moment {
            ms: (i64::from(day) * 86_400 + hour * 3_600 + minute * 60 - i64::from(offset)) * 1_000,
            offset,
        };
        let Hold::Span(span) = &read("/-mon 18:00../-mon 18:30").holds[0] else {
            panic!("a span");
        };
        assert!(!span.holds(at(17, 59, 0)));
        assert!(span.holds(at(18, 0, 0)) && span.holds(at(18, 30, 0)));
        assert!(!span.holds(at(18, 31, 0)));
        assert!(
            span.holds(at(18, 15, -4 * 3_600)),
            "by the clock where the note was written"
        );
    }

    #[test]
    fn a_tilde_makes_the_words_letters_and_the_rest_still_holds() {
        let query = read("  ~bsv #house ");
        assert!(query.letters && query.ranks());
        assert_eq!(query.free.trim(), "bsv");
        assert_eq!(held(&query), ["#house"]);
        assert!(!read("~").ranks() && read("~").is_empty());
        assert!(!read("x ~bsv").letters, "only at the start of the line");
    }

    #[test]
    fn the_box_of_an_open_task_is_one_label_with_a_space_in_it_and_its_start_is_nothing() {
        for (line, labels) in [
            ("[ ]", &["[ ]"][..]),
            ("[ ] #house ", &["[ ]", "#house"]),
            ("#house [ ]", &["#house", "[ ]"]),
            ("(oil [ ])", &["[ ]"]),
            ("[ ]oil", &["[ ]"]),
            ("#house [", &["#house"]),
        ] {
            let query = read(line);
            assert_eq!(held(&query), labels, "{line:?}");
            assert!(
                text::tokens(&query.words).iter().all(|w| w == "oil"),
                "{line:?}"
            );
        }
        assert!(read("[ ] #house").only_labels());
        assert!(read("[").is_empty() && read("[ ").is_empty());
        for line in ["[x] oil", "oil[ ]", "[ oil", "[[ ]]", "\"[ ] oil\"", "[X]"] {
            let query = read(line);
            assert!(
                query.labels.iter().all(|typed| typed.label != Label::Open),
                "{line:?}"
            );
        }
    }

    #[test]
    fn the_task_word_is_read_as_the_open_box_and_says_so() {
        for (line, labels) in [
            ("/task #house ", ["[ ]", "#house"]),
            ("/ta #house ", ["[ ]", "#house"]),
            ("#house /tas", ["#house", "[ ]"]),
            ("/Task #house ", ["[ ]", "#house"]),
        ] {
            let query = read(line);
            assert_eq!(held(&query), labels, "{line:?}");
            assert!(query.only_labels(), "{line:?}");
            assert_eq!(query.shown(), ["[ ]"], "{line:?}");
        }
        let query = read("/task oil /fri");
        assert_eq!(text::tokens(&query.words), ["oil"]);
        assert_eq!(query.shown(), ["[ ]", "Fri 2026-10-16"]);
        for line in ["/t", "/tasks", "x/task", "\"/task\""] {
            let query = read(line);
            assert!(
                query.labels.iter().all(|typed| typed.label != Label::Open),
                "{line:?}"
            );
        }
    }

    #[test]
    fn the_box_of_a_done_task_is_a_label_too_and_with_it_a_span_measures_when_notes_were_done() {
        for (line, labels) in [
            ("[x]", &["[x]"][..]),
            ("[x] /-mon.. #work ", &["[x]", "#work", "span"]),
            ("#work [x] [ ]", &["#work", "[x]", "[ ]"]),
            ("oil[x]", &[]),
            ("\"[x]\"", &["\"[x]\""]),
        ] {
            assert_eq!(held(&read(line)), labels, "{line:?}");
        }
        assert!(read("[x] #work").only_labels() && read("[x]").asks_done());
        assert!(!read("[ ] /-mon..").asks_done());
        let spans = |query: &Query| -> Vec<bool> {
            query
                .holds
                .iter()
                .filter_map(|hold| match hold {
                    Hold::Span(span) => Some(span.done),
                    Hold::Phrase(_) | Hold::Without(_) => None,
                })
                .collect()
        };
        assert_eq!(spans(&read("/-mon.. [x]")), [true], "wherever the box is");
        assert_eq!(spans(&read("/-mon.. #work")), [false]);
        assert_eq!(
            spans(&read("/-mon.. \"[x]\"")),
            [false],
            "a phrase is not the box"
        );
    }

    #[test]
    fn a_line_is_labels_alone_only_when_nothing_else_is_on_it() {
        assert!(read("#house /today @joe 2026-10-16 [ ]").only_labels());
        for line in [
            "#house boiler",
            "#house /today..",
            "#house \"oil\"",
            "~#house",
            "",
        ] {
            assert!(!read(line).only_labels(), "{line}");
        }
    }

    #[test]
    fn a_url_is_words_and_a_hash_in_it_is_not_a_tag() {
        let query = read("https://example.com/#house");
        assert!(query.labels.is_empty());
        assert_eq!(text::tokens(&query.words), ["example.com"]);
    }
}
