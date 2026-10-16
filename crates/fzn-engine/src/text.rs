//! Text analysis: tokens, trigrams, and the explicit facts a text states about itself.

use unicode_segmentation::UnicodeSegmentation;

use crate::time::{self, Day};

/// Terms longer than this are dropped; nothing that long is a word.
pub const MAX_TERM_CHARS: usize = 64;

/// Lowercased words in order, per Unicode word segmentation. A URL contributes only its host
/// (without `www.`), so that "same site" counts and scheme fragments do not. Single characters
/// are dropped.
pub fn tokens(text: &str) -> Vec<String> {
    hosts_for_urls(text)
        .unicode_words()
        .filter(|w| (2..=MAX_TERM_CHARS).contains(&w.chars().count()))
        .map(str::to_lowercase)
        .collect()
}

/// Replaces every URL in `text` with its host.
fn hosts_for_urls(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let boundary = i == 0 || !is_word_char(chars[i - 1]);
        if boundary
            && chars[i].is_ascii_alphabetic()
            && let Some((url, end)) = take_url(&chars, i)
        {
            out.push(' ');
            out.push_str(&url_host(&url));
            out.push(' ');
            i = end;
            continue;
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// The host of a URL as `take_url` returns it, lowercased, without a leading `www.`.
fn url_host(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let host = rest
        .split(['/', '?', '#', ':'])
        .next()
        .unwrap_or("")
        .to_lowercase();
    host.strip_prefix("www.").unwrap_or(&host).to_owned()
}

/// The set of character trigrams of a term, padded with a space on each side.
pub fn trigrams(term: &str) -> Vec<String> {
    let padded: Vec<char> = std::iter::once(' ')
        .chain(term.chars())
        .chain(std::iter::once(' '))
        .collect();
    let mut grams: Vec<String> = padded.windows(3).map(|w| w.iter().collect()).collect();
    grams.sort_unstable();
    grams.dedup();
    grams
}

/// Normalises a head or a wikilink target for matching: lowercased, whitespace collapsed.
pub fn normalise_head(s: &str) -> String {
    s.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// The explicit structure a text declares. All strings are lowercased except URLs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Facts {
    pub tags: Vec<String>,
    pub mentions: Vec<String>,
    pub links: Vec<String>,
    pub urls: Vec<String>,
    pub dates: Vec<Day>,
}

/// Extracts `#tags`, `@mentions`, `[[wikilinks]]`, URLs and dates written as `2026-09-05` or
/// `Mon, Jan 02, 2006` (the form the old `{d}` token expanded to).
pub fn facts(text: &str) -> Facts {
    let chars: Vec<char> = text.chars().collect();
    let mut f = Facts::default();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let boundary = i == 0 || !is_word_char(chars[i - 1]);
        let next = if boundary && c == '#' {
            take_tag(&chars, i + 1).map(|(tag, end)| {
                f.tags.push(tag);
                end
            })
        } else if boundary && c == '@' {
            take_mention(&chars, i + 1).map(|(m, end)| {
                f.mentions.push(m);
                end
            })
        } else if c == '[' && chars.get(i + 1) == Some(&'[') {
            take_wikilink(&chars, i + 2).map(|(target, end)| {
                f.links.push(target);
                end
            })
        } else if boundary && c.is_ascii_digit() {
            take_iso_date(&chars, i).map(|(day, end)| {
                f.dates.push(day);
                end
            })
        } else if boundary && c.is_ascii_alphabetic() {
            if let Some((url, end)) = take_url(&chars, i) {
                f.urls.push(url);
                Some(end)
            } else if let Some((day, end)) = take_fzn_date(&chars, i) {
                f.dates.push(day);
                Some(end)
            } else {
                None
            }
        } else {
            None
        };
        i = next.unwrap_or(i + 1);
    }
    for v in [&mut f.tags, &mut f.mentions, &mut f.links, &mut f.urls] {
        v.sort();
        v.dedup();
    }
    f.dates.sort_unstable();
    f.dates.dedup();
    f
}

/// Whether a line says nothing but tags.
pub fn only_tags(line: &str) -> bool {
    let mut words = line.split_whitespace().peekable();
    words.peek().is_some() && words.all(|w| w.starts_with('#') && facts(w).tags.len() == 1)
}

/// A note's head: its first line, trimmed.
pub fn head(text: &str) -> &str {
    text.lines().next().unwrap_or("").trim()
}

/// Whether a note has more to read than its head. A line of nothing but tags is a label, not
/// reading.
pub fn has_body(text: &str) -> bool {
    text.lines()
        .skip(1)
        .any(|line| !line.trim().is_empty() && !only_tags(line))
}

/// The box an open task starts with.
pub const OPEN: &str = "[ ]";
/// The box a done task starts with.
const DONE: &str = "[x]";

/// Whether a text is a task, and if it is, whether it is done: whether it starts with a box,
/// `[ ]` or `[x]`. A box anywhere else, as a markdown task in a body is, makes nothing a task.
pub fn task(text: &str) -> Option<bool> {
    if text.starts_with(OPEN) {
        Some(false)
    } else if text.starts_with(DONE) {
        Some(true)
    } else {
        None
    }
}

/// A text as a task, open or done: its box ticked or not, or put at its start with a space.
pub fn tick(text: &str, done: bool) -> String {
    let mark = if done { DONE } else { OPEN };
    match task(text) {
        Some(_) => format!("{mark}{}", &text[mark.len()..]),
        None => format!("{mark} {text}"),
    }
}

/// Truncates to `max` characters, marking the cut.
pub fn fit(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_owned();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    if max > 0 {
        out.push('…');
    }
    out
}

/// The text with each date written in it blanked out, for when a date is to count as a date
/// and not as the numbers in it.
pub fn without_dates(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let boundary = i == 0 || !is_word_char(chars[i - 1]);
        let date = if boundary && c.is_ascii_digit() {
            take_iso_date(&chars, i)
        } else if boundary && c.is_ascii_alphabetic() {
            take_fzn_date(&chars, i)
        } else {
            None
        };
        match date {
            Some((_, end)) => {
                out.push(' ');
                i = end;
            }
            None => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

pub(crate) fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

pub(crate) fn take_tag(chars: &[char], start: usize) -> Option<(String, usize)> {
    if !chars.get(start)?.is_alphanumeric() {
        return None;
    }
    let mut end = start;
    while end < chars.len() && (is_word_char(chars[end]) || matches!(chars[end], '-' | '/')) {
        end += 1;
    }
    while end > start && matches!(chars[end - 1], '-' | '/') {
        end -= 1;
    }
    Some((
        chars[start..end].iter().collect::<String>().to_lowercase(),
        end,
    ))
}

pub(crate) fn take_mention(chars: &[char], start: usize) -> Option<(String, usize)> {
    if !chars.get(start)?.is_alphanumeric() {
        return None;
    }
    let mut end = start;
    while end < chars.len() && (is_word_char(chars[end]) || matches!(chars[end], '-' | '.' | '@')) {
        end += 1;
    }
    while end > start && matches!(chars[end - 1], '-' | '.' | '@') {
        end -= 1;
    }
    Some((
        chars[start..end].iter().collect::<String>().to_lowercase(),
        end,
    ))
}

fn take_wikilink(chars: &[char], start: usize) -> Option<(String, usize)> {
    let mut j = start;
    loop {
        if j + 1 >= chars.len() || chars[j] == '\n' || j - start > 256 {
            return None;
        }
        if chars[j] == ']' && chars[j + 1] == ']' {
            break;
        }
        j += 1;
    }
    let inner: String = chars[start..j].iter().collect();
    let target = normalise_head(inner.split(['|', '#']).next().unwrap_or(""));
    if target.is_empty() {
        return None;
    }
    Some((target, j + 2))
}

pub(crate) fn take_url(chars: &[char], start: usize) -> Option<(String, usize)> {
    let lower: String = chars[start..chars.len().min(start + 8)]
        .iter()
        .flat_map(|c| c.to_lowercase())
        .collect();
    let prefix = ["https://", "http://", "www."]
        .into_iter()
        .find(|p| lower.starts_with(p))?;
    let mut end = start + prefix.len();
    while end < chars.len()
        && !chars[end].is_whitespace()
        && !matches!(chars[end], '<' | '>' | '"' | '\'')
    {
        end += 1;
    }
    while end > start + prefix.len()
        && matches!(
            chars[end - 1],
            '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']' | '}'
        )
    {
        end -= 1;
    }
    if end == start + prefix.len() {
        return None;
    }
    Some((chars[start..end].iter().collect(), end))
}

pub(crate) fn digits(chars: &[char], start: usize, n: usize) -> Option<u32> {
    let slice = chars.get(start..start + n)?;
    slice
        .iter()
        .try_fold(0u32, |acc, c| c.to_digit(10).map(|d| acc * 10 + d))
}

pub(crate) fn followed_by_digit(chars: &[char], at: usize) -> bool {
    chars.get(at).is_some_and(char::is_ascii_digit)
}

fn civil(year: u32, month: u32, day: u32) -> Option<Day> {
    ((1..=12).contains(&month) && (1..=31).contains(&day))
        .then(|| time::day_from_civil(year as i32, month, day))
}

pub(crate) fn take_iso_date(chars: &[char], start: usize) -> Option<(Day, usize)> {
    let year = digits(chars, start, 4)?;
    if chars.get(start + 4) != Some(&'-') || chars.get(start + 7) != Some(&'-') {
        return None;
    }
    let month = digits(chars, start + 5, 2)?;
    let day = digits(chars, start + 8, 2)?;
    if followed_by_digit(chars, start + 10) {
        return None;
    }
    Some((civil(year, month, day)?, start + 10))
}

const WEEKDAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const MONTHS: [&str; 12] = [
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
];

fn word3(chars: &[char], start: usize) -> Option<String> {
    let w = chars.get(start..start + 3)?;
    w.iter()
        .all(char::is_ascii_alphabetic)
        .then(|| w.iter().flat_map(|c| c.to_lowercase()).collect())
}

/// Parses `Mon, Jan 02, 2006` and `Mon, Jan 2, 2006`.
pub(crate) fn take_fzn_date(chars: &[char], start: usize) -> Option<(Day, usize)> {
    let weekday = word3(chars, start)?;
    if !WEEKDAYS.contains(&weekday.as_str())
        || chars.get(start + 3) != Some(&',')
        || chars.get(start + 4) != Some(&' ')
    {
        return None;
    }
    let month_name = word3(chars, start + 5)?;
    let month = MONTHS.iter().position(|m| *m == month_name)? as u32 + 1;
    if chars.get(start + 8) != Some(&' ') {
        return None;
    }
    let mut i = start + 9;
    let mut day = 0u32;
    let mut n = 0;
    while n < 2 && chars.get(i).is_some_and(char::is_ascii_digit) {
        day = day * 10 + chars[i].to_digit(10)?;
        i += 1;
        n += 1;
    }
    if n == 0 || chars.get(i) != Some(&',') || chars.get(i + 1) != Some(&' ') {
        return None;
    }
    let year = digits(chars, i + 2, 4)?;
    if followed_by_digit(chars, i + 6) {
        return None;
    }
    Some((civil(year, month, day)?, i + 6))
}

/// A word that stands for a date, and where it is in the text, slash included.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DateWord {
    start: usize,
    end: usize,
    day: Day,
}

const DATE_NAMES: [&str; 10] = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "today",
    "tomorrow",
    "yesterday",
];

/// Finds the words that stand for a date: a slash at the start of a word, then `today`,
/// `tomorrow`, `yesterday`, a day of the week (the next one after today, or with a `-` the last
/// one before it), or a number of days from today as `+3` or `-2`. Any beginning of a name that
/// only one name has will do: `/tom`, `/f`, `/-tu`. Anything else after a slash is left alone, a
/// path included. A word starts after whitespace, a bracket, or the `..` of a span.
fn date_words(text: &str, today: Day) -> Vec<DateWord> {
    let chars: Vec<char> = text.chars().collect();
    let at: Vec<usize> = text
        .char_indices()
        .map(|(at, _)| at)
        .chain([text.len()])
        .collect();
    (0..chars.len())
        .filter(|&i| at_word_start(&chars, i))
        .filter_map(|i| {
            let (day, end) = take_date_word(&chars, i, today)?;
            Some(DateWord {
                start: at[i],
                end: at[end],
                day,
            })
        })
        .collect()
}

/// Whether a word can start here, as a date word does: at the start, after whitespace or a
/// bracket, or straight after the `..` of a span.
pub(crate) fn at_word_start(chars: &[char], i: usize) -> bool {
    i == 0
        || chars[i - 1].is_whitespace()
        || chars[i - 1] == '('
        || (i >= 2 && chars[i - 2..i] == ['.', '.'])
}

/// A date word at `start`, slash included, and where it ends: at whitespace, at the `..` of a
/// span, and short of any punctuation that ends a sentence.
pub(crate) fn take_date_word(chars: &[char], start: usize, today: Day) -> Option<(Day, usize)> {
    if chars.get(start) != Some(&'/') {
        return None;
    }
    let mut end = start + 1;
    while end < chars.len() && !chars[end].is_whitespace() && !dots(chars, end) {
        end += 1;
    }
    while end > start + 1 && matches!(chars[end - 1], '.' | ',' | ';' | ':' | '!' | '?' | ')') {
        end -= 1;
    }
    let word: String = chars[start + 1..end].iter().collect();
    Some((date_named(&word, today)?, end))
}

/// Whether the `..` of a span starts here.
pub(crate) fn dots(chars: &[char], at: usize) -> bool {
    chars.get(at..at + 2) == Some(&['.', '.'])
}

/// Writes out the date each date word stands for, as `2026-10-16`.
pub fn expand_date_words(text: &str, today: Day) -> String {
    let mut out = String::with_capacity(text.len());
    let mut from = 0;
    for word in date_words(text, today) {
        out.push_str(&text[from..word.start]);
        out.push_str(&time::format_day(word.day));
        from = word.end;
    }
    out.push_str(&text[from..]);
    out
}

fn date_named(word: &str, today: Day) -> Option<Day> {
    if let Some(days) = word.strip_prefix('+').or_else(|| word.strip_prefix('-'))
        && days.bytes().all(|b| b.is_ascii_digit())
    {
        if !(1..=4).contains(&days.len()) {
            return None;
        }
        let days: Day = days.parse().ok()?;
        return Some(if word.starts_with('-') {
            today - days
        } else {
            today + days
        });
    }
    // With a `-` only the days of the week are named, as the last one before today.
    let (word, back) = match word.strip_prefix('-') {
        Some(word) => (word, true),
        None => (word, false),
    };
    if word.is_empty() || !word.bytes().all(|b| b.is_ascii_alphabetic()) {
        return None;
    }
    let word = word.to_ascii_lowercase();
    let names = if back {
        &DATE_NAMES[..7]
    } else {
        &DATE_NAMES[..]
    };
    let mut named = (0..names.len()).filter(|&i| names[i].starts_with(&word));
    let (name, None) = (named.next()?, named.next()) else {
        return None;
    };
    let weekday = time::weekday(today);
    Some(match names[name] {
        "today" => today,
        "tomorrow" => today + 1,
        "yesterday" => today - 1,
        // A day of the week is never today, that being what `/today` is for: it is the next one,
        // or the last one.
        _ if back => today - ((weekday + 6 - name) % 7) as Day - 1,
        _ => today + ((name + 6 - weekday) % 7) as Day + 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heads_and_bodies() {
        assert!(has_body("head\nbody"));
        assert!(!has_body("head\n  \n"));
        assert!(!has_body("head\n#house #plan\n"));
        assert!(has_body("head\n#house\n- and more"));
        assert!(has_body("head\n# A heading"));
        assert!(!has_body("head"));
    }

    #[test]
    fn fit_truncates_by_characters() {
        assert_eq!(fit("héllo", 10), "héllo");
        assert_eq!(fit("héllo world", 5), "héll…");
        assert_eq!(fit("x", 0), "");
    }

    #[test]
    fn date_words_stand_for_dates() {
        // A Tuesday.
        let today = time::day_from_civil(2026, 10, 13);
        let expand = |text: &str| expand_date_words(text, today);
        assert_eq!(expand("/today"), "2026-10-13");
        assert_eq!(
            expand("dentist /fri, then /tomorrow and (/yesterday)"),
            "dentist 2026-10-16, then 2026-10-14 and (2026-10-12)"
        );
        assert_eq!(expand("call back /yesterday."), "call back 2026-10-12.");
        assert_eq!(expand("/+3 /-2 /+10"), "2026-10-16 2026-10-11 2026-10-23");
        assert_eq!(
            expand("/tue /wed /Monday"),
            "2026-10-20 2026-10-14 2026-10-19",
            "a day of the week is the next one, never today"
        );
        assert_eq!(
            expand("/tod /tom /y /f /sa /su /m /w /th /tu"),
            "2026-10-13 2026-10-14 2026-10-12 2026-10-16 2026-10-17 2026-10-18 2026-10-19 \
             2026-10-14 2026-10-15 2026-10-20"
        );
        assert_eq!(
            expand("/-mon /-tu /-w /-f /-Sunday"),
            "2026-10-12 2026-10-06 2026-10-07 2026-10-09 2026-10-11",
            "with a minus it is the last one, never today"
        );
        assert_eq!(
            expand("/-mon 18:00../fri /today..."),
            "2026-10-12 18:00..2026-10-16 2026-10-13...",
            "a span's ends, and a word ends at two dots"
        );
        for untouched in [
            "/t /to /s /-t /-s /-today /-y /+mon",
            "/home/sam and/or w/friday",
            "/friday/night /fridays /fr1 / /+ /+12345 /-",
            "https://example.com/today x/tomorrow",
            "/network/events /feeds",
        ] {
            assert_eq!(expand(untouched), untouched);
            assert!(date_words(untouched, today).is_empty());
        }
        assert_eq!(
            date_words("é /fri", today),
            [DateWord {
                start: 3,
                end: 7,
                day: today + 3
            }]
        );
    }

    #[test]
    fn tokens_are_lowercased_words() {
        assert_eq!(
            tokens("Hello, World! #house @sam don't 2006"),
            ["hello", "world", "house", "sam", "don't", "2006"]
        );
        assert_eq!(
            tokens("weekly >> Mon, Jan 02, 2022 >> plan"),
            ["weekly", "mon", "jan", "02", "2022", "plan"]
        );
        assert_eq!(tokens("a b cd I 7"), ["cd"]);
    }

    #[test]
    fn urls_tokenise_to_their_host_only() {
        assert_eq!(
            tokens(
                "see https://www.Kettlebell.test/why-a-kettlebell/ and www.foo.org/x?y=1 or http://localhost:8080/p"
            ),
            [
                "see",
                "kettlebell.test",
                "and",
                "foo.org",
                "or",
                "localhost"
            ]
        );
        assert_eq!(url_host("https://www.Example.com/a"), "example.com");
        assert_eq!(url_host("www.foo.org"), "foo.org");
    }

    #[test]
    fn trigrams_are_padded_and_deduplicated() {
        assert_eq!(trigrams("go"), [" go", "go "]);
        assert_eq!(trigrams("aaa"), [" aa", "aa ", "aaa"]);
        assert!(trigrams("").is_empty());
    }

    #[test]
    fn tags_and_mentions() {
        let f = facts("#House and #a/b-c, not a#b nor # heading, @joe@house.test and @Sam. (#x)");
        assert_eq!(f.tags, ["a/b-c", "house", "x"]);
        assert_eq!(f.mentions, ["joe@house.test", "sam"]);
    }

    #[test]
    fn a_line_of_nothing_but_tags() {
        assert!(only_tags("#house"));
        assert!(only_tags("  #house #a/b-c "));
        for line in ["", "  ", "# Heading", "#house today", "today #house", "#"] {
            assert!(!only_tags(line), "{line:?}");
        }
    }

    #[test]
    fn a_task_is_a_text_that_starts_with_a_box() {
        assert_eq!(task("[ ] order oil"), Some(false));
        assert_eq!(task("[x] order oil\n#house"), Some(true));
        for text in [
            "order oil",
            " [ ] order oil",
            "[X] order oil",
            "Boiler\n- [ ] order oil",
        ] {
            assert_eq!(task(text), None, "{text:?}");
        }
        assert_eq!(tick("[ ] order oil", true), "[x] order oil");
        assert_eq!(
            tick("[x] order oil\n#house", false),
            "[ ] order oil\n#house"
        );
        assert_eq!(tick("order oil", false), "[ ] order oil");
        assert_eq!(tick("", false), "[ ] ", "the box and the space after it");
    }

    #[test]
    fn wikilinks_normalise_and_strip_aliases_and_sections() {
        let f = facts("see [[Some  Page|alias]] and [[Other#Section]] and [[]] and [[broken\n]]");
        assert_eq!(f.links, ["other", "some page"]);
    }

    #[test]
    fn urls_keep_case_and_drop_trailing_punctuation() {
        let f = facts("read https://Example.com/A?b=1, then www.foo.org. not http:// alone");
        assert_eq!(f.urls, ["https://Example.com/A?b=1", "www.foo.org"]);
    }

    #[test]
    fn dates_can_be_blanked_out() {
        assert_eq!(
            without_dates("dentist 2026-09-05, 16 teeth; Mon, Jan 02, 2006 x 2026-09"),
            "dentist  , 16 teeth;   x 2026-09"
        );
        assert_eq!(without_dates("2026-09-05"), " ");
    }

    #[test]
    fn dates_in_both_formats() {
        let f = facts(
            "on 2026-09-05 and Mon, Jan 02, 2006 and Wed, Feb 9, 2022; not 2026-13-01 or 12026-09-05",
        );
        assert_eq!(
            f.dates,
            [
                time::day_from_civil(2006, 1, 2),
                time::day_from_civil(2022, 2, 9),
                time::day_from_civil(2026, 9, 5)
            ]
        );
    }
}
