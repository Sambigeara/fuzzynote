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

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn take_tag(chars: &[char], start: usize) -> Option<(String, usize)> {
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

fn take_mention(chars: &[char], start: usize) -> Option<(String, usize)> {
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

fn take_url(chars: &[char], start: usize) -> Option<(String, usize)> {
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

fn digits(chars: &[char], start: usize, n: usize) -> Option<u32> {
    let slice = chars.get(start..start + n)?;
    slice
        .iter()
        .try_fold(0u32, |acc, c| c.to_digit(10).map(|d| acc * 10 + d))
}

fn followed_by_digit(chars: &[char], at: usize) -> bool {
    chars.get(at).is_some_and(char::is_ascii_digit)
}

fn civil(year: u32, month: u32, day: u32) -> Option<Day> {
    ((1..=12).contains(&month) && (1..=31).contains(&day))
        .then(|| time::day_from_civil(year as i32, month, day))
}

fn take_iso_date(chars: &[char], start: usize) -> Option<(Day, usize)> {
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
fn take_fzn_date(chars: &[char], start: usize) -> Option<(Day, usize)> {
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

#[cfg(test)]
mod tests {
    use super::*;

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
