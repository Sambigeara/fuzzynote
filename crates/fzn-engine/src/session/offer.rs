//! What is offered while a tag or a person is typed, or a note's name inside brackets: the ones
//! the space already has that start with what is typed, and a key to take one. It is a part of
//! its own because it is the one thing the session keeps that follows the face's caret, which the
//! session is otherwise never told: the face says where the caret is after typing, and only typing
//! opens what is offered, so that a word walked past with the arrows is not offered for, and a
//! word closed with Escape stays closed until it is typed into again. Anything else asked for
//! closes it.

use serde::Serialize;

use crate::score::Label;
use crate::text;

use super::{Action, Caret, Session};

/// The most that is offered at once.
const OFFERED: usize = 5;

/// What is offered to complete the tag, person or bracketed name the caret ends, while it is
/// offered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Offer {
    /// The tags or the people, as they are written with their mark, `#food` or `@joe`: those of
    /// the space that start with the word, the one typed whole first and then those carried by the
    /// most notes. Or the names of notes, as brackets name them and their rows read them, without
    /// the box and the labels that end them, cut as an arrow's head is: the one typed whole first
    /// and then those of the notes most linked to.
    pub words: Vec<String>,
    /// The one Tab or Enter takes, as an index into `words`.
    pub chosen: usize,
    /// Where the word's mark is, in characters, in the line or the head, so that a face can draw
    /// what is offered under the word. For a name it is where the name starts, after the
    /// brackets, so that a face draws a name as it draws a tag, under what it completes.
    pub from: usize,
    /// Where the word ends, in characters, which is where the caret was when it was offered.
    #[serde(skip)]
    to: usize,
    /// What taking each writes, which for a name is the name whole and not as it is drawn.
    #[serde(skip)]
    written: Vec<String>,
    /// Whether these are names, which taking closes with brackets rather than a space.
    #[serde(skip)]
    heads: bool,
}

impl Offer {
    /// The choice moved by this many, stopping at the first and the last.
    pub(super) fn chosen_by(self, by: i64) -> Self {
        let last = self.words.len().saturating_sub(1) as i64;
        Self {
            chosen: (self.chosen as i64 + by).clamp(0, last) as usize,
            ..self
        }
    }
}

/// The tag or person whose end is at `at`, in characters, as the index holds it, lowercased, and
/// where its mark is: the word the caret is at the end of starts with `#` or `@`, has a letter
/// after it, and is a tag or a person as a note's text reads one, from the mark to the caret; a
/// word inside code is not one. On the line the word may start with a `!` before its mark, which
/// leaves the label out.
fn ending(text: &str, at: usize, on_line: bool) -> Option<(usize, Label)> {
    let chars: Vec<char> = text.chars().collect();
    if at > chars.len() || chars.get(at).is_some_and(|c| !c.is_whitespace()) {
        return None;
    }
    if text::in_code(&chars, at) {
        return None;
    }
    let from = chars[..at]
        .iter()
        .rposition(|c| c.is_whitespace())
        .map_or(0, |i| i + 1);
    let from = if on_line && chars.get(from) == Some(&'!') {
        from + 1
    } else {
        from
    };
    let (label, end) = match chars.get(from)? {
        '#' => text::take_tag(&chars, from + 1).map(|(tag, end)| (Label::Tag(tag), end))?,
        '@' => text::take_mention(&chars, from + 1).map(|(who, end)| (Label::Mention(who), end))?,
        _ => return None,
    };
    (end == at).then_some((from, label))
}

/// Where the name being typed in brackets starts, in characters, if the caret at `at` is after an
/// opening `[[` on its line that nothing has closed, read as a note's text reads brackets, with
/// code passed over.
fn bracketed(chars: &[char], at: usize) -> Option<usize> {
    let line = chars[..at]
        .iter()
        .rposition(|&c| c == '\n')
        .map_or(0, |i| i + 1);
    let code = text::code(chars);
    let pair = |i: usize, c: char| chars[i] == c && chars.get(i + 1) == Some(&c) && i + 1 < at;
    let mut open = None;
    let mut i = line;
    while i < at {
        if code.iter().any(|r| r.contains(&i)) {
            i += 1;
        } else if open.is_none() && pair(i, '[') {
            open = Some(i + 2);
            i += 2;
        } else if open.is_some() && pair(i, ']') {
            open = None;
            i += 2;
        } else {
            i += 1;
        }
    }
    open
}

/// Whether the brackets open before the caret at `at` are closed further on along its line, by a
/// `]]` that is not straight after the caret, before any other `[[`: the caret is then inside a
/// name already written, which taking would close a second time.
fn closed_later(chars: &[char], at: usize) -> bool {
    let end = chars[at..]
        .iter()
        .position(|&c| c == '\n')
        .map_or(chars.len(), |i| at + i);
    let rest = &chars[at..end];
    for (k, pair) in rest.windows(2).enumerate() {
        match pair {
            ['[', '['] => return false,
            [']', ']'] => return k > 0,
            _ => {}
        }
    }
    false
}

/// The name being typed from `from` to the caret at `at`, normalised as a bracketed name is, a
/// space typed after it kept so that a word is known to be done. There is none if nothing but
/// spaces is typed, if another `[[` or a `]]` comes between, if a `#` or a `|` does, which would
/// make the rest a section or another name, if the caret is in code, or if it is in a word.
fn head_typed(chars: &[char], from: usize, at: usize) -> Option<String> {
    let typed = &chars[from..at];
    let inside = typed.windows(2).any(|w| w == ['[', '['] || w == [']', ']']);
    let after = chars.get(at).is_none_or(|&c| c.is_whitespace() || c == ']');
    if inside || !after || typed.iter().any(|&c| c == '#' || c == '|') || text::in_code(chars, at) {
        return None;
    }
    let mut head = text::normalise_head(&typed.iter().collect::<String>());
    if head.is_empty() {
        return None;
    }
    if typed.last().is_some_and(|c| c.is_whitespace()) {
        head.push(' ');
    }
    Some(head)
}

impl Session {
    /// The text the caret is in where a word can be offered for: the line, or the head of the
    /// note under the cursor while its body is shut. An open body is its editor's.
    fn caret_text(&self) -> Option<&str> {
        match &self.field {
            None => Some(&self.line),
            Some(field) if !field.whole => Some(field.head()),
            Some(_) => None,
        }
    }

    /// What is offered now that the face says the caret is at `at`: after typing, what the space
    /// has that the word the caret ends starts, or inside brackets the names that what is typed in
    /// them starts; after the caret has only moved, what was offered, for as long as the caret is
    /// still at the end of its word. The note being written does not count, since its word is the
    /// one still being typed, and the word alone is nothing to offer.
    pub(super) fn offered_at(&self, at: usize, was: Option<Offer>, typed: bool) -> Option<Offer> {
        if !typed {
            return was.filter(|offer| offer.to == at);
        }
        let text = self.caret_text()?;
        let besides = self.field.as_ref().map(|field| field.id.as_str());
        let chars: Vec<char> = text.chars().collect();
        if at > chars.len() {
            return None;
        }
        // Inside brackets closed further on, the caret is in a name already written, where
        // nothing is offered. Inside open ones a name is, and where none is, as when a `#` makes
        // the rest a section, a tag or a person is, as a note's text would read one there.
        if let Some(from) = bracketed(&chars, at) {
            if closed_later(&chars, at) {
                return None;
            }
            if let Some(offer) = self.names_offered(&chars, from, at, besides) {
                return Some(offer);
            }
        }
        let (from, label) = ending(text, at, self.field.is_none())?;
        let words = self.engine.labels_starting(&label, besides, OFFERED);
        let alone = match &label {
            Label::Tag(tag) => format!("#{tag}"),
            Label::Mention(who) => format!("@{who}"),
            _ => String::new(),
        };
        if words.is_empty() || words == [alone] {
            return None;
        }
        Some(Offer {
            written: words.clone(),
            words,
            chosen: 0,
            from,
            to: at,
            heads: false,
        })
    }

    /// What is offered for the name typed in brackets from `from` to the caret at `at`: the names
    /// it starts, unless it is a name no note has, or the only one it starts.
    fn names_offered(
        &self,
        chars: &[char],
        from: usize,
        at: usize,
        besides: Option<&str>,
    ) -> Option<Offer> {
        let head = head_typed(chars, from, at)?;
        let written = self.engine.names_starting(&head, besides, OFFERED);
        if written.is_empty() || written.len() == 1 && text::normalise_head(&written[0]) == head {
            return None;
        }
        let words = written.iter().map(|name| text::fit(name, 40)).collect();
        Some(Offer {
            words,
            chosen: 0,
            from,
            to: at,
            written,
            heads: true,
        })
    }

    /// Puts the word offered, the chosen one or the one asked for, in place of the word the
    /// caret ends, with a space after it unless one follows already, and says where the caret
    /// goes: after that space. A name is closed with brackets instead, as many as do not follow
    /// it already, and the caret goes after them.
    pub(super) fn take(&mut self, offer: Offer, which: Option<usize>) -> Option<Caret> {
        let word = offer.written.get(which.unwrap_or(offer.chosen))?;
        let chars: Vec<char> = self.caret_text()?.chars().collect();
        if offer.to > chars.len() {
            return None;
        }
        let before: String = chars[..offer.from].iter().collect();
        let after: String = chars[offer.to..].iter().collect();
        let end = match (offer.heads, after.starts_with(char::is_whitespace)) {
            (true, _) if after.starts_with("]]") => "",
            (true, _) if after.starts_with(']') => "]",
            (true, _) => "]]",
            (false, true) => "",
            (false, false) => " ",
        };
        let taken = format!("{before}{word}{end}{after}");
        let at = offer.from + word.chars().count() + if offer.heads { 2 } else { 1 };
        match self.field.as_ref() {
            None => {
                self.carry_out(Action::Type(taken));
                Some(Caret {
                    line: Some(at),
                    head: None,
                })
            }
            Some(field) => {
                let text = match field.text.split_once('\n') {
                    Some((_, body)) => format!("{taken}\n{body}"),
                    None => taken,
                };
                self.on_field(Action::Edited(text));
                Some(Caret {
                    line: None,
                    head: Some(at),
                })
            }
        }
    }
}
