//! What is offered while a tag or a person is typed: the ones the space already has that start
//! with what is typed, and a key to take one. It is a part of its own because it is the one thing
//! the session keeps that follows the face's caret, which the session is otherwise never told:
//! the face says where the caret is after typing, and only typing opens what is offered, so that
//! a word walked past with the arrows is not offered for, and a word closed with Escape stays
//! closed until it is typed into again. Anything else asked for closes it.

use serde::Serialize;

use crate::score::Label;
use crate::text;

use super::{Action, Caret, Session};

/// The most that is offered at once.
const OFFERED: usize = 5;

/// What is offered to complete the tag or person the caret ends, while it is offered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Offer {
    /// The tags or the people, as they are written with their mark, `#food` or `@joe`: those of
    /// the space that start with the word, carried by the most notes first.
    pub words: Vec<String>,
    /// The one Tab or Enter takes, as an index into `words`.
    pub chosen: usize,
    /// Where the word's mark is, in characters, in the line or the head, so that a face can draw
    /// what is offered under the word.
    pub from: usize,
    /// Where the word ends, in characters, which is where the caret was when it was offered.
    #[serde(skip)]
    to: usize,
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
/// after it, and is a tag or a person as a note's text reads one, from the mark to the caret.
fn ending(text: &str, at: usize) -> Option<(usize, Label)> {
    let chars: Vec<char> = text.chars().collect();
    if at > chars.len() || chars.get(at).is_some_and(|c| !c.is_whitespace()) {
        return None;
    }
    let from = chars[..at]
        .iter()
        .rposition(|c| c.is_whitespace())
        .map_or(0, |i| i + 1);
    let (label, end) = match chars.get(from)? {
        '#' => text::take_tag(&chars, from + 1).map(|(tag, end)| (Label::Tag(tag), end))?,
        '@' => text::take_mention(&chars, from + 1).map(|(who, end)| (Label::Mention(who), end))?,
        _ => return None,
    };
    (end == at).then_some((from, label))
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
    /// has that the word the caret ends starts; after the caret has only moved, what was offered,
    /// for as long as the caret is still at the end of its word. The note being written does not
    /// count, since its word is the one still being typed, and the word alone is nothing to
    /// offer.
    pub(super) fn offered_at(&self, at: usize, was: Option<Offer>, typed: bool) -> Option<Offer> {
        if !typed {
            return was.filter(|offer| offer.to == at);
        }
        let (from, label) = ending(self.caret_text()?, at)?;
        let besides = self.field.as_ref().map(|field| field.id.as_str());
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
            words,
            chosen: 0,
            from,
            to: at,
        })
    }

    /// Puts the word offered, the chosen one or the one asked for, in place of the word the
    /// caret ends, with a space after it unless one follows already, and says where the caret
    /// goes: after that space.
    pub(super) fn take(&mut self, offer: Offer, which: Option<usize>) -> Option<Caret> {
        let word = offer.words.get(which.unwrap_or(offer.chosen))?;
        let chars: Vec<char> = self.caret_text()?.chars().collect();
        if offer.to > chars.len() {
            return None;
        }
        let before: String = chars[..offer.from].iter().collect();
        let after: String = chars[offer.to..].iter().collect();
        let space = if after.starts_with(char::is_whitespace) {
            ""
        } else {
            " "
        };
        let taken = format!("{before}{word}{space}{after}");
        let at = offer.from + word.chars().count() + 1;
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
