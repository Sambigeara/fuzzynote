//! The note model.

use crate::text;
use crate::time::{Day, Moment};

/// A note is one text. Its first line is its head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Note {
    /// Stable identity of the note. Opaque to the engine and assigned by whoever stores the note.
    pub id: String,
    pub text: String,
    /// Notes this one is linked to, by id. A link held here survives its target being renamed
    /// and adds no words to the text, unlike a `[[head]]` written into it.
    pub links: Vec<String>,
    /// When the note was written, to the moment, since a span typed on the line holds the notes
    /// written in it to the minute. Relevance reads only its day.
    pub created: Option<Moment>,
    pub updated: Option<Day>,
    pub archived: bool,
}

impl Note {
    pub fn new(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            text: text.into(),
            links: Vec::new(),
            created: None,
            updated: None,
            archived: false,
        }
    }

    /// The first line, trimmed.
    pub fn head(&self) -> &str {
        text::head(&self.text)
    }

    /// Everything after the first line.
    pub fn body(&self) -> &str {
        self.text.split_once('\n').map_or("", |(_, body)| body)
    }

    /// The day the note was written on, if it is known.
    pub fn written(&self) -> Option<Day> {
        self.created.map(Moment::day)
    }

    /// The most recent day known for the note, if any.
    pub fn day(&self) -> Option<Day> {
        self.updated.or(self.written())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_and_body_split_on_the_first_newline() {
        let n = Note::new("a", "  Shopping list \nmilk\neggs");
        assert_eq!(n.head(), "Shopping list");
        assert_eq!(n.body(), "milk\neggs");
        let single = Note::new("b", "just a line");
        assert_eq!(single.head(), "just a line");
        assert_eq!(single.body(), "");
    }
}
