//! The note model.

use crate::time::Day;

/// Stable identity of a note. Opaque to the engine and assigned by whoever stores the note.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NoteId(pub String);

impl NoteId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NoteId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A note is one text. Its first line is its head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Note {
    pub id: NoteId,
    pub text: String,
    pub created: Option<Day>,
    pub updated: Option<Day>,
    pub archived: bool,
}

impl Note {
    pub fn new(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: NoteId::new(id),
            text: text.into(),
            created: None,
            updated: None,
            archived: false,
        }
    }

    /// The first line, trimmed.
    pub fn head(&self) -> &str {
        self.text.lines().next().unwrap_or("").trim()
    }

    /// Everything after the first line.
    pub fn body(&self) -> &str {
        self.text.split_once('\n').map_or("", |(_, body)| body)
    }

    /// The most recent day known for the note, if any.
    pub fn day(&self) -> Option<Day> {
        self.updated.or(self.created)
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
