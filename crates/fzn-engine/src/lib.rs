//! The fuzzynote engine: notes, an incrementally maintained index, and explainable relevance.
//!
//! The engine does no I/O and knows nothing about terminals, browsers or storage. Callers feed it
//! notes, ask it questions, and render the answers. Every answer carries its reasons.

mod engine;
mod index;
mod note;
mod score;
pub mod text;
pub mod time;

pub use engine::{Engine, Hit, SearchOptions, Stats};
pub use note::{Note, NoteId};
pub use score::{Reason, Weights};
pub use time::Day;
