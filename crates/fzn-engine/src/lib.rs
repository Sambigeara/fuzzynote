//! The fuzzynote engine: the whole app, with no screen and no keyboard.
//!
//! A face drives a [`Session`]. It says what was asked for, as an [`Action`], and gets back what
//! it has to do about it, as [`Effect`]s, and what to show, as a [`View`]. Which key or gesture
//! asks for what, and how the view is painted, are all a face decides. The engine does no I/O
//! and knows nothing about terminals, browsers, files or clocks: the notes come in as
//! [`Record`]s, the versions to keep go out as records, and the time comes from a [`Host`].
//!
//! Under the session is the part that finds things: an incrementally maintained index and the
//! scorers over it, in [`Engine`]. Every answer carries its reasons.

mod engine;
mod index;
mod note;
mod record;
mod score;
mod session;
pub mod text;
pub mod time;

pub use engine::{Engine, Hit, SearchOptions, Stats};
pub use note::{Note, NoteId};
pub use record::{Notes, Record, links_on_save};
pub use score::{Reason, Weights};
pub use session::{Action, Effect, Hint, Host, Row, Session, View};
pub use time::{Day, Moment};
