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
//!
//! Beside it is the part that gets a space from one device to another, in [`replica`]: the same
//! shape, with messages in and out in place of files and sockets. What it carries is sealed
//! under the space's key first, in [`seal`], so that a relay holds nothing it can read. The two
//! meet in a [`Device`], which is what a face that syncs drives in place of a bare session.

pub mod device;
mod engine;
mod index;
mod note;
mod query;
mod record;
pub mod replica;
mod score;
pub mod seal;
mod session;
pub mod text;
pub mod time;

pub use device::{Device, Editing, Kept, Todo};
pub use engine::Engine;
pub use note::{Note, NoteId};
pub use record::{Notes, Read, Record, links_on_save};
pub use score::{Reason, Scorer, Weights};
pub use seal::{Key, Opened, Seal};
pub use session::{Action, Effect, Field, Hint, Host, Row, Session, View};
pub use time::{Day, Moment};
