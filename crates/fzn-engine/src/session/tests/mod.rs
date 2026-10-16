//! The session's tests, by topic. They drive a session as a face would, through `act`, on a
//! machine whose clock has stopped, so that nothing depends on when they run. The helpers here
//! are the ones more than one topic uses.

use super::*;
use crate::time;
use std::cell::Cell;

mod apply;
mod field;
mod forms;
mod line;
mod order;
mod places;
mod restore;
mod shown;
mod tasks;

/// Midday on Tuesday 2026-10-13, in UTC.
const NOON: Moment = Moment {
    ms: 1_791_892_800_000,
    offset: 0,
};

/// A machine whose clock has stopped, and whose ids and randomness count up.
#[derive(Default)]
struct Stopped {
    ids: Cell<u32>,
    random: Cell<u8>,
}

impl Host for Stopped {
    fn now(&self) -> Moment {
        NOON
    }

    fn new_id(&self) -> String {
        self.ids.set(self.ids.get() + 1);
        format!("new-{:04}", self.ids.get())
    }

    fn random(&self, buf: &mut [u8]) {
        for byte in buf {
            self.random.set(self.random.get().wrapping_add(1));
            *byte = self.random.get();
        }
    }
}

fn session() -> Session {
    Session::new(
        Box::new(Stopped::default()),
        "dev",
        Weights::default(),
        Notes::new(),
    )
}

fn add(s: &mut Session, text: &str) -> Vec<Effect> {
    s.act(Action::Type(text.to_owned()));
    s.act(Action::Write)
}

/// A note with its own time, placed at the top: the list is in the order the notes were put,
/// the last first, and their dates do not hang on the clock. The version to keep goes out
/// with whatever is asked next.
fn put(s: &mut Session, id: &str, text: &str, links: &[&str], updated: i64) {
    s.commit(Record {
        links: links.iter().map(|&l| l.to_owned()).collect(),
        created: Some(Moment::utc(updated)),
        updated: Some(Moment::utc(updated)),
        ..Record::new(id, text)
    });
    s.refresh(true);
    s.land();
    s.derive();
}

fn heads(s: &Session) -> Vec<&str> {
    s.rows.iter().map(|r| r.head.as_str()).collect()
}

fn select(s: &mut Session, head: &str) {
    let i = s.rows.iter().position(|r| r.head == head).unwrap();
    s.act(Action::Select(Some(i)));
}

fn go_into(s: &mut Session, head: &str) {
    select(s, head);
    s.act(Action::GoIn);
}

/// Types onto the end of the line.
fn type_in(s: &mut Session, text: &str) {
    s.act(Action::Type(format!("{}{text}", s.line)));
}

fn clear(s: &mut Session) {
    s.act(Action::Type(String::new()));
}

fn labels(s: &Session) -> Vec<&'static str> {
    s.view().hints.iter().map(|h| h.label).collect()
}

fn path(s: &Session) -> Vec<&str> {
    s.view().path
}

/// The versions asked to be kept, put through the lines of a log and read back.
fn reopened<'a>(effects: impl IntoIterator<Item = &'a Effect>) -> Notes {
    let mut notes = Notes::new();
    for effect in effects {
        if let Effect::Keep(record) = effect {
            let line = record.line().unwrap();
            notes.apply(Record::read(line.as_bytes()).unwrap());
        }
    }
    notes
}

/// The versions an action asked to be kept.
fn kept(effects: &[Effect]) -> Vec<&Record> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Keep(r) => Some(r),
            _ => None,
        })
        .collect()
}

/// The field, as the view gives it: what it reads, whether it is new, and whether its body
/// is open.
fn field(s: &Session) -> Option<(String, bool, bool)> {
    s.view()
        .field
        .map(|field| (field.text.to_owned(), field.fresh.is_some(), field.whole))
}

/// A version written on another device, a minute or two after midday.
fn theirs(id: &str, text: &str, at: i64) -> Record {
    Record {
        created: Some(NOON),
        updated: Some(NOON.plus_secs(at)),
        stamp: Some(NOON.plus_secs(at)),
        placed: Some(NOON.plus_secs(at)),
        device: "3f9a01c2".into(),
        ..Record::new(id, text)
    }
}

fn edited(s: &mut Session, text: &str) -> Vec<Effect> {
    s.act(Action::Edited(text.to_owned()))
}

/// Three notes, the last put first, with what writing them asked for already gone out.
fn three(s: &mut Session) {
    put(s, "garden", "a garden", &[], 1_000);
    put(s, "house", "House", &[], 2_000);
    put(s, "boiler", "Boiler", &[], 3_000);
    s.act(Action::Select(None));
}

/// Three notes tagged `#house`, one of them by a line under its head as the importer files
/// tags, one that says the word without the tag, and one that has nothing to do with it.
fn house(s: &mut Session) {
    put(s, "boiler", "Boiler\n#house", &[], 1_000);
    put(s, "gutter", "clear the gutter #house", &[], 2_000);
    put(s, "roof", "fix the roof tiles #house", &[], 3_000);
    put(s, "plan", "house move plan", &[], 4_000);
    put(s, "cake", "cake recipe", &[], 5_000);
}

fn linked(s: &Session) -> Vec<bool> {
    s.rows.iter().map(|r| r.linked).collect()
}

/// A boiler tagged `#house`, with a note written in it yesterday and one today, and today
/// two notes that are not about it, one of them in `#house` too.
fn boiler(s: &mut Session) {
    let today = NOON.secs() - 3_600;
    put(s, "boiler", "Boiler\n#house", &[], NOON.secs() - 9 * 86_400);
    put(
        s,
        "service",
        "Service booked",
        &["boiler"],
        NOON.secs() - 86_400,
    );
    put(s, "pressure", "Pressure dropped #house", &["boiler"], today);
    put(s, "oil", "Order oil #house", &[], today);
    put(s, "plants", "Water the plants", &[], today);
}

/// Goes into what the line names, typed from an empty line.
fn go_to(s: &mut Session, typed: &str) {
    s.act(Action::Select(None));
    clear(s);
    type_in(s, typed);
    s.act(Action::GoIn);
}
