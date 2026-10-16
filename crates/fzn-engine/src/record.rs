//! A note as it is kept. One version of a note is one line of a log, and the newest version wins.
//!
//! A line is one JSON object: `v`, the version of this format, and then the note. This is also
//! what `fzn export` writes, and it stays the way notes get in and out whatever the logs turn
//! into. Where the lines are kept is a face's business: files under a terminal, something else
//! in a browser. What a line says, and which of two versions is the newer, is settled here, so
//! that every device agrees. When a text CRDT arrives, the lines carry updates instead of
//! snapshots and nothing above this module changes.
//!
//! A version has two sides, each with a clock of its own, as the original kept a note's content
//! and its position in separate registers. What the note says, its text, links, times and
//! whether it is archived or deleted, is stamped by `stamp`; where it sits in the list, `after`,
//! is stamped by `placed`. Two versions merge side by side, the newer of each, so that an edit
//! on one device and a move on another both hold once the two have met. A version that changes
//! one side carries the other as it was written, with its clock and what breaks a tie on it, so
//! that the side carried is decided as if the version had never been written. The list is the
//! walk of the tree that `after` makes, in [`Notes::order`].
//!
//! In a log the line is sealed, and what is sealed is exactly the plain line, so that a sealed
//! log and an export differ by the envelope and nothing else. Logs from before sealing hold plain
//! lines, and those are read as they are.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::note::Note;
use crate::seal::{Opened, Seal};
use crate::session::Host;
use crate::text;
use crate::time::Moment;

/// The version of the line format this build writes. Lines with no version are from before
/// there was one, and hold their times as unix seconds. A line with a later version is left
/// alone rather than misread. Version 2 added the place, which a build of version 1 would drop
/// from every note it rewrote. Version 3 keeps times to the millisecond, which a build of
/// version 2 would drop, and so take two versions of a note written in one second for a tie
/// that devices of version 3 had told apart.
const FORMAT: u32 = 3;

/// One version of one note.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub text: String,
    /// Ids of the notes this one is linked to. The links the app makes are held here and not in
    /// the text, so that they survive a rename and add no words.
    #[serde(default)]
    pub links: Vec<String>,
    #[serde(default)]
    pub created: Option<Moment>,
    /// When the text last changed. Archiving and deleting leave it alone.
    #[serde(default)]
    pub updated: Option<Moment>,
    /// When this version was written, which decides the newest. Logs from before stamps have
    /// none, and `updated` stands in.
    #[serde(default)]
    pub stamp: Option<Moment>,
    /// The note this one sits after in the list, or none for the top. A note it names that no
    /// version is held for is not there yet, so until it arrives this one sits at the top.
    #[serde(default)]
    pub after: Option<String>,
    /// When the note was put where it sits, which decides the newest place as `stamp` decides
    /// the newest text. Notes from before places have none, and `created` stands in.
    #[serde(default)]
    pub placed: Option<Moment>,
    #[serde(default)]
    pub archived: bool,
    /// A tombstone, kept so that an older version arriving later cannot resurrect the note.
    #[serde(default)]
    pub deleted: bool,
    /// The device that wrote what this version says, which a version that only moves the note
    /// carries over with its stamp. Breaks ties between versions with equal stamps.
    #[serde(default)]
    pub device: String,
}

/// What one line of a log came to.
// A read is taken apart as soon as it is made and never kept, so the version is not boxed: that
// would be an allocation a line for nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read {
    /// A version of a note.
    Version(Record),
    /// An envelope this seal opens around what this build does not read, a version of a later
    /// format. The key is the space's, since it opened it, and the line is left as it is.
    Later,
    /// An envelope this seal does not open.
    Shut,
    /// Not a version: a compaction header, or garbage.
    Other,
}

/// A line of a log: the version of the format, then the note.
#[derive(Serialize, Deserialize)]
struct Versioned<R> {
    #[serde(default)]
    v: u32,
    #[serde(flatten)]
    record: R,
}

impl Record {
    /// A version of a note with this text and nothing else: no links, no times, at the top, and
    /// with no stamp or writer, which writing it fills in.
    pub fn new(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            text: text.into(),
            links: Vec::new(),
            created: None,
            updated: None,
            stamp: None,
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: String::new(),
        }
    }

    /// The first line, trimmed.
    pub fn head(&self) -> &str {
        text::head(&self.text)
    }

    /// The note as relevance sees it: the text, the links, when it was written, and the day it
    /// was last changed on.
    pub fn note(&self) -> Note {
        Note {
            id: self.id.clone(),
            text: self.text.clone(),
            links: self.links.clone(),
            created: self.created,
            updated: self.updated.map(Moment::day),
            archived: self.archived,
        }
    }

    /// The version as a line of a log, newline included.
    pub fn line(&self) -> serde_json::Result<String> {
        let mut line = serde_json::to_string(&Versioned {
            v: FORMAT,
            record: self,
        })?;
        line.push('\n');
        Ok(line)
    }

    /// Reads a line of a log, if it is one this build understands.
    pub fn read(line: &[u8]) -> Option<Self> {
        serde_json::from_slice::<Versioned<Record>>(line)
            .ok()
            .filter(|line| line.v <= FORMAT)
            .map(|line| line.record)
    }

    /// Reads one line of a log, sealed or plain. An envelope is opened with the seal and what
    /// is inside read as a plain line; a line that is not an envelope is read as a plain line.
    /// `Later` is an envelope this seal opens around what this build does not read, and `Shut`
    /// one it does not open. `Other` is any other line that is not a version: a compaction
    /// header, or garbage.
    pub fn read_line(seal: &Seal, line: &str) -> Read {
        match seal.open(line) {
            Opened::Plain => Record::read(line.as_bytes()).map_or(Read::Other, Read::Version),
            Opened::Text(plain) => {
                Record::read(plain.as_bytes()).map_or(Read::Later, Read::Version)
            }
            Opened::Shut => Read::Shut,
        }
    }

    /// The version sealed as one line of a log, no newline.
    pub fn sealed(&self, seal: &Seal, host: &dyn Host) -> serde_json::Result<String> {
        let mut plain = self.line()?;
        plain.pop();
        Ok(seal.seal(&plain, host))
    }

    /// When the version was written, in unix milliseconds.
    fn written(&self) -> Option<i64> {
        self.stamp.or(self.updated).map(|m| m.ms)
    }

    fn supersedes(&self, other: &Record) -> bool {
        (self.written(), &self.device) > (other.written(), &other.device)
    }

    /// Whether this version's place is newer. A tie is broken by the note it is after, so that a
    /// place is decided by nothing but itself, however many versions have carried it.
    fn moves(&self, other: &Record) -> bool {
        (self.placed, &self.after) > (other.placed, &other.after)
    }

    /// Whether two versions say the same, wherever they put the note.
    pub fn says_the_same(&self, other: &Record) -> bool {
        self.text == other.text
            && self.links == other.links
            && self.created == other.created
            && self.updated == other.updated
            && self.archived == other.archived
            && self.deleted == other.deleted
    }

    /// Where the note falls among those placed after the same note, the newest placed first, in
    /// unix milliseconds.
    fn rank(&self) -> Option<i64> {
        self.placed.or(self.created).or(self.updated).map(|m| m.ms)
    }
}

/// A moment past `latest`, in unix milliseconds, or `now` if that is past it already. The step
/// is a millisecond, so that a burst of versions runs ahead of the clock by a millisecond each
/// at most.
fn past(latest: Option<i64>, now: Moment) -> Moment {
    match latest {
        Some(held) if held >= now.ms => Moment {
            ms: held.saturating_add(1),
            ..now
        },
        _ => now,
    }
}

/// The notes of a space: the newest of each side of each, tombstones included. Versions can be
/// applied in any order, from any number of logs, and the same notes come out.
#[derive(Default)]
pub struct Notes {
    newest: BTreeMap<String, Record>,
}

impl Notes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes from a version each side that is newer than the one held, and says whether it took
    /// either.
    pub fn apply(&mut self, record: Record) -> bool {
        let Some(held) = self.newest.get_mut(&record.id) else {
            self.newest.insert(record.id.clone(), record);
            return true;
        };
        match (record.supersedes(held), record.moves(held)) {
            (false, false) => return false,
            (true, true) => *held = record,
            (true, false) => {
                *held = Record {
                    after: held.after.take(),
                    placed: held.placed,
                    ..record
                }
            }
            (false, true) => (held.after, held.placed) = (record.after, record.placed),
        }
        true
    }

    /// Live notes, not counting tombstones.
    pub fn len(&self) -> usize {
        self.records().count()
    }

    pub fn is_empty(&self) -> bool {
        self.records().next().is_none()
    }

    /// A live note.
    pub fn get(&self, id: &str) -> Option<&Record> {
        self.newest.get(id).filter(|r| !r.deleted)
    }

    /// The newest version of a note, tombstone included.
    pub fn version(&self, id: &str) -> Option<&Record> {
        self.newest.get(id)
    }

    /// Live notes.
    pub fn records(&self) -> impl Iterator<Item = &Record> {
        self.newest.values().filter(|r| !r.deleted)
    }

    /// The note a note sits under: the one it was placed after, if a version of that is held.
    fn under<'a>(&self, record: &'a Record) -> Option<&'a str> {
        record
            .after
            .as_deref()
            .filter(|after| self.newest.contains_key(*after))
    }

    /// A stamp for a new version of a note that is sure to supersede the one held, even when
    /// two changes land in the same millisecond or another device's clock runs ahead.
    pub fn next_stamp(&self, id: &str, now: Moment) -> Moment {
        past(self.version(id).and_then(Record::written), now)
    }

    /// A moment to place a note at that is sure to supersede the place held for it, and to put
    /// it before every note placed after the same note so far, so that a note placed after
    /// another sits directly under it, wherever the clocks that placed the others were.
    pub fn next_placed(&self, id: &str, after: Option<&str>, now: Moment) -> Moment {
        let after = after.filter(|after| self.newest.contains_key(*after));
        let latest = self
            .newest
            .values()
            .filter(|r| {
                r.id == id
                    || r.after.as_deref() == after
                    || after.is_none() && self.under(r).is_none()
            })
            .filter_map(Record::rank)
            .max();
        past(latest, now)
    }

    /// Every note in the order it is listed, tombstones and archived notes among them, so that
    /// what was placed after one keeps its place: the walk of the tree that `after` makes, as
    /// in the original. The top is every note placed at the top, or after a note not held, and
    /// each note is followed by the notes placed after it, and theirs, the newest placed first.
    /// Two devices moving at once can make a cycle, which nothing at the top reaches. It is
    /// listed as if it were at the top, with what hangs from it, so that no note is lost.
    pub fn order(&self) -> Vec<&Record> {
        // A note is named by where it is in id order, the order the notes are held in, so that
        // nothing is hashed: this runs each time the list is made.
        let all: Vec<&Record> = self.newest.values().collect();
        let under: Vec<Option<usize>> = all
            .iter()
            .map(|r| {
                let after = r.after.as_deref()?;
                all.binary_search_by(|held| held.id.as_str().cmp(after))
                    .ok()
            })
            .collect();
        let mut tops = Vec::new();
        let mut hanging = vec![Vec::new(); all.len()];
        for (i, after) in under.iter().enumerate() {
            match after {
                Some(after) => hanging[*after].push(i),
                None => tops.push(i),
            }
        }
        // The newest placed first, and of two placed in the same millisecond the later id.
        let rank: Vec<_> = all.iter().map(|r| Reverse((r.rank(), &r.id))).collect();
        let newest = |a: &usize, b: &usize| rank[*a].cmp(&rank[*b]);
        tops.sort_unstable_by(newest);
        for notes in &mut hanging {
            notes.sort_unstable_by(newest);
        }
        let mut seen = vec![false; all.len()];
        let mut walked = Vec::with_capacity(all.len());
        let walk = |head: usize, seen: &mut [bool], walked: &mut Vec<usize>| {
            let mut stack = vec![head];
            while let Some(i) = stack.pop() {
                if !std::mem::replace(&mut seen[i], true) {
                    walked.push(i);
                    stack.extend(hanging[i].iter().rev());
                }
            }
        };
        let mut runs = Vec::with_capacity(tops.len());
        for &top in &tops {
            let start = walked.len();
            walk(top, &mut seen, &mut walked);
            runs.push((top, start..walked.len()));
        }
        if walked.len() < all.len() {
            let mut strays: Vec<usize> = (0..all.len()).filter(|&i| !seen[i]).collect();
            strays.sort_unstable_by(newest);
            for stray in strays {
                if seen[stray] {
                    continue;
                }
                // A note nothing at the top reaches hangs from a cycle. The cycle is walked
                // from the note that climbing from this one comes back to.
                let mut climbed = HashSet::new();
                let mut head = stray;
                while climbed.insert(head) {
                    // It cannot fire: what hangs from a walked note is walked, so every note
                    // above an unwalked one is unwalked too, and no unwalked note is a top.
                    head = under[head].expect("only a note at the top hangs from nothing");
                }
                let start = walked.len();
                walk(head, &mut seen, &mut walked);
                runs.push((head, start..walked.len()));
            }
            runs.sort_by(|(a, _), (b, _)| newest(a, b));
        }
        runs.into_iter()
            .flat_map(|(_, run)| walked[run].iter().map(|&i| all[i]))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seal::{Counting, Key};
    use std::cell::Cell;

    fn rec(id: &str, text: &str, updated: i64) -> Record {
        Record {
            created: Some(Moment::utc(updated)),
            updated: Some(Moment::utc(updated)),
            device: "dev-a".into(),
            ..Record::new(id, text)
        }
    }

    #[test]
    fn the_newest_version_wins_whatever_order_they_come_in() {
        let versions = [
            rec("n1", "first", 10),
            rec("n1", "second", 20),
            rec("n1", "stale", 15),
            Record {
                device: "dev-b".into(),
                ..rec("n1", "second, from a later device", 20)
            },
        ];
        let mut forwards = Notes::new();
        let mut backwards = Notes::new();
        for v in &versions {
            forwards.apply(v.clone());
        }
        for v in versions.iter().rev() {
            backwards.apply(v.clone());
        }
        for notes in [&forwards, &backwards] {
            assert_eq!(
                notes.get("n1").unwrap().text,
                "second, from a later device",
                "equal times: the greater device id wins"
            );
            assert_eq!(notes.len(), 1);
        }
        assert!(
            !forwards.apply(versions[3].clone()),
            "a version already held is not news"
        );
    }

    #[test]
    fn a_deleted_note_stays_dead() {
        let mut notes = Notes::new();
        notes.apply(rec("n1", "alive", 10));
        notes.apply(Record {
            deleted: true,
            ..rec("n1", "alive", 20)
        });
        assert!(notes.get("n1").is_none());
        assert!(notes.version("n1").is_some_and(|r| r.deleted));
        assert!(notes.is_empty());
        assert!(!notes.apply(rec("n1", "late arrival", 15)));
        assert!(notes.get("n1").is_none());
    }

    #[test]
    fn a_new_stamp_always_supersedes_the_version_held() {
        let mut notes = Notes::new();
        let now = Moment::utc(1_000);
        notes.apply(Record {
            stamp: Some(Moment::utc(4_600)),
            ..rec("n1", "written by a clock running ahead", 10)
        });
        for text in ["first change", "second change in the same millisecond"] {
            let stamp = notes.next_stamp("n1", now);
            assert!(notes.apply(Record {
                stamp: Some(stamp),
                ..rec("n1", text, 10)
            }));
        }
        assert_eq!(
            notes.get("n1").unwrap().stamp.map(|m| m.ms),
            Some(4_600_002)
        );
        assert_eq!(notes.next_stamp("unseen", now), now);
    }

    #[test]
    fn two_versions_in_the_same_second_are_told_apart_by_their_milliseconds() {
        let at = |ms: i64| Some(Moment { ms, offset: 0 });
        let earlier = Record {
            stamp: at(1_791_892_800_100),
            device: "dev-z".into(),
            ..rec("boiler", "Boiler", 10)
        };
        let later = Record {
            stamp: at(1_791_892_800_900),
            device: "dev-a".into(),
            ..rec("boiler", "Boiler: service due", 10)
        };
        // To the second they tie, and the writer id would have given it to the earlier one.
        assert!(earlier.device > later.device);
        for versions in [[&earlier, &later], [&later, &earlier]] {
            let mut notes = Notes::new();
            for version in versions {
                notes.apply(version.clone());
            }
            assert_eq!(notes.get("boiler").unwrap().text, "Boiler: service due");
        }
    }

    #[test]
    fn a_line_of_this_format_reads_whatever_the_precision_of_its_times() {
        let line = |stamp: &str| {
            format!(r#"{{"v":3,"id":"oil","text":"Order oil","stamp":"{stamp}","device":"dev-a"}}"#)
        };
        for (stamp, ms) in [
            ("2026-10-13T12:00:00.250+01:00", 1_791_889_200_250),
            ("2026-10-13T12:00:00.25Z", 1_791_892_800_250),
            ("2026-10-13T12:00:00.250999999Z", 1_791_892_800_250),
            ("2026-10-13T12:00:00Z", 1_791_892_800_000),
        ] {
            let record = Record::read(line(stamp).as_bytes()).unwrap();
            assert_eq!(record.stamp.map(|m| m.ms), Some(ms), "{stamp}");
        }
        let written = line("2026-10-13T12:00:00.250+01:00");
        let record = Record::read(written.as_bytes()).unwrap();
        assert_eq!(
            Record::read(record.line().unwrap().as_bytes()),
            Some(record)
        );
    }

    #[test]
    fn a_second_precision_line_from_before_still_reads_and_merges_as_it_did() {
        // Two versions from version 2, in one second, decided by the writer id as they were.
        let line = |text: &str, device: &str| {
            format!(
                r#"{{"v":2,"id":"oil","text":"{text}","stamp":"2026-10-13T12:00:00Z","device":"{device}"}}"#
            )
        };
        let a = Record::read(line("Order oil", "dev-a").as_bytes()).unwrap();
        let b = Record::read(line("Order oil by Friday", "dev-b").as_bytes()).unwrap();
        assert_eq!(a.stamp, Some(Moment::utc(1_791_892_800)));
        for versions in [[&a, &b], [&b, &a]] {
            let mut notes = Notes::new();
            for version in versions {
                notes.apply(version.clone());
            }
            assert_eq!(notes.get("oil").unwrap().text, "Order oil by Friday");
        }
        // A version written now, a millisecond into that second, is newer than both.
        let mut notes = Notes::new();
        notes.apply(b.clone());
        let stamp = notes.next_stamp("oil", Moment::utc(1_791_892_800));
        assert_eq!(stamp.ms, 1_791_892_800_001);
    }

    #[test]
    fn a_line_reads_back_as_it_was_written_and_older_lines_are_still_read() {
        let record = Record {
            created: Some(Moment {
                ms: 1_791_934_200_250,
                offset: 3600,
            }),
            links: vec!["n0".into()],
            after: Some("n0".into()),
            placed: Some(Moment::utc(30)),
            ..rec("new", "written now\nwith a body", 20)
        };
        let line = record.line().unwrap();
        assert!(line.starts_with(r#"{"v":3,"id":"new","text":"written now\nwith a body","#));
        assert!(line.contains(
            r#""created":"2026-10-14T00:30:00.250+01:00","updated":"1970-01-01T00:00:20.000Z","#
        ));
        assert!(line.contains(r#""after":"n0","placed":"1970-01-01T00:00:30.000Z","#));
        assert!(line.ends_with('\n') && line.matches('\n').count() == 1);
        assert_eq!(Record::read(line.as_bytes()), Some(record));

        // A line from before versions holds its times in unix seconds.
        let old = Record::read(
            br#"{"id":"old","text":"from before","created":10,"updated":30,"stamp":40,"device":"dev-b"}"#,
        )
        .unwrap();
        assert_eq!(
            (old.created, old.updated, old.stamp),
            (
                Some(Moment::utc(10)),
                Some(Moment::utc(30)),
                Some(Moment::utc(40))
            )
        );
        // A line from before places has none, and sits by when it was written.
        let unplaced = Record::read(
            br#"{"v":1,"id":"v1","text":"Order oil","created":"1970-01-01T00:00:10Z","device":"dev-b"}"#,
        )
        .unwrap();
        assert_eq!((&unplaced.after, unplaced.placed), (&None, None));
        assert_eq!(unplaced.rank(), Some(10_000));
        // A line from a later format is left alone rather than misread.
        assert_eq!(
            Record::read(br#"{"v":4,"id":"new","text":"from the future"}"#),
            None
        );
        assert_eq!(Record::read(b"not json"), None);
    }

    #[test]
    fn a_line_of_a_log_reads_back_whether_sealed_or_plain() {
        let seal = Seal::new(Key::from_bytes([7; 32]));
        let host = Counting(Cell::new(0));
        let record = Record {
            links: vec!["n0".into()],
            ..rec("n1", "Boiler: service due\nbook it before winter", 20)
        };

        let sealed = record.sealed(&seal, &host).unwrap();
        assert!(!sealed.contains('\n'));
        assert!(crate::seal::is_sealed(&sealed));
        assert_eq!(
            Record::read_line(&seal, &sealed),
            Read::Version(record.clone())
        );

        let plain = record.line().unwrap();
        assert_eq!(
            Record::read_line(&seal, plain.trim_end()),
            Read::Version(record.clone()),
            "a line from before sealing is read as it is"
        );

        let elsewhere = Seal::new(Key::from_bytes([8; 32]));
        assert_eq!(Record::read_line(&elsewhere, &sealed), Read::Shut);

        let header = r#"{"compacts":["3f9a01c2"],"snapshot":293}"#;
        assert_eq!(Record::read_line(&seal, header), Read::Other);
        assert_eq!(Record::read_line(&seal, "not json"), Read::Other);
        let later = r#"{"v":99,"id":"n1","text":"from a later build"}"#;
        assert_eq!(
            Record::read_line(&seal, &seal.seal(later, &host)),
            Read::Later,
            "opened, and left for a build that reads it"
        );
        assert_eq!(Record::read_line(&seal, later), Read::Other);
    }

    fn placed(id: &str, after: Option<&str>, at: i64) -> Record {
        Record {
            after: after.map(str::to_owned),
            placed: Some(Moment::utc(at)),
            ..rec(id, id, 10)
        }
    }

    fn order(notes: &Notes) -> Vec<&str> {
        notes.order().iter().map(|r| r.id.as_str()).collect()
    }

    #[test]
    fn notes_from_before_places_are_in_the_order_they_were_written_newest_first() {
        let mut notes = Notes::new();
        for (id, created) in [("boiler", 20), ("house", 30), ("garden", 10)] {
            notes.apply(rec(id, id, created));
        }
        assert_eq!(order(&notes), ["house", "boiler", "garden"]);
        notes.apply(placed("oil", Some("garden"), 40));
        assert_eq!(order(&notes), ["house", "boiler", "garden", "oil"]);
    }

    #[test]
    fn a_note_placed_after_one_not_yet_here_is_at_the_top_until_it_arrives() {
        let mut notes = Notes::new();
        notes.apply(placed("boiler", None, 20));
        notes.apply(placed("oil", Some("house"), 50));
        assert_eq!(order(&notes), ["oil", "boiler"]);
        notes.apply(placed("house", None, 10));
        assert_eq!(order(&notes), ["boiler", "house", "oil"]);
    }

    #[test]
    fn the_text_and_the_place_of_a_note_merge_apart() {
        let edit = Record {
            stamp: Some(Moment::utc(30)),
            ..rec("house", "House insurance", 30)
        };
        let moved = Record {
            stamp: Some(Moment::utc(10)),
            device: "dev-b".into(),
            ..placed("house", Some("boiler"), 40)
        };
        for versions in [[&edit, &moved], [&moved, &edit]] {
            let mut notes = Notes::new();
            for version in versions {
                assert!(notes.apply(version.clone()));
            }
            let held = notes.get("house").unwrap();
            assert_eq!(held.text, "House insurance");
            assert_eq!(held.after.as_deref(), Some("boiler"));
            assert!(!notes.apply(moved.clone()) && !notes.apply(edit.clone()));
        }
    }

    #[test]
    fn placed_under_one_note_keeps_the_newest_first_order_at_millisecond_spacing() {
        let mut notes = Notes::new();
        notes.apply(placed("boiler", None, 1_791_892_800));
        // Written under the boiler one after another inside a second, by a clock that has not
        // moved, and then by one that moved a millisecond.
        let mut now = Moment::utc(1_791_892_801);
        for id in ["gauge", "oil", "service"] {
            let at = notes.next_placed(id, Some("boiler"), now);
            notes.apply(Record {
                after: Some("boiler".into()),
                placed: Some(at),
                ..rec(id, id, 10)
            });
            now.ms += 1;
        }
        let placed_ms = |id: &str| notes.get(id).unwrap().placed.unwrap().ms;
        assert_eq!(placed_ms("oil") - placed_ms("gauge"), 1);
        assert_eq!(placed_ms("service") - placed_ms("oil"), 1);
        assert_eq!(placed_ms("service") - 1_791_892_801_000, 2, "on the clock");
        assert_eq!(order(&notes), ["boiler", "service", "oil", "gauge"]);
    }

    #[test]
    fn a_cycle_is_listed_as_if_it_were_at_the_top() {
        let mut notes = Notes::new();
        notes.apply(placed("boiler", None, 20));
        notes.apply(placed("house", Some("garden"), 30));
        notes.apply(placed("garden", Some("house"), 40));
        notes.apply(placed("oil", Some("garden"), 50));
        notes.apply(placed("radiators", None, 35));
        notes.apply(placed("shed", Some("shed"), 25));
        assert_eq!(
            order(&notes),
            ["garden", "oil", "house", "radiators", "shed", "boiler"]
        );
    }

    #[test]
    fn a_record_is_the_note_relevance_sees() {
        let record = Record {
            links: vec!["hub".into()],
            archived: true,
            ..rec("n1", "  a head \nand a body", 86_400 * 3)
        };
        assert_eq!(record.head(), "a head");
        let note = record.note();
        assert_eq!(note.head(), "a head");
        assert_eq!(note.links, ["hub"]);
        assert_eq!((note.written(), note.updated), (Some(3), Some(3)));
        assert!(note.archived);
    }
}
