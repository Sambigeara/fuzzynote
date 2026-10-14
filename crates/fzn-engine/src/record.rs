//! A note as it is kept. One version of a note is one line of a log, and the newest version wins.
//!
//! A line is one JSON object: `v`, the version of this format, and then the note. This is also
//! what `fzn export` writes, and it stays the way notes get in and out whatever the logs turn
//! into. Where the lines are kept is a face's business: files under a terminal, something else
//! in a browser. What a line says, and which of two versions is the newer, is settled here, so
//! that every device agrees. When a text CRDT arrives, the lines carry updates instead of
//! snapshots and nothing above this module changes.
//!
//! In a log the line is sealed, and what is sealed is exactly the plain line, so that a sealed
//! log and an export differ by the envelope and nothing else. Logs from before sealing hold plain
//! lines, and those are read as they are.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::engine::Engine;
use crate::note::{Note, NoteId};
use crate::seal::{Opened, Seal};
use crate::session::Host;
use crate::time::Moment;

/// The version of the line format this build writes. Lines with no version are from before
/// there was one, and hold their times as unix seconds. A line with a later version is left
/// alone rather than misread.
const FORMAT: u32 = 1;

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
    #[serde(default)]
    pub archived: bool,
    /// A tombstone, kept so that an older version arriving later cannot resurrect the note.
    #[serde(default)]
    pub deleted: bool,
    /// The device that wrote this version. Breaks ties between versions with equal stamps.
    #[serde(default)]
    pub device: String,
}

/// What one line of a log came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Read {
    /// A version of a note.
    Version(Record),
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
    /// The first line, trimmed.
    pub fn head(&self) -> &str {
        self.text.lines().next().unwrap_or("").trim()
    }

    /// The note as relevance sees it: the text, the links, and days in place of moments.
    pub fn note(&self) -> Note {
        Note {
            id: NoteId::new(self.id.clone()),
            text: self.text.clone(),
            links: self.links.iter().cloned().map(NoteId::new).collect(),
            created: self.created.map(Moment::day),
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
    /// `Shut` is an envelope this seal does not open. `Other` is anything that is not a version:
    /// a compaction header, or garbage.
    pub fn read_line(seal: &Seal, line: &str) -> Read {
        let version = |plain: &str| match Record::read(plain.as_bytes()) {
            Some(record) => Read::Version(record),
            None => Read::Other,
        };
        match seal.open(line) {
            Opened::Plain => version(line),
            Opened::Text(plain) => version(&plain),
            Opened::Shut => Read::Shut,
        }
    }

    /// The version sealed as one line of a log, no newline.
    pub fn sealed(&self, seal: &Seal, host: &dyn Host) -> serde_json::Result<String> {
        let mut plain = self.line()?;
        plain.pop();
        Ok(seal.seal(&plain, host))
    }

    fn written(&self) -> Option<i64> {
        self.stamp.or(self.updated).map(|m| m.secs)
    }

    fn supersedes(&self, other: &Record) -> bool {
        (self.written(), &self.device) > (other.written(), &other.device)
    }
}

/// The notes of a space: the newest version of each, tombstones included. Versions can be
/// applied in any order, from any number of logs, and the same notes come out.
#[derive(Default)]
pub struct Notes {
    newest: BTreeMap<String, Record>,
}

impl Notes {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes a version if it is the newest of its note, and says whether it was.
    pub fn apply(&mut self, record: Record) -> bool {
        match self.newest.get(&record.id) {
            Some(held) if !record.supersedes(held) => false,
            _ => {
                self.newest.insert(record.id.clone(), record);
                true
            }
        }
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

    /// A stamp for a new version of a note that is sure to supersede the one held, even when
    /// two changes land in the same second or another device's clock runs ahead.
    pub fn next_stamp(&self, id: &str, now: Moment) -> Moment {
        match self.newest.get(id).and_then(Record::written) {
            Some(held) if held >= now.secs => now.plus(held - now.secs + 1),
            _ => now,
        }
    }
}

/// The links a note holds once its text is saved: what it held, what the new text names in
/// brackets, and no longer what only the old text named.
pub fn links_on_save(
    engine: &Engine,
    id: &str,
    old: &str,
    new: &str,
    links: &[String],
) -> Vec<String> {
    let held: Vec<NoteId> = links.iter().cloned().map(NoteId::new).collect();
    engine
        .links_on_save(&NoteId::new(id.to_owned()), old, new, &held)
        .into_iter()
        .map(|l| l.0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seal::{Counting, Key};
    use std::cell::Cell;

    fn rec(id: &str, text: &str, updated: i64) -> Record {
        Record {
            id: id.into(),
            text: text.into(),
            links: Vec::new(),
            created: Some(Moment::utc(updated)),
            updated: Some(Moment::utc(updated)),
            stamp: None,
            archived: false,
            deleted: false,
            device: "dev-a".into(),
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
        for text in ["first change", "second change in the same second"] {
            let stamp = notes.next_stamp("n1", now);
            assert!(notes.apply(Record {
                stamp: Some(stamp),
                ..rec("n1", text, 10)
            }));
        }
        assert_eq!(notes.get("n1").unwrap().stamp, Some(Moment::utc(4_602)));
        assert_eq!(notes.next_stamp("unseen", now), now);
    }

    #[test]
    fn a_line_reads_back_as_it_was_written_and_older_lines_are_still_read() {
        let record = Record {
            created: Some(Moment {
                secs: 1_791_934_200,
                offset: 3600,
            }),
            links: vec!["n0".into()],
            ..rec("new", "written now\nwith a body", 20)
        };
        let line = record.line().unwrap();
        assert!(line.starts_with(r#"{"v":1,"id":"new","text":"written now\nwith a body","#));
        assert!(line.contains(
            r#""created":"2026-10-14T00:30:00+01:00","updated":"1970-01-01T00:00:20Z","#
        ));
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
        // A line from a later format is left alone rather than misread.
        assert_eq!(
            Record::read(br#"{"v":2,"id":"new","text":"from the future"}"#),
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
        assert_eq!(
            Record::read_line(&seal, &seal.seal(header, &host)),
            Read::Other,
            "a sealed header is still not a version"
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
        assert_eq!(note.links, [NoteId::new("hub")]);
        assert_eq!((note.created, note.updated), (Some(3), Some(3)));
        assert!(note.archived);
    }
}
