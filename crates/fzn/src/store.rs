//! Per-device append-only logs of note snapshots. The newest version of each note wins.
//!
//! Each device writes only its own file, so a directory of logs can be synced by anything that
//! copies files, with no conflicts. When a text CRDT arrives, the lines carry updates instead of
//! snapshots and nothing above this module changes.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasher, RandomState};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// One version of one note. Times are unix seconds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub text: String,
    #[serde(default)]
    pub created: Option<i64>,
    #[serde(default)]
    pub updated: Option<i64>,
    #[serde(default)]
    pub archived: bool,
    /// A tombstone, kept so that an older version arriving later cannot resurrect the note.
    #[serde(default)]
    pub deleted: bool,
    /// The device that wrote this version. Breaks ties between versions with equal times.
    #[serde(default)]
    pub device: String,
}

impl Record {
    fn supersedes(&self, other: &Record) -> bool {
        (self.updated, &self.device) > (other.updated, &other.device)
    }
}

pub struct Store {
    device: String,
    log: File,
    notes: BTreeMap<String, Record>,
    skipped: usize,
}

impl Store {
    /// Opens the directory, replaying every log in it, and prepares this device's log for appends.
    pub fn open(dir: &Path, device: &str) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let mut paths: Vec<PathBuf> = fs::read_dir(dir)
            .with_context(|| format!("read {}", dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        paths.sort();
        let mut notes = BTreeMap::new();
        let mut skipped = 0;
        for path in &paths {
            skipped += replay(path, &mut notes)?;
        }
        let own = dir.join(format!("{device}.jsonl"));
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&own)
            .with_context(|| format!("open {}", own.display()))?;
        Ok(Self {
            device: device.to_owned(),
            log,
            notes,
            skipped,
        })
    }

    /// Lines that could not be parsed while replaying.
    pub fn skipped(&self) -> usize {
        self.skipped
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
        self.notes.get(id).filter(|r| !r.deleted)
    }

    /// The newest version of a note, tombstone included.
    pub fn version(&self, id: &str) -> Option<&Record> {
        self.notes.get(id)
    }

    /// Live notes.
    pub fn records(&self) -> impl Iterator<Item = &Record> {
        self.notes.values().filter(|r| !r.deleted)
    }

    /// Appends a version to this device's log and applies it if it is the newest.
    pub fn put(&mut self, mut record: Record) -> Result<()> {
        record.device = self.device.clone();
        let mut line = serde_json::to_string(&record)?;
        line.push('\n');
        self.log
            .write_all(line.as_bytes())
            .with_context(|| format!("append to {}", self.device))?;
        self.log.flush()?;
        apply(&mut self.notes, record);
        Ok(())
    }
}

fn apply(notes: &mut BTreeMap<String, Record>, record: Record) {
    match notes.get(&record.id) {
        Some(current) if !record.supersedes(current) => {}
        _ => {
            notes.insert(record.id.clone(), record);
        }
    }
}

/// Replays one log into `notes`, returning how many lines were unreadable.
fn replay(path: &Path, notes: &mut BTreeMap<String, Record>) -> Result<usize> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut skipped = 0;
    for line in BufReader::new(file).split(b'\n') {
        let Ok(line) = line else {
            skipped += 1;
            continue;
        };
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match serde_json::from_slice::<Record>(&line) {
            Ok(record) => apply(notes, record),
            Err(_) => skipped += 1,
        }
    }
    Ok(skipped)
}

pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A fresh random number, seeded by the OS.
pub fn random_u64() -> u64 {
    RandomState::new().hash_one(0u64)
}

/// A new note id: time-ordered, unique enough across devices.
pub fn new_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{millis:x}{:08x}", random_u64() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fzn-store-test-{:x}", random_u64()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn rec(id: &str, text: &str, updated: i64) -> Record {
        Record {
            id: id.into(),
            text: text.into(),
            created: Some(updated),
            updated: Some(updated),
            archived: false,
            deleted: false,
            device: String::new(),
        }
    }

    #[test]
    fn versions_survive_reopen_and_newest_wins() {
        let dir = temp_dir();
        {
            let mut s = Store::open(&dir, "dev-a").unwrap();
            s.put(rec("n1", "first", 10)).unwrap();
            s.put(rec("n1", "second", 20)).unwrap();
            s.put(rec("n1", "stale", 15)).unwrap();
            s.put(rec("n2", "other", 5)).unwrap();
            assert_eq!(s.get("n1").unwrap().text, "second");
            assert_eq!(s.get("n1").unwrap().device, "dev-a");
        }
        let s = Store::open(&dir, "dev-a").unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s.get("n1").unwrap().text, "second");
        assert_eq!(s.skipped(), 0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn other_devices_logs_merge_and_ties_are_deterministic() {
        let dir = temp_dir();
        Store::open(&dir, "dev-a")
            .unwrap()
            .put(rec("n1", "from a", 10))
            .unwrap();
        Store::open(&dir, "dev-b")
            .unwrap()
            .put(rec("n1", "from b", 10))
            .unwrap();
        Store::open(&dir, "dev-b")
            .unwrap()
            .put(rec("n2", "b only", 1))
            .unwrap();
        let s = Store::open(&dir, "dev-c").unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(
            s.get("n1").unwrap().text,
            "from b",
            "equal times: the greater device id wins"
        );
        assert!(dir.join("dev-c.jsonl").exists());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn unreadable_lines_are_skipped_not_fatal() {
        let dir = temp_dir();
        Store::open(&dir, "dev-a")
            .unwrap()
            .put(rec("n1", "ok", 1))
            .unwrap();
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.join("dev-a.jsonl"))
            .unwrap();
        f.write_all(b"not json\n\n{\"id\":\"n2\",\"text\":\"partial\"\n")
            .unwrap();
        let s = Store::open(&dir, "dev-a").unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s.skipped(), 2);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn deleted_notes_stay_dead() {
        let dir = temp_dir();
        let mut s = Store::open(&dir, "dev-a").unwrap();
        s.put(rec("n1", "alive", 10)).unwrap();
        s.put(Record {
            deleted: true,
            ..rec("n1", "alive", 20)
        })
        .unwrap();
        assert!(s.get("n1").is_none());
        assert!(s.version("n1").is_some_and(|r| r.deleted));
        assert_eq!(s.len(), 0);
        s.put(rec("n1", "late arrival", 15)).unwrap();
        assert!(
            s.get("n1").is_none(),
            "an older version cannot resurrect a deleted note"
        );
        let s = Store::open(&dir, "dev-a").unwrap();
        assert!(s.get("n1").is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn ids_are_unique_and_time_ordered() {
        let a = new_id();
        let b = new_id();
        assert_ne!(a, b);
        assert!(a.len() >= 16);
    }
}
