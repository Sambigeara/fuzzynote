//! Per-device append-only logs of note versions, as files in one directory.
//!
//! Each device writes only its own file, so a directory of logs can be synced by anything that
//! copies files, with no conflicts. Logs are reread as they grow, so what another device or
//! another instance writes turns up without a restart. What a line says, and which version of a
//! note is the newest, is the engine's business: this module only keeps the lines.
//!
//! A file of exported lines put into a space is read as one more log.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use fzn_engine::{Moment, Notes, Record};

use crate::system;

/// The logs of one space, and this device's own among them.
pub struct Logs {
    dir: PathBuf,
    device: String,
    log: File,
    /// How many bytes of each log have been read.
    read: BTreeMap<PathBuf, u64>,
    skipped: usize,
}

impl Logs {
    /// Opens the directory and prepares this device's log for appends. Nothing is read yet.
    pub fn open(dir: &Path, device: &str) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let own = dir.join(format!("{device}.jsonl"));
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&own)
            .with_context(|| format!("open {}", own.display()))?;
        Ok(Self {
            dir: dir.to_owned(),
            device: device.to_owned(),
            log,
            read: BTreeMap::new(),
            skipped: 0,
        })
    }

    /// Every version appended to the logs since they were last read, by any device or instance,
    /// this one included.
    pub fn read(&mut self) -> Result<Vec<Record>> {
        let mut paths: Vec<PathBuf> = fs::read_dir(&self.dir)
            .with_context(|| format!("read {}", self.dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        paths.sort();
        let mut records = Vec::new();
        for path in paths {
            let from = self.read.get(&path).copied().unwrap_or(0);
            let (upto, skipped) = read_log(&path, from, &mut records)?;
            self.read.insert(path, upto);
            self.skipped += skipped;
        }
        Ok(records)
    }

    /// Appends a version to this device's log.
    pub fn append(&mut self, record: &Record) -> Result<()> {
        self.log
            .write_all(record.line()?.as_bytes())
            .with_context(|| format!("append to {}", self.device))?;
        self.log.flush()?;
        Ok(())
    }

    /// Lines that could not be read.
    pub fn skipped(&self) -> usize {
        self.skipped
    }
}

/// A space on disk: its logs, and the notes they add up to. This is what the importers and the
/// exporter work on. The app takes the two apart, and gives the notes to a session.
pub struct Store {
    notes: Notes,
    logs: Logs,
}

impl Store {
    /// Opens the directory, reading every log in it, and prepares this device's log for appends.
    pub fn open(dir: &Path, device: &str) -> Result<Self> {
        let mut logs = Logs::open(dir, device)?;
        let mut notes = Notes::new();
        for record in logs.read()? {
            notes.apply(record);
        }
        Ok(Self { notes, logs })
    }

    pub fn into_parts(self) -> (Notes, Logs) {
        (self.notes, self.logs)
    }

    /// The notes the logs add up to.
    pub fn notes(&self) -> &Notes {
        &self.notes
    }

    /// Lines that could not be read.
    pub fn skipped(&self) -> usize {
        self.logs.skipped()
    }

    /// A stamp for a new version of a note that is sure to supersede the one held.
    pub fn next_stamp(&self, id: &str) -> Moment {
        self.notes.next_stamp(id, system::now())
    }

    /// Appends a version to this device's log and takes it in if it is the newest.
    pub fn put(&mut self, mut record: Record) -> Result<()> {
        record.device = self.logs.device.clone();
        self.logs.append(&record)?;
        self.notes.apply(record);
        Ok(())
    }

    /// Writes every live note, archived ones included, as the lines a log holds, oldest first.
    pub fn export(&self, out: &mut impl Write) -> Result<usize> {
        let mut records: Vec<&Record> = self.notes.records().collect();
        records.sort_by(|a, b| a.created.cmp(&b.created).then_with(|| a.id.cmp(&b.id)));
        for record in &records {
            out.write_all(record.line()?.as_bytes())?;
        }
        out.flush()?;
        Ok(records.len())
    }
}

/// Reads one log from byte `from`, adding its versions to `records`. Returns how far it got and
/// how many lines were unreadable.
fn read_log(path: &Path, from: u64, records: &mut Vec<Record>) -> Result<(u64, usize)> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = file.metadata()?.len();
    // A log only grows. One that shrank was replaced, so it is read again from the start.
    let from = if len < from { 0 } else { from };
    if len == from {
        return Ok((from, 0));
    }
    file.seek(SeekFrom::Start(from))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read {}", path.display()))?;
    // A last line with no newline is still being written. It is left for the next read.
    let complete = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    let mut skipped = 0;
    for line in bytes[..complete].split(|&b| b == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match Record::read(line) {
            Some(record) => records.push(record),
            None => skipped += 1,
        }
    }
    Ok((from + complete as u64, skipped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::{now_secs, random_u64};

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fzn-store-test-{:x}", random_u64()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

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
            assert_eq!(s.notes().get("n1").unwrap().text, "second");
            assert_eq!(s.notes().get("n1").unwrap().device, "dev-a");
        }
        let s = Store::open(&dir, "dev-a").unwrap();
        assert_eq!(s.notes().len(), 2);
        assert_eq!(s.notes().get("n1").unwrap().text, "second");
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
        assert_eq!(s.notes().len(), 2);
        assert_eq!(
            s.notes().get("n1").unwrap().text,
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
        assert_eq!(s.notes().len(), 1);
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
        assert!(s.notes().get("n1").is_none());
        assert!(s.notes().version("n1").is_some_and(|r| r.deleted));
        assert_eq!(s.notes().len(), 0);
        s.put(rec("n1", "late arrival", 15)).unwrap();
        assert!(
            s.notes().get("n1").is_none(),
            "an older version cannot resurrect a deleted note"
        );
        let s = Store::open(&dir, "dev-a").unwrap();
        assert!(s.notes().get("n1").is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_stamp_decides_the_newest_and_a_local_change_always_wins() {
        let dir = temp_dir();
        let mut s = Store::open(&dir, "dev-a").unwrap();
        let far_ahead = now_secs() + 3600;
        s.put(Record {
            stamp: Some(Moment::utc(far_ahead)),
            ..rec("n1", "written by a clock running ahead", 10)
        })
        .unwrap();
        for text in ["first change", "second change in the same second"] {
            let stamp = s.next_stamp("n1");
            s.put(Record {
                stamp: Some(stamp),
                ..rec("n1", text, 10)
            })
            .unwrap();
            assert_eq!(s.notes().get("n1").unwrap().text, text);
        }
        assert_eq!(
            s.notes().get("n1").unwrap().stamp.map(|m| m.secs),
            Some(far_ahead + 2)
        );
        assert_eq!(s.notes().get("n1").unwrap().updated, Some(Moment::utc(10)));
        assert!(s.next_stamp("unseen").secs <= now_secs());
        let s = Store::open(&dir, "dev-a").unwrap();
        assert_eq!(
            s.notes().get("n1").unwrap().text,
            "second change in the same second"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reading_takes_in_what_has_been_appended_since() {
        let dir = temp_dir();
        let ids =
            |records: Vec<Record>| -> Vec<String> { records.into_iter().map(|r| r.id).collect() };
        let mut a = Logs::open(&dir, "dev-a").unwrap();
        assert!(a.read().unwrap().is_empty());
        a.append(&rec("n1", "mine", 10)).unwrap();
        assert_eq!(
            ids(a.read().unwrap()),
            ["n1"],
            "its own lines too: a second instance on the device shares the log"
        );
        assert!(a.read().unwrap().is_empty(), "and each line once");

        let mut b = Logs::open(&dir, "dev-b").unwrap();
        b.append(&rec("n1", "theirs, newer", 20)).unwrap();
        b.append(&rec("n2", "theirs", 5)).unwrap();
        assert_eq!(ids(a.read().unwrap()), ["n1", "n2"]);
        assert!(a.read().unwrap().is_empty());

        // A half-written line waits for the rest of it.
        let mut own = OpenOptions::new()
            .append(true)
            .open(dir.join("dev-a.jsonl"))
            .unwrap();
        let line = rec("n3", "from another instance", 30).line().unwrap();
        let (head, tail) = line.split_at(line.len() / 2);
        own.write_all(head.as_bytes()).unwrap();
        assert!(a.read().unwrap().is_empty());
        own.write_all(tail.as_bytes()).unwrap();
        assert_eq!(ids(a.read().unwrap()), ["n3"]);
        assert_eq!(a.skipped(), 0);

        // A log that shrank was replaced, so it is read again from the start.
        let replaced = rec("n4", "replaced", 40).line().unwrap();
        fs::write(dir.join("dev-b.jsonl"), replaced).unwrap();
        assert_eq!(ids(a.read().unwrap()), ["n4"]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn lines_carry_a_version_and_older_ones_are_still_read() {
        let dir = temp_dir();
        let mut s = Store::open(&dir, "dev-a").unwrap();
        s.put(Record {
            created: Some(Moment {
                secs: 1_791_934_200,
                offset: 3600,
            }),
            ..rec("new", "written now", 20)
        })
        .unwrap();
        let written = fs::read_to_string(dir.join("dev-a.jsonl")).unwrap();
        assert!(written.starts_with(r#"{"v":1,"id":"new","text":"written now","#));
        assert!(written.contains(
            r#""created":"2026-10-14T00:30:00+01:00","updated":"1970-01-01T00:00:20Z","#
        ));

        // A log from before versions, with its times in unix seconds, and a line from a later
        // format, which is left alone rather than misread.
        fs::write(
            dir.join("dev-b.jsonl"),
            concat!(
                "{\"id\":\"old\",\"text\":\"from before\",\"created\":10,\"updated\":30,\"stamp\":40,\"device\":\"dev-b\"}\n",
                "{\"v\":2,\"id\":\"new\",\"text\":\"from the future\",\"stamp\":\"2999-01-01T00:00:00Z\"}\n",
            ),
        )
        .unwrap();
        let s = Store::open(&dir, "dev-a").unwrap();
        let old = s.notes().get("old").unwrap();
        assert_eq!(
            (old.created, old.updated, old.stamp),
            (
                Some(Moment::utc(10)),
                Some(Moment::utc(30)),
                Some(Moment::utc(40))
            )
        );
        assert_eq!(s.notes().get("new").unwrap().text, "written now");
        assert_eq!(s.skipped(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_export_is_a_log_another_space_can_read() {
        let dir = temp_dir();
        let mut s = Store::open(&dir.join("a"), "dev-a").unwrap();
        s.put(rec("n2", "second", 20)).unwrap();
        s.put(rec("n1", "first", 10)).unwrap();
        s.put(Record {
            archived: true,
            ..rec("n1", "first, done", 30)
        })
        .unwrap();
        s.put(rec("gone", "deleted", 5)).unwrap();
        s.put(Record {
            deleted: true,
            ..rec("gone", "deleted", 50)
        })
        .unwrap();
        let mut out = Vec::new();
        assert_eq!(s.export(&mut out).unwrap(), 2);
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text.lines().count(), 2, "one line a note, and no history");

        fs::create_dir_all(dir.join("b")).unwrap();
        fs::write(dir.join("b").join("from-a.jsonl"), &text).unwrap();
        let b = Store::open(&dir.join("b"), "dev-b").unwrap();
        assert_eq!(b.notes().len(), 2);
        assert_eq!(b.notes().get("n1"), s.notes().get("n1"));
        assert!(b.notes().get("n1").unwrap().archived);
        fs::remove_dir_all(&dir).unwrap();
    }
}
