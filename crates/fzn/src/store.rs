//! Per-device append-only logs of note snapshots. The newest version of each note wins.
//!
//! Each device writes only its own file, so a directory of logs can be synced by anything that
//! copies files, with no conflicts. Logs are reread as they grow, so what another device or
//! another instance writes turns up without a restart. When a text CRDT arrives, the lines carry
//! updates instead of snapshots and nothing above this module changes.
//!
//! A line is one JSON object: `v`, the version of this format, and then the note. This is also
//! what `fzn export` writes, and it stays the way notes get in and out whatever the logs turn
//! into. A file of these lines put into a space is read as one more log.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::hash::{BuildHasher, RandomState};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use fzn_engine::time::{self, Day};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The version of the line format this build writes. Lines with no version are from before
/// there was one, and hold their times as unix seconds. A line with a later version is left
/// alone rather than misread.
const FORMAT: u32 = 1;

const SECS_PER_DAY: i64 = 86_400;

/// A moment, and how far from UTC the clock that recorded it was. The offset keeps the day the
/// moment fell on where it was written, whatever zone it is read in. Written as RFC 3339,
/// `2026-10-13T10:12:56+01:00`, so that a line can be read without the app.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Moment {
    /// Unix seconds.
    pub secs: i64,
    /// Seconds east of UTC.
    pub offset: i32,
}

impl Moment {
    pub fn now() -> Self {
        Self::local(now_secs())
    }

    /// A moment as this machine's clock shows it.
    pub fn local(secs: i64) -> Self {
        Self {
            secs,
            offset: local_offset(secs),
        }
    }

    pub fn utc(secs: i64) -> Self {
        Self { secs, offset: 0 }
    }

    /// A time of day on a day, by this machine's clock.
    pub fn on(day: Day, secs_into_day: i64) -> Self {
        let wall = i64::from(day) * SECS_PER_DAY + secs_into_day;
        let offset = local_offset(wall);
        Self {
            secs: wall - i64::from(offset),
            offset,
        }
    }

    /// The day it was where the moment was recorded.
    pub fn day(self) -> Day {
        time::day_from_unix_secs(self.wall())
    }

    pub fn plus(self, secs: i64) -> Self {
        Self {
            secs: self.secs.saturating_add(secs),
            ..self
        }
    }

    fn wall(self) -> i64 {
        self.secs.saturating_add(i64::from(self.offset))
    }

    /// Reads RFC 3339. Fractions of a second are dropped.
    fn parse(s: &str) -> Option<Self> {
        let b = s.as_bytes();
        let num = |from: usize, n: usize| -> Option<i64> {
            b.get(from..from + n)?.iter().try_fold(0i64, |acc, c| {
                c.is_ascii_digit().then(|| acc * 10 + i64::from(c - b'0'))
            })
        };
        let is = |at: usize, c: u8| b.get(at) == Some(&c);
        if !(is(4, b'-') && is(7, b'-') && is(13, b':') && is(16, b':'))
            || !matches!(b.get(10), Some(b'T' | b't' | b' '))
        {
            return None;
        }
        let (month, day) = (num(5, 2)?, num(8, 2)?);
        let (hour, minute, second) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 {
            return None;
        }
        let mut at = 19;
        if is(at, b'.') {
            at += 1;
            while b.get(at).is_some_and(u8::is_ascii_digit) {
                at += 1;
            }
        }
        let offset = match b.get(at)? {
            b'Z' | b'z' if at + 1 == b.len() => 0,
            sign @ (b'+' | b'-') if at + 6 == b.len() && is(at + 3, b':') => {
                let east = num(at + 1, 2)? * 3600 + num(at + 4, 2)? * 60;
                if *sign == b'+' { east } else { -east }
            }
            _ => return None,
        };
        let days = i64::from(time::day_from_civil(
            num(0, 4)? as i32,
            month as u32,
            day as u32,
        ));
        Some(Self {
            secs: days * SECS_PER_DAY + hour * 3600 + minute * 60 + second - offset,
            offset: offset as i32,
        })
    }
}

impl fmt::Display for Moment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let wall = self.wall();
        let (y, m, d) = time::civil_from_day(time::day_from_unix_secs(wall));
        let secs = wall.rem_euclid(SECS_PER_DAY);
        write!(
            f,
            "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
            secs / 3600,
            secs % 3600 / 60,
            secs % 60
        )?;
        if self.offset == 0 {
            return f.write_str("Z");
        }
        let (sign, east) = if self.offset < 0 {
            ('-', -self.offset)
        } else {
            ('+', self.offset)
        };
        write!(f, "{sign}{:02}:{:02}", east / 3600, east % 3600 / 60)
    }
}

impl Serialize for Moment {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Moment {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = Moment;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an RFC 3339 time, or unix seconds")
            }

            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Moment, E> {
                Moment::parse(s).ok_or_else(|| E::custom(format!("not an RFC 3339 time: {s:?}")))
            }

            fn visit_i64<E: serde::de::Error>(self, secs: i64) -> Result<Moment, E> {
                Ok(Moment::utc(secs))
            }

            fn visit_u64<E: serde::de::Error>(self, secs: u64) -> Result<Moment, E> {
                i64::try_from(secs)
                    .map(Moment::utc)
                    .map_err(|_| E::custom("unix seconds out of range"))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// This machine's offset from UTC at a moment, in seconds east. UTC where the system cannot say.
#[cfg(unix)]
fn local_offset(secs: i64) -> i32 {
    let mut tm = std::mem::MaybeUninit::<libc::tm>::uninit();
    let t = secs as libc::time_t;
    // SAFETY: localtime_r reads `t` and fills the whole of `tm` through the pointers it is given,
    // and keeps neither. `tm` is read only once it has said it succeeded.
    let tm = unsafe {
        if libc::localtime_r(&t, tm.as_mut_ptr()).is_null() {
            return 0;
        }
        tm.assume_init()
    };
    tm.tm_gmtoff as i32
}

#[cfg(not(unix))]
fn local_offset(_secs: i64) -> i32 {
    0
}

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

impl Record {
    fn written(&self) -> Option<i64> {
        self.stamp.or(self.updated).map(|m| m.secs)
    }

    fn supersedes(&self, other: &Record) -> bool {
        (self.written(), &self.device) > (other.written(), &other.device)
    }
}

pub struct Store {
    dir: PathBuf,
    device: String,
    log: File,
    notes: BTreeMap<String, Record>,
    /// How many bytes of each log have been replayed.
    replayed: BTreeMap<PathBuf, u64>,
    skipped: usize,
}

impl Store {
    /// Opens the directory, replaying every log in it, and prepares this device's log for appends.
    pub fn open(dir: &Path, device: &str) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let own = dir.join(format!("{device}.jsonl"));
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&own)
            .with_context(|| format!("open {}", own.display()))?;
        let mut store = Self {
            dir: dir.to_owned(),
            device: device.to_owned(),
            log,
            notes: BTreeMap::new(),
            replayed: BTreeMap::new(),
            skipped: 0,
        };
        store.sync()?;
        Ok(store)
    }

    /// Replays whatever has been appended to the logs since they were last read, by any device
    /// or instance, and returns the ids of the notes whose newest version changed.
    pub fn sync(&mut self) -> Result<Vec<String>> {
        let mut paths: Vec<PathBuf> = fs::read_dir(&self.dir)
            .with_context(|| format!("read {}", self.dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        paths.sort();
        let mut changed = Vec::new();
        for path in paths {
            let from = self.replayed.get(&path).copied().unwrap_or(0);
            let (upto, skipped) = replay(&path, from, &mut self.notes, &mut changed)?;
            self.replayed.insert(path, upto);
            self.skipped += skipped;
        }
        changed.sort_unstable();
        changed.dedup();
        Ok(changed)
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

    /// A stamp for a new version of a note that is sure to supersede the one held, even when
    /// two changes land in the same second or another device's clock runs ahead.
    pub fn next_stamp(&self, id: &str) -> Moment {
        let now = Moment::now();
        match self.notes.get(id).and_then(Record::written) {
            Some(held) if held >= now.secs => now.plus(held - now.secs + 1),
            _ => now,
        }
    }

    /// Appends a version to this device's log and applies it if it is the newest.
    pub fn put(&mut self, mut record: Record) -> Result<()> {
        record.device = self.device.clone();
        let line = line(&record)?;
        self.log
            .write_all(line.as_bytes())
            .with_context(|| format!("append to {}", self.device))?;
        self.log.flush()?;
        apply(&mut self.notes, record);
        Ok(())
    }

    /// Writes every live note, archived ones included, as the lines a log holds, oldest first.
    pub fn export(&self, out: &mut impl Write) -> Result<usize> {
        let mut records: Vec<&Record> = self.records().collect();
        records.sort_by(|a, b| a.created.cmp(&b.created).then_with(|| a.id.cmp(&b.id)));
        for record in &records {
            out.write_all(line(record)?.as_bytes())?;
        }
        out.flush()?;
        Ok(records.len())
    }
}

/// A line of a log: the version of the format, then the note.
#[derive(Serialize, Deserialize)]
struct Versioned<R> {
    #[serde(default)]
    v: u32,
    #[serde(flatten)]
    record: R,
}

/// A version as a line of a log, newline included.
fn line(record: &Record) -> Result<String> {
    let mut line = serde_json::to_string(&Versioned { v: FORMAT, record })?;
    line.push('\n');
    Ok(line)
}

/// Reads a line of a log, if it is one this build understands.
fn read_line(line: &[u8]) -> Option<Record> {
    serde_json::from_slice::<Versioned<Record>>(line)
        .ok()
        .filter(|line| line.v <= FORMAT)
        .map(|line| line.record)
}

/// Applies a version if it is the newest, and says whether it was.
fn apply(notes: &mut BTreeMap<String, Record>, record: Record) -> bool {
    match notes.get(&record.id) {
        Some(current) if !record.supersedes(current) => false,
        _ => {
            notes.insert(record.id.clone(), record);
            true
        }
    }
}

/// Replays one log into `notes` from byte `from`, noting the ids that changed. Returns how far
/// it got and how many lines were unreadable.
fn replay(
    path: &Path,
    from: u64,
    notes: &mut BTreeMap<String, Record>,
    changed: &mut Vec<String>,
) -> Result<(u64, usize)> {
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
        match read_line(line) {
            Some(record) => {
                let id = record.id.clone();
                if apply(notes, record) {
                    changed.push(id);
                }
            }
            None => skipped += 1,
        }
    }
    Ok((from + complete as u64, skipped))
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
            assert_eq!(s.get("n1").unwrap().text, text);
        }
        assert_eq!(
            s.get("n1").unwrap().stamp.map(|m| m.secs),
            Some(far_ahead + 2)
        );
        assert_eq!(s.get("n1").unwrap().updated, Some(Moment::utc(10)));
        assert!(s.next_stamp("unseen").secs <= now_secs());
        let s = Store::open(&dir, "dev-a").unwrap();
        assert_eq!(
            s.get("n1").unwrap().text,
            "second change in the same second"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn sync_takes_in_what_others_append() {
        let dir = temp_dir();
        let mut a = Store::open(&dir, "dev-a").unwrap();
        a.put(rec("n1", "mine", 10)).unwrap();
        assert!(a.sync().unwrap().is_empty(), "its own writes are not news");

        let mut b = Store::open(&dir, "dev-b").unwrap();
        b.put(rec("n1", "theirs, newer", 20)).unwrap();
        b.put(rec("n2", "theirs", 5)).unwrap();
        b.put(rec("n1", "theirs, stale", 1)).unwrap();
        assert_eq!(a.sync().unwrap(), ["n1", "n2"]);
        assert_eq!(a.get("n1").unwrap().text, "theirs, newer");
        assert!(a.sync().unwrap().is_empty());

        // A second instance on the same device shares the log, and a half-written line waits.
        let mut own = OpenOptions::new()
            .append(true)
            .open(dir.join("dev-a.jsonl"))
            .unwrap();
        let line = line(&rec("n3", "from another instance", 30)).unwrap();
        let (head, tail) = line.split_at(line.len() / 2);
        own.write_all(head.as_bytes()).unwrap();
        assert!(a.sync().unwrap().is_empty());
        own.write_all(tail.as_bytes()).unwrap();
        assert_eq!(a.sync().unwrap(), ["n3"]);
        assert_eq!(a.skipped(), 0);
        assert_eq!(a.len(), 3);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_moment_is_written_as_rfc_3339_and_keeps_its_day() {
        // 23:30 on the 13th in Lisbon, in summer: the 13th there, already the 13th in UTC too.
        // 00:30 on the 14th in Lisbon is still the 13th in UTC, and the note says the 14th.
        let late = Moment {
            secs: 1_791_934_200,
            offset: 3600,
        };
        assert_eq!(late.to_string(), "2026-10-14T00:30:00+01:00");
        assert_eq!(time::format_day(late.day()), "2026-10-14");
        assert_eq!(time::format_day(Moment::utc(late.secs).day()), "2026-10-13");
        assert_eq!(Moment::utc(late.secs).to_string(), "2026-10-13T23:30:00Z");

        for text in [
            "2026-10-14T00:30:00+01:00",
            "2026-10-13T23:30:00Z",
            "2026-10-13T18:30:00-05:00",
            "1969-12-31T23:59:59Z",
        ] {
            let moment = Moment::parse(text).unwrap();
            assert_eq!(moment.to_string(), text);
            assert_eq!(
                moment.secs,
                if text.starts_with("19") {
                    -1
                } else {
                    late.secs
                }
            );
        }
        assert_eq!(
            Moment::parse("2026-10-13t23:30:00.250z"),
            Some(Moment::utc(late.secs)),
            "fractions are dropped"
        );
        for bad in [
            "",
            "2026-10-13",
            "2026-10-13T23:30:00",
            "2026-13-13T23:30:00Z",
            "soon",
        ] {
            assert_eq!(Moment::parse(bad), None, "{bad:?}");
        }

        // Whatever zone the tests run in, midday on a day is on that day.
        let day = time::day_from_civil(2026, 9, 7);
        assert_eq!(Moment::on(day, 12 * 3600).day(), day);
        assert_eq!(Moment::now().plus(60).secs - Moment::now().secs, 60);
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
        let old = s.get("old").unwrap();
        assert_eq!(
            (old.created, old.updated, old.stamp),
            (
                Some(Moment::utc(10)),
                Some(Moment::utc(30)),
                Some(Moment::utc(40))
            )
        );
        assert_eq!(s.get("new").unwrap().text, "written now");
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
        assert_eq!(b.len(), 2);
        assert_eq!(b.get("n1"), s.get("n1"));
        assert!(b.get("n1").unwrap().archived);
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
