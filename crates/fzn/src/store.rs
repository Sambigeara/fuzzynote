//! A space's directory: a file of lines for each log, the lock that says who writes them, and
//! the id this machine writes the space under.
//!
//! One process writes a space's directory at a time, and the lock settles which. That process
//! reads every log once, when it opens the space, and from then on the files are only ever what
//! it was asked to keep, so nothing is reread and nothing is locked per write. The app gives the
//! lines to its device, which knows what they say; the importers and the exporter, which work
//! while the app is closed, read them here as a [`Store`].
//!
//! The writer's id lives in the directory, not in the config, because it names the log this
//! machine writes in this space, and a compaction changes it.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use fzn_engine::replica::{Header, names_a_log};
use fzn_engine::seal::is_sealed;
use fzn_engine::{Moment, Notes, Read, Record, Seal};

use crate::config::DEVICE_FILE;
use crate::system::{self, System};

/// The file whose lock says which process writes a space's directory. A relay locks the same
/// file, so a relay and an app never share a directory.
const LOCK_FILE: &str = ".lock";
/// The file holding the id this machine writes the space under.
const WRITER_FILE: &str = ".writer";

/// The files of one space's logs.
pub struct Logs {
    dir: PathBuf,
}

impl Logs {
    /// Opens a space's directory, making it if it is new.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        Ok(Self {
            dir: dir.to_owned(),
        })
    }

    fn path(&self, log: &str) -> PathBuf {
        self.dir.join(format!("{log}.jsonl"))
    }

    /// Every log, by name and in order of name, as its lines without their newlines. A last
    /// line with no newline is what a crash left in the middle of a write: it was never a line,
    /// and it is left out. With `repair` it is also cut from the file, so that what is added
    /// next starts a line of its own; only the holder of the lock repairs.
    pub fn load(&mut self, repair: bool) -> Result<Vec<(String, Vec<String>)>> {
        let mut logs: Vec<(String, PathBuf)> = fs::read_dir(&self.dir)
            .with_context(|| format!("read {}", self.dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .filter_map(|p| {
                let stem = p.file_stem()?.to_str()?.to_owned();
                names_a_log(&stem).then_some((stem, p))
            })
            .collect();
        logs.sort();
        let mut loaded = Vec::with_capacity(logs.len());
        for (log, path) in logs {
            let mut bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
            let whole = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
            if whole < bytes.len() {
                if repair {
                    OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .and_then(|file| file.set_len(whole as u64))
                        .with_context(|| {
                            format!("cut the unfinished line from {}", path.display())
                        })?;
                }
                bytes.truncate(whole);
            }
            let lines = match bytes.split_last() {
                Some((_, body)) => body
                    .split(|&b| b == b'\n')
                    .map(|line| String::from_utf8_lossy(line).into_owned())
                    .collect(),
                None => Vec::new(),
            };
            loaded.push((log, lines));
        }
        Ok(loaded)
    }

    /// Adds lines to the end of a log, each with a newline.
    pub fn append(&mut self, log: &str, lines: &[String]) -> Result<()> {
        let path = self.path(log);
        let mut text = String::new();
        for line in lines {
            text.push_str(line);
            text.push('\n');
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        file.write_all(text.as_bytes())
            .and_then(|()| file.flush())
            .with_context(|| format!("append to {}", path.display()))
    }

    /// Deletes a log's file. One that is not there is already as asked.
    pub fn delete(&mut self, log: &str) -> Result<()> {
        let path = self.path(log);
        match fs::remove_file(&path) {
            Err(e) if e.kind() != ErrorKind::NotFound => {
                Err(e).with_context(|| format!("delete {}", path.display()))
            }
            _ => Ok(()),
        }
    }
}

/// The right to write a space's directory, for as long as it is held.
pub struct Lock {
    _file: File,
}

impl Lock {
    /// Takes the lock on a space's directory, making the directory if it is new. Gives nothing
    /// if another process holds it.
    pub fn take(dir: &Path) -> Result<Option<Lock>> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let path = dir.join(LOCK_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Lock { _file: file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(e)) => {
                Err(e).with_context(|| format!("lock {}", path.display()))
            }
        }
    }
}

/// The id this machine writes a space under, made the first time the space is opened here. A
/// machine that had one id for every space, kept in the config directory, goes on with it.
pub fn writer(dir: &Path, config_dir: &Path) -> Result<String> {
    let path = dir.join(WRITER_FILE);
    if let Some(id) = read_id(&path)? {
        return Ok(id);
    }
    let id = match read_id(&config_dir.join(DEVICE_FILE))? {
        Some(legacy) => legacy,
        None => format!("{:08x}", system::random_u64() as u32),
    };
    set_writer(dir, &id)?;
    Ok(id)
}

/// Keeps a new writer id for a space, as after a compaction.
pub fn set_writer(dir: &Path, id: &str) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(WRITER_FILE);
    fs::write(&path, format!("{id}\n")).with_context(|| format!("write {}", path.display()))
}

/// The id in a file, if the file is there and holds one.
fn read_id(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text.trim().to_owned()).filter(|id| !id.is_empty())),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// A space on disk as the importers and the exporter see it: the notes its logs add up to, and
/// the writer's log to add to. They work while the app is closed, and add versions sealed as
/// the app would.
pub struct Store {
    notes: Notes,
    logs: Logs,
    writer: String,
    seal: Seal,
    skipped: usize,
}

impl Store {
    /// Reads every log in the directory. An empty `writer` opens it only to read. Fails if the
    /// directory holds sealed lines and the seal opened none of them, since a version sealed
    /// under another key would split the space in two.
    pub fn open(dir: &Path, writer: String, seal: Seal, repair: bool) -> Result<Self> {
        let mut logs = Logs::open(dir)?;
        let mut notes = Notes::new();
        let (mut sealed, mut opened, mut shut, mut skipped) = (0, 0, 0, 0);
        for (_, lines) in logs.load(repair)? {
            for line in lines {
                if line.trim().is_empty() {
                    continue;
                }
                let was_sealed = is_sealed(&line);
                sealed += usize::from(was_sealed);
                match Record::read_line(&seal, &line) {
                    Read::Version(record) => {
                        opened += usize::from(was_sealed);
                        notes.apply(record);
                    }
                    Read::Shut => shut += 1,
                    Read::Other if Header::read(&line).is_some() => {}
                    Read::Other => skipped += 1,
                }
            }
        }
        if sealed > 0 && opened == 0 {
            bail!("the passphrase does not open {}", dir.display());
        }
        Ok(Self {
            notes,
            logs,
            writer,
            seal,
            skipped: skipped + shut,
        })
    }

    /// The notes the logs add up to.
    pub fn notes(&self) -> &Notes {
        &self.notes
    }

    /// Lines that could not be read, or that this key does not open.
    pub fn skipped(&self) -> usize {
        self.skipped
    }

    /// A stamp for a new version of a note that is sure to supersede the one held.
    pub fn next_stamp(&self, id: &str) -> Moment {
        self.notes.next_stamp(id, system::now())
    }

    /// Seals a version, appends it to the writer's log and takes it in if it is the newest.
    pub fn put(&mut self, mut record: Record) -> Result<()> {
        if self.writer.is_empty() {
            bail!("this space was opened to be read, not written");
        }
        record.device = self.writer.clone();
        let line = record.sealed(&self.seal, &System)?;
        self.logs.append(&self.writer, &[line])?;
        self.notes.apply(record);
        Ok(())
    }

    /// Writes every live note, archived ones included, as plain lines, one a note, oldest first.
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::system::{now_secs, random_u64};
    use fzn_engine::Key;

    pub(crate) fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fzn-store-test-{:x}", random_u64()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    pub(crate) fn seal() -> Seal {
        Seal::new(Key::from_bytes([7; 32]))
    }

    fn rec(id: &str, text: &str, updated: i64) -> Record {
        Record {
            id: id.into(),
            text: text.into(),
            links: Vec::new(),
            created: Some(Moment::utc(updated)),
            updated: Some(Moment::utc(updated)),
            stamp: None,
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: String::new(),
        }
    }

    fn open(dir: &Path, writer: &str) -> Store {
        Store::open(dir, writer.into(), seal(), true).unwrap()
    }

    #[test]
    fn logs_give_back_their_lines_and_a_torn_tail_is_cut_only_when_repairing() {
        let dir = temp_dir();
        let mut logs = Logs::open(&dir.join("space")).unwrap();
        let sealed: Vec<String> = ["Boiler", "House"]
            .iter()
            .map(|text| rec("n1", text, 1).sealed(&seal(), &System).unwrap())
            .collect();
        logs.append("3f9a01c2", &sealed).unwrap();
        logs.append("dev-b", &["b1".to_owned()]).unwrap();
        fs::write(dir.join("space").join("not a log.jsonl"), "x\n").unwrap();
        fs::write(dir.join("space").join("notes.txt"), "x\n").unwrap();
        assert_eq!(
            logs.load(false).unwrap(),
            [
                ("3f9a01c2".to_owned(), sealed.clone()),
                ("dev-b".to_owned(), vec!["b1".to_owned()])
            ]
        );

        // What a crash leaves, the start of a line with no end, is left out, and cut only by
        // whoever holds the lock.
        let path = dir.join("space").join("dev-b.jsonl");
        fs::write(&path, "b1\n{\"half").unwrap();
        assert_eq!(logs.load(false).unwrap()[1].1, ["b1"]);
        assert_eq!(fs::read_to_string(&path).unwrap(), "b1\n{\"half");
        assert_eq!(logs.load(true).unwrap()[1].1, ["b1"]);
        assert_eq!(fs::read_to_string(&path).unwrap(), "b1\n");
        logs.append("dev-b", &["b2".to_owned()]).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "b1\nb2\n");

        logs.delete("dev-b").unwrap();
        assert!(!path.exists());
        logs.delete("dev-b").unwrap();
        assert_eq!(logs.load(true).unwrap().len(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn one_process_holds_a_space_until_it_lets_go() {
        let dir = temp_dir();
        let lock = Lock::take(&dir).unwrap().unwrap();
        assert!(
            Lock::take(&dir).unwrap().is_none(),
            "a second taker is refused"
        );
        drop(lock);
        assert!(Lock::take(&dir).unwrap().is_some());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_writer_id_is_made_once_and_taken_from_the_old_device_file() {
        let dir = temp_dir();
        let (space, config) = (dir.join("space"), dir.join("config"));
        let id = writer(&space, &config).unwrap();
        assert_eq!(id.len(), 8);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(writer(&space, &config).unwrap(), id, "and is stable");
        set_writer(&space, "3f9a01c2").unwrap();
        assert_eq!(writer(&space, &config).unwrap(), "3f9a01c2");

        fs::create_dir_all(&config).unwrap();
        fs::write(config.join(DEVICE_FILE), "dev-a\n").unwrap();
        let other = dir.join("other");
        assert_eq!(writer(&other, &config).unwrap(), "dev-a");
        assert_eq!(
            fs::read_to_string(other.join(".writer")).unwrap(),
            "dev-a\n"
        );
        assert!(
            config.join(DEVICE_FILE).exists(),
            "the old file is left alone"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn what_is_put_is_sealed_and_the_newest_version_wins() {
        let dir = temp_dir();
        {
            let mut s = open(&dir, "dev-a");
            s.put(rec("n1", "first", 10)).unwrap();
            s.put(rec("n1", "second", 20)).unwrap();
            s.put(rec("n1", "stale", 15)).unwrap();
            s.put(rec("n2", "other", 5)).unwrap();
            assert_eq!(s.notes().get("n1").unwrap().text, "second");
            assert_eq!(s.notes().get("n1").unwrap().device, "dev-a");
        }
        let written = fs::read_to_string(dir.join("dev-a.jsonl")).unwrap();
        assert_eq!(written.lines().count(), 4);
        for line in written.lines() {
            assert!(line.starts_with(r#"{"s":1,"#), "{line}");
            assert!(!line.contains("second"));
        }
        let s = open(&dir, "dev-a");
        assert_eq!(s.notes().len(), 2);
        assert_eq!(s.notes().get("n1").unwrap().text, "second");
        assert_eq!(s.skipped(), 0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_wrong_key_is_refused_and_plain_lines_are_read_beside_sealed_ones() {
        let dir = temp_dir();
        open(&dir, "dev-a").put(rec("n1", "sealed", 10)).unwrap();
        let wrong = Store::open(
            &dir,
            "dev-b".into(),
            Seal::new(Key::from_bytes([9; 32])),
            true,
        );
        let why = wrong.err().unwrap().to_string();
        assert!(why.starts_with("the passphrase does not open"), "{why}");

        // A log from before sealing, with a line from before versions and times in unix
        // seconds, a line from a later format, a compaction header and garbage.
        fs::write(
            dir.join("dev-b.jsonl"),
            concat!(
                "{\"compacts\":[\"dev-x\"],\"snapshot\":1}\n",
                "{\"id\":\"old\",\"text\":\"from before\",\"created\":10,\"updated\":30,\"stamp\":40,\"device\":\"dev-b\"}\n",
                "{\"v\":3,\"id\":\"new\",\"text\":\"from the future\"}\n",
                "not json\n",
            ),
        )
        .unwrap();
        let s = open(&dir, "dev-a");
        let old = s.notes().get("old").unwrap();
        assert_eq!(
            (old.created, old.updated, old.stamp),
            (
                Some(Moment::utc(10)),
                Some(Moment::utc(30)),
                Some(Moment::utc(40))
            )
        );
        assert_eq!(s.notes().get("n1").unwrap().text, "sealed");
        assert_eq!(s.skipped(), 2, "the later format and the garbage");

        // A space of nothing but plain lines takes any key.
        let plain = dir.join("plain");
        fs::create_dir_all(&plain).unwrap();
        fs::copy(dir.join("dev-b.jsonl"), plain.join("dev-b.jsonl")).unwrap();
        assert!(
            Store::open(
                &plain,
                String::new(),
                Seal::new(Key::from_bytes([9; 32])),
                false
            )
            .is_ok()
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn deleted_notes_stay_dead() {
        let dir = temp_dir();
        let mut s = open(&dir, "dev-a");
        s.put(rec("n1", "alive", 10)).unwrap();
        s.put(Record {
            deleted: true,
            ..rec("n1", "alive", 20)
        })
        .unwrap();
        assert!(s.notes().get("n1").is_none());
        assert!(s.notes().version("n1").is_some_and(|r| r.deleted));
        s.put(rec("n1", "late arrival", 15)).unwrap();
        assert!(
            s.notes().get("n1").is_none(),
            "an older version cannot resurrect a deleted note"
        );
        assert!(open(&dir, "dev-a").notes().get("n1").is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_stamp_decides_the_newest_and_a_local_change_always_wins() {
        let dir = temp_dir();
        let mut s = open(&dir, "dev-a");
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
        assert!(s.next_stamp("unseen").secs <= now_secs());
        assert_eq!(
            open(&dir, "dev-a").notes().get("n1").unwrap().text,
            "second change in the same second"
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_export_is_plain_lines_one_a_live_note() {
        let dir = temp_dir();
        let mut s = open(&dir, "dev-a");
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
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "one line a note, and no history");
        assert!(lines[1].starts_with(r#"{"v":2,"id":"n1","text":"first, done","#));
        assert_eq!(
            Record::read(lines[1].as_bytes()).as_ref(),
            s.notes().get("n1")
        );

        let read_only = Store::open(&dir, String::new(), seal(), false).unwrap();
        assert_eq!(read_only.notes().len(), 2);
        let mut read_only = read_only;
        assert!(read_only.put(rec("n3", "x", 1)).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }
}
