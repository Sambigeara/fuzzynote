//! A space's directory: a file of lines for each log, the locks that say who writes them, and
//! the ids this machine writes the space under.
//!
//! Two writers on a machine write a space: the app, and the commands that write beside it while
//! it is open, `fzn tick` say. Each appends to a log of its own under an id of its own, so that a
//! log has one process appending to it, and the two cannot conflict, since versions settle by
//! their stamps whichever log they are in. The app holds the space's lock, which a second window,
//! an importer and a relay are refused; a command waits its turn on a lock of the commands' own
//! and never asks for the app's. So a log may be read while another process appends to it, and
//! what is read of it is only as far as its last whole line; an unfinished line is cut only by
//! the process about to append to that log, since only it could have left one there. The app
//! reads every log when it opens the space and, about once a second after that, what has been
//! added since, and gives the lines to its device, which knows what they say; a command does the
//! same once. The importers and the exporter read them here as a [`Store`].
//!
//! A writer's id lives in the directory, not in the config, because it names the log this
//! machine writes in this space, and a compaction changes it.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read as _, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::replica::{Header, names_a_log};
use fzn_engine::seal::is_sealed;
use fzn_engine::{Device, Given, Key, Moment, Notes, Read, Record, Seal, Session, Todo, Weights};

use crate::config::DEVICE_FILE;
use crate::system::{self, System};

/// The file whose lock says which process writes a space's directory. A relay locks the same
/// file, so a relay and an app never share a directory.
const LOCK_FILE: &str = ".lock";

/// A writer on this machine: the app, or the commands that write beside it while it is open.
/// Each appends to a log of its own, named for the id kept in a file of its own.
#[derive(Clone, Copy)]
pub enum Writer {
    App,
    Commands,
}

impl Writer {
    /// The file in a space's directory that holds the id it writes under. The commands' is
    /// also their lock, which holds only while an id is written over in place.
    fn file(self) -> &'static str {
        match self {
            Writer::App => ".writer",
            Writer::Commands => ".commands",
        }
    }
}

/// The files of one space's logs.
pub struct Logs {
    dir: PathBuf,
    /// For each log read or written here, how far in its last whole line ends, in bytes.
    ends: BTreeMap<String, u64>,
}

impl Logs {
    /// Opens a space's directory, making it if it is new.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        Ok(Self {
            dir: dir.to_owned(),
            ends: BTreeMap::new(),
        })
    }

    fn path(&self, log: &str) -> PathBuf {
        self.dir.join(format!("{log}.jsonl"))
    }

    /// The lines added to each log since it was last read or written here, by name and in order
    /// of name, without their newlines: every line, the first time. A last line with no newline
    /// is being written, or is what a crash left in the middle of a write, and is not a line yet.
    /// A log is a file, not a link to one, so a file gone between listing the directory and
    /// reading it was deleted: a log a compaction retired, whose successor was kept before it
    /// went. The directory is listed again for that.
    pub fn load(&mut self) -> Result<Vec<(String, Vec<String>)>> {
        let mut loaded = Vec::new();
        loop {
            let mut logs: Vec<(String, PathBuf)> = fs::read_dir(&self.dir)
                .with_context(|| format!("read {}", self.dir.display()))?
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
                .filter_map(|entry| {
                    let path = entry.path();
                    let stem = path.file_stem()?.to_str()?.to_owned();
                    let named =
                        path.extension().is_some_and(|x| x == "jsonl") && names_a_log(&stem);
                    named.then_some((stem, path))
                })
                .collect();
            logs.sort();
            let mut gone = false;
            for (log, path) in logs {
                let mut file = match File::open(&path) {
                    Ok(file) => file,
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        gone = true;
                        continue;
                    }
                    Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
                };
                let end = self.ends.get(&log).copied().unwrap_or(0);
                let mut bytes = Vec::new();
                file.seek(SeekFrom::Start(end))
                    .and_then(|_| file.read_to_end(&mut bytes))
                    .with_context(|| format!("read {}", path.display()))?;
                let Some(last) = bytes.iter().rposition(|&b| b == b'\n') else {
                    continue;
                };
                self.ends.insert(log.clone(), end + last as u64 + 1);
                let lines = bytes[..last]
                    .split(|&b| b == b'\n')
                    .map(|line| String::from_utf8_lossy(line).into_owned())
                    .collect();
                loaded.push((log, lines));
            }
            if !gone {
                return Ok(loaded);
            }
        }
    }

    /// Adds lines to the end of a log, each with a newline, once any unfinished line at its end
    /// is cut. That is what a crash left in the middle of a write: it was never a line, and these
    /// start one of their own. One process appends to a log, so only this one could have left it.
    pub fn append(&mut self, log: &str, lines: &[String]) -> Result<()> {
        let path = self.path(log);
        let mut text = String::new();
        for line in lines {
            text.push_str(line);
            text.push('\n');
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        let mut end = self.ends.get(log).copied().unwrap_or(0);
        let mut past = Vec::new();
        file.seek(SeekFrom::Start(end))
            .and_then(|_| file.read_to_end(&mut past))
            .with_context(|| format!("read {}", path.display()))?;
        if !past.is_empty() {
            end += past
                .iter()
                .rposition(|&b| b == b'\n')
                .map_or(0, |i| i as u64 + 1);
            file.set_len(end)
                .with_context(|| format!("cut the unfinished line from {}", path.display()))?;
        }
        file.write_all(text.as_bytes())
            .and_then(|()| file.flush())
            .with_context(|| format!("append to {}", path.display()))?;
        self.ends.insert(log.to_owned(), end + text.len() as u64);
        Ok(())
    }

    /// Deletes a log's file, unless a writer here writes it now. A header from elsewhere retires
    /// such a log when a copy of this directory wrote under its name, and its lines may then be
    /// ones nobody else has: its writer carries them into a log of its own, and keeps the new
    /// id, before it goes. One that is not there is already as asked.
    pub fn delete(&mut self, log: &str) -> Result<()> {
        for writer in [Writer::App, Writer::Commands] {
            if read_id(&self.dir.join(writer.file()))?.as_deref() == Some(log) {
                return Ok(());
            }
        }
        self.ends.remove(log);
        let path = self.path(log);
        match fs::remove_file(&path) {
            Err(e) if e.kind() != ErrorKind::NotFound => {
                Err(e).with_context(|| format!("delete {}", path.display()))
            }
            _ => Ok(()),
        }
    }

    /// Does what a device writing as `writer` asks of the directory: keeps its lines, then its
    /// new id, then deletes the logs it retires. The lines first, so that a compaction stopped
    /// half way leaves both logs and not neither, and the id before the deleting, so that the
    /// old log is no longer its writer's when it goes.
    pub fn keep(&mut self, todo: &Todo, writer: Writer) -> Result<()> {
        for kept in &todo.keep {
            self.append(&kept.log, &kept.lines)?;
        }
        if let Some(id) = &todo.device {
            keep_id(&self.dir.join(writer.file()), id)?;
        }
        for log in &todo.delete {
            self.delete(log)?;
        }
        Ok(())
    }
}

/// Opens a space's directory for a device writing as `writer`: every log is given to the device
/// and what it asks for kept, and then whatever opening it is due, a compaction say. Fails if the
/// key opens none of the sealed lines there, since nothing could be written.
pub fn open(dir: &Path, space: &str, writer: Writer, device: &mut Device) -> Result<Logs> {
    let mut logs = Logs::open(dir)?;
    for (log, lines) in logs.load()? {
        let todo = device.load(&log, lines);
        logs.keep(&todo, writer)?;
    }
    if !device.key_verified() {
        bail!("the passphrase does not open {space}: run fzn key to enter it again");
    }
    let todo = device.opened();
    logs.keep(&todo, writer)?;
    Ok(logs)
}

/// Ticks these tasks, or unticks them, as Ctrl-V does, while the app may have the space open: as
/// the commands' writer, in their turn and never on the app's lock. Writes each note as it now
/// stands, as the export writes it. All or none: if one cannot be, nothing is written.
pub fn tick(
    dir: &Path,
    space: &str,
    key: Key,
    ids: &[String],
    done: bool,
    out: &mut impl Write,
) -> Result<()> {
    let _turn = Lock::wait(dir)?;
    let mut device = Device::new(Box::new(System), commands(dir)?, Weights::default(), key);
    let mut logs = open(dir, space, Writer::Commands, &mut device)?;
    let (todo, ticked) = device
        .tick(ids, done)
        .map_err(|why| anyhow!("{why}, so nothing was written"))?;
    logs.keep(&todo, Writer::Commands)?;
    write_lines(&ticked.iter().collect::<Vec<_>>(), out)?;
    Ok(())
}

/// Writes notes given as the lines `fzn query` writes, one a line, as [`Device::apply`] says,
/// while the app may have the space open: as the commands' writer, in their turn, as a tick is.
/// Writes each note it was given as it now stands, in the order given. All or none: a line that
/// is not a note, or one the device refuses, is said and nothing is written.
pub fn apply(dir: &Path, space: &str, key: Key, input: &str, out: &mut impl Write) -> Result<()> {
    let given = given(input)?;
    if given.is_empty() {
        return Ok(());
    }
    let _turn = Lock::wait(dir)?;
    let mut device = Device::new(Box::new(System), commands(dir)?, Weights::default(), key);
    let mut logs = open(dir, space, Writer::Commands, &mut device)?;
    let (todo, notes) = device
        .apply(given)
        .map_err(|why| anyhow!("{why}, so nothing was written"))?;
    logs.keep(&todo, Writer::Commands)?;
    write_lines(&notes.iter().collect::<Vec<_>>(), out)?;
    Ok(())
}

/// The notes in what `fzn apply` was given, one a line. Blank lines at the end are how a file
/// ends; one anywhere else is a line that is not a note, so that a line's number is the one the
/// device names it by.
fn given(input: &str) -> Result<Vec<Given>> {
    // A file an editor saved with a byte-order mark is a file of notes all the same.
    let input = input.trim_start_matches('\u{feff}').trim_end();
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let read = |n: usize, line: &str| -> Result<Given> {
        let not_a_note = || anyhow!("line {n} is not a note, so nothing was written");
        let value: serde_json::Value = serde_json::from_str(line).map_err(|_| not_a_note())?;
        // A line with neither an id nor a text says nothing of a note: a sealed line of a log,
        // say, which is not what apply reads.
        let names = |field: &str| value.get(field).is_some();
        if !value.is_object() || !names("id") && !names("text") {
            return Err(not_a_note());
        }
        let format = value.get("v").and_then(serde_json::Value::as_u64);
        if format.is_some_and(|v| v > u64::from(Record::FORMAT)) {
            bail!("line {n} is from a later fzn, so nothing was written");
        }
        serde_json::from_value(value).map_err(|_| not_a_note())
    };
    input
        .lines()
        .enumerate()
        .map(|(i, line)| read(i + 1, line))
        .collect()
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
        let file = lock_file(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Lock { _file: file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(e)) => {
                Err(e).with_context(|| format!("lock {}", path.display()))
            }
        }
    }

    /// Waits for the commands' turn to write a space beside the app, on the file that holds
    /// their id: a lock of their own, so that the app is never asked to let go of its own.
    pub fn wait(dir: &Path) -> Result<Lock> {
        let path = dir.join(Writer::Commands.file());
        let file = lock_file(&path)?;
        file.lock()
            .with_context(|| format!("lock {}", path.display()))?;
        Ok(Lock { _file: file })
    }
}

fn lock_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))
}

/// The id this machine's app writes a space under, made the first time the space is opened here.
/// A machine that had one id for every space, kept in the config directory, goes on with it.
pub fn writer(dir: &Path, config_dir: &Path) -> Result<String> {
    let path = dir.join(Writer::App.file());
    if let Some(id) = read_id(&path)? {
        return Ok(id);
    }
    let id = match read_id(&config_dir.join(DEVICE_FILE))? {
        Some(legacy) => legacy,
        None => new_id(),
    };
    set_writer(dir, &id)?;
    Ok(id)
}

/// The id the commands write a space under, made the first time one writes there. Read and
/// kept only in their turn.
fn commands(dir: &Path) -> Result<String> {
    let path = dir.join(Writer::Commands.file());
    if let Some(id) = read_id(&path)? {
        return Ok(id);
    }
    let id = new_id();
    keep_id(&path, &id)?;
    Ok(id)
}

/// Keeps the id this machine's app writes a space under, making the directory if it is new.
pub fn set_writer(dir: &Path, id: &str) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    keep_id(&dir.join(Writer::App.file()), id)
}

fn new_id() -> String {
    format!("{:08x}", system::random_u64() as u32)
}

fn keep_id(path: &Path, id: &str) -> Result<()> {
    fs::write(path, format!("{id}\n")).with_context(|| format!("write {}", path.display()))
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
/// the writer's log to add to. The importers work while the app is closed, holding its lock, and
/// add versions sealed as the app would; the exporter only reads, and may while it is open.
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
    pub fn open(dir: &Path, writer: String, seal: Seal) -> Result<Self> {
        let mut logs = Logs::open(dir)?;
        let mut notes = Notes::new();
        let (mut sealed, mut opened, mut shut, mut skipped) = (0, 0, 0, 0);
        for (_, lines) in logs.load()? {
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
                    Read::Later => {
                        opened += 1;
                        skipped += 1;
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

    /// Writes every live note, archived ones included, as plain lines, one a note, oldest first,
    /// since a whole export is read back in the order it was written.
    pub fn export(&self, out: &mut impl Write) -> Result<usize> {
        let mut records: Vec<&Record> = self.notes.records().collect();
        records.sort_by(|a, b| a.created.cmp(&b.created).then_with(|| a.id.cmp(&b.id)));
        write_lines(&records, out)
    }

    /// Writes the notes a line holds at the top of the space as plain lines, in the order the app
    /// would list them, ranked by the same weights, and every one of them, archived ones too:
    /// with nothing to rank by the space's order, and otherwise best first.
    pub fn query(self, line: &str, weights: Weights, out: &mut impl Write) -> Result<usize> {
        let session = Session::new(Box::new(System), String::new(), weights, self.notes);
        write_lines(&session.query(line), out)
    }
}

fn write_lines(records: &[&Record], out: &mut impl Write) -> Result<usize> {
    for record in records {
        out.write_all(record.line()?.as_bytes())?;
    }
    out.flush()?;
    Ok(records.len())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::system::{self, random_u64};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

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
            created: Some(Moment::utc(updated)),
            updated: Some(Moment::utc(updated)),
            ..Record::new(id, text)
        }
    }

    fn open(dir: &Path, writer: &str) -> Store {
        Store::open(dir, writer.into(), seal()).unwrap()
    }

    #[test]
    fn logs_give_back_what_was_added_since_and_only_whole_lines() {
        let dir = temp_dir();
        let space = dir.join("space");
        let mut logs = Logs::open(&space).unwrap();
        let sealed: Vec<String> = ["Boiler", "House"]
            .iter()
            .map(|text| rec("n1", text, 1).sealed(&seal(), &System).unwrap())
            .collect();
        logs.append("3f9a01c2", &sealed).unwrap();
        fs::write(space.join("not a log.jsonl"), "x\n").unwrap();
        fs::write(space.join("notes.txt"), "x\n").unwrap();
        let mut other = Logs::open(&space).unwrap();
        other.append("dev-b", &["b1".to_owned()]).unwrap();
        assert_eq!(
            Logs::open(&space).unwrap().load().unwrap(),
            [
                ("3f9a01c2".to_owned(), sealed),
                ("dev-b".to_owned(), vec!["b1".to_owned()])
            ],
            "every line, the first time"
        );
        assert_eq!(
            logs.load().unwrap(),
            [("dev-b".to_owned(), vec!["b1".to_owned()])],
            "and then only what another writer added"
        );
        assert!(logs.load().unwrap().is_empty());

        // A line still being written is left until it is whole, and left as it is.
        let path = space.join("dev-b.jsonl");
        other.append("dev-b", &["b2".to_owned()]).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"b3 half")
            .unwrap();
        assert_eq!(logs.load().unwrap()[0].1, ["b2"]);
        assert!(logs.load().unwrap().is_empty());
        assert_eq!(fs::read_to_string(&path).unwrap(), "b1\nb2\nb3 half");
        fs::write(&path, "b1\nb2\nb3 half, and whole\n").unwrap();
        assert_eq!(logs.load().unwrap()[0].1, ["b3 half, and whole"]);

        // What a crash left unfinished is cut by the one process that appends to that log.
        fs::write(&path, "b1\n{\"torn").unwrap();
        let mut after = Logs::open(&space).unwrap();
        assert_eq!(after.load().unwrap()[1].1, ["b1"]);
        assert_eq!(fs::read_to_string(&path).unwrap(), "b1\n{\"torn");
        after.append("dev-b", &["b2".to_owned()]).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "b1\nb2\n");

        // A log a writer here writes now stays, whatever retires it; any other goes.
        set_writer(&space, "3f9a01c2").unwrap();
        after.delete("3f9a01c2").unwrap();
        after.delete("dev-b").unwrap();
        assert!(space.join("3f9a01c2.jsonl").exists());
        assert!(!path.exists());
        after.delete("dev-b").unwrap();
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

        // The commands take turns on a lock of their own, whoever holds the space's.
        let turn = Lock::wait(&dir).unwrap();
        let (told, waited) = mpsc::channel();
        let second = thread::spawn({
            let dir = dir.clone();
            move || told.send(Lock::wait(&dir).unwrap())
        });
        assert!(waited.recv_timeout(Duration::from_millis(200)).is_err());
        drop(turn);
        waited.recv_timeout(Duration::from_secs(5)).unwrap();
        second.join().unwrap().unwrap();

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
        let wrong = Store::open(&dir, "dev-b".into(), Seal::new(Key::from_bytes([9; 32])));
        let why = wrong.err().unwrap().to_string();
        assert!(why.starts_with("the passphrase does not open"), "{why}");

        // A log from before sealing, with a line from before versions and times in unix
        // seconds, a line from a later format, a compaction header and garbage.
        fs::write(
            dir.join("dev-b.jsonl"),
            concat!(
                "{\"compacts\":[\"dev-x\"],\"snapshot\":1}\n",
                "{\"id\":\"old\",\"text\":\"from before\",\"created\":10,\"updated\":30,\"stamp\":40,\"device\":\"dev-b\"}\n",
                "{\"v\":4,\"id\":\"new\",\"text\":\"from the future\"}\n",
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
        assert!(Store::open(&plain, String::new(), Seal::new(Key::from_bytes([9; 32]))).is_ok());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_space_of_lines_from_a_later_format_opens_with_its_key() {
        let dir = temp_dir();
        let later = seal().seal(r#"{"v":99,"id":"new","text":"from the future"}"#, &System);
        Logs::open(&dir).unwrap().append("dev-b", &[later]).unwrap();
        let s = open(&dir, "dev-a");
        assert!(s.notes().is_empty());
        assert_eq!(s.skipped(), 1);
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
        let far_ahead = system::now().secs() + 3600;
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
            s.notes().get("n1").unwrap().stamp.map(|m| m.ms),
            Some(far_ahead * 1_000 + 2)
        );
        assert!(s.next_stamp("unseen") <= system::now());
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
        assert!(lines[1].starts_with(r#"{"v":3,"id":"n1","text":"first, done","#));
        assert_eq!(
            Record::read(lines[1].as_bytes()).as_ref(),
            s.notes().get("n1")
        );

        let read_only = Store::open(&dir, String::new(), seal()).unwrap();
        assert_eq!(read_only.notes().len(), 2);
        let mut read_only = read_only;
        assert!(read_only.put(rec("n3", "x", 1)).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_query_writes_what_the_line_holds_archived_ones_too_in_its_order() {
        let dir = temp_dir();
        let mut s = open(&dir, "dev-a");
        s.put(rec("gutter", "clear the gutter #house", 10)).unwrap();
        s.put(rec("oil", "order oil #house", 20)).unwrap();
        s.put(Record {
            archived: true,
            ..rec("roof", "fix the roof #house", 30)
        })
        .unwrap();
        s.put(rec("cake", "cake recipe", 40)).unwrap();
        let ids = |line: &str| {
            let store = Store::open(&dir, String::new(), seal()).unwrap();
            let mut out = Vec::new();
            let n = store.query(line, Weights::default(), &mut out).unwrap();
            let text = String::from_utf8(out).unwrap();
            let ids: Vec<String> = text
                .lines()
                .map(|line| Record::read(line.as_bytes()).unwrap().id)
                .collect();
            assert_eq!(ids.len(), n);
            ids
        };
        assert_eq!(
            ids("#house"),
            ["roof", "oil", "gutter"],
            "nothing to rank by: the space's order, archived ones included"
        );
        assert_eq!(ids("#house oil"), ["oil"], "words rank among them");
        assert!(ids("~zzz").is_empty());
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Runs `fzn tick`, or `fzn untick`, on a space, and gives back what it wrote out.
    fn ticked(dir: &Path, ids: &[&str], done: bool) -> Result<String> {
        let ids: Vec<String> = ids.iter().map(|&id| id.to_owned()).collect();
        let mut out = Vec::new();
        tick(dir, "main", Key::from_bytes([7; 32]), &ids, done, &mut out)?;
        Ok(String::from_utf8(out).unwrap())
    }

    /// The lines of a log.
    fn lines(dir: &Path, log: &str) -> Vec<String> {
        let text = fs::read_to_string(dir.join(format!("{log}.jsonl"))).unwrap_or_default();
        text.lines().map(str::to_owned).collect()
    }

    fn commands_id(dir: &Path) -> String {
        read_id(&dir.join(".commands")).unwrap().unwrap()
    }

    /// Runs `fzn query` on a space, and gives back each line it wrote, as JSON.
    fn queried(dir: &Path, line: &str) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        let store = Store::open(dir, String::new(), seal()).unwrap();
        store.query(line, Weights::default(), &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        out.lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    /// Runs `fzn apply` on a space with these lines, and gives back what it wrote out.
    fn applied(dir: &Path, lines: &[serde_json::Value]) -> Result<Vec<Record>> {
        let input: String = lines.iter().map(|line| format!("{line}\n")).collect();
        let mut out = Vec::new();
        apply(dir, "main", Key::from_bytes([7; 32]), &input, &mut out)?;
        let out = String::from_utf8(out).unwrap();
        Ok(out
            .lines()
            .map(|line| Record::read(line.as_bytes()).unwrap())
            .collect())
    }

    /// A space the app has written three notes in, and their lines as `fzn query` writes them.
    fn written(dir: &Path) -> [serde_json::Value; 3] {
        set_writer(dir, "3f9a01c2").unwrap();
        let mut app = open(dir, "3f9a01c2");
        for (id, text, at) in [
            ("gate", "Fix the gate", 10),
            ("oil", "[ ] Order oil #house", 20),
            ("boiler", "Boiler #house", 30),
        ] {
            let stamp = Some(Moment::utc(at));
            app.put(Record {
                stamp,
                ..rec(id, text, at)
            })
            .unwrap();
        }
        let line = |id: &str| {
            let found = queried(dir, "");
            found.into_iter().find(|line| line["id"] == id).unwrap()
        };
        [line("gate"), line("oil"), line("boiler")]
    }

    #[test]
    fn apply_writes_what_it_is_given_by_the_apps_rules_and_says_each_note_as_it_now_stands() {
        let dir = temp_dir();
        let [gate, oil, boiler] = written(&dir);

        // Given as it was read, nothing is written, and each is said as it stands.
        let said = applied(&dir, &[oil.clone(), boiler.clone()]).unwrap();
        let ids: Vec<&str> = said.iter().map(|note| note.id.as_str()).collect();
        assert_eq!(ids, ["oil", "boiler"]);
        assert!(lines(&dir, &commands_id(&dir)).is_empty());

        let mut ticked = oil.clone();
        ticked["archived"] = true.into();
        let mut moved = boiler.clone();
        moved["text"] = "Boiler #garden, see [[Fix the gate]]".into();
        let new = serde_json::json!({"text": "Prune the roses /today #garden"});
        let said = applied(&dir, &[moved, new, ticked]).unwrap();
        let texts: Vec<&str> = said.iter().map(|note| note.text.as_str()).collect();
        assert_eq!(texts[0], "Boiler #garden, see [[Fix the gate]]");
        assert!(texts[1].starts_with("Prune the roses 20") && !texts[1].contains("/today"));
        assert_eq!(texts[2], "[x] Order oil #house");
        assert!(said[2].archived);
        assert_eq!(said[0].links, ["gate"], "the brackets are a link");
        assert_eq!(
            lines(&dir, &commands_id(&dir)).len(),
            3,
            "a version a note changed"
        );
        let found = queried(&dir, "#garden");
        assert_eq!(found.len(), 2);
        assert_eq!(
            found[0]["id"],
            said[1].id.as_str(),
            "the new note is at the top"
        );

        let mut gone = gate;
        gone["deleted"] = true.into();
        assert!(applied(&dir, &[gone]).unwrap()[0].deleted);
        assert!(queried(&dir, "").iter().all(|line| line["id"] != "gate"));
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn apply_writes_nothing_when_a_line_cannot_be_written_and_says_which() {
        let dir = temp_dir();
        let [gate, oil, _] = written(&dir);
        let mut later = oil.clone();
        later["text"] = "[ ] Order oil and coal #house".into();
        applied(&dir, &[later]).unwrap();
        let log = lines(&dir, &commands_id(&dir));

        let mut stale = oil.clone();
        stale["text"] = "[ ] Order oil today #house".into();
        let mut unknown = gate.clone();
        unknown["id"] = "gutter".into();
        for (given, why) in [
            (vec![stale], "oil has changed since it was read"),
            (vec![gate.clone(), unknown], "no note has the id gutter"),
            (vec![gate.clone(), gate.clone()], "gate is given twice"),
        ] {
            let said = applied(&dir, &given).unwrap_err().to_string();
            assert_eq!(said, format!("{why}, so nothing was written"));
        }
        let apply_text = |input: &str| {
            apply(
                &dir,
                "main",
                Key::from_bytes([7; 32]),
                input,
                &mut Vec::new(),
            )
            .unwrap_err()
            .to_string()
        };
        let gate = gate.to_string();
        for (input, why) in [
            (format!("{gate}\nnot a note\n"), "line 2 is not a note"),
            (format!("[1, 2]\n{gate}"), "line 1 is not a note"),
            (r#"{"s":1,"c":"sealed"}"#.to_owned(), "line 1 is not a note"),
            (format!("{gate}\n\n{gate}"), "line 2 is not a note"),
            (
                r#"{"v":99,"id":"gate"}"#.to_owned(),
                "line 1 is from a later fzn",
            ),
        ] {
            assert_eq!(apply_text(&input), format!("{why}, so nothing was written"));
        }
        assert_eq!(lines(&dir, &commands_id(&dir)), log, "and nothing was");

        // Nothing given is nothing to do, and a key that opens nothing here writes nothing.
        apply(
            &dir,
            "main",
            Key::from_bytes([7; 32]),
            "\n\n",
            &mut Vec::new(),
        )
        .unwrap();
        let wrong = apply(
            &dir,
            "main",
            Key::from_bytes([9; 32]),
            &gate,
            &mut Vec::new(),
        );
        assert!(
            wrong
                .unwrap_err()
                .to_string()
                .starts_with("the passphrase does not open main")
        );
        assert_eq!(lines(&dir, &commands_id(&dir)), log);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_tick_is_a_version_a_note_in_the_commands_own_log_said_as_the_export_says_it() {
        let dir = temp_dir();
        set_writer(&dir, "3f9a01c2").unwrap();
        let mut app = open(&dir, "3f9a01c2");
        app.put(rec("oil", "[ ] Order oil #house", 10)).unwrap();
        app.put(rec("gate", "[ ] Fix the gate #house", 20)).unwrap();
        app.put(rec("boiler", "Boiler #house", 30)).unwrap();
        let apps = lines(&dir, "3f9a01c2");

        for (ids, why) in [
            (&["oil", "gutter"][..], "no note has the id gutter"),
            (&["oil", "boiler"], "boiler is not an open task"),
        ] {
            let said = ticked(&dir, ids, true).unwrap_err().to_string();
            assert_eq!(said, format!("{why}, so nothing was written"));
        }
        assert!(lines(&dir, &commands_id(&dir)).is_empty());

        let said = ticked(&dir, &["oil", "gate"], true).unwrap();
        let id = commands_id(&dir);
        assert_eq!(lines(&dir, &id).len(), 2, "a version a note");
        let mut exported = Vec::new();
        Store::open(&dir, String::new(), seal())
            .unwrap()
            .export(&mut exported)
            .unwrap();
        let exported = String::from_utf8(exported).unwrap();
        let line = |id: &str| {
            let of = |line: &&str| Record::read(line.as_bytes()).unwrap().id == id;
            format!("{}\n", exported.lines().find(of).unwrap())
        };
        assert_eq!(said, [line("oil"), line("gate")].concat());
        let gate = Record::read(line("gate").as_bytes()).unwrap();
        assert_eq!(
            (gate.text.as_str(), gate.archived),
            ("[x] Fix the gate #house", true)
        );
        assert_eq!(gate.device, id);

        // Brought back by the next run, as one more version in the same log.
        let said = ticked(&dir, &["gate"], false).unwrap();
        let gate = Record::read(said.as_bytes()).unwrap();
        assert_eq!(
            (gate.text.as_str(), gate.archived),
            ("[ ] Fix the gate #house", false)
        );
        assert_eq!(commands_id(&dir), id);
        assert_eq!(lines(&dir, &id).len(), 3);
        assert_eq!(
            lines(&dir, "3f9a01c2"),
            apps,
            "and the app's log is the app's"
        );

        // A key that opens nothing here writes nothing.
        let wrong = tick(
            &dir,
            "main",
            Key::from_bytes([9; 32]),
            &["oil".into()],
            false,
            &mut Vec::new(),
        );
        assert_eq!(
            wrong.unwrap_err().to_string(),
            "the passphrase does not open main: run fzn key to enter it again"
        );
        assert_eq!(lines(&dir, &id).len(), 3);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn each_writer_here_compacts_its_own_log_by_one_rule_and_leaves_the_others() {
        let dir = temp_dir();
        let grown = |log: &str, id: &str, text: &str| {
            let versions: Vec<String> = (0..1_500)
                .map(|n| rec(id, text, n).sealed(&seal(), &System).unwrap())
                .collect();
            Logs::open(&dir).unwrap().append(log, &versions).unwrap();
        };
        set_writer(&dir, "3f9a01c2").unwrap();
        grown("3f9a01c2", "oil", "[ ] Order oil #house");
        keep_id(&dir.join(".commands"), "c0ffee00").unwrap();
        grown("c0ffee00", "gate", "[ ] Fix the gate #house");
        let commands = lines(&dir, "c0ffee00");

        let key = Key::from_bytes([7; 32]);
        let mut app = Device::new(Box::new(System), "3f9a01c2", Weights::default(), key);
        super::open(&dir, "main", Writer::App, &mut app).unwrap();
        let apps = read_id(&dir.join(".writer")).unwrap().unwrap();
        assert_ne!(apps, "3f9a01c2");
        assert!(lines(&dir, "3f9a01c2").is_empty());
        assert_eq!(lines(&dir, "c0ffee00"), commands);

        let compacted = lines(&dir, &apps);
        ticked(&dir, &["gate"], true).unwrap();
        let id = commands_id(&dir);
        assert_ne!(id, "c0ffee00");
        assert!(lines(&dir, "c0ffee00").is_empty());
        let log = lines(&dir, &id);
        assert_eq!(
            Header::read(&log[0]),
            Some(Header {
                compacts: vec!["c0ffee00".into()],
                snapshot: 1
            })
        );
        assert_eq!(log.len(), 3, "the header, the snapshot, the tick");
        assert_eq!(lines(&dir, &apps), compacted);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_commands_log_retired_from_elsewhere_stays_until_they_carry_it_into_their_own() {
        let dir = temp_dir();
        set_writer(&dir, "3f9a01c2").unwrap();
        keep_id(&dir.join(".commands"), "c0ffee00").unwrap();
        let mut logs = Logs::open(&dir).unwrap();
        let gate = rec("gate", "[ ] Fix the gate #house", 10);
        logs.append("c0ffee00", &[gate.sealed(&seal(), &System).unwrap()])
            .unwrap();
        // A copy of this directory elsewhere wrote under the commands' id, and compacted it.
        let header = Header {
            compacts: vec!["c0ffee00".into()],
            snapshot: 0,
        };
        logs.append("feed0001", &[header.line()]).unwrap();

        // The app retires it, and leaves it for its writer.
        let key = Key::from_bytes([7; 32]);
        let mut app = Device::new(Box::new(System), "3f9a01c2", Weights::default(), key);
        super::open(&dir, "main", Writer::App, &mut app).unwrap();
        assert_eq!(lines(&dir, "c0ffee00").len(), 1);

        // The next command carries what it held into a log of its own, as any writer does.
        ticked(&dir, &["gate"], true).unwrap();
        let id = commands_id(&dir);
        assert_ne!(id, "c0ffee00");
        assert!(lines(&dir, "c0ffee00").is_empty());
        let log = lines(&dir, &id);
        assert_eq!(
            Header::read(&log[0]).unwrap().compacts,
            ["c0ffee00".to_owned()]
        );
        let held = |line: &str| match Record::read_line(&seal(), line) {
            Read::Version(record) => (record.text, record.archived),
            other => panic!("{other:?}"),
        };
        assert_eq!(held(&log[1]), ("[ ] Fix the gate #house".into(), false));
        assert_eq!(held(&log[2]), ("[x] Fix the gate #house".into(), true));
        fs::remove_dir_all(&dir).unwrap();
    }
}
