//! A space's directory: a file of lines for each log, the locks that say who writes them, and
//! the ids this machine writes the space under.
//!
//! Every client open on a space on this machine writes it: each window, and each command that
//! writes beside them, `fzn tick` say, and an importer. Each is a writer of its own, so that a
//! log has one process appending to it, and they cannot conflict, since versions settle by their
//! stamps whichever log they are in. A client takes a slot of a small pool for its life: slot 0
//! is `.writer`, slot n `.writer.n`, the lowest nobody holds first, held by a lock on that file
//! and given back by the kernel however the client ends. The slot's file holds the id it writes
//! under, which the next holder of the slot goes on with, so the pool grows to the most clients
//! ever open at once and no further. Nobody waits for a slot, and nothing is locked per write.
//!
//! So a log may be read while another process appends to it, and what is read of it is only as
//! far as its last whole line; an unfinished line is cut only by the process about to append to
//! that log, since only it could have left one there. A window reads every log when it opens the
//! space and, four times a second after that, what has been added since, and gives the lines to
//! its device, which knows what they say: that is how windows see each other. A command does the
//! same once. One window at a time holds the keeper's lease, `.relay`, and with it the socket to
//! the relay and the appending of the relay's lines, since those are logs too and want one
//! appender; another window takes the lease within a look of its going, and needs nothing handed
//! over, since what the keeper kept is in the directory. Clients share the space's lock, `.lock`,
//! and a relay holds it alone, so a relay and a client never share a directory. The exporter and
//! the importers read the logs here as a [`Store`].
//!
//! A writer's id lives in the directory, not in the config, because it names the log this
//! machine writes in this space, and a compaction changes it.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read as _, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::PoisonError;

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::replica::{Header, names_a_log};
use fzn_engine::seal::is_sealed;
use fzn_engine::{Device, Given, Key, Moment, Notes, Read, Record, Seal, Session, Todo, Weights};

use crate::config::DEVICE_FILE;
use crate::system::{self, System};

/// The file whose lock says the directory is in use: shared by clients, held alone by a relay.
const LOCK_FILE: &str = ".lock";
/// Slot 0's file, and the start of every other slot's, `.writer.1` and on.
const SLOT_FILE: &str = ".writer";
/// The keeper's lease: which window holds the socket to the relay.
const KEEPER_FILE: &str = ".relay";
/// Where the commands kept their id before they took slots, and slot 1's file since.
const COMMANDS_FILE: &str = ".commands";
/// More slots than this would be clients without end, which is a bug and not a use.
const SLOTS_MOST: usize = 64;

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

    /// Deletes a log's file, unless a slot here names it. A header from elsewhere retires such a
    /// log when a copy of this directory wrote under its name, and its lines may then be ones
    /// nobody else has: the slot's holder carries them into a log of its own, and keeps the new
    /// id, before it goes. One that is not there is already as asked.
    pub fn delete(&mut self, log: &str) -> Result<()> {
        if self.slot_names(log)? {
            return Ok(());
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

    /// Whether a slot in the directory names this log, as the one its holder writes.
    fn slot_names(&self, log: &str) -> Result<bool> {
        Ok(self.slot_naming(log)?.is_some())
    }

    /// The file of the slot in the directory that names this log, if one does.
    fn slot_naming(&self, log: &str) -> Result<Option<PathBuf>> {
        let entries =
            fs::read_dir(&self.dir).with_context(|| format!("read {}", self.dir.display()))?;
        for entry in entries.filter_map(|entry| entry.ok()) {
            let named = entry.file_name().to_str().is_some_and(holds_an_id)
                && read_id(&entry.path())?.as_deref() == Some(log);
            if named {
                return Ok(Some(entry.path()));
            }
        }
        Ok(None)
    }

    /// Does what a device writing in `slot` asks of the directory: keeps its lines, then its new
    /// id, then deletes the logs it retires. The lines first, so that a compaction stopped half
    /// way leaves both logs and not neither, and the id before the deleting, so that the old log
    /// is no longer its writer's when it goes. Lines of a log another slot here names, which
    /// the relay holds further than the disk does, as after a power cut lost a tail that had
    /// reached it, are appended under that slot's lock, when nobody holds it, since this is then
    /// that log's one appender for the moment; while a sibling holds it, the log is its to write
    /// and they are left.
    pub fn keep(&mut self, todo: &Todo, slot: &mut Slot) -> Result<()> {
        for kept in &todo.keep {
            if kept.log != slot.id()
                && let Some(path) = self.slot_naming(&kept.log)?
            {
                let theirs = lock_file(&path)?;
                if try_lock(&theirs, false).is_ok() {
                    self.append(&kept.log, &kept.lines)?;
                }
                continue;
            }
            self.append(&kept.log, &kept.lines)?;
        }
        if let Some(id) = &todo.device {
            slot.keep_id(id)?;
        }
        for log in &todo.delete {
            self.delete(log)?;
        }
        Ok(())
    }
}

/// Opens a space's directory for a device writing in `slot`: every log is given to the device
/// and what it asks for kept, and then whatever opening it is due, a compaction say. Fails if the
/// key opens none of the sealed lines there, since nothing could be written.
pub fn open(dir: &Path, space: &str, slot: &mut Slot, device: &mut Device) -> Result<Logs> {
    let mut logs = Logs::open(dir)?;
    for (log, lines) in logs.load()? {
        let todo = device.load(&log, lines);
        logs.keep(&todo, slot)?;
    }
    if !device.key_verified() {
        bail!("the passphrase does not open {space}: run fzn key to enter it again");
    }
    let todo = device.opened();
    logs.keep(&todo, slot)?;
    Ok(logs)
}

/// Ticks these tasks, or unticks them, as Ctrl-V does, while windows may have the space open.
/// Writes each note as it now stands, as `fzn query` writes it. All or none: if one cannot be,
/// nothing is written.
pub fn tick(
    dir: &Path,
    space: &str,
    config_dir: &Path,
    key: Key,
    ids: &[String],
    done: bool,
    out: &mut impl Write,
) -> Result<()> {
    beside(dir, space, config_dir, key, out, |device| {
        device.tick(ids, done)
    })
}

/// Writes notes given as the lines `fzn query` writes, one a line, as [`Device::apply`] says,
/// while windows may have the space open. Writes each note it was given as it now stands, in
/// the order given. All or none: a line that is not a note, or one the device refuses, is said
/// and nothing is written.
pub fn apply(
    dir: &Path,
    space: &str,
    config_dir: &Path,
    key: Key,
    input: &str,
    out: &mut impl Write,
) -> Result<()> {
    let given = given(input)?;
    if given.is_empty() {
        return Ok(());
    }
    beside(dir, space, config_dir, key, out, |device| {
        device.apply(given)
    })
}

/// Does what a command asks of a device of its own, as a client of the space beside whatever
/// windows have it open: in a slot of its own, which it never waits for, and hearing no relay.
/// Writes out the notes the device gives back.
fn beside(
    dir: &Path,
    space: &str,
    config_dir: &Path,
    key: Key,
    out: &mut impl Write,
    act: impl FnOnce(&mut Device) -> Result<(Todo, Vec<Record>), String>,
) -> Result<()> {
    let _in_use = Lock::share(dir, space)?;
    let mut slot = Slot::take(dir, config_dir)?;
    let mut device = Device::new(Box::new(System), slot.id(), Weights::default(), key);
    let mut logs = open(dir, space, &mut slot, &mut device)?;
    let (todo, notes) = act(&mut device).map_err(|why| anyhow!("{why}, so nothing was written"))?;
    logs.keep(&todo, &mut slot)?;
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

/// The right to use a space's directory, for as long as it is held: shared by every client, so
/// that windows, commands and importers run side by side, and refused while a relay holds it.
pub struct Lock {
    _file: File,
}

impl Lock {
    /// Shares the lock on a space's directory, making the directory if it is new, or says that
    /// a relay holds it.
    pub fn share(dir: &Path, space: &str) -> Result<Lock> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let path = dir.join(LOCK_FILE);
        let file = lock_file(&path)?;
        match try_lock(&file, true) {
            Ok(()) => Ok(Lock { _file: file }),
            Err(TryLockError::WouldBlock) => {
                bail!("a relay, or an fzn from before 0.26, holds {space}")
            }
            Err(TryLockError::Error(e)) => {
                Err(e).with_context(|| format!("lock {}", path.display()))
            }
        }
    }
}

/// A slot of the pool: the id this client writes under, and the lock that says the slot is this
/// client's, held for its life and given back by the kernel however it ends.
pub struct Slot {
    file: File,
    path: PathBuf,
    id: String,
}

impl Slot {
    /// Takes the lowest slot nobody holds, making the directory if it is new, and the id kept in
    /// it: for slot 0 on a space new here the one this machine had for every space, if it had
    /// one, and otherwise a new one, kept.
    pub fn take(dir: &Path, config_dir: &Path) -> Result<Slot> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        commands_to_slot_one(dir)?;
        for n in 0..SLOTS_MOST {
            let path = match n {
                0 => dir.join(SLOT_FILE),
                n => dir.join(format!("{SLOT_FILE}.{n}")),
            };
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("open {}", path.display()))?;
            match try_lock(&file, false) {
                Ok(()) => {}
                Err(TryLockError::WouldBlock) => continue,
                Err(TryLockError::Error(e)) => {
                    return Err(e).with_context(|| format!("lock {}", path.display()));
                }
            }
            let mut text = String::new();
            file.read_to_string(&mut text)
                .with_context(|| format!("read {}", path.display()))?;
            let mut slot = Slot {
                file,
                path,
                id: text.trim().to_owned(),
            };
            if slot.id.is_empty() {
                let legacy = match n {
                    0 => read_id(&config_dir.join(DEVICE_FILE))?,
                    _ => None,
                };
                slot.keep_id(&legacy.unwrap_or_else(new_id))?;
            }
            return Ok(slot);
        }
        bail!("{SLOTS_MOST} clients have {} open at once", dir.display())
    }

    /// The id this slot's client writes under.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Writes a new id over the old, through the locked file and in place, never by renaming
    /// another over it, which would leave the lock on a file no longer in the directory. The id
    /// goes over the old one before the file is cut to its length, so that another process
    /// reading it sees one id or the other and never an empty file.
    fn keep_id(&mut self, id: &str) -> Result<()> {
        let path = &self.path;
        let line = format!("{id}\n");
        self.file
            .seek(SeekFrom::Start(0))
            .and_then(|_| self.file.write_all(line.as_bytes()))
            .and_then(|()| self.file.set_len(line.len() as u64))
            .and_then(|()| self.file.flush())
            .with_context(|| format!("write {}", path.display()))?;
        self.id = id.to_owned();
        Ok(())
    }
}

/// Makes the commands' file from before slots slot 1's, once, by a link that fails when slot 1
/// is there already, so that a client that took slot 1 meanwhile keeps the file it locked.
fn commands_to_slot_one(dir: &Path) -> Result<()> {
    let (from, to) = (dir.join(COMMANDS_FILE), dir.join(format!("{SLOT_FILE}.1")));
    match fs::hard_link(&from, &to) {
        Ok(()) => fs::remove_file(&from).with_context(|| format!("remove {}", from.display())),
        Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::AlreadyExists) => Ok(()),
        Err(e) => Err(e).with_context(|| format!("link {} as {}", from.display(), to.display())),
    }
}

/// The keeper's lease: the one window that holds the socket to the relay and keeps the relay's
/// lines in the directory, so that each of those logs has one appender too.
pub struct Keeper {
    _file: File,
}

impl Keeper {
    /// Takes the lease, or gives nothing while another window holds it.
    pub fn take(dir: &Path) -> Result<Option<Keeper>> {
        let path = dir.join(KEEPER_FILE);
        let file = lock_file(&path)?;
        match try_lock(&file, false) {
            Ok(()) => Ok(Some(Keeper { _file: file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(e)) => {
                Err(e).with_context(|| format!("lock {}", path.display()))
            }
        }
    }
}

/// Takes a lock on a file, shared or alone, or says that it would have to wait.
fn try_lock(file: &File, shared: bool) -> Result<(), TryLockError> {
    #[cfg(test)]
    let _settled = STARTING.write().unwrap_or_else(PoisonError::into_inner);
    if shared {
        file.try_lock_shared()
    } else {
        file.try_lock()
    }
}

/// Tests run side by side in one process, and a process that one of them starts holds a copy of
/// every file open in it until the program it runs has begun, and with it every lock; a test that
/// lets a lock go and takes it again could find it held. So in tests a lock is taken only while
/// no process is being started, and a process is started under [`starting`], which is held for
/// the start and not for the run. fzn itself needs nothing of the kind: every file is opened to
/// be closed when a program starts, a start returns only once the program has, and fzn never lets
/// a lock go while it runs to take it again, so a copy held for that moment costs a look at most.
#[cfg(test)]
static STARTING: std::sync::RwLock<()> = std::sync::RwLock::new(());

#[cfg(test)]
pub(crate) fn starting() -> std::sync::RwLockReadGuard<'static, ()> {
    STARTING.read().unwrap_or_else(PoisonError::into_inner)
}

fn lock_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))
}

fn new_id() -> String {
    format!("{:08x}", system::random_u64() as u32)
}

/// The id in a file, if the file is there and holds one.
fn read_id(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text.trim().to_owned()).filter(|id| !id.is_empty())),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// Whether a file in a space's directory is a slot's, or the commands' from before slots.
fn holds_an_id(name: &str) -> bool {
    name == SLOT_FILE
        || name == COMMANDS_FILE
        || name
            .strip_prefix(SLOT_FILE)
            .and_then(|n| n.strip_prefix('.'))
            .is_some_and(|n| n.parse::<usize>().is_ok())
}

/// A space on disk as the importers, `fzn query` and `fzn export` see it: the notes its logs add
/// up to, and the writer's log to add to. An importer writes in a slot of its own, beside any
/// window open on the space, and adds versions sealed as the app would; the others only read.
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

    /// Keeps the id slot 0 writes a space under, making the directory if it is new, as a window
    /// that had written there would have left it.
    pub(crate) fn set_writer(dir: &Path, id: &str) -> Result<()> {
        fs::create_dir_all(dir)?;
        fs::write(dir.join(SLOT_FILE), format!("{id}\n"))?;
        Ok(())
    }

    #[test]
    fn slots_are_taken_lowest_free_first_and_continued_by_the_next_holder() {
        let dir = temp_dir();
        let (space, config) = (dir.join("space"), dir.join("config"));
        let first = Slot::take(&space, &config).unwrap();
        let second = Slot::take(&space, &config).unwrap();
        assert_eq!(first.id().len(), 8);
        assert!(first.id().chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(first.id(), second.id());
        assert_eq!(
            read_id(&space.join(".writer")).unwrap().as_deref(),
            Some(first.id())
        );
        assert_eq!(
            read_id(&space.join(".writer.1")).unwrap().as_deref(),
            Some(second.id())
        );

        // The lowest free is taken again, with its id; a slot is never waited for.
        let (id, held) = (first.id().to_owned(), second.id().to_owned());
        drop(first);
        let again = Slot::take(&space, &config).unwrap();
        assert_eq!(again.id(), id);
        let third = Slot::take(&space, &config).unwrap();
        assert_eq!(
            read_id(&space.join(".writer.2")).unwrap().as_deref(),
            Some(third.id())
        );
        drop(second);
        assert_eq!(Slot::take(&space, &config).unwrap().id(), held);

        // A new id is written over the old in place, under the lock.
        let mut slot = Slot::take(&space, &config).unwrap();
        slot.keep_id("3f9a01c2").unwrap();
        assert_eq!(
            read_id(&space.join(".writer.1")).unwrap().as_deref(),
            Some("3f9a01c2")
        );
        drop(slot);
        assert_eq!(Slot::take(&space, &config).unwrap().id(), "3f9a01c2");

        // Slot 0 of a space new here goes on with the id the machine had for every space.
        fs::create_dir_all(&config).unwrap();
        fs::write(config.join(DEVICE_FILE), "dev-a\n").unwrap();
        let other = dir.join("other");
        assert_eq!(Slot::take(&other, &config).unwrap().id(), "dev-a");
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
    fn a_commands_file_from_before_is_slot_one() {
        let dir = temp_dir();
        set_writer(&dir, "3f9a01c2").unwrap();
        fs::write(dir.join(".commands"), "c0ffee00\n").unwrap();
        let config = dir.join("config");
        let window = Slot::take(&dir, &config).unwrap();
        assert_eq!(window.id(), "3f9a01c2");
        assert!(!dir.join(".commands").exists());
        assert_eq!(Slot::take(&dir, &config).unwrap().id(), "c0ffee00");
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_client_shares_the_directory_and_a_relay_holds_it_alone() {
        let dir = temp_dir();
        let relays = || {
            let file = File::open(dir.join(LOCK_FILE)).unwrap();
            try_lock(&file, false).map(|()| file)
        };
        let window = Lock::share(&dir, "main").unwrap();
        let tick = Lock::share(&dir, "main").unwrap();
        assert!(
            relays().is_err(),
            "a relay is refused while a client is open"
        );
        drop((window, tick));
        let relay = relays().unwrap();
        let why = Lock::share(&dir, "main").err().unwrap().to_string();
        assert_eq!(why, "a relay, or an fzn from before 0.26, holds main");
        drop(relay);
        assert!(Lock::share(&dir, "main").is_ok());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn one_window_at_a_time_holds_the_keepers_lease() {
        let dir = temp_dir();
        let keeper = Keeper::take(&dir).unwrap().unwrap();
        assert!(Keeper::take(&dir).unwrap().is_none());
        drop(keeper);
        assert!(Keeper::take(&dir).unwrap().is_some());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_window_appends_to_a_log_another_slot_here_names_only_while_nobody_holds_it() {
        let dir = temp_dir();
        let config = dir.join("config");
        let mut mine = Slot::take(&dir, &config).unwrap();
        let theirs = Slot::take(&dir, &config).unwrap();
        let (id, held) = (mine.id().to_owned(), theirs.id().to_owned());
        let mut logs = Logs::open(&dir).unwrap();
        let kept = |log: &str| fzn_engine::Kept {
            log: log.to_owned(),
            from: 0,
            lines: vec!["x".to_owned()],
        };
        let todo = Todo {
            keep: vec![kept(&id), kept(&held), kept("dev-b")],
            ..Todo::default()
        };
        logs.keep(&todo, &mut mine).unwrap();
        assert_eq!(lines(&dir, &id), ["x"]);
        assert!(lines(&dir, &held).is_empty(), "a sibling holds it");
        assert_eq!(lines(&dir, "dev-b"), ["x"]);

        drop(theirs);
        logs.keep(&todo, &mut mine).unwrap();
        assert_eq!(lines(&dir, &held), ["x"], "nobody holds it now");
        assert_eq!(Slot::take(&dir, &config).unwrap().id(), held);
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
            done: Some(Moment::utc(30)),
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
        tick(
            dir,
            "main",
            &config(dir),
            Key::from_bytes([7; 32]),
            &ids,
            done,
            &mut out,
        )?;
        Ok(String::from_utf8(out).unwrap())
    }

    /// The lines of a log.
    fn lines(dir: &Path, log: &str) -> Vec<String> {
        let text = fs::read_to_string(dir.join(format!("{log}.jsonl"))).unwrap_or_default();
        text.lines().map(str::to_owned).collect()
    }

    /// The id slot 1 writes under, which is the slot a command takes beside a window.
    fn slot_one(dir: &Path) -> String {
        read_id(&dir.join(".writer.1")).unwrap().unwrap()
    }

    /// Where the tests keep a machine's config, which is nowhere a slot looks for an id.
    fn config(dir: &Path) -> PathBuf {
        dir.join("config")
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
        apply(
            dir,
            "main",
            &config(dir),
            Key::from_bytes([7; 32]),
            &input,
            &mut out,
        )?;
        let out = String::from_utf8(out).unwrap();
        Ok(out
            .lines()
            .map(|line| Record::read(line.as_bytes()).unwrap())
            .collect())
    }

    /// A space a window has open and has written three notes in, their lines as `fzn query`
    /// writes them, and the window's slot.
    fn written(dir: &Path) -> ([serde_json::Value; 3], Slot) {
        set_writer(dir, "3f9a01c2").unwrap();
        let window = Slot::take(dir, &config(dir)).unwrap();
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
        ([line("gate"), line("oil"), line("boiler")], window)
    }

    #[test]
    fn apply_writes_what_it_is_given_by_the_apps_rules_and_says_each_note_as_it_now_stands() {
        let dir = temp_dir();
        let ([gate, oil, boiler], _window) = written(&dir);

        // Given as it was read, nothing is written, and each is said as it stands.
        let said = applied(&dir, &[oil.clone(), boiler.clone()]).unwrap();
        let ids: Vec<&str> = said.iter().map(|note| note.id.as_str()).collect();
        assert_eq!(ids, ["oil", "boiler"]);
        assert!(lines(&dir, &slot_one(&dir)).is_empty());

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
            lines(&dir, &slot_one(&dir)).len(),
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
        let ([gate, oil, _], _window) = written(&dir);
        let mut later = oil.clone();
        later["text"] = "[ ] Order oil and coal #house".into();
        applied(&dir, &[later]).unwrap();
        let log = lines(&dir, &slot_one(&dir));

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
                &config(&dir),
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
        assert_eq!(lines(&dir, &slot_one(&dir)), log, "and nothing was");

        // Nothing given is nothing to do, a byte-order mark is no line, and a key that opens
        // nothing here writes nothing.
        for input in ["\n\n".to_owned(), format!("\u{feff}{gate}")] {
            let key = Key::from_bytes([7; 32]);
            apply(&dir, "main", &config(&dir), key, &input, &mut Vec::new()).unwrap();
        }
        let wrong = apply(
            &dir,
            "main",
            &config(&dir),
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
        assert_eq!(lines(&dir, &slot_one(&dir)), log);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_tick_is_a_version_a_note_in_a_slot_of_its_own_said_as_the_query_says_it() {
        let dir = temp_dir();
        set_writer(&dir, "3f9a01c2").unwrap();
        let _window = Slot::take(&dir, &config(&dir)).unwrap();
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
        assert!(lines(&dir, &slot_one(&dir)).is_empty());

        let said = ticked(&dir, &["oil", "gate"], true).unwrap();
        let id = slot_one(&dir);
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
        assert_eq!(slot_one(&dir), id);
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
            &config(&dir),
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
    fn a_tick_takes_the_first_free_slot_and_never_waits() {
        let dir = temp_dir();
        set_writer(&dir, "3f9a01c2").unwrap();
        open(&dir, "3f9a01c2")
            .put(rec("oil", "[ ] Order oil #house", 10))
            .unwrap();
        ticked(&dir, &["oil"], true).unwrap();
        assert_eq!(
            lines(&dir, "3f9a01c2").len(),
            2,
            "with no window open, slot 0"
        );

        let window = Slot::take(&dir, &config(&dir)).unwrap();
        let other = Slot::take(&dir, &config(&dir)).unwrap();
        ticked(&dir, &["oil"], false).unwrap();
        let third = read_id(&dir.join(".writer.2")).unwrap().unwrap();
        assert_eq!(lines(&dir, &third).len(), 1);
        assert!(lines(&dir, other.id()).is_empty());
        assert_eq!(lines(&dir, window.id()).len(), 2);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn each_slot_here_compacts_its_own_log_by_one_rule_and_leaves_the_others() {
        let dir = temp_dir();
        let grown = |log: &str, id: &str, text: &str| {
            let versions: Vec<String> = (0..1_500)
                .map(|n| rec(id, text, n).sealed(&seal(), &System).unwrap())
                .collect();
            Logs::open(&dir).unwrap().append(log, &versions).unwrap();
        };
        set_writer(&dir, "3f9a01c2").unwrap();
        grown("3f9a01c2", "oil", "[ ] Order oil #house");
        fs::write(dir.join(".writer.1"), "c0ffee00\n").unwrap();
        grown("c0ffee00", "gate", "[ ] Fix the gate #house");
        let commands = lines(&dir, "c0ffee00");

        let key = Key::from_bytes([7; 32]);
        let mut app = Device::new(Box::new(System), "3f9a01c2", Weights::default(), key);
        let mut window = Slot::take(&dir, &config(&dir)).unwrap();
        super::open(&dir, "main", &mut window, &mut app).unwrap();
        let apps = read_id(&dir.join(".writer")).unwrap().unwrap();
        assert_ne!(apps, "3f9a01c2");
        assert!(lines(&dir, "3f9a01c2").is_empty());
        assert_eq!(lines(&dir, "c0ffee00"), commands);

        let compacted = lines(&dir, &apps);
        ticked(&dir, &["gate"], true).unwrap();
        let id = slot_one(&dir);
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
    fn a_slots_log_retired_from_elsewhere_stays_until_its_holder_carries_it_into_its_own() {
        let dir = temp_dir();
        set_writer(&dir, "3f9a01c2").unwrap();
        fs::write(dir.join(".writer.1"), "c0ffee00\n").unwrap();
        let mut logs = Logs::open(&dir).unwrap();
        let gate = rec("gate", "[ ] Fix the gate #house", 10);
        logs.append("c0ffee00", &[gate.sealed(&seal(), &System).unwrap()])
            .unwrap();
        // A copy of this directory elsewhere wrote under slot 1's id, and compacted it.
        let header = Header {
            compacts: vec!["c0ffee00".into()],
            snapshot: 0,
        };
        logs.append("feed0001", &[header.line()]).unwrap();

        // A window retires it, and leaves it for its writer.
        let key = Key::from_bytes([7; 32]);
        let mut app = Device::new(Box::new(System), "3f9a01c2", Weights::default(), key);
        let mut window = Slot::take(&dir, &config(&dir)).unwrap();
        super::open(&dir, "main", &mut window, &mut app).unwrap();
        assert_eq!(lines(&dir, "c0ffee00").len(), 1);

        // The next client in slot 1 carries what it held into a log of its own, as any writer
        // does.
        ticked(&dir, &["gate"], true).unwrap();
        let id = slot_one(&dir);
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
