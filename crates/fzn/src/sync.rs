//! Sync, as the terminal does it: the logs of a space kept in step with a relay over a
//! websocket. The relay is in `relay`, and the websocket both ends hold is in `wire`.
//!
//! The engine's `Replica` knows what to send and what to keep. This module gives it the two
//! things it has not got, the files and the socket: a [`Shelf`] is a replica kept as the logs in
//! one directory, and the app puts one on the end of a connection to a relay.
//!
//! The app does this on a thread of its own and tells the session nothing. What arrives is
//! added to the files, and the app finds it there the way it finds what another instance
//! wrote. Only one process at a time syncs a directory, which a lock on a file in it settles:
//! a second instance leaves it to the first, and takes over when the first is gone.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::replica::{Message, Out, Peer, Replica, names_a_log};

use crate::wire::{GREETING, PORT, VERSION, Wire};

/// How often the logs are looked at for what has been added to them.
const LOOK: Duration = Duration::from_millis(250);
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MOST: Duration = Duration::from_secs(30);
/// The file whose lock says which process is syncing a directory.
const LOCK_FILE: &str = ".sync.lock";
/// The relay, as the app numbers its one peer.
const RELAY: Peer = 0;

/// A replica kept as the logs in one directory. While it lives, no other process syncs that
/// directory.
struct Shelf {
    dir: PathBuf,
    replica: Replica,
    /// How many bytes of each log are in the replica.
    held: BTreeMap<String, u64>,
    _lock: File,
}

impl Shelf {
    /// Opens a directory that is there already and takes in every log in it. Gives nothing if
    /// another process is syncing the directory.
    fn open(dir: &Path) -> Result<Option<Self>> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(LOCK_FILE))
            .with_context(|| format!("open {}", dir.display()))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(e)) => return Err(e.into()),
        }
        let mut shelf = Self {
            dir: dir.to_owned(),
            replica: Replica::new(),
            held: BTreeMap::new(),
            _lock: lock,
        };
        shelf.grown()?;
        Ok(Some(shelf))
    }

    /// Takes in what has been added to the logs since they were last looked at, by the app or
    /// by anything else, and gives what the replica wants done about it. Gives nothing if the
    /// logs are no longer what the replica holds, because one was replaced or taken away: the
    /// shelf is then to be opened again.
    fn grown(&mut self) -> Result<Option<Vec<Out>>> {
        let mut paths: Vec<PathBuf> = fs::read_dir(&self.dir)
            .with_context(|| format!("read {}", self.dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        paths.sort();
        let mut out = Vec::new();
        let mut seen = BTreeSet::new();
        for path in paths {
            let Some(log) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .filter(|stem| names_a_log(stem))
            else {
                continue;
            };
            seen.insert(log.to_owned());
            let from = self.held.get(log).copied().unwrap_or(0);
            let mut file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
            let len = file.metadata()?.len();
            if len < from {
                return Ok(None);
            }
            if len == from {
                continue;
            }
            file.seek(SeekFrom::Start(from))?;
            let mut bytes = Vec::new();
            file.take(len - from)
                .read_to_end(&mut bytes)
                .with_context(|| format!("read {}", path.display()))?;
            // A last line with no newline is still being written. It is left for the next look.
            let Some(end) = bytes.iter().rposition(|&b| b == b'\n') else {
                continue;
            };
            let lines: Vec<String> = bytes[..end]
                .split(|&b| b == b'\n')
                .map(|line| String::from_utf8_lossy(line).into_owned())
                .collect();
            out.extend(self.replica.append(log, lines));
            self.held.insert(log.to_owned(), from + end as u64 + 1);
        }
        if self.held.keys().any(|log| !seen.contains(log)) {
            return Ok(None);
        }
        Ok(Some(out))
    }

    /// Adds lines that came from a peer to the end of a log. Says no if the log is not what the
    /// replica holds, because something else has added to it: the shelf is then to be opened
    /// again. The log is locked meanwhile, as the app locks it to write, so that nothing lands
    /// between the look and the write.
    fn keep(&mut self, log: &str, lines: &[String]) -> Result<bool> {
        let path = self.dir.join(format!("{log}.jsonl"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        file.lock()?;
        let kept = self.keep_locked(&file, &path, log, lines);
        let _ = file.unlock();
        kept
    }

    fn keep_locked(
        &mut self,
        mut file: &File,
        path: &Path,
        log: &str,
        lines: &[String],
    ) -> Result<bool> {
        let held = self.held.get(log).copied().unwrap_or(0);
        let len = file.metadata()?.len();
        if len < held {
            return Ok(false);
        }
        if len > held {
            // The start of a line and no end to it is what a crash leaves. It was never a line,
            // and nothing is in the middle of writing it while the lock is held, so it goes.
            let mut rest = Vec::new();
            let mut reader = File::open(path)?;
            reader.seek(SeekFrom::Start(held))?;
            reader.read_to_end(&mut rest)?;
            if rest.contains(&b'\n') {
                return Ok(false);
            }
            file.set_len(held)?;
        }
        let mut text = String::new();
        for line in lines {
            text.push_str(line);
            text.push('\n');
        }
        file.write_all(text.as_bytes())
            .with_context(|| format!("append to {}", path.display()))?;
        file.flush()?;
        self.held.insert(log.to_owned(), held + text.len() as u64);
        Ok(true)
    }
}

/// What the app is told about its sync, when it changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum News {
    /// Lines are passing to and from the relay.
    Connected,
    /// The relay cannot be reached, or would not have us, and why. It is tried again.
    Unreachable(String),
    /// Another fzn on this machine is syncing this space, and this one leaves it to it.
    Elsewhere,
    /// The log of this name here and the one the relay has are not one log, so nothing of it
    /// is passing between them.
    Forked(String),
}

/// A relay's address as it is given, `ws://host:port` or `host:port` or `host`, as the host and
/// port to connect to.
pub fn authority(relay: &str) -> Result<String> {
    let relay = relay.trim();
    if relay.starts_with("wss://") {
        bail!("wss is not built yet: use ws:// over a network you trust");
    }
    let rest = relay.strip_prefix("ws://").unwrap_or(relay);
    let host = rest.split('/').next().unwrap_or_default();
    if host.is_empty() || rest.contains("://") {
        bail!("{relay:?} is not the address of a relay");
    }
    // A port is whatever follows the last colon, unless that colon is inside an IPv6 address.
    let has_port = host
        .rsplit_once(':')
        .is_some_and(|(_, port)| !port.contains(']') && !port.is_empty());
    Ok(if has_port {
        host.to_owned()
    } else {
        format!("{host}:{PORT}")
    })
}

/// Connects to a relay and asks it for a space.
fn dial(authority: &str, space: &str) -> Result<Wire> {
    let mut failure = anyhow!("{authority} has no address");
    let mut stream = None;
    for address in authority
        .to_socket_addrs()
        .with_context(|| format!("look up {authority}"))?
    {
        match TcpStream::connect_timeout(&address, GREETING) {
            Ok(connected) => {
                stream = Some(connected);
                break;
            }
            Err(e) => failure = e.into(),
        }
    }
    let Some(stream) = stream else {
        return Err(failure);
    };
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(GREETING))?;
    stream.set_write_timeout(Some(GREETING))?;
    let request = format!("ws://{authority}/{VERSION}/{space}");
    let (socket, _) = tungstenite::client(request.as_str(), stream).map_err(|e| anyhow!("{e}"))?;
    Wire::new(socket)
}

/// Keeps a space's directory in step with a relay for as long as the app runs. What changes is
/// told through the receiver. Nothing here touches the session: lines that arrive are added to
/// the logs, where the app finds them.
pub fn start(dir: PathBuf, relay: String, space: String) -> Receiver<News> {
    let (news, heard) = mpsc::channel();
    thread::spawn(move || {
        let mut told = None;
        let mut tell = |what: News| {
            // The same thing is said once, however many times it happens in a row.
            if told.as_ref() != Some(&what) {
                told = Some(what.clone());
                let _ = news.send(what);
            }
        };
        let authority = match authority(&relay) {
            Ok(authority) => authority,
            Err(why) => return tell(News::Unreachable(format!("{why:#}"))),
        };
        // A space is a directory on the relay and part of the address it is asked for by.
        if !names_a_log(&space) {
            let why = format!("a space has to be plainly named to sync, and {space:?} is not");
            return tell(News::Unreachable(why));
        }
        let mut wait = RETRY_FIRST;
        loop {
            match session(&dir, &authority, &space, &mut tell) {
                // Whoever has the directory may go at any time, so it is asked for often.
                Ok(Ended::Elsewhere) => {
                    tell(News::Elsewhere);
                    wait = RETRY_FIRST;
                }
                // The logs changed under the replica. It is opened again from what is there.
                Ok(Ended::Stale) => {
                    wait = RETRY_FIRST;
                    continue;
                }
                Ok(Ended::Dropped) => wait = RETRY_FIRST,
                Err(why) => tell(News::Unreachable(format!("{why:#}"))),
            }
            thread::sleep(wait);
            wait = (wait * 2).min(RETRY_MOST);
        }
    });
    heard
}

/// How one go at syncing ended.
enum Ended {
    /// Another process has the directory.
    Elsewhere,
    /// The logs are no longer what the replica holds.
    Stale,
    /// The connection was made, and then lost.
    Dropped,
}

/// One go at syncing: open the directory, connect, and pass lines both ways until something
/// gives. An error is a relay that could not be reached or would not have us.
fn session(dir: &Path, authority: &str, space: &str, tell: &mut impl FnMut(News)) -> Result<Ended> {
    let Some(mut shelf) = Shelf::open(dir)? else {
        return Ok(Ended::Elsewhere);
    };
    let mut wire = dial(authority, space)?;
    tell(News::Connected);
    let mut looked = Instant::now();
    let mut out = shelf.replica.connect(RELAY);
    loop {
        for step in std::mem::take(&mut out) {
            match step {
                Out::Keep { log, lines } => {
                    if !shelf.keep(&log, &lines)? {
                        return Ok(Ended::Stale);
                    }
                }
                Out::Send(_, message) => {
                    if wire.say(message.encode()?).is_err() {
                        return Ok(Ended::Dropped);
                    }
                }
                Out::Forked { log, .. } => tell(News::Forked(log)),
                // Retiring a log and compacting are the device's business, and the device is
                // on its way into the engine; the file-backed shelf here is about to go.
                Out::Retire(_) | Out::Superseded(_) => {}
            }
        }
        if wire.keep_up().is_err() {
            return Ok(Ended::Dropped);
        }
        match wire.hear() {
            Ok(Some(text)) => {
                if let Some(message) = Message::decode(&text) {
                    out = shelf.replica.receive(RELAY, message);
                }
            }
            Ok(None) => {}
            Err(_) => return Ok(Ended::Dropped),
        }
        if out.is_empty() && looked.elapsed() >= LOOK {
            looked = Instant::now();
            match shelf.grown()? {
                Some(grown) => out = grown,
                None => return Ok(Ended::Stale),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::tests::{read, temp_dir, write};

    fn lines(texts: &[&str]) -> Vec<String> {
        texts.iter().map(|&t| t.to_owned()).collect()
    }

    #[test]
    fn a_shelf_holds_what_the_logs_hold_and_takes_in_what_is_added() {
        let dir = temp_dir();
        write(&dir, "dev-a", "a1\na2\n");
        write(&dir, "not a log", "x\n");
        fs::write(dir.join("notes.txt"), "not a log either\n").unwrap();
        let mut shelf = Shelf::open(&dir).unwrap().unwrap();
        assert_eq!(shelf.replica.lines("dev-a"), ["a1", "a2"]);
        assert_eq!(shelf.replica.have().len(), 1, "only plainly named logs");
        assert!(
            Shelf::open(&dir).unwrap().is_none(),
            "one syncer to a directory"
        );

        // A line still being written waits for its end, and an empty line is a line.
        assert!(shelf.grown().unwrap().unwrap().is_empty());
        write(&dir, "dev-a", "\na4 is half");
        write(&dir, "dev-b", "b1\n");
        shelf.grown().unwrap().unwrap();
        assert_eq!(shelf.replica.lines("dev-a"), ["a1", "a2", ""]);
        assert_eq!(shelf.replica.lines("dev-b"), ["b1"]);
        write(&dir, "dev-a", " written\n");
        shelf.grown().unwrap().unwrap();
        assert_eq!(
            shelf.replica.lines("dev-a"),
            ["a1", "a2", "", "a4 is half written"]
        );

        // What a peer sent is added to the file, and is not then taken for something new.
        shelf.replica.connect(7);
        let out = shelf.replica.receive(
            7,
            Message::Lines {
                log: "dev-b".into(),
                after: shelf.replica.have()["dev-b"],
                lines: lines(&["b2", "b3"]),
            },
        );
        let [Out::Keep { log, lines: kept }] = &out[..] else {
            panic!("{out:?}");
        };
        assert!(shelf.keep(log, kept).unwrap());
        assert_eq!(read(&dir, "dev-b"), "b1\nb2\nb3\n");
        assert!(shelf.grown().unwrap().unwrap().is_empty());
        assert_eq!(shelf.replica.lines("dev-b"), ["b1", "b2", "b3"]);

        // Once the shelf is gone the directory is free again, and holds the same.
        let have = shelf.replica.have();
        drop(shelf);
        let again = Shelf::open(&dir).unwrap().unwrap();
        assert_eq!(again.replica.have(), have);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_shelf_notices_when_the_logs_are_not_what_it_holds() {
        let dir = temp_dir();
        write(&dir, "dev-a", "a1\na2\n");
        write(&dir, "dev-b", "b1\n");
        let mut shelf = Shelf::open(&dir).unwrap().unwrap();

        // Something else added to a log between the replica taking lines and their being kept.
        write(&dir, "dev-b", "from elsewhere\n");
        assert!(!shelf.keep("dev-b", &lines(&["b2"])).unwrap());
        assert_eq!(
            read(&dir, "dev-b"),
            "b1\nfrom elsewhere\n",
            "left as it was"
        );

        // What a crash leaves, the start of a line with no end, is not a line and goes.
        write(&dir, "dev-a", "{\"half");
        assert!(shelf.keep("dev-a", &lines(&["a3"])).unwrap());
        assert_eq!(read(&dir, "dev-a"), "a1\na2\na3\n");

        // A log replaced by a shorter one, or taken away.
        fs::write(dir.join("dev-a.jsonl"), "x\n").unwrap();
        assert!(shelf.grown().unwrap().is_none());
        drop(shelf);
        let mut shelf = Shelf::open(&dir).unwrap().unwrap();
        assert_eq!(
            shelf.replica.lines("dev-a"),
            ["x"],
            "opened again, it is as on disk"
        );
        fs::remove_file(dir.join("dev-b.jsonl")).unwrap();
        assert!(shelf.grown().unwrap().is_none());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_relay_is_given_as_a_host_and_a_port() {
        assert_eq!(authority("ws://nas.local:4000").unwrap(), "nas.local:4000");
        assert_eq!(authority(" nas.local ").unwrap(), "nas.local:3960");
        assert_eq!(authority("ws://10.0.0.2/").unwrap(), "10.0.0.2:3960");
        assert_eq!(authority("ws://[::1]").unwrap(), "[::1]:3960");
        assert_eq!(authority("[::1]:4000").unwrap(), "[::1]:4000");
        assert!(authority("wss://nas.local").is_err());
        assert!(authority("http://nas.local").is_err());
        assert!(authority("").is_err());
    }
}
