//! Sync, as the terminal does it: the logs of a space kept in step with a relay over a
//! websocket, and the relay itself.
//!
//! The engine's `Replica` knows what to send and what to keep. This module gives it the two
//! things it has not got, the files and the sockets, and it is the same for both ends: a
//! [`Shelf`] is a replica kept as the logs in one directory, the app puts one on the end of a
//! connection to a relay, and a relay puts one on the end of every connection that comes in.
//!
//! The app does this on a thread of its own and tells the session nothing. What arrives is
//! added to the files, and the app finds it there the way it finds what another instance
//! wrote. Only one process at a time syncs a directory, which a lock on a file in it settles:
//! a second instance leaves it to the first, and takes over when the first is gone.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::replica::{Message, Out, Peer, Replica, names_a_log};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::StatusCode;
use tungstenite::{Message as Frame, WebSocket};

/// The port a relay listens on when it is not told another.
pub const PORT: u16 = 3960;
/// The version of the exchange. A connection names it first in its path, then the space.
const VERSION: &str = "1";
/// How long a socket waits for something to read or for room to write. Each end of a
/// connection is one thread that listens and speaks by turns, so neither may wait long.
const TICK: Duration = Duration::from_millis(50);
/// How often the logs are looked at for what has been added to them.
const LOOK: Duration = Duration::from_millis(250);
/// How long a connection has to be made and greeted.
const GREETING: Duration = Duration::from_secs(5);
const PING: Duration = Duration::from_secs(15);
/// A peer not heard from for this long is taken to be gone.
const SILENCE: Duration = Duration::from_secs(45);
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

/// A websocket whose socket never waits long, so that one thread can listen and speak by turns.
struct Wire {
    socket: WebSocket<TcpStream>,
    heard: Instant,
    pinged: Instant,
}

/// Whether an error only says that the socket would have had to wait.
fn would_wait(error: &tungstenite::Error) -> bool {
    matches!(error, tungstenite::Error::Io(e)
        if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut))
}

impl Wire {
    fn new(socket: WebSocket<TcpStream>) -> Result<Self> {
        socket.get_ref().set_read_timeout(Some(TICK))?;
        socket.get_ref().set_write_timeout(Some(TICK))?;
        Ok(Self {
            socket,
            heard: Instant::now(),
            pinged: Instant::now(),
        })
    }

    /// The next message, if one comes within a tick. An error means the connection is over.
    fn hear(&mut self) -> Result<Option<String>> {
        match self.socket.read() {
            Ok(Frame::Text(text)) => {
                self.heard = Instant::now();
                Ok(Some(text.as_str().to_owned()))
            }
            Ok(Frame::Close(_)) => bail!("closed"),
            Ok(_) => {
                self.heard = Instant::now();
                Ok(None)
            }
            Err(e) if would_wait(&e) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Puts a message in line to go. It goes now if there is room, and otherwise as room comes.
    fn say(&mut self, text: String) -> Result<()> {
        match self.socket.write(Frame::text(text)) {
            Err(e) if !would_wait(&e) => Err(e.into()),
            _ => Ok(()),
        }
    }

    /// Sends on what is waiting to go, pings a quiet peer, and gives up on a silent one.
    fn keep_up(&mut self) -> Result<()> {
        if self.heard.elapsed() > SILENCE {
            bail!("nothing heard for {} seconds", SILENCE.as_secs());
        }
        if self.pinged.elapsed() > PING {
            self.pinged = Instant::now();
            if let Err(e) = self.socket.write(Frame::Ping(Default::default()))
                && !would_wait(&e)
            {
                return Err(e.into());
            }
        }
        match self.socket.flush() {
            Err(e) if !would_wait(&e) => Err(e.into()),
            _ => Ok(()),
        }
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

/// A space as a relay holds it: its shelf, and where to put what is to go to each peer.
struct Space {
    shelf: Shelf,
    peers: HashMap<Peer, Sender<String>>,
}

impl Space {
    /// Does what the replica wants done. Says no if the logs are no longer what it holds.
    fn carry_out(&mut self, name: &str, out: Vec<Out>) -> Result<bool> {
        for out in out {
            match out {
                Out::Keep { log, lines } => {
                    if !self.shelf.keep(&log, &lines)? {
                        return Ok(false);
                    }
                }
                Out::Send(peer, message) => {
                    if let Some(to) = self.peers.get(&peer) {
                        let _ = to.send(message.encode()?);
                    }
                }
                Out::Forked { log, .. } => {
                    eprintln!(
                        "fzn relay: in {name}, a device's log {log} is not the {log} held here, \
                         so nothing of it passes to or from that device"
                    );
                }
            }
        }
        Ok(true)
    }
}

/// A relay: a replica of every space under a root that writes nothing of its own. It keeps the
/// same files a device keeps, so the root of a relay can be opened with fzn like any other.
pub struct Relay {
    listener: TcpListener,
    hub: Arc<Hub>,
}

struct Hub {
    root: PathBuf,
    spaces: Mutex<HashMap<String, Space>>,
    peers: AtomicU32,
}

impl Hub {
    fn spaces(&self) -> MutexGuard<'_, HashMap<String, Space>> {
        self.spaces.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Does something with a space that is open, and closes the space if its logs turn out not
    /// to be what its replica holds. Closing it drops every connection to it. Each device
    /// connects again, and the space is opened again from what is on disk.
    fn with(&self, name: &str, act: impl FnOnce(&mut Space) -> Result<bool>) {
        let mut spaces = self.spaces();
        let Some(space) = spaces.get_mut(name) else {
            return;
        };
        match act(space) {
            Ok(true) => {}
            Ok(false) => {
                spaces.remove(name);
            }
            Err(why) => {
                eprintln!("fzn relay: {name}: {why:#}");
                spaces.remove(name);
            }
        }
    }
}

/// What a connection asks for, from the path it asks with: `/1/main` is the space `main`, in
/// version 1 of the exchange.
fn asked_for(path: &str) -> Result<&str, StatusCode> {
    let mut parts = path.trim_start_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some(VERSION), Some(space), None) if names_a_log(space) => Ok(space),
        (Some(version), Some(_), None) if version != VERSION => Err(StatusCode::NOT_IMPLEMENTED),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

impl Relay {
    /// Starts listening. Nothing is served until [`Relay::serve`].
    pub fn bind(root: PathBuf, listen: &str) -> Result<Self> {
        let listener = TcpListener::bind(listen).with_context(|| format!("listen on {listen}"))?;
        Ok(Self {
            listener,
            hub: Arc::new(Hub {
                root,
                spaces: Mutex::new(HashMap::new()),
                peers: AtomicU32::new(1),
            }),
        })
    }

    pub fn address(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Serves until the process ends.
    pub fn serve(self) -> Result<()> {
        // What is written into a space's logs on this machine, by an fzn opened on the relay's
        // own root, is passed on like anything else.
        let hub = Arc::clone(&self.hub);
        thread::spawn(move || {
            loop {
                thread::sleep(LOOK);
                let open: Vec<String> = hub.spaces().keys().cloned().collect();
                for name in open {
                    hub.with(&name, |space| match space.shelf.grown()? {
                        Some(out) => space.carry_out(&name, out),
                        None => Ok(false),
                    });
                }
            }
        });
        for stream in self.listener.incoming() {
            let Ok(stream) = stream else {
                continue;
            };
            let hub = Arc::clone(&self.hub);
            thread::spawn(move || {
                // A connection that ends, however it ends, is only a device that will be back.
                let _ = serve(&hub, stream);
            });
        }
        Ok(())
    }
}

/// One connection to a relay, from its greeting to its end.
// The refusal a greeting can end in is the websocket library's type, and it is a large one.
#[allow(clippy::result_large_err)]
fn serve(hub: &Hub, stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(GREETING))?;
    stream.set_write_timeout(Some(GREETING))?;
    let mut name = String::new();
    let socket =
        tungstenite::accept_hdr(
            stream,
            |request: &Request, response: Response| match asked_for(request.uri().path()) {
                Ok(space) => {
                    name = space.to_owned();
                    Ok(response)
                }
                Err(status) => {
                    let mut refusal = ErrorResponse::new(None);
                    *refusal.status_mut() = status;
                    Err(refusal)
                }
            },
        )
        .map_err(|e| anyhow!("{e}"))?;
    let mut wire = Wire::new(socket)?;

    let peer = hub.peers.fetch_add(1, Ordering::Relaxed);
    let (to, waiting) = mpsc::channel();
    {
        let mut spaces = hub.spaces();
        if !spaces.contains_key(&name) {
            // A space a relay has not seen before starts here, with nothing in it.
            let dir = hub.root.join(&name);
            fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
            let Some(shelf) = Shelf::open(&dir)? else {
                bail!("{name} is being synced by another process");
            };
            spaces.insert(
                name.clone(),
                Space {
                    shelf,
                    peers: HashMap::new(),
                },
            );
        }
    }
    hub.with(&name, |space| {
        space.peers.insert(peer, to);
        let out = space.shelf.replica.connect(peer);
        space.carry_out(&name, out)
    });

    let ended = (|| -> Result<()> {
        loop {
            if let Some(text) = wire.hear()?
                && let Some(message) = Message::decode(&text)
            {
                hub.with(&name, |space| {
                    let out = space.shelf.replica.receive(peer, message);
                    space.carry_out(&name, out)
                });
            }
            loop {
                match waiting.try_recv() {
                    Ok(text) => wire.say(text)?,
                    Err(TryRecvError::Empty) => break,
                    // The space was closed under this connection.
                    Err(TryRecvError::Disconnected) => bail!("{name} was closed"),
                }
            }
            wire.keep_up()?;
        }
    })();
    hub.with(&name, |space| {
        if space.peers.remove(&peer).is_some() {
            space.shelf.replica.disconnect(peer);
        }
        Ok(true)
    });
    ended
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::random_u64;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fzn-sync-test-{:x}", random_u64()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn lines(texts: &[&str]) -> Vec<String> {
        texts.iter().map(|&t| t.to_owned()).collect()
    }

    /// Adds to a log the way the app does: whole lines, each ending in a newline.
    fn write(dir: &Path, log: &str, text: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(format!("{log}.jsonl")))
            .unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    fn read(dir: &Path, log: &str) -> String {
        fs::read_to_string(dir.join(format!("{log}.jsonl"))).unwrap_or_default()
    }

    /// Waits for something to become so, and says whether it did.
    fn eventually(mut so: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if so() {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
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
    fn a_relay_is_given_as_a_host_and_asked_for_a_space() {
        assert_eq!(authority("ws://nas.local:4000").unwrap(), "nas.local:4000");
        assert_eq!(authority(" nas.local ").unwrap(), "nas.local:3960");
        assert_eq!(authority("ws://10.0.0.2/").unwrap(), "10.0.0.2:3960");
        assert_eq!(authority("ws://[::1]").unwrap(), "[::1]:3960");
        assert_eq!(authority("[::1]:4000").unwrap(), "[::1]:4000");
        assert!(authority("wss://nas.local").is_err());
        assert!(authority("http://nas.local").is_err());
        assert!(authority("").is_err());

        assert_eq!(asked_for("/1/main"), Ok("main"));
        assert_eq!(asked_for("/1/fzn-2022"), Ok("fzn-2022"));
        assert_eq!(asked_for("/2/main"), Err(StatusCode::NOT_IMPLEMENTED));
        for path in ["/", "/main", "/1/", "/1/main/more", "/1/..", "/1/a b"] {
            assert_eq!(asked_for(path), Err(StatusCode::NOT_FOUND), "{path}");
        }
    }

    /// A relay on a port of its own choosing, serving a root, and where to find it.
    fn relay(root: &Path) -> String {
        let relay = Relay::bind(root.to_owned(), "127.0.0.1:0").unwrap();
        let address = relay.address().unwrap().to_string();
        thread::spawn(move || relay.serve());
        address
    }

    #[test]
    fn two_devices_and_a_relay_come_to_hold_the_same_logs() {
        let root = temp_dir();
        let (a, b, hub) = (root.join("a"), root.join("b"), root.join("relay"));
        fs::create_dir_all(&a).unwrap();
        write(&a, "dev-a", "a1\na2\n");
        fs::create_dir_all(&b).unwrap();
        write(&b, "dev-b", "b1\n");
        fs::create_dir_all(&hub).unwrap();
        let address = relay(&hub);

        let news_a = start(a.clone(), format!("ws://{address}"), "main".into());
        assert_eq!(
            news_a.recv_timeout(Duration::from_secs(10)),
            Ok(News::Connected)
        );
        assert!(eventually(|| read(&hub.join("main"), "dev-a") == "a1\na2\n"));

        // A device that comes later gets what the relay was given before it came, and what
        // either writes from then on reaches the other.
        let news_b = start(b.clone(), address.clone(), "main".into());
        assert_eq!(
            news_b.recv_timeout(Duration::from_secs(10)),
            Ok(News::Connected)
        );
        assert!(eventually(|| read(&b, "dev-a") == "a1\na2\n"));
        assert!(eventually(|| read(&a, "dev-b") == "b1\n"));
        write(&a, "dev-a", "a3\n");
        write(&b, "dev-b", "b2\nb3\n");
        assert!(eventually(|| read(&b, "dev-a") == "a1\na2\na3\n"));
        assert!(eventually(|| read(&a, "dev-b") == "b1\nb2\nb3\n"));
        assert_eq!(read(&hub.join("main"), "dev-b"), "b1\nb2\nb3\n");

        // A log dropped into the relay's own directory goes out too.
        write(&hub.join("main"), "dev-c", "c1\n");
        assert!(eventually(
            || read(&a, "dev-c") == "c1\n" && read(&b, "dev-c") == "c1\n"
        ));

        // A second syncer on a directory leaves it to the first.
        let second = start(a.clone(), address.clone(), "main".into());
        assert_eq!(
            second.recv_timeout(Duration::from_secs(10)),
            Ok(News::Elsewhere)
        );

        // Another space on the same relay is another set of logs.
        let other = root.join("other");
        fs::create_dir_all(&other).unwrap();
        write(&other, "dev-a", "elsewhere\n");
        let news = start(other.clone(), address, "other".into());
        assert_eq!(
            news.recv_timeout(Duration::from_secs(10)),
            Ok(News::Connected)
        );
        assert!(eventually(
            || read(&hub.join("other"), "dev-a") == "elsewhere\n"
        ));
        thread::sleep(LOOK * 2);
        assert_eq!(read(&other, "dev-b"), "", "and nothing of main comes to it");
        assert!(news_a.try_recv().is_err() && news_b.try_recv().is_err());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_fork_and_a_relay_that_is_not_there_are_said() {
        let root = temp_dir();
        let (a, b, hub) = (root.join("a"), root.join("b"), root.join("relay"));
        fs::create_dir_all(&a).unwrap();
        write(&a, "dev-a", "a1\na2\na3\n");
        // The same device id on a second machine, with a log of its own under it.
        fs::create_dir_all(&b).unwrap();
        write(&b, "dev-a", "a1\nx2\n");
        write(&b, "dev-b", "b1\n");
        let address = relay(&hub);

        let news_a = start(a.clone(), address.clone(), "main".into());
        assert_eq!(
            news_a.recv_timeout(Duration::from_secs(10)),
            Ok(News::Connected)
        );
        assert!(eventually(
            || read(&hub.join("main"), "dev-a") == "a1\na2\na3\n"
        ));
        let news_b = start(b.clone(), address, "main".into());
        assert_eq!(
            news_b.recv_timeout(Duration::from_secs(10)),
            Ok(News::Connected)
        );
        assert_eq!(
            news_b.recv_timeout(Duration::from_secs(10)),
            Ok(News::Forked("dev-a".into()))
        );
        assert!(
            eventually(|| read(&a, "dev-b") == "b1\n"),
            "other logs go on"
        );
        assert_eq!(read(&b, "dev-a"), "a1\nx2\n", "and neither log is touched");
        assert_eq!(read(&a, "dev-a"), "a1\na2\na3\n");

        // Nobody listening: it is said once, and tried again.
        let nowhere = root.join("nowhere");
        fs::create_dir_all(&nowhere).unwrap();
        let free = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let news = start(nowhere, free.to_string(), "main".into());
        assert!(matches!(
            news.recv_timeout(Duration::from_secs(10)),
            Ok(News::Unreachable(_))
        ));
        assert!(
            matches!(news_a.try_recv(), Err(TryRecvError::Empty)),
            "the device whose log the relay has is told nothing"
        );
        fs::remove_dir_all(&root).unwrap();
    }
}
