//! The relay: a replica of every space under a root that writes nothing of its own, and the
//! web app's page, which it carries.
//!
//! A relay is the only writer of its directories while it runs. Each space is locked when the
//! first connection asks for it, read whole once, and from then on its files are only ever
//! what the replica asked to be kept, so nothing is reread and nothing is locked per write.
//! Anything else that wants to write there, an fzn opened on the relay's root or a second relay,
//! is kept out by the lock, and a connection asking a relay for a space held elsewhere is
//! refused. If writing a space fails, the space is closed and its connections dropped: each
//! device connects again and the space is opened again from what is on disk.
//!
//! A space is entered with its key and made with a token. The relay keeps, beside each space's
//! logs, an auth key derived one way from the space's key, which lets it check a device and
//! read nothing, and it challenges every connection before it opens the space or says anything
//! of it; the first device to answer for a space it keeps no auth key for gives it that key.
//! Until a connection is let in, nothing is opened, made or written for it, so a peer that can
//! reach the relay and has no key costs a thread for at most three greetings' time, about
//! fifteen seconds, and nothing on disk; and no more than a few dozen connections are in their
//! greeting at once, a handful of them from any one address, so that one peer cannot keep the
//! rest out. A space's key is kept only with the token of the name it is under (see
//! `names`), which is when the space is made, and everything that can be refused before the
//! challenge is, so that a device can tell a refusal of its key from any other.
//!
//! A relay also hands out the web app, so that a browser has one address to open: the page
//! comes from the relay and connects back to it. The page is built into fzn, so it and the
//! policy it is served under are always of one release; `--web` points the relay at a directory
//! instead, read again for every request, which is how the page is worked on. Every answer names
//! the page's build, and `index.html` carries it, so a tab can tell when the relay has a page
//! newer than the one it runs. A built-in file is sent with a tag the browser can ask again by,
//! answered `304` with nothing, and gzipped when the browser takes it.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{self, Sender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use fzn_engine::replica::{Message, Out, Peer, Replica, names_a_log};
use fzn_engine::seal::is_sealed;
use fzn_engine::{Host, Key, Opened, Seal};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tungstenite::http::{HeaderValue, StatusCode};
use tungstenite::protocol::WebSocketConfig;

use crate::names;
use crate::system;
use crate::web_files;
use crate::wire::{GREETING, MESSAGE_MAX, Stream, VERSION, Wire};

/// The file whose lock says which process writes a space's directory.
const LOCK_FILE: &str = ".lock";
/// The file a space's auth key is kept in, once a device has claimed the space.
const AUTH_FILE: &str = ".auth";
/// How many sealed lines of a space with no key a challenge offers at most, one a log.
const LINES_OFFERED: usize = 16;
/// How many connections may be in their greeting at once. Past it a new one is closed at once,
/// so that a peer cannot hold every thread the machine has, nor fill the log with refusals.
const GREETINGS_MAX: usize = 64;
/// How many of those places one address may hold, so that one peer opening bare connections over
/// and over cannot keep everyone else out.
const GREETINGS_FROM_ONE: usize = 8;
/// The subprotocol a device offers, which the relay says back.
const PROTOCOL: &str = "fzn";
/// How a token is offered: as a subprotocol, the one place a browser can put a credential in its
/// greeting, and one that a proxy in front of the relay does not write into its log as it would
/// a query.
const TOKEN_PROTOCOL: &str = "fzn.token.";
/// What a page of the web app may do: load its own files and its own WebAssembly, talk to the
/// relay it came from, and use inline styles, because the body's editor injects its stylesheet.
/// No script from anywhere else; and the page writes nothing into itself as markup, which is its
/// own practice, since allowing inline styles means the policy no longer holds it to that.
const PAGE_POLICY: &str = "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; \
    style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; \
    base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

/// A space as a relay holds it: its replica, its directory, which nothing else writes while
/// the lock is held, and where to put what is to go to each peer.
struct Space {
    name: String,
    dir: PathBuf,
    replica: Replica,
    peers: HashMap<Peer, Sender<String>>,
    _lock: File,
}

impl Space {
    /// Locks a space's directory, making it if it is new, and takes in every log in it. Gives
    /// nothing if another process holds the lock.
    fn open(name: &str, dir: &Path) -> Result<Option<Self>> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
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
        let mut space = Self {
            name: name.to_owned(),
            dir: dir.to_owned(),
            replica: Replica::new(),
            peers: HashMap::new(),
            _lock: lock,
        };
        let mut logs: Vec<(PathBuf, String)> = fs::read_dir(dir)
            .with_context(|| format!("read {}", dir.display()))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .filter_map(|p| {
                let stem = p.file_stem()?.to_str()?.to_owned();
                names_a_log(&stem).then_some((p, stem))
            })
            .collect();
        logs.sort();
        for (path, log) in logs {
            let lines = read_log(&path)?;
            // A log retired by one read before it is a file left from before, and goes now.
            let out = space.replica.append(&log, lines);
            space.carry_out(out)?;
        }
        Ok(Some(space))
    }

    fn path(&self, log: &str) -> PathBuf {
        self.dir.join(format!("{log}.jsonl"))
    }

    /// Does what the replica wants done.
    fn carry_out(&mut self, out: Vec<Out>) -> Result<()> {
        let name = &self.name;
        for out in out {
            match out {
                Out::Keep { log, lines } => {
                    let path = self.path(&log);
                    let mut text = String::new();
                    for line in &lines {
                        text.push_str(line);
                        text.push('\n');
                    }
                    let mut file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .with_context(|| format!("open {}", path.display()))?;
                    file.write_all(text.as_bytes())
                        .with_context(|| format!("append to {}", path.display()))?;
                    file.flush()?;
                }
                Out::Send(peer, message) => {
                    if let Some(to) = self.peers.get(&peer) {
                        let _ = to.send(message.encode()?);
                    }
                }
                Out::Retire(log) => {
                    let path = self.path(&log);
                    match fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == ErrorKind::NotFound => {}
                        Err(e) => {
                            return Err(e).with_context(|| format!("delete {}", path.display()));
                        }
                    }
                    eprintln!(
                        "fzn relay: in {name}, the log {log} is retired by a compaction, and \
                         its file is deleted"
                    );
                }
                Out::Superseded(log) => {
                    eprintln!(
                        "fzn relay: in {name}, the log {log} was taken to be the relay's own and \
                         superseded, which cannot happen to a relay"
                    );
                }
                Out::Forked { log, .. } => {
                    eprintln!(
                        "fzn relay: in {name}, a device's log {log} is not the {log} held here, \
                         so nothing of it passes to or from that device"
                    );
                }
            }
        }
        Ok(())
    }
}

/// The lines of a log file, without their newlines. A last line with no newline is what a
/// crash of this relay left in the middle of a write: it was never a line, so it is cut from
/// the file before anything is added after it.
fn read_log(path: &Path) -> Result<Vec<String>> {
    let mut bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.last().is_some_and(|&b| b != b'\n') {
        let keep = bytes
            .iter()
            .rposition(|&b| b == b'\n')
            .map_or(0, |end| end + 1);
        OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|file| file.set_len(keep as u64))
            .with_context(|| format!("cut the unfinished line from {}", path.display()))?;
        bytes.truncate(keep);
    }
    let Some((_, body)) = bytes.split_last() else {
        return Ok(Vec::new());
    };
    Ok(body
        .split(|&b| b == b'\n')
        .map(|line| String::from_utf8_lossy(line).into_owned())
        .collect())
}

/// A relay: a replica of every space under a root that writes nothing of its own. It keeps the
/// same files a device keeps, and while it runs it is the only thing that writes them.
pub struct Relay {
    listener: TcpListener,
    hub: Arc<Hub>,
}

/// The page as fzn was built with it, from the table the build script wrote.
mod built {
    /// A file's path, its bytes, its gzip twin when that is smaller, and its tag.
    pub type File = (
        &'static str,
        &'static [u8],
        Option<&'static [u8]>,
        &'static str,
    );
    include!(concat!(env!("OUT_DIR"), "/page.rs"));
}

/// A file of the page as the relay serves it: its bytes, a gzip twin when that is smaller, and
/// the tag a browser holds it by.
struct Served {
    body: Cow<'static, [u8]>,
    gzip: Option<Cow<'static, [u8]>>,
    etag: String,
}

/// A page the relay carries, ready to serve, by path, with the build its files make.
struct Page {
    files: HashMap<String, Served>,
    build: String,
}

impl Page {
    /// The page built into fzn, or nothing if it was built without one.
    fn built_in() -> Option<Self> {
        if built::FILES.is_empty() {
            return None;
        }
        let files = built::FILES
            .iter()
            .map(|&(path, body, gzip, etag)| {
                let served = Served {
                    body: Cow::Borrowed(body),
                    gzip: gzip.map(Cow::Borrowed),
                    etag: etag.to_owned(),
                };
                (path.to_owned(), served)
            })
            .collect();
        Some(Self {
            files,
            build: built::BUILD.to_owned(),
        })
    }

    /// A page from files in memory, as the build script would make it but with no gzip twins.
    #[cfg(test)]
    fn from_files(files: Vec<(&str, Vec<u8>)>) -> Self {
        let mut files = files;
        files.sort();
        let build = web_files::build_of(files.iter().map(|(p, b)| (*p, b.as_slice())));
        let files = files
            .into_iter()
            .map(|(path, body)| {
                let body = if path == "index.html" {
                    web_files::with_build(&body, &build)
                } else {
                    body
                };
                let served = Served {
                    etag: web_files::etag_of(&body),
                    body: Cow::Owned(body),
                    gzip: None,
                };
                (path.to_owned(), served)
            })
            .collect();
        Self { files, build }
    }
}

struct Hub {
    root: PathBuf,
    /// fzn's config directory, where the relay finds who may make spaces on it.
    config: PathBuf,
    /// Where the page's files are read from, when a directory was given with --web.
    web: Option<PathBuf>,
    /// The page served when no directory was given.
    page: Option<Page>,
    /// For a directory, its build as last worked out and when, worked out again at most once a
    /// second, so that a page being worked on is named anew within a second of a change.
    dir_build: Mutex<Option<(Instant, String)>>,
    /// The spaces open, by the name the relay knows each by: `main` at the root, `joe/main`
    /// under a name.
    spaces: Mutex<HashMap<String, Space>>,
    /// Taken while a space's auth key is kept, so that two devices claiming one space at once
    /// cannot both be let in under different keys.
    claiming: Mutex<()>,
    /// The connections taken and not yet let in or let go, in all and from each address.
    greeting: Mutex<Greetings>,
    /// When the relay last said it was closing connections at once, so that it says so at most
    /// once a minute however many come.
    crowded: Mutex<Option<Instant>>,
    peers: AtomicU32,
}

impl Hub {
    fn spaces(&self) -> MutexGuard<'_, HashMap<String, Space>> {
        self.spaces.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Opens a space if it is not open, or says why it cannot be, which is the relay's trouble
    /// and not the device's, and so is not said to it.
    fn open(&self, label: &str, dir: &Path) -> Result<(), Refusal> {
        let mut spaces = self.spaces();
        if spaces.contains_key(label) {
            return Ok(());
        }
        match Space::open(label, dir) {
            Ok(Some(space)) => {
                spaces.insert(label.to_owned(), space);
                Ok(())
            }
            Ok(None) => Err(Refusal::fault("the space is held by another process")),
            Err(why) => Err(Refusal::fault("the space could not be opened").with(why)),
        }
    }

    /// Does something with a space that is open, and closes the space if it fails. Closing it
    /// drops every connection to it. Each device connects again, and the space is opened again
    /// from what is on disk.
    fn with(&self, name: &str, act: impl FnOnce(&mut Space) -> Result<()>) {
        let mut spaces = self.spaces();
        let Some(space) = spaces.get_mut(name) else {
            return;
        };
        if let Err(why) = act(space) {
            eprintln!("fzn relay: {name}: {why:#}");
            spaces.remove(name);
        }
    }

    /// Sealed lines of a space, for a device to open one of before it claims the space as its
    /// key's: the first of each log, up to [`LINES_OFFERED`], from the replica if the space is
    /// open, else from its logs on disk, each read only as far as its first sealed line. One a
    /// log, so that a log under another key, a passphrase once mistyped and written with, does
    /// not shut the space to the key that opens the rest.
    fn sealed_lines(&self, label: &str, dir: &Path) -> Vec<String> {
        if let Some(space) = self.spaces().get(label) {
            return space
                .replica
                .have()
                .keys()
                .filter_map(|log| {
                    let lines = space.replica.lines(log);
                    lines.iter().find(|line| is_sealed(line)).cloned()
                })
                .take(LINES_OFFERED)
                .collect();
        }
        let Ok(entries) = fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut logs: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        logs.sort();
        logs.into_iter()
            .filter_map(|path| {
                let file = File::open(path).ok()?;
                BufReader::new(file)
                    .lines()
                    .map_while(Result::ok)
                    .find(|line| is_sealed(line))
            })
            .take(LINES_OFFERED)
            .collect()
    }

    /// Keeps the auth key a space is first claimed with, readable only by the relay, and says
    /// whether this claim was the first. A space claimed already, even a moment ago by another
    /// device, is let in only under the key it was claimed with. The key is written whole to a
    /// file of its own and then linked into place, which fails if a key is there already, so
    /// `.auth` is either missing or whole, whoever reads it and whenever the relay stops.
    fn claim(&self, dir: &Path, key: &Key) -> Result<bool, Refusal> {
        let _claiming = self.claiming.lock().unwrap_or_else(PoisonError::into_inner);
        let cannot = |why: std::io::Error| {
            Refusal::fault("the space's key could not be kept").with(why.into())
        };
        fs::create_dir_all(dir).map_err(cannot)?;
        let mut random = [0u8; 8];
        system::System.random(&mut random);
        let fresh = dir.join(format!(
            "{AUTH_FILE}.{}",
            random
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let written = options.open(&fresh).and_then(|mut file| {
            writeln!(file, "{}", key.encode())?;
            file.sync_all()
        });
        if let Err(why) = written {
            let _ = fs::remove_file(&fresh);
            return Err(cannot(why));
        }
        let linked = fs::hard_link(&fresh, dir.join(AUTH_FILE));
        let _ = fs::remove_file(&fresh);
        match linked {
            Ok(()) => {
                // The link is kept only once the directory that holds it is.
                File::open(dir).and_then(|d| d.sync_all()).map_err(cannot)?;
                Ok(true)
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => match kept_auth(dir) {
                Ok(Some(kept))
                    if names::same(kept.encode().as_bytes(), key.encode().as_bytes()) =>
                {
                    Ok(false)
                }
                Ok(_) => Err(Refusal::wrong_key()),
                Err(why) => Err(Refusal::fault("the space's key could not be read").with(why)),
            },
            Err(e) => Err(cannot(e)),
        }
    }

    fn greetings(&self) -> MutexGuard<'_, Greetings> {
        self.greeting.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes a connection from an address into its greeting, unless too many are in theirs
    /// already, in all or from that address, which is said at most once a minute.
    fn take(self: &Arc<Self>, from: IpAddr) -> Option<Greeting> {
        let mut greetings = self.greetings();
        let held = greetings.from.get(&from).copied().unwrap_or(0);
        if greetings.all >= GREETINGS_MAX || held >= GREETINGS_FROM_ONE {
            drop(greetings);
            let mut crowded = self.crowded.lock().unwrap_or_else(PoisonError::into_inner);
            if crowded.is_none_or(|when| when.elapsed() >= Duration::from_secs(60)) {
                *crowded = Some(Instant::now());
                eprintln!(
                    "fzn relay: too many connections are in their greeting, {GREETINGS_MAX} in \
                     all or {GREETINGS_FROM_ONE} from one address, so new ones are closed at once"
                );
            }
            return None;
        }
        greetings.all += 1;
        greetings.from.insert(from, held + 1);
        Some(Greeting {
            hub: Arc::clone(self),
            from,
        })
    }
}

/// How many connections are in their greeting, in all and from each address.
#[derive(Default)]
struct Greetings {
    all: usize,
    from: HashMap<IpAddr, usize>,
}

/// A connection's place among those in their greeting, given up however it ends: let in, let
/// go, or its thread never started.
struct Greeting {
    hub: Arc<Hub>,
    from: IpAddr,
}

impl Drop for Greeting {
    fn drop(&mut self) {
        let mut greetings = self.hub.greetings();
        greetings.all = greetings.all.saturating_sub(1);
        if let Some(held) = greetings.from.get_mut(&self.from) {
            *held -= 1;
            if *held == 0 {
                greetings.from.remove(&self.from);
            }
        }
    }
}

/// The auth key kept for a space, or nothing if none has been kept yet.
fn kept_auth(dir: &Path) -> Result<Option<Key>> {
    let path = dir.join(AUTH_FILE);
    match fs::read_to_string(&path) {
        Ok(text) => Key::decode(&text)
            .map(Some)
            .with_context(|| format!("{} holds no key", path.display())),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

/// Why a connection was let go: what the device is told, if anything, and what only the relay's
/// log hears, which never holds a token, a nonce or a key. A device is told why it is refused
/// only of its name, its token or its key; the relay's own trouble is logged and the connection
/// closed, so that a device tries again and does not doubt a passphrase that is right.
struct Refusal {
    said: Option<String>,
    logged: Option<String>,
}

impl Refusal {
    fn said(why: String) -> Self {
        Self {
            said: Some(why),
            logged: None,
        }
    }

    /// The device went, or the relay is at fault: nobody is told.
    fn fault(why: &str) -> Self {
        Self {
            said: None,
            logged: Some(why.to_owned()),
        }
    }

    /// With what only the log is told.
    fn with(mut self, why: anyhow::Error) -> Self {
        self.logged = Some(match self.logged.take() {
            Some(logged) => format!("{logged}: {why:#}"),
            None => format!("{why:#}"),
        });
        self
    }

    fn wrong_key() -> Self {
        Self::said("the passphrase does not open this space on the relay".to_owned())
    }

    /// The line the relay's log holds for this refusal of a connection from `from` for `label`.
    fn logged(&self, from: &str, label: &str) -> String {
        let why = match (&self.said, &self.logged) {
            (Some(said), Some(logged)) => format!("{said} ({logged})"),
            (Some(said), None) => said.clone(),
            (None, Some(logged)) => logged.clone(),
            (None, None) => "let go".to_owned(),
        };
        format!("fzn relay: {from} {label}: {why}")
    }
}

/// What a connection asks for, from the path it asks with, in version 1 of the exchange:
/// `/1/main` is the space `main` at the relay's root, and `/1/joe/main` is joe's `main`.
fn asked_for(path: &str) -> Result<(Option<&str>, &str), StatusCode> {
    let mut parts = path.trim_start_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(VERSION), Some(space), None, _) if names_a_log(space) => Ok((None, space)),
        (Some(VERSION), Some(name), Some(space), None)
            if names_a_log(name) && names_a_log(space) =>
        {
            Ok((Some(name), space))
        }
        (Some(version), Some(_), _, _) if version != VERSION => Err(StatusCode::NOT_IMPLEMENTED),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

/// What a connection asked for when it was greeted.
struct Asked {
    /// The name the space is under, or none for a space at the relay's root.
    name: Option<String>,
    space: String,
    /// The token offered among the subprotocols, if one was.
    token: Option<String>,
}

impl Asked {
    /// The space as the relay's log and its open spaces name it: `main`, or `joe/main`.
    fn label(&self) -> String {
        match &self.name {
            Some(name) => format!("{name}/{}", self.space),
            None => self.space.clone(),
        }
    }
}

/// The subprotocols a request offers, from every header that names them.
fn offered(request: &Request) -> Vec<String> {
    request
        .headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|protocol| protocol.trim().to_owned())
        .filter(|protocol| !protocol.is_empty())
        .collect()
}

impl Relay {
    /// Starts listening. Nothing is served until [`Relay::serve`]. The relay hands out the page
    /// built into fzn, or with a directory for `web`, the files in it, and finds who may make
    /// spaces on it under `config`, fzn's config directory.
    pub fn bind(
        root: PathBuf,
        config: PathBuf,
        listen: &str,
        web: Option<PathBuf>,
    ) -> Result<Self> {
        Self::bind_with(root, config, listen, web, Page::built_in())
    }

    fn bind_with(
        root: PathBuf,
        config: PathBuf,
        listen: &str,
        web: Option<PathBuf>,
        page: Option<Page>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(listen).with_context(|| format!("listen on {listen}"))?;
        Ok(Self {
            listener,
            hub: Arc::new(Hub {
                root,
                config,
                web,
                page,
                dir_build: Mutex::new(None),
                spaces: Mutex::new(HashMap::new()),
                claiming: Mutex::new(()),
                greeting: Mutex::new(Greetings::default()),
                crowded: Mutex::new(None),
                peers: AtomicU32::new(1),
            }),
        })
    }

    pub fn address(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// The build of the page this relay hands out, or nothing if it has none to hand out.
    pub fn build(&self) -> Option<String> {
        self.hub.build()
    }

    /// Serves until the process ends.
    pub fn serve(self) -> Result<()> {
        for stream in self.listener.incoming() {
            let Ok(stream) = stream else {
                continue;
            };
            let Some(greeting) = stream.peer_addr().ok().and_then(|a| self.hub.take(a.ip())) else {
                continue;
            };
            let hub = Arc::clone(&self.hub);
            // A machine out of threads drops the connection, which the device makes again.
            let _ = thread::Builder::new().spawn(move || serve(&hub, stream, greeting));
        }
        Ok(())
    }
}

/// The head of the request a connection opens with, looked at without taking it off the socket,
/// so that a request for a websocket can still be handed whole to the websocket library.
fn opening(stream: &TcpStream) -> Result<Vec<u8>> {
    let mut seen = vec![0u8; 16 * 1024];
    let deadline = Instant::now() + GREETING;
    loop {
        let so_far = stream.peek(&mut seen)?;
        if let Some(end) = seen[..so_far].windows(4).position(|w| w == b"\r\n\r\n") {
            seen.truncate(end + 4);
            return Ok(seen);
        }
        if so_far == 0 || so_far == seen.len() || Instant::now() > deadline {
            bail!("not a request");
        }
        thread::sleep(Duration::from_millis(5));
    }
}

/// A header of a request, by its name in any case, with its value trimmed.
fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// Whether a request asks for a websocket, and not for a page.
fn asks_for_a_socket(head: &str) -> bool {
    header(head, "upgrade").is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

/// The file of the web app that a request names, as a path under the page, if it names one
/// plainly: every part of its path a plain name, as a log's is, so that nothing outside the
/// directory can be asked for.
fn file_asked_for(target: &str) -> Option<String> {
    let path = target.split(['?', '#']).next()?.strip_prefix('/')?;
    if path.is_empty() {
        return Some("index.html".to_owned());
    }
    path.split('/').all(names_a_log).then(|| path.to_owned())
}

/// What kind of thing a file is, for the browser, going by its name.
fn kind_of(file: &str) -> &'static str {
    match Path::new(file).extension().and_then(|x| x.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("wasm") => "application/wasm",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("json" | "webmanifest") => "application/json",
        _ => "application/octet-stream",
    }
}

/// Whether a file is named by its contents, as the bundles' shared chunks are, so that a browser
/// may keep it without asking again: a change to it is a new name.
fn named_by_its_contents(path: &str) -> bool {
    path.starts_with("vendor/chunk-") && path.ends_with(".js")
}

/// Whether a browser already holds the file it asks for, under this tag.
fn still_held(head: &str, etag: &str) -> bool {
    header(head, "if-none-match").is_some_and(|tags| {
        tags.split(',').any(|tag| {
            let tag = tag.trim();
            tag == "*" || tag.trim_start_matches("W/").trim_matches('"') == etag
        })
    })
}

/// Whether a browser takes a gzipped answer: gzip named, and not with a weight of nothing.
fn takes_gzip(head: &str) -> bool {
    header(head, "accept-encoding").is_some_and(|codings| {
        codings.split(',').any(|coding| {
            let mut parts = coding.split(';');
            parts
                .next()
                .is_some_and(|c| c.trim().eq_ignore_ascii_case("gzip"))
                && !parts.any(|p| {
                    p.trim()
                        .strip_prefix("q=")
                        .and_then(|q| q.parse::<f32>().ok())
                        .is_some_and(|q| q == 0.0)
                })
        })
    })
}

/// An answer to a request for a page, before it is written. No body is a `304`.
struct Answer {
    status: &'static str,
    kind: &'static str,
    body: Option<Cow<'static, [u8]>>,
    /// The header lines particular to this answer, each ending in a line break.
    headers: String,
}

impl Answer {
    fn plain(status: &'static str, body: &'static str, headers: String) -> Self {
        Self {
            status,
            kind: "text/plain; charset=utf-8",
            body: Some(Cow::Borrowed(body.as_bytes())),
            headers: headers + "Cache-Control: no-cache\r\n",
        }
    }
}

impl Hub {
    /// The build of the page this relay hands out, if it hands one out.
    fn build(&self) -> Option<String> {
        match (&self.web, &self.page) {
            (Some(web), _) => Some(self.dir_build(web)),
            (None, Some(page)) => Some(page.build.clone()),
            (None, None) => None,
        }
    }

    /// The build of the page in a directory, worked out over its listed files as the build
    /// script works it out, so that the same files are the same build either way.
    fn dir_build(&self, web: &Path) -> String {
        let mut held = self
            .dir_build
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some((when, build)) = held.as_ref()
            && when.elapsed() < Duration::from_secs(1)
        {
            return build.clone();
        }
        let files: Vec<(String, Vec<u8>)> = web_files::listed(web)
            .into_iter()
            .filter_map(|path| fs::read(web.join(&path)).ok().map(|bytes| (path, bytes)))
            .collect();
        let build = web_files::build_of(files.iter().map(|(p, b)| (p.as_str(), b.as_slice())));
        *held = Some((Instant::now(), build.clone()));
        build
    }

    /// What a request for a page is answered with: from the directory if one was given, else
    /// from the page built in.
    fn answer(&self, target: &str, head: &str) -> Answer {
        let build = self.build();
        let mut headers = String::new();
        if let Some(build) = &build {
            headers.push_str(&format!("Fzn-Build: {build}\r\n"));
        }
        let missing = |headers| Answer::plain("404 Not Found", "nothing here\n", headers);
        let Some(path) = file_asked_for(target) else {
            return missing(headers);
        };
        if let Some(web) = &self.web {
            // Only what would be built in, so that a file of a kind the page's list lacks is
            // missed while the page is worked on, not once it is built in.
            if !web_files::is_listed(&path) {
                return missing(headers);
            }
            let Ok(body) = fs::read(web.join(&path)) else {
                return missing(headers);
            };
            let body = match &build {
                Some(build) if path == "index.html" => web_files::with_build(&body, build),
                _ => body,
            };
            return Answer {
                status: "200 OK",
                kind: kind_of(&path),
                body: Some(Cow::Owned(body)),
                headers: headers + "Cache-Control: no-cache\r\n",
            };
        }
        let Some(served) = self.page.as_ref().and_then(|page| page.files.get(&path)) else {
            return missing(headers);
        };
        let cache = if named_by_its_contents(&path) {
            "max-age=31536000, immutable"
        } else {
            "no-cache"
        };
        headers.push_str(&format!(
            "ETag: \"{}\"\r\nCache-Control: {cache}\r\nVary: Accept-Encoding\r\n",
            served.etag
        ));
        let body = if still_held(head, &served.etag) {
            None
        } else if let Some(gzip) = served.gzip.as_ref().filter(|_| takes_gzip(head)) {
            headers.push_str("Content-Encoding: gzip\r\n");
            Some(gzip.clone())
        } else {
            Some(served.body.clone())
        };
        Answer {
            status: if body.is_some() {
                "200 OK"
            } else {
                "304 Not Modified"
            },
            kind: kind_of(&path),
            body,
            headers,
        }
    }
}

/// Answers a request for a page of the web app, and closes.
fn page(hub: &Hub, mut stream: TcpStream, head: &[u8]) -> Result<()> {
    // The request is taken off the socket before it is answered, or closing with it unread
    // would cut the answer short.
    let mut taken = vec![0u8; head.len()];
    stream.read_exact(&mut taken)?;
    let head = String::from_utf8_lossy(head);
    let mut request = head.lines().next().unwrap_or_default().split(' ');
    let method = request.next().unwrap_or_default();
    let target = request.next().unwrap_or_default();
    let Answer {
        status,
        kind,
        body,
        headers,
    } = match method {
        "GET" | "HEAD" => hub.answer(target, &head),
        _ => Answer::plain(
            "405 Method Not Allowed",
            "only GET and HEAD\n",
            String::new(),
        ),
    };
    let length = body
        .as_ref()
        .map(|body| format!("Content-Length: {}\r\n", body.len()))
        .unwrap_or_default();
    let said = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\n{length}{headers}\
         X-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n\
         Content-Security-Policy: {PAGE_POLICY}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(said.as_bytes())?;
    // A HEAD is answered as a GET would be, without the body.
    if let Some(body) = body.filter(|_| method != "HEAD") {
        stream.write_all(&body)?;
    }
    stream.flush()?;
    Ok(())
}

/// One connection to a relay, from its greeting to its end. A request for a page is answered;
/// a request for a space is let in by [`enter`], or let go, and every refusal is logged. A
/// connection that ends once it is in is only a device that will be back, and is not. Each step
/// of the greeting has a greeting's time, so a peer that says nothing is let go within one, and
/// one that is greeted and answers nothing within three.
// The greeting's callback refuses with the websocket library's own type, a large one.
#[allow(clippy::result_large_err)]
fn serve(hub: &Hub, stream: TcpStream, greeting: Greeting) {
    let from = stream
        .peer_addr()
        .map_or_else(|_| "somewhere".to_owned(), |a| a.to_string());
    let timed = stream
        .set_nodelay(true)
        .and_then(|()| stream.set_read_timeout(Some(GREETING)))
        .and_then(|()| stream.set_write_timeout(Some(GREETING)));
    if timed.is_err() {
        return;
    }
    let Ok(head) = opening(&stream) else {
        return;
    };
    if !asks_for_a_socket(&String::from_utf8_lossy(&head)) {
        // A page that fails to go has nobody to tell.
        let _ = page(hub, stream, &head);
        return;
    }
    let mut asked = None;
    let mut turned_away = None;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MESSAGE_MAX))
        .max_frame_size(Some(MESSAGE_MAX));
    // The greeting only takes note of what is asked for. Nothing is opened or made for it until
    // the device has shown it holds the space's key.
    let socket = tungstenite::accept_hdr_with_config(
        Stream::Plain(stream),
        |request: &Request, mut response: Response| match asked_for(request.uri().path()) {
            Ok((name, space)) => {
                let offered = offered(request);
                // The token is never said back: `fzn` is, to a device that offers it.
                if offered.iter().any(|p| p == PROTOCOL) {
                    response
                        .headers_mut()
                        .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(PROTOCOL));
                }
                asked = Some(Asked {
                    name: name.map(str::to_owned),
                    space: space.to_owned(),
                    token: offered
                        .iter()
                        .find_map(|p| p.strip_prefix(TOKEN_PROTOCOL))
                        .map(str::to_owned),
                });
                Ok(response)
            }
            Err(status) => {
                turned_away = Some(format!(
                    "{:?} is not a space, so it was turned away with {status}",
                    request.uri().path()
                ));
                let mut refusal = ErrorResponse::new(None);
                *refusal.status_mut() = status;
                Err(refusal)
            }
        },
        Some(config),
    )
    // The error holds the greeting, and with it what the greeting borrows.
    .map_err(|why| why.to_string());
    let (socket, asked) = match (socket, asked) {
        (Ok(socket), Some(asked)) => (socket, asked),
        (Ok(_), None) => return,
        (Err(why), _) => {
            let why = turned_away.unwrap_or_else(|| format!("the greeting failed: {why}"));
            eprintln!("fzn relay: {from}: {why}");
            return;
        }
    };
    let label = asked.label();
    let Ok(mut wire) = Wire::new(socket) else {
        return;
    };
    // The relay's clock is said first, so that a device can say when its own is far off.
    let Ok(now) = Message::Now(system::now()).encode() else {
        return;
    };
    let _ = wire.say(now);
    if let Err(refusal) = enter(hub, &mut wire, &asked, &label, &from) {
        eprintln!("{}", refusal.logged(&from, &label));
        if let Some(said) = refusal.said
            && let Ok(text) = Message::Refused(said).encode()
        {
            let _ = wire.say(text);
        }
        wire.close();
        return;
    }
    drop(greeting);
    let _ = connected(hub, wire, &label);
}

/// Where a space asked for lives, and the auth key kept for it, once everything that is not a
/// matter of the key has been settled.
struct Admitted {
    dir: PathBuf,
    kept: Option<Key>,
}

/// Settles everything about a connection that is not its key, before the challenge, so that a
/// device can tell a refusal of its name or its token from a refusal of its key. A space's key
/// is kept only with the token of the name it is under, or at an open root with none: that is
/// when the space is made, and again if its key file is ever lost, so that nobody without the
/// token can claim a space that has no key. A token file is read only then, so one that cannot
/// be read keeps nobody out of a space that has its key.
fn admit(hub: &Hub, asked: &Asked, label: &str) -> Result<Admitted, Refusal> {
    let space = &asked.space;
    let dir = match &asked.name {
        Some(name) => {
            if !names::is_name(&hub.config, name) {
                return Err(Refusal::said(format!("there is no {name} on this relay")));
            }
            hub.root.join(name).join(space)
        }
        None => {
            if names::is_name(&hub.config, space) {
                return Err(Refusal::said(format!(
                    "{space} is a name on this relay, not a space"
                )));
            }
            let dir = hub.root.join(space);
            // A name taken off the relay leaves its spaces' directories, which are not a space.
            if fs::read_dir(&dir).is_ok_and(|mut entries| {
                entries.any(|entry| entry.is_ok_and(|e| e.path().is_dir()))
            }) {
                return Err(Refusal::said(format!(
                    "{space} holds spaces on this relay, and is not one"
                )));
            }
            dir
        }
    };
    let kept = kept_auth(&dir).map_err(|why| {
        Refusal::said("the relay cannot read the key it keeps for this space".to_owned()).with(why)
    })?;
    if kept.is_some() {
        return Ok(Admitted { dir, kept });
    }
    let whose = match &asked.name {
        Some(name) => format!("{name}'s"),
        None => "the relay's".to_owned(),
    };
    let unreadable = |why| Refusal::said(format!("{whose} token cannot be read")).with(why);
    let token = match &asked.name {
        Some(name) => match names::of_name(&hub.config, name) {
            Ok(Some(hash)) => Some(hash),
            Ok(None) => return Err(Refusal::said(format!("there is no {name} on this relay"))),
            Err(why) => return Err(unreadable(why)),
        },
        None => names::of_root(&hub.config).map_err(unreadable)?,
    };
    if let Some(wanted) = token {
        let made = hub.spaces().contains_key(label) || names::holds_a_space(&dir);
        match &asked.token {
            None if made => {
                return Err(Refusal::said(format!(
                    "{space} has no key on the relay yet, and keeping one needs {whose} token"
                )));
            }
            None => {
                return Err(Refusal::said(format!(
                    "{space} is not on the relay, and making it needs {whose} token"
                )));
            }
            Some(offered) if !names::same(&names::hash(offered), &wanted) => {
                return Err(Refusal::said(format!("the token is not {whose}")));
            }
            Some(_) => {}
        }
    }
    Ok(Admitted { dir, kept })
}

/// Lets a connection into the space it asked for, or says why not. What is not a matter of the
/// key is settled first, by [`admit`]. Then the challenge, and what the device says before it
/// answers is passed over, since a device says what it holds as soon as it connects and again
/// once it is let in. The answer is a proof under the auth key the relay keeps, or for a space
/// it keeps none for, the auth key itself, which it keeps from then on. Only then is the space
/// opened. After the answer the device is told only of its key; the relay's own trouble then is
/// logged and the connection closed.
fn enter(
    hub: &Hub,
    wire: &mut Wire,
    asked: &Asked,
    label: &str,
    from: &str,
) -> Result<(), Refusal> {
    let Admitted { dir, kept } = admit(hub, asked, label)?;
    let lines = match kept {
        Some(_) => Vec::new(),
        None => hub.sealed_lines(label, &dir),
    };
    let mut nonce = [0u8; 32];
    system::System.random(&mut nonce);
    // A key's text is 32 bytes in base64, which is what a nonce is on the wire.
    let nonce = Key::from_bytes(nonce).encode();
    let challenge = Message::Challenge {
        nonce: nonce.clone(),
        claimed: kept.is_some(),
        lines,
    };
    let said = challenge
        .encode()
        .map_err(|why| Refusal::fault(&format!("the challenge could not be written: {why}")))?;
    wire.say(said)
        .map_err(|why| Refusal::fault(&format!("went before the challenge: {why}")))?;

    let deadline = Instant::now() + GREETING;
    let mut passed_over = 0;
    let answer = loop {
        if Instant::now() > deadline {
            let mut refusal =
                Refusal::said("no proof of the key came, so the relay let go".to_owned());
            if passed_over > 0 {
                refusal.logged = Some(format!(
                    "{passed_over} other messages came instead: a build from before auth?"
                ));
            }
            return Err(refusal);
        }
        match wire.hear() {
            // Only an answer is read; anything else, up to a message's bound, is passed over
            // without being parsed.
            Ok(Some(text)) if text.starts_with("{\"proof\"") || text.starts_with("{\"claim\"") => {
                match Message::decode(&text) {
                    Some(answer @ (Message::Proof(_) | Message::Claim(_))) => break answer,
                    _ => passed_over += 1,
                }
            }
            Ok(Some(_)) => passed_over += 1,
            Ok(None) => {}
            Err(_) => return Err(Refusal::fault("went before it answered the challenge")),
        }
        wire.keep_up()
            .map_err(|_| Refusal::fault("went before it answered the challenge"))?;
    };

    match (kept, answer) {
        (Some(kept), Message::Proof(proof)) => {
            if Seal::new(kept).open(&proof) != Opened::Text(nonce) {
                return Err(Refusal::wrong_key());
            }
        }
        (None, Message::Claim(claim)) => {
            let key = Key::decode(&claim)
                .ok_or_else(|| Refusal::said("a claim has to be a key".to_owned()))?;
            if hub.claim(&dir, &key)? {
                eprintln!("fzn relay: {from} {label}: claimed by its first key");
            }
        }
        // A device told the relay keeps a key proves it, and one told it keeps none claims, so
        // the key itself is never taken for a space that has one.
        _ => {
            return Err(Refusal::said(
                "the answer is not the one the challenge asked for".to_owned(),
            ));
        }
    }
    hub.open(label, &dir)
}

/// A connection let into a space, from the replica's first word to the connection's end.
fn connected(hub: &Hub, mut wire: Wire, label: &str) -> Result<()> {
    let peer = hub.peers.fetch_add(1, Ordering::Relaxed);
    let (to, waiting) = mpsc::channel();
    // If the space was closed since it was opened, `to` is dropped here and the connection
    // ends below, to be made again.
    hub.with(label, |space| {
        space.peers.insert(peer, to);
        let out = space.replica.connect(peer);
        space.carry_out(out)
    });

    let ended = (|| -> Result<()> {
        loop {
            if let Some(text) = wire.hear()?
                && let Some(message) = Message::decode(&text)
            {
                hub.with(label, |space| {
                    let out = space.replica.receive(peer, message);
                    space.carry_out(out)
                });
            }
            loop {
                match waiting.try_recv() {
                    Ok(text) => wire.say(text)?,
                    Err(TryRecvError::Empty) => break,
                    // The space was closed under this connection.
                    Err(TryRecvError::Disconnected) => bail!("{label} was closed"),
                }
            }
            wire.keep_up()?;
        }
    })();
    hub.with(label, |space| {
        if space.peers.remove(&peer).is_some() {
            space.replica.disconnect(peer);
        }
        Ok(())
    });
    ended
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::sync::{Event, Link, start};
    use crate::system::random_u64;
    use fzn_engine::replica::Header;
    use std::net::Ipv4Addr;
    use tungstenite::http::Uri;
    use tungstenite::{ClientRequestBuilder, Message as Frame, WebSocket};

    pub(crate) fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fzn-sync-test-{:x}", random_u64()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Adds to a log the way the app does: whole lines, each ending in a newline.
    pub(crate) fn write(dir: &Path, log: &str, text: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(format!("{log}.jsonl")))
            .unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    pub(crate) fn read(dir: &Path, log: &str) -> String {
        fs::read_to_string(dir.join(format!("{log}.jsonl"))).unwrap_or_default()
    }

    /// Waits for something to become so, and says whether it did.
    pub(crate) fn eventually(mut so: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if so() {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }

    /// A relay on a port of its own choosing, serving a root, and where to find it. Who may
    /// make spaces on it is under `.config` in the root, where no test writes unless it means to.
    pub(crate) fn relay(root: &Path) -> String {
        relay_with(root, None)
    }

    pub(crate) fn relay_with(root: &Path, web: Option<PathBuf>) -> String {
        relay_in(root, &config_of(root), web)
    }

    /// Where a relay started by [`relay`] finds who may make spaces on it.
    fn config_of(root: &Path) -> PathBuf {
        root.join(".config")
    }

    fn relay_in(root: &Path, config: &Path, web: Option<PathBuf>) -> String {
        let relay = Relay::bind(root.to_owned(), config.to_owned(), "127.0.0.1:0", web).unwrap();
        let address = relay.address().unwrap().to_string();
        thread::spawn(move || relay.serve());
        address
    }

    /// The key the clients of a test hold unless it gives them another.
    fn a_key() -> Key {
        Key::from_bytes([7; 32])
    }

    /// A device as a relay sees one: a replica on the end of a link, holding its logs in
    /// memory, with what it has been told. It answers a challenge as a device does, from its
    /// key; with no key it is a build from before auth, which answers none.
    pub(crate) struct Client {
        replica: Replica,
        link: Link,
        key: Option<Key>,
        connected: bool,
        /// Whether the relay has let it in, which its saying what it holds shows.
        let_in: bool,
        forked: Vec<String>,
        unreachable: Vec<String>,
        refused: Vec<String>,
    }

    impl Client {
        pub(crate) fn start(relay: &str, space: &str) -> Self {
            Self::with_key(relay, space, Some(a_key()))
        }

        fn with_key(relay: &str, space: &str, key: Option<Key>) -> Self {
            Self {
                replica: Replica::new(),
                link: start(relay.to_owned(), space.to_owned()),
                key,
                connected: false,
                let_in: false,
                forked: Vec::new(),
                unreachable: Vec::new(),
                refused: Vec::new(),
            }
        }

        /// Takes in what the link has brought since it was last asked.
        fn hear(&mut self) {
            let heard: Vec<Event> = self.link.from_relay.try_iter().collect();
            for event in heard {
                let out = match event {
                    Event::Connected => {
                        self.connected = true;
                        self.replica.connect(0)
                    }
                    Event::Message(text) => match Message::decode(&text) {
                        Some(Message::Challenge {
                            nonce,
                            claimed,
                            lines,
                        }) => {
                            self.answer(&nonce, claimed, &lines);
                            Vec::new()
                        }
                        Some(Message::Refused(why)) => {
                            self.refused.push(why);
                            Vec::new()
                        }
                        Some(message) => {
                            self.let_in |= matches!(message, Message::Have(_));
                            self.replica.receive(0, message)
                        }
                        None => Vec::new(),
                    },
                    Event::Disconnected => {
                        self.connected = false;
                        self.replica.disconnect(0);
                        Vec::new()
                    }
                    Event::Unreachable(why) => {
                        self.unreachable.push(why);
                        Vec::new()
                    }
                };
                self.carry_out(out);
            }
        }

        /// Answers a challenge as a device does: a proof when the relay keeps an auth key, a
        /// claim when it keeps none and its key opens one of the lines offered, if any are, and
        /// then what it holds.
        fn answer(&mut self, nonce: &str, claimed: bool, lines: &[String]) {
            let Some(key) = &self.key else {
                return;
            };
            let answer = if claimed {
                Message::Proof(Seal::new(key.auth()).seal(nonce, &system::System))
            } else if !lines.is_empty()
                && lines
                    .iter()
                    .all(|line| Seal::new(key.clone()).open(line) == Opened::Shut)
            {
                return;
            } else {
                Message::Claim(key.auth().encode())
            };
            for message in [answer, Message::Have(self.replica.have())] {
                let _ = self.link.to_relay.send(message.encode().unwrap());
            }
        }

        fn carry_out(&mut self, out: Vec<Out>) {
            for out in out {
                match out {
                    Out::Send(_, message) => {
                        let _ = self.link.to_relay.send(message.encode().unwrap());
                    }
                    Out::Forked { log, .. } => self.forked.push(log),
                    _ => {}
                }
            }
        }

        /// Adds lines to the end of a log, as its writer does.
        fn write(&mut self, log: &str, text: &str) {
            let out = self.replica.append(log, text.lines().map(str::to_owned));
            self.carry_out(out);
        }

        /// A log as held, each line with a newline.
        fn read(&mut self, log: &str) -> String {
            self.hear();
            self.replica
                .lines(log)
                .iter()
                .map(|line| format!("{line}\n"))
                .collect()
        }

        /// Waits for something to become so, taking in what the link brings meanwhile.
        fn until(&mut self, so: impl Fn() -> bool) -> bool {
            eventually(|| {
                self.hear();
                so()
            })
        }

        /// Waits for the relay to let it in.
        fn connects(&mut self) {
            assert!(eventually(|| {
                self.hear();
                self.let_in
            }));
        }

        /// Waits for the relay to refuse it, and gives why.
        fn refused(&mut self) -> String {
            assert!(eventually(|| {
                self.hear();
                !self.refused.is_empty()
            }));
            self.refused[0].clone()
        }
    }

    /// A connection made by hand, for what a link does not do: ask for a space under a name,
    /// offer a token, see the greeting's answer, and answer the challenge or not.
    struct Raw {
        socket: WebSocket<TcpStream>,
        /// The subprotocol the relay said back.
        protocol: Option<String>,
    }

    impl Raw {
        fn knock(address: &str, path: &str, token: Option<&str>) -> Self {
            let stream = TcpStream::connect(address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let uri: Uri = format!("ws://{address}{path}").parse().unwrap();
            let mut request = ClientRequestBuilder::new(uri).with_sub_protocol(PROTOCOL);
            if let Some(token) = token {
                request = request.with_sub_protocol(format!("{TOKEN_PROTOCOL}{token}"));
            }
            let (socket, response) = tungstenite::client(request, stream).unwrap();
            let protocol = response
                .headers()
                .get(SEC_WEBSOCKET_PROTOCOL)
                .map(|value| value.to_str().unwrap().to_owned());
            Self { socket, protocol }
        }

        /// The next message, or nothing once the relay has closed.
        fn next(&mut self) -> Option<Message> {
            loop {
                match self.socket.read() {
                    Ok(Frame::Text(text)) => return Message::decode(&text),
                    Ok(Frame::Close(_)) | Err(_) => return None,
                    Ok(_) => {}
                }
            }
        }

        fn say(&mut self, message: &Message) {
            let text = message.encode().unwrap();
            self.socket.send(Frame::text(text)).unwrap();
        }

        /// Hears the clock, and then the challenge or a refusal before it.
        fn challenged(&mut self) -> Result<(String, bool, Vec<String>), String> {
            assert!(matches!(self.next(), Some(Message::Now(_))));
            match self.next() {
                Some(Message::Challenge {
                    nonce,
                    claimed,
                    lines,
                }) => Ok((nonce, claimed, lines)),
                Some(Message::Refused(why)) => Err(why),
                other => panic!("{other:?}"),
            }
        }

        /// Answers a challenge as a device holding this key does, and says what it holds.
        fn answer(&mut self, key: &Key, nonce: &str, claimed: bool) {
            let answer = if claimed {
                Message::Proof(Seal::new(key.auth()).seal(nonce, &system::System))
            } else {
                Message::Claim(key.auth().encode())
            };
            self.say(&answer);
            self.say(&Message::Have(Default::default()));
        }

        /// What came of the answer: let in, which the relay's saying what it holds shows, or
        /// refused, and why. Nothing else may come before either.
        fn outcome(&mut self) -> Result<(), String> {
            match self.next() {
                Some(Message::Have(_)) => Ok(()),
                Some(Message::Refused(why)) => {
                    assert_eq!(self.next(), None, "and the relay closes");
                    Err(why)
                }
                other => panic!("{other:?}"),
            }
        }

        /// Enters a space with a key, or says why it could not.
        fn enter(&mut self, key: &Key) -> Result<(), String> {
            let (nonce, claimed, _) = self.challenged()?;
            self.answer(key, &nonce, claimed);
            self.outcome()
        }
    }

    fn enters(address: &str, path: &str, token: Option<&str>, key: &Key) -> Result<(), String> {
        Raw::knock(address, path, token).enter(key)
    }

    #[test]
    fn a_connection_asks_for_a_space_by_the_version_and_its_name() {
        assert_eq!(asked_for("/1/main"), Ok((None, "main")));
        assert_eq!(asked_for("/1/garden-2022"), Ok((None, "garden-2022")));
        assert_eq!(asked_for("/1/joe/main"), Ok((Some("joe"), "main")));
        assert_eq!(asked_for("/2/main"), Err(StatusCode::NOT_IMPLEMENTED));
        assert_eq!(asked_for("/2/joe/main"), Err(StatusCode::NOT_IMPLEMENTED));
        for path in [
            "/",
            "/main",
            "/1/",
            "/1/joe/",
            "/1/a/b/c",
            "/1/../main",
            "/1/joe/..",
            "/1/..",
            "/1/a b",
        ] {
            assert_eq!(asked_for(path), Err(StatusCode::NOT_FOUND), "{path}");
        }
    }

    #[test]
    fn a_relay_says_its_clock_before_anything_else() {
        let root = temp_dir();
        let hub = root.join("relay");
        fs::create_dir_all(&hub).unwrap();
        let link = start(relay(&hub), "main".into());
        let first = loop {
            if let Event::Message(text) = link.from_relay.recv_timeout(GREETING).unwrap() {
                break text;
            }
        };
        let Some(Message::Now(then)) = Message::decode(&first) else {
            panic!("{first}");
        };
        assert!((system::now().ms - then.ms).abs() < 5_000, "{first}");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_relay_answers_a_close_and_ends_a_refusal_with_one_so_that_a_proxy_passes_it_on() {
        let root = temp_dir();
        let hub = root.join("relay");
        fs::create_dir_all(&hub).unwrap();
        let address = relay(&hub);

        let mut done = Raw::knock(&address, "/1/main", None);
        assert!(matches!(done.next(), Some(Message::Now(_))));
        done.socket.close(None).unwrap();
        let answered = loop {
            match done.socket.read() {
                Ok(Frame::Close(_)) => break true,
                Ok(_) => {}
                Err(_) => break false,
            }
        };
        assert!(
            answered,
            "the relay dropped the socket without answering the close"
        );

        let mut turned = Raw::knock(&address, "/1/nobody/main", None);
        assert_eq!(
            turned.challenged().unwrap_err(),
            "there is no nobody on this relay"
        );
        let ended = turned.socket.read();
        assert!(matches!(ended, Ok(Frame::Close(_))), "{ended:?}");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn two_devices_and_a_relay_come_to_hold_the_same_logs() {
        let root = temp_dir();
        let hub = root.join("relay");
        fs::create_dir_all(&hub).unwrap();
        let address = relay(&hub);

        let mut a = Client::start(&format!("ws://{address}"), "main");
        a.write("dev-a", "a1\na2\n");
        a.connects();
        assert!(a.until(|| read(&hub.join("main"), "dev-a") == "a1\na2\n"));

        // A device that comes later gets what the relay was given before it came, and what
        // either writes from then on reaches the other.
        let mut b = Client::start(&address, "main");
        b.write("dev-b", "b1\n");
        b.connects();
        assert!(eventually(|| b.read("dev-a") == "a1\na2\n"));
        assert!(eventually(|| a.read("dev-b") == "b1\n"));
        a.write("dev-a", "a3\n");
        b.write("dev-b", "b2\nb3\n");
        assert!(eventually(|| b.read("dev-a") == "a1\na2\na3\n"));
        assert!(eventually(|| a.read("dev-b") == "b1\nb2\nb3\n"));
        assert!(b.until(|| read(&hub.join("main"), "dev-b") == "b1\nb2\nb3\n"));

        // Another space on the same relay is another set of logs.
        let mut other = Client::start(&address, "other");
        other.write("dev-a", "elsewhere\n");
        other.connects();
        assert!(other.until(|| read(&hub.join("other"), "dev-a") == "elsewhere\n"));
        thread::sleep(Duration::from_millis(500));
        assert_eq!(other.read("dev-b"), "", "and nothing of main comes to it");
        assert!(a.unreachable.is_empty() && b.unreachable.is_empty());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_relay_deletes_the_file_of_a_log_that_a_compaction_retires() {
        let root = temp_dir();
        let hub = root.join("relay");
        let address = relay(&hub);
        let mut a = Client::start(&address, "main");
        a.write("dev-a", "a1\na2\na3\n");
        a.connects();
        let space = hub.join("main");
        assert!(a.until(|| read(&space, "dev-a") == "a1\na2\na3\n"));

        // The writer starts a new log whose header names the old one, and the snapshot after it.
        let header = Header {
            compacts: vec!["dev-a".into()],
            snapshot: 2,
        };
        let compacted = format!("{}\ns1\ns2\n", header.line());
        a.write("dev-b", &compacted);
        assert!(
            a.until(|| read(&space, "dev-b") == compacted && !space.join("dev-a.jsonl").exists())
        );

        // A device that comes later is given the new log, and never the old.
        let mut b = Client::start(&address, "main");
        b.connects();
        assert!(eventually(|| b.read("dev-b") == compacted));
        assert_eq!(b.read("dev-a"), "");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_line_a_crash_left_unfinished_is_cut_when_a_space_is_opened() {
        let root = temp_dir();
        let hub = root.join("relay");
        let space = hub.join("main");
        fs::create_dir_all(&space).unwrap();
        write(&space, "dev-a", "a1\na2\n{\"half");
        let address = relay(&hub);

        let mut a = Client::start(&address, "main");
        a.connects();
        assert!(eventually(|| a.read("dev-a") == "a1\na2\n"));
        assert_eq!(read(&space, "dev-a"), "a1\na2\n");

        // What comes after is added where the whole lines end.
        a.write("dev-a", "a3\n");
        assert!(a.until(|| read(&space, "dev-a") == "a1\na2\na3\n"));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_relay_turns_away_a_space_another_relay_holds() {
        let root = temp_dir();
        let hub = root.join("relay");
        let (first, second) = (relay(&hub), relay(&hub));

        let mut a = Client::start(&first, "main");
        a.write("dev-a", "a1\n");
        a.connects();
        let space = hub.join("main");
        assert!(a.until(|| read(&space, "dev-a") == "a1\n"));

        // The second relay lets the device prove its key, and then cannot open the space. That
        // is the relay's trouble, not the device's, so it is not told it was refused, which a
        // device would take as doubt about its passphrase: the connection only ends.
        let mut b = Client::start(&second, "main");
        b.write("dev-b", "b1\n");
        thread::sleep(Duration::from_secs(3));
        b.hear();
        assert!(!b.let_in, "and is not let in when it tries again");
        assert!(b.refused.is_empty(), "{:?}", b.refused);
        let mut raw = Raw::knock(&second, "/1/main", None);
        let (nonce, claimed, _) = raw.challenged().unwrap();
        raw.answer(&a_key(), &nonce, claimed);
        assert_eq!(raw.next(), None, "closed with nothing said");
        assert_eq!(read(&space, "dev-b"), "");
        assert_eq!(b.read("dev-a"), "");

        // The relay that holds the space goes on as before.
        a.write("dev-a", "a2\n");
        assert!(a.until(|| read(&space, "dev-a") == "a1\na2\n"));
        fs::remove_dir_all(&root).unwrap();
    }

    /// Asks a relay for something as a browser would, and gives the answer's head and body.
    fn ask(address: &str, request: &str) -> (String, Vec<u8>) {
        let mut stream = TcpStream::connect(address).unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).unwrap();
        let end = answer.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let head = String::from_utf8_lossy(&answer[..end]).into_owned();
        (head, answer[end + 4..].to_vec())
    }

    fn get(address: &str, path: &str) -> (String, Vec<u8>) {
        ask(
            address,
            &format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n\r\n"),
        )
    }

    #[test]
    fn a_relay_hands_out_the_pages_of_the_web_app() {
        let root = temp_dir();
        let (web, hub) = (root.join("web"), root.join("relay"));
        fs::create_dir_all(web.join("pkg")).unwrap();
        fs::create_dir_all(&hub).unwrap();
        fs::write(web.join("index.html"), "<!doctype html><title>fzn</title>").unwrap();
        fs::write(web.join("app.js"), "export {};").unwrap();
        fs::write(web.join("pkg").join("fzn_web_bg.wasm"), [0u8, 97, 115, 109]).unwrap();
        fs::write(web.join(".secret"), "no").unwrap();
        fs::write(web.join("build.sh"), "no").unwrap();
        fs::write(web.join("pkg").join("notes.txt"), "no").unwrap();
        fs::create_dir_all(web.join("editor")).unwrap();
        fs::write(web.join("editor").join("render.js"), "no").unwrap();
        fs::write(root.join("outside"), "no").unwrap();
        let address = relay_with(&hub, Some(web));

        let (head, body) = get(&address, "/");
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        assert!(head.contains("Content-Type: text/html; charset=utf-8"));
        assert!(head.contains("Content-Security-Policy: default-src 'self'"));
        assert!(head.contains("style-src 'self' 'unsafe-inline'"));
        assert_eq!(body, b"<!doctype html><title>fzn</title>");
        let (head, body) = get(&address, "/app.js?v=2");
        assert!(head.contains("Content-Type: text/javascript"), "{head}");
        assert_eq!(body, b"export {};");
        let (head, body) = get(&address, "/pkg/fzn_web_bg.wasm");
        assert!(head.contains("Content-Type: application/wasm"), "{head}");
        assert!(head.contains("Content-Length: 4"));
        assert_eq!(body, [0u8, 97, 115, 109]);

        for path in [
            "/missing.js",
            "/../outside",
            "/pkg/../../outside",
            "/.secret",
            "//app.js",
            // In the directory, but not the page's: only what would be built in is served.
            "/build.sh",
            "/pkg/notes.txt",
            "/editor/render.js",
        ] {
            let (head, _) = get(&address, path);
            assert!(head.starts_with("HTTP/1.1 404"), "{path}: {head}");
        }
        let post = format!("POST / HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\n\r\n");
        assert!(ask(&address, &post).0.starts_with("HTTP/1.1 405"));

        // And it is still a relay, on the same address.
        let mut a = Client::start(&address, "main");
        a.write("dev-a", "a1\n");
        a.connects();
        assert!(a.until(|| read(&hub.join("main"), "dev-a") == "a1\n"));

        // A relay built with no page and given no directory has none to hand out.
        let bare = relay_page(&hub.join("elsewhere-root"), None);
        assert!(get(&bare, "/").0.starts_with("HTTP/1.1 404"));
        fs::remove_dir_all(&root).unwrap();
    }

    /// A relay that carries this page and was given no directory.
    fn relay_page(root: &Path, page: Option<Page>) -> String {
        let relay =
            Relay::bind_with(root.to_owned(), config_of(root), "127.0.0.1:0", None, page).unwrap();
        let address = relay.address().unwrap().to_string();
        thread::spawn(move || relay.serve());
        address
    }

    fn ask_with(address: &str, method: &str, path: &str, headers: &str) -> (String, Vec<u8>) {
        ask(
            address,
            &format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\n{headers}\r\n"),
        )
    }

    const INDEX: &str = "<!doctype html><meta name=\"fzn-build\" content=\"\"><title>fzn</title>";

    /// A page as small as one can be: the files the relay treats as the page's.
    fn a_page() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("index.html", INDEX.into()),
            ("app.js", "export {};".into()),
            ("pkg/fzn_web_bg.wasm", vec![0, 97, 115, 109]),
            ("vendor/chunk-ABCDEFGH.js", "export const a = 1;".into()),
        ]
    }

    fn write_page(web: &Path, files: &[(&str, Vec<u8>)]) {
        for (path, bytes) in files {
            let file = web.join(path);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            fs::write(file, bytes).unwrap();
        }
    }

    #[test]
    fn a_relay_names_the_build_of_a_directory_on_every_answer_and_writes_it_into_the_page() {
        let root = temp_dir();
        let (web, hub) = (root.join("web"), root.join("relay"));
        write_page(&web, &a_page());
        let address = relay_with(&hub, Some(web.clone()));

        let builds: Vec<String> = ["/", "/app.js", "/pkg/fzn_web_bg.wasm", "/missing.js"]
            .into_iter()
            .map(|path| {
                let (head, _) = get(&address, path);
                header(&head, "fzn-build").unwrap().to_owned()
            })
            .collect();
        let build = &builds[0];
        assert_eq!(build.len(), 16, "{build}");
        assert!(builds.iter().all(|b| b == build), "{builds:?}");
        // The same files are the same build, whether in a directory or built in.
        assert_eq!(*build, Page::from_files(a_page()).build);
        let (_, body) = get(&address, "/");
        let filled = format!("<meta name=\"fzn-build\" content=\"{build}\">");
        assert!(String::from_utf8_lossy(&body).contains(&filled));

        // Writing the same files again names the same build; changing one names another, once
        // the second the last one is held for has passed.
        write_page(&web, &a_page());
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(
            header(&get(&address, "/").0, "fzn-build"),
            Some(build.as_str())
        );
        fs::write(web.join("app.js"), "export {}; ").unwrap();
        thread::sleep(Duration::from_millis(1100));
        let (head, body) = get(&address, "/");
        let now = header(&head, "fzn-build").unwrap();
        assert_ne!(now, build);
        assert!(String::from_utf8_lossy(&body).contains(now));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_page_the_browser_still_holds_is_answered_304_without_a_body() {
        let root = temp_dir();
        let page = Page::from_files(a_page());
        let build = page.build.clone();
        let address = relay_page(&root, Some(page));

        let (head, body) = get(&address, "/app.js");
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        assert_eq!(body, b"export {};");
        assert_eq!(header(&head, "cache-control"), Some("no-cache"));
        assert_eq!(header(&head, "fzn-build"), Some(build.as_str()));
        let etag = header(&head, "etag").unwrap().to_owned();

        let (head, body) = ask_with(
            &address,
            "GET",
            "/app.js",
            &format!("If-None-Match: {etag}\r\n"),
        );
        assert!(head.starts_with("HTTP/1.1 304 Not Modified"), "{head}");
        assert!(body.is_empty());
        assert_eq!(header(&head, "content-length"), None);
        assert_eq!(header(&head, "etag"), Some(etag.as_str()));
        assert_eq!(header(&head, "fzn-build"), Some(build.as_str()));

        let other = "If-None-Match: \"0000000000000000\", W/\"1111111111111111\"\r\n";
        let (head, body) = ask_with(&address, "GET", "/app.js", other);
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        assert_eq!(body, b"export {};");

        // The page itself carries its build, and its tag is over what is sent.
        let (head, body) = get(&address, "/");
        let filled = format!("<meta name=\"fzn-build\" content=\"{build}\">");
        assert!(String::from_utf8_lossy(&body).contains(&filled));
        let etag = header(&head, "etag").unwrap();
        assert_eq!(etag, format!("\"{}\"", web_files::etag_of(&body)));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_file_is_sent_gzipped_only_to_a_browser_that_takes_it() {
        use flate2::read::GzDecoder;
        let root = temp_dir();
        let mut page = Page::from_files(a_page());
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        gz.write_all(b"export {};").unwrap();
        page.files.get_mut("app.js").unwrap().gzip = Some(Cow::Owned(gz.finish().unwrap()));
        let address = relay_page(&root, Some(page));

        let takes = "Accept-Encoding: br, gzip;q=0.8, deflate\r\n";
        let (head, body) = ask_with(&address, "GET", "/app.js", takes);
        assert_eq!(header(&head, "content-encoding"), Some("gzip"), "{head}");
        assert_eq!(header(&head, "vary"), Some("Accept-Encoding"));
        let length = body.len().to_string();
        assert_eq!(header(&head, "content-length"), Some(length.as_str()));
        let mut inflated = Vec::new();
        GzDecoder::new(body.as_slice())
            .read_to_end(&mut inflated)
            .unwrap();
        assert_eq!(inflated, b"export {};");

        for refuses in [
            "",
            "Accept-Encoding: br\r\n",
            "Accept-Encoding: gzip;q=0\r\n",
        ] {
            let (head, body) = ask_with(&address, "GET", "/app.js", refuses);
            assert_eq!(header(&head, "content-encoding"), None, "{refuses}");
            assert_eq!(body, b"export {};");
        }
        // A file with no twin is sent as it is, whatever the browser takes.
        let (head, body) = ask_with(&address, "GET", "/pkg/fzn_web_bg.wasm", takes);
        assert_eq!(header(&head, "content-encoding"), None);
        assert_eq!(body, [0u8, 97, 115, 109]);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_head_is_answered_as_a_get_without_the_body() {
        let root = temp_dir();
        let address = relay_page(&root, Some(Page::from_files(a_page())));
        let (got, _) = get(&address, "/app.js");
        let (head, body) = ask_with(&address, "HEAD", "/app.js", "");
        assert_eq!(head, got);
        assert!(body.is_empty());
        assert_eq!(header(&head, "content-length"), Some("10"));
        let (head, body) = ask_with(&address, "HEAD", "/missing.js", "");
        assert!(head.starts_with("HTTP/1.1 404"), "{head}");
        assert!(body.is_empty());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_chunk_named_by_its_contents_is_held_for_a_year() {
        let root = temp_dir();
        let address = relay_page(&root, Some(Page::from_files(a_page())));
        let (head, body) = get(&address, "/vendor/chunk-ABCDEFGH.js");
        assert_eq!(
            header(&head, "cache-control"),
            Some("max-age=31536000, immutable")
        );
        assert_eq!(body, b"export const a = 1;");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_directory_given_with_web_is_served_in_place_of_the_page_built_in() {
        let root = temp_dir();
        let web = root.join("web");
        write_page(&web, &[("app.js", "export const fromDisk = 1;".into())]);
        let relay = Relay::bind_with(
            root.join("relay"),
            config_of(&root),
            "127.0.0.1:0",
            Some(web),
            Some(Page::from_files(a_page())),
        )
        .unwrap();
        let address = relay.address().unwrap().to_string();
        thread::spawn(move || relay.serve());
        let (head, body) = get(&address, "/app.js");
        assert_eq!(body, b"export const fromDisk = 1;");
        assert_eq!(header(&head, "etag"), None);
        assert!(
            get(&address, "/vendor/chunk-ABCDEFGH.js")
                .0
                .starts_with("HTTP/1.1 404")
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn the_page_built_in_is_the_web_directory_named_as_a_directory_would_be() {
        let Some(page) = Page::built_in() else {
            // Built without web/pkg: there is nothing built in to check.
            return;
        };
        let web = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../web");
        let listed = web_files::listed(&web);
        let mut held: Vec<&String> = page.files.keys().collect();
        held.sort();
        assert_eq!(held, listed.iter().collect::<Vec<_>>());
        assert!(listed.contains(&"pkg/fzn_web_bg.wasm".to_owned()));
        assert!(
            !listed
                .iter()
                .any(|p| p.starts_with("editor/") || p == "build.sh")
        );
        assert_eq!(page.build.len(), 16);
        let index = String::from_utf8_lossy(&page.files["index.html"].body).into_owned();
        assert!(
            index.contains(&format!("content=\"{}\"", page.build)),
            "{index}"
        );
        let files: Vec<(String, Vec<u8>)> = listed
            .into_iter()
            .map(|p| {
                let bytes = fs::read(web.join(&p)).unwrap();
                (p, bytes)
            })
            .collect();
        let build = web_files::build_of(files.iter().map(|(p, b)| (p.as_str(), b.as_slice())));
        assert_eq!(page.build, build);

        // Every twin is smaller and is its file gzipped, and each tag is over what is sent.
        for (path, served) in &page.files {
            assert_eq!(served.etag, web_files::etag_of(&served.body), "{path}");
            if let Some(gzip) = &served.gzip {
                assert!(gzip.len() < served.body.len(), "{path}");
                let mut inflated = Vec::new();
                flate2::read::GzDecoder::new(&gzip[..])
                    .read_to_end(&mut inflated)
                    .unwrap();
                assert_eq!(inflated, &served.body[..], "{path}");
            }
        }
        assert!(page.files["pkg/fzn_web_bg.wasm"].gzip.is_some());
    }

    #[test]
    fn a_relay_given_no_directory_hands_out_the_page_built_in() {
        if Page::built_in().is_none() {
            return;
        }
        let root = temp_dir();
        let address = relay(&root);
        let (head, body) = ask_with(&address, "GET", "/", "Accept-Encoding: gzip\r\n");
        assert!(head.starts_with("HTTP/1.1 200 OK"), "{head}");
        assert_eq!(header(&head, "fzn-build"), Some(built::BUILD));
        assert_eq!(header(&head, "content-encoding"), Some("gzip"));
        let mut page = String::new();
        flate2::read::GzDecoder::new(&body[..])
            .read_to_string(&mut page)
            .unwrap();
        let filled = format!("<meta name=\"fzn-build\" content=\"{}\">", built::BUILD);
        assert!(page.contains(&filled));
        let etag = header(&head, "etag").unwrap();
        let again = format!("If-None-Match: {etag}\r\n");
        let (head, body) = ask_with(&address, "GET", "/", &again);
        assert!(head.starts_with("HTTP/1.1 304"), "{head}");
        assert!(body.is_empty());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_fork_and_a_relay_that_is_not_there_are_said() {
        let root = temp_dir();
        let hub = root.join("relay");
        let address = relay(&hub);

        let mut a = Client::start(&address, "main");
        a.write("dev-a", "a1\na2\na3\n");
        a.connects();
        assert!(a.until(|| read(&hub.join("main"), "dev-a") == "a1\na2\na3\n"));
        // The same writer's id on a second machine, with a log of its own under it.
        let mut b = Client::start(&address, "main");
        b.write("dev-a", "a1\nx2\n");
        b.write("dev-b", "b1\n");
        b.connects();
        assert!(eventually(|| {
            b.hear();
            b.forked == ["dev-a"]
        }));
        assert!(eventually(|| a.read("dev-b") == "b1\n"), "other logs go on");
        assert_eq!(b.read("dev-a"), "a1\nx2\n", "and neither log is touched");
        assert_eq!(a.read("dev-a"), "a1\na2\na3\n");

        // Nobody listening: it is said once, and tried again.
        let free = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let mut nowhere = Client::start(&free.to_string(), "main");
        assert!(eventually(|| {
            nowhere.hear();
            !nowhere.unreachable.is_empty()
        }));
        a.hear();
        assert!(
            a.unreachable.is_empty() && a.forked.is_empty(),
            "the device whose log the relay has is told nothing"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_space_is_claimed_by_the_first_key_and_refuses_another() {
        let root = temp_dir();
        let address = relay(&root);
        let mut a = Client::start(&address, "main");
        a.write("dev-a", "a1\n");
        a.connects();
        let space = root.join("main");
        assert!(a.until(|| read(&space, "dev-a") == "a1\n"));
        let kept = fs::read_to_string(space.join(AUTH_FILE)).unwrap();
        assert_eq!(Key::decode(&kept), Some(a_key().auth()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(space.join(AUTH_FILE)).unwrap().permissions();
            assert_eq!(mode.mode() & 0o777, 0o600);
        }

        // Another passphrase is told so, and is sent nothing of the space.
        let mut b = Client::with_key(&address, "main", Some(Key::from_bytes([9; 32])));
        b.write("dev-b", "b1\n");
        assert_eq!(
            b.refused(),
            "the passphrase does not open this space on the relay"
        );
        thread::sleep(Duration::from_millis(500));
        assert_eq!(b.read("dev-a"), "");
        assert_eq!(read(&space, "dev-b"), "");

        // The first key is let in again, by its proof.
        let mut again = Client::start(&address, "main");
        again.connects();
        assert!(eventually(|| again.read("dev-a") == "a1\n"));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_proof_under_the_wrong_key_is_refused_before_anything_of_the_space_is_said() {
        let root = temp_dir();
        let address = relay(&root);
        let mut a = Client::start(&address, "main");
        a.write("dev-a", "a1\n");
        a.connects();
        assert!(a.until(|| read(&root.join("main"), "dev-a") == "a1\n"));

        // `outcome` takes the next message after the answer, and nothing may come before it.
        let mut raw = Raw::knock(&address, "/1/main", None);
        let (nonce, claimed, lines) = raw.challenged().unwrap();
        assert!(claimed);
        assert!(lines.is_empty());
        assert_eq!(nonce.len(), 44, "{nonce}");
        raw.answer(&Key::from_bytes([9; 32]), &nonce, claimed);
        assert_eq!(
            raw.outcome(),
            Err("the passphrase does not open this space on the relay".into())
        );

        // A proof of another nonce is no proof either.
        let mut raw = Raw::knock(&address, "/1/main", None);
        let (nonce, claimed, _) = raw.challenged().unwrap();
        raw.answer(&a_key(), &format!("{nonce}x"), claimed);
        assert!(raw.outcome().is_err());
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_device_that_says_what_it_holds_before_its_proof_is_let_in_after_it() {
        let root = temp_dir();
        let address = relay(&root);
        let mut raw = Raw::knock(&address, "/1/main", None);
        let (nonce, claimed, _) = raw.challenged().unwrap();
        raw.say(&Message::Have(Default::default()));
        raw.say(&Message::Now(system::now()));
        thread::sleep(Duration::from_millis(300));
        raw.answer(&a_key(), &nonce, claimed);
        assert_eq!(raw.outcome(), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_device_from_before_auth_is_refused_after_the_greeting() {
        let root = temp_dir();
        let space = root.join("main");
        fs::create_dir_all(&space).unwrap();
        write(&space, "dev-a", "a1\n");
        let address = relay(&root);
        let mut old = Client::with_key(&address, "main", None);
        old.write("dev-b", "b1\n");
        assert_eq!(
            old.refused(),
            "no proof of the key came, so the relay let go"
        );
        assert!(!old.let_in);
        assert_eq!(old.read("dev-a"), "");
        assert_eq!(read(&space, "dev-b"), "");
        assert!(!space.join(AUTH_FILE).exists());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn an_unclaimed_space_that_holds_lines_offers_a_sealed_line_of_each_log_with_its_challenge() {
        let root = temp_dir();
        let space = root.join("main");
        fs::create_dir_all(&space).unwrap();
        let sealed = Seal::new(a_key()).seal("a note", &system::System);
        let stray = Seal::new(Key::from_bytes([9; 32])).seal("a stray note", &system::System);
        // A plain line from before sealing comes first, and is not one offered; and a log
        // written under another key comes first by its name, and does not shut the space.
        write(&space, "dev-a", &format!("{stray}\n"));
        write(&space, "dev-b", &format!("plain\n{sealed}\n{sealed}\n"));
        let address = relay(&root);
        let mut raw = Raw::knock(&address, "/1/main", None);
        let (_, claimed, lines) = raw.challenged().unwrap();
        assert!(!claimed);
        assert_eq!(lines, [stray.clone(), sealed.clone()]);

        // An empty space offers none.
        let mut raw = Raw::knock(&address, "/1/empty", None);
        let (_, claimed, lines) = raw.challenged().unwrap();
        assert!(!claimed);
        assert!(lines.is_empty());
        assert!(
            !root.join("empty").exists(),
            "and nothing is made by asking"
        );

        // A device whose key opens none of the lines does not claim the space, and one whose
        // key opens one of them does.
        let mut wrong = Client::with_key(&address, "main", Some(Key::from_bytes([8; 32])));
        thread::sleep(Duration::from_millis(500));
        wrong.hear();
        assert!(!wrong.let_in);
        let mut right = Client::start(&address, "main");
        right.connects();
        assert!(eventually(|| right.read("dev-b").lines().count() == 3));
        let kept = fs::read_to_string(space.join(AUTH_FILE)).unwrap();
        assert_eq!(Key::decode(&kept), Some(a_key().auth()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_space_is_made_under_a_name_with_its_token_and_not_without() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        let token = names::add(&config, &spaces, "joe").unwrap();
        let address = relay_in(&spaces, &config, None);
        let made = spaces.join("joe").join("main");

        let refused = |path: &str, token: Option<&str>| {
            Raw::knock(&address, path, token).challenged().unwrap_err()
        };
        assert_eq!(
            refused("/1/joe/main", None),
            "main is not on the relay, and making it needs joe's token"
        );
        let wrong = "0".repeat(32);
        assert_eq!(
            refused("/1/joe/main", Some(&wrong)),
            "the token is not joe's"
        );
        assert_eq!(
            refused("/1/nobody/main", Some(&token)),
            "there is no nobody on this relay"
        );
        assert!(!spaces.join("joe").exists(), "and nothing was made");

        assert_eq!(
            enters(&address, "/1/joe/main", Some(&token), &a_key()),
            Ok(())
        );
        assert!(made.join(AUTH_FILE).is_file());

        // Once made, the space is entered with its key and no token, from anywhere.
        assert_eq!(enters(&address, "/1/joe/main", None, &a_key()), Ok(()));
        assert_eq!(
            enters(&address, "/1/joe/main", None, &Key::from_bytes([9; 32])),
            Err("the passphrase does not open this space on the relay".into())
        );
        // joe's main is not the root's main.
        assert_eq!(
            enters(&address, "/1/main", None, &Key::from_bytes([9; 32])),
            Ok(())
        );
        assert!(spaces.join("main").join(AUTH_FILE).is_file());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn the_root_is_open_without_a_token_file_and_closed_with_one() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        let address = relay_in(&spaces, &config, None);
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));

        let token = names::close_root(&config).unwrap();
        assert_eq!(
            Raw::knock(&address, "/1/garden", None)
                .challenged()
                .unwrap_err(),
            "garden is not on the relay, and making it needs the relay's token"
        );
        assert_eq!(
            Raw::knock(&address, "/1/garden", Some(&"0".repeat(32)))
                .challenged()
                .unwrap_err(),
            "the token is not the relay's"
        );
        assert!(!spaces.join("garden").exists());
        assert_eq!(
            enters(&address, "/1/garden", Some(&token), &a_key()),
            Ok(())
        );
        // A space there already needs no token.
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_space_at_the_root_named_like_a_name_is_refused() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        names::add(&config, &spaces, "joe").unwrap();
        let address = relay_in(&spaces, &config, None);
        assert_eq!(
            Raw::knock(&address, "/1/joe", None)
                .challenged()
                .unwrap_err(),
            "joe is a name on this relay, not a space"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn the_relay_says_fzn_back_and_never_the_token() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        let token = names::add(&config, &spaces, "joe").unwrap();
        let address = relay_in(&spaces, &config, None);
        let raw = Raw::knock(&address, "/1/joe/main", Some(&token));
        assert_eq!(raw.protocol.as_deref(), Some(PROTOCOL));

        // A request offering no subprotocol is answered with none.
        let stream = TcpStream::connect(&address).unwrap();
        let (_, response) = tungstenite::client(format!("ws://{address}/1/main"), stream).unwrap();
        assert!(response.headers().get(SEC_WEBSOCKET_PROTOCOL).is_none());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn two_claims_at_once_keep_one_key() {
        let root = temp_dir();
        let address = relay(&root);
        for round in 0..5 {
            let space = format!("race-{round}");
            let path = format!("/1/{space}");
            let mut first = Raw::knock(&address, &path, None);
            let mut second = Raw::knock(&address, &path, None);
            let (one, claimed_one, _) = first.challenged().unwrap();
            let (two, claimed_two, _) = second.challenged().unwrap();
            assert!(!claimed_one && !claimed_two);
            let (a, b) = (Key::from_bytes([1; 32]), Key::from_bytes([2; 32]));
            let racing = thread::spawn(move || {
                second.answer(&b, &two, false);
                second.outcome()
            });
            first.answer(&a, &one, false);
            let outcomes = [first.outcome(), racing.join().unwrap()];
            assert_eq!(
                outcomes.iter().filter(|o| o.is_ok()).count(),
                1,
                "{outcomes:?}"
            );
            let kept = fs::read_to_string(root.join(&space).join(AUTH_FILE)).unwrap();
            let winner = if outcomes[0].is_ok() {
                [1; 32]
            } else {
                [2; 32]
            };
            assert_eq!(Key::decode(&kept), Some(Key::from_bytes(winner).auth()));
        }

        // Two claims with one key are both let in.
        let mut first = Raw::knock(&address, "/1/same", None);
        let mut second = Raw::knock(&address, "/1/same", None);
        let (one, _, _) = first.challenged().unwrap();
        let (two, _, _) = second.challenged().unwrap();
        first.answer(&a_key(), &one, false);
        second.answer(&a_key(), &two, false);
        assert_eq!(first.outcome(), Ok(()));
        assert_eq!(second.outcome(), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_message_larger_than_the_bound_ends_the_connection() {
        let root = temp_dir();
        let address = relay(&root);
        let mut raw = Raw::knock(&address, "/1/main", None);
        raw.enter(&a_key()).unwrap();
        let line = "x".repeat(MESSAGE_MAX + 1);
        let _ = raw.socket.send(Frame::text(line));
        let ended = loop {
            match raw.socket.read() {
                Ok(_) => {}
                Err(e) => break e,
            }
        };
        // Reading gave up because the relay closed, not for want of anything to read.
        assert!(!crate::wire::would_wait(&ended), "{ended}");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_space_with_no_key_cannot_be_claimed_at_a_closed_root_without_the_token() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        // A space from before keys: lines and no key.
        fs::create_dir_all(spaces.join("main")).unwrap();
        write(&spaces.join("main"), "dev-a", "a1\n");
        let token = names::close_root(&config).unwrap();
        let address = relay_in(&spaces, &config, None);
        assert_eq!(
            Raw::knock(&address, "/1/main", None)
                .challenged()
                .unwrap_err(),
            "main has no key on the relay yet, and keeping one needs the relay's token"
        );
        assert!(!spaces.join("main").join(AUTH_FILE).exists());
        assert_eq!(enters(&address, "/1/main", Some(&token), &a_key()), Ok(()));
        // Once it has its key, the token is not asked for.
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_space_whose_key_file_was_deleted_while_open_is_claimed_only_with_the_token() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        let token = names::close_root(&config).unwrap();
        let address = relay_in(&spaces, &config, None);
        // Held open by a device that stays.
        let mut held = Raw::knock(&address, "/1/main", Some(&token));
        held.enter(&a_key()).unwrap();
        fs::remove_file(spaces.join("main").join(AUTH_FILE)).unwrap();

        let stranger = Key::from_bytes([9; 32]);
        assert_eq!(
            Raw::knock(&address, "/1/main", None)
                .challenged()
                .unwrap_err(),
            "main has no key on the relay yet, and keeping one needs the relay's token"
        );
        assert_eq!(enters(&address, "/1/main", Some(&token), &stranger), Ok(()));
        let kept = fs::read_to_string(spaces.join("main").join(AUTH_FILE)).unwrap();
        assert_eq!(Key::decode(&kept), Some(stranger.auth()));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_connection_that_says_nothing_is_let_go_within_a_greeting() {
        let root = temp_dir();
        let address = relay(&root);
        let mut silent = TcpStream::connect(&address).unwrap();
        silent
            .set_read_timeout(Some(GREETING + Duration::from_secs(1)))
            .unwrap();
        let began = Instant::now();
        let mut got = [0u8; 1];
        // Nothing comes back, and the relay closes: a read of nothing, not a timeout.
        assert_eq!(silent.read(&mut got).unwrap(), 0);
        assert!(began.elapsed() <= GREETING + Duration::from_secs(1));
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn no_more_than_so_many_connections_are_in_their_greeting_at_once() {
        let root = temp_dir();
        let relay =
            Relay::bind_with(root.clone(), config_of(&root), "127.0.0.1:0", None, None).unwrap();
        let address = |n: usize| IpAddr::V4(Ipv4Addr::new(10, 0, (n >> 8) as u8, n as u8));
        let held: Vec<Greeting> = (0..GREETINGS_MAX)
            .map(|n| relay.hub.take(address(n)).unwrap())
            .collect();
        assert!(relay.hub.take(address(GREETINGS_MAX)).is_none());
        drop(held);
        assert!(relay.hub.take(address(0)).is_some());
        assert_eq!(relay.hub.greetings().all, 0, "every place given back");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn one_address_holding_its_share_of_the_greeting_does_not_keep_another_out() {
        let root = temp_dir();
        let relay =
            Relay::bind_with(root.clone(), config_of(&root), "127.0.0.1:0", None, None).unwrap();
        let hub = Arc::clone(&relay.hub);
        let address = relay.address().unwrap().to_string();
        thread::spawn(move || relay.serve());

        let busy = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
        let held: Vec<Greeting> = (0..GREETINGS_FROM_ONE)
            .map(|_| hub.take(busy).unwrap())
            .collect();
        assert!(hub.take(busy).is_none(), "that address has had its share");
        // A device from another address is still greeted, and let in.
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));
        drop(held);
        assert!(hub.take(busy).is_some());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_key_file_is_whole_or_not_there_and_leaves_nothing_beside_it() {
        let root = temp_dir();
        let address = relay(&root);
        let space = root.join("main");
        let readers = {
            let space = space.clone();
            thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(2);
                while Instant::now() < deadline {
                    // Whenever it is there, it holds a key.
                    assert!(kept_auth(&space).is_ok());
                }
            })
        };
        for _ in 0..5 {
            let _ = enters(&address, "/1/main", None, &a_key());
        }
        readers.join().unwrap();
        let kept = fs::read_to_string(space.join(AUTH_FILE)).unwrap();
        assert_eq!(Key::decode(&kept), Some(a_key().auth()));
        let beside: Vec<_> = fs::read_dir(&space)
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|name| name.starts_with(".auth."))
            .collect();
        assert!(beside.is_empty(), "{beside:?}");
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_root_token_file_that_cannot_be_read_keeps_nobody_out_of_a_space_that_is_there() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        let address = relay_in(&spaces, &config, None);
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));
        fs::create_dir_all(names::dir(&config)).unwrap();
        fs::write(names::dir(&config).join("token"), "garbled").unwrap();
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));
        // Making a space does need it read.
        assert_eq!(
            Raw::knock(&address, "/1/garden", None)
                .challenged()
                .unwrap_err(),
            "the relay's token cannot be read"
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_claim_sent_for_a_space_that_has_its_key_is_refused() {
        let root = temp_dir();
        let address = relay(&root);
        assert_eq!(enters(&address, "/1/main", None, &a_key()), Ok(()));
        let mut raw = Raw::knock(&address, "/1/main", None);
        let (_, claimed, _) = raw.challenged().unwrap();
        assert!(claimed);
        raw.say(&Message::Claim(a_key().auth().encode()));
        assert_eq!(
            raw.outcome(),
            Err("the answer is not the one the challenge asked for".into())
        );
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn no_line_of_the_relays_log_holds_the_token_offered() {
        let root = temp_dir();
        let config = root.join("config");
        let spaces = root.join("spaces");
        let token = names::add(&config, &spaces, "joe").unwrap();
        let relay =
            Relay::bind_with(spaces.clone(), config.clone(), "127.0.0.1:0", None, None).unwrap();
        let wrong = "0123456789abcdef0123456789abcdef";
        fs::write(names::dir(&config).join("token"), "garbled").unwrap();
        for (name, space, offered) in [
            (Some("joe"), "main", wrong),
            (Some("nobody"), "main", token.as_str()),
            (None, "joe", token.as_str()),
            (None, "garden", token.as_str()),
        ] {
            let asked = Asked {
                name: name.map(str::to_owned),
                space: space.to_owned(),
                token: Some(offered.to_owned()),
            };
            let label = asked.label();
            let Err(refusal) = admit(&relay.hub, &asked, &label) else {
                panic!("{label} was let in");
            };
            let logged = refusal.logged("127.0.0.1:4000", &label);
            assert!(!logged.contains(offered), "{logged}");
            assert!(!logged.contains(&hex_of(&names::hash(offered))), "{logged}");
            let said = refusal.said.unwrap_or_default();
            assert!(!said.contains(offered), "{said}");
        }
        fs::remove_dir_all(&root).unwrap();
    }

    fn hex_of(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
