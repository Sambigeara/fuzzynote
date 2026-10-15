//! The relay: a replica of every space under a root that writes nothing of its own, and the
//! pages of the web app if it is given them.
//!
//! A relay is the only writer of its directories while it runs. Each space is locked when the
//! first connection asks for it, read whole once, and from then on its files are only ever
//! what the replica asked to be kept, so nothing is reread and nothing is locked per write.
//! Anything else that wants to write there, an fzn opened on the relay's root or a second relay,
//! is kept out by the lock, and a connection asking a relay for a space held elsewhere is
//! refused. If writing a space fails, the space is closed and its connections dropped: each
//! device connects again and the space is opened again from what is on disk.
//!
//! A relay can also hand out the pages of the web app, from a directory it is pointed at, so
//! that a browser has one address to open: the page comes from the relay and connects back to
//! it.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{self, Sender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::replica::{Message, Out, Peer, Replica, names_a_log};
use tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tungstenite::http::StatusCode;

use crate::system;
use crate::wire::{GREETING, VERSION, Wire};

/// The file whose lock says which process writes a space's directory.
const LOCK_FILE: &str = ".lock";
/// What a page of the web app may do: load its own files and its own WebAssembly, talk to a
/// relay, and use inline styles, because the body's editor injects its stylesheet. No script
/// from anywhere else; and the page writes nothing into itself as markup, which is its own
/// practice, since allowing inline styles means the policy no longer holds it to that.
const PAGE_POLICY: &str = "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; \
    style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; img-src 'self' data:; \
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

struct Hub {
    root: PathBuf,
    /// Where the pages of the web app are, if this relay hands them out.
    web: Option<PathBuf>,
    spaces: Mutex<HashMap<String, Space>>,
    /// Spaces held by another process, said once each.
    refused: Mutex<HashSet<String>>,
    peers: AtomicU32,
}

impl Hub {
    fn spaces(&self) -> MutexGuard<'_, HashMap<String, Space>> {
        self.spaces.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Opens a space if it is not open, or says why a connection asking for it is refused.
    fn open(&self, name: &str) -> Result<(), StatusCode> {
        let mut spaces = self.spaces();
        if spaces.contains_key(name) {
            return Ok(());
        }
        let mut refused = self.refused.lock().unwrap_or_else(PoisonError::into_inner);
        match Space::open(name, &self.root.join(name)) {
            Ok(Some(space)) => {
                refused.remove(name);
                spaces.insert(name.to_owned(), space);
                Ok(())
            }
            Ok(None) => {
                if refused.insert(name.to_owned()) {
                    eprintln!(
                        "fzn relay: {name} is held by another process, so devices asking for it \
                         are turned away"
                    );
                }
                Err(StatusCode::LOCKED)
            }
            Err(why) => {
                eprintln!("fzn relay: {name}: {why:#}");
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
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
    /// Starts listening. Nothing is served until [`Relay::serve`]. With a directory for `web`,
    /// the relay hands out the files in it to a browser that asks.
    pub fn bind(root: PathBuf, listen: &str, web: Option<PathBuf>) -> Result<Self> {
        let listener = TcpListener::bind(listen).with_context(|| format!("listen on {listen}"))?;
        Ok(Self {
            listener,
            hub: Arc::new(Hub {
                root,
                web,
                spaces: Mutex::new(HashMap::new()),
                refused: Mutex::new(HashSet::new()),
                peers: AtomicU32::new(1),
            }),
        })
    }

    pub fn address(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Serves until the process ends.
    pub fn serve(self) -> Result<()> {
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

/// Whether a request asks for a websocket, and not for a page.
fn asks_for_a_socket(head: &str) -> bool {
    head.lines().skip(1).any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.trim().eq_ignore_ascii_case("upgrade")
                && value.trim().eq_ignore_ascii_case("websocket")
        })
    })
}

/// The file of the web app that a request names, if it names one plainly: every part of its
/// path a plain name, as a log's is, so that nothing outside the directory can be asked for.
fn file_asked_for(web: &Path, target: &str) -> Option<PathBuf> {
    let path = target.split(['?', '#']).next()?.strip_prefix('/')?;
    if path.is_empty() {
        return Some(web.join("index.html"));
    }
    let mut file = web.to_owned();
    for part in path.split('/') {
        if !names_a_log(part) {
            return None;
        }
        file.push(part);
    }
    Some(file)
}

/// What kind of thing a file is, for the browser, going by its name.
fn kind_of(file: &Path) -> &'static str {
    match file.extension().and_then(|x| x.to_str()) {
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
    let found = hub
        .web
        .as_deref()
        .filter(|_| method == "GET")
        .and_then(|web| file_asked_for(web, target))
        .and_then(|file| fs::read(&file).ok().map(|body| (kind_of(&file), body)));
    let plain = "text/plain; charset=utf-8";
    let (status, kind, body) = match found {
        Some((kind, body)) => ("200 OK", kind, body),
        None if method != "GET" => ("405 Method Not Allowed", plain, b"only GET\n".to_vec()),
        None => ("404 Not Found", plain, b"nothing here\n".to_vec()),
    };
    let answer = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\n\
         Cache-Control: no-cache\r\nX-Content-Type-Options: nosniff\r\n\
         Referrer-Policy: no-referrer\r\nContent-Security-Policy: {PAGE_POLICY}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(answer.as_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

/// One connection to a relay, from its greeting to its end.
// The refusal a greeting can end in is the websocket library's type, and it is a large one.
#[allow(clippy::result_large_err)]
fn serve(hub: &Hub, stream: TcpStream) -> Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(GREETING))?;
    stream.set_write_timeout(Some(GREETING))?;
    let head = opening(&stream)?;
    if !asks_for_a_socket(&String::from_utf8_lossy(&head)) {
        return page(hub, stream, &head);
    }
    // The space is opened before the greeting is answered, so that a device asking for one that
    // is held elsewhere is refused, and not greeted and then dropped.
    let mut name = String::new();
    let socket =
        tungstenite::accept_hdr(
            stream,
            |request: &Request, response: Response| match asked_for(request.uri().path())
                .and_then(|space| hub.open(space).map(|()| space))
            {
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
    // The relay's clock is said first, so that a device can say when its own is far off.
    wire.say(Message::Now(system::now()).encode()?)?;

    let peer = hub.peers.fetch_add(1, Ordering::Relaxed);
    let (to, waiting) = mpsc::channel();
    // If the space was closed since it was opened above, `to` is dropped here and the
    // connection ends below, to be made again.
    hub.with(&name, |space| {
        space.peers.insert(peer, to);
        let out = space.replica.connect(peer);
        space.carry_out(out)
    });

    let ended = (|| -> Result<()> {
        loop {
            if let Some(text) = wire.hear()?
                && let Some(message) = Message::decode(&text)
            {
                hub.with(&name, |space| {
                    let out = space.replica.receive(peer, message);
                    space.carry_out(out)
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

    /// A relay on a port of its own choosing, serving a root, and where to find it.
    pub(crate) fn relay(root: &Path) -> String {
        relay_with(root, None)
    }

    pub(crate) fn relay_with(root: &Path, web: Option<PathBuf>) -> String {
        let relay = Relay::bind(root.to_owned(), "127.0.0.1:0", web).unwrap();
        let address = relay.address().unwrap().to_string();
        thread::spawn(move || relay.serve());
        address
    }

    /// A device as a relay sees one: a replica on the end of a link, holding its logs in
    /// memory, with what it has been told.
    pub(crate) struct Client {
        replica: Replica,
        link: Link,
        connected: bool,
        forked: Vec<String>,
        unreachable: Vec<String>,
    }

    impl Client {
        pub(crate) fn start(relay: &str, space: &str) -> Self {
            Self {
                replica: Replica::new(),
                link: start(relay.to_owned(), space.to_owned()),
                connected: false,
                forked: Vec::new(),
                unreachable: Vec::new(),
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
                    Event::Message(text) => Message::decode(&text)
                        .map_or_else(Vec::new, |message| self.replica.receive(0, message)),
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

        fn connects(&mut self) {
            assert!(eventually(|| {
                self.hear();
                self.connected
            }));
        }
    }

    #[test]
    fn a_connection_asks_for_a_space_by_the_version_and_its_name() {
        assert_eq!(asked_for("/1/main"), Ok("main"));
        assert_eq!(asked_for("/1/fzn-2022"), Ok("fzn-2022"));
        assert_eq!(asked_for("/2/main"), Err(StatusCode::NOT_IMPLEMENTED));
        for path in ["/", "/main", "/1/", "/1/main/more", "/1/..", "/1/a b"] {
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

        let mut b = Client::start(&second, "main");
        b.write("dev-b", "b1\n");
        assert!(eventually(|| {
            b.hear();
            !b.unreachable.is_empty()
        }));
        thread::sleep(Duration::from_secs(3));
        b.hear();
        assert_eq!(
            b.unreachable.len(),
            1,
            "and is not let in when it tries again, which is said once"
        );
        assert!(!b.connected);
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

        // A relay that was given no pages has none to hand out.
        let bare = relay(&hub.join("elsewhere-root"));
        assert!(get(&bare, "/").0.starts_with("HTTP/1.1 404"));
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
}
