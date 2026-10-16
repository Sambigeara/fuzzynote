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
//! A relay also hands out the web app, so that a browser has one address to open: the page
//! comes from the relay and connects back to it. The page is built into fzn, so it and the
//! policy it is served under are always of one release; `--web` points the relay at a directory
//! instead, read again for every request, which is how the page is worked on. Every answer names
//! the page's build, and `index.html` carries it, so a tab can tell when the relay has a page
//! newer than the one it runs. A built-in file is sent with a tag the browser can ask again by,
//! answered `304` with nothing, and gzipped when the browser takes it.

use std::borrow::Cow;
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
use crate::web_files;
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
    /// Where the page's files are read from, when a directory was given with --web.
    web: Option<PathBuf>,
    /// The page served when no directory was given.
    page: Option<Page>,
    /// For a directory, its build as last worked out and when, worked out again at most once a
    /// second, so that a page being worked on is named anew within a second of a change.
    dir_build: Mutex<Option<(Instant, String)>>,
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
    /// Starts listening. Nothing is served until [`Relay::serve`]. The relay hands out the page
    /// built into fzn, or with a directory for `web`, the files in it.
    pub fn bind(root: PathBuf, listen: &str, web: Option<PathBuf>) -> Result<Self> {
        Self::bind_with(root, listen, web, Page::built_in())
    }

    fn bind_with(
        root: PathBuf,
        listen: &str,
        web: Option<PathBuf>,
        page: Option<Page>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(listen).with_context(|| format!("listen on {listen}"))?;
        Ok(Self {
            listener,
            hub: Arc::new(Hub {
                root,
                web,
                page,
                dir_build: Mutex::new(None),
                spaces: Mutex::new(HashMap::new()),
                refused: Mutex::new(HashSet::new()),
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
        let relay = Relay::bind_with(root.to_owned(), "127.0.0.1:0", None, page).unwrap();
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
}
