//! The terminal's socket: a websocket to a relay, held for a window by a thread of its own, with
//! text passed to and from the app over channels. The relay is in `relay`, and the websocket both
//! ends hold is in `wire`.
//!
//! The app's device decides what to say and what to make of what is heard; this thread only
//! carries it, and dials again when the connection is lost. Nothing said while there is no
//! connection is kept for the next one. A device says what it holds whenever it connects, and
//! the relay sends it the rest, so whatever was dropped is worked out again then.
//!
//! A command that reads or writes a space while no window here keeps it in step has one
//! exchange of its own instead, [`Once`], carried on the command's own thread, which starts no
//! other, since the command does nothing else meanwhile: dialled once, carried until the device
//! holds what the relay holds, and closed once what the command wrote has gone. It is never
//! dialled again; a relay away is said, and the command goes on with this machine's copy.
//!
//! A relay's address may carry a token, `ws://NAME:TOKEN@host`, which is what lets this device
//! make a space there. It is read by a URI parser rather than by hand, so that an address that
//! is not one is refused and not half read, and it goes no further than the handshake: offered
//! as a subprotocol, never in the path, never in what the status row says, and never in an error,
//! which names the address with what comes before its `@` left out.

use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::replica::names_a_log;
use fzn_engine::{Device, RELAY, Todo};
use tungstenite::ClientRequestBuilder;
use tungstenite::http::Uri;
use tungstenite::protocol::WebSocketConfig;

use crate::wire::{GREETING, MESSAGE_MAX, PORT, VERSION, Wire};

const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MOST: Duration = Duration::from_secs(30);
/// How long a token is, in hex, as `fzn relay add` prints it.
const TOKEN_LEN: usize = 32;

/// What the app hears from the thread that holds the socket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The relay is connected, and what is said to it from now on goes to it.
    Connected,
    /// The relay said this.
    Message(String),
    /// The connection is lost. The relay is dialled again.
    Disconnected,
    /// The relay cannot be reached, or would not have us, and why. Said once for each reason,
    /// however many times it is tried again.
    Unreachable(String),
}

/// The app's ends of the channels to and from the socket.
pub struct Link {
    pub to_relay: Sender<String>,
    pub from_relay: Receiver<Event>,
}

/// Where to dial and as whom, from a relay's address as it is given.
#[derive(Clone, PartialEq, Eq)]
pub struct Dial {
    /// The host and port to connect to.
    pub authority: String,
    /// The host as it was given, and its port if one was: all the status row says of a relay.
    pub shown: String,
    /// The name on the relay whose spaces are asked for, or none for those at its root.
    pub name: Option<String>,
    /// What lets this device make a space on the relay.
    pub token: Option<String>,
}

/// A relay's address as it is given, `ws://NAME:TOKEN@host:port`, `ws://TOKEN@host`,
/// `ws://host:port`, `host:port` or `host`, as where to dial and as whom.
pub fn dial_of(relay: &str) -> Result<Dial> {
    let relay = relay.trim();
    let said = unnamed(relay);
    if relay.starts_with("wss://") {
        bail!("wss is not built yet: use ws:// over a network you trust");
    }
    let not = || anyhow!("{said} is not the address of a relay");
    let uri: Uri = if relay.contains("://") {
        relay.parse()
    } else {
        format!("ws://{relay}").parse()
    }
    .map_err(|_| not())?;
    let whole =
        uri.scheme_str() == Some("ws") && matches!(uri.path(), "" | "/") && uri.query().is_none();
    let authority = uri.authority().filter(|_| whole).ok_or_else(not)?;
    let host = authority.host();
    if host.is_empty() {
        return Err(not());
    }
    let (name, token) = match authority.as_str().rsplit_once('@') {
        None => (None, None),
        Some((given, _)) => match given.split_once(':') {
            Some((name, token)) => (Some(name), Some(token)),
            None => (None, Some(given)),
        },
    };
    if name.is_none() && token.is_some_and(|token| !token.bytes().all(|b| b.is_ascii_hexdigit())) {
        bail!("{said}: a name comes with its token, as ws://NAME:TOKEN@host");
    }
    if let Some(name) = name.filter(|name| !names_a_log(name)) {
        bail!("{said}: {name:?} is not a name a relay knows");
    }
    let token = match token {
        Some(token) if token.len() == TOKEN_LEN && token.bytes().all(|b| b.is_ascii_hexdigit()) => {
            Some(token.to_ascii_lowercase())
        }
        Some(_) => bail!("{said}: a token is {TOKEN_LEN} hex characters, as fzn relay add says it"),
        None => None,
    };
    // A port that is given has to be one: `http` reads one past 65535 as none at all.
    let given_port = authority.as_str().rsplit('@').next().unwrap_or_default();
    let given_port = given_port
        .rsplit_once(':')
        .map(|(_, port)| port)
        .filter(|port| !port.contains(']'));
    if given_port.is_some_and(|port| port.parse::<u16>().is_err()) {
        return Err(not());
    }
    let port = authority.port_u16();
    Ok(Dial {
        authority: format!("{host}:{}", port.unwrap_or(PORT)),
        shown: match port {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        },
        name: name.map(str::to_owned),
        token,
    })
}

/// Said as it would be shown, the token as `…`, so that printing a dial never says one.
impl std::fmt::Debug for Dial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dial")
            .field("authority", &self.authority)
            .field("shown", &self.shown)
            .field("name", &self.name)
            .field("token", &self.token.as_ref().map(|_| "…"))
            .finish()
    }
}

/// A relay's address with whatever comes before its `@` left out, so that a token is never said.
fn unnamed(relay: &str) -> String {
    match relay.rsplit_once('@') {
        None => relay.to_owned(),
        Some((before, after)) => match before.split_once("://") {
            Some((scheme, _)) => format!("{scheme}://…@{after}"),
            None => format!("…@{after}"),
        },
    }
}

/// Connects to a relay and asks it for a space: at `/1/NAME/space` when the address names whose
/// spaces, else at `/1/space`, offering the token, when there is one, beside fzn's own
/// subprotocol. The whole of it, the name looked up, each of its addresses tried and the
/// handshake, has one [`GREETING`], so that a relay away costs a command no more than that. The
/// system's lookup cannot be told how long it has, so it is asked on a thread, which is left to
/// finish on its own when it takes too long.
pub fn dial(to: &Dial, space: &str) -> Result<Wire> {
    let authority = &to.authority;
    let deadline = Instant::now() + GREETING;
    let late = || anyhow!("no answer within {} seconds", GREETING.as_secs());
    let left = || {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
    };
    let (found, addresses) = mpsc::channel();
    let asked = authority.clone();
    thread::spawn(move || {
        let _ = found.send(asked.to_socket_addrs().map(Vec::from_iter));
    });
    let addresses = addresses
        .recv_timeout(left().unwrap_or_default())
        .map_err(|_| late())
        .and_then(|found| found.map_err(anyhow::Error::from))
        .with_context(|| format!("look up {authority}"))?;
    let mut failure = anyhow!("{authority} has no address");
    let mut stream = None;
    for address in addresses {
        let Some(left) = left() else {
            failure = late();
            break;
        };
        match TcpStream::connect_timeout(&address, left) {
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
    let left = left().ok_or_else(late)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(left))?;
    stream.set_write_timeout(Some(left))?;
    let path = match &to.name {
        Some(name) => format!("/{VERSION}/{name}/{space}"),
        None => format!("/{VERSION}/{space}"),
    };
    let uri: Uri = format!("ws://{authority}{path}").parse()?;
    let mut request = ClientRequestBuilder::new(uri);
    if let Some(token) = &to.token {
        request = request
            .with_sub_protocol("fzn")
            .with_sub_protocol(format!("fzn.token.{token}"));
    }
    let config = WebSocketConfig::default()
        .max_message_size(Some(MESSAGE_MAX))
        .max_frame_size(Some(MESSAGE_MAX));
    let (socket, _) = tungstenite::client::client_with_config(request, stream, Some(config))
        .map_err(|e| anyhow!("{e}"))?;
    Wire::new(socket)
}

/// Holds a connection to a relay for a space, for as long as the app keeps its end of the
/// link: dials, carries text both ways, and dials again when the connection is lost.
pub fn start(relay: String, space: String) -> Link {
    let (to_relay, said) = mpsc::channel();
    let (heard, from_relay) = mpsc::channel();
    thread::spawn(move || hold(&relay, &space, &said, &heard));
    Link {
        to_relay,
        from_relay,
    }
}

/// How a connection that was made came to end.
enum Ended {
    /// It was lost, and is to be made again.
    Lost,
    /// The app has gone, and so has the reason for it.
    AppGone,
}

fn hold(relay: &str, space: &str, said: &Receiver<String>, heard: &Sender<Event>) {
    let to = match dial_of(relay) {
        Ok(to) => to,
        Err(why) => {
            let _ = heard.send(Event::Unreachable(format!("{why:#}")));
            return;
        }
    };
    // A space is a directory on the relay and part of the address it is asked for by.
    if !names_a_log(space) {
        let why = format!("a space has to be plainly named to sync, and {space:?} is not");
        let _ = heard.send(Event::Unreachable(why));
        return;
    }
    let mut told: Option<String> = None;
    let mut wait = RETRY_FIRST;
    loop {
        match dial(&to, space) {
            Ok(mut wire) => {
                let began = Instant::now();
                told = None;
                // What was said for a connection that is gone is not for this one.
                while said.try_recv().is_ok() {}
                if heard.send(Event::Connected).is_err() {
                    return;
                }
                let ended = carry(&mut wire, said, heard);
                if heard.send(Event::Disconnected).is_err() {
                    return;
                }
                if let Ended::AppGone = ended {
                    return;
                }
                // A connection the relay ends within its greeting, refusing this device say, is
                // waited on as one that was never made, so that it is not asked again each second.
                // Twice the greeting, since a refusal comes at its end and the handshake before
                // it takes a moment of its own.
                if began.elapsed() >= 2 * GREETING {
                    wait = RETRY_FIRST;
                }
            }
            Err(why) => {
                let why = format!("{why:#}");
                if told.as_ref() != Some(&why) {
                    if heard.send(Event::Unreachable(why.clone())).is_err() {
                        return;
                    }
                    told = Some(why);
                }
            }
        }
        thread::sleep(wait);
        wait = (wait * 2).min(RETRY_MOST);
    }
}

/// Carries text both ways until the connection or the app goes.
fn carry(wire: &mut Wire, said: &Receiver<String>, heard: &Sender<Event>) -> Ended {
    loop {
        loop {
            match said.try_recv() {
                Ok(text) => {
                    if wire.say(text).is_err() {
                        return Ended::Lost;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ended::AppGone,
            }
        }
        if wire.keep_up().is_err() {
            return Ended::Lost;
        }
        match wire.hear() {
            Ok(Some(text)) => {
                if heard.send(Event::Message(text)).is_err() {
                    return Ended::AppGone;
                }
            }
            Ok(None) => {}
            Err(_) => return Ended::Lost,
        }
    }
}

/// One exchange with a relay, as a command has it when no window here keeps the space in step:
/// dialled, carried until this device holds what the relay holds, and closed once what the
/// command then wrote has gone.
pub struct Once {
    wire: Wire,
    shown: String,
}

impl Once {
    /// Dials the relay and carries messages both ways until the device is in step with it,
    /// keeping each [`Todo`] with `keep` and sending what it has for the relay. What could not be
    /// done with the relay is the inner error, said as `not synced with HOST: why`, and leaves
    /// the device as it was or further on, never further back; the outer is what `keep` failed
    /// with, which is the directory's trouble and not the relay's.
    pub fn start(
        to: &Dial,
        space: &str,
        device: &mut Device,
        mut keep: impl FnMut(&Todo) -> Result<()>,
    ) -> Result<Result<Once, String>> {
        let not = |why: &str| Err(format!("not synced with {}: {why}", to.shown));
        // A space is a directory on the relay and part of the address it is asked for by.
        if !names_a_log(space) {
            return Ok(not(&format!(
                "a space has to be plainly named to sync, and {space:?} is not"
            )));
        }
        let wire = match dial(to, space) {
            Ok(wire) => wire,
            Err(why) => return Ok(not(&format!("{why:#}"))),
        };
        let mut once = Once {
            wire,
            shown: to.shown.clone(),
        };
        let todo = device.connected(RELAY);
        keep(&todo)?;
        if once.send(&todo).is_err() {
            return Ok(not(LOST));
        }
        let mut heard = Instant::now();
        loop {
            if let Some(why) = device.refused() {
                return Ok(not(why));
            }
            if device.in_step() {
                return Ok(Ok(once));
            }
            if heard.elapsed() > GREETING {
                return Ok(not(&format!(
                    "nothing was heard from it for {} seconds",
                    GREETING.as_secs()
                )));
            }
            if once.wire.keep_up().is_err() {
                return Ok(not(LOST));
            }
            match once.wire.hear() {
                Ok(Some(text)) => {
                    heard = Instant::now();
                    let todo = device.received(RELAY, &text);
                    keep(&todo)?;
                    if once.send(&todo).is_err() {
                        return Ok(not(LOST));
                    }
                }
                Ok(None) => {}
                Err(_) => return Ok(not(LOST)),
            }
        }
    }

    /// Puts what a [`Todo`] has for the relay in line to go.
    fn send(&mut self, todo: &Todo) -> Result<()> {
        for said in todo.send.iter().filter(|said| said.to == RELAY) {
            self.wire.say(said.text.clone())?;
        }
        Ok(())
    }

    /// Sends what the command wrote, and whatever else is in line, and closes, giving the relay a
    /// greeting to hear it all. Says why not, when the connection went first or the relay did not
    /// close in that time: what was written is kept here all the same, and goes at the next sync.
    pub fn close(mut self, todo: &Todo) -> Option<String> {
        let sent = self.send(todo).and_then(|()| self.wire.keep_up()).is_ok();
        let why = match (sent, self.wire.close()) {
            (true, true) => return None,
            (false, _) => LOST.to_owned(),
            (true, false) => format!("it did not close within {} seconds", GREETING.as_secs()),
        };
        Some(format!(
            "not synced with {}: {why}, so what was written here goes at the next sync",
            self.shown
        ))
    }
}

/// Why a command's exchange with a relay ended, when the connection went before it was done.
const LOST: &str = "the connection was lost";

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    const TOKEN: &str = "3f9a01c23f9a01c23f9a01c23f9a01c2";

    fn authority(relay: &str) -> Result<String> {
        dial_of(relay).map(|dial| dial.authority)
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
        assert!(authority("ws://nas.local/1/main").is_err());
        assert!(authority("").is_err());
    }

    #[test]
    fn a_relay_is_given_with_or_without_whom_and_only_its_host_is_said() {
        let named = dial_of(&format!("ws://joe:{TOKEN}@nas.local:4000")).unwrap();
        assert_eq!(
            named,
            Dial {
                authority: "nas.local:4000".into(),
                shown: "nas.local:4000".into(),
                name: Some("joe".into()),
                token: Some(TOKEN.into()),
            }
        );
        assert!(!format!("{named:?}").contains(TOKEN), "nor printed");
        let root = dial_of(&format!("ws://{}@nas.local", TOKEN.to_uppercase())).unwrap();
        assert_eq!((root.name, root.token.as_deref()), (None, Some(TOKEN)));
        assert_eq!(
            (root.authority.as_str(), root.shown.as_str()),
            ("nas.local:3960", "nas.local")
        );
        let bare = dial_of("nas.local").unwrap();
        assert_eq!((bare.name, bare.token), (None, None));

        for (relay, why) in [
            (
                "ws://joe:3f9a@nas.local".to_owned(),
                "a token is 32 hex characters",
            ),
            (
                format!("ws://a b:{TOKEN}@nas.local"),
                "is not the address of a relay",
            ),
            (
                "ws://nas.local:99999".to_owned(),
                "is not the address of a relay",
            ),
            (
                "ws://joe@nas.local".to_owned(),
                "a name comes with its token",
            ),
            (
                format!("ws://:{TOKEN}@nas.local"),
                "is not a name a relay knows",
            ),
            (
                format!("wss://joe:{TOKEN}@nas.local"),
                "wss is not built yet",
            ),
        ] {
            let said = dial_of(&relay).unwrap_err().to_string();
            assert!(said.contains(why), "{said}");
            assert!(!said.contains(TOKEN) && !said.contains("3f9a@"), "{said}");
        }
    }

    #[test]
    fn a_space_that_cannot_be_asked_for_is_said_and_not_dialled() {
        let link = start("nas.local".into(), "a b".into());
        let Ok(Event::Unreachable(why)) = link.from_relay.recv_timeout(Duration::from_secs(5))
        else {
            panic!("nothing said");
        };
        assert!(why.contains("plainly named"), "{why}");
    }

    /// A relay that takes every connection, answers its greeting and ends it at once, as one
    /// that refuses a device does, and counts them.
    fn refusing() -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (asked, heard) = mpsc::channel();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut path = String::new();
                // The error it may give is tungstenite's own answer to a handshake, large as it is.
                #[allow(clippy::result_large_err)]
                let answer = |request: &tungstenite::handshake::server::Request, response| {
                    path = request.uri().path().to_owned();
                    let offered = request.headers().get("Sec-WebSocket-Protocol");
                    let mut response: tungstenite::handshake::server::Response = response;
                    if offered.is_some() {
                        let fzn = "fzn".parse().unwrap();
                        response.headers_mut().insert("Sec-WebSocket-Protocol", fzn);
                    }
                    Ok(response)
                };
                if let Ok(mut socket) = tungstenite::accept_hdr(stream, answer) {
                    let _ = socket.close(None);
                    let _ = socket.flush();
                }
                if asked.send(path).is_err() {
                    return;
                }
            }
        });
        (address, heard)
    }

    #[test]
    fn a_connection_the_relay_ends_at_once_does_not_shorten_the_wait() {
        let (address, asked) = refusing();
        let link = start(format!("ws://joe:{TOKEN}@{address}"), "main".into());
        let began = Instant::now();
        let mut paths = Vec::new();
        while began.elapsed() < Duration::from_millis(4_500) {
            if let Ok(path) = asked.recv_timeout(Duration::from_millis(100)) {
                paths.push(path);
            }
        }
        drop(link);
        // Dialled at once, after a second and after two more; a wait put back to a second each
        // time would have dialled five times by now.
        assert_eq!(paths.len(), 3, "{paths:?}");
        assert!(paths.iter().all(|path| path == "/1/joe/main"), "{paths:?}");
    }
}
