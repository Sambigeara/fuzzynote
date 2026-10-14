//! The terminal's one socket: a websocket to a relay, held by a thread of its own, with text
//! passed to and from the app over channels. The relay is in `relay`, and the websocket both
//! ends hold is in `wire`.
//!
//! The app's device decides what to say and what to make of what is heard; this thread only
//! carries it, and dials again when the connection is lost. Nothing said while there is no
//! connection is kept for the next one. A device says what it holds whenever it connects, and
//! the relay sends it the rest, so whatever was dropped is worked out again then.

use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::replica::names_a_log;

use crate::wire::{GREETING, PORT, VERSION, Wire};

const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MOST: Duration = Duration::from_secs(30);

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
    let authority = match authority(relay) {
        Ok(authority) => authority,
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
        match dial(&authority, space) {
            Ok(mut wire) => {
                told = None;
                wait = RETRY_FIRST;
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn a_space_that_cannot_be_asked_for_is_said_and_not_dialled() {
        let link = start("nas.local".into(), "a b".into());
        let Ok(Event::Unreachable(why)) = link.from_relay.recv_timeout(Duration::from_secs(5))
        else {
            panic!("nothing said");
        };
        assert!(why.contains("plainly named"), "{why}");
    }
}
