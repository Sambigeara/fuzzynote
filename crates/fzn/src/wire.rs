//! The websocket between a device and a relay, as both ends hold it.
//!
//! Each end of a connection is one thread that listens and speaks by turns, so its socket may
//! never wait long for either, and the same few numbers have to hold at both ends: how long a
//! turn is, how long a greeting may take, and when a quiet peer is pinged or given up on.

use std::io::ErrorKind;
use std::net::TcpStream;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tungstenite::{Message as Frame, WebSocket};

/// The port a relay listens on when it is not told another.
pub const PORT: u16 = 3960;
/// The version of the exchange. A connection names it first in its path, then the space.
pub const VERSION: &str = "1";
/// How long a socket waits for something to read or for room to write. Each end of a
/// connection is one thread that listens and speaks by turns, so neither may wait long.
pub const TICK: Duration = Duration::from_millis(50);
/// How long a connection has to be made and greeted.
pub const GREETING: Duration = Duration::from_secs(5);
pub const PING: Duration = Duration::from_secs(15);
/// The most one message may hold, at either end, so that a peer cannot make the other set aside
/// more than this for it. Lines go in batches far smaller, and a line too long for a batch goes
/// alone, so only a single note larger than this could not sync.
pub const MESSAGE_MAX: usize = 8 << 20;
/// A peer not heard from for this long is taken to be gone.
pub const SILENCE: Duration = Duration::from_secs(45);

/// A websocket whose socket never waits long, so that one thread can listen and speak by turns.
pub struct Wire {
    socket: WebSocket<TcpStream>,
    heard: Instant,
    pinged: Instant,
}

/// Whether an error only says that the socket would have had to wait.
pub fn would_wait(error: &tungstenite::Error) -> bool {
    matches!(error, tungstenite::Error::Io(e)
        if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut))
}

impl Wire {
    pub fn new(socket: WebSocket<TcpStream>) -> Result<Self> {
        socket.get_ref().set_read_timeout(Some(TICK))?;
        socket.get_ref().set_write_timeout(Some(TICK))?;
        Ok(Self {
            socket,
            heard: Instant::now(),
            pinged: Instant::now(),
        })
    }

    /// The next message, if one comes within a tick. An error means the connection is over.
    pub fn hear(&mut self) -> Result<Option<String>> {
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
    pub fn say(&mut self, text: String) -> Result<()> {
        match self.socket.write(Frame::text(text)) {
            Err(e) if !would_wait(&e) => Err(e.into()),
            _ => Ok(()),
        }
    }

    /// Sends what is waiting to go, then closes, giving the peer a greeting's time to hear it,
    /// and says whether the peer closed in that time. Any end of the connection counts, since a
    /// relay, this one and those before it, drops the socket once it reads the close rather than
    /// answering it, and has read everything sent before the close by then all the same.
    pub fn close(mut self) -> bool {
        let deadline = Instant::now() + GREETING;
        let _ = self.socket.close(None);
        // Reading drives the close: it sends what is queued and ends when the peer closes too.
        while Instant::now() < deadline {
            match self.socket.read() {
                Err(e) if !would_wait(&e) => return true,
                _ => {}
            }
        }
        false
    }

    /// Sends on what is waiting to go, pings a quiet peer, and gives up on a silent one.
    pub fn keep_up(&mut self) -> Result<()> {
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
