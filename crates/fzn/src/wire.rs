//! The websocket between a device and a relay, as both ends hold it.
//!
//! Each end of a connection is one thread that listens and speaks by turns, so its socket may
//! never wait long for either, and the same few numbers have to hold at both ends: how long a
//! turn is, how long a greeting may take, and when a quiet peer is pinged or given up on.

use std::io::{self, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use rustls::{ClientConnection, StreamOwned};
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

/// What a websocket runs over: a TCP connection as it is, or under TLS, which a device speaks to
/// a relay behind a proxy that has it. The relay has no TLS of its own, so its end is always the
/// first. Either way the TCP connection is what waits, and what is told how long it may. It
/// notes when bytes last came, so that a peer in the middle of sending a large message, an
/// image over a slow link, is a peer being heard and not a silent one.
pub struct Stream {
    over: Over,
    heard: Instant,
}

enum Over {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Stream {
    pub fn plain(tcp: TcpStream) -> Self {
        Self {
            over: Over::Plain(tcp),
            heard: Instant::now(),
        }
    }

    pub fn tls(tls: StreamOwned<ClientConnection, TcpStream>) -> Self {
        Self {
            over: Over::Tls(Box::new(tls)),
            heard: Instant::now(),
        }
    }

    fn tcp(&self) -> &TcpStream {
        match &self.over {
            Over::Plain(tcp) => tcp,
            Over::Tls(tls) => tls.get_ref(),
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = match &mut self.over {
            Over::Plain(tcp) => tcp.read(buf),
            Over::Tls(tls) => tls.read(buf),
        };
        if matches!(read, Ok(n) if n > 0) {
            self.heard = Instant::now();
        }
        read
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.over {
            Over::Plain(tcp) => tcp.write(buf),
            Over::Tls(tls) => tls.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.over {
            Over::Plain(tcp) => tcp.flush(),
            Over::Tls(tls) => tls.flush(),
        }
    }
}

/// A websocket whose socket never waits long, so that one thread can listen and speak by turns.
pub struct Wire {
    socket: WebSocket<Stream>,
    pinged: Instant,
}

/// Whether an error only says that the socket would have had to wait.
pub fn would_wait(error: &tungstenite::Error) -> bool {
    matches!(error, tungstenite::Error::Io(e)
        if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut))
}

impl Wire {
    pub fn new(socket: WebSocket<Stream>) -> Result<Self> {
        socket.get_ref().tcp().set_read_timeout(Some(TICK))?;
        socket.get_ref().tcp().set_write_timeout(Some(TICK))?;
        Ok(Self {
            socket,
            pinged: Instant::now(),
        })
    }

    /// The next message, if one comes within a tick. An error means the connection is over.
    pub fn hear(&mut self) -> Result<Option<String>> {
        match self.socket.read() {
            Ok(Frame::Text(text)) => Ok(Some(text.as_str().to_owned())),
            Ok(Frame::Close(_)) => bail!("closed"),
            Ok(_) => Ok(None),
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
    /// and says whether the peer closed in that time. The peer's close answering it is the end,
    /// since a relay takes in each message whole before it reads the next, so its answer says it
    /// has everything sent before; the socket may stay open after it, as a proxy in front holds
    /// one. Any other end of the connection counts too, since a relay from before closes were
    /// answered drops the socket once it reads the close, having read the rest by then all the
    /// same.
    pub fn close(mut self) -> bool {
        let deadline = Instant::now() + GREETING;
        let _ = self.socket.close(None);
        // Reading drives the close: it sends what is queued and ends when the peer closes too.
        while Instant::now() < deadline {
            match self.socket.read() {
                Ok(Frame::Close(_)) => return true,
                Err(e) if !would_wait(&e) => return true,
                _ => {}
            }
        }
        false
    }

    /// Sends on what is waiting to go, as far as the socket takes it now, and says whether it all
    /// went: nothing is left waiting. An error means the connection is over.
    pub fn drained(&mut self) -> Result<bool> {
        match self.socket.flush() {
            Ok(()) => Ok(true),
            Err(e) if would_wait(&e) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Sends on what is waiting to go, pings a quiet peer, and gives up on a silent one: one from
    /// which no byte has come, whole message or not, for [`SILENCE`].
    pub fn keep_up(&mut self) -> Result<()> {
        if self.socket.get_ref().heard.elapsed() > SILENCE {
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

/// A connection let go is said to be over with a close, or the peer's close is answered, before
/// the socket goes. A proxy in front of a relay holds a socket dropped behind it open for seconds,
/// in which the other end cannot tell a connection ended from a quiet one, nor that a refusal was
/// quick; a close is passed on at once. Under TLS the TLS is closed too, after it. It is said in
/// the time a socket has to write, and only tried: a peer already gone needs nothing said.
impl Drop for Wire {
    fn drop(&mut self) {
        let _ = self.socket.close(None);
        let _ = self.socket.flush();
        if let Over::Tls(tls) = &mut self.socket.get_mut().over {
            tls.conn.send_close_notify();
            let _ = tls.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn a_peer_sending_a_message_slowly_is_heard_before_the_message_is_whole() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (go, went) = std::sync::mpsc::channel::<()>();
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tungstenite::accept(stream).unwrap();
            went.recv().unwrap();
            // The start of a text frame of a thousand bytes, and a tenth of them.
            let mut part = vec![0x81, 126, 0x03, 0xE8];
            part.extend_from_slice(&[b'x'; 100]);
            socket.get_mut().write_all(&part).unwrap();
            thread::sleep(GREETING);
        });
        let stream = TcpStream::connect(address).unwrap();
        let asked = format!("ws://{address}/");
        let (socket, _) = tungstenite::client(asked.as_str(), Stream::plain(stream)).unwrap();
        let mut wire = Wire::new(socket).unwrap();
        // As if nothing had come for longer than a silent peer is given.
        wire.socket.get_mut().heard = Instant::now() - SILENCE - Duration::from_secs(1);
        assert!(wire.keep_up().is_err());
        wire.socket.get_mut().heard = Instant::now() - SILENCE - Duration::from_secs(1);
        go.send(()).unwrap();
        let began = Instant::now();
        while began.elapsed() < Duration::from_secs(2) {
            assert_eq!(wire.hear().unwrap(), None, "the message is not whole");
        }
        assert!(wire.keep_up().is_ok(), "bytes came, so the peer is heard");
    }

    #[test]
    fn a_close_answered_is_the_end_even_while_a_proxy_holds_the_socket_open() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut socket = tungstenite::accept(stream).unwrap();
            // Reading the close queues its answer and flushing sends it; then the socket is held.
            while !matches!(socket.read(), Ok(Frame::Close(_)) | Err(_)) {}
            let _ = socket.flush();
            thread::sleep(GREETING);
        });
        let stream = TcpStream::connect(address).unwrap();
        let asked = format!("ws://{address}/");
        let (socket, _) = tungstenite::client(asked.as_str(), Stream::plain(stream)).unwrap();
        let wire = Wire::new(socket).unwrap();
        let began = Instant::now();
        assert!(wire.close(), "the close was not answered");
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "{:?}",
            began.elapsed()
        );
    }
}
