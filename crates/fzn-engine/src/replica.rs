//! Replicas: how the lines of a space get from one device to another.
//!
//! A space is a set of logs. A log has one writer, which only ever adds lines to the end of it,
//! so any copy of a log is the start of the writer's own. That makes "what I hold" one number
//! for each log, and what a peer lacks the rest of each. Two replicas that have told each other
//! how far they hold every log, and sent each other the rest, hold the same lines. A device is a
//! replica, a browser tab is a replica, and a relay is a replica that writes nothing of its own.
//!
//! Nothing else is remembered. What a replica holds is the whole of what it knows, so there is
//! no cursor to lose, and a relay that loses everything is filled again by the next device that
//! connects to it.
//!
//! A point in a log is a [`Mark`]: how many lines, and a digest of them. A replica adds lines to
//! a log only where they continue exactly what it holds, which the mark they come after says.
//! Two different logs that have come to share a name, as when a device's id is copied to a
//! second machine, are therefore noticed and not mistaken for one: neither is changed, nothing
//! of that log passes between the two, and both are told.
//!
//! A log only grows, so a log is made shorter by starting another. Its first line is a
//! [`Header`] naming every log its writer has had before, and how many lines after it are the
//! snapshot that takes their place. A replica that holds the header and the whole snapshot
//! *retires* the logs it names: drops them, ignores a peer that offers them, and tells the face
//! to delete them. Rewriting a log in place would be a fork by the rule above, and a message
//! saying "this was rewritten" would be state a replica could miss; a new log is read the same
//! by a replica that was there and one that was not. Which logs are retired follows from the
//! logs held, and because a header names its writer's whole chain, it still follows when the
//! middle of the chain was never seen. Putting a fork right is the same move: one of the two
//! writers starts a new log, and its header retires the name they shared.
//!
//! A replica never reads a line, but one. Lines are passed on byte for byte, so one that this
//! build cannot read, or cannot decrypt, still gets where it is going. The one it reads is the
//! first of each log, to see whether it is a header: bookkeeping about logs, not about notes.
//!
//! This module does no I/O. A face holds the connections and the storage: it says what
//! happened, and gets back the lines to keep and the messages to send.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::time::Moment;

/// A peer, as the face that holds its connection numbers it.
pub type Peer = u32;

/// The most lines a single message carries, and the most text, so that a whole log is not one
/// message. A line longer than the second goes alone.
const BATCH_LINES: usize = 1_000;
const BATCH_BYTES: usize = 256 * 1024;

/// The longest name a log can have.
const NAME_MAX: usize = 64;

const FNV_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a, 64 bits, carried on over more bytes.
fn fnv(mut digest: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        digest ^= u64::from(b);
        digest = digest.wrapping_mul(FNV_PRIME);
    }
    digest
}

/// The digest of a log one line further on. Each line counts with the newline that ends it.
fn after(digest: u64, line: &str) -> u64 {
    fnv(fnv(digest, line.as_bytes()), b"\n")
}

/// Whether a name can be a log's. A log is a file on most replicas and its name comes from
/// other machines, so a name is plain: letters, digits, `-`, `_` and `.`, and no leading dot.
pub fn names_a_log(name: &str) -> bool {
    (1..=NAME_MAX).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// A point in a log: this many lines in, with a digest of those lines. The digest is FNV-1a over
/// each line and the newline after it. It is a check against accidents, not a defence against
/// anyone.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mark {
    pub lines: u64,
    #[serde(with = "hex")]
    pub digest: u64,
}

/// A digest is written as sixteen hex digits: a number that size does not survive JSON.
mod hex {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(digest: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&format_args!("{digest:016x}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let text = String::deserialize(deserializer)?;
        u64::from_str_radix(&text, 16).map_err(D::Error::custom)
    }
}

/// What one replica says to another. On the wire each is one JSON object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Message {
    /// Every log the sender holds, and how far. Said on connecting, and again whenever lines
    /// arrive that the receiver cannot place.
    Have(BTreeMap<String, Mark>),
    /// Lines of a log, and the mark they come straight after.
    Lines {
        log: String,
        after: Mark,
        lines: Vec<String>,
    },
    /// The log of this name held by the sender and the one held by the receiver are not one
    /// log: neither is the start of the other.
    Forked(String),
    /// The sender's clock. A relay says it first on connecting, so that a device can say when
    /// its own is far off; a build from before this passes over it as it would any message it
    /// does not know. A replica makes nothing of it.
    Now(Moment),
    /// A relay's challenge, said after its clock and before anything else: a nonce for the
    /// device to seal under its auth key, whether the relay holds an auth key for the space yet,
    /// and, when it holds none and the space holds lines, a sealed line of each of its logs, up
    /// to a few, of which a device opens one before it claims the space as its key's. This and
    /// the three after it are between a device and its relay, and a replica makes nothing of
    /// them.
    Challenge {
        nonce: String,
        claimed: bool,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        lines: Vec<String>,
    },
    /// The nonce sealed under the auth key, as a line is sealed.
    Proof(String),
    /// The auth key itself, as `Key::encode` writes it, for a space the relay holds none for.
    Claim(String),
    /// Why the relay will not go on. It closes the connection after saying it.
    Refused(String),
}

impl Message {
    pub fn encode(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    /// Reads a message, if it is one this build knows. Any other is to be ignored.
    pub fn decode(text: &str) -> Option<Self> {
        serde_json::from_str(text).ok()
    }
}

/// The first line of a log that takes the place of others: the logs it retires, and how many
/// lines after it are the snapshot of them. Its writer names every log it has had before, not
/// only the last, so that each is retired by the newest log alone. Anywhere but the first line of
/// a log, a line shaped like this is a line like any other.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    pub compacts: Vec<String>,
    pub snapshot: u64,
}

impl Header {
    /// The header as the line it is written as.
    pub fn line(&self) -> String {
        // A `Value` prints without failing, and its keys come out in this order.
        serde_json::json!({ "compacts": self.compacts, "snapshot": self.snapshot }).to_string()
    }

    /// Reads a line as a header, if it is exactly one: those two fields and no others, at least
    /// one log named, and every name a plain one. Anything else is an ordinary line.
    pub fn read(line: &str) -> Option<Self> {
        // Derived, a struct would also be read from an array of its fields.
        if !line.trim_start().starts_with('{') {
            return None;
        }
        let header: Self = serde_json::from_str(line).ok()?;
        let plain = !header.compacts.is_empty() && header.compacts.iter().all(|n| names_a_log(n));
        plain.then_some(header)
    }
}

/// What a face has to do after a replica has taken something in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Out {
    /// Lines that came from a peer, to keep at the end of the log named. The replica holds them
    /// already.
    Keep { log: String, lines: Vec<String> },
    /// A message to send to a peer.
    Send(Peer, Message),
    /// The log of this name held here and the one a peer holds are not one log. Nothing of it
    /// passes between the two until that is put right. Every other log goes on as before.
    Forked { log: String, peer: Peer },
    /// The log of this name is retired: a log held here starts with a header naming it, and
    /// holds the whole snapshot that takes its place. Delete it from wherever lines are kept. The
    /// replica no longer holds it, and drops any more of it that comes.
    Retire(String),
    /// A peer's header retires the log this replica writes. It is kept, and goes on being sent,
    /// since its lines may be ones nobody else has, as when two machines write under one id. The
    /// device should compact: start a log of its own whose header names this one.
    Superseded(String),
}

/// A log as it is held: its lines, the digest of the log up to the end of each, and its header
/// if its first line is one.
#[derive(Default)]
struct Log {
    lines: Vec<String>,
    ends: Vec<u64>,
    header: Option<Header>,
}

impl Log {
    fn len(&self) -> u64 {
        self.lines.len() as u64
    }

    /// The digest of the first `lines` lines. There have to be that many.
    fn digest_at(&self, lines: u64) -> u64 {
        match lines {
            0 => FNV_BASIS,
            n => self.ends[n as usize - 1],
        }
    }

    fn mark(&self) -> Mark {
        Mark {
            lines: self.len(),
            digest: self.digest_at(self.len()),
        }
    }

    /// Whether a mark is a point in this log.
    fn holds(&self, mark: Mark) -> bool {
        mark.lines <= self.len() && mark.digest == self.digest_at(mark.lines)
    }

    fn push(&mut self, line: String) {
        if self.lines.is_empty() {
            self.header = Header::read(&line);
        }
        self.ends.push(after(self.digest_at(self.len()), &line));
        self.lines.push(line);
    }

    /// The header, once the whole snapshot after it is held too.
    fn complete(&self) -> Option<&Header> {
        self.header
            .as_ref()
            .filter(|header| self.len() > header.snapshot)
    }
}

/// What is known of a peer.
#[derive(Default)]
struct Known {
    /// Whether the peer has said what it holds. Until it has, nothing is sent to it.
    told: bool,
    /// How many lines of each log the peer holds.
    has: BTreeMap<String, u64>,
    /// Logs that are not the same log here and there.
    forked: BTreeSet<String>,
}

/// The lines of one space as one place holds them, and what it knows of the peers it is
/// connected to.
#[derive(Default)]
pub struct Replica {
    logs: BTreeMap<String, Log>,
    peers: BTreeMap<Peer, Known>,
    /// Logs a header held here has taken the place of. None of them is held.
    retired: BTreeSet<String>,
    /// The log this replica writes, if it writes one.
    writes: Option<String>,
    /// Logs the face has been told are superseded, so that it is told once.
    superseded: BTreeSet<String>,
}

impl Replica {
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the one log this replica writes, in place of any marked before. A relay writes
    /// none. A peer's header does not retire this log, since a writer that shares its id with
    /// another machine would lose lines nobody else has; the face is told it is
    /// [`Out::Superseded`] instead. A writer that compacts marks its new log here before it
    /// appends the header, so that its own header retires the old one.
    pub fn writes(&mut self, log: &str) {
        self.writes = Some(log.to_owned());
    }

    /// Logs a header held here has taken the place of.
    pub fn retired(&self) -> &BTreeSet<String> {
        &self.retired
    }

    /// Every log held, and how far.
    pub fn have(&self) -> BTreeMap<String, Mark> {
        self.logs
            .iter()
            .map(|(name, log)| (name.clone(), log.mark()))
            .collect()
    }

    /// The lines of a log, as far as they are held.
    pub fn lines(&self, log: &str) -> &[String] {
        self.logs.get(log).map_or(&[], |held| &held.lines)
    }

    /// Takes lines that are at the end of a log here: read from where they are kept, or just
    /// written. Peers that lack them are sent them. A line is given without the newline that
    /// ends it, or with it. A log whose name is not a plain one stays where it is. A log that is
    /// retired is not taken, and the face is told again to delete it, which is what tidies up a
    /// file left from before.
    pub fn append(&mut self, log: &str, lines: impl IntoIterator<Item = String>) -> Vec<Out> {
        let mut out = Vec::new();
        if !names_a_log(log) {
            return out;
        }
        if self.retired.contains(log) {
            out.push(Out::Retire(log.to_owned()));
            return out;
        }
        let mut lines = lines.into_iter().peekable();
        if lines.peek().is_none() {
            return out;
        }
        let held = self.logs.entry(log.to_owned()).or_default();
        for mut line in lines {
            if line.ends_with('\n') {
                line.pop();
            }
            debug_assert!(!line.contains('\n'), "a line holds no newline");
            held.push(line);
        }
        self.retire_by(log, &mut out);
        self.pass_on(log, None, &mut out);
        out
    }

    /// A peer has connected. It is told what is held here.
    pub fn connect(&mut self, peer: Peer) -> Vec<Out> {
        self.peers.insert(peer, Known::default());
        vec![Out::Send(peer, Message::Have(self.have()))]
    }

    /// A peer has gone. What was known of it is forgotten, and learnt again when it comes back.
    pub fn disconnect(&mut self, peer: Peer) {
        self.peers.remove(&peer);
    }

    /// Takes a message from a connected peer.
    pub fn receive(&mut self, peer: Peer, message: Message) -> Vec<Out> {
        let mut out = Vec::new();
        if !self.peers.contains_key(&peer) {
            return out;
        }
        match message {
            Message::Have(theirs) => self.told(peer, &theirs, &mut out),
            Message::Lines { log, after, lines } => self.take(peer, &log, after, lines, &mut out),
            Message::Forked(log) => {
                if let Some(known) = self.peers.get_mut(&peer)
                    && known.forked.insert(log.clone())
                {
                    out.push(Out::Forked { log, peer });
                }
            }
            Message::Now(_)
            | Message::Challenge { .. }
            | Message::Proof(_)
            | Message::Claim(_)
            | Message::Refused(_) => {}
        }
        out
    }

    /// A peer has said what it holds. That replaces whatever was believed of it. Whoever holds
    /// more of a log can check the other's mark against its own lines, and then sends the rest.
    /// A log retired here is passed over: the peer retires it too once it has the log that
    /// takes its place.
    fn told(&mut self, peer: Peer, theirs: &BTreeMap<String, Mark>, out: &mut Vec<Out>) {
        let Some(known) = self.peers.get_mut(&peer) else {
            return;
        };
        let were_forked = std::mem::take(&mut known.forked);
        known.told = true;
        known.has = theirs
            .iter()
            .filter(|(log, _)| names_a_log(log) && !self.retired.contains(*log))
            .map(|(log, mark)| (log.clone(), mark.lines))
            .collect();
        for (log, held) in &self.logs {
            let Some(&mark) = theirs.get(log) else {
                continue;
            };
            if mark.lines <= held.len() && !held.holds(mark) {
                known.forked.insert(log.clone());
                if !were_forked.contains(log) {
                    out.push(Out::Forked {
                        log: log.clone(),
                        peer,
                    });
                    out.push(Out::Send(peer, Message::Forked(log.clone())));
                }
            }
        }
        let logs: Vec<String> = self.logs.keys().cloned().collect();
        for log in logs {
            self.bring(peer, &log, out);
        }
    }

    /// Lines have come from a peer. They are added only where they continue exactly what is
    /// held: lines already held are passed over, and lines from further on than is held are
    /// answered by saying what is held, so that the peer sends from there. Lines of a retired
    /// log are dropped.
    fn take(&mut self, peer: Peer, log: &str, from: Mark, lines: Vec<String>, out: &mut Vec<Out>) {
        if !names_a_log(log)
            || self.retired.contains(log)
            || lines.iter().any(|line| line.contains('\n'))
        {
            return;
        }
        let Some(known) = self.peers.get_mut(&peer) else {
            return;
        };
        if known.forked.contains(log) {
            return;
        }
        let held = self.logs.get(log);
        let holding = held.map_or(0, Log::len);
        if from.lines > holding {
            out.push(Out::Send(peer, Message::Have(self.have())));
            return;
        }
        // What is held up to where these lines start, and through any of them held already, has
        // to be what the peer holds there too.
        let overlap = ((holding - from.lines) as usize).min(lines.len());
        let through = lines[..overlap]
            .iter()
            .fold(from.digest, |digest, line| after(digest, line));
        let same = match held {
            Some(held) => {
                held.holds(from)
                    && held.holds(Mark {
                        lines: from.lines + overlap as u64,
                        digest: through,
                    })
            }
            None => from.digest == FNV_BASIS,
        };
        if !same {
            known.forked.insert(log.to_owned());
            out.push(Out::Forked {
                log: log.to_owned(),
                peer,
            });
            out.push(Out::Send(peer, Message::Forked(log.to_owned())));
            return;
        }
        let end = from.lines + lines.len() as u64;
        let has = known.has.entry(log.to_owned()).or_insert(0);
        *has = (*has).max(end);
        let fresh: Vec<String> = lines.into_iter().skip(overlap).collect();
        if fresh.is_empty() {
            return;
        }
        let held = self.logs.entry(log.to_owned()).or_default();
        for line in &fresh {
            held.push(line.clone());
        }
        out.push(Out::Keep {
            log: log.to_owned(),
            lines: fresh,
        });
        self.retire_by(log, out);
        self.pass_on(log, Some(peer), out);
    }

    /// Retires what a log's header names, if the log has one and holds the whole snapshot after
    /// it. The log this replica writes is kept, and the face told once that it is superseded.
    fn retire_by(&mut self, log: &str, out: &mut Vec<Out>) {
        let Some(header) = self.logs.get(log).and_then(Log::complete) else {
            return;
        };
        let names = header.compacts.clone();
        for name in names {
            if name == log {
                continue;
            }
            if self.writes.as_ref() == Some(&name) {
                if self.superseded.insert(name.clone()) {
                    out.push(Out::Superseded(name));
                }
                continue;
            }
            if !self.retired.insert(name.clone()) {
                continue;
            }
            self.logs.remove(&name);
            for known in self.peers.values_mut() {
                known.has.remove(&name);
                known.forked.remove(&name);
            }
            out.push(Out::Retire(name));
        }
    }

    /// Sends the end of a log to every peer that lacks it, but for the one it came from.
    fn pass_on(&mut self, log: &str, but: Option<Peer>, out: &mut Vec<Out>) {
        let peers: Vec<Peer> = self.peers.keys().copied().collect();
        for peer in peers {
            if Some(peer) != but {
                self.bring(peer, log, out);
            }
        }
    }

    /// Sends a peer whatever it is known to lack of a log, a batch to a message.
    fn bring(&mut self, peer: Peer, log: &str, out: &mut Vec<Out>) {
        let (Some(known), Some(held)) = (self.peers.get_mut(&peer), self.logs.get(log)) else {
            return;
        };
        if !known.told || known.forked.contains(log) {
            return;
        }
        let mut from = known.has.get(log).copied().unwrap_or(0) as usize;
        while from < held.lines.len() {
            let mut to = from;
            let mut bytes = 0;
            while to < held.lines.len()
                && to - from < BATCH_LINES
                && (to == from || bytes + held.lines[to].len() <= BATCH_BYTES)
            {
                bytes += held.lines[to].len();
                to += 1;
            }
            out.push(Out::Send(
                peer,
                Message::Lines {
                    log: log.to_owned(),
                    after: Mark {
                        lines: from as u64,
                        digest: held.digest_at(from as u64),
                    },
                    lines: held.lines[from..to].to_vec(),
                },
            ));
            from = to;
        }
        let has = known.has.entry(log.to_owned()).or_insert(0);
        *has = (*has).max(held.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    fn lines(texts: &[&str]) -> Vec<String> {
        texts.iter().map(|&t| t.to_owned()).collect()
    }

    fn replica(logs: &[(&str, &[&str])]) -> Replica {
        let mut replica = Replica::new();
        for (log, texts) in logs {
            assert!(replica.append(log, lines(texts)).is_empty());
        }
        replica
    }

    /// The lines of a log that compacts others: a header naming them, then the snapshot.
    fn compaction(compacts: &[&str], snapshot: &[String]) -> Vec<String> {
        let header = Header {
            compacts: lines(compacts),
            snapshot: snapshot.len() as u64,
        };
        std::iter::once(header.line())
            .chain(snapshot.iter().cloned())
            .collect()
    }

    /// A point in a set of lines, as a peer holding them would give it.
    fn mark_of(texts: &[String], n: usize) -> Mark {
        Mark {
            lines: n as u64,
            digest: texts[..n].iter().fold(FNV_BASIS, |d, line| after(d, line)),
        }
    }

    /// Replicas, and the connections between them. A connection carries messages one way in the
    /// order they were sent, as a socket does, and every message goes through its wire form.
    #[derive(Default)]
    struct Net {
        replicas: Vec<Replica>,
        wires: BTreeMap<(usize, usize), VecDeque<String>>,
        /// What each replica was told to keep.
        kept: Vec<BTreeMap<String, Vec<String>>>,
        /// Every fork reported: where, of which log, with which peer.
        forked: Vec<(usize, String, usize)>,
        /// What each replica was told to delete, and told was superseded, in order.
        retired: Vec<Vec<String>>,
        superseded: Vec<Vec<String>>,
        sent: usize,
    }

    impl Net {
        fn of(replicas: Vec<Replica>) -> Self {
            Self {
                kept: replicas.iter().map(|_| BTreeMap::new()).collect(),
                retired: replicas.iter().map(|_| Vec::new()).collect(),
                superseded: replicas.iter().map(|_| Vec::new()).collect(),
                replicas,
                ..Self::default()
            }
        }

        fn carry_out(&mut self, at: usize, out: Vec<Out>) {
            for out in out {
                match out {
                    Out::Keep { log, lines } => {
                        self.kept[at].entry(log).or_default().extend(lines);
                    }
                    Out::Retire(log) => {
                        self.kept[at].remove(&log);
                        self.retired[at].push(log);
                    }
                    Out::Superseded(log) => self.superseded[at].push(log),
                    Out::Send(peer, message) => {
                        let wire = self
                            .wires
                            .get_mut(&(at, peer as usize))
                            .expect("a message for a peer that is not connected");
                        wire.push_back(message.encode().unwrap());
                        self.sent += 1;
                    }
                    Out::Forked { log, peer } => self.forked.push((at, log, peer as usize)),
                }
            }
        }

        fn connect(&mut self, a: usize, b: usize) {
            if a == b || self.wires.contains_key(&(a, b)) {
                return;
            }
            self.wires.insert((a, b), VecDeque::new());
            self.wires.insert((b, a), VecDeque::new());
            let out = self.replicas[a].connect(b as Peer);
            self.carry_out(a, out);
            let out = self.replicas[b].connect(a as Peer);
            self.carry_out(b, out);
        }

        fn disconnect(&mut self, a: usize, b: usize) {
            if self.wires.remove(&(a, b)).is_some() {
                self.wires.remove(&(b, a));
                self.replicas[a].disconnect(b as Peer);
                self.replicas[b].disconnect(a as Peer);
            }
        }

        fn append(&mut self, at: usize, log: &str, texts: &[&str]) {
            let out = self.replicas[at].append(log, lines(texts));
            self.carry_out(at, out);
        }

        /// A writer compacts: it starts a log under a new name, whose header names every log it
        /// has had before and whose snapshot is the last of them, and writes there from now on.
        /// Returns the new log's lines.
        fn compact(&mut self, at: usize, before: &[&str], to: &str) -> Vec<String> {
            let last = before.last().expect("a log to compact");
            let new = compaction(before, self.replicas[at].lines(last));
            self.replicas[at].writes(to);
            let out = self.replicas[at].append(to, new.clone());
            self.carry_out(at, out);
            new
        }

        /// Delivers the next message on one wire, if there is one.
        fn deliver(&mut self, from: usize, to: usize) -> bool {
            let Some(text) = self
                .wires
                .get_mut(&(from, to))
                .and_then(VecDeque::pop_front)
            else {
                return false;
            };
            let message = Message::decode(&text).expect("a message this build wrote");
            let out = self.replicas[to].receive(from as Peer, message);
            self.carry_out(to, out);
            true
        }

        /// Delivers until there is nothing left to deliver.
        fn settle(&mut self) {
            loop {
                let waiting: Vec<(usize, usize)> = self
                    .wires
                    .iter()
                    .filter(|(_, wire)| !wire.is_empty())
                    .map(|(&ends, _)| ends)
                    .collect();
                if waiting.is_empty() {
                    return;
                }
                for (from, to) in waiting {
                    self.deliver(from, to);
                }
            }
        }
    }

    #[test]
    fn the_digest_is_fnv_1a_over_each_line_and_its_newline() {
        assert_eq!(fnv(FNV_BASIS, b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv(FNV_BASIS, b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv(FNV_BASIS, b"foobar"), 0x8594_4171_f739_67e8);

        let held = replica(&[("dev-a", &["foo", "bar"])]);
        assert_eq!(
            held.have()["dev-a"],
            Mark {
                lines: 2,
                digest: fnv(FNV_BASIS, b"foo\nbar\n")
            }
        );
        let with_newlines = replica(&[("dev-a", &["foo\n", "bar\n"])]);
        assert_eq!(with_newlines.have(), held.have(), "given either way");
        assert_eq!(with_newlines.lines("dev-a"), ["foo", "bar"]);
        assert!(Replica::new().have().is_empty());
        assert!(Replica::new().lines("dev-a").is_empty());
    }

    #[test]
    fn a_message_is_one_json_object() {
        let mark = Mark {
            lines: 2,
            digest: 0x00ff_0000_0000_0a0b,
        };
        let have = Message::Have(BTreeMap::from([("dev-a".to_owned(), mark)]));
        assert_eq!(
            have.encode().unwrap(),
            r#"{"have":{"dev-a":{"lines":2,"digest":"00ff000000000a0b"}}}"#
        );
        let some = Message::Lines {
            log: "dev-a".into(),
            after: mark,
            lines: lines(&[r#"{"v":1,"id":"n1","text":"a \"quoted\" line"}"#, ""]),
        };
        assert_eq!(
            some.encode().unwrap(),
            concat!(
                r#"{"lines":{"log":"dev-a","after":{"lines":2,"digest":"00ff000000000a0b"},"#,
                r#""lines":["{\"v\":1,\"id\":\"n1\",\"text\":\"a \\\"quoted\\\" line\"}",""]}}"#
            )
        );
        let forked = Message::Forked("dev-a".into());
        assert_eq!(forked.encode().unwrap(), r#"{"forked":"dev-a"}"#);
        let now = Message::Now(Moment {
            ms: 1_791_892_800_250,
            offset: 0,
        });
        assert_eq!(
            now.encode().unwrap(),
            r#"{"now":"2026-10-13T12:00:00.250Z"}"#
        );
        let unclaimed = Message::Challenge {
            nonce: "bm9uY2U=".into(),
            claimed: false,
            lines: vec![r#"{"s":1,"c":"AAAA"}"#.into()],
        };
        assert_eq!(
            unclaimed.encode().unwrap(),
            r#"{"challenge":{"nonce":"bm9uY2U=","claimed":false,"lines":["{\"s\":1,\"c\":\"AAAA\"}"]}}"#
        );
        let challenge = Message::Challenge {
            nonce: "bm9uY2U=".into(),
            claimed: true,
            lines: Vec::new(),
        };
        assert_eq!(
            challenge.encode().unwrap(),
            r#"{"challenge":{"nonce":"bm9uY2U=","claimed":true}}"#
        );
        let proof = Message::Proof(r#"{"s":1,"c":"AAAA"}"#.into());
        assert_eq!(
            proof.encode().unwrap(),
            r#"{"proof":"{\"s\":1,\"c\":\"AAAA\"}"}"#
        );
        let claim = Message::Claim("a2V5".into());
        assert_eq!(claim.encode().unwrap(), r#"{"claim":"a2V5"}"#);
        let refused = Message::Refused("the passphrase does not open main on the relay".into());
        assert_eq!(
            refused.encode().unwrap(),
            r#"{"refused":"the passphrase does not open main on the relay"}"#
        );
        // A replica passes over what is between a device and its relay.
        let mut replica = Replica::new();
        replica.connect(1);
        for message in [&challenge, &proof, &claim, &refused] {
            assert!(
                replica.receive(1, message.clone()).is_empty(),
                "{message:?}"
            );
        }
        for message in [
            have, some, forked, now, challenge, unclaimed, proof, claim, refused,
        ] {
            let text = message.encode().unwrap();
            assert!(!text.contains('\n'));
            assert_eq!(Message::decode(&text), Some(message));
        }

        // What this build does not know is passed over, not guessed at.
        assert_eq!(Message::decode(r#"{"hello":{"v":2}}"#), None);
        assert_eq!(Message::decode(r#"{"forked":"a","have":{}}"#), None);
        assert_eq!(
            Message::decode(r#"{"have":{"a":{"lines":1,"digest":"xyz"}}}"#),
            None
        );
        assert_eq!(Message::decode("not json"), None);
    }

    #[test]
    fn a_log_has_a_plain_name() {
        for name in [
            "3f9a01c2",
            "dev-a",
            "from_a",
            "3f9a01c2.sync-conflict-20261013-101010-ABCDEFG",
        ] {
            assert!(names_a_log(name), "{name}");
        }
        let long = "a".repeat(65);
        for name in [
            "",
            ".",
            "..",
            ".hidden",
            "a/b",
            "a\\b",
            "a b",
            "naïve",
            long.as_str(),
        ] {
            assert!(!names_a_log(name), "{name}");
        }
        let mut replica = Replica::new();
        assert!(replica.append("../elsewhere", lines(&["x"])).is_empty());
        assert!(replica.have().is_empty(), "and such a log is not held");
    }

    #[test]
    fn two_replicas_send_each_other_the_rest_of_each_log() {
        let mut net = Net::of(vec![
            replica(&[("dev-a", &["a1", "a2", "a3"]), ("dev-b", &["b1"])]),
            replica(&[
                ("dev-a", &["a1"]),
                ("dev-b", &["b1", "b2"]),
                ("dev-c", &["c1"]),
            ]),
        ]);
        net.connect(0, 1);
        net.settle();
        assert_eq!(net.replicas[0].have(), net.replicas[1].have());
        assert_eq!(net.replicas[1].lines("dev-a"), ["a1", "a2", "a3"]);
        assert_eq!(net.replicas[0].lines("dev-b"), ["b1", "b2"]);
        assert_eq!(net.replicas[0].lines("dev-c"), ["c1"]);
        assert_eq!(
            net.kept[0],
            BTreeMap::from([
                ("dev-b".to_owned(), lines(&["b2"])),
                ("dev-c".to_owned(), lines(&["c1"]))
            ]),
            "only what was lacking is kept"
        );
        assert_eq!(
            net.kept[1],
            BTreeMap::from([("dev-a".to_owned(), lines(&["a2", "a3"]))])
        );
        assert_eq!(net.sent, 5, "what each holds, and then one message a log");
        assert!(net.forked.is_empty());

        // From then on a line goes over as it is written, and once.
        net.append(0, "dev-a", &["a4"]);
        net.settle();
        assert_eq!(net.replicas[1].lines("dev-a"), ["a1", "a2", "a3", "a4"]);
        assert_eq!(net.sent, 6);
    }

    #[test]
    fn a_relay_passes_on_what_it_is_sent_and_writes_nothing() {
        // The relay is replica 0, and the devices only ever talk to it.
        let mut net = Net::of(vec![
            Replica::new(),
            replica(&[("dev-a", &["a1", "a2"])]),
            replica(&[("dev-b", &["b1"])]),
        ]);
        net.connect(1, 0);
        net.settle();
        assert_eq!(net.replicas[0].lines("dev-a"), ["a1", "a2"]);

        // A device that turns up later gets what the relay was given before it came.
        net.connect(2, 0);
        net.settle();
        assert_eq!(net.replicas[2].lines("dev-a"), ["a1", "a2"]);
        assert_eq!(net.replicas[1].lines("dev-b"), ["b1"]);

        // And what one device writes while both are there reaches the other, and does not come
        // back to the one that wrote it.
        let before = net.sent;
        net.append(1, "dev-a", &["a3"]);
        net.settle();
        assert_eq!(net.replicas[2].lines("dev-a"), ["a1", "a2", "a3"]);
        assert_eq!(net.sent, before + 2, "to the relay, and on to the other");
        assert!(!net.kept[1].contains_key("dev-a"));

        // A relay that has lost everything is filled again by whoever connects.
        net.disconnect(1, 0);
        net.disconnect(2, 0);
        net.replicas[0] = Replica::new();
        net.connect(2, 0);
        net.settle();
        assert_eq!(net.replicas[0].have(), net.replicas[2].have());
        assert!(net.forked.is_empty());
    }

    #[test]
    fn a_device_put_back_from_a_backup_gets_back_what_it_wrote() {
        let mut net = Net::of(vec![
            replica(&[("dev-a", &["a1", "a2", "a3"])]),
            replica(&[("dev-a", &["a1"])]),
        ]);
        net.connect(0, 1);
        net.settle();
        assert_eq!(net.replicas[1].lines("dev-a"), ["a1", "a2", "a3"]);
        assert_eq!(net.kept[1]["dev-a"], ["a2", "a3"]);
        assert!(net.forked.is_empty());
    }

    #[test]
    fn two_logs_with_one_name_are_noticed_and_left_alone() {
        // The same device id on two machines, each with a log of its own under it. The second
        // holds less, so only the first can see that the two are not one.
        let mut net = Net::of(vec![
            replica(&[("dev-a", &["a1", "a2", "a3"]), ("dev-b", &["b1"])]),
            replica(&[("dev-a", &["a1", "x2"]), ("dev-c", &["c1"])]),
        ]);
        net.connect(0, 1);
        net.settle();
        let both = [(0, "dev-a".to_owned(), 1), (1, "dev-a".to_owned(), 0)];
        assert_eq!(net.forked, both, "both are told, once each");
        assert_eq!(net.replicas[0].lines("dev-a"), ["a1", "a2", "a3"]);
        assert_eq!(net.replicas[1].lines("dev-a"), ["a1", "x2"]);
        assert!(net.kept.iter().all(|kept| !kept.contains_key("dev-a")));
        assert_eq!(net.replicas[1].lines("dev-b"), ["b1"], "other logs go on");
        assert_eq!(net.replicas[0].lines("dev-c"), ["c1"]);

        // Nor does anything written to either afterwards cross.
        net.append(0, "dev-a", &["a4"]);
        net.append(1, "dev-a", &["x3"]);
        net.settle();
        assert_eq!(net.replicas[1].lines("dev-a"), ["a1", "x2", "x3"]);
        assert_eq!(net.forked.len(), 2);

        // Two logs of the same length are seen from both ends at once.
        let mut net = Net::of(vec![
            replica(&[("dev-a", &["a1", "a2"])]),
            replica(&[("dev-a", &["a1", "x2"])]),
        ]);
        net.connect(0, 1);
        net.settle();
        net.forked.sort();
        assert_eq!(net.forked, both);

        // Connecting again says so again, and still nothing crosses.
        net.disconnect(0, 1);
        net.connect(0, 1);
        net.settle();
        assert_eq!(net.forked.len(), 4);
        assert_eq!(net.replicas[1].lines("dev-a"), ["a1", "x2"]);
    }

    #[test]
    fn lines_are_added_only_where_they_continue_what_is_held() {
        let start = Mark {
            lines: 0,
            digest: FNV_BASIS,
        };
        let sender = replica(&[("dev-a", &["a1", "a2", "a3", "a4"])]);
        let at = |n: u64| Mark {
            lines: n,
            digest: sender.logs["dev-a"].digest_at(n),
        };
        let some = |after: Mark, texts: &[&str]| Message::Lines {
            log: "dev-a".into(),
            after,
            lines: lines(texts),
        };
        let mut held = replica(&[("dev-a", &["a1", "a2"])]);
        held.connect(7);

        // Lines held already are passed over, and the rest are kept.
        assert_eq!(
            held.receive(7, some(at(1), &["a2", "a3"])),
            [Out::Keep {
                log: "dev-a".into(),
                lines: lines(&["a3"])
            }]
        );
        assert!(held.receive(7, some(start, &["a1", "a2", "a3"])).is_empty());
        assert_eq!(held.lines("dev-a"), ["a1", "a2", "a3"]);

        // Lines from further on than is held are answered with what is held.
        let mut behind = replica(&[("dev-a", &["a1"])]);
        behind.connect(7);
        assert_eq!(
            behind.receive(7, some(at(3), &["a4"])),
            [Out::Send(7, Message::Have(behind.have()))]
        );
        assert_eq!(behind.lines("dev-a"), ["a1"]);

        // Lines that follow some other log's start are not added, whether what differs is
        // before them or among the ones held already.
        let forked = [
            Out::Forked {
                log: "dev-a".into(),
                peer: 7,
            },
            Out::Send(7, Message::Forked("dev-a".into())),
        ];
        let wrong = Mark {
            lines: 3,
            digest: 1,
        };
        assert_eq!(held.receive(7, some(wrong, &["a4"])), forked);
        assert!(held.receive(7, some(at(3), &["a4"])).is_empty());
        assert_eq!(
            held.lines("dev-a"),
            ["a1", "a2", "a3"],
            "nor anything after"
        );

        let mut held = replica(&[("dev-a", &["a1", "a2"])]);
        held.connect(7);
        assert_eq!(held.receive(7, some(at(1), &["x2", "a3"])), forked);
        assert_eq!(held.lines("dev-a"), ["a1", "a2"]);

        // A log not held at all is taken from its start and from nowhere else.
        let mut empty = Replica::new();
        empty.connect(7);
        assert_eq!(
            empty.receive(7, some(at(3), &["a4"])),
            [Out::Send(7, Message::Have(BTreeMap::new()))]
        );
        let not_the_start = Mark {
            lines: 0,
            digest: 1,
        };
        assert_eq!(empty.receive(7, some(not_the_start, &["a1"])), forked);
        assert!(empty.have().is_empty());
        let mut empty = Replica::new();
        empty.connect(7);
        assert_eq!(empty.receive(7, some(start, &["a1"])).len(), 1);
        assert_eq!(empty.lines("dev-a"), ["a1"]);

        // What could not be a line, or a log, or comes from nobody, is dropped.
        assert!(empty.receive(7, some(at(1), &["two\nlines"])).is_empty());
        let elsewhere = Message::Lines {
            log: "../elsewhere".into(),
            after: start,
            lines: lines(&["x"]),
        };
        assert!(empty.receive(7, elsewhere).is_empty());
        assert!(empty.receive(8, some(at(1), &["a2"])).is_empty());
        assert_eq!(empty.have().len(), 1);
        assert_eq!(empty.lines("dev-a"), ["a1"]);
    }

    #[test]
    fn nothing_is_sent_to_a_peer_before_it_says_what_it_holds() {
        let mut a = replica(&[("dev-a", &["a1"])]);
        assert_eq!(a.connect(1), [Out::Send(1, Message::Have(a.have()))]);
        assert!(a.append("dev-a", lines(&["a2"])).is_empty());
        let out = a.receive(1, Message::Have(BTreeMap::new()));
        assert_eq!(
            out,
            [Out::Send(
                1,
                Message::Lines {
                    log: "dev-a".into(),
                    after: Mark {
                        lines: 0,
                        digest: FNV_BASIS
                    },
                    lines: lines(&["a1", "a2"])
                }
            )],
            "then everything it lacks, what was written meanwhile included"
        );
        assert!(a.receive(1, Message::Have(a.have())).is_empty());

        // A peer that has gone is forgotten, and told afresh when it is back.
        a.disconnect(1);
        assert!(a.append("dev-a", lines(&["a3"])).is_empty());
        assert!(a.receive(1, Message::Have(BTreeMap::new())).is_empty());
        assert_eq!(a.connect(1).len(), 1);
    }

    #[test]
    fn a_long_log_goes_a_batch_at_a_time() {
        let many: Vec<String> = (0..2_500).map(|i| format!("line {i}")).collect();
        let big = "x".repeat(BATCH_BYTES + 1);
        let mut a = Replica::new();
        a.append("dev-a", many.clone());
        a.append(
            "dev-b",
            vec!["small".to_owned(), big.clone(), "small".to_owned()],
        );
        a.connect(1);
        let out = a.receive(1, Message::Have(BTreeMap::new()));
        let sizes: Vec<(String, u64, usize)> = out
            .iter()
            .map(|out| match out {
                Out::Send(1, Message::Lines { log, after, lines }) => {
                    (log.clone(), after.lines, lines.len())
                }
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            sizes,
            [
                ("dev-a".to_owned(), 0, 1_000),
                ("dev-a".to_owned(), 1_000, 1_000),
                ("dev-a".to_owned(), 2_000, 500),
                ("dev-b".to_owned(), 0, 1),
                ("dev-b".to_owned(), 1, 1),
                ("dev-b".to_owned(), 2, 1),
            ],
            "a line too long for a batch goes alone"
        );

        let mut b = Replica::new();
        b.connect(0);
        for out in out {
            let Out::Send(_, message) = out else {
                unreachable!()
            };
            b.receive(0, message);
        }
        assert_eq!(b.have(), a.have());
        assert_eq!(b.lines("dev-a"), many);
        assert_eq!(b.lines("dev-b")[1], big);
    }

    #[test]
    fn a_header_names_the_logs_it_compacts_and_is_nothing_else() {
        let header = Header {
            compacts: lines(&["dev-a", "dev-a-1"]),
            snapshot: 293,
        };
        assert_eq!(
            header.line(),
            r#"{"compacts":["dev-a","dev-a-1"],"snapshot":293}"#
        );
        assert_eq!(Header::read(&header.line()), Some(header));
        assert_eq!(
            Header::read(r#"{ "snapshot": 0, "compacts": ["dev-a"] }"#),
            Some(Header {
                compacts: lines(&["dev-a"]),
                snapshot: 0
            }),
            "as JSON, in any order"
        );
        for line in [
            r#"{"compacts":["dev-a"],"snapshot":2,"v":1}"#,
            r#"{"compacts":[],"snapshot":2}"#,
            r#"{"compacts":["../elsewhere"],"snapshot":2}"#,
            r#"{"compacts":["dev-a",""],"snapshot":2}"#,
            r#"{"compacts":["dev-a"]}"#,
            r#"{"compacts":["dev-a"],"snapshot":-1}"#,
            r#"{"compacts":"dev-a","snapshot":2}"#,
            r#"{"compacts":["dev-a"],"compacts":["dev-b"],"snapshot":2}"#,
            r#"{"v":1,"id":"n1","text":"Boiler service"}"#,
            r#"[["dev-a"],2]"#,
            "Boiler service",
            "",
        ] {
            assert_eq!(Header::read(line), None, "{line}");
        }

        // Anywhere but the first line of a log, a header is an ordinary line.
        let later = compaction(&["dev-a"], &lines(&["Boiler service"]));
        let mut held = replica(&[("dev-a", &["Boiler service"])]);
        let mut out = held.append("dev-b", lines(&["House insurance"]));
        out.extend(held.append("dev-b", later));
        assert!(out.is_empty());
        assert_eq!(held.lines("dev-a"), ["Boiler service"]);
        assert!(held.retired().is_empty());
    }

    #[test]
    fn a_log_is_retired_once_the_whole_snapshot_that_replaces_it_is_held() {
        let old = lines(&["o1", "o2", "o3"]);
        let new = compaction(&["dev-a"], &old);

        // Appended a line at a time, from where it is kept.
        let mut held = replica(&[("dev-a", &["o1", "o2", "o3"])]);
        for line in &new[..3] {
            assert!(held.append("dev-a-1", [line.clone()]).is_empty());
        }
        assert_eq!(held.lines("dev-a"), old, "one line short, both are kept");
        assert_eq!(
            held.append("dev-a-1", [new[3].clone()]),
            [Out::Retire("dev-a".into())]
        );
        assert_eq!(held.have().keys().collect::<Vec<_>>(), ["dev-a-1"]);
        assert!(held.lines("dev-a").is_empty());
        assert_eq!(held.retired(), &BTreeSet::from(["dev-a".to_owned()]));

        // Taken from a peer, a batch at a time.
        let mut held = replica(&[("dev-a", &["o1", "o2", "o3"])]);
        held.connect(7);
        let some = |from: usize, to: usize| Message::Lines {
            log: "dev-a-1".into(),
            after: mark_of(&new, from),
            lines: new[from..to].to_vec(),
        };
        assert_eq!(
            held.receive(7, some(0, 3)),
            [Out::Keep {
                log: "dev-a-1".into(),
                lines: new[..3].to_vec()
            }]
        );
        assert_eq!(held.lines("dev-a"), old);
        assert_eq!(
            held.receive(7, some(3, 4)),
            [
                Out::Keep {
                    log: "dev-a-1".into(),
                    lines: new[3..].to_vec()
                },
                Out::Retire("dev-a".into())
            ]
        );
        let mut whole = Replica::new();
        whole.append("dev-a-1", new.clone());
        assert_eq!(held.have(), whole.have());

        // A log goes on after its snapshot as any log does, and nothing is retired twice.
        assert!(held.append("dev-a-1", lines(&["n1"])).is_empty());
    }

    #[test]
    fn a_retired_log_found_where_lines_are_kept_is_deleted_whichever_is_read_first() {
        let new = compaction(&["dev-a"], &lines(&["o1", "o2"]));
        let retire = [Out::Retire("dev-a".into())];

        let mut old_first = replica(&[("dev-a", &["o1", "o2"])]);
        assert_eq!(old_first.append("dev-a-1", new.clone()), retire);

        let mut new_first = Replica::new();
        assert_eq!(new_first.append("dev-a-1", new.clone()), retire);
        assert_eq!(new_first.append("dev-a", lines(&["o1", "o2"])), retire);

        for held in [&mut old_first, &mut new_first] {
            assert_eq!(held.have().keys().collect::<Vec<_>>(), ["dev-a-1"]);
            assert_eq!(held.retired(), &BTreeSet::from(["dev-a".to_owned()]));
            // Found again, it is to be deleted again, and is still not held.
            assert_eq!(held.append("dev-a", lines(&["o1"])), retire);
            assert!(held.lines("dev-a").is_empty());
            assert_eq!(held.have().len(), 1);
        }
    }

    #[test]
    fn a_peer_offering_a_retired_log_is_not_listened_to() {
        let new = compaction(&["dev-a"], &lines(&["o1"]));
        let mut held = Replica::new();
        held.append("dev-a-1", new.clone());
        held.connect(7);

        // It is sent the log that takes the place of the old one, and nothing of the old one,
        // whatever it holds of it.
        let theirs = BTreeMap::from([
            ("dev-a".to_owned(), mark_of(&lines(&["x1", "x2"]), 2)),
            ("dev-a-1".to_owned(), mark_of(&new, 0)),
        ]);
        assert_eq!(
            held.receive(7, Message::Have(theirs)),
            [Out::Send(
                7,
                Message::Lines {
                    log: "dev-a-1".into(),
                    after: mark_of(&new, 0),
                    lines: new.clone()
                }
            )]
        );

        // And lines of it are dropped, from anywhere in it.
        for after in [0, 2, 5] {
            let some = Message::Lines {
                log: "dev-a".into(),
                after: mark_of(&lines(&["x1", "x2", "x3", "x4", "x5"]), after),
                lines: lines(&["x6"]),
            };
            assert!(held.receive(7, some).is_empty());
        }
        assert_eq!(held.have().keys().collect::<Vec<_>>(), ["dev-a-1"]);
    }

    #[test]
    fn the_newest_log_of_a_chain_retires_the_whole_chain() {
        // The writer went from dev-a to dev-a-1 to dev-a-2, and this replica never saw dev-a-1.
        let mut held = replica(&[("dev-a", &["o1"])]);
        let newest = compaction(&["dev-a", "dev-a-1"], &lines(&["o1", "m1"]));
        assert_eq!(
            held.append("dev-a-2", newest),
            [Out::Retire("dev-a".into()), Out::Retire("dev-a-1".into())]
        );
        assert_eq!(held.retired().len(), 2);
        assert_eq!(
            held.append("dev-a-1", lines(&["m1"])),
            [Out::Retire("dev-a-1".into())]
        );
        assert_eq!(held.have().keys().collect::<Vec<_>>(), ["dev-a-2"]);
    }

    #[test]
    fn a_writer_keeps_its_own_log_when_a_peer_compacts_it_until_it_compacts_too() {
        // Two machines write under dev-a. The other one has compacted, into dev-a-1.
        let mut writer = replica(&[("dev-a", &["o1", "o2"])]);
        writer.writes("dev-a");
        writer.connect(7);
        writer.receive(7, Message::Have(BTreeMap::new()));
        let mut theirs = compaction(&["dev-a"], &lines(&["o1", "x2"]));
        theirs.push("n1".into());
        let some = |from: usize, to: usize| Message::Lines {
            log: "dev-a-1".into(),
            after: mark_of(&theirs, from),
            lines: theirs[from..to].to_vec(),
        };
        let out = writer.receive(7, some(0, 3));
        assert_eq!(out[1..], [Out::Superseded("dev-a".into())]);
        assert_eq!(
            writer.lines("dev-a"),
            ["o1", "o2"],
            "its own lines are kept"
        );
        assert!(writer.retired().is_empty());

        // It is said once, however much more of that log comes.
        assert_eq!(writer.receive(7, some(3, 4)).len(), 1);

        // What it writes meanwhile still goes out.
        assert_eq!(
            writer.append("dev-a", lines(&["o3"])),
            [Out::Send(
                7,
                Message::Lines {
                    log: "dev-a".into(),
                    after: mark_of(&lines(&["o1", "o2"]), 2),
                    lines: lines(&["o3"])
                }
            )]
        );

        // Once it compacts, its own header retires the log it wrote before.
        writer.writes("dev-a-2");
        let ours = compaction(&["dev-a"], writer.lines("dev-a"));
        let out = writer.append("dev-a-2", ours);
        assert_eq!(out[0], Out::Retire("dev-a".into()));
        assert!(
            matches!(&out[1..], [Out::Send(7, Message::Lines { log, .. })] if log == "dev-a-2")
        );
        assert_eq!(
            writer.have().keys().collect::<Vec<_>>(),
            ["dev-a-1", "dev-a-2"]
        );
    }

    #[test]
    fn a_writer_that_compacts_retires_its_old_log_everywhere() {
        // The relay is replica 0.
        let mut net = Net::of(vec![
            Replica::new(),
            replica(&[("dev-a", &["a1", "a2", "a3"])]),
            replica(&[("dev-b", &["b1"])]),
        ]);
        net.replicas[1].writes("dev-a");
        net.replicas[2].writes("dev-b");
        net.connect(1, 0);
        net.connect(2, 0);
        net.settle();
        assert_eq!(net.replicas[2].lines("dev-a"), ["a1", "a2", "a3"]);

        let new = net.compact(1, &["dev-a"], "dev-a-1");
        net.append(1, "dev-a-1", &["a4"]);
        net.settle();
        for (at, replica) in net.replicas.iter().enumerate() {
            assert_eq!(replica.have(), net.replicas[0].have());
            assert_eq!(
                replica.have().keys().collect::<Vec<_>>(),
                ["dev-a-1", "dev-b"]
            );
            assert_eq!(replica.lines("dev-a-1")[..4], new[..]);
            assert_eq!(net.retired[at], ["dev-a"], "told once, at {at}");
            assert!(!net.kept[at].contains_key("dev-a"));
        }
        assert!(net.superseded.iter().all(Vec::is_empty));
        assert!(net.forked.is_empty());
    }

    #[test]
    fn a_fork_is_put_right_by_both_sides_compacting() {
        // The same id on two machines, each with a log of its own under it, through a relay.
        let mut net = Net::of(vec![
            Replica::new(),
            replica(&[("dev-a", &["a1", "a2", "a3"]), ("dev-b", &["b1"])]),
            replica(&[("dev-a", &["a1", "x2"]), ("dev-c", &["c1"])]),
        ]);
        net.replicas[1].writes("dev-a");
        net.replicas[2].writes("dev-a");
        net.connect(1, 0);
        net.settle();
        net.connect(2, 0);
        net.settle();
        assert!(!net.forked.is_empty());

        // One of them compacts. The other is told its log is superseded, and keeps it.
        net.compact(1, &["dev-a"], "dev-a-1");
        net.settle();
        assert_eq!(net.superseded, [vec![], vec![], vec!["dev-a".to_owned()]]);
        assert_eq!(net.replicas[2].lines("dev-a"), ["a1", "x2"]);
        assert_eq!(
            net.replicas[2].lines("dev-a-1")[1..],
            lines(&["a1", "a2", "a3"])
        );

        // It compacts in turn, and the name the two shared is gone from everywhere.
        net.compact(2, &["dev-a"], "dev-d");
        net.settle();
        for (at, replica) in net.replicas.iter().enumerate() {
            assert_eq!(replica.have(), net.replicas[0].have());
            assert_eq!(
                replica.have().keys().collect::<Vec<_>>(),
                ["dev-a-1", "dev-b", "dev-c", "dev-d"]
            );
            assert_eq!(net.retired[at], ["dev-a"], "told once, at {at}");
        }
        assert_eq!(net.replicas[0].lines("dev-d")[1..], lines(&["a1", "x2"]));
        assert_eq!(net.superseded[2].len(), 1);
    }

    /// A small generator of the same numbers every time.
    struct Dice(u64);

    impl Dice {
        fn roll(&mut self, sides: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            (self.0 % sides as u64) as usize
        }
    }

    #[test]
    fn whatever_happens_in_whatever_order_everyone_ends_up_with_the_same_lines() {
        const REPLICAS: usize = 5;
        let mut chains = 0;
        for seed in 1..=100 {
            let mut dice = Dice(u64::wrapping_mul(seed, 0x9e37_79b9_7f4a_7c15));
            let mut net = Net::of((0..REPLICAS).map(|_| Replica::new()).collect());
            // Every log each device has written, the one it writes now last, and what it has
            // written to that one. Replica 0 writes nothing.
            let mut names: Vec<Vec<String>> =
                (0..REPLICAS).map(|a| vec![format!("dev-{a}")]).collect();
            let mut written: Vec<Vec<String>> = vec![Vec::new(); REPLICAS];
            for (a, names) in names.iter().enumerate().skip(1) {
                net.replicas[a].writes(&names[0]);
            }
            for step in 0..400 {
                let (a, b) = (dice.roll(REPLICAS), dice.roll(REPLICAS));
                match dice.roll(10) {
                    0..=2 if a > 0 => {
                        let line = format!("{{\"n\":\"{seed}-{a}-{step}\"}}");
                        written[a].push(line.clone());
                        let log = names[a].last().cloned().unwrap_or_default();
                        net.append(a, &log, &[line.as_str()]);
                    }
                    3 => net.connect(a, b),
                    4 => net.disconnect(a, b),
                    5 if a > 0 && dice.roll(4) == 0 => {
                        let to = format!("dev-{a}-{}", names[a].len());
                        let before: Vec<&str> = names[a].iter().map(String::as_str).collect();
                        written[a] = net.compact(a, &before, &to);
                        names[a].push(to);
                    }
                    _ => {
                        net.deliver(a, b);
                    }
                }
            }
            // Then everyone reaches the relay, by a wire of their own or through someone else.
            for device in 1..REPLICAS {
                if dice.roll(3) > 0 {
                    net.connect(device, 0);
                } else {
                    net.connect(device, 1 + dice.roll(REPLICAS - 1));
                    net.connect(1 + dice.roll(REPLICAS - 1), 0);
                    net.connect(device, 0);
                }
            }
            net.settle();

            assert!(net.forked.is_empty(), "seed {seed}: {:?}", net.forked);
            assert!(net.superseded.iter().all(Vec::is_empty), "seed {seed}");
            let mut compacted: Vec<String> = names
                .iter()
                .flat_map(|names| names[..names.len() - 1].iter().cloned())
                .collect();
            compacted.sort();
            for (at, replica) in net.replicas.iter().enumerate() {
                for (device, lines) in written.iter().enumerate() {
                    let log = names[device].last().cloned().unwrap_or_default();
                    assert_eq!(replica.lines(&log), lines, "seed {seed}: {log} at {at}");
                    // A replica was told to keep exactly the lines it did not write itself.
                    let kept = net.kept[at].get(&log).cloned().unwrap_or_default();
                    if at == device {
                        assert!(kept.is_empty(), "seed {seed}: {log} kept at its writer");
                    } else {
                        assert_eq!(&kept, lines, "seed {seed}: {log} kept at {at}");
                    }
                }
                assert_eq!(replica.have(), net.replicas[0].have(), "seed {seed}");
                // Every log a header names is retired, and the face told to delete it once.
                for log in &compacted {
                    assert!(
                        !replica.have().contains_key(log),
                        "seed {seed}: {log} at {at}"
                    );
                }
                let mut retired = net.retired[at].clone();
                retired.sort();
                assert_eq!(retired, compacted, "seed {seed}: at {at}");
            }
            chains += usize::from(names.iter().any(|names| names.len() > 2));
        }
        assert!(
            chains > 10,
            "writers compacted more than once in {chains} runs"
        );
    }
}
