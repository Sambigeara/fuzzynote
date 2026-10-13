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
//! A replica never reads a line. Lines are passed on byte for byte, so one that this build
//! cannot read, or cannot decrypt, still gets where it is going.
//!
//! This module does no I/O. A face holds the connections and the storage: it says what
//! happened, and gets back the lines to keep and the messages to send.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

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
}

/// A log as it is held: its lines, and the digest of the log up to the end of each.
#[derive(Default)]
struct Log {
    lines: Vec<String>,
    ends: Vec<u64>,
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
        self.ends.push(after(self.digest_at(self.len()), &line));
        self.lines.push(line);
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
}

impl Replica {
    pub fn new() -> Self {
        Self::default()
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
    /// ends it, or with it. A log whose name is not a plain one stays where it is.
    pub fn append(&mut self, log: &str, lines: impl IntoIterator<Item = String>) -> Vec<Out> {
        let mut out = Vec::new();
        if !names_a_log(log) {
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
        }
        out
    }

    /// A peer has said what it holds. That replaces whatever was believed of it. Whoever holds
    /// more of a log can check the other's mark against its own lines, and then sends the rest.
    fn told(&mut self, peer: Peer, theirs: &BTreeMap<String, Mark>, out: &mut Vec<Out>) {
        let Some(known) = self.peers.get_mut(&peer) else {
            return;
        };
        let were_forked = std::mem::take(&mut known.forked);
        known.told = true;
        known.has = theirs
            .iter()
            .filter(|(log, _)| names_a_log(log))
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
    /// answered by saying what is held, so that the peer sends from there.
    fn take(&mut self, peer: Peer, log: &str, from: Mark, lines: Vec<String>, out: &mut Vec<Out>) {
        if !names_a_log(log) || lines.iter().any(|line| line.contains('\n')) {
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
        self.pass_on(log, Some(peer), out);
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
        sent: usize,
    }

    impl Net {
        fn of(replicas: Vec<Replica>) -> Self {
            Self {
                kept: replicas.iter().map(|_| BTreeMap::new()).collect(),
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
        for message in [have, some, forked] {
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
        for seed in 1..=100 {
            let mut dice = Dice(u64::wrapping_mul(seed, 0x9e37_79b9_7f4a_7c15));
            let mut net = Net::of((0..REPLICAS).map(|_| Replica::new()).collect());
            // What each device has written to its own log. Replica 0 writes nothing.
            let mut written: Vec<Vec<String>> = vec![Vec::new(); REPLICAS];
            for step in 0..400 {
                let (a, b) = (dice.roll(REPLICAS), dice.roll(REPLICAS));
                match dice.roll(10) {
                    0..=2 if a > 0 => {
                        let line = format!("{{\"n\":\"{seed}-{a}-{step}\"}}");
                        written[a].push(line.clone());
                        net.append(a, &format!("dev-{a}"), &[line.as_str()]);
                    }
                    3 => net.connect(a, b),
                    4 => net.disconnect(a, b),
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
            for (at, replica) in net.replicas.iter().enumerate() {
                for (device, lines) in written.iter().enumerate() {
                    let log = format!("dev-{device}");
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
            }
        }
    }
}
