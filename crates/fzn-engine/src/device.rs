//! A device: a session and a replica, joined in memory, with the space's key.
//!
//! The session knows notes and the replica knows logs, and neither should know the other. They
//! have to meet somewhere, and once they met at the files: the app wrote versions into its log, a
//! thread sent what it found there, and what the thread brought the app found in turn. That made
//! two readers of one directory, a lock on every write and a way for the replica to go stale. A
//! browser tab has no files, so they were joined in memory for it, and that join turned out to
//! be the general thing: the same code serves the terminal, a tab and a test. A version the
//! session wants kept is sealed and becomes a line of this writer's log, which the replica holds
//! and sends on; a line the replica takes from a peer is opened and given to the session, and so
//! is one that another writer on the same machine has added where the face keeps them. The face
//! is left with what only it can do: keeping lines for the next time, and the connections.
//!
//! A device's peers are the relay and its siblings: other clients open on the same space on the
//! same machine, such as another tab, each writing a log of its own. A sibling is a peer like
//! the relay, so what one writes reaches the others as a line from anywhere does, and what one
//! hears from the relay it passes on. The relay is peer 0, as every face numbers it, and is the
//! one peer whose clock is taken.
//!
//! What the face is to do comes back as a [`Todo`], and it does the keeping before the deleting.
//! A compaction starts a new log whose header retires the old one, so one `Todo` can hold both
//! the new log and the old log's deletion. Done the other way round, a face stopped between the
//! two would have neither, and the notes the old log held would be on no disk.
//!
//! A relay lets a device into a space only once it has shown it holds the space's key. It says a
//! nonce, and the device seals it under a key derived one way from the space's, which the relay
//! keeps and can read nothing with; the first device to reach a space the relay holds no such key
//! for hands it over. Nothing new is remembered for it: the proof is made from the key the device
//! already has. A relay that will not go on says why, and that is kept and said until a relay
//! takes the device in again.
//!
//! A device writes nothing until it knows its key is the space's. A line sealed under a second
//! key would split the space in two, readable on some devices and shut on the rest. So the key
//! is checked against what is there: once a sealed line has been seen, a key that has opened
//! none is the wrong one, and the device says so and writes nothing. A space with no sealed line
//! yet takes the first key it is given.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

use serde::Serialize;

use crate::record::{Notes, Read, Record};
use crate::replica::{Header, Message, Out, Peer, Replica};
use crate::score::Weights;
use crate::seal::{Key, Opened, Seal, is_sealed};
use crate::session::{Action, Caret, Effect, Given, Host, KeptStep, Session, View};
use crate::time::Moment;

/// The relay, as every face numbers it. Siblings on the same machine are 1, 2, and so on.
pub const RELAY: Peer = 0;

/// A writer's log longer than this, and more than [`GROWTH`] times as long as the notes in it,
/// is compacted when the device opens.
const COMPACT_LINES: usize = 1_000;
const GROWTH: usize = 4;

/// What a device says when its key opens none of the lines of the space the relay offers, and so
/// claims nothing there.
const NOT_THE_SPACES: &str = "the passphrase does not open this space on the relay";

/// What a device says when it will not write.
const WRONG_KEY: &str = "the passphrase does not open this space, so nothing is written";

/// How far this device's clock may be from the relay's, in milliseconds, before it is said.
const CLOCK_OFF: u64 = 2 * 60 * 1_000;

/// Lines to keep at the end of a log, the first of them at `from`. A line has no newline.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Kept {
    pub log: String,
    pub from: u64,
    pub lines: Vec<String>,
}

/// A message for a peer, to send as it is.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Said {
    pub to: Peer,
    pub text: String,
}

/// A note to put in front of the person for editing.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Editing {
    pub id: String,
    pub text: String,
}

/// What the face has to do after the device has taken something in.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct Todo {
    /// Lines to keep at the end of the log named, in order. Done before anything else.
    pub keep: Vec<Kept>,
    /// Logs retired, to delete from wherever lines are kept. Done after `keep`.
    pub delete: Vec<String>,
    /// Messages for peers, each one to send as it is, in order.
    pub send: Vec<Said>,
    /// This writer's new id, when it has changed, for the face to keep in place of the old.
    pub device: Option<String>,
    /// A note to hand to the face's editor. What is made of it comes back as the action `save`.
    pub editor: Option<Editing>,
    /// Text for the clipboard.
    pub copy: Option<String>,
    /// A URL to open.
    pub open: Option<String>,
    /// Where the session put the caret, for the face to put its own there.
    pub caret: Caret,
}

impl Todo {
    /// Adds what comes after this to it: its lines kept after these, its logs deleted after
    /// these, its messages sent after these.
    fn then(&mut self, after: Todo) {
        self.keep.extend(after.keep);
        self.delete.extend(after.delete);
        self.send.extend(after.send);
        if after.device.is_some() {
            self.device = after.device;
        }
        self.editor = self.editor.take().or(after.editor);
        self.copy = self.copy.take().or(after.copy);
        self.open = self.open.take().or(after.open);
        self.caret.then(after.caret);
    }
}

/// The session is given the device's host through this, since it takes one of its own.
struct Shared(Rc<dyn Host>);

impl Host for Shared {
    fn now(&self) -> Moment {
        self.0.now()
    }

    fn new_id(&self) -> String {
        self.0.new_id()
    }

    fn random(&self, buf: &mut [u8]) {
        self.0.random(buf)
    }
}

/// One person at one space on one device, with sync.
pub struct Device {
    host: Rc<dyn Host>,
    session: Session,
    replica: Replica,
    seal: Seal,
    /// This writer's id, and so the name of the log it writes.
    device: String,
    /// Sealed lines seen, and how many of them this key opened.
    sealed_seen: usize,
    sealed_opened: usize,
    /// Sealed lines seen that this key did not open.
    unopened: usize,
    /// Whether a peer's header has retired the log this device writes, so that it has to start
    /// another.
    superseded: bool,
    /// How far this device's clock was ahead of the relay's, in milliseconds, when the relay
    /// last said its own on connecting. None while there is no relay, or one from before relays
    /// said it.
    ahead: Option<i64>,
    /// The key this device proves itself to a relay with, derived once from the space's key.
    auth: Key,
    /// Why the relay last refused this device, until a relay has taken it in again.
    refused: Option<String>,
    /// Whether this device has answered the relay's challenge on this connection, so that a
    /// refusal heard since is one of its key, and not of a token or a name.
    answered: bool,
    /// Whether the last refusal was heard after this device answered a challenge.
    refused_answer: bool,
    /// Whether this device's key opened none of the lines of the space a relay offered. That is a
    /// verdict on the key, which holds through whatever the relay says next of having heard no
    /// answer, until a relay takes the device in.
    shut: bool,
}

impl Device {
    /// A device writing as `device`, with no notes until it is given lines.
    pub fn new(host: Box<dyn Host>, device: impl Into<String>, weights: Weights, key: Key) -> Self {
        let host: Rc<dyn Host> = Rc::from(host);
        let device = device.into();
        let mut replica = Replica::new();
        replica.writes(&device);
        Self {
            session: Session::new(
                Box::new(Shared(host.clone())),
                device.clone(),
                weights,
                Notes::new(),
            ),
            host,
            replica,
            auth: key.auth(),
            seal: Seal::new(key),
            device,
            sealed_seen: 0,
            sealed_opened: 0,
            unopened: 0,
            superseded: false,
            ahead: None,
            refused: None,
            answered: false,
            refused_answer: false,
            shut: false,
        }
    }

    /// Takes in lines the face kept from before, a log at a time, before anything else is asked
    /// of the device. Trailing newlines are tolerated.
    pub fn load(&mut self, log: &str, lines: Vec<String>) -> Todo {
        let todo = self.added(log, lines);
        // A device opened with its own notes says nothing of them having changed, unless what
        // it was given is sealed under some other key.
        self.session
            .say(if self.key_verified() { "" } else { WRONG_KEY });
        todo
    }

    /// Takes in lines that another writer on this machine has added to the end of a log where
    /// the face keeps it, as lines from a peer are taken in, but for keeping them, since they
    /// are kept already. The peers are sent them as they would be a peer's. The face hands over
    /// only lines past those it has handed over before, in order, since they are added to the
    /// end of what is held without being checked against it.
    pub fn added(&mut self, log: &str, lines: Vec<String>) -> Todo {
        let lines: Vec<String> = lines
            .into_iter()
            .map(|mut line| {
                if line.ends_with('\n') {
                    line.pop();
                }
                line
            })
            .collect();
        let mut todo = Todo::default();
        // A log retired here is left from before: its notes are in the log that retired it, and
        // it is only to be deleted.
        let retired = self.replica.retired().contains(log);
        let out = self.replica.append(log, lines.iter().cloned());
        self.carry_out(out, &mut todo);
        if !retired {
            let versions = self.read(&lines);
            self.session.absorb(versions);
            todo.caret.then(self.session.take_caret());
        }
        todo
    }

    /// Once, after every `load` and before `connected`: compacts this writer's log if that is
    /// due. It is due when a peer's log has retired it, when it holds lines from before sealing,
    /// or when it has grown past [`GROWTH`] times the notes in it. A line of a later format is
    /// counted as a note, since this build cannot tell which note it is, and a compaction keeps
    /// every such line.
    pub fn opened(&mut self) -> Todo {
        if !self.key_verified() {
            return Todo::default();
        }
        let own = self.replica.lines(&self.device);
        let mut plain = false;
        let mut ids = BTreeSet::new();
        let mut later = 0;
        for line in own {
            match Record::read_line(&self.seal, line) {
                Read::Version(record) => {
                    plain |= !is_sealed(line);
                    ids.insert(record.id);
                }
                Read::Later => later += 1,
                Read::Shut | Read::Other => {}
            }
        }
        let grown = own.len() > COMPACT_LINES && own.len() > GROWTH * (ids.len() + later);
        if self.superseded || plain || grown {
            self.compact()
        } else {
            Todo::default()
        }
    }

    /// Does what was asked. Nothing that writes is done until the key is known to be the
    /// space's.
    pub fn act(&mut self, action: Action) -> Todo {
        // A head is written as it is typed, but a body typed into, or a date word not yet
        // written out, waits for the cursor to leave. When the key turns out to be wrong that
        // cannot be written, and so cannot be left either, since leaving writes; it can only be
        // put back, and then left. Putting back writes the version the cursor landed on, if the
        // head was written as it was typed before the key was found wrong: one line, under the
        // key every line this device has written is already under.
        let gated = if self.session.unwritten() {
            !matches!(action, Action::Undo | Action::ShowScores)
        } else {
            action.writes()
        };
        if gated && !self.key_verified() {
            self.session.say(WRONG_KEY);
            return Todo::default();
        }
        let effects = self.session.act(action);
        let mut todo = self.todo(effects);
        todo.caret.then(self.session.take_caret());
        todo
    }

    /// Ticks these tasks, or unticks them, as archiving each would, as [`Session::tick`] says,
    /// and gives back each note as it now stands, with what the face is to do. Nothing is
    /// written until the key is known to be the space's.
    pub fn tick(&mut self, ids: &[String], done: bool) -> Result<(Todo, Vec<Record>), String> {
        if !self.key_verified() {
            return Err(WRONG_KEY.to_owned());
        }
        let effects = self.session.tick(ids, done)?;
        let ticked = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Keep(version) => Some(version.clone()),
                _ => None,
            })
            .collect();
        Ok((self.todo(effects), ticked))
    }

    /// Writes notes given from outside, as [`Session::apply`] says, and gives back each note as
    /// it now stands, in the order given, with what the face is to do. Nothing is written until
    /// the key is known to be the space's.
    pub fn apply(&mut self, given: Vec<Given>) -> Result<(Todo, Vec<Record>), String> {
        if !self.key_verified() {
            return Err(WRONG_KEY.to_owned());
        }
        let (notes, effects) = self.session.apply(given)?;
        Ok((self.todo(effects), notes))
    }

    /// What the face is to do about what the session asks for: each version it wants kept
    /// sealed as a line of this writer's log, and the rest handed on.
    fn todo(&mut self, effects: Vec<Effect>) -> Todo {
        let mut todo = Todo::default();
        for effect in effects {
            match effect {
                Effect::Keep(version) => {
                    let Ok(line) = version.sealed(&self.seal, &*self.host) else {
                        continue;
                    };
                    let from = self.replica.lines(&self.device).len() as u64;
                    let out = self.replica.append(&self.device, [line.clone()]);
                    todo.keep.push(Kept {
                        log: self.device.clone(),
                        from,
                        lines: vec![line],
                    });
                    self.carry_out(out, &mut todo);
                }
                Effect::Editor { id, text } => todo.editor = Some(Editing { id, text }),
                Effect::Copy(text) => todo.copy = Some(text),
                Effect::Open(url) => todo.open = Some(url),
            }
        }
        self.compact_if_superseded(&mut todo);
        todo
    }

    /// A connection to a peer has opened: the relay's socket, or a sibling's channel.
    pub fn connected(&mut self, peer: Peer) -> Todo {
        if peer == RELAY {
            self.answered = false;
        }
        let mut todo = Todo::default();
        let out = self.replica.connect(peer);
        self.carry_out(out, &mut todo);
        self.compact_if_superseded(&mut todo);
        todo
    }

    /// A connection to a peer has closed. What the relay said of its clock is learnt again
    /// when it comes back.
    pub fn disconnected(&mut self, peer: Peer) {
        self.replica.disconnect(peer);
        if peer == RELAY {
            self.ahead = None;
            self.answered = false;
        }
    }

    /// A peer has said something. What this build does not know is passed over, and so is a
    /// clock from any peer but the relay, since a sibling's is this machine's own. The relay's
    /// challenge is answered, as [`Device::answer`] says. A refusal is kept and said, and
    /// anything the relay's replica says clears it.
    pub fn received(&mut self, peer: Peer, text: &str) -> Todo {
        let mut todo = Todo::default();
        match Message::decode(text) {
            Some(Message::Now(relay)) => {
                if peer == RELAY {
                    self.ahead = Some(self.host.now().ms.saturating_sub(relay.ms));
                }
            }
            Some(Message::Challenge {
                nonce,
                claimed,
                lines,
            }) if peer == RELAY => {
                for message in self.answer(&nonce, claimed, &lines) {
                    if let Ok(text) = message.encode() {
                        todo.send.push(Said { to: RELAY, text });
                    }
                }
            }
            // Once the space's lines have not opened, what the relay says of the answer it then did
            // not hear is not the reason.
            Some(Message::Refused(_)) if peer == RELAY && self.shut => {
                self.session.say(NOT_THE_SPACES);
            }
            Some(Message::Refused(why)) if peer == RELAY => {
                self.session.say(why.clone());
                self.refused = Some(why);
                self.refused_answer = self.answered;
            }
            Some(message) => {
                if peer == RELAY
                    && matches!(
                        message,
                        Message::Have(_) | Message::Lines { .. } | Message::Forked(_)
                    )
                {
                    self.refused = None;
                    self.refused_answer = false;
                    self.shut = false;
                }
                let out = self.replica.receive(peer, message);
                self.carry_out(out, &mut todo);
            }
            None => {}
        }
        self.compact_if_superseded(&mut todo);
        todo
    }

    /// What answers the relay's challenge: the nonce sealed under the auth key when the relay
    /// holds that key, and the key itself when it holds none yet, and then what this device
    /// holds, since what it said before it was let in was not heard. A nonce that is not one is
    /// not sealed. The auth key is not handed over as the space's by a device whose key has
    /// opened nothing of what it holds, nor one whose key opens none of the lines of the space
    /// the relay offers, which is a passphrase mistyped; that is kept as a refusal is.
    fn answer(&mut self, nonce: &str, claimed: bool, lines: &[String]) -> Vec<Message> {
        let answer = if claimed {
            if STANDARD
                .decode(nonce)
                .map_or(true, |bytes| bytes.len() != 32)
            {
                return Vec::new();
            }
            Message::Proof(Seal::new(self.auth.clone()).seal(nonce, &*self.host))
        } else if !self.key_verified() {
            self.session.say(WRONG_KEY);
            return Vec::new();
        } else if !lines.is_empty()
            && lines
                .iter()
                .all(|line| matches!(self.seal.open(line), Opened::Shut))
        {
            self.session.say(NOT_THE_SPACES);
            self.refused = Some(NOT_THE_SPACES.to_owned());
            self.refused_answer = true;
            self.shut = true;
            return Vec::new();
        } else {
            Message::Claim(self.auth.encode())
        };
        self.answered = true;
        vec![answer, Message::Have(self.replica.have())]
    }

    /// What to say of this device's clock: how far it is from the relay's, when that is more
    /// than two minutes. Nothing is put right, since every device has to decide between the
    /// same two stamps the same way; it is only said, so that the clock can be set.
    pub fn clock(&self) -> Option<String> {
        let ahead = self.ahead.filter(|ms| ms.unsigned_abs() > CLOCK_OFF)?;
        let way = if ahead > 0 { "ahead" } else { "behind" };
        let minutes = (ahead.unsigned_abs() + 30_000) / 60_000;
        let by = match minutes {
            ..60 => format!("{minutes} min"),
            60..2_880 => format!("{} h", (minutes + 30) / 60),
            _ => format!("{} days", (minutes + 720) / 1_440),
        };
        Some(format!("this device's clock is {by} {way}"))
    }

    /// Puts a message where the session puts its own.
    pub fn say(&mut self, status: impl Into<String>) {
        self.session.say(status);
    }

    /// What to show.
    pub fn view(&self) -> View<'_> {
        self.session.view()
    }

    /// Where you are, as a face keeps it across a reload: the path's steps, outermost first.
    pub fn steps(&self) -> Vec<KeptStep> {
        self.session.steps()
    }

    /// This writer's id, and so the name of the log it writes.
    pub fn device(&self) -> &str {
        &self.device
    }

    /// Whether the key has opened a sealed line, or no sealed line has been seen. Until then
    /// nothing is written.
    pub fn key_verified(&self) -> bool {
        self.sealed_opened > 0 || self.sealed_seen == 0
    }

    /// Sealed lines seen that this key did not open.
    pub fn unopened(&self) -> usize {
        self.unopened
    }

    /// Why the relay refused this device, if it did and has not taken it in since.
    pub fn refused(&self) -> Option<&str> {
        self.refused.as_deref()
    }

    /// Whether this device holds everything the relay said it holds on this connection, as
    /// [`Replica::in_step`] says, which is when a face that syncs once may stop listening.
    pub fn in_step(&self) -> bool {
        self.replica.in_step(RELAY)
    }

    /// Whether the passphrase is in doubt: the key opened no sealed line here though there are
    /// some, or opened none of the lines of the space a relay offered, whatever it opens here, or
    /// the relay refused this device after it answered the challenge, which is a refusal of its
    /// key, and nothing here says the key is right.
    pub fn key_in_doubt(&self) -> bool {
        !self.key_verified()
            || self.shut
            || self.refused.is_some() && self.refused_answer && self.sealed_opened == 0
    }

    /// Starts a new log for this writer, with a header and a snapshot, and retires the old one.
    /// The snapshot is this writer's latest version of every note in its log, tombstones
    /// included, sealed afresh, and then every line of a later format as it was: which of those
    /// a later build would keep is not for this one to say, and dropping them would lose what
    /// that build wrote here.
    pub fn compact(&mut self) -> Todo {
        let mut todo = Todo::default();
        if !self.key_verified() {
            self.session.say(WRONG_KEY);
            return todo;
        }
        let old = self.device.clone();
        let lines = self.replica.lines(&old);
        let mut chain = lines
            .first()
            .and_then(|line| Header::read(line))
            .map_or_else(Vec::new, |header| header.compacts);
        chain.push(old.clone());

        // The last version of each note, in the order of those last versions.
        let mut last: BTreeMap<String, usize> = BTreeMap::new();
        let mut versions: Vec<Option<Record>> = Vec::new();
        let mut later = Vec::new();
        for line in lines {
            match Record::read_line(&self.seal, line) {
                Read::Version(record) => {
                    if let Some(&at) = last.get(&record.id) {
                        versions[at] = None;
                    }
                    last.insert(record.id.clone(), versions.len());
                    versions.push(Some(record));
                }
                Read::Later => later.push(line.clone()),
                Read::Shut | Read::Other => {}
            }
        }
        let written = versions.len();
        let snapshot: Vec<Record> = versions.into_iter().flatten().collect();

        let new = self.new_writer(&old);
        // The new log is marked as this writer's before its header is appended, or the header
        // would read as a peer's and the old log as superseded rather than retired.
        self.replica.writes(&new);
        let mut kept = vec![
            Header {
                compacts: chain,
                snapshot: (snapshot.len() + later.len()) as u64,
            }
            .line(),
        ];
        kept.extend(
            snapshot
                .iter()
                .filter_map(|record| record.sealed(&self.seal, &*self.host).ok()),
        );
        kept.extend(later);
        todo.keep.push(Kept {
            log: new.clone(),
            from: 0,
            lines: kept.clone(),
        });
        let out = self.replica.append(&new, kept);
        self.carry_out(out, &mut todo);
        self.session.writes_as(&new);
        self.device = new.clone();
        todo.device = Some(new);
        self.superseded = false;
        self.session.say(format!(
            "compacted {written} versions into {}",
            snapshot.len()
        ));
        todo
    }

    /// A writer id for a new log: eight hex digits, not one already in use here.
    fn new_writer(&self, old: &str) -> String {
        loop {
            let mut bytes = [0u8; 4];
            self.host.random(&mut bytes);
            let new: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            let taken = new == old
                || !self.replica.lines(&new).is_empty()
                || self.replica.retired().contains(&new);
            if !taken {
                return new;
            }
        }
    }

    /// Compacts, if a peer's header has retired this writer's log, and adds what that asks of
    /// the face after what is there already.
    fn compact_if_superseded(&mut self, todo: &mut Todo) {
        if self.superseded {
            let compacted = self.compact();
            todo.then(compacted);
        }
    }

    /// Reads lines as versions, counting the sealed ones and those this key opened.
    fn read(&mut self, lines: &[String]) -> Vec<Record> {
        let mut versions = Vec::new();
        for line in lines {
            let sealed = is_sealed(line);
            self.sealed_seen += usize::from(sealed);
            match Record::read_line(&self.seal, line) {
                Read::Version(record) => {
                    self.sealed_opened += usize::from(sealed);
                    versions.push(record);
                }
                Read::Later => self.sealed_opened += 1,
                Read::Shut => self.unopened += 1,
                Read::Other => {}
            }
        }
        versions
    }

    /// Does what the replica wants done. Lines that came from a peer are for the face to keep
    /// and for the session to read.
    fn carry_out(&mut self, out: Vec<Out>, todo: &mut Todo) {
        for out in out {
            match out {
                Out::Keep { log, lines } => {
                    let versions = self.read(&lines);
                    self.session.absorb(versions);
                    todo.caret.then(self.session.take_caret());
                    // A key that has just been found not to open the space is said so at
                    // once, and not only when a write is tried.
                    if !self.key_verified() {
                        self.session.say(WRONG_KEY);
                    }
                    let held = self.replica.lines(&log).len();
                    let from = held.saturating_sub(lines.len()) as u64;
                    todo.keep.push(Kept { log, from, lines });
                }
                Out::Send(to, message) => {
                    if let Ok(text) = message.encode() {
                        todo.send.push(Said { to, text });
                    }
                }
                // A fork of this device's own log is two machines writing under one id. The
                // way out is a new log of this device's own, whose header retires the shared
                // name; the other machine is superseded by it and does the same.
                Out::Forked { log, .. } if log == self.device => {
                    self.superseded = true;
                    self.session.say(format!(
                        "the log {log} here is not the {log} elsewhere, so a new one is started"
                    ));
                }
                Out::Forked { log, .. } => self.session.say(format!(
                    "the log {log} here is not the {log} elsewhere, so it is not syncing"
                )),
                Out::Retire(log) => todo.delete.push(log),
                Out::Superseded(log) => {
                    if log == self.device {
                        self.superseded = true;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// A machine whose clock has stopped at midday on 2026-10-13, or `ahead` seconds after it,
    /// and whose ids and randomness count up from where they are told to start.
    struct Stopped {
        name: &'static str,
        ids: Cell<u32>,
        random: Cell<u8>,
        ahead: i64,
    }

    impl Host for Stopped {
        fn now(&self) -> Moment {
            Moment::utc(1_791_892_800 + self.ahead)
        }

        fn new_id(&self) -> String {
            self.ids.set(self.ids.get() + 1);
            format!("{}-{:04}", self.name, self.ids.get())
        }

        fn random(&self, buf: &mut [u8]) {
            for byte in buf {
                self.random.set(self.random.get().wrapping_add(1));
                *byte = self.random.get();
            }
        }
    }

    fn key() -> Key {
        Key::from_bytes([7; 32])
    }

    fn host(name: &'static str, random: u8) -> Box<dyn Host> {
        Box::new(Stopped {
            name,
            ids: Cell::new(0),
            random: Cell::new(random),
            ahead: 0,
        })
    }

    /// A device whose notes' ids start with its name.
    fn device(name: &'static str) -> Device {
        Device::new(host(name, 0), name, Weights::default(), key())
    }

    /// A second device writing under `writer`, with notes and randomness of its own, as when a
    /// directory is copied to another machine.
    fn twin(writer: &str, name: &'static str, random: u8) -> Device {
        Device::new(host(name, random), writer, Weights::default(), key())
    }

    fn write(device: &mut Device, text: &str) -> Todo {
        device.act(Action::Type(text.to_owned()));
        device.act(Action::Write)
    }

    fn heads(device: &Device) -> Vec<String> {
        device.view().rows.iter().map(|r| r.head.clone()).collect()
    }

    fn status(device: &Device) -> String {
        device.view().status.to_owned()
    }

    fn opened(line: &str) -> Record {
        match Record::read_line(&Seal::new(key()), line) {
            Read::Version(record) => record,
            other => panic!("{other:?}"),
        }
    }

    /// A version written before sealing, as a plain line.
    fn plain(id: &str, text: &str, secs: i64) -> String {
        Record {
            created: Some(Moment::utc(secs)),
            updated: Some(Moment::utc(secs)),
            stamp: Some(Moment::utc(secs)),
            device: "dev-a".to_owned(),
            ..Record::new(id, text)
        }
        .line()
        .unwrap()
    }

    /// `versions` sealed versions of `notes` notes, round and round.
    fn sealed_versions(versions: usize, notes: usize) -> Vec<String> {
        let seal = Seal::new(key());
        let host = Stopped {
            name: "dev-a",
            ids: Cell::new(0),
            random: Cell::new(0),
            ahead: 0,
        };
        (0..versions)
            .map(|n| {
                let line = plain(
                    &format!("note-{}", n % notes),
                    &format!("garden {n}"),
                    n as i64,
                );
                let record = Record::read(line.as_bytes()).unwrap();
                record.sealed(&seal, &host).unwrap()
            })
            .collect()
    }

    /// Versions of `notes` notes as a later build would seal them: this key opens them, and this
    /// build cannot read what is inside.
    fn later_versions(notes: usize) -> Vec<String> {
        let (seal, host) = (Seal::new(key()), host("dev-a", 0));
        (0..notes)
            .map(|n| seal.seal(&format!(r#"{{"v":99,"id":"note-{n}"}}"#), &*host))
            .collect()
    }

    /// A relay between devices: what each device sends goes to it, and what it sends goes to
    /// them. Two devices may also be siblings, as two tabs are, each a peer of the other.
    struct Net {
        relay: Replica,
        devices: Vec<Device>,
        /// Each pair of siblings, and the peer each numbers the other by.
        siblings: Vec<[(usize, Peer); 2]>,
    }

    impl Net {
        fn new(devices: Vec<Device>) -> Self {
            Self {
                relay: Replica::new(),
                devices,
                siblings: Vec::new(),
            }
        }

        fn connect(&mut self, device: usize) {
            let greeting = self.relay.connect(device as Peer);
            let todo = self.devices[device].connected(RELAY);
            self.deliver(device, todo);
            self.devices_hear(greeting);
        }

        /// Makes two devices siblings, each numbering the other by the next peer it has.
        fn sibling(&mut self, a: usize, b: usize) {
            let next = |d: usize| {
                let had = self.siblings.iter().flatten().filter(|(x, _)| *x == d);
                1 + had.count() as Peer
            };
            let (calls_b, calls_a) = (next(a), next(b));
            self.siblings.push([(a, calls_b), (b, calls_a)]);
            let (to_b, to_a) = (
                self.devices[a].connected(calls_b),
                self.devices[b].connected(calls_a),
            );
            self.deliver(a, to_b);
            self.deliver(b, to_a);
        }

        /// What a device has said reaches the peer it is for: the relay, or a sibling.
        fn deliver(&mut self, from: usize, todo: Todo) {
            for said in todo.send {
                if said.to == RELAY {
                    let message = Message::decode(&said.text).unwrap();
                    let out = self.relay.receive(from as Peer, message);
                    self.devices_hear(out);
                    continue;
                }
                let (to, back) = self
                    .siblings
                    .iter()
                    .find_map(|[x, y]| match () {
                        _ if *x == (from, said.to) => Some(*y),
                        _ if *y == (from, said.to) => Some(*x),
                        _ => None,
                    })
                    .expect("a sibling");
                let todo = self.devices[to].received(back, &said.text);
                self.deliver(to, todo);
            }
        }

        fn devices_hear(&mut self, out: Vec<Out>) {
            for out in out {
                if let Out::Send(peer, message) = out {
                    let text = message.encode().unwrap();
                    let todo = self.devices[peer as usize].received(RELAY, &text);
                    self.deliver(peer as usize, todo);
                }
            }
        }

        fn act(&mut self, device: usize, action: Action) {
            let todo = self.devices[device].act(action);
            self.deliver(device, todo);
        }

        /// Every device loses the relay, to write on its own until it is connected again.
        fn part(&mut self) {
            for (i, device) in self.devices.iter_mut().enumerate() {
                self.relay.disconnect(i as Peer);
                device.disconnected(RELAY);
            }
        }

        fn rejoin(&mut self) {
            for i in 0..self.devices.len() {
                self.connect(i);
            }
        }

        /// The heads each device lists, having checked that they all list the same.
        fn agreed(&self) -> Vec<String> {
            let all: Vec<Vec<String>> = self.devices.iter().map(heads).collect();
            assert!(all.windows(2).all(|w| w[0] == w[1]), "{all:?}");
            all[0].clone()
        }
    }

    /// Two devices and a relay, each device having written these notes from the line in turn,
    /// and each come back to the line.
    fn pair(texts: &[&str]) -> Net {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b")]);
        net.rejoin();
        for text in texts {
            net.act(0, Action::Type((*text).to_owned()));
            net.act(0, Action::Write);
            net.act(0, Action::Select(None));
        }
        net
    }

    /// Opens a new row under the row the cursor is on and writes a note in it.
    fn under(net: &mut Net, device: usize, text: &str) {
        net.act(device, Action::Write);
        net.act(device, Action::Edited(text.to_owned()));
    }

    #[test]
    fn what_is_written_is_kept_sealed_as_a_line_of_this_devices_log() {
        let mut a = device("dev-a");
        let todo = write(&mut a, "boiler service booked");
        assert_eq!(heads(&a), ["boiler service booked"]);
        let [kept] = &todo.keep[..] else {
            panic!("{todo:?}");
        };
        assert_eq!((kept.log.as_str(), kept.from), ("dev-a", 0));
        let line = &kept.lines[0];
        assert!(is_sealed(line));
        assert_eq!(
            Record::read(line.as_bytes()),
            None,
            "the relay cannot read it"
        );
        let version = opened(line);
        assert_eq!(version.text, "boiler service booked");
        assert_eq!(version.device, "dev-a");
        assert!(!line.contains('\n'));
        assert!(todo.send.is_empty(), "with nobody to send it to");
        assert_eq!(write(&mut a, "pressure dropped").keep[0].from, 1);

        // Opened again, with what the face kept, it is all there and nothing is said about it.
        let mut again = device("dev-a");
        assert_eq!(
            again.load("dev-a", vec![format!("{line}\n")]),
            Todo::default()
        );
        assert_eq!(again.opened(), Todo::default(), "nothing to compact");
        assert_eq!(heads(&again), ["boiler service booked"]);
        assert_eq!(status(&again), "");
        assert!(again.key_verified());
        assert_eq!(write(&mut again, "and one more").keep[0].from, 1);
    }

    #[test]
    fn a_device_filled_from_several_logs_says_nothing_of_it() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b"), device("dev-c")]);
        net.connect(0);
        net.connect(1);
        net.act(0, Action::Type("bleed the radiators".into()));
        net.act(0, Action::Write);
        net.act(1, Action::Type("order oil".into()));
        net.act(1, Action::Write);
        net.connect(2);
        assert_eq!(heads(&net.devices[2]).len(), 2);
        assert_eq!(status(&net.devices[2]), "");
    }

    #[test]
    fn two_devices_and_a_relay_show_each_other_what_they_write() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b")]);
        write(&mut net.devices[0], "written before anyone was connected");
        net.connect(0);
        net.connect(1);
        assert_eq!(
            heads(&net.devices[1]),
            ["written before anyone was connected"]
        );
        // A device that held nothing is filled, which is not news.
        assert_eq!(status(&net.devices[1]), "");

        net.act(1, Action::Type("engineer quoted 240".into()));
        net.act(1, Action::Write);
        assert_eq!(status(&net.devices[0]), "1 changed elsewhere");
        net.act(0, Action::Type("quoted".into()));
        assert_eq!(heads(&net.devices[0]), ["engineer quoted 240"]);

        // Going into a note and writing there links to it, on the other device too.
        net.act(0, Action::Move(1));
        net.act(0, Action::GoIn);
        net.act(0, Action::Type("booked for friday".into()));
        net.act(0, Action::Write);
        net.act(1, Action::Type("booked".into()));
        let view = net.devices[1].view();
        assert_eq!(view.rows[0].head, "booked for friday");
        assert_eq!(view.rows[0].links, ["engineer quoted 240"]);
        assert_eq!(net.relay.have().len(), 2, "a log to a device");
        for log in net.relay.have().keys() {
            for line in net.relay.lines(log) {
                assert!(is_sealed(line));
                assert_eq!(Record::read(line.as_bytes()), None);
            }
        }
    }

    #[test]
    fn a_device_opened_afresh_gets_its_own_log_back_from_the_relay() {
        let mut net = Net::new(vec![device("dev-a")]);
        net.connect(0);
        net.act(0, Action::Type("kept only by the relay".into()));
        net.act(0, Action::Write);

        // The face's storage is cleared, and the writer's id with it or not: here not.
        net.relay.disconnect(0);
        net.devices[0] = device("dev-a");
        let greeting = net.relay.connect(0);
        let todo = net.devices[0].connected(RELAY);
        net.deliver(0, todo);
        net.devices_hear(greeting);
        assert_eq!(heads(&net.devices[0]), ["kept only by the relay"]);
    }

    #[test]
    fn the_rest_is_handed_to_the_face() {
        let mut a = device("dev-a");
        write(&mut a, "see https://example.com/boiler");
        a.act(Action::Select(None));
        assert_eq!(
            a.act(Action::Editor),
            Todo::default(),
            "nothing under the cursor"
        );
        a.act(Action::Move(1));
        let todo = a.act(Action::Editor);
        let editing = todo.editor.unwrap();
        assert_eq!(editing.text, "see https://example.com/boiler");
        assert_eq!(
            a.act(Action::Copy).copy.as_deref(),
            Some("https://example.com/boiler")
        );
        assert_eq!(
            a.act(Action::OpenUrl).open.as_deref(),
            Some("https://example.com/boiler")
        );
        let saved = a.act(Action::Save {
            id: editing.id,
            text: "see the manual".into(),
        });
        assert_eq!(saved.keep.len(), 1);
        assert_eq!(heads(&a), ["see the manual"]);

        // What is not a message is passed over. A fork of another writer's log is said; a fork
        // of this device's own is put right there and then, with a new log.
        assert_eq!(a.received(RELAY, "not a message"), Todo::default());
        a.connected(RELAY);
        a.received(RELAY, r#"{"forked":"dev-z"}"#);
        assert!(
            status(&a).starts_with("the log dev-z here is not"),
            "{}",
            status(&a)
        );
        let todo = a.received(RELAY, r#"{"forked":"dev-a"}"#);
        let new = todo.device.clone().expect("a new writer id");
        assert_ne!(new, "dev-a");
        assert_eq!(a.device(), new);
        assert_eq!(todo.delete, ["dev-a"]);
        assert!(status(&a).starts_with("compacted"), "{}", status(&a));
        assert_eq!(heads(&a), ["see the manual"]);
    }

    #[test]
    fn what_another_writer_here_added_is_taken_in_and_sent_on_but_not_kept_again() {
        let mut net = pair(&["[ ] Order oil #house"]);
        // A second writer on the first device's machine, which reads what is kept there and
        // writes a log of its own, with no relay.
        let mut beside = device("dev-c");
        beside.load("dev-a", net.devices[0].replica.lines("dev-a").to_vec());
        let (todo, ticked) = beside.tick(&["dev-a-0001".to_owned()], true).unwrap();
        assert_eq!(
            (ticked[0].text.as_str(), ticked[0].archived),
            ("[x] Order oil #house", true)
        );
        let [kept] = &todo.keep[..] else {
            panic!("{todo:?}");
        };
        assert_eq!((kept.log.as_str(), kept.from), ("dev-c", 0));
        assert_eq!(opened(&kept.lines[0]), ticked[0]);
        assert!(todo.send.is_empty());

        let added = net.devices[0].added("dev-c", kept.lines.clone());
        assert!(added.keep.is_empty(), "it is kept already");
        assert_eq!(status(&net.devices[0]), "1 changed elsewhere");
        net.deliver(0, added);
        assert_eq!(net.relay.lines("dev-c"), kept.lines);
        assert!(
            net.agreed().is_empty(),
            "ticked, and so archived, everywhere"
        );
    }

    #[test]
    fn notes_applied_beside_the_app_are_sealed_into_a_log_of_their_own_and_given_back() {
        let net = pair(&["Boiler #house"]);
        let mut beside = device("dev-c");
        beside.load("dev-a", net.devices[0].replica.lines("dev-a").to_vec());
        let given = vec![
            Given {
                id: Some("dev-a-0001".into()),
                text: Some("Boiler service #house".into()),
                ..Given::default()
            },
            Given {
                text: Some("Bleed the radiators #house".into()),
                ..Given::default()
            },
        ];
        let (todo, notes) = beside.apply(given).unwrap();
        assert_eq!(
            notes.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
            ["Boiler service #house", "Bleed the radiators #house"]
        );
        let kept: Vec<(&str, Record)> = todo
            .keep
            .iter()
            .flat_map(|kept| {
                kept.lines
                    .iter()
                    .map(|line| (kept.log.as_str(), opened(line)))
            })
            .collect();
        assert_eq!(
            kept,
            [("dev-c", notes[0].clone()), ("dev-c", notes[1].clone())]
        );

        let mut shut = Device::new(
            host("dev-d", 0),
            "dev-d",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        shut.load("dev-a", net.devices[0].replica.lines("dev-a").to_vec());
        assert_eq!(
            shut.apply(vec![Given::default()]),
            Err(WRONG_KEY.to_owned())
        );
    }

    #[test]
    fn a_todo_is_json_with_every_field_named() {
        let json = serde_json::to_string(&Todo::default()).unwrap();
        assert_eq!(
            json,
            r#"{"keep":[],"delete":[],"send":[],"device":null,"editor":null,"copy":null,"open":null,"caret":{"line":null,"head":null}}"#
        );
        let said = Todo {
            send: vec![Said {
                to: 2,
                text: "{}".into(),
            }],
            ..Todo::default()
        };
        assert!(
            serde_json::to_string(&said)
                .unwrap()
                .contains(r#""send":[{"to":2,"text":"{}"}]"#)
        );
    }

    #[test]
    fn two_tabs_on_one_browser_show_each_other_each_keystroke_with_no_relay() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b")]);
        net.sibling(0, 1);
        net.act(0, Action::Write);
        for text in ["B", "Bo", "Boiler #house"] {
            net.act(0, Action::Edited(text.into()));
            assert_eq!(heads(&net.devices[1]), [text]);
        }
        // What the second keeps of the first's log is where the first wrote it.
        let todo = net.devices[0].act(Action::Edited("Boiler #house, serviced".into()));
        let [Said { to, text }] = &todo.send[..] else {
            panic!("{todo:?}");
        };
        let kept = net.devices[1].received(1, text).keep;
        assert_eq!(*to, 1);
        assert_eq!(
            (kept[0].log.as_str(), kept[0].from, kept[0].lines.len()),
            ("dev-a", 3, 1)
        );
        assert!(net.relay.have().is_empty(), "no relay was there");
    }

    #[test]
    fn a_siblings_lines_reach_the_relay_through_whichever_tab_is_connected() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b"), device("dev-c")]);
        net.sibling(0, 1);
        net.connect(1);
        net.connect(2);
        write(&mut net.devices[0], "written offline");
        net.act(0, Action::Write);
        net.act(0, Action::Edited("Boiler #house".into()));
        assert_eq!(net.relay.lines("dev-a").len(), 2, "through the second tab");
        assert_eq!(
            heads(&net.devices[2]),
            ["written offline", "Boiler #house"],
            "and on to another device"
        );
    }

    #[test]
    fn a_line_from_the_relay_reaches_every_sibling_through_the_one_that_heard_it() {
        let mut net = Net::new(vec![
            device("dev-a"),
            device("dev-b"),
            device("dev-c"),
            device("dev-z"),
        ]);
        net.sibling(0, 1);
        net.sibling(1, 2);
        net.connect(0);
        net.connect(3);
        write(&mut net.devices[3], "Order oil #house");
        net.act(3, Action::Select(None));
        net.connect(3);
        for i in 0..3 {
            assert_eq!(heads(&net.devices[i]), ["Order oil #house"], "tab {i}");
        }
        assert_eq!(net.devices[2].replica.lines("dev-z").len(), 1);
    }

    #[test]
    fn a_sibling_that_compacts_is_followed_and_the_old_log_is_told_to_be_deleted_everywhere() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b")]);
        net.sibling(0, 1);
        net.connect(1);
        write(&mut net.devices[0], "Boiler #house");
        net.act(0, Action::Select(None));
        let todo = net.devices[0].compact();
        let new = todo.device.clone().unwrap();
        assert_eq!(todo.delete, ["dev-a"]);
        let siblings = net.devices[1].replica.retired().clone();
        assert!(!siblings.contains("dev-a"));
        net.deliver(0, todo);
        assert!(net.devices[1].replica.retired().contains("dev-a"));
        assert!(net.devices[1].replica.lines("dev-a").is_empty());
        assert_eq!(net.relay.lines(&new).len(), 2, "a header and the snapshot");
        assert!(net.relay.retired().contains("dev-a"));
        assert_eq!(heads(&net.devices[1]), ["Boiler #house"]);
    }

    /// A relay's nonce: 32 bytes, as base64.
    fn nonce() -> String {
        STANDARD.encode([5; 32])
    }

    fn challenge(claimed: bool) -> String {
        offering(claimed, Vec::new())
    }

    /// A challenge that offers lines of the space.
    fn offering(claimed: bool, lines: Vec<String>) -> String {
        Message::Challenge {
            nonce: nonce(),
            claimed,
            lines,
        }
        .encode()
        .unwrap()
    }

    /// What a device sends the relay, as messages.
    fn sent(todo: &Todo) -> Vec<Message> {
        todo.send
            .iter()
            .map(|said| {
                assert_eq!(said.to, RELAY);
                Message::decode(&said.text).unwrap()
            })
            .collect()
    }

    #[test]
    fn a_device_told_the_relay_holds_its_auth_key_answers_with_the_nonce_sealed_under_it() {
        let mut a = device("dev-a");
        write(&mut a, "Boiler");
        a.connected(RELAY);
        let todo = a.received(RELAY, &challenge(true));
        let [Message::Proof(line), Message::Have(have)] = &sent(&todo)[..] else {
            panic!("{todo:?}");
        };
        assert_eq!(Seal::new(key().auth()).open(line), Opened::Text(nonce()));
        assert_eq!(
            Seal::new(key()).open(line),
            Opened::Shut,
            "not the key itself"
        );
        assert_eq!(have, &a.replica.have(), "and then what it holds, again");

        // A nonce that is not one is not sealed.
        let odd = Message::Challenge {
            nonce: "anything at all".into(),
            claimed: true,
            lines: Vec::new(),
        };
        assert!(a.received(RELAY, &odd.encode().unwrap()).send.is_empty());
    }

    #[test]
    fn a_key_the_spaces_line_does_not_open_stays_refused_and_in_doubt() {
        let mut c = device("dev-c");
        let line = write(&mut c, "Boiler").keep.remove(0).lines.remove(0);
        let timeout = Message::Refused("no proof of the key came, so the relay let go".into())
            .encode()
            .unwrap();

        // A fresh device with the passphrase mistyped hears the relay give up on it.
        let mistyped = || {
            Device::new(
                host("dev-b", 0),
                "dev-b",
                Weights::default(),
                Key::from_bytes([9; 32]),
            )
        };
        let mut b = mistyped();
        b.connected(RELAY);
        b.received(RELAY, &offering(false, vec![line.clone()]));
        b.received(RELAY, &timeout);
        b.disconnected(RELAY);
        b.connected(RELAY);
        assert_eq!(b.refused(), Some(NOT_THE_SPACES));
        assert_eq!(status(&b), NOT_THE_SPACES);
        assert!(b.key_in_doubt());

        // One that has its own lines, which its key opens, is in doubt all the same.
        let mut d = mistyped();
        let own = write(&mut d, "Garden").keep.remove(0).lines;
        let mut d = mistyped();
        d.load("dev-b", own);
        assert!(d.key_verified() && !d.key_in_doubt());
        d.connected(RELAY);
        d.received(RELAY, &offering(false, vec![line]));
        assert!(d.key_in_doubt());

        // A relay that takes it in after all clears it.
        d.received(RELAY, r#"{"have":{}}"#);
        assert!(!d.key_in_doubt() && d.refused().is_none());
    }

    #[test]
    fn a_second_challenge_after_being_let_in_is_answered_as_the_first() {
        let mut net = pair(&["Boiler"]);
        let a = &mut net.devices[0];
        let first = sent(&a.received(RELAY, &challenge(true)));
        a.received(RELAY, r#"{"have":{}}"#);
        let second = sent(&a.received(RELAY, &challenge(true)));
        assert_eq!(second.len(), 2);
        assert!(matches!(second[0], Message::Proof(_)));
        assert_eq!(first[1], second[1], "what it holds, again");
        assert_eq!(heads(a), ["Boiler"]);
        assert_eq!(a.refused(), None);
    }

    #[test]
    fn a_device_told_the_relay_holds_no_auth_key_sends_its_auth_key() {
        let mut a = device("dev-a");
        a.connected(RELAY);
        let todo = a.received(RELAY, &challenge(false));
        let [Message::Claim(auth), Message::Have(_)] = &sent(&todo)[..] else {
            panic!("{todo:?}");
        };
        assert_eq!(Key::decode(auth), Some(key().auth()));
        assert_ne!(Key::decode(auth), Some(key()));

        // A challenge from a sibling is nobody's business, and a key that opens nothing here is
        // not handed over as the space's.
        assert_eq!(a.received(1, &challenge(false)), Todo::default());
        let mut b = Device::new(
            host("dev-b", 0),
            "dev-b",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        let mut c = device("dev-c");
        b.load("dev-c", write(&mut c, "Boiler").keep.remove(0).lines);
        b.connected(RELAY);
        assert!(b.received(RELAY, &challenge(false)).send.is_empty());
        assert!(status(&b).contains("passphrase"), "{}", status(&b));
    }

    #[test]
    fn a_space_claimed_by_a_fresh_device_is_claimed_only_if_its_key_opens_a_line_of_it() {
        let mut c = device("dev-c");
        let line = write(&mut c, "Boiler").keep.remove(0).lines.remove(0);

        let mut opens = device("dev-a");
        opens.connected(RELAY);
        let todo = opens.received(RELAY, &offering(false, vec![line.clone()]));
        assert!(matches!(
            sent(&todo)[..],
            [Message::Claim(_), Message::Have(_)]
        ));

        let mut mistyped = Device::new(
            host("dev-b", 0),
            "dev-b",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        mistyped.connected(RELAY);
        let todo = mistyped.received(RELAY, &offering(false, vec![line]));
        assert!(todo.send.is_empty(), "nothing claimed, nothing more said");
        assert_eq!(
            mistyped.refused(),
            Some("the passphrase does not open this space on the relay")
        );
        assert_eq!(status(&mistyped), mistyped.refused().unwrap());
        assert!(mistyped.key_in_doubt());

        // A log under another key among the space's does not keep out a key that opens one of
        // the others.
        let mut stray = Device::new(
            host("dev-d", 0),
            "dev-d",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        let foreign = write(&mut stray, "Pump").keep.remove(0).lines.remove(0);
        let mut fresh = device("dev-e");
        fresh.connected(RELAY);
        let todo = fresh.received(RELAY, &offering(false, vec![foreign, line_of(&mut c)]));
        assert!(matches!(
            sent(&todo)[..],
            [Message::Claim(_), Message::Have(_)]
        ));
    }

    /// A sealed line a device writes, as the relay would hold it.
    fn line_of(device: &mut Device) -> String {
        write(device, "Garden").keep.remove(0).lines.remove(0)
    }

    #[test]
    fn a_refusal_is_said_and_kept_until_a_relay_takes_the_device_in() {
        let mut net = pair(&["Boiler"]);
        let why = "the passphrase does not open main on the relay";
        let refused = Message::Refused(why.into()).encode().unwrap();
        let a = &mut net.devices[1];
        a.received(RELAY, &refused);
        assert_eq!((a.refused(), status(a).as_str()), (Some(why), why));
        assert!(!a.key_in_doubt(), "the key opened the other device's line");
        a.disconnected(RELAY);
        a.connected(RELAY);
        assert_eq!(a.refused(), Some(why), "through the wait to try again");
        a.received(1, r#"{"have":{}}"#);
        assert_eq!(a.refused(), Some(why), "a sibling is not the relay");
        a.received(RELAY, r#"{"have":{}}"#);
        assert_eq!(a.refused(), None);
    }

    #[test]
    fn the_passphrase_is_in_doubt_when_the_relay_refuses_a_device_that_opened_nothing_here() {
        let mut fresh = device("dev-c");
        assert!(!fresh.key_in_doubt());

        // Refused before it has answered, the token or the name is the trouble, not the key.
        fresh.connected(RELAY);
        let token =
            Message::Refused("main is not on the relay, and making it needs a token".into());
        fresh.received(RELAY, &token.encode().unwrap());
        assert!(fresh.refused().is_some());
        assert!(!fresh.key_in_doubt());

        fresh.disconnected(RELAY);
        fresh.connected(RELAY);
        fresh.received(RELAY, &challenge(true));
        let refused = Message::Refused("the passphrase does not open main on the relay".into());
        fresh.received(RELAY, &refused.encode().unwrap());
        assert!(fresh.key_in_doubt());

        let mut b = Device::new(
            host("dev-b", 0),
            "dev-b",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        let mut a = device("dev-a");
        b.load("dev-a", write(&mut a, "Boiler").keep.remove(0).lines);
        assert!(b.key_in_doubt(), "a key that opens nothing here");
    }

    #[test]
    fn a_sibling_gone_is_sent_nothing_and_a_line_is_not_sent_back_where_it_came_from() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b")]);
        net.sibling(0, 1);
        net.act(0, Action::Write);
        let todo = net.devices[0].act(Action::Edited("Boiler".into()));
        let [Said { to: 1, text }] = &todo.send[..] else {
            panic!("{todo:?}");
        };
        let back = net.devices[1].received(1, text);
        assert_eq!(back.keep.len(), 1);
        assert!(back.send.is_empty(), "not back to the sibling it came from");

        net.devices[0].disconnected(1);
        let todo = net.devices[0].act(Action::Edited("Boiler #house".into()));
        assert_eq!(todo.keep.len(), 1);
        assert!(todo.send.is_empty(), "nobody is there");
    }

    #[test]
    fn now_is_taken_only_from_the_relay() {
        let relay = Message::Now(Moment::utc(1_791_892_800 - 600))
            .encode()
            .unwrap();
        let mut device = clocked(0);
        device.connected(1);
        device.received(1, &relay);
        assert_eq!(device.clock(), None, "a sibling's clock is this machine's");
        device.connected(RELAY);
        device.received(RELAY, &relay);
        assert_eq!(
            device.clock().as_deref(),
            Some("this device's clock is 10 min ahead")
        );
        device.disconnected(1);
        assert!(device.clock().is_some(), "a sibling going is not the relay");
        device.disconnected(RELAY);
        assert_eq!(device.clock(), None);
    }

    #[test]
    fn a_fork_is_said_to_be_elsewhere_whichever_peer_told_of_it() {
        let mut a = device("dev-a");
        write(&mut a, "Boiler");
        a.connected(1);
        a.received(1, r#"{"forked":"dev-z"}"#);
        assert_eq!(
            status(&a),
            "the log dev-z here is not the dev-z elsewhere, so it is not syncing"
        );
        let todo = a.received(1, r#"{"forked":"dev-a"}"#);
        assert!(
            status(&a).starts_with("compacted"),
            "{} {todo:?}",
            status(&a)
        );
    }

    #[test]
    fn typing_into_a_head_is_refused_when_the_key_is_not_verified() {
        let mut a = device("dev-a");
        let lines: Vec<String> = write(&mut a, "Boiler").keep.remove(0).lines;
        let mut b = Device::new(
            host("dev-b", 0),
            "dev-b",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        // A plain line from before sealing is read whatever the key, so there is a note to try.
        b.load("dev-x", vec![plain("x-1", "a garden", 100)]);
        b.load("dev-a", lines);
        assert!(!b.key_verified());
        assert_eq!(heads(&b), ["a garden"]);

        // No new note is opened, and a head can be walked to but not changed, so nothing is
        // ever changed that could not be written.
        b.say("");
        assert_eq!(b.act(Action::Write), Todo::default());
        assert!(b.view().field.is_none());
        assert_eq!(status(&b), WRONG_KEY);
        b.act(Action::Move(1));
        assert_eq!(b.view().cursor, Some(0));
        assert_eq!(
            b.act(Action::Edited("a garden gate".into())),
            Todo::default()
        );
        assert_eq!(b.view().field.map(|f| f.text), Some("a garden"));
        assert_eq!(b.act(Action::Open), Todo::default());
        assert!(b.view().field.is_some_and(|f| !f.whole));
        assert_eq!(b.act(Action::Close), Todo::default());

        // With the right key a new note is opened, and is written as one sealed line at its
        // first letter.
        a.act(Action::Type(String::new()));
        a.act(Action::Write);
        assert!(a.view().field.is_some_and(|field| field.fresh.is_some()));
        let typed = a.act(Action::Edited("House".into()));
        assert_eq!(typed.keep.len(), 1);
        assert_eq!(opened(&typed.keep[0].lines[0]).text, "House");
        assert_eq!(a.act(Action::Close).keep.len(), 0, "nothing more to write");
    }

    #[test]
    fn a_body_changed_when_the_key_turns_out_to_be_wrong_can_only_be_put_back() {
        let mut a = device("dev-a");
        let lines: Vec<String> = write(&mut a, "Boiler").keep.remove(0).lines;
        let mut b = Device::new(
            host("dev-b", 0),
            "dev-b",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        // With only a plain line seen, the key is taken on trust and a body can be changed. A
        // head would be written as it was typed; a body waits for it to close.
        b.load("dev-x", vec![plain("x-1", "a garden", 100)]);
        b.act(Action::Move(1));
        b.act(Action::Open);
        b.act(Action::Edited("a garden\ngate".into()));
        assert!(b.key_verified());
        assert!(b.session.unwritten());

        // Then a sealed line arrives that the key does not open. Nothing that would leave the
        // body is done, since leaving writes, and nor is any more typing.
        b.load("dev-a", lines);
        assert!(!b.key_verified());
        b.say("");
        assert_eq!(b.act(Action::Move(1)), Todo::default());
        assert_eq!(status(&b), WRONG_KEY);
        assert_eq!(b.act(Action::Type("house".into())), Todo::default());
        assert_eq!(b.act(Action::Close), Todo::default());
        assert_eq!(
            b.act(Action::Edited("a garden\ngate, painted".into())),
            Todo::default()
        );
        let text = |b: &Device| b.view().field.map(|field| field.text.to_owned());
        assert_eq!(text(&b), Some("a garden\ngate".to_owned()));

        // Putting back what was never written writes nothing, so it is not refused, and then the
        // row is left.
        assert_eq!(b.act(Action::Undo), Todo::default());
        assert_eq!(text(&b), Some("a garden".to_owned()));
        assert_eq!(status(&b), "put back as it was");
        assert_eq!(b.act(Action::Move(-1)), Todo::default());
        assert_eq!(b.view().cursor, None);
    }

    #[test]
    fn typing_into_a_new_row_sends_the_relay_a_line_a_letter() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b")]);
        net.connect(0);
        net.connect(1);
        net.act(0, Action::Write);
        for (n, text) in ["B", "Bo", "Boi"].into_iter().enumerate() {
            net.act(0, Action::Edited(text.into()));
            assert_eq!(net.devices[0].replica.lines("dev-a").len(), n + 1);
            assert_eq!(net.relay.lines("dev-a").len(), n + 1, "sent as it is typed");
            assert_eq!(heads(&net.devices[1]), [text], "and on the other device");
        }
        net.act(0, Action::Move(-1));
        assert_eq!(
            net.relay.lines("dev-a").len(),
            3,
            "and nothing more on leaving"
        );
    }

    #[test]
    fn a_new_row_inside_a_tag_is_in_the_tag_on_another_device_from_its_first_letter() {
        let mut net = Net::new(vec![device("dev-a"), device("dev-b")]);
        net.connect(0);
        net.connect(1);
        for device in 0..2 {
            net.act(device, Action::Type("#house".into()));
            net.act(device, Action::GoIn);
        }

        // Opened and left with only the label in it, it was never anything.
        net.act(0, Action::Write);
        net.act(0, Action::Select(None));
        assert!(net.relay.lines("dev-a").is_empty());

        net.act(0, Action::Write);
        net.act(0, Action::Edited("B #house".into()));
        assert_eq!(net.relay.lines("dev-a").len(), 1);
        assert_eq!(heads(&net.devices[1]), ["B #house"], "listed in its #house");
    }

    #[test]
    fn a_device_with_the_wrong_key_reads_nothing_and_writes_nothing() {
        let mut a = device("dev-a");
        let lines: Vec<String> = ["House", "Boiler"]
            .iter()
            .flat_map(|text| write(&mut a, text).keep)
            .flat_map(|kept| kept.lines)
            .collect();

        let mut b = Device::new(
            host("dev-b", 0),
            "dev-b",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        b.load("dev-a", lines);
        assert_eq!(b.unopened(), 2);
        assert!(!b.key_verified());
        assert!(heads(&b).is_empty());
        assert!(
            status(&b).contains("passphrase"),
            "said as soon as it is seen: {}",
            status(&b)
        );
        assert_eq!(b.opened(), Todo::default(), "nor compacts");
        assert_eq!(write(&mut b, "a garden"), Todo::default());
        assert!(status(&b).contains("passphrase"), "{}", status(&b));
        assert_eq!(
            b.tick(&["dev-a-0001".to_owned()], true),
            Err(WRONG_KEY.to_owned())
        );
        assert!(b.replica.lines("dev-b").is_empty());

        // The same when the sealed lines arrive from the relay: said before any write is tried.
        let wrong = Device::new(
            host("dev-d", 0),
            "dev-d",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        let mut net = Net::new(vec![device("dev-a"), wrong]);
        net.connect(0);
        net.act(0, Action::Type("Boiler".into()));
        net.act(0, Action::Write);
        net.connect(1);
        assert!(!net.devices[1].key_verified());
        assert_eq!(net.devices[1].unopened(), 1);
        assert_eq!(status(&net.devices[1]), WRONG_KEY);

        // Plain lines say nothing about a key, and a space of them takes the first one given.
        let mut c = Device::new(
            host("dev-c", 0),
            "dev-c",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        c.load("dev-x", vec![plain("x-1", "House", 100)]);
        assert!(c.key_verified());
        assert_eq!(c.unopened(), 0);
    }

    #[test]
    fn opening_a_plain_log_compacts_it_into_a_sealed_one() {
        let mut a = device("dev-a");
        let lines = vec![
            plain("n-1", "Boiler", 100),
            plain("n-2", "House", 200),
            plain("n-1", "Boiler serviced", 300),
            "not a line of anything".to_owned(),
        ];
        assert_eq!(a.load("dev-a", lines), Todo::default());
        let before = heads(&a);
        let todo = a.opened();

        let [kept] = &todo.keep[..] else {
            panic!("{todo:?}");
        };
        let new = todo.device.clone().unwrap();
        assert_ne!(new, "dev-a");
        assert_eq!(new.len(), 8);
        assert_eq!(a.device(), new);
        assert_eq!((kept.log.as_str(), kept.from), (new.as_str(), 0));
        assert_eq!(
            Header::read(&kept.lines[0]),
            Some(Header {
                compacts: vec!["dev-a".into()],
                snapshot: 2
            })
        );
        assert_eq!(kept.lines.len(), 3);
        let texts: Vec<String> = kept.lines[1..]
            .iter()
            .inspect(|line| assert_eq!(Record::read(line.as_bytes()), None))
            .map(|line| opened(line).text)
            .collect();
        assert_eq!(texts, ["House", "Boiler serviced"], "the last of each");
        assert_eq!(todo.delete, ["dev-a"]);
        assert!(todo.send.is_empty());
        assert_eq!(heads(&a), before);
        assert_eq!(status(&a), "compacted 3 versions into 2");

        // The next version goes to the new log, under the new name.
        let written = write(&mut a, "a garden");
        assert_eq!(written.keep[0].log, new);
        assert_eq!(written.keep[0].from, 3);
        assert_eq!(opened(&written.keep[0].lines[0]).device, new);

        // Opened again from the new log, there is nothing to do.
        let mut again = twin(&new, "dev-a", 0);
        let mut lines = kept.lines.clone();
        lines.extend(written.keep[0].lines.clone());
        again.load(&new, lines);
        assert_eq!(again.opened(), Todo::default());
        assert_eq!(heads(&again).len(), 3);
        // And a file left from before is to be deleted, not read.
        let left = again.load("dev-a", vec![plain("n-9", "House", 9)]);
        assert_eq!(left.delete, ["dev-a"]);
        assert_eq!(heads(&again).len(), 3);
    }

    #[test]
    fn a_log_grown_past_four_times_its_notes_is_compacted_on_opening() {
        let mut a = device("dev-a");
        a.load("dev-a", sealed_versions(1_500, 10));
        let todo = a.opened();
        assert_eq!(todo.keep.len(), 1);
        assert_eq!(todo.keep[0].lines.len(), 11, "a header and a line a note");
        assert_eq!(todo.delete, ["dev-a"]);

        let mut b = device("dev-a");
        b.load("dev-a", sealed_versions(1_500, 400));
        assert_eq!(b.opened(), Todo::default());
    }

    #[test]
    fn lines_of_a_later_format_say_the_key_is_the_spaces_and_are_not_compacted_away() {
        // A peer's log that a later build wrote: the key opens it, though nothing in it reads.
        let mut b = device("dev-b");
        b.load("dev-a", later_versions(3));
        assert!(b.key_verified());
        assert_eq!(status(&b), "", "the passphrase is not said to be wrong");
        assert_eq!(write(&mut b, "Boiler").keep.len(), 1, "and it writes");

        // This writer's own log, written mostly by a later build on this machine: the lines of
        // that build count as notes, so the log has not grown past them, and a compaction for
        // any other reason keeps them as they are.
        let mut a = device("dev-a");
        let mut lines = sealed_versions(10, 10);
        lines.extend(later_versions(1_200));
        a.load("dev-a", lines.clone());
        assert_eq!(a.opened(), Todo::default());
        let todo = a.compact();
        let kept = &todo.keep[0].lines;
        assert_eq!(Header::read(&kept[0]).unwrap().snapshot, 1_210);
        assert_eq!(kept[11..], lines[10..]);
        assert_eq!(todo.delete, ["dev-a"]);
        assert_eq!(status(&a), "compacted 10 versions into 10");
    }

    #[test]
    fn a_second_compaction_names_the_whole_chain() {
        let mut a = device("dev-a");
        write(&mut a, "Boiler");
        let first = a.compact().device.unwrap();
        write(&mut a, "House");
        let second = a.compact();
        let header = Header::read(&second.keep[0].lines[0]).unwrap();
        assert_eq!(header.compacts, ["dev-a".to_owned(), first.clone()]);
        assert_eq!(header.snapshot, 2);
        assert_eq!(second.delete, [first]);
    }

    #[test]
    fn a_fork_is_put_right_by_compacting_on_both_sides() {
        // One writer's id on two machines: the second starts from the first's lines, then each
        // writes its own. Nobody has to do anything about it: the one the relay tells of the
        // fork starts a new log, and the other is superseded by that log and starts its own.
        let mut a = twin("dev-a", "one", 0);
        let shared = write(&mut a, "Boiler").keep.remove(0).lines;
        let mut b = twin("dev-a", "two", 100);
        b.load("dev-a", shared);
        write(&mut a, "House");
        write(&mut b, "a garden");

        let mut net = Net::new(vec![a, b]);
        net.connect(0);
        net.connect(1);
        let first = net.devices[0].device().to_owned();
        let second = net.devices[1].device().to_owned();
        assert_ne!(second, "dev-a", "told of the fork, so compacted");
        assert_ne!(
            first, "dev-a",
            "superseded by the other's new log, so compacted"
        );
        assert_ne!(second, first);

        net.devices.push(device("dev-c"));
        net.connect(2);
        let mut seen = heads(&net.devices[2]);
        seen.sort();
        assert_eq!(seen, ["Boiler", "House", "a garden"]);
        let have: Vec<String> = net.relay.have().into_keys().collect();
        assert_eq!(have, {
            let mut logs = vec![first, second];
            logs.sort();
            logs
        });
        assert!(net.relay.retired().contains("dev-a"));
        for device in &net.devices[..2] {
            assert!(device.replica.retired().contains("dev-a"));
        }
    }

    #[test]
    fn a_log_another_machine_has_retired_is_compacted_on_opening() {
        let mut a = twin("dev-a", "one", 0);
        let shared = write(&mut a, "Boiler").keep.remove(0).lines;
        let compacted = a.compact();
        let first = compacted.device.unwrap();

        // The second machine kept the shared log, wrote more to it, and was then given the
        // first's new log, which names it.
        let mut b = twin("dev-a", "two", 100);
        b.load("dev-a", shared.clone());
        let mut lines = shared;
        lines.extend(write(&mut b, "a garden").keep.remove(0).lines);
        let mut b = twin("dev-a", "two", 200);
        assert_eq!(b.load("dev-a", lines), Todo::default());
        let header = &compacted.keep[0].lines;
        assert_eq!(b.load(&first, header.clone()), Todo::default());
        let todo = b.opened();
        let second = todo.device.clone().unwrap();
        assert_ne!(second, first);
        assert_eq!(todo.delete, ["dev-a"]);
        let snapshot: Vec<String> = todo.keep[0].lines[1..]
            .iter()
            .map(|line| opened(line).text)
            .collect();
        assert_eq!(snapshot, ["Boiler", "a garden"]);
    }

    #[test]
    fn two_devices_writing_under_one_note_at_once_both_sit_under_it_the_later_nearest() {
        let mut net = pair(&["Boiler"]);
        net.devices[0] = Device::new(
            Box::new(Stopped {
                name: "dev-a",
                ids: Cell::new(1),
                random: Cell::new(0),
                ahead: 60,
            }),
            "dev-a",
            Weights::default(),
            key(),
        );
        net.connect(0);
        net.part();
        for (device, run) in [
            (0, ["Bleed the radiators", "Order oil"]),
            (1, ["House", "a garden"]),
        ] {
            net.act(device, Action::Move(1));
            for text in run {
                under(&mut net, device, text);
            }
            net.act(device, Action::Select(None));
        }
        net.rejoin();
        assert_eq!(
            net.agreed(),
            [
                "Boiler",
                "Bleed the radiators",
                "Order oil",
                "House",
                "a garden"
            ],
            "the clock a minute ahead placed its run later, and so nearer"
        );
    }

    #[test]
    fn an_edit_on_one_device_and_a_move_on_another_both_hold() {
        let mut net = pair(&["Boiler", "House", "a garden"]);
        assert_eq!(net.agreed(), ["a garden", "House", "Boiler"]);
        net.part();
        net.act(0, Action::Move(2));
        net.act(0, Action::Edited("House insurance".into()));
        net.act(0, Action::Select(None));
        net.act(1, Action::Move(2));
        net.act(1, Action::Shift(-1));
        net.act(1, Action::Select(None));
        net.rejoin();
        assert_eq!(net.agreed(), ["House insurance", "a garden", "Boiler"]);
    }

    #[test]
    fn a_cycle_two_devices_make_by_moving_at_once_still_lists_every_note() {
        let mut net = pair(&["Boiler", "House", "a garden"]);
        net.part();
        // One puts the top note under the last, the other the last under the top.
        net.act(0, Action::Move(1));
        net.act(0, Action::Shift(2));
        net.act(1, Action::Move(3));
        net.act(1, Action::Shift(-1));
        assert_eq!(heads(&net.devices[0]), ["House", "Boiler", "a garden"]);
        assert_eq!(heads(&net.devices[1]), ["a garden", "Boiler", "House"]);
        net.act(0, Action::Select(None));
        net.act(1, Action::Select(None));
        net.rejoin();
        let mut listed = net.agreed();
        listed.sort();
        assert_eq!(listed, ["Boiler", "House", "a garden"]);
    }

    #[test]
    fn compacting_keeps_where_each_note_sits() {
        let mut a = device("dev-a");
        write(&mut a, "Boiler");
        a.act(Action::Write);
        a.act(Action::Edited("Order oil".into()));
        a.act(Action::Select(None));
        write(&mut a, "House");
        a.act(Action::Select(None));
        let before = heads(&a);
        assert_eq!(before, ["House", "Boiler", "Order oil"]);
        let todo = a.compact();
        let oil = todo.keep[0].lines[1..]
            .iter()
            .map(|line| opened(line))
            .find(|record| record.text == "Order oil")
            .unwrap();
        assert_eq!(oil.after.as_deref(), Some("dev-a-0001"));
        assert!(oil.placed.is_some());

        let new = todo.device.unwrap();
        let mut again = twin(&new, "dev-a", 0);
        again.load(&new, todo.keep[0].lines.clone());
        assert_eq!(heads(&again), before);
    }

    #[test]
    fn compacting_keeps_when_each_task_was_done() {
        let mut a = device("dev-a");
        write(&mut a, "[ ] Bleed the radiators");
        a.act(Action::Archive);
        let done = a.session.query("\"[x]\"")[0].done;
        assert!(done.is_some());
        let todo = a.compact();
        let bled = todo.keep[0].lines[1..]
            .iter()
            .map(|line| opened(line))
            .find(|record| record.text == "[x] Bleed the radiators")
            .unwrap();
        assert_eq!(bled.done, done);
    }

    /// A device whose clock is `ahead` seconds past the relay's midday.
    fn clocked(ahead: i64) -> Device {
        Device::new(
            Box::new(Stopped {
                name: "dev-a",
                ids: Cell::new(0),
                random: Cell::new(0),
                ahead,
            }),
            "dev-a",
            Weights::default(),
            key(),
        )
    }

    #[test]
    fn the_relays_time_far_off_gives_the_status_and_close_gives_nothing() {
        let relay = Message::Now(Moment::utc(1_791_892_800)).encode().unwrap();
        for (ahead, said) in [
            (0, None),
            (-90, None),
            (120, None),
            (121, Some("this device's clock is 2 min ahead")),
            (300, Some("this device's clock is 5 min ahead")),
            (-3_569, Some("this device's clock is 59 min behind")),
            (3_600, Some("this device's clock is 1 h ahead")),
            (-7_200, Some("this device's clock is 2 h behind")),
            (3 * 86_400, Some("this device's clock is 3 days ahead")),
        ] {
            let mut device = clocked(ahead);
            device.connected(RELAY);
            assert!(device.received(RELAY, &relay).send.is_empty());
            assert_eq!(device.clock().as_deref(), said, "{ahead} s");
            assert_eq!(status(&device), "", "a passing word is not used for it");
        }

        let mut device = clocked(300);
        device.connected(RELAY);
        device.received(RELAY, &relay);
        device.disconnected(RELAY);
        assert_eq!(device.clock(), None, "learnt again on connecting");

        // A time no clock could show is still only said.
        device.received(RELAY, r#"{"now":-9223372036854775808}"#);
        assert!(device.clock().unwrap().ends_with("days ahead"));
    }

    #[test]
    fn a_hello_without_the_field_gives_nothing() {
        // A relay from before clocks says what it holds first, and nothing of the time.
        let mut device = clocked(3_600);
        device.connected(RELAY);
        let have = Message::Have(BTreeMap::new()).encode().unwrap();
        device.received(RELAY, &have);
        assert_eq!(device.clock(), None);
    }
}
