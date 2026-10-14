//! A device: a session and a replica, joined in memory, with the space's key.
//!
//! The session knows notes and the replica knows logs, and neither should know the other. They
//! have to meet somewhere, and once they met at the files: the app wrote versions into its log, a
//! thread sent what it found there, and what the thread brought the app found in turn. That made
//! two readers of one directory, a lock on every write and a way for the replica to go stale. A
//! browser tab has no files, so they were joined in memory for it, and that join turned out to
//! be the general thing: the same code serves the terminal, a tab and a test. A version the
//! session wants kept is sealed and becomes a line of this writer's log, which the replica holds
//! and sends on; a line the replica takes from a peer is opened and given to the session. The
//! face is left with what only it can do: keeping lines for the next time, and the socket.
//!
//! What the face is to do comes back as a [`Todo`], and it does the keeping before the deleting.
//! A compaction starts a new log whose header retires the old one, so one `Todo` can hold both
//! the new log and the old log's deletion. Done the other way round, a face stopped between the
//! two would have neither, and the notes the old log held would be on no disk.
//!
//! A device writes nothing until it knows its key is the space's. A line sealed under a second
//! key would split the space in two, readable on some devices and shut on the rest. So the key
//! is checked against what is there: once a sealed line has been seen, a key that has opened
//! none is the wrong one, and the device says so and writes nothing. A space with no sealed line
//! yet takes the first key it is given.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use serde::Serialize;

use crate::record::{Notes, Read, Record};
use crate::replica::{Header, Message, Out, Peer, Replica};
use crate::score::Weights;
use crate::seal::{Key, Seal, is_sealed};
use crate::session::{Action, Effect, Host, Session, View};
use crate::time::Moment;

/// The relay, as a device numbers its one peer.
const RELAY: Peer = 0;

/// A writer's log longer than this, and more than [`GROWTH`] times as long as the notes in it,
/// is compacted when the device opens.
const COMPACT_LINES: usize = 1_000;
const GROWTH: usize = 4;

/// What a device says when it will not write.
const WRONG_KEY: &str = "the passphrase does not open this space, so nothing is written";

/// Lines to keep at the end of a log, the first of them at `from`. A line has no newline.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct Kept {
    pub log: String,
    pub from: u64,
    pub lines: Vec<String>,
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
    /// Messages for the relay, each one to send as it is.
    pub send: Vec<String>,
    /// This writer's new id, when it has changed, for the face to keep in place of the old.
    pub device: Option<String>,
    /// A note to edit. What is made of it comes back as the action `save`.
    pub edit: Option<Editing>,
    /// Text for the clipboard.
    pub copy: Option<String>,
    /// A URL to open.
    pub open: Option<String>,
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
        self.edit = self.edit.take().or(after.edit);
        self.copy = self.copy.take().or(after.copy);
        self.open = self.open.take().or(after.open);
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
            seal: Seal::new(key),
            device,
            sealed_seen: 0,
            sealed_opened: 0,
            unopened: 0,
            superseded: false,
        }
    }

    /// Takes in lines the face kept from before, a log at a time, before anything else is asked
    /// of the device. Trailing newlines are tolerated.
    pub fn load(&mut self, log: &str, lines: Vec<String>) -> Todo {
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
        }
        // A device opened with its own notes says nothing of them having changed, unless what
        // it was given is sealed under some other key.
        self.session
            .say(if self.key_verified() { "" } else { WRONG_KEY });
        todo
    }

    /// Once, after every `load` and before `connected`: compacts this writer's log if that is
    /// due. It is due when a peer's log has retired it, when it holds lines from before sealing,
    /// or when it has grown past [`GROWTH`] times the notes in it.
    pub fn opened(&mut self) -> Todo {
        if !self.key_verified() {
            return Todo::default();
        }
        let own = self.replica.lines(&self.device);
        let mut plain = false;
        let mut ids = BTreeSet::new();
        for line in own {
            if let Read::Version(record) = Record::read_line(&self.seal, line) {
                plain |= !is_sealed(line);
                ids.insert(record.id);
            }
        }
        let grown = own.len() > COMPACT_LINES && own.len() > GROWTH * ids.len();
        if self.superseded || plain || grown {
            self.compact()
        } else {
            Todo::default()
        }
    }

    /// Does what was asked. Nothing that writes is done until the key is known to be the
    /// space's.
    pub fn act(&mut self, action: Action) -> Todo {
        let mut todo = Todo::default();
        if action.writes() && !self.key_verified() {
            self.session.say(WRONG_KEY);
            return todo;
        }
        for effect in self.session.act(action) {
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
                Effect::Edit { id, text } => todo.edit = Some(Editing { id, text }),
                Effect::Copy(text) => todo.copy = Some(text),
                Effect::Open(url) => todo.open = Some(url),
            }
        }
        self.compact_if_superseded(&mut todo);
        todo
    }

    /// The socket to the relay has opened.
    pub fn connected(&mut self) -> Todo {
        let mut todo = Todo::default();
        let out = self.replica.connect(RELAY);
        self.carry_out(out, &mut todo);
        self.compact_if_superseded(&mut todo);
        todo
    }

    /// The socket to the relay has closed.
    pub fn disconnected(&mut self) {
        self.replica.disconnect(RELAY);
    }

    /// The relay has said something. What this build does not know is passed over.
    pub fn received(&mut self, text: &str) -> Todo {
        let mut todo = Todo::default();
        if let Some(message) = Message::decode(text) {
            let out = self.replica.receive(RELAY, message);
            self.carry_out(out, &mut todo);
        }
        self.compact_if_superseded(&mut todo);
        todo
    }

    /// Puts a message where the session puts its own.
    pub fn say(&mut self, status: impl Into<String>) {
        self.session.say(status);
    }

    /// What to show.
    pub fn view(&self) -> View<'_> {
        self.session.view()
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

    /// Starts a new log for this writer, with a header and a snapshot, and retires the old one.
    /// The snapshot is this writer's latest version of every note in its log, tombstones
    /// included, sealed afresh.
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
        for line in lines {
            if let Read::Version(record) = Record::read_line(&self.seal, line) {
                if let Some(&at) = last.get(&record.id) {
                    versions[at] = None;
                }
                last.insert(record.id.clone(), versions.len());
                versions.push(Some(record));
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
                snapshot: snapshot.len() as u64,
            }
            .line(),
        ];
        kept.extend(
            snapshot
                .iter()
                .filter_map(|record| record.sealed(&self.seal, &*self.host).ok()),
        );
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
                Read::Shut => self.unopened += 1,
                Read::Other => {}
            }
        }
        versions
    }

    /// Does what the replica wants done. Lines that came from the relay are for the face to
    /// keep and for the session to read.
    fn carry_out(&mut self, out: Vec<Out>, todo: &mut Todo) {
        for out in out {
            match out {
                Out::Keep { log, lines } => {
                    let versions = self.read(&lines);
                    self.session.absorb(versions);
                    // A key that has just been found not to open the space is said so at
                    // once, and not only when a write is tried.
                    if !self.key_verified() {
                        self.session.say(WRONG_KEY);
                    }
                    let held = self.replica.lines(&log).len();
                    let from = held.saturating_sub(lines.len()) as u64;
                    todo.keep.push(Kept { log, from, lines });
                }
                Out::Send(_, message) => {
                    if let Ok(text) = message.encode() {
                        todo.send.push(text);
                    }
                }
                // A fork of this device's own log is two machines writing under one id. The
                // way out is a new log of this device's own, whose header retires the shared
                // name; the other machine is superseded by it and does the same.
                Out::Forked { log, .. } if log == self.device => {
                    self.superseded = true;
                    self.session.say(format!(
                        "the log {log} here is not the {log} on the relay, so a new one is started"
                    ));
                }
                Out::Forked { log, .. } => self.session.say(format!(
                    "the log {log} here is not the {log} on the relay, so it is not syncing"
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

    /// A machine whose clock has stopped at midday on 2026-10-13, and whose ids and randomness
    /// count up from where they are told to start.
    struct Stopped {
        name: &'static str,
        ids: Cell<u32>,
        random: Cell<u8>,
    }

    impl Host for Stopped {
        fn now(&self) -> Moment {
            Moment::utc(1_791_892_800)
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
            id: id.to_owned(),
            text: text.to_owned(),
            links: Vec::new(),
            created: Some(Moment::utc(secs)),
            updated: Some(Moment::utc(secs)),
            stamp: Some(Moment::utc(secs)),
            archived: false,
            deleted: false,
            device: "dev-a".to_owned(),
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

    /// A relay between devices: what each device sends goes to it, and what it sends goes to
    /// them.
    struct Net {
        relay: Replica,
        devices: Vec<Device>,
    }

    impl Net {
        fn connect(&mut self, device: usize) {
            let greeting = self.relay.connect(device as Peer);
            let todo = self.devices[device].connected();
            self.relay_hears(device, todo);
            self.devices_hear(greeting);
        }

        fn relay_hears(&mut self, device: usize, todo: Todo) {
            for text in todo.send {
                let message = Message::decode(&text).unwrap();
                let out = self.relay.receive(device as Peer, message);
                self.devices_hear(out);
            }
        }

        fn devices_hear(&mut self, out: Vec<Out>) {
            for out in out {
                if let Out::Send(peer, message) = out {
                    let todo = self.devices[peer as usize].received(&message.encode().unwrap());
                    self.relay_hears(peer as usize, todo);
                }
            }
        }

        fn act(&mut self, device: usize, action: Action) {
            let todo = self.devices[device].act(action);
            self.relay_hears(device, todo);
        }
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
    fn two_devices_and_a_relay_show_each_other_what_they_write() {
        let mut net = Net {
            relay: Replica::new(),
            devices: vec![device("dev-a"), device("dev-b")],
        };
        write(&mut net.devices[0], "written before anyone was connected");
        net.connect(0);
        net.connect(1);
        assert_eq!(
            heads(&net.devices[1]),
            ["written before anyone was connected"]
        );
        assert_eq!(status(&net.devices[1]), "1 changed elsewhere");

        net.act(1, Action::Type("engineer quoted 240".into()));
        net.act(1, Action::Write);
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
        let mut net = Net {
            relay: Replica::new(),
            devices: vec![device("dev-a")],
        };
        net.connect(0);
        net.act(0, Action::Type("kept only by the relay".into()));
        net.act(0, Action::Write);

        // The face's storage is cleared, and the writer's id with it or not: here not.
        net.relay.disconnect(0);
        net.devices[0] = device("dev-a");
        let greeting = net.relay.connect(0);
        let todo = net.devices[0].connected();
        net.relay_hears(0, todo);
        net.devices_hear(greeting);
        assert_eq!(heads(&net.devices[0]), ["kept only by the relay"]);
    }

    #[test]
    fn the_rest_is_handed_to_the_face() {
        let mut a = device("dev-a");
        write(&mut a, "see https://example.com/boiler");
        assert_eq!(
            a.act(Action::Edit),
            Todo::default(),
            "nothing under the cursor"
        );
        a.act(Action::Move(1));
        let todo = a.act(Action::Edit);
        let editing = todo.edit.unwrap();
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
        assert_eq!(a.received("not a message"), Todo::default());
        a.connected();
        a.received(r#"{"forked":"dev-z"}"#);
        assert!(
            status(&a).starts_with("the log dev-z here is not"),
            "{}",
            status(&a)
        );
        let todo = a.received(r#"{"forked":"dev-a"}"#);
        let new = todo.device.clone().expect("a new writer id");
        assert_ne!(new, "dev-a");
        assert_eq!(a.device(), new);
        assert_eq!(todo.delete, ["dev-a"]);
        assert!(status(&a).starts_with("compacted"), "{}", status(&a));
        assert_eq!(heads(&a), ["see the manual"]);
    }

    #[test]
    fn a_todo_is_json_with_every_field_named() {
        let json = serde_json::to_string(&Todo::default()).unwrap();
        assert_eq!(
            json,
            r#"{"keep":[],"delete":[],"send":[],"device":null,"edit":null,"copy":null,"open":null}"#
        );
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
        assert!(b.replica.lines("dev-b").is_empty());

        // The same when the sealed lines arrive from the relay: said before any write is tried.
        let wrong = Device::new(
            host("dev-d", 0),
            "dev-d",
            Weights::default(),
            Key::from_bytes([9; 32]),
        );
        let mut net = Net {
            relay: Replica::new(),
            devices: vec![device("dev-a"), wrong],
        };
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

        let mut net = Net {
            relay: Replica::new(),
            devices: vec![a, b],
        };
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
}
