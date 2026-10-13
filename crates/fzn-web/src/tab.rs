//! A browser tab as a device: a session and a replica, joined in memory.
//!
//! In the terminal the two meet at the files: the app writes versions into its log, a thread
//! sends what it finds there, and what the thread brings the app finds in turn. A tab has no
//! files, so they meet here. A version the session wants kept becomes a line of this device's
//! log, which the replica holds and sends on. Lines the replica takes from the relay are read
//! as versions and given to the session. The page is left with the two things only it can do:
//! keeping lines for the next time it is opened, and the socket.

use fzn_engine::replica::{Message, Out, Peer, Replica};
use fzn_engine::{Action, Effect, Host, Notes, Record, Session, Weights};
use serde::Serialize;

/// The relay, as a tab numbers its one peer.
const RELAY: Peer = 0;

/// Lines to keep at the end of a log, the first of them at `from`.
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

/// What the page has to do after the tab has taken something in.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct Todo {
    /// Lines to keep, for the next time the page is opened.
    pub keep: Vec<Kept>,
    /// Messages for the relay, each one to send as it is.
    pub send: Vec<String>,
    /// A note to edit. What is made of it comes back as the action `save`.
    pub edit: Option<Editing>,
    /// Text for the clipboard.
    pub copy: Option<String>,
    /// A URL to open.
    pub open: Option<String>,
}

pub struct Tab {
    session: Session,
    replica: Replica,
    /// The name of this device, and so of the log it writes.
    device: String,
}

impl Tab {
    pub fn new(host: Box<dyn Host>, device: impl Into<String>) -> Self {
        let device = device.into();
        Self {
            session: Session::new(host, device.clone(), Weights::default(), Notes::new()),
            replica: Replica::new(),
            device,
        }
    }

    /// Takes in lines the page kept from before, a log at a time, before anything else is
    /// asked of the tab.
    pub fn load(&mut self, log: &str, lines: Vec<String>) {
        let versions: Vec<Record> = lines
            .iter()
            .filter_map(|line| Record::read(line.as_bytes()))
            .collect();
        self.replica.append(log, lines);
        self.session.absorb(versions);
        self.session.say("");
    }

    /// Does what was asked.
    pub fn act(&mut self, action: Action) -> Todo {
        let mut todo = Todo::default();
        for effect in self.session.act(action) {
            match effect {
                Effect::Keep(version) => {
                    let Ok(line) = version.line() else {
                        continue;
                    };
                    let line = line.trim_end_matches('\n').to_owned();
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
        todo
    }

    /// The socket to the relay has opened.
    pub fn connected(&mut self) -> Todo {
        let mut todo = Todo::default();
        let out = self.replica.connect(RELAY);
        self.carry_out(out, &mut todo);
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
        todo
    }

    /// Puts a message where the session puts its own.
    pub fn say(&mut self, status: &str) {
        self.session.say(status);
    }

    /// What to show, as JSON.
    pub fn view(&self) -> String {
        serde_json::to_string(&self.session.view()).unwrap_or_default()
    }

    /// Does what the replica wants done. Lines that came from the relay are for the page to
    /// keep and for the session to read.
    fn carry_out(&mut self, out: Vec<Out>, todo: &mut Todo) {
        for out in out {
            match out {
                Out::Keep { log, lines } => {
                    let versions: Vec<Record> = lines
                        .iter()
                        .filter_map(|line| Record::read(line.as_bytes()))
                        .collect();
                    self.session.absorb(versions);
                    let from = (self.replica.lines(&log).len() - lines.len()) as u64;
                    todo.keep.push(Kept { log, from, lines });
                }
                Out::Send(_, message) => {
                    if let Ok(text) = message.encode() {
                        todo.send.push(text);
                    }
                }
                Out::Forked { log, .. } => self.session.say(format!(
                    "the log {log} here is not the {log} on the relay, so it is not syncing"
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fzn_engine::Moment;
    use std::cell::Cell;

    /// A machine whose clock has stopped at midday on 2026-10-13, and whose ids count up.
    struct Stopped {
        name: &'static str,
        ids: Cell<u32>,
    }

    impl Host for Stopped {
        fn now(&self) -> Moment {
            Moment::utc(1_791_892_800)
        }

        fn new_id(&self) -> String {
            self.ids.set(self.ids.get() + 1);
            format!("{}-{:04}", self.name, self.ids.get())
        }
    }

    fn tab(device: &'static str) -> Tab {
        let host = Stopped {
            name: device,
            ids: Cell::new(0),
        };
        Tab::new(Box::new(host), device)
    }

    fn write(tab: &mut Tab, text: &str) -> Todo {
        tab.act(Action::Type(text.to_owned()));
        tab.act(Action::Write)
    }

    fn heads(tab: &Tab) -> Vec<String> {
        let view: serde_json::Value = serde_json::from_str(&tab.view()).unwrap();
        view["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["head"].as_str().unwrap().to_owned())
            .collect()
    }

    fn status(tab: &Tab) -> String {
        let view: serde_json::Value = serde_json::from_str(&tab.view()).unwrap();
        view["status"].as_str().unwrap().to_owned()
    }

    /// A relay between tabs: what each tab sends goes to it, and what it sends goes to them.
    struct Net {
        relay: Replica,
        tabs: Vec<Tab>,
    }

    impl Net {
        fn connect(&mut self, tab: usize) {
            let greeting = self.relay.connect(tab as Peer);
            let todo = self.tabs[tab].connected();
            self.relay_hears(tab, todo);
            self.tabs_hear(greeting);
        }

        fn relay_hears(&mut self, tab: usize, todo: Todo) {
            for text in todo.send {
                let message = Message::decode(&text).unwrap();
                let out = self.relay.receive(tab as Peer, message);
                self.tabs_hear(out);
            }
        }

        fn tabs_hear(&mut self, out: Vec<Out>) {
            for out in out {
                if let Out::Send(peer, message) = out {
                    let todo = self.tabs[peer as usize].received(&message.encode().unwrap());
                    self.relay_hears(peer as usize, todo);
                }
            }
        }

        fn act(&mut self, tab: usize, action: Action) {
            let todo = self.tabs[tab].act(action);
            self.relay_hears(tab, todo);
        }
    }

    #[test]
    fn what_is_written_is_kept_as_a_line_of_this_devices_log() {
        let mut a = tab("dev-a");
        let todo = write(&mut a, "boiler service booked");
        assert_eq!(heads(&a), ["boiler service booked"]);
        let [kept] = &todo.keep[..] else {
            panic!("{todo:?}");
        };
        assert_eq!((kept.log.as_str(), kept.from), ("dev-a", 0));
        let version = Record::read(kept.lines[0].as_bytes()).unwrap();
        assert_eq!(version.text, "boiler service booked");
        assert_eq!(version.device, "dev-a");
        assert!(!kept.lines[0].contains('\n'));
        assert!(todo.send.is_empty(), "with nobody to send it to");
        assert_eq!(write(&mut a, "pressure dropped").keep[0].from, 1);

        // Opened again, with what the page kept, it is all there and nothing is said about it.
        let mut again = tab("dev-a");
        let lines = vec![kept.lines[0].clone()];
        again.load("dev-a", lines);
        assert_eq!(heads(&again), ["boiler service booked"]);
        assert_eq!(status(&again), "");
        assert_eq!(write(&mut again, "and one more").keep[0].from, 1);
    }

    #[test]
    fn two_tabs_and_a_relay_show_each_other_what_they_write() {
        let mut net = Net {
            relay: Replica::new(),
            tabs: vec![tab("dev-a"), tab("dev-b")],
        };
        write(&mut net.tabs[0], "written before anyone was connected");
        net.connect(0);
        net.connect(1);
        assert_eq!(heads(&net.tabs[1]), ["written before anyone was connected"]);
        assert_eq!(status(&net.tabs[1]), "1 changed elsewhere");

        net.act(1, Action::Type("engineer quoted 240".into()));
        net.act(1, Action::Write);
        net.act(0, Action::Type("quoted".into()));
        assert_eq!(heads(&net.tabs[0]), ["engineer quoted 240"]);

        // Going into a note and writing there links to it, on the other device too.
        net.act(0, Action::Move(1));
        net.act(0, Action::GoIn);
        net.act(0, Action::Type("booked for friday".into()));
        net.act(0, Action::Write);
        net.act(1, Action::Type("booked".into()));
        let view: serde_json::Value = serde_json::from_str(&net.tabs[1].view()).unwrap();
        assert_eq!(view["rows"][0]["head"], "booked for friday");
        assert_eq!(
            view["rows"][0]["links"],
            serde_json::json!(["engineer quoted 240"])
        );
        assert_eq!(net.relay.have().len(), 2, "a log to a device");
    }

    #[test]
    fn a_tab_opened_afresh_gets_its_own_log_back_from_the_relay() {
        let mut net = Net {
            relay: Replica::new(),
            tabs: vec![tab("dev-a")],
        };
        net.connect(0);
        net.act(0, Action::Type("kept only by the relay".into()));
        net.act(0, Action::Write);

        // The browser's storage is cleared, and the device id with it or not: here not.
        net.relay.disconnect(0);
        net.tabs[0] = tab("dev-a");
        let greeting = net.relay.connect(0);
        let todo = net.tabs[0].connected();
        net.relay_hears(0, todo);
        net.tabs_hear(greeting);
        assert_eq!(heads(&net.tabs[0]), ["kept only by the relay"]);
    }

    #[test]
    fn the_rest_is_handed_to_the_page() {
        let mut a = tab("dev-a");
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

        // What is not a message is passed over, and a fork is said.
        assert_eq!(a.received("not a message"), Todo::default());
        a.connected();
        a.received(r#"{"forked":"dev-a"}"#);
        assert!(
            status(&a).starts_with("the log dev-a here is not"),
            "{}",
            status(&a)
        );
        let json = serde_json::to_string(&Todo::default()).unwrap();
        assert_eq!(
            json,
            r#"{"keep":[],"send":[],"edit":null,"copy":null,"open":null}"#
        );
    }
}
