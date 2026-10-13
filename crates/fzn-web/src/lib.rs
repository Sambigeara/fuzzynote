//! fuzzynote in a browser tab.
//!
//! The engine is compiled to WebAssembly and the page drives it through [`Fzn`]. Everything
//! crosses as text: an action goes in as JSON and what the page has to do comes back as JSON,
//! and so does the view. The page owns what only a page can: the keys, the painting, the socket
//! to the relay, and somewhere to keep lines between visits.

mod tab;

pub use tab::{Editing, Kept, Tab, Todo};

use fzn_engine::{Action, Host, Moment};
use wasm_bindgen::prelude::*;

/// The browser, as a session sees it: its clock, and ids for new notes.
struct Browser;

impl Host for Browser {
    fn now(&self) -> Moment {
        let now = js_sys::Date::new_0();
        Moment {
            secs: (now.get_time() / 1000.0).floor() as i64,
            // The browser counts minutes west of UTC.
            offset: -(now.get_timezone_offset() as i32) * 60,
        }
    }

    /// Time-ordered and unique enough across devices, as the terminal makes them.
    fn new_id(&self) -> String {
        let millis = js_sys::Date::now() as u64;
        let random = (js_sys::Math::random() * 4_294_967_296.0) as u32;
        format!("{millis:x}{random:08x}")
    }
}

fn json(todo: &Todo) -> String {
    serde_json::to_string(todo).unwrap_or_default()
}

/// One space, open in one tab.
#[wasm_bindgen]
pub struct Fzn {
    tab: Tab,
}

#[wasm_bindgen]
impl Fzn {
    /// `device` names this browser in the versions it writes, and is the name of its log.
    #[wasm_bindgen(constructor)]
    pub fn new(device: String) -> Fzn {
        Fzn {
            tab: Tab::new(Box::new(Browser), device),
        }
    }

    /// Takes in the lines of a log that the page kept from before.
    pub fn load(&mut self, log: &str, lines: Vec<String>) {
        self.tab.load(log, lines);
    }

    /// Does what was asked, which is an action as JSON, and says what the page has to do.
    pub fn act(&mut self, action: &str) -> String {
        match serde_json::from_str::<Action>(action) {
            Ok(action) => json(&self.tab.act(action)),
            Err(_) => json(&Todo::default()),
        }
    }

    /// The socket to the relay has opened.
    pub fn connected(&mut self) -> String {
        json(&self.tab.connected())
    }

    /// The socket to the relay has closed.
    pub fn disconnected(&mut self) {
        self.tab.disconnected();
    }

    /// The relay has said something.
    pub fn received(&mut self, text: &str) -> String {
        json(&self.tab.received(text))
    }

    /// Puts a message on the status line.
    pub fn say(&mut self, status: &str) {
        self.tab.say(status);
    }

    /// What to show, as JSON.
    pub fn view(&self) -> String {
        self.tab.view()
    }
}
