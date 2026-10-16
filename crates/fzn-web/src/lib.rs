//! fuzzynote in a browser tab.
//!
//! The engine is compiled to WebAssembly and the page drives it through [`Fzn`], which is the
//! engine's [`Device`] with a text boundary round it. Everything crosses as text: an action goes
//! in as JSON and what the page has to do comes back as JSON, and so does the view. The page
//! owns what only a page can: the keys, the painting, the socket to the relay, somewhere to keep
//! lines between visits, and the asking for a passphrase.

use fzn_engine::{Action, Device, Host, Key, Moment, Todo, Weights};
use wasm_bindgen::prelude::*;

/// The browser, as a device sees it: its clock, ids for new notes, and randomness.
struct Browser;

impl Host for Browser {
    fn now(&self) -> Moment {
        let now = js_sys::Date::new_0();
        Moment {
            ms: now.get_time() as i64,
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

    /// From the browser's cryptographic source, which every browser that runs WebAssembly has.
    fn random(&self, buf: &mut [u8]) {
        let filled = web_sys::window()
            .and_then(|window| window.crypto().ok())
            .and_then(|crypto| crypto.get_random_values_with_u8_array(buf).ok());
        if filled.is_none() {
            panic!("this browser supplied no randomness, which sealing needs");
        }
    }
}

fn json(todo: &Todo) -> String {
    serde_json::to_string(todo).unwrap_or_default()
}

/// The key a passphrase gives for a space, as text to keep. Argon2 takes a moment.
#[wasm_bindgen(js_name = deriveKey)]
pub fn derive_key(passphrase: &str, space: &str) -> String {
    Key::from_passphrase(passphrase, space).encode()
}

/// One space, open in one tab.
#[wasm_bindgen]
pub struct Fzn {
    device: Device,
}

#[wasm_bindgen]
impl Fzn {
    /// `device` is this tab's writer id for the space, and so the name of its log. `key` is the
    /// space's key as [`deriveKey`](derive_key) gave it; one that is not a key is refused, with
    /// a reason the page can show.
    #[wasm_bindgen(constructor)]
    pub fn new(device: String, key: String) -> Result<Fzn, JsValue> {
        let key =
            Key::decode(&key).ok_or_else(|| JsValue::from_str("the kept key is not a key"))?;
        Ok(Fzn {
            device: Device::new(Box::new(Browser), device, Weights::default(), key),
        })
    }

    /// Takes in the lines of a log that the page kept from before, and says what to do about
    /// them, which may be to delete a log that has been retired.
    pub fn load(&mut self, log: &str, lines: Vec<String>) -> String {
        json(&self.device.load(log, lines))
    }

    /// Once, after every `load` and before `connected`: compacts this writer's log if that is
    /// due, which gives the writer a new id.
    pub fn opened(&mut self) -> String {
        json(&self.device.opened())
    }

    /// Does what was asked, which is an action as JSON, and says what the page has to do.
    pub fn act(&mut self, action: &str) -> String {
        match serde_json::from_str::<Action>(action) {
            Ok(action) => json(&self.device.act(action)),
            // A face cannot otherwise tell a bug from nothing happening.
            Err(err) => {
                self.device.say(format!("not an action: {err}"));
                json(&Todo::default())
            }
        }
    }

    /// A connection to a peer has opened: the relay's socket, which is peer 0, or a sibling
    /// tab's.
    pub fn connected(&mut self, peer: u32) -> String {
        json(&self.device.connected(peer))
    }

    /// A connection to a peer has closed.
    pub fn disconnected(&mut self, peer: u32) {
        self.device.disconnected(peer);
    }

    /// A peer has said something.
    pub fn received(&mut self, peer: u32, text: &str) -> String {
        json(&self.device.received(peer, text))
    }

    /// Puts a message on the status line.
    pub fn say(&mut self, status: &str) {
        self.device.say(status);
    }

    /// What to show, as JSON.
    pub fn view(&mut self) -> String {
        match serde_json::to_string(&self.device.view()) {
            Ok(view) => view,
            // Said, so that the next view that can be written says why this one was not.
            Err(err) => {
                self.device
                    .say(format!("the view could not be written: {err}"));
                String::new()
            }
        }
    }

    /// Where the page is, to keep across a reload: the path's steps as JSON, to give back as
    /// `{"restore": …}`.
    pub fn steps(&self) -> String {
        // It cannot fail, but if it did, no path is still a path to give back.
        serde_json::to_string(&self.device.steps()).unwrap_or_else(|_| "[]".into())
    }

    /// This writer's id, and so the name of the log it writes.
    pub fn device(&self) -> String {
        self.device.device().to_owned()
    }

    /// Whether the key is known to be the space's, or nothing yet says otherwise.
    pub fn key_verified(&self) -> bool {
        self.device.key_verified()
    }

    /// How far this tab's clock is from the relay's, when that is far enough to say.
    pub fn clock(&self) -> Option<String> {
        self.device.clock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_passphrase_gives_the_same_key_text_every_time() {
        let key = derive_key("a garden in spring", "main");
        assert_eq!(key.len(), 44);
        assert_eq!(key, derive_key("a garden in spring", "main"));
        assert_eq!(
            key,
            Key::from_passphrase("a garden in spring", "main").encode()
        );
        assert_ne!(key, derive_key("a garden in spring", "house"));
    }
}
