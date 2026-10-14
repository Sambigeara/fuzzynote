//! Sealing the lines of a log, so that what leaves a device can be read only on another device
//! that has the space's passphrase.
//!
//! A sealed line is an envelope, `{"s":1,"c":"…"}`, where `c` is a random 24-byte nonce and then
//! the plain line encrypted with XChaCha20-Poly1305, in base64. The nonce is long enough to be
//! drawn at random for every line without a second thought, and it comes from the [`Host`], so
//! that nothing here reaches for the machine. The key is derived from a passphrase with Argon2id,
//! salted with the space's name and not with anything random, so that the same passphrase gives
//! the same key on every device and nothing has to travel between them to agree on it.
//!
//! Lines from before sealing are still plain, and are read as they are. That is why opening a
//! line has three outcomes and not two: a line that is not an envelope at all is told apart from
//! one this key does not open, which is the sign of a wrong passphrase.

use std::fmt;

use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::session::Host;

/// The version of the envelope this build writes, and the only one it opens.
const ENVELOPE: u32 = 1;

const NONCE: usize = 24;

/// The cost of deriving a key: 19 MiB, two passes, one lane. Slow enough to make guessing a
/// passphrase expensive, quick enough to do once a device, on a phone, in a tab.
const MEMORY_KIB: u32 = 19_456;
const PASSES: u32 = 2;
const LANES: u32 = 1;

/// A space's key: 32 bytes, the same on every device that was given the space's passphrase.
#[derive(Clone, PartialEq, Eq)]
pub struct Key([u8; 32]);

/// The bytes are left out, so that a key never turns up in a log or a panic.
impl fmt::Debug for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Key(..)")
    }
}

impl Key {
    /// The key a passphrase gives for a space. Argon2id, 19456 KiB, 2 passes, 1 lane, 32 bytes
    /// out. The salt is the first 16 bytes of SHA-256 of `fzn/space/` followed by the space's
    /// name, so that one passphrase gives each space its own key.
    pub fn from_passphrase(passphrase: &str, space: &str) -> Key {
        let mut hash = Sha256::new();
        hash.update(b"fzn/space/");
        hash.update(space.as_bytes());
        let digest = hash.finalize();
        let salt = &digest[..16];

        // Neither call can fail: the parameters are constants within Argon2's limits and the
        // salt is longer than its minimum, as every test that derives a key shows.
        let params = Params::new(MEMORY_KIB, PASSES, LANES, Some(32))
            .expect("the key derivation's parameters are within Argon2's limits");
        let mut key = [0u8; 32];
        Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
            .hash_password_into(passphrase.as_bytes(), salt, &mut key)
            .expect("a 16-byte salt and a 32-byte key are within Argon2's limits");
        Key(key)
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Key {
        Key(bytes)
    }

    /// The key as text to keep: standard base64 with padding, 44 characters.
    pub fn encode(&self) -> String {
        STANDARD.encode(self.0)
    }

    /// A key read back from [`Key::encode`], with whatever whitespace was kept around it.
    pub fn decode(text: &str) -> Option<Key> {
        let bytes = STANDARD.decode(text.trim()).ok()?;
        bytes.try_into().ok().map(Key)
    }
}

/// What opening a line of a log came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Opened {
    /// Not an envelope at all: a line from before sealing, to be taken as it is.
    Plain,
    /// What the envelope held.
    Text(String),
    /// An envelope this key does not open, one that was tampered with, or one of a version this
    /// build does not know.
    Shut,
}

/// The envelope as it is written. Any other field makes a line something else.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    s: u32,
    c: String,
}

fn envelope(line: &str) -> Option<Envelope> {
    serde_json::from_str(line).ok()
}

/// Whether a line is an envelope: an object with exactly the fields `s`, a number, and `c`, a
/// string. Says nothing of whether it opens.
pub fn is_sealed(line: &str) -> bool {
    envelope(line).is_some()
}

/// Seals and opens the lines of one space.
#[derive(Clone, Debug)]
pub struct Seal {
    key: Key,
}

impl Seal {
    pub fn new(key: Key) -> Seal {
        Seal { key }
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(&self.key.0.into())
    }

    /// One line, with no newline and no whitespace: `{"s":1,"c":"…"}`, where `c` is the nonce
    /// and then the ciphertext, in base64. A fresh nonce is drawn from the host every time, so
    /// one text sealed twice gives two lines.
    pub fn seal(&self, plaintext: &str, host: &dyn Host) -> String {
        let mut nonce = [0u8; NONCE];
        host.random(&mut nonce);
        // Encryption fails only on a text of hundreds of gigabytes.
        let sealed = self
            .cipher()
            .encrypt(XNonce::from_slice(&nonce), plaintext.as_bytes())
            .expect("a line is far shorter than XChaCha20-Poly1305's limit");
        let mut c = Vec::with_capacity(NONCE + sealed.len());
        c.extend_from_slice(&nonce);
        c.extend_from_slice(&sealed);
        let envelope = Envelope {
            s: ENVELOPE,
            c: STANDARD.encode(c),
        };
        // An object of a number and a base64 string always serialises.
        serde_json::to_string(&envelope).unwrap_or_default()
    }

    /// Opens a line of a log, if it is an envelope, and says what came of it.
    pub fn open(&self, line: &str) -> Opened {
        let Some(envelope) = envelope(line) else {
            return Opened::Plain;
        };
        if envelope.s != ENVELOPE {
            return Opened::Shut;
        }
        let opened = STANDARD
            .decode(envelope.c)
            .ok()
            .filter(|c| c.len() >= NONCE)
            .and_then(|c| {
                let (nonce, sealed) = c.split_at(NONCE);
                self.cipher()
                    .decrypt(XNonce::from_slice(nonce), sealed)
                    .ok()
            })
            .and_then(|plain| String::from_utf8(plain).ok());
        match opened {
            Some(text) => Opened::Text(text),
            None => Opened::Shut,
        }
    }
}

/// A machine whose randomness counts up, for tests that seal.
#[cfg(test)]
pub(crate) struct Counting(pub std::cell::Cell<u8>);

#[cfg(test)]
impl Host for Counting {
    fn now(&self) -> crate::time::Moment {
        crate::time::Moment::utc(1_791_892_800)
    }

    fn new_id(&self) -> String {
        "new-0001".into()
    }

    fn random(&self, buf: &mut [u8]) {
        for byte in buf {
            self.0.set(self.0.get().wrapping_add(1));
            *byte = self.0.get();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn seal(byte: u8) -> Seal {
        Seal::new(Key::from_bytes([byte; 32]))
    }

    fn host() -> Counting {
        Counting(Cell::new(0))
    }

    #[test]
    fn a_sealed_text_opens_to_what_was_sealed() {
        let seal = seal(7);
        let line = seal.seal("check the boiler pressure", &host());
        assert_eq!(
            seal.open(&line),
            Opened::Text("check the boiler pressure".into())
        );
    }

    #[test]
    fn one_text_sealed_twice_gives_two_lines_that_both_open() {
        let (seal, host) = (seal(7), host());
        let first = seal.seal("House: gutters", &host);
        let second = seal.seal("House: gutters", &host);
        assert_ne!(first, second);
        for line in [first, second] {
            assert_eq!(seal.open(&line), Opened::Text("House: gutters".into()));
        }
    }

    #[test]
    fn a_line_sealed_under_another_key_stays_shut() {
        let line = seal(7).seal("plant the garlic", &host());
        assert_eq!(seal(8).open(&line), Opened::Shut);
    }

    #[test]
    fn a_line_with_one_byte_changed_stays_shut() {
        let seal = seal(7);
        let line = seal.seal("plant the garlic", &host());
        let Envelope { c, .. } = envelope(&line).unwrap();
        let mut bytes = STANDARD.decode(c).unwrap();
        let last = bytes.len() - 1;
        for at in [0, NONCE, last] {
            let mut tampered = bytes.clone();
            tampered[at] ^= 1;
            let line = format!(r#"{{"s":1,"c":"{}"}}"#, STANDARD.encode(&tampered));
            assert_eq!(seal.open(&line), Opened::Shut, "byte {at} flipped");
        }
        bytes.truncate(NONCE - 1);
        let short = format!(r#"{{"s":1,"c":"{}"}}"#, STANDARD.encode(&bytes));
        assert_eq!(seal.open(&short), Opened::Shut);
        assert_eq!(seal.open(r#"{"s":1,"c":"not base64!"}"#), Opened::Shut);
    }

    #[test]
    fn an_envelope_of_a_later_version_stays_shut() {
        let seal = seal(7);
        let line = seal.seal("plant the garlic", &host());
        let later = line.replacen(r#"{"s":1,"#, r#"{"s":2,"#, 1);
        assert_eq!(seal.open(&later), Opened::Shut);
    }

    #[test]
    fn a_plain_line_is_not_an_envelope() {
        let seal = seal(7);
        let plain = r#"{"v":1,"id":"n1","text":"service the boiler"}"#;
        assert_eq!(seal.open(plain), Opened::Plain);
        assert_eq!(seal.open("not json"), Opened::Plain);
        assert_eq!(
            seal.open(r#"{"compacts":["3f9a01c2"],"snapshot":293}"#),
            Opened::Plain
        );
    }

    #[test]
    fn only_an_object_of_exactly_s_and_c_is_sealed() {
        let line = seal(7).seal("plant the garlic", &host());
        assert!(is_sealed(&line));
        assert!(is_sealed(r#"{"s":2,"c":"anything"}"#));
        assert!(!is_sealed(r#"{"s":1}"#));
        assert!(!is_sealed(r#"{"s":1,"c":"x","v":1}"#));
        assert!(!is_sealed(r#"{"s":"1","c":"x"}"#));
        assert!(!is_sealed(r#"{"s":1,"c":7}"#));
        assert!(!is_sealed(
            r#"{"v":1,"id":"n1","text":"service the boiler"}"#
        ));
        assert!(!is_sealed("not json"));
    }

    #[test]
    fn an_envelope_is_one_line_with_no_spaces() {
        let line = seal(7).seal("a training app\nwith a body of two lines", &host());
        assert!(line.starts_with(r#"{"s":1,"c":""#));
        assert!(line.ends_with(r#""}"#));
        assert!(!line.contains(char::is_whitespace));
    }

    #[test]
    fn a_passphrase_gives_one_key_per_space() {
        let key = Key::from_passphrase("correct horse", "garden");
        assert_eq!(key, Key::from_passphrase("correct horse", "garden"));
        assert_ne!(key, Key::from_passphrase("correct horse", "house"));
        assert_ne!(key, Key::from_passphrase("wrong horse", "garden"));
    }

    #[test]
    fn a_key_reads_back_from_its_text_and_nothing_else_does() {
        let key = Key::from_bytes(*b"a key of exactly thirty-two byte");
        let text = key.encode();
        assert_eq!(text.len(), 44);
        assert_eq!(Key::decode(&text), Some(key.clone()));
        assert_eq!(Key::decode(&format!("  {text}\n")), Some(key.clone()));
        assert_eq!(Key::decode(&STANDARD.encode([1u8; 31])), None);
        assert_eq!(Key::decode(&STANDARD.encode([1u8; 33])), None);
        assert_eq!(Key::decode("not base64!"), None);
        assert_eq!(format!("{key:?}"), "Key(..)");
    }
}
