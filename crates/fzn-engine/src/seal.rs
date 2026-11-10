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
//!
//! An image is sealed the same way but kept as bytes, since it is a file of its own and never a
//! line, and it is named by an id made under a key of its own, so that the name of an image says
//! nothing to anyone without the space's key.

use std::fmt;

use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::session::Host;

/// The version of the envelope this build writes, and the only one it opens.
const ENVELOPE: u32 = 1;

/// The first byte of a sealed image, the version of its form, and the only one this build opens.
const IMAGE_FORMAT: u8 = 1;

const NONCE: usize = 24;

/// The length of XChaCha20-Poly1305's tag, which every ciphertext carries at its end.
const TAG: usize = 16;

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

    /// The key a device proves itself to a relay with: SHA-256 of `fzn/auth/` and then the
    /// space's key. The relay keeps this one, and nothing of the space's key can be had from it,
    /// so a relay that can let a device in still cannot read a line.
    pub fn auth(&self) -> Key {
        let mut hash = Sha256::new();
        hash.update(b"fzn/auth/");
        hash.update(self.0);
        Key(hash.finalize().into())
    }

    /// The key an image's id is made under: SHA-256 of `fzn/image/` and then the space's key. An
    /// id made with no key would let a relay test whether a space holds a picture it has seen
    /// elsewhere, and nothing of the space's key can be had from this one.
    pub(crate) fn image(&self) -> Key {
        let mut hash = Sha256::new();
        hash.update(b"fzn/image/");
        hash.update(self.0);
        Key(hash.finalize().into())
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
    image: Key,
}

impl Seal {
    pub fn new(key: Key) -> Seal {
        Seal {
            image: key.image(),
            key,
        }
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

    /// An image sealed as bytes: the byte 1, a fresh 24-byte nonce from the host, and then the
    /// ciphertext with its tag. Bytes and not base64, since an image is kept as a file of its own
    /// and not as a line. It comes to 41 bytes more than what was sealed.
    pub fn seal_bytes(&self, plain: &[u8], host: &dyn Host) -> Vec<u8> {
        let mut nonce = [0u8; NONCE];
        host.random(&mut nonce);
        // Encryption fails only on hundreds of gigabytes, and an image is at most a few MiB.
        let sealed = self
            .cipher()
            .encrypt(XNonce::from_slice(&nonce), plain)
            .expect("an image is far shorter than XChaCha20-Poly1305's limit");
        let mut out = Vec::with_capacity(1 + NONCE + sealed.len());
        out.push(IMAGE_FORMAT);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        out
    }

    /// What [`Seal::seal_bytes`] sealed, if these bytes are of its form and open under this key.
    pub fn open_bytes(&self, sealed: &[u8]) -> Option<Vec<u8>> {
        if sealed.len() < 1 + NONCE + TAG || sealed[0] != IMAGE_FORMAT {
            return None;
        }
        let (nonce, sealed) = sealed[1..].split_at(NONCE);
        self.cipher()
            .decrypt(XNonce::from_slice(nonce), sealed)
            .ok()
    }

    /// An image's id in this space: the first 16 bytes of HMAC-SHA256 over its bytes, under the
    /// key [`Key::image`] derives, as 32 lowercase hex digits. The same bytes give the same id on
    /// every device of the space, and another id in any other space.
    pub fn image_id(&self, plain: &[u8]) -> String {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.image.0)
            .expect("HMAC takes a key of any length");
        mac.update(plain);
        let digest = mac.finalize().into_bytes();
        digest[..16].iter().map(|b| format!("{b:02x}")).collect()
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
    fn an_auth_key_is_the_same_from_one_key_and_different_from_it_and_from_anothers() {
        let key = Key::from_bytes([7; 32]);
        assert_eq!(key.auth(), Key::from_bytes([7; 32]).auth());
        assert_ne!(key.auth(), key);
        assert_ne!(key.auth(), Key::from_bytes([8; 32]).auth());
        assert_ne!(key.auth().auth(), key.auth());
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
    fn an_image_is_sealed_as_bytes_and_opens_to_what_was_sealed() {
        let seal = seal(7);
        let plain = b"\xFF\xD8\xFFa boiler's data plate";
        let sealed = seal.seal_bytes(plain, &host());
        assert_eq!(sealed.len(), plain.len() + 41);
        assert_eq!(sealed[0], 1);
        assert_eq!(seal.open_bytes(&sealed).as_deref(), Some(&plain[..]));
    }

    #[test]
    fn an_image_sealed_twice_is_two_different_sealings_of_one_image() {
        let (seal, host) = (seal(7), host());
        let plain = b"\x89PNG\r\n\x1a\na garden";
        let first = seal.seal_bytes(plain, &host);
        let second = seal.seal_bytes(plain, &host);
        assert_ne!(first, second);
        for sealed in [first, second] {
            assert_eq!(seal.open_bytes(&sealed).as_deref(), Some(&plain[..]));
        }
    }

    #[test]
    fn a_sealed_image_opens_under_its_own_key_alone() {
        let sealed = seal(7).seal_bytes(b"\xFF\xD8\xFFhouse", &host());
        assert_eq!(seal(8).open_bytes(&sealed), None);
        let mut tampered = sealed.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert_eq!(seal(7).open_bytes(&tampered), None);
    }

    #[test]
    fn a_sealed_image_with_another_first_byte_or_too_short_does_not_open() {
        let seal = seal(7);
        let sealed = seal.seal_bytes(b"", &host());
        assert_eq!(sealed.len(), 41);
        assert_eq!(seal.open_bytes(&sealed), Some(vec![]));
        let mut other = sealed.clone();
        other[0] = 2;
        assert_eq!(seal.open_bytes(&other), None);
        assert_eq!(seal.open_bytes(&sealed[..40]), None);
        assert_eq!(seal.open_bytes(&[]), None);
    }

    #[test]
    fn an_images_id_is_pinned_so_that_every_build_names_an_image_alike() {
        let (seven, eight) = (seal(7), seal(8));
        let png = [b"\x89PNG\r\n\x1a\n".as_slice(), b"garden"].concat();
        let jpeg = [b"\xFF\xD8\xFF".as_slice(), b"house"].concat();
        assert_eq!(seven.image_id(&png), "4f998e75f22477b2519998d0731eeec8");
        assert_eq!(seven.image_id(&jpeg), "3ae5eb6a75633c8ff7a8d2a11602aae2");
        assert_ne!(eight.image_id(&png), seven.image_id(&png));
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
