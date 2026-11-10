//! Images: what a note refers to by its address, kept and carried apart from the logs.
//!
//! An image is kept beside the logs and never in one, because every replica keeps every line in
//! memory, the relay included, and a version is the whole note, written again at every keystroke
//! in its head; a photo in a line would be carried and kept again each time. So a note holds only
//! the image's address, and the image is a file of its own, sealed under the space's key as a
//! line is, and named by an id that says nothing without that key.
//!
//! Images are pulled, never pushed. A device says which it holds and asks for those it lacks, and
//! the relay does the same of it, so that a build from before images passes over what it does not
//! know, and no peer is sent what it did not ask for. The relay can read none of it: it holds
//! sealed bytes under names it cannot test a picture against.

use std::collections::BTreeSet;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};

use crate::seal::Seal;
use crate::session::Host;

/// The hex digits of an image's id.
pub const ID_DIGITS: usize = 32;
/// The most an image may be, in plaintext bytes, once a face has made it smaller.
pub const IMAGE_MOST: usize = 2 * 1024 * 1024;
/// What sealing adds to an image: the format's byte, the nonce and the tag.
const SEALING: usize = 1 + 24 + 16;
/// The least a sealed image may be, sealed from a single byte: 42.
pub const SEALED_LEAST: usize = SEALING + 1;
/// The most a sealed image may be.
pub const SEALED_MOST: usize = IMAGE_MOST + SEALING;
/// Wants outstanding on a connection, either way.
pub const AT_ONCE: usize = 4;
/// The ids of one want that are answered.
pub const ANSWERED_MOST: usize = 8;
/// What a relay says when it will take no more images.
pub const FULL: &str = "the relay has no room for more images";

const MIB: f64 = 1_048_576.0;

/// Whether a text is an image's id: exactly 32 lowercase hex digits.
pub fn is_id(text: &str) -> bool {
    text.len() == ID_DIGITS && text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Whether a character ends an image's address after its digits: `)`, a space, a tab or a line's
/// end. A note's text reads an address only so ended, and [`id_in`] the same or at the text's end.
pub(crate) fn ends_address(c: char) -> bool {
    matches!(c, ')' | ' ' | '\t' | '\n' | '\r')
}

/// The id in an image's address or reference, `fzn:<id>` anywhere in the text, if there is one.
/// The digits have to be followed by the text's end or by what ends an address in a note, so
/// that a longer run, or a file's name with an address inside it, is not taken for one.
pub fn id_in(text: &str) -> Option<&str> {
    text.match_indices("fzn:").find_map(|(at, _)| {
        let start = at + 4;
        let id = text.get(start..start + ID_DIGITS)?;
        let after = text[start + ID_DIGITS..].chars().next();
        (is_id(id) && after.is_none_or(ends_address)).then_some(id)
    })
}

/// The reference a face writes into a note for an image: `![](fzn:<id>)`.
pub fn reference(id: &str) -> String {
    format!("![](fzn:{id})")
}

/// What an image kept is: the two that every browser and terminal can show.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Jpeg,
    Png,
}

impl Kind {
    /// What an image's first bytes say it is, if it is one of the two.
    pub fn of(bytes: &[u8]) -> Option<Kind> {
        if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
            Some(Kind::Jpeg)
        } else if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
            Some(Kind::Png)
        } else {
            None
        }
    }

    /// Its media type, for a browser to draw it by.
    pub fn mime(self) -> &'static str {
        match self {
            Kind::Jpeg => "image/jpeg",
            Kind::Png => "image/png",
        }
    }

    /// The end of a file's name for it, for a face that writes one out.
    pub fn extension(self) -> &'static str {
        match self {
            Kind::Jpeg => "jpg",
            Kind::Png => "png",
        }
    }
}

/// Checks an image is one that is kept, a JPEG or a PNG of 1 byte to 2 MiB, and seals it: its id
/// and its sealed bytes, or why not, in words to show.
pub fn seal(seal: &Seal, plain: &[u8], host: &dyn Host) -> Result<(String, Vec<u8>), String> {
    if Kind::of(plain).is_none() {
        return Err("an image is kept as a JPEG or a PNG, and this is neither".to_owned());
    }
    if plain.len() > IMAGE_MOST {
        // Rounded up, so that an image just over the most never reads as the most.
        let mib = (plain.len() as f64 / MIB * 10.0).ceil() / 10.0;
        return Err(format!(
            "an image is at most 2 MiB once made smaller, and this one is {mib:.1} MiB"
        ));
    }
    Ok((seal.image_id(plain), seal.seal_bytes(plain, host)))
}

/// Opens sealed bytes kept under `id`, if they open, give that id, and are an image that is kept.
pub fn open(seal: &Seal, id: &str, sealed: &[u8]) -> Option<Vec<u8>> {
    seal.open_bytes(sealed).filter(|plain| {
        Kind::of(plain).is_some() && plain.len() <= IMAGE_MOST && seal.image_id(plain) == id
    })
}

/// Whether bytes are of a length a sealed image may be.
pub(crate) fn sealed_length(sealed: &[u8]) -> bool {
    (SEALED_LEAST..=SEALED_MOST).contains(&sealed.len())
}

/// The sealed bytes a message's `c` carries, if it is base64 of a length a sealed image may be.
pub fn sealed_of(c: &str) -> Option<Vec<u8>> {
    // Base64 of more than the most a sealed image may be is not decoded at all.
    if c.len() > SEALED_MOST.div_ceil(3) * 4 {
        return None;
    }
    STANDARD.decode(c).ok().filter(|bytes| sealed_length(bytes))
}

/// What a device and its relay say to each other of images. Each is a JSON object of one key,
/// which a build from before images fails to read and passes over.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Message {
    /// Images the sender holds that it has not said it holds on this connection.
    Images(Vec<String>),
    /// Images the sender asks for.
    Want(Vec<String>),
    /// An image asked for, sealed, in base64.
    Image { id: String, c: String },
    /// Why the relay will take no more images.
    Full(String),
    /// Kept for letting images go, which nothing does yet. It is read and passed over.
    Drop(Vec<String>),
}

impl Message {
    pub fn encode(&self) -> String {
        // An object of strings always serialises.
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Reads a message, if it is one of these.
    pub fn decode(text: &str) -> Option<Self> {
        serde_json::from_str(text).ok()
    }

    /// An image as it is sent: its sealed bytes in standard base64.
    pub fn image(id: &str, sealed: &[u8]) -> Self {
        Message::Image {
            id: id.to_owned(),
            c: STANDARD.encode(sealed),
        }
    }
}

/// What a device knows of its space's images, for its exchange with the relay. All but `here`
/// is of one connection, and forgotten when it goes.
#[derive(Debug, Default)]
pub(crate) struct Images {
    /// Held in this machine's store.
    here: BTreeSet<String>,
    /// Said by the relay to be held there, on this connection; none until it says.
    relay: Option<BTreeSet<String>>,
    /// Said by this device to be held here, on this connection.
    said: BTreeSet<String>,
    /// Asked for and not yet come, on this connection, at most [`AT_ONCE`].
    asked: BTreeSet<String>,
    /// Came on this connection and did not open, so not asked for again on it.
    shut: BTreeSet<String>,
    /// Asked for by the relay on this connection, held here, and not yet sent, so that nothing
    /// goes to a relay that did not ask for it on the connection it goes over.
    owed: BTreeSet<String>,
}

/// What came of an image message from the relay.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Heard {
    /// An image to keep, by its id, sealed.
    pub keep: Option<(String, Vec<u8>)>,
    /// Images the relay asked for, to send if they are held.
    pub upload: Vec<String>,
    /// Something to say on the status line.
    pub say: Option<String>,
}

impl Images {
    /// Counts these as held here. What is not an id is passed over.
    pub fn held(&mut self, ids: impl IntoIterator<Item = String>) {
        self.here.extend(ids.into_iter().filter(|id| is_id(id)));
    }

    /// The connection to the relay went, or a new one came: all it knew is forgotten.
    pub fn went(&mut self) {
        let here = std::mem::take(&mut self.here);
        *self = Images {
            here,
            ..Images::default()
        };
    }

    /// What to say to the relay, of the images the notes refer to: those held here that it has
    /// not said it holds, once a connection, and then those it holds and this device lacks, in
    /// order of id, while fewer than [`AT_ONCE`] are outstanding. Nothing until the relay has said
    /// what it holds.
    pub fn step(&mut self, referred: &BTreeSet<String>) -> Vec<Message> {
        let Some(relay) = &self.relay else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let announce: Vec<String> = referred
            .iter()
            .filter(|id| {
                self.here.contains(*id) && !relay.contains(*id) && !self.said.contains(*id)
            })
            .cloned()
            .collect();
        if !announce.is_empty() {
            self.said.extend(announce.iter().cloned());
            out.push(Message::Images(announce));
        }
        let room = AT_ONCE.saturating_sub(self.asked.len());
        let want: Vec<String> = referred
            .iter()
            .filter(|id| {
                relay.contains(*id)
                    && !self.here.contains(*id)
                    && !self.asked.contains(*id)
                    && !self.shut.contains(*id)
            })
            .take(room)
            .cloned()
            .collect();
        if !want.is_empty() {
            self.asked.extend(want.iter().cloned());
            out.push(Message::Want(want));
        }
        out
    }

    /// Takes in what the relay said of images.
    pub fn heard(&mut self, message: Message, seal: &Seal) -> Heard {
        let mut heard = Heard::default();
        match message {
            Message::Images(ids) => {
                let relay = self.relay.get_or_insert_with(BTreeSet::new);
                relay.extend(ids.into_iter().filter(|id| is_id(id)));
            }
            Message::Want(ids) => {
                for id in ids.into_iter().take(ANSWERED_MOST) {
                    if self.here.contains(&id) && self.owed.insert(id.clone()) {
                        heard.upload.push(id);
                    }
                }
            }
            Message::Image { id, c } => {
                if self.asked.remove(&id) {
                    let opened = sealed_of(&c).filter(|sealed| open(seal, &id, sealed).is_some());
                    match opened {
                        Some(sealed) => {
                            self.here.insert(id.clone());
                            heard.keep = Some((id, sealed));
                        }
                        None => {
                            self.shut.insert(id);
                        }
                    }
                }
            }
            // The words shown are this build's own, so that a relay cannot put words of its
            // choosing on the status line.
            Message::Full(_) => heard.say = Some(FULL.to_owned()),
            Message::Drop(_) => {}
        }
        heard
    }

    /// Whether the relay asked for this image on this connection and it has not gone yet, which
    /// it now has: it is owed once.
    pub fn pay(&mut self, id: &str) -> bool {
        self.owed.remove(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seal::{Counting, Key};
    use std::cell::Cell;

    fn space(byte: u8) -> Seal {
        Seal::new(Key::from_bytes([byte; 32]))
    }

    fn host() -> Counting {
        Counting(Cell::new(0))
    }

    fn jpeg(rest: &[u8]) -> Vec<u8> {
        [&[0xFF, 0xD8, 0xFF][..], rest].concat()
    }

    #[test]
    fn only_a_jpeg_or_a_png_of_at_most_2_mib_is_sealed_and_why_not_is_said() {
        let (space, host) = (space(7), host());
        let neither = "an image is kept as a JPEG or a PNG, and this is neither";
        assert_eq!(seal(&space, b"GIF89a", &host), Err(neither.to_owned()));
        assert_eq!(seal(&space, b"", &host), Err(neither.to_owned()));

        let too_large = |mib: &str| {
            format!("an image is at most 2 MiB once made smaller, and this one is {mib} MiB")
        };
        let big = jpeg(&vec![0; 2_516_582 - 3]);
        assert_eq!(seal(&space, &big, &host), Err(too_large("2.4")));
        let just_over = jpeg(&vec![0; IMAGE_MOST - 2]);
        assert_eq!(seal(&space, &just_over, &host), Err(too_large("2.1")));

        let most = jpeg(&vec![0; IMAGE_MOST - 3]);
        let (id, sealed) = seal(&space, &most, &host).unwrap();
        assert_eq!(sealed.len(), SEALED_MOST);
        assert_eq!(id, space.image_id(&most));

        let png = [&b"\x89PNG\r\n\x1a\n"[..], b"garden"].concat();
        let (id, _) = seal(&space, &png, &host).unwrap();
        assert_eq!(id, "4f998e75f22477b2519998d0731eeec8");
        assert_eq!(Kind::of(&png), Some(Kind::Png));
        assert_eq!(Kind::Png.mime(), "image/png");
        assert_eq!(Kind::Jpeg.extension(), "jpg");
    }

    #[test]
    fn an_image_opens_only_under_the_id_it_was_sealed_by() {
        let (space, host) = (space(7), host());
        let plate = jpeg(b"a boiler's data plate");
        let (id, sealed) = seal(&space, &plate, &host).unwrap();
        assert_eq!(open(&space, &id, &sealed), Some(plate));
        let (other, _) = seal(&space, &jpeg(b"a radiator valve"), &host).unwrap();
        assert_eq!(open(&space, &other, &sealed), None);
        assert_eq!(open(&self::space(8), &id, &sealed), None);
    }

    #[test]
    fn what_opens_to_its_id_but_is_no_image_that_is_kept_does_not_open() {
        let (space, host) = (space(7), host());
        let gif = b"GIF89a a garden".to_vec();
        let sealed = space.seal_bytes(&gif, &host);
        assert_eq!(open(&space, &space.image_id(&gif), &sealed), None);
        let big = jpeg(&vec![0; IMAGE_MOST]);
        let sealed = space.seal_bytes(&big, &host);
        assert_eq!(open(&space, &space.image_id(&big), &sealed), None);
    }

    #[test]
    fn the_image_messages_read_and_write_as_objects_of_one_key() {
        let id = "4f998e75f22477b2519998d0731eeec8";
        let cases = [
            (
                Message::Images(vec![id.into()]),
                format!(r#"{{"images":["{id}"]}}"#),
            ),
            (Message::Images(vec![]), r#"{"images":[]}"#.to_owned()),
            (
                Message::Want(vec![id.into()]),
                format!(r#"{{"want":["{id}"]}}"#),
            ),
            (
                Message::image(id, &[1, 2, 3]),
                format!(r#"{{"image":{{"id":"{id}","c":"AQID"}}}}"#),
            ),
            (
                Message::Full(FULL.into()),
                r#"{"full":"the relay has no room for more images"}"#.to_owned(),
            ),
            (
                Message::Drop(vec![id.into()]),
                format!(r#"{{"drop":["{id}"]}}"#),
            ),
        ];
        for (message, json) in cases {
            assert_eq!(message.encode(), json);
            assert_eq!(Message::decode(&json), Some(message));
        }
        assert_eq!(Message::decode(r#"{"images":["a"],"want":[]}"#), None);
        assert_eq!(Message::decode("not json"), None);
    }

    #[test]
    fn a_line_message_is_no_image_message_and_an_image_message_no_line_message() {
        use crate::replica;
        let id = "4f998e75f22477b2519998d0731eeec8";
        for message in [
            Message::Images(vec![id.into()]),
            Message::Want(vec![id.into()]),
            Message::image(id, &[1; 42]),
            Message::Full(FULL.into()),
            Message::Drop(vec![id.into()]),
        ] {
            assert_eq!(replica::Message::decode(&message.encode()), None);
        }
        for message in [
            replica::Message::Have(Default::default()),
            replica::Message::Forked("3f9a01c2".into()),
            replica::Message::Refused("no".into()),
            replica::Message::Claim("key".into()),
        ] {
            assert_eq!(Message::decode(&message.encode().unwrap()), None);
        }
    }

    #[test]
    fn sealed_bytes_of_the_wrong_length_or_not_base64_are_refused() {
        assert_eq!(SEALED_LEAST, 42);
        let at = |n: usize| STANDARD.encode(vec![1u8; n]);
        assert_eq!(sealed_of(&at(SEALED_LEAST - 1)), None);
        assert_eq!(sealed_of(&at(SEALED_LEAST)).map(|b| b.len()), Some(42));
        assert_eq!(
            sealed_of(&at(SEALED_MOST)).map(|b| b.len()),
            Some(SEALED_MOST)
        );
        assert_eq!(sealed_of(&at(SEALED_MOST + 1)), None);
        assert_eq!(sealed_of("not base64!"), None);
        assert_eq!(sealed_of(at(43).trim_end_matches('=')), None);
    }

    #[test]
    fn an_id_is_32_lowercase_hex_digits_and_is_found_in_its_address() {
        let id = "4f998e75f22477b2519998d0731eeec8";
        assert!(is_id(id));
        assert!(!is_id(&id.to_uppercase()));
        assert!(!is_id(&id[..31]));
        assert_eq!(reference(id), format!("![](fzn:{id})"));
        for text in [
            format!("fzn:{id}"),
            reference(id),
            format!("![valve](fzn:{id} \"the valve\")"),
            format!("fzn:{id}\n"),
            format!("fzn:{id}\r\n"),
            format!("fzn:{id}\tand on"),
        ] {
            assert_eq!(id_in(&text), Some(id), "{text:?}");
        }
        for text in [
            format!("fzn:{id}0"),
            format!("fzn:{}", &id[..31]),
            format!("fzn:{id}-x"),
            format!("./fzn:{id}.jpg"),
            format!("fzn:{id}]"),
            id.to_owned(),
        ] {
            assert_eq!(id_in(&text), None, "{text:?}");
        }
    }
}
