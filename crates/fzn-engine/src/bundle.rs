//! A note taken out with its images: a tar of a directory holding the note as markdown and its
//! images beside it, which any editor, Obsidian included, reads as a document with its pictures.
//!
//! It is made here, and not in a face, so that every face makes the same one by the same rule,
//! and the rule is tested without a browser. The images come in already opened, by a function the
//! face supplies, since the engine holds none of them: a face's store does. The tar is written by
//! hand, POSIX ustar and nothing more, since a directory of a few files needs none of what a
//! library would bring.
//!
//! Which addresses become files is exactly which the engine reads as images, by the same reading
//! ([`text::addresses`]), so an address in code is left as typed, as it is read as no image.

use std::collections::{HashMap, HashSet};

use crate::image::Kind;
use crate::text;
use crate::time::Moment;

/// The most bytes of a name the directory and the note's file are given, well inside the 100 a
/// ustar name holds whole, the note's file being this and `.md`.
const NAME_MOST: usize = 64;
/// The most bytes of an image's file name made from its alt text, before a `-2` and its end.
const SLUG_MOST: usize = 40;
/// A tar is written in blocks of 512 bytes and padded to a record of twenty of them.
const BLOCK: usize = 512;
const RECORD: usize = 20 * BLOCK;

/// A note made into a tar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bundle {
    /// What the directory and the note's file are called, which the tar's own file is called too.
    pub name: String,
    /// The tar's bytes, whole records of them.
    pub tar: Vec<u8>,
    /// How many images went in, each once however often the note refers to it.
    pub put: usize,
    /// How many the note refers to that are not here: their addresses are left in the markdown
    /// as they were.
    pub missing: usize,
    /// How many are here but could not be read as an image that is kept, their addresses left as
    /// well.
    pub unread: usize,
}

/// Makes a note into a tar: `<name>/<name>.md`, its head made a title and each image's address
/// that of its file, and `<name>/images/`, one file an image. `changed` is the time every entry
/// carries, the note's last change. `image` gives an image's plain bytes by its id, or none if it
/// is not here; bytes that are no JPEG or PNG, as an image that did not open is given, are
/// unread. Each image goes into the tar as it comes, so that no more than one is held twice.
pub fn bundle(
    text: &str,
    changed: Moment,
    mut image: impl FnMut(&str) -> Option<Vec<u8>>,
) -> Bundle {
    let chars: Vec<char> = text.chars().collect();
    let addresses = text::addresses(text);
    let id_at =
        |at: &std::ops::Range<usize>| chars[at.start + 4..at.end].iter().collect::<String>();

    // Each image once, in the order the note first refers to it, with the first alt text any of
    // its references gives it.
    let mut images: Vec<(String, Option<String>)> = Vec::new();
    for at in &addresses {
        let id = id_at(at);
        let alt = alt_before(&chars, at.start - 1).filter(|alt| !alt.trim().is_empty());
        match images.iter_mut().find(|(known, _)| *known == id) {
            Some((_, known)) => {
                if known.is_none() {
                    *known = alt;
                }
            }
            None => images.push((id, alt)),
        }
    }

    let name = name_of(text);
    let secs = changed.ms.div_euclid(1000);
    let mut tar = Tar::default();
    tar.directory(&format!("{name}/"), secs);
    let mut taken = HashSet::new();
    let mut files: HashMap<String, String> = HashMap::new();
    let (mut missing, mut unread) = (0, 0);
    for (n, (id, alt)) in images.iter().enumerate() {
        let Some(plain) = image(id) else {
            missing += 1;
            continue;
        };
        let Some(kind) = Kind::of(&plain) else {
            unread += 1;
            continue;
        };
        let stem = alt
            .as_deref()
            .map(slug)
            .filter(|slug| !slug.is_empty())
            .unwrap_or_else(|| format!("image-{}", n + 1));
        let (mut unique, mut k) = (stem.clone(), 1);
        while !taken.insert(unique.clone()) {
            k += 1;
            unique = format!("{stem}-{k}");
        }
        let file = format!("{unique}.{}", kind.extension());
        if files.is_empty() {
            tar.directory(&format!("{name}/images/"), secs);
        }
        tar.file(&format!("{name}/images/{file}"), &plain, secs);
        files.insert(id.clone(), file);
    }

    let mut markdown = String::with_capacity(text.len());
    let mut from = 0;
    for at in &addresses {
        markdown.extend(&chars[from..at.start]);
        match files.get(&id_at(at)) {
            Some(file) => {
                markdown.push_str("images/");
                markdown.push_str(file);
            }
            None => markdown.extend(&chars[at.clone()]),
        }
        from = at.end;
    }
    markdown.extend(&chars[from..]);
    tar.file(
        &format!("{name}/{name}.md"),
        titled(&markdown).as_bytes(),
        secs,
    );
    Bundle {
        name,
        tar: tar.end(),
        put: files.len(),
        missing,
        unread,
    }
}

/// The alt text of the image whose address follows the `(` at `paren`: what the brackets just
/// before it hold, on its line, `![radiator valve]` giving `radiator valve`. A link's words name
/// the picture as well as an alt text does, so `[plate](fzn:…)` gives `plate`.
fn alt_before(chars: &[char], paren: usize) -> Option<String> {
    let close = paren.checked_sub(1).filter(|&at| chars[at] == ']')?;
    let mut depth = 0;
    for at in (0..close).rev() {
        match chars[at] {
            '\n' => return None,
            ']' => depth += 1,
            '[' if depth == 0 => return Some(chars[at + 1..close].iter().collect()),
            '[' => depth -= 1,
            _ => {}
        }
    }
    None
}

/// Whether a line is an ATX heading: one to six `#` and then a space, a tab or its end.
fn heading(line: &str) -> bool {
    let line = line.trim_start_matches(' ');
    let marks = line.len() - line.trim_start_matches('#').len();
    (1..=6).contains(&marks)
        && line[marks..]
            .chars()
            .next()
            .is_none_or(|c| c == ' ' || c == '\t')
}

/// A heading's words without its marks, a closing run of them included; any other line as it is.
fn unheaded(line: &str) -> &str {
    let line = line.trim();
    if !heading(line) {
        return line;
    }
    let words = line.trim_start_matches('#').trim();
    let closed = words.trim_end_matches('#');
    if closed.is_empty() || closed.ends_with([' ', '\t']) {
        closed.trim_end()
    } else {
        words
    }
}

/// Whether a line under a head makes it a heading already, as a line of `=` or of `-` does.
fn underline(line: &str) -> bool {
    let line = line.trim();
    !line.is_empty() && (line.bytes().all(|b| b == b'=') || line.bytes().all(|b| b == b'-'))
}

/// Whether a head is something other than words: a block of code it opens, which the engine reads
/// as code running past it, or a table's row. A `# ` before either would break what follows, and
/// neither is a name.
fn unworded(head: &str) -> bool {
    let probe: Vec<char> = head.chars().chain(['\n']).collect();
    let block = text::code(&probe).iter().any(|r| r.end >= probe.len());
    block || head.trim_start().starts_with('|')
}

/// The text with its head made its title, as the page shows an open note's, unless it is a
/// heading already, says nothing, or is no words. The rest is as it was.
fn titled(text: &str) -> String {
    let mut lines = text.lines();
    let head = lines.next().unwrap_or("");
    let leave = head.trim().is_empty()
        || heading(head)
        || lines.next().is_some_and(underline)
        || unworded(head);
    if leave {
        text.to_owned()
    } else {
        format!("# {text}")
    }
}

/// What the directory and the note's file are called: the head as brackets name it, without a
/// heading's marks, its box and the labels that end it, made a name every system takes, Windows'
/// devices given ` note` after them. A name is no wider than [`NAME_MOST`] bytes; one with no
/// letter or digit left, or a head that is no words, is `note`.
fn name_of(text: &str) -> String {
    let head = text::head(text);
    if unworded(head) {
        return "note".to_owned();
    }
    let unsafe_char = |c: char| {
        c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|')
    };
    let spaced: String = unheaded(text::bracket_name(unheaded(head)))
        .chars()
        .map(|c| if unsafe_char(c) { ' ' } else { c })
        .collect();
    let joined = spaced.split_whitespace().collect::<Vec<_>>().join(" ");
    let trim = |name: &str| name.trim_matches([' ', '.']).to_owned();
    let name = trim(cut(&trim(&joined), NAME_MOST));
    if !name.chars().any(char::is_alphanumeric) {
        "note".to_owned()
    } else if device(&name) {
        format!("{} note", trim(cut(&name, NAME_MOST - 5)))
    } else {
        name
    }
}

/// Whether Windows takes a name for a device, whatever follows its first dot: `CON`, `PRN`, `AUX`,
/// `NUL`, and `COM` or `LPT` with a digit, in any case.
fn device(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or("").trim_end();
    let upper = stem.to_ascii_uppercase();
    match upper.as_bytes() {
        b"CON" | b"PRN" | b"AUX" | b"NUL" => true,
        [b'C', b'O', b'M', d] | [b'L', b'P', b'T', d] => d.is_ascii_digit(),
        _ => false,
    }
}

/// An alt text as a file's name: lowercased, ASCII letters and digits kept and every character
/// past ASCII but a space or a control, combining marks included, every run of anything else one
/// `-`, none at either end, no wider than [`SLUG_MOST`] bytes, and Windows' devices given
/// `-image` after them.
fn slug(alt: &str) -> String {
    let mut slug = String::new();
    for c in alt.chars().flat_map(char::to_lowercase) {
        let kept =
            c.is_ascii_alphanumeric() || !(c.is_ascii() || c.is_whitespace() || c.is_control());
        if kept {
            if slug.len() + c.len_utf8() > SLUG_MOST {
                break;
            }
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug.trim_end_matches('-');
    if device(slug) {
        format!("{slug}-image")
    } else {
        slug.to_owned()
    }
}

/// A POSIX ustar archive, written an entry at a time.
#[derive(Default)]
struct Tar(Vec<u8>);

impl Tar {
    fn directory(&mut self, path: &str, secs: i64) {
        self.entry(path, b'5', 0o755, &[], secs);
    }

    fn file(&mut self, path: &str, bytes: &[u8], secs: i64) {
        self.entry(path, b'0', 0o644, bytes, secs);
    }

    fn entry(&mut self, path: &str, kind: u8, mode: u64, bytes: &[u8], secs: i64) {
        let mut header = [0u8; BLOCK];
        let (prefix, name) = split(path);
        header[..name.len()].copy_from_slice(name.as_bytes());
        octal(&mut header[100..108], mode);
        octal(&mut header[108..116], 0);
        octal(&mut header[116..124], 0);
        octal(&mut header[124..136], bytes.len() as u64);
        octal(&mut header[136..148], secs.clamp(0, 0o777_7777_7777) as u64);
        header[148..156].fill(b' ');
        header[156] = kind;
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        header[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
        let sum: u64 = header.iter().map(|&b| u64::from(b)).sum();
        header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        self.0.extend_from_slice(&header);
        self.0.extend_from_slice(bytes);
        self.0.resize(self.0.len().next_multiple_of(BLOCK), 0);
    }

    /// Two empty blocks, and the archive padded to a whole record.
    fn end(mut self) -> Vec<u8> {
        self.0.resize(self.0.len() + 2 * BLOCK, 0);
        self.0.resize(self.0.len().next_multiple_of(RECORD), 0);
        self.0
    }
}

/// A path as ustar holds it: whole in the name when it is 100 bytes or fewer, and otherwise
/// split at a `/` into a prefix of at most 155 and a name of at most 100. The names made here
/// always fit; one that could not would be cut, never written past its field.
fn split(path: &str) -> (&str, &str) {
    if path.len() <= 100 {
        return ("", path);
    }
    let fits = path[..path.len() - 1]
        .match_indices('/')
        .map(|(at, _)| (&path[..at], &path[at + 1..]))
        .find(|(prefix, name)| prefix.len() <= 155 && name.len() <= 100);
    fits.unwrap_or_else(|| ("", cut(path, 100)))
}

/// A text cut to at most `most` bytes, at a character's edge.
fn cut(text: &str, most: usize) -> &str {
    let mut end = most.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// A number in octal, as wide as its field less the NUL that ends it.
fn octal(field: &mut [u8], n: u64) {
    let width = field.len() - 1;
    field[..width].copy_from_slice(format!("{n:0width$o}").as_bytes());
    field[width] = 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALVE: &str = "4f998e75f22477b2519998d0731eeec8";
    const PLATE: &str = "3ae5eb6a75633c8ff7a8d2a11602aae2";
    const NEVER: &str = "3f9a01c23f9a01c23f9a01c23f9a01c2";

    fn jpeg(rest: &[u8]) -> Vec<u8> {
        [&[0xFF, 0xD8, 0xFF][..], rest].concat()
    }

    fn png(rest: &[u8]) -> Vec<u8> {
        [&b"\x89PNG\r\n\x1a\n"[..], rest].concat()
    }

    /// The images to be had: the valve a JPEG and the plate a PNG.
    fn held(id: &str) -> Option<Vec<u8>> {
        match id {
            VALVE => Some(jpeg(b"a radiator valve")),
            PLATE => Some(png(b"a boiler's data plate")),
            _ => None,
        }
    }

    fn at() -> Moment {
        Moment::utc(1_791_892_800)
    }

    /// An entry read back from a tar.
    #[derive(Debug, PartialEq, Eq)]
    struct Entry {
        path: String,
        kind: u8,
        mode: u64,
        mtime: u64,
        bytes: Vec<u8>,
    }

    fn field(header: &[u8]) -> &str {
        let end = header.iter().position(|&b| b == 0).unwrap_or(header.len());
        std::str::from_utf8(&header[..end]).unwrap()
    }

    fn number(header: &[u8]) -> u64 {
        u64::from_str_radix(field(header).trim(), 8).unwrap()
    }

    /// Reads a tar as ustar says, checking each header's sum and magic, the two empty blocks at
    /// its end, and that it is whole records.
    fn untar(tar: &[u8]) -> Vec<Entry> {
        assert_eq!(tar.len() % RECORD, 0);
        let mut entries = Vec::new();
        let mut at = 0;
        loop {
            let header = &tar[at..at + BLOCK];
            if header.iter().all(|&b| b == 0) {
                assert!(tar[at..at + 2 * BLOCK].iter().all(|&b| b == 0));
                break;
            }
            let sum: u64 = header
                .iter()
                .enumerate()
                .map(|(i, &b)| {
                    if (148..156).contains(&i) {
                        32
                    } else {
                        u64::from(b)
                    }
                })
                .sum();
            assert_eq!(number(&header[148..156]), sum);
            assert_eq!(&header[257..265], b"ustar\x0000");
            let (prefix, name) = (field(&header[345..500]), field(&header[..100]));
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            let size = number(&header[124..136]) as usize;
            at += BLOCK;
            entries.push(Entry {
                path,
                kind: header[156],
                mode: number(&header[100..108]),
                mtime: number(&header[136..148]),
                bytes: tar[at..at + size].to_vec(),
            });
            at += size.next_multiple_of(BLOCK);
        }
        entries
    }

    fn markdown(bundle: &Bundle) -> String {
        let md = format!("{0}/{0}.md", bundle.name);
        let entry = untar(&bundle.tar)
            .into_iter()
            .find(|e| e.path == md)
            .unwrap();
        String::from_utf8(entry.bytes).unwrap()
    }

    #[test]
    fn a_note_goes_into_a_directory_of_its_name_with_its_images_beside_it() {
        let text = format!("Boiler service\nthe valve ![radiator valve](fzn:{VALVE})\n");
        let bundle = bundle(&text, at(), held);
        assert_eq!(
            (bundle.name.as_str(), bundle.put, bundle.missing),
            ("Boiler service", 1, 0)
        );
        let entries = untar(&bundle.tar);
        let made = |path: &str, kind, mode, bytes: &[u8]| Entry {
            path: path.into(),
            kind,
            mode,
            mtime: 1_791_892_800,
            bytes: bytes.to_vec(),
        };
        assert_eq!(
            entries,
            [
                made("Boiler service/", b'5', 0o755, b""),
                made("Boiler service/images/", b'5', 0o755, b""),
                made(
                    "Boiler service/images/radiator-valve.jpg",
                    b'0',
                    0o644,
                    &jpeg(b"a radiator valve")
                ),
                made(
                    "Boiler service/Boiler service.md",
                    b'0',
                    0o644,
                    b"# Boiler service\nthe valve ![radiator valve](images/radiator-valve.jpg)\n"
                ),
            ]
        );
    }

    #[test]
    fn a_note_with_no_images_has_no_images_directory() {
        let bundle = bundle("House\nthe gutters", at(), held);
        let paths: Vec<String> = untar(&bundle.tar).into_iter().map(|e| e.path).collect();
        assert_eq!(paths, ["House/", "House/House.md"]);
        assert_eq!(bundle.tar.len(), RECORD);
    }

    #[test]
    fn only_an_address_outside_code_is_rewritten_and_nothing_else_changes() {
        let text = format!(
            "Boiler\n![](fzn:{VALVE}) and `![](fzn:{VALVE})`\n```\n![](fzn:{PLATE})\n```\n\
             ![plate](fzn:{PLATE} \"the plate\") fzn:{VALVE} [[House]] #house\n"
        );
        let bundle = bundle(&text, at(), held);
        assert_eq!(
            markdown(&bundle),
            format!(
                "# Boiler\n![](images/image-1.jpg) and `![](fzn:{VALVE})`\n```\n![](fzn:{PLATE})\n```\n\
                 ![plate](images/plate.png \"the plate\") fzn:{VALVE} [[House]] #house\n"
            )
        );
        assert_eq!((bundle.put, bundle.missing), (2, 0));
    }

    #[test]
    fn one_image_referred_to_twice_is_one_file_both_references_name() {
        let text = format!("Boiler\n![](fzn:{VALVE})\nagain ![radiator valve](fzn:{VALVE})");
        let bundle = bundle(&text, at(), held);
        assert_eq!(
            markdown(&bundle),
            "# Boiler\n![](images/radiator-valve.jpg)\nagain ![radiator valve](images/radiator-valve.jpg)"
        );
        let files = untar(&bundle.tar)
            .into_iter()
            .filter(|e| e.kind == b'0')
            .count();
        assert_eq!((files, bundle.put), (2, 1));
    }

    #[test]
    fn an_image_is_named_by_its_alt_text_or_its_place_and_a_clash_takes_a_number() {
        let images = [VALVE, PLATE, NEVER, "0123456789abcdef0123456789abcdef"];
        let any = |id: &str| images.contains(&id).then(|| jpeg(id.as_bytes()));
        let text = format!(
            "Garden\n![](fzn:{VALVE})\n![Radiator  Valve (old)](fzn:{PLATE})\n\
             ![radiator valve!](fzn:{NEVER})\n![Café über](fzn:0123456789abcdef0123456789abcdef)"
        );
        let md = markdown(&bundle(&text, at(), any));
        assert_eq!(
            md,
            "# Garden\n![](images/image-1.jpg)\n![Radiator  Valve (old)](images/radiator-valve-old.jpg)\n\
             ![radiator valve!](images/radiator-valve.jpg)\n![Café über](images/café-über.jpg)"
        );
        let clash = format!(
            "Garden\n![valve](fzn:{VALVE})\n![Valve](fzn:{PLATE})\n![image 3](fzn:{NEVER})\n![](fzn:0123456789abcdef0123456789abcdef)"
        );
        assert!(markdown(&bundle(&clash, at(), any)).ends_with(
            "![valve](images/valve.jpg)\n![Valve](images/valve-2.jpg)\n![image 3](images/image-3.jpg)\n![](images/image-4.jpg)"
        ));
        assert_eq!(
            slug(&"long ".repeat(20)),
            "long-".repeat(8).trim_end_matches('-')
        );
        assert_eq!(slug("--- ..."), "");
    }

    #[test]
    fn a_slug_keeps_every_character_past_ascii_but_spaces_and_controls() {
        assert_eq!(slug("İstanbul"), "i\u{307}stanbul");
        assert_eq!(slug("cafe\u{301} (old)"), "cafe\u{301}-old");
        assert_eq!(slug("a\u{a0}b\u{85}c"), "a-b-c");
    }

    #[test]
    fn a_name_windows_keeps_for_a_device_is_given_more() {
        let name = |head: &str| bundle(head, at(), held).name;
        assert_eq!(name("CON"), "CON note");
        assert_eq!(name("nul.txt #house"), "nul.txt note");
        assert_eq!(name("Com1"), "Com1 note");
        assert_eq!(name("LPT9"), "LPT9 note");
        assert_eq!(name("Console"), "Console");
        assert_eq!(name("COM"), "COM");
        assert_eq!(slug("Aux"), "aux-image");
        assert_eq!(slug("prn.x"), "prn-x");
        let text = format!("Boiler\n![con](fzn:{VALVE})");
        assert!(markdown(&bundle(&text, at(), held)).ends_with("![con](images/con-image.jpg)"));
    }

    #[test]
    fn an_image_not_here_keeps_its_address_and_is_counted_missing_and_one_unread_apart() {
        let text = format!(
            "Boiler\n![](fzn:{NEVER})\n![](fzn:{VALVE})\n![](fzn:{PLATE})\n![](fzn:{NEVER})"
        );
        let gif = |id: &str| {
            if id == PLATE {
                Some(b"GIF89a".to_vec())
            } else {
                held(id)
            }
        };
        let bundle = bundle(&text, at(), gif);
        assert_eq!((bundle.put, bundle.missing, bundle.unread), (1, 1, 1));
        assert_eq!(
            markdown(&bundle),
            format!(
                "# Boiler\n![](fzn:{NEVER})\n![](images/image-2.jpg)\n![](fzn:{PLATE})\n![](fzn:{NEVER})"
            )
        );
    }

    #[test]
    fn the_head_becomes_a_title_unless_it_is_a_heading_already() {
        let md = |text: &str| markdown(&bundle(text, at(), held));
        assert_eq!(
            md("[ ] Boiler service #house\nbook it"),
            "# [ ] Boiler service #house\nbook it"
        );
        assert_eq!(md("## Boiler\nbook it"), "## Boiler\nbook it");
        assert_eq!(md("#\tBoiler"), "#\tBoiler");
        assert_eq!(md("###"), "###");
        assert_eq!(md("#house Boiler"), "# #house Boiler");
        assert_eq!(md("####### Boiler"), "# ####### Boiler");
        assert_eq!(md("\nbook it"), "\nbook it");
        assert_eq!(md("#\r\nbook it"), "#\r\nbook it");
        assert_eq!(md("Boiler\r\nbook it"), "# Boiler\r\nbook it");
    }

    #[test]
    fn a_head_that_is_no_words_or_a_heading_by_its_underline_is_left_as_it_is() {
        let md = |text: &str| markdown(&bundle(text, at(), held));
        for text in [
            "```\ncode\n```\nafter",
            "```sh\nls",
            "  ~~~\ncode\n~~~",
            "| Boiler | House |\n| --- | --- |",
            "Boiler\n======\nbook it",
            "Boiler\n---\nbook it",
            "Boiler\n  -- \nbook it",
        ] {
            assert_eq!(md(text), text);
        }
        assert_eq!(md("``ls`` Boiler\nbook it"), "# ``ls`` Boiler\nbook it");
        assert_eq!(md("```ls``` Boiler"), "# ```ls``` Boiler");
        assert_eq!(md("Boiler\n-- x"), "# Boiler\n-- x");
    }

    #[test]
    fn a_head_that_is_no_words_names_the_note_note() {
        let name = |head: &str| bundle(head, at(), held).name;
        assert_eq!(name("```\ncode\n```"), "note");
        assert_eq!(name("```sh"), "note");
        assert_eq!(name("~~~"), "note");
        assert_eq!(name("| Boiler | House |"), "note");
        assert_eq!(name("---"), "note");
        assert_eq!(name("Boiler\n======"), "Boiler");
    }

    #[test]
    fn the_name_is_the_heads_as_brackets_name_it_made_safe_for_every_system() {
        let name = |head: &str| bundle(head, at(), held).name;
        assert_eq!(
            name("[ ] Boiler service #house 2026-11-11"),
            "Boiler service"
        );
        assert_eq!(name("## Boiler ##"), "Boiler");
        assert_eq!(name("# House #garden"), "House");
        assert_eq!(name("[ ] # Boiler #house"), "Boiler");
        assert_eq!(name("# [x] Boiler"), "Boiler");
        assert_eq!(name("Boiler: a/b \\ c*?\"<>|\u{7}d"), "Boiler a b c d");
        assert_eq!(name("..Boiler.. "), "Boiler");
        assert_eq!(name("#house"), "note");
        assert_eq!(name(""), "note");
        assert_eq!(name("..."), "note");
        let long = name(&"é".repeat(40));
        assert_eq!(long, "é".repeat(32));
        assert_eq!(name(&format!("{} end", "a".repeat(63))), "a".repeat(63));
    }

    #[test]
    fn a_path_too_long_for_a_name_is_split_at_a_slash() {
        let head = "b".repeat(64);
        let text = format!("{head}\n![{}](fzn:{VALVE})", "v".repeat(60));
        let bundle = bundle(&text, at(), held);
        let path = format!("{head}/images/{}.jpg", "v".repeat(40));
        assert!(path.len() > 100);
        let entries = untar(&bundle.tar);
        assert!(entries.iter().any(|e| e.path == path));
        assert_eq!(split(&path), (head.as_str(), &path[head.len() + 1..]));
    }

    #[test]
    fn a_time_before_1970_is_written_as_1970() {
        let bundle = bundle("Boiler", Moment::utc(-5), held);
        assert!(untar(&bundle.tar).iter().all(|e| e.mtime == 0));
    }
}
