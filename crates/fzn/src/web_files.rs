//! Which files of the web directory are the page, and the name a set of them goes by.
//!
//! The build script reads this file as well as the relay, so that the page built into fzn and a
//! directory given with `--web` are listed and named the same way: the same tree gives the same
//! build, whichever of the two serves it. The list is by place and kind, not everything under
//! `web/`, because the bundles' sources and their `node_modules` live there too; release.sh
//! packs the same files for the page's archive.

use std::fs;
use std::path::Path;

use sha2::{Digest, Sha256};

/// Where the page's files are, and of which kinds: the top's files of the page's kinds (not
/// build.sh), the engine in `pkg`, and the bundles in `vendor` and `vendor/code`.
const PLACES: &[(&str, &[&str])] = &[
    (
        "",
        &["html", "js", "css", "svg", "png", "ico", "webmanifest"],
    ),
    ("pkg", &["js", "wasm"]),
    ("vendor", &["js"]),
    ("vendor/code", &["js"]),
];

/// Whether a path under the web directory, with `/` between its parts, is one of the page's
/// files by its place and kind, so that a directory given with `--web` serves what would be
/// built in and nothing else.
pub fn is_listed(path: &str) -> bool {
    let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
    let kind = name.rsplit_once('.').map_or("", |(_, kind)| kind);
    !name.starts_with('.')
        && PLACES
            .iter()
            .any(|&(place, kinds)| place == dir && kinds.contains(&kind))
}

/// The page's files under `web`, as paths relative to it with `/` between their parts, sorted.
pub fn listed(web: &Path) -> Vec<String> {
    let mut files = Vec::new();
    for &(dir, _) in PLACES {
        let Ok(entries) = fs::read_dir(web.join(dir)) else {
            continue;
        };
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let path = if dir.is_empty() {
                name
            } else {
                format!("{dir}/{name}")
            };
            if is_listed(&path) && entry.path().is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// The name of a page: sixteen hex digits of a SHA-256 over its files in path order, each as its
/// path, a zero byte, its length in eight bytes and its bytes. A content hash, so that the same
/// files put back in place are the same build and a tab holding them has nothing to reload.
pub fn build_of<'a>(files: impl IntoIterator<Item = (&'a str, &'a [u8])>) -> String {
    let mut hash = Sha256::new();
    for (path, bytes) in files {
        hash.update(path.as_bytes());
        hash.update([0]);
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    hex16(&hash.finalize())
}

/// The tag a browser holds one file by: sixteen hex digits of its SHA-256. The build script
/// tags the page it builds in, so the relay itself tags files only in its tests.
#[cfg_attr(not(test), allow(dead_code))]
pub fn etag_of(bytes: &[u8]) -> String {
    hex16(&Sha256::digest(bytes))
}

fn hex16(digest: &[u8]) -> String {
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// The page's own record of its build: `index.html` carries this tag empty, and whoever serves
/// it fills it, so a tab knows which page it is running.
pub const BUILD_META: &str = r#"<meta name="fzn-build" content="">"#;

/// `index.html` with its build written into it, or as it is if it has no place for one.
pub fn with_build(html: &[u8], build: &str) -> Vec<u8> {
    let tag = BUILD_META.as_bytes();
    match html.windows(tag.len()).position(|w| w == tag) {
        Some(at) => {
            let filled = format!(r#"<meta name="fzn-build" content="{build}">"#);
            let mut out = html[..at].to_vec();
            out.extend_from_slice(filled.as_bytes());
            out.extend_from_slice(&html[at + tag.len()..]);
            out
        }
        None => html.to_vec(),
    }
}
