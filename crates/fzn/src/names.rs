//! Who may make a space on a relay: a name the relay's admin has given a token, or the relay's
//! own root.
//!
//! Entering a space needs only its key, so the token is for one act alone, making a space, which
//! is what takes the relay's disk. A name's spaces live under the name in the relay's root, so
//! two people's `main` are two directories; the root's own spaces stay where they always were,
//! so every device that had them keeps them with nothing to change. The relay keeps only the
//! SHA-256 of each token, so a copy of its files gives no token, and those files may be read by
//! anyone, as a relay run by a service manager under a user of its own has to read files its
//! admin wrote. They are read on each connection that needs them, so a name given or taken away
//! holds from the next connection, with no restart.

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use fzn_engine::replica::names_a_log;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::wire::PORT;

/// Under the config directory, where a relay keeps who may make spaces on it.
const RELAY_DIR: &str = "relay";
/// The root's token, beside the names.
const ROOT_TOKEN: &str = "token";
/// One file a name, named by it.
const NAMES_DIR: &str = "tenants";

/// What a token file holds: the SHA-256 of the token, in lowercase hex.
#[derive(Serialize, Deserialize)]
struct Held {
    token: String,
}

/// The hash of a token, as it is kept and compared.
pub type Hash = [u8; 32];

/// Whether two secrets, or their hashes, are the same, taking as long whichever byte differs.
pub fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |differ, (x, y)| differ | (x ^ y)) == 0
}

/// The SHA-256 of a token as it was offered.
pub fn hash(token: &str) -> Hash {
    Sha256::digest(token.as_bytes()).into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Option<Hash> {
    let text = text.trim();
    if text.len() != 64 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut hash = [0u8; 32];
    for (i, byte) in hash.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(hash)
}

/// The directory a relay keeps its tokens in, under fzn's config directory.
pub fn dir(config: &Path) -> PathBuf {
    config.join(RELAY_DIR)
}

fn name_file(config: &Path, name: &str) -> PathBuf {
    dir(config).join(NAMES_DIR).join(name)
}

fn root_file(config: &Path) -> PathBuf {
    dir(config).join(ROOT_TOKEN)
}

/// The hash a token file holds, or nothing if there is no such file.
fn read(path: &Path) -> Result<Option<Hash>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    // Said without the parser's words, which quote the file.
    let held: Held =
        toml::from_str(&text).map_err(|_| anyhow::anyhow!("{} does not parse", path.display()))?;
    match unhex(&held.token) {
        Some(hash) => Ok(Some(hash)),
        None => bail!("{} holds no token's hash", path.display()),
    }
}

/// The hash of a name's token, or nothing if the relay has no such name.
pub fn of_name(config: &Path, name: &str) -> Result<Option<Hash>> {
    if !names_a_log(name) {
        return Ok(None);
    }
    read(&name_file(config, name))
}

/// The hash of the root's token, or nothing if anyone may make a space at the root.
pub fn of_root(config: &Path) -> Result<Option<Hash>> {
    read(&root_file(config))
}

/// Whether a name has been given on this relay, so that no space at the root may take it.
pub fn is_name(config: &Path, name: &str) -> bool {
    names_a_log(name) && name_file(config, name).is_file()
}

/// The names given on this relay, in order.
pub fn names(config: &Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(dir(config).join(NAMES_DIR)) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| names_a_log(name))
        .collect();
    names.sort();
    names
}

/// Whether a directory holds a space: a log, a lock, or a kept auth key.
pub fn holds_a_space(dir: &Path) -> bool {
    fs::read_dir(dir).is_ok_and(|entries| {
        entries.filter_map(|entry| entry.ok()).any(|entry| {
            let path = entry.path();
            path.extension().is_some_and(|x| x == "jsonl")
                || path
                    .file_name()
                    .is_some_and(|n| n == ".lock" || n == ".auth")
        })
    })
}

/// A new token: 16 bytes from the operating system, as 32 lowercase hex digits.
fn new_token() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("no randomness for a token: {e}"))?;
    Ok(hex(&bytes))
}

/// Writes a token's hash, replacing what was there, so that a connection reading it at the same
/// moment reads the old file or the new and never half of one.
fn write(path: &Path, token: &str) -> Result<()> {
    let parent = path.parent().context("a token file has a directory")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    // Readable by all whatever the umask of whoever runs this, since a relay run under a user
    // of its own reads what its admin wrote: `relay/`, and `relay/tenants/` for a name.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let relay = if parent.ends_with(NAMES_DIR) {
            parent.parent()
        } else {
            None
        };
        for dir in std::iter::once(parent).chain(relay) {
            fs::set_permissions(dir, fs::Permissions::from_mode(0o755))
                .with_context(|| format!("open {} to the relay", dir.display()))?;
        }
    }
    let text = toml::to_string(&Held {
        token: hex(&hash(token)),
    })?;
    // Named with a dot first, so that it is never taken for a name while it is written.
    let file = path.file_name().context("a token file has a name")?;
    let fresh = parent.join(format!(".{}.new", file.to_string_lossy()));
    let written = File::create(&fresh).and_then(|mut file| {
        file.write_all(text.as_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o644))?;
        }
        file.sync_all()
    });
    written.with_context(|| format!("write {}", fresh.display()))?;
    fs::rename(&fresh, path).with_context(|| format!("write {}", path.display()))
}

/// Lets a name make spaces on the relay, under that name in its root, and gives the token that
/// does it, which is kept nowhere. Giving a name again replaces its token.
pub fn add(config: &Path, root: &Path, name: &str) -> Result<String> {
    // A name may not start with a dash, so that a flag given in its place is not taken for one.
    if !names_a_log(name) || name.starts_with('-') {
        bail!("{name:?} cannot be a name on the relay: use letters, digits, '-', '_' and '.'");
    }
    if holds_a_space(&root.join(name)) {
        bail!("{name} is a space at the root of {}", root.display());
    }
    let token = new_token()?;
    write(&name_file(config, name), &token)?;
    Ok(token)
}

/// Closes the root to anyone without the token this gives, which is kept nowhere. Asking again
/// replaces it.
pub fn close_root(config: &Path) -> Result<String> {
    let token = new_token()?;
    write(&root_file(config), &token)?;
    Ok(token)
}

/// What `fzn relay add` and `fzn relay token` say, the one time a token is shown: the two
/// places it goes, the relay's address in a terminal's config and a link for a browser, and
/// where the spaces it makes will be. The command does not know the host the devices reach
/// the relay by, so it writes `RELAY` for it.
pub fn told(config: &Path, root: &Path, name: Option<&str>, token: &str) -> String {
    let credential = match name {
        Some(name) => format!("{name}:{token}"),
        None => token.to_owned(),
    };
    let forms = format!(
        "  in config.toml:  relay = \"ws://{credential}@RELAY:{PORT}\"\n  \
         in a browser:    http://RELAY:{PORT}/#{credential}\n\
         where RELAY is this relay's host as the devices reach it.\n"
    );
    match name {
        Some(name) => format!(
            "{name} may make spaces on this relay. This is shown once; fzn relay add {name} \
             again replaces it.\n{forms}{name}'s spaces will be under {}. To take {name} off \
             the relay, delete {}; the directory is yours to delete.",
            root.join(name).display(),
            name_file(config, name).display(),
        ),
        None => format!(
            "Spaces at the root of {} now need this to be made; the devices that have them \
             need nothing. This is shown once; fzn relay token again replaces it.\n{forms}\
             To open the root again, delete {}.",
            root.display(),
            root_file(config).display(),
        ),
    }
}

/// What a relay says when it starts about who may make spaces on it.
pub fn said_at_start(config: &Path) -> Vec<String> {
    let mut said = vec![match of_root(config) {
        Ok(Some(_)) => "spaces at the root are made with the relay's token".to_owned(),
        Ok(None) => "spaces at the root can be made by anyone who connects; fzn relay token \
                     closes that"
            .to_owned(),
        Err(why) => format!("no space can be made at the root, since {why:#}"),
    }];
    let names = names(config);
    if !names.is_empty() {
        said.push(format!(
            "the names that may make spaces here, from {}: {}",
            dir(config).join(NAMES_DIR).display(),
            names.join(", ")
        ));
    }
    said
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relay::tests::temp_dir;

    #[test]
    fn a_name_keeps_the_hash_of_its_token_and_not_the_token() {
        let root = temp_dir();
        let (config, spaces) = (root.join("config"), root.join("spaces"));
        let token = add(&config, &spaces, "joe").unwrap();
        assert_eq!(token.len(), 32);
        assert!(token.bytes().all(|b| b.is_ascii_hexdigit()));
        let file = name_file(&config, "joe");
        let text = fs::read_to_string(&file).unwrap();
        assert!(!text.contains(&token), "{text}");
        assert_eq!(of_name(&config, "joe").unwrap(), Some(hash(&token)));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&file), 0o644);
            // Whatever the umask, the relay can read its way to the file.
            assert_eq!(mode(&dir(&config)), 0o755);
            assert_eq!(mode(&dir(&config).join(NAMES_DIR)), 0o755);
        }

        // Given again, the name has a new token and the old one makes nothing.
        let again = add(&config, &spaces, "joe").unwrap();
        assert_ne!(again, token);
        assert_eq!(of_name(&config, "joe").unwrap(), Some(hash(&again)));
        assert!(is_name(&config, "joe"));
        assert!(!is_name(&config, "alice"));
        assert_eq!(of_name(&config, "alice").unwrap(), None);
        assert_eq!(names(&config), ["joe"]);
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_name_that_is_a_space_at_the_root_or_is_not_plain_is_refused() {
        let root = temp_dir();
        let (config, spaces) = (root.join("config"), root.join("spaces"));
        fs::create_dir_all(spaces.join("main")).unwrap();
        fs::write(spaces.join("main").join("3f9a01c2.jsonl"), "a1\n").unwrap();
        let why = add(&config, &spaces, "main").unwrap_err().to_string();
        assert!(why.contains("main is a space at the root"), "{why}");
        for name in ["", ".hidden", "a/b", "a b", "--listen"] {
            assert!(add(&config, &spaces, name).is_err(), "{name:?}");
        }
        assert!(names(&config).is_empty());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_token_is_shown_in_the_two_forms_it_is_used_in() {
        let (config, spaces) = (Path::new("/etc/fzn"), Path::new("/var/lib/fzn"));
        let token = "3f9a01c2d4e5f60718293a4b5c6d7e8f";
        let told = told(config, spaces, Some("joe"), token);
        assert!(
            told.contains(&format!("relay = \"ws://joe:{token}@RELAY:3960\"")),
            "{told}"
        );
        assert!(
            told.contains(&format!("http://RELAY:3960/#joe:{token}")),
            "{told}"
        );
        assert!(told.contains("/var/lib/fzn/joe"), "{told}");
        assert!(told.contains("/etc/fzn/relay/tenants/joe"), "{told}");
        let told = super::told(config, spaces, None, token);
        assert!(
            told.contains(&format!("relay = \"ws://{token}@RELAY:3960\"")),
            "{told}"
        );
        assert!(
            told.contains(&format!("http://RELAY:3960/#{token}")),
            "{told}"
        );
    }

    #[test]
    fn the_root_is_open_until_it_has_a_token() {
        let root = temp_dir();
        let config = root.join("config");
        assert_eq!(of_root(&config).unwrap(), None);
        assert!(said_at_start(&config)[0].contains("anyone who connects"));
        let token = close_root(&config).unwrap();
        assert_eq!(of_root(&config).unwrap(), Some(hash(&token)));
        add(&config, &root.join("spaces"), "joe").unwrap();
        let said = said_at_start(&config);
        assert_eq!(
            said[0],
            "spaces at the root are made with the relay's token"
        );
        assert!(said[1].ends_with(": joe"), "{said:?}");
        fs::write(root_file(&config), "token = \"short\"\n").unwrap();
        assert!(
            of_root(&config).is_err(),
            "a file that holds no hash is said"
        );
        fs::remove_dir_all(&root).unwrap();
    }
}
