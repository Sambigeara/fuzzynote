//! A space's key on this machine: asked for once, as a passphrase, and kept.
//!
//! Deriving the key takes a moment on purpose, so it is done once a machine and the key kept in
//! the config directory, readable only by you, beside the config and never with the notes. The
//! passphrase is asked for on the terminal before the app takes the screen, with nothing shown
//! as it is typed, and twice for a space that has nothing in it yet, since a mistyped first
//! passphrase is the space's passphrase from then on. Scripts answer with `$FZN_PASSPHRASE`.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, IsTerminal, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use fzn_engine::Key;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::terminal;

use crate::config;

/// Where a space's key is kept.
fn path(space: &str, config_dir: &Path) -> PathBuf {
    config::keys_dir(config_dir).join(space)
}

/// The space's key: the one kept, else one derived from `$FZN_PASSPHRASE`, else one derived
/// from a passphrase asked for on the terminal. A key derived here is kept for next time.
pub fn key_for(space: &str, config_dir: &Path, dir: &Path) -> Result<Key> {
    let given = std::env::var("FZN_PASSPHRASE").ok();
    key_from(space, config_dir, dir, given)
}

/// [`key_for`], with what `$FZN_PASSPHRASE` says given to it.
fn key_from(space: &str, config_dir: &Path, dir: &Path, given: Option<String>) -> Result<Key> {
    let kept = path(space, config_dir);
    match fs::read_to_string(&kept) {
        Ok(text) => {
            return Key::decode(&text).ok_or_else(|| {
                anyhow!(
                    "{} does not hold a key: run fzn key to make it again",
                    kept.display()
                )
            });
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("read {}", kept.display())),
    }
    let passphrase = match given.filter(|p| !p.is_empty()) {
        Some(passphrase) => passphrase,
        None => ask(space, is_new(dir))?,
    };
    eprintln!("deriving the key…");
    let key = Key::from_passphrase(&passphrase, space);
    keep(&kept, &key)?;
    Ok(key)
}

/// Forgets the key kept for a space, so that the next open asks for the passphrase again.
pub fn forget(space: &str, config_dir: &Path) -> Result<()> {
    let kept = path(space, config_dir);
    match fs::remove_file(&kept) {
        Err(e) if e.kind() != ErrorKind::NotFound => {
            Err(e).with_context(|| format!("delete {}", kept.display()))
        }
        _ => Ok(()),
    }
}

/// Whether a space has nothing in it yet, so that its passphrase is being chosen now.
fn is_new(dir: &Path) -> bool {
    fs::read_dir(dir).map_or(true, |entries| {
        !entries
            .filter_map(|entry| entry.ok())
            .any(|entry| entry.path().extension().is_some_and(|x| x == "jsonl"))
    })
}

/// Writes a key where only its owner can read it.
fn keep(path: &Path, key: &Key) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    }
    let mut options = OpenOptions::new();
    options.create(true).write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        options.mode(0o600);
        // A file there already keeps its mode through an open, so it is set again.
        if path.exists() {
            fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        }
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("write {}", path.display()))?;
    writeln!(file, "{}", key.encode()).with_context(|| format!("write {}", path.display()))
}

/// Asks for the passphrase on the terminal, twice for a new space.
fn ask(space: &str, new: bool) -> Result<String> {
    if !std::io::stdin().is_terminal() {
        bail!("fzn needs the passphrase for {space}: set FZN_PASSPHRASE, or run fzn in a terminal");
    }
    // A space with nothing in it here is either new, and its passphrase is being chosen, or
    // is being opened on another machine; either way it is typed twice, since nothing here can
    // yet say whether it is right.
    let prompt = if new {
        format!("The passphrase for {space}, which has nothing in it here yet: ")
    } else {
        format!("The passphrase for {space}: ")
    };
    let passphrase = read_hidden(&prompt)?;
    if passphrase.is_empty() {
        bail!("an empty passphrase is refused");
    }
    if new {
        if read_hidden("Type it again: ")? != passphrase {
            bail!("the two passphrases were not the same, so nothing was kept");
        }
        eprintln!(
            "There is no way back from a forgotten passphrase: the notes in {space} are sealed \
             with it."
        );
    }
    Ok(passphrase)
}

/// Reads a line from the terminal without showing it.
fn read_hidden(prompt: &str) -> Result<String> {
    eprint!("{prompt}");
    std::io::stderr().flush()?;
    terminal::enable_raw_mode()?;
    let read = read_raw();
    let _ = terminal::disable_raw_mode();
    eprintln!();
    read
}

fn read_raw() -> Result<String> {
    let mut text = String::new();
    loop {
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Enter => return Ok(text),
            KeyCode::Backspace => {
                text.pop();
            }
            KeyCode::Esc => bail!("no passphrase given"),
            KeyCode::Char('c' | 'd') if ctrl => bail!("no passphrase given"),
            KeyCode::Char(c) if !ctrl => text.push(c),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::temp_dir;

    #[test]
    fn a_key_is_derived_from_the_passphrase_once_and_kept() {
        let dir = temp_dir();
        let (config, space) = (dir.join("config"), dir.join("garden"));
        let key = key_from("garden", &config, &space, Some("hedge".into())).unwrap();
        assert_eq!(key, Key::from_passphrase("hedge", "garden"));
        let kept = config.join("keys").join("garden");
        assert_eq!(
            fs::read_to_string(&kept).unwrap(),
            format!("{}\n", key.encode())
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&kept).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // Kept, it is what is used, whatever is given.
        let again = key_from("garden", &config, &space, Some("another".into())).unwrap();
        assert_eq!(again, key);

        // Forgotten, it is asked for again. With nothing given and no terminal to ask on, it
        // says how to give it.
        forget("garden", &config).unwrap();
        forget("garden", &config).unwrap();
        assert!(!kept.exists());
        if !std::io::stdin().is_terminal() {
            let why = key_from("garden", &config, &space, Some(String::new()))
                .unwrap_err()
                .to_string();
            assert!(why.contains("FZN_PASSPHRASE"), "{why}");
        }

        fs::write(&kept, "not a key\n").unwrap();
        assert!(key_from("garden", &config, &space, None).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_space_with_no_logs_is_new() {
        let dir = temp_dir();
        assert!(is_new(&dir.join("missing")));
        fs::write(dir.join(".writer"), "3f9a01c2\n").unwrap();
        assert!(is_new(&dir));
        fs::write(dir.join("3f9a01c2.jsonl"), "").unwrap();
        assert!(!is_new(&dir));
        fs::remove_dir_all(&dir).unwrap();
    }
}
