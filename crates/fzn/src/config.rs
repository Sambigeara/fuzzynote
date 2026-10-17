//! Configuration: where notes live, which space to open, which editor to use, whether the list
//! takes vim's keys, and the engine weights.
//!
//! The config directory also holds the key to each space this machine opens. It must never be
//! copied along with the notes, which is why it is separate from them.

use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use fzn_engine::Weights;
use serde::{Deserialize, Serialize};

pub const CONFIG_FILE: &str = "config.toml";
/// Where this machine's one writer id was kept before each space had its own. A space opened
/// for the first time since takes it, and the file is left alone.
pub const DEVICE_FILE: &str = "device";
/// The directory of the keys to spaces, one file a space.
const KEYS_DIR: &str = "keys";
pub const DEFAULT_SPACE: &str = "main";

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Where the spaces live, one directory each. Defaults to `~/.fzn`.
    pub root: Option<String>,
    /// The space opened when none is named. Defaults to `main`.
    pub space: Option<String>,
    /// Editor for Ctrl-O. Defaults to `$VISUAL`, then `$EDITOR`, then `vi`.
    pub editor: Option<String>,
    /// Vim's keys for the list: a row walked to takes vim's letters, and one written in types.
    pub vim: bool,
    /// A relay to sync with. With none, nothing leaves this machine.
    pub relay: Option<String>,
    pub weights: Weights,
}

/// The default config file, with comments. It parses to `Config::default()`.
pub const TEMPLATE: &str = r#"# fzn configuration.
# The weights tune the relevance engine: change a number and restart.

# Where your spaces live, one directory each. Only fzn writes them; a relay is how they get from
# one machine to another.
# root = "~/.fzn"

# The space that plain `fzn` opens. `fzn --space NAME` opens another.
# space = "main"

# Editor opened by Ctrl-O. Falls back to $VISUAL, then $EDITOR, then vi. It is split on spaces,
# so "code --wait" runs code with --wait, and a path with a space in it cannot be given here.
# editor = "vim"

# Vim's keys for the list. A row walked to is in normal mode, a block cursor, where j and k walk,
# i and a type, o opens a row, x ticks, D deletes, u undoes and Esc does nothing; a row written in
# types, a bar, until Esc. The line always types. The body is your editor's, so this is the list.
# vim = true

# A relay to sync with: a machine running `fzn relay` that your devices can reach. With this
# set, fzn syncs whichever space it opens for as long as it is open, and works as before when
# the relay is out of reach. What it is sent is sealed with the space's passphrase, so the
# relay cannot read it. Leave it out and nothing leaves this machine. A token in the address,
# ws://NAME:TOKEN@host, is what lets this device make a space on the relay; fzn relay add prints
# one.
# relay = "ws://nas.local:3960"

[weights]
lexical = 1.0              # words in common, weighted by how rare they are
explicit = 0.8             # shared #tags, @people and [[links]]
temporal = 0.3             # shared dates and closeness in time
fuzzy = 0.7                # how much a misspelt word counts relative to an exact one
fuzzy_min_similarity = 0.5 # how close a misspelling has to be to count
about = 0.8                # how much the name of a note counts for the notes linked to it
bm25_k1 = 1.2
bm25_b = 0.75
halflife_days = 30.0       # the closeness-in-time bonus halves every this many days
"#;

pub fn home() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn config_dir() -> PathBuf {
    match std::env::var_os("XDG_CONFIG_HOME") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("fzn"),
        _ => home().join(".config").join("fzn"),
    }
}

pub fn expand_tilde(path: &str) -> PathBuf {
    match path.strip_prefix("~/") {
        Some(rest) => home().join(rest),
        None if path == "~" => home(),
        None => PathBuf::from(path),
    }
}

/// Reads the config file, writing the commented template first if there is none.
pub fn load_or_create(dir: &Path) -> Result<Config> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(CONFIG_FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            fs::write(&path, TEMPLATE).with_context(|| format!("write {}", path.display()))?;
            TEMPLATE.to_owned()
        }
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    toml::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

/// The directory of the keys to spaces.
pub fn keys_dir(config_dir: &Path) -> PathBuf {
    config_dir.join(KEYS_DIR)
}

/// Root precedence: command line, then `$FZN_ROOT`, then the config file, then `~/.fzn`.
pub fn resolve_root(cli: Option<&str>, env: Option<&str>, cfg: &Config) -> PathBuf {
    [cli, env, cfg.root.as_deref()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(expand_tilde)
        .unwrap_or_else(|| home().join(".fzn"))
}

/// Relay precedence: command line, then `$FZN_RELAY`, then the config file. With none of them,
/// there is no sync.
pub fn resolve_relay(cli: Option<&str>, env: Option<&str>, cfg: &Config) -> Option<String> {
    [cli, env, cfg.relay.as_deref()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Space precedence: command line, then the config file, then `main`. A space is one directory
/// under the root, so its name has to be a single plain path component.
pub fn resolve_space(cli: Option<&str>, cfg: &Config) -> Result<String> {
    let name = [cli, cfg.space.as_deref()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SPACE);
    let mut parts = Path::new(name).components();
    match (parts.next(), parts.next()) {
        (Some(Component::Normal(_)), None) => Ok(name.to_owned()),
        _ => bail!("{name:?} cannot name a space"),
    }
}

/// Editor precedence: the config file, then `$VISUAL`, then `$EDITOR`, then `vi`.
pub fn editor(cfg: &Config) -> String {
    cfg.editor
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::env::var("VISUAL")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .or_else(|| {
            std::env::var("EDITOR")
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .unwrap_or_else(|| "vi".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::system::random_u64;

    #[test]
    fn template_parses_to_defaults() {
        let cfg: Config = toml::from_str(TEMPLATE).unwrap();
        assert_eq!(cfg, Config::default());
    }

    #[test]
    fn partial_config_keeps_defaults_elsewhere() {
        let cfg: Config = toml::from_str("editor = \"nano\"\n[weights]\nlexical = 2.0\n").unwrap();
        assert_eq!(cfg.editor.as_deref(), Some("nano"));
        assert_eq!(cfg.weights.lexical, 2.0);
        assert_eq!(cfg.weights.explicit, Weights::default().explicit);
        assert!(!cfg.vim, "vim's keys are asked for");
        let cfg: Config = toml::from_str("vim = true\n").unwrap();
        assert!(cfg.vim);
    }

    #[test]
    fn root_precedence() {
        let cfg = Config {
            root: Some("/cfg".into()),
            ..Config::default()
        };
        assert_eq!(
            resolve_root(Some("/cli"), Some("/env"), &cfg),
            PathBuf::from("/cli")
        );
        assert_eq!(
            resolve_root(None, Some("/env"), &cfg),
            PathBuf::from("/env")
        );
        assert_eq!(resolve_root(None, Some(""), &cfg), PathBuf::from("/cfg"));
        assert_eq!(
            resolve_root(None, None, &Config::default()),
            home().join(".fzn")
        );
        assert_eq!(expand_tilde("~/x"), home().join("x"));
    }

    #[test]
    fn relay_precedence() {
        let cfg = Config {
            relay: Some("ws://cfg".into()),
            ..Config::default()
        };
        let relay = |cli, env| resolve_relay(cli, env, &cfg);
        assert_eq!(
            relay(Some("ws://cli"), Some("ws://env")).unwrap(),
            "ws://cli"
        );
        assert_eq!(relay(None, Some("ws://env")).unwrap(), "ws://env");
        assert_eq!(relay(Some(" "), Some("")).unwrap(), "ws://cfg");
        assert_eq!(
            resolve_relay(None, None, &Config::default()),
            None,
            "and with none, nothing leaves this machine"
        );
    }

    #[test]
    fn space_precedence_and_names() {
        let cfg = Config {
            space: Some("work".into()),
            ..Config::default()
        };
        assert_eq!(resolve_space(Some("old"), &cfg).unwrap(), "old");
        assert_eq!(resolve_space(None, &cfg).unwrap(), "work");
        assert_eq!(resolve_space(Some(" "), &cfg).unwrap(), "work");
        assert_eq!(resolve_space(None, &Config::default()).unwrap(), "main");
        for bad in ["..", ".", "a/b", "/abs"] {
            assert!(resolve_space(Some(bad), &cfg).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_config_is_created_on_first_run() {
        let dir = std::env::temp_dir().join(format!("fzn-config-test-{:x}", random_u64()));
        let cfg = load_or_create(&dir).unwrap();
        assert_eq!(cfg, Config::default());
        assert!(dir.join(CONFIG_FILE).exists());
        assert_eq!(keys_dir(&dir), dir.join("keys"));
        fs::remove_dir_all(&dir).unwrap();
    }
}
