//! fzn: a blank page that remembers what you wrote.
//!
//! The command line is read by hand, because the manual is prose in fzn's own voice, which no
//! parser writes, and the parsing is a few dozen lines. Whatever goes wrong is said on one line,
//! `fzn: ` and why, and the exit status says what kind of wrong it was: 1 when nothing was done,
//! 2 when the arguments made no sense, which is what a script or an agent driving fzn checks.

mod config;
mod import;
mod key;
mod names;
mod relay;
mod store;
mod sync;
mod system;
mod ui;
mod web_files;
mod wire;

use std::io::{IsTerminal as _, Read as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow};
use fzn_engine::Seal;

const USAGE: &str = "\
fzn [--space NAME]                      open your notes
fzn [--space NAME] query LINE           write what the line finds, one note to a line
fzn [--space NAME] export               write every note, one to a line, oldest first
fzn [--space NAME] apply [FILE]         write the notes given, from FILE or standard input
fzn [--space NAME] tick ID [ID ...]     tick these tasks, by the ids query writes
fzn [--space NAME] untick ID [ID ...]   bring these ticked tasks back
fzn [--space NAME] import-jsonl FILE    import such a file, or a fuzzynote 2022 export
fzn [--space NAME] import-markdown DIR  import a directory of markdown pages (Silverbullet, Obsidian)
fzn [--space NAME] key                  type the space's passphrase again
fzn relay [--listen ADDRESS] [--web DIR]  pass notes between your devices
fzn relay add NAME                      let NAME make spaces on this relay, and print the token once
fzn relay token                         the same for the spaces at the relay's root
fzn --help
fzn --version                           say the version, and the commit and day it was built from

Notes live in spaces: separate sets of notes, one directory each under the root. Plain fzn opens
the space named in ~/.config/fzn/config.toml, which is main to begin with. --space NAME (or -s)
opens another, and creates it if it is new.

The root is --root DIR, else $FZN_ROOT, else the root in the config file, else ~/.fzn. Only fzn
writes a space's directory. Several windows may have a space open at once on one machine, each
in its own place, and what one writes the others list within a moment; query, export, apply,
tick, untick and the importers work beside them. A relay, not copying files, is how a space gets
from one machine to another.

Every space has a passphrase. The first time fzn opens a space on a machine it asks for it,
twice if the space is new, and keeps the key it makes from it in ~/.config/fzn/keys. Each note is
sealed with that key before it is written, so neither the files nor a relay can be read without
it. A forgotten passphrase is lost notes: there is no reset. If it was mistyped, fzn key asks
again. $FZN_PASSPHRASE answers in place of the question, for scripts.

query writes the notes a line finds, typed as on the line, one plain line of JSON each, readable
without fzn: every one of them, archived ones too, in the order the app lists them, which with
nothing to rank by is the space's order and otherwise best first. fzn query '#house /-mon..' is
everything tagged #house written since last Monday, and fzn query '[ ]' the open tasks. export
writes every live note the same way, oldest first. Neither writes anything, and jq orders and
picks from either.

apply reads such lines, one note each, and writes each note as it then reads, by the app's own
rules: date words are written out, [[brackets]] become links, and a task's box and its archived
flag follow each other, as Ctrl-V sets them. Its text, links, archived and deleted are taken
from the line, and the rest is fzn's: a line with no id is a new note at the top, and where a
note sits is not changed from outside. All or none: a line that is not a note, that names no
note, or whose stamp is not the note's as it is held, since the note changed after it was read,
is said, nothing is written, and apply exits 1. It writes each note it was given as it now
stands, as query writes it, so fzn query '#old' | jq ... | fzn apply is a change with its
receipt, and the same again writes nothing. tick and untick are the common case: each does to
the tasks named what Ctrl-V does, [ ] to [x] and archived, or back. All three take the key kept
for the space and never ask for the passphrase. import-jsonl is the way back in for a whole
export: it takes the lines as they are, ids and times included, so it restores notes and does not
edit them, and a file put in a space's directory is not read.

Exit status: 0 when done, 1 when nothing was written and why is said, 2 when the arguments made
no sense.

To sync, run fzn relay on a machine your devices can reach, and name that machine on each
device: relay = \"ws://that-machine:3960\" in the config file, or --relay, or $FZN_RELAY. fzn then
syncs whichever space it opens for as long as it is open, and works as before when the relay is
out of reach. A relay serves every space under its root and holds their sealed lines, which it
cannot read. Its root is its own: fzn will not open a space the relay holds. It listens on
127.0.0.1:3960 unless --listen says otherwise. A space is entered with its passphrase: the relay
asks each connection to prove the key and sends nothing before it does. A space is made with a
token: fzn relay add NAME prints one for NAME's spaces, which live under NAME in the relay's
root and are asked for as ws://NAME:TOKEN@host; fzn relay token prints one for the spaces at the
root, which anyone who connects may make until then. Put the token in the relay's address on a
device that makes spaces; a device that only opens spaces that exist needs none. The relay has
no TLS, so let it listen only on a private network, or behind a proxy that has it. It also
hands out the web app, which is built into
fzn, so that a browser can open the relay's address and be one more device. With --web DIR it
hands out the page in that directory instead, the web directory of this repository, which is
how the page is worked on.

If fzn says a log has forked, two machines have been writing under one writer id, usually
because a space's directory was copied from one to the other. Nothing is lost, and nothing is
yours to do: each machine starts a new log of its own, which retires the shared one, and every
note both wrote is kept.

A date in a note is plain text, written 2026-10-16. /today, /tomorrow, /yesterday, a day of the
week such as /fri, and /+3 or /-2 for days from today are quicker ways of typing one: saving the
note writes the date in their place. /-mon is the last Monday before today.

A note whose head starts [ ] is a task. Archiving it ticks it, [x], and bringing it back unticks
it.

On the line, words find and everything else holds: \"a phrase\" must be in a note, #tag, @person
and a day must be on it, [ ] holds the open tasks, and a span such as /-mon 18:00..,
2026-10-01..2026-10-05 or ..2026-10-05 holds the notes written in it. ~ at the start of the line
finds heads by their letters in order: ~bsv finds Boiler service.";

/// What `fzn query` with nothing after it is told.
const QUERY_NEEDS: &str = "query needs a line, as typed in the app";

/// What the command line asked for.
#[derive(Debug, PartialEq)]
enum Cmd {
    Open,
    Help,
    Version,
    Relay,
    /// Let this name make spaces on the relay.
    RelayAdd(String),
    /// Let only the holder of a token make spaces at the relay's root.
    RelayToken,
    Key,
    /// What a line finds.
    Query(String),
    /// Every note.
    Export,
    /// The notes given in a file, or on standard input when there is none.
    Apply(Option<PathBuf>),
    /// These notes ticked, or with `done` false unticked.
    Tick {
        ids: Vec<String>,
        done: bool,
    },
    ImportJsonl(PathBuf),
    ImportMarkdown(PathBuf),
}

/// What the command line said, besides the command.
#[derive(Debug, PartialEq)]
struct Args {
    root: Option<String>,
    space: Option<String>,
    relay: Option<String>,
    listen: Option<String>,
    web: Option<PathBuf>,
    cmd: Cmd,
}

/// A command line that made no sense, which exits 2 where anything else that fails exits 1.
#[derive(Debug)]
struct Usage(String);

impl std::fmt::Display for Usage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Usage {}

fn usage(why: impl Into<String>) -> anyhow::Error {
    Usage(why.into()).into()
}

/// The argument after the one being read, which is what it needs: `what` says so when there
/// is none, or only an empty one.
fn value(args: &mut impl Iterator<Item = String>, what: &str) -> Result<String> {
    args.next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| usage(what))
}

fn parse(args: impl IntoIterator<Item = String>) -> Result<Args> {
    let mut args = args.into_iter().peekable();
    let mut parsed = Args {
        root: None,
        space: None,
        relay: None,
        listen: None,
        web: None,
        cmd: Cmd::Open,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => parsed.root = Some(value(&mut args, "--root needs a directory")?),
            "-s" | "--space" => parsed.space = Some(value(&mut args, "--space needs a name")?),
            "--relay" => parsed.relay = Some(value(&mut args, "--relay needs an address")?),
            "--listen" => parsed.listen = Some(value(&mut args, "--listen needs an address")?),
            "--web" => parsed.web = Some(value(&mut args, "--web needs a directory")?.into()),
            "-h" | "--help" => parsed.cmd = Cmd::Help,
            "-V" | "--version" => parsed.cmd = Cmd::Version,
            "relay" => {
                parsed.cmd = match args.next_if(|a| a == "add" || a == "token").as_deref() {
                    Some("add") => Cmd::RelayAdd(value(&mut args, "relay add needs a name")?),
                    Some(_) => Cmd::RelayToken,
                    None => Cmd::Relay,
                }
            }
            "key" => parsed.cmd = Cmd::Key,
            // An empty line is a line too: everything at the top of the space, as in the app.
            "query" => {
                let line = args.next();
                parsed.cmd = Cmd::Query(line.ok_or_else(|| usage(QUERY_NEEDS))?)
            }
            "export" => {
                // A query was once the export's to take, and a script that still gives one is
                // told where it went rather than handed every note.
                if args.next_if(|arg| !arg.starts_with('-')).is_some() {
                    return Err(usage(
                        "export takes no query; fzn query LINE writes what a line finds",
                    ));
                }
                parsed.cmd = Cmd::Export;
            }
            "apply" => {
                let from = args.next_if(|arg| arg == "-" || !arg.starts_with('-'));
                parsed.cmd = Cmd::Apply(from.filter(|from| from != "-").map(PathBuf::from));
            }
            "tick" | "untick" => {
                let ids: Vec<String> =
                    std::iter::from_fn(|| args.next_if(|arg| !arg.starts_with('-'))).collect();
                if ids.is_empty() {
                    return Err(usage(format!(
                        "{arg} needs the id of a note, as query writes it"
                    )));
                }
                parsed.cmd = Cmd::Tick {
                    ids,
                    done: arg == "tick",
                };
            }
            "import-jsonl" => {
                parsed.cmd = Cmd::ImportJsonl(value(&mut args, "import-jsonl needs a file")?.into())
            }
            "import-markdown" => {
                let pages = value(&mut args, "import-markdown needs a directory")?;
                parsed.cmd = Cmd::ImportMarkdown(pages.into());
            }
            other => return Err(usage(format!("unknown argument {other:?}; see fzn --help"))),
        }
    }
    Ok(parsed)
}

fn main() {
    if let Err(why) = run() {
        // A reader that has had enough, as `head` has, is not a failure.
        let closed = why.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == std::io::ErrorKind::BrokenPipe)
        });
        if closed {
            std::process::exit(0);
        }
        // The chain of causes on one line, as `fzn: open x: permission denied`.
        eprintln!("fzn: {why:#}");
        std::process::exit(if why.is::<Usage>() { 2 } else { 1 });
    }
}

fn run() -> Result<()> {
    let Args {
        root: root_flag,
        space: space_flag,
        relay: relay_flag,
        listen,
        web,
        cmd,
    } = parse(std::env::args().skip(1))?;
    match cmd {
        Cmd::Help => {
            println!("{USAGE}");
            return Ok(());
        }
        Cmd::Version => {
            println!("{}", version());
            return Ok(());
        }
        // Said before the passphrase is asked for or the space taken, since neither is any use
        // without a screen to draw on.
        Cmd::Open => ui::need_terminal()?,
        _ => {}
    }
    let config_dir = config::config_dir();
    let cfg = config::load_or_create(&config_dir)?;
    let env_root = std::env::var("FZN_ROOT").ok();
    let root = config::resolve_root(root_flag.as_deref(), env_root.as_deref(), &cfg);
    match &cmd {
        Cmd::RelayAdd(name) => {
            let token = names::add(&config_dir, &root, name)?;
            println!("{}", names::told(&config_dir, &root, Some(name), &token));
            return Ok(());
        }
        Cmd::RelayToken => {
            let token = names::close_root(&config_dir)?;
            println!("{}", names::told(&config_dir, &root, None, &token));
            return Ok(());
        }
        _ => {}
    }
    if let Cmd::Relay = cmd {
        return serve(root, config_dir, listen, web);
    }
    let space = config::resolve_space(space_flag.as_deref(), &cfg)?;
    let dir = root.join(&space);

    match cmd {
        Cmd::Key => {
            key::forget(&space, &config_dir)?;
            key::key_for(&space, &config_dir, &dir)?;
            println!("the key for {space} was replaced");
        }
        Cmd::Query(line) => {
            // Reading takes no lock, and leaves a torn line for whoever appends to its log to cut.
            let store = reading(&dir, &space, &config_dir)?;
            store.query(&line, cfg.weights.clone(), &mut std::io::stdout().lock())?;
        }
        Cmd::Export => {
            let store = reading(&dir, &space, &config_dir)?;
            store.export(&mut std::io::stdout().lock())?;
        }
        Cmd::Apply(from) => {
            if from.is_none() && std::io::stdin().is_terminal() {
                return Err(usage("apply reads notes from a file or a pipe"));
            }
            let input = match &from {
                Some(path) => std::fs::read_to_string(path)
                    .with_context(|| format!("read {}", path.display()))?,
                None => {
                    let mut input = String::new();
                    std::io::stdin()
                        .read_to_string(&mut input)
                        .context("read standard input")?;
                    input
                }
            };
            // Nothing given is nothing to do, and needs no key.
            if input.trim().is_empty() {
                return Ok(());
            }
            let key = kept_key(&space, &config_dir)?;
            let out = &mut std::io::stdout().lock();
            store::apply(&dir, &space, &config_dir, key, &input, out)?;
        }
        Cmd::Tick { ids, done } => {
            let key = kept_key(&space, &config_dir)?;
            let out = &mut std::io::stdout().lock();
            store::tick(&dir, &space, &config_dir, key, &ids, done, out)?;
        }
        Cmd::ImportJsonl(path) => {
            let mut into = importing(&dir, &space, &config_dir)?;
            let n = import::jsonl(&path, &mut into.store)?;
            let linked = import::link_named(&mut into.store)?;
            println!(
                "imported {n} notes into {}, and linked {linked} to the notes they name",
                dir.display()
            );
        }
        Cmd::ImportMarkdown(pages) => {
            let mut into = importing(&dir, &space, &config_dir)?;
            let done = import::markdown(&pages, &mut into.store)?;
            let linked = import::link_named(&mut into.store)?;
            println!(
                "imported {} notes from {} pages into {}, and linked {linked} to the notes they name",
                done.notes,
                done.pages,
                dir.display()
            );
            if !done.skipped.is_empty() {
                println!(
                    "left out, as programs and not notes: {}",
                    done.skipped.join(", ")
                );
            }
        }
        Cmd::Open => {
            // A window is a writer of its own, in a slot it holds for as long as it is open.
            let _in_use = store::Lock::share(&dir, &space)?;
            let slot = store::Slot::take(&dir, &config_dir)?;
            let key = key::key_for(&space, &config_dir, &dir)?;
            // The device is the app. The terminal is one face on it, and the directory is where
            // it keeps its lines.
            let mut app = ui::App::open(
                &dir,
                &space,
                slot,
                key,
                cfg.weights.clone(),
                config::editor(&cfg),
            )?;
            // With a relay named, the window that keeps the space's lines from it holds a socket
            // to it, on a thread of its own, for as long as it is open.
            let env_relay = std::env::var("FZN_RELAY").ok();
            let named = config::resolve_relay(relay_flag.as_deref(), env_relay.as_deref(), &cfg);
            if let Some(relay) = named {
                // The status row names the relay by its host, as it was given.
                let host = relay
                    .trim()
                    .trim_start_matches("ws://")
                    .trim_end_matches('/')
                    .to_owned();
                app.relay(host, move || sync::start(relay.clone(), space.clone()))?;
            }
            app.run()?;
        }
        Cmd::Help | Cmd::Version | Cmd::Relay | Cmd::RelayAdd(_) | Cmd::RelayToken => {
            unreachable!("answered above")
        }
    }
    Ok(())
}

/// Serves every space under the root to whoever connects, until stopped.
fn serve(
    root: PathBuf,
    config_dir: PathBuf,
    listen: Option<String>,
    web: Option<PathBuf>,
) -> Result<()> {
    // A relay is nobody's device: it writes nothing of its own, so it has no id.
    let listen = listen.unwrap_or_else(|| format!("127.0.0.1:{}", wire::PORT));
    let relay = relay::Relay::bind(root.clone(), config_dir.clone(), &listen, web.clone())?;
    println!("fzn relay: {}", version());
    println!(
        "fzn relay: passing on the spaces under {} to the devices that connect to {}",
        root.display(),
        relay.address()?
    );
    println!(
        "fzn relay: a space is entered with its passphrase and made with a token; there is \
         no TLS, so run it on a private network or behind a proxy that has it"
    );
    for line in names::said_at_start(&config_dir) {
        println!("fzn relay: {line}");
    }
    let address = relay.address()?;
    match (relay.build(), &web) {
        (Some(build), Some(web)) => println!(
            "fzn relay: the web app in {} (build {build}) is at http://{address}/",
            web.display()
        ),
        (Some(build), None) => {
            println!("fzn relay: the web app (build {build}) is at http://{address}/")
        }
        (None, _) => println!(
            "fzn relay: no web app: this build has none built in and --web names no directory"
        ),
    }
    relay.serve()
}

/// What this fzn is: the version, then the commit and the day it was built from, which the
/// build takes from git or from `FZN_BUILD`, so that a machine can be asked what it runs.
fn version() -> String {
    said_version(env!("CARGO_PKG_VERSION"), option_env!("FZN_BUILD"))
}

fn said_version(version: &str, build: Option<&str>) -> String {
    match build {
        Some(build) if !build.is_empty() => format!("fzn {version} ({build})"),
        _ => format!("fzn {version}"),
    }
}

/// The key kept for a space, for a command that writes beside the app, which may have the
/// terminal, and so asks for nothing.
fn kept_key(space: &str, config_dir: &Path) -> Result<fzn_engine::Key> {
    key::kept(space, config_dir)?.ok_or_else(|| {
        anyhow!("no key for {space} is kept here: open it in fzn once to give its passphrase")
    })
}

/// A space opened to be read, as `query` and `export` read it.
fn reading(dir: &Path, space: &str, config_dir: &Path) -> Result<store::Store> {
    let key = key::key_for(space, config_dir, dir)?;
    store::Store::open(dir, String::new(), Seal::new(key))
}

/// A space opened for an importer to write, in a slot of its own, beside any window open on it,
/// which lists what it imports within a look.
struct Importing {
    store: store::Store,
    _slot: store::Slot,
    _in_use: store::Lock,
}

fn importing(dir: &Path, space: &str, config_dir: &Path) -> Result<Importing> {
    let in_use = store::Lock::share(dir, space)?;
    let slot = store::Slot::take(dir, config_dir)?;
    let key = key::key_for(space, config_dir, dir)?;
    let store = store::Store::open(dir, slot.id().to_owned(), Seal::new(key))?;
    if store.skipped() > 0 {
        eprintln!(
            "{} lines in {} could not be read, and are left as they are",
            store.skipped(),
            dir.display()
        );
    }
    Ok(Importing {
        store,
        _slot: slot,
        _in_use: in_use,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(args: &[&str]) -> Result<Args> {
        parse(args.iter().map(|&arg| arg.to_owned()))
    }

    fn cmd(args: &[&str]) -> Cmd {
        parsed(args).unwrap().cmd
    }

    /// What a command line that makes no sense says, and that it is a usage mistake.
    fn refused(args: &[&str]) -> String {
        let why = parsed(args).unwrap_err();
        assert!(why.is::<Usage>(), "{why}");
        why.to_string()
    }

    #[test]
    fn the_version_says_the_commit_and_the_day_when_the_build_knows_them() {
        assert_eq!(
            said_version("0.26.0", Some("d115e1815 2026-10-16")),
            "fzn 0.26.0 (d115e1815 2026-10-16)"
        );
        assert_eq!(said_version("0.26.0", Some("")), "fzn 0.26.0");
        assert_eq!(said_version("0.26.0", None), "fzn 0.26.0");
        assert!(version().starts_with(&format!("fzn {}", env!("CARGO_PKG_VERSION"))));
    }

    #[test]
    fn a_line_is_asked_with_query_and_export_takes_none() {
        assert_eq!(
            cmd(&["query", "#house /-mon.."]),
            Cmd::Query("#house /-mon..".into())
        );
        let args = parsed(&["-s", "garden", "query", "[ ]"]).unwrap();
        assert_eq!(
            (args.space.as_deref(), args.cmd),
            (Some("garden"), Cmd::Query("[ ]".into()))
        );
        assert_eq!(cmd(&["export"]), Cmd::Export);
        assert_eq!(cmd(&["export", "--space", "garden"]), Cmd::Export);
        assert_eq!(
            refused(&["export", "#house"]),
            "export takes no query; fzn query LINE writes what a line finds"
        );
        assert_eq!(
            refused(&["query"]),
            "query needs a line, as typed in the app"
        );
        assert_eq!(cmd(&["query", ""]), Cmd::Query(String::new()));
        assert_eq!(
            refused(&["query", "#house", "#garden"]),
            "unknown argument \"#garden\"; see fzn --help"
        );
    }

    #[test]
    fn apply_reads_a_file_or_standard_input() {
        assert_eq!(cmd(&["apply"]), Cmd::Apply(None));
        assert_eq!(cmd(&["apply", "-"]), Cmd::Apply(None));
        assert_eq!(
            cmd(&["apply", "notes.jsonl"]),
            Cmd::Apply(Some("notes.jsonl".into()))
        );
        let args = parsed(&["apply", "--space", "garden"]).unwrap();
        assert_eq!(
            (args.space.as_deref(), args.cmd),
            (Some("garden"), Cmd::Apply(None))
        );
    }

    #[test]
    fn a_command_line_that_makes_no_sense_is_said_in_one_line() {
        assert_eq!(
            refused(&["--nope"]),
            "unknown argument \"--nope\"; see fzn --help"
        );
        assert_eq!(refused(&["--space"]), "--space needs a name");
        assert_eq!(
            refused(&["tick"]),
            "tick needs the id of a note, as query writes it"
        );
        assert!(!refused(&["--root"]).contains('\n'));
        assert_eq!(cmd(&["--version"]), Cmd::Version);
        assert_eq!(cmd(&["-h"]), Cmd::Help);
        assert_eq!(
            cmd(&["untick", "oil", "gate"]),
            Cmd::Tick {
                ids: vec!["oil".into(), "gate".into()],
                done: false
            }
        );
    }
}
