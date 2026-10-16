//! fzn: a blank page that remembers what you wrote.

mod config;
mod import;
mod key;
mod relay;
mod store;
mod sync;
mod system;
mod ui;
mod wire;

use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use fzn_engine::Seal;

const USAGE: &str = "\
fzn [--space NAME]                      open your notes
fzn [--space NAME] export [QUERY]       write every note to standard output, one to a line,
                                        or what QUERY finds at the top of the space
fzn [--space NAME] import-jsonl FILE    import such a file, or a fuzzynote 2022 export
fzn [--space NAME] import-markdown DIR  import a directory of markdown pages (Silverbullet, Obsidian)
fzn [--space NAME] key                  type the space's passphrase again
fzn relay [--listen ADDRESS] [--web DIR]  pass notes between your devices
fzn --help

Notes live in spaces: separate sets of notes, one directory each under the root. Plain fzn opens
the space named in ~/.config/fzn/config.toml, which is main to begin with. --space NAME (or -s)
opens another, and creates it if it is new.

The root is --root DIR, else $FZN_ROOT, else the root in the config file, else ~/.fzn. Only fzn
writes a space's directory, and one fzn at a time: a second window on a space is told so and
stops. A relay, not copying files, is how a space gets from one machine to another.

Every space has a passphrase. The first time fzn opens a space on a machine it asks for it,
twice if the space is new, and keeps the key it makes from it in ~/.config/fzn/keys. Each note is
sealed with that key before it is written, so neither the files nor a relay can be read without
it. A forgotten passphrase is lost notes: there is no reset. If it was mistyped, fzn key asks
again. $FZN_PASSPHRASE answers in place of the question, for scripts.

export writes every note as one plain line of JSON, readable without fzn, to wherever you send
it. Given a query, typed as on the line, it writes the notes that line finds at the top of the
space instead, every one of them and archived ones too, in the order the line lists them: fzn
export '#house /-mon..' is everything tagged #house written since last Monday. import-jsonl is
the way back in: a file put in a space's directory is not read.

To sync, run fzn relay on a machine your devices can reach, and name that machine on each
device: relay = \"ws://that-machine:3960\" in the config file, or --relay, or $FZN_RELAY. fzn then
syncs whichever space it opens for as long as it is open, and works as before when the relay is
out of reach. A relay serves every space under its root and holds their sealed lines, which it
cannot read. Its root is its own: fzn will not open a space the relay holds. It listens on
127.0.0.1:3960 unless --listen says otherwise. It has no password and no TLS yet, so anyone who
can reach it can take a copy of the sealed lines, or add lines that no device can open: let it
listen only where your own devices reach it. With --web DIR it also hands out the web app from
that directory, the web directory of this repository once built, so that a browser can open the
relay's address and be one more device.

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

enum Cmd {
    Open,
    Relay,
    Key,
    /// With a query, what that line finds; without one, every note.
    Export(Option<String>),
    ImportJsonl(PathBuf),
    ImportMarkdown(PathBuf),
}

/// What the command line asked for.
struct Args {
    root: Option<String>,
    space: Option<String>,
    relay: Option<String>,
    listen: Option<String>,
    web: Option<PathBuf>,
    cmd: Cmd,
}

fn parse_args() -> Result<Args> {
    let mut args = std::env::args().skip(1).peekable();
    let mut root = None;
    let mut space = None;
    let mut relay = None;
    let mut listen = None;
    let mut web = None;
    let mut cmd = Cmd::Open;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                root = Some(
                    args.next()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("--root needs a directory"))?,
                )
            }
            "-s" | "--space" => {
                space = Some(
                    args.next()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("--space needs a name"))?,
                )
            }
            "--relay" => {
                relay = Some(
                    args.next()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("--relay needs an address"))?,
                )
            }
            "--listen" => {
                listen = Some(
                    args.next()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("--listen needs an address"))?,
                )
            }
            "--web" => {
                web = Some(
                    args.next()
                        .map(PathBuf::from)
                        .ok_or_else(|| anyhow::anyhow!("--web needs a directory"))?,
                )
            }
            "relay" => cmd = Cmd::Relay,
            "key" => cmd = Cmd::Key,
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "export" => cmd = Cmd::Export(args.next_if(|arg| !arg.starts_with('-'))),
            "import-jsonl" => {
                cmd = Cmd::ImportJsonl(
                    args.next()
                        .map(PathBuf::from)
                        .ok_or_else(|| anyhow::anyhow!("import-jsonl needs a file"))?,
                )
            }
            "import-markdown" => {
                cmd = Cmd::ImportMarkdown(
                    args.next()
                        .map(PathBuf::from)
                        .ok_or_else(|| anyhow::anyhow!("import-markdown needs a directory"))?,
                )
            }
            other => bail!("unknown argument {other:?}\n\n{USAGE}"),
        }
    }
    Ok(Args {
        root,
        space,
        relay,
        listen,
        web,
        cmd,
    })
}

fn main() -> Result<()> {
    let Args {
        root: root_flag,
        space: space_flag,
        relay: relay_flag,
        listen,
        web,
        cmd,
    } = parse_args()?;
    let config_dir = config::config_dir();
    let cfg = config::load_or_create(&config_dir)?;
    let env_root = std::env::var("FZN_ROOT").ok();
    let root = config::resolve_root(root_flag.as_deref(), env_root.as_deref(), &cfg);
    if let Cmd::Relay = cmd {
        // A relay is nobody's device: it writes nothing of its own, so it has no id.
        let listen = listen.unwrap_or_else(|| format!("127.0.0.1:{}", wire::PORT));
        let relay = relay::Relay::bind(root.clone(), &listen, web.clone())?;
        println!(
            "fzn relay: passing on the spaces under {} to whoever connects to {}",
            root.display(),
            relay.address()?
        );
        if let Some(web) = &web {
            println!(
                "fzn relay: the web app in {} is at http://{}/",
                web.display(),
                relay.address()?
            );
        }
        println!(
            "fzn relay: there is no password and no TLS yet, so anyone who can reach that \
             address can take a copy of the sealed lines and add lines of their own"
        );
        return relay.serve();
    }
    let space = config::resolve_space(space_flag.as_deref(), &cfg)?;
    let dir = root.join(&space);

    match cmd {
        Cmd::Key => {
            key::forget(&space, &config_dir)?;
            key::key_for(&space, &config_dir, &dir)?;
            println!("the key for {space} was replaced");
            return Ok(());
        }
        Cmd::Export(query) => {
            // Reading takes no lock, and leaves a torn line for whoever holds one to cut.
            let key = key::key_for(&space, &config_dir, &dir)?;
            let store = store::Store::open(&dir, String::new(), Seal::new(key), false)?;
            let mut out = std::io::stdout().lock();
            match query {
                Some(line) => store.export_found(&line, cfg.weights.clone(), &mut out)?,
                None => store.export(&mut out)?,
            };
            return Ok(());
        }
        Cmd::ImportJsonl(path) => {
            let _lock = hold(&dir, &space)?;
            let mut store = importing(&dir, &space, &config_dir)?;
            let n = import::jsonl(&path, &mut store)?;
            let linked = import::link_named(&mut store)?;
            println!(
                "imported {n} notes into {}, and linked {linked} to the notes they name",
                dir.display()
            );
            return Ok(());
        }
        Cmd::ImportMarkdown(pages) => {
            let _lock = hold(&dir, &space)?;
            let mut store = importing(&dir, &space, &config_dir)?;
            let done = import::markdown(&pages, &mut store)?;
            let linked = import::link_named(&mut store)?;
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
            return Ok(());
        }
        Cmd::Open | Cmd::Relay => {}
    }

    // This process is the only writer of the space's directory for as long as it is open.
    let _lock = hold(&dir, &space)?;
    let writer = store::writer(&dir, &config_dir)?;
    let key = key::key_for(&space, &config_dir, &dir)?;
    // The space you live in needs no label. Any other one says where you are.
    let title = if space == config::resolve_space(None, &cfg)? {
        "fzn".to_owned()
    } else {
        format!("fzn · {space}")
    };
    // The device is the app. The terminal is one face on it, and the directory is where it
    // keeps its lines.
    let opened = ui::App::open(
        &dir,
        &space,
        writer,
        key,
        cfg.weights.clone(),
        config::editor(&cfg),
        title,
    );
    let mut app = match opened {
        Ok(app) => app,
        Err(why) => {
            eprintln!("{why:#}");
            std::process::exit(1);
        }
    };
    // With a relay named, a thread holds a socket to it for as long as the app is open.
    let env_relay = std::env::var("FZN_RELAY").ok();
    if let Some(relay) = config::resolve_relay(relay_flag.as_deref(), env_relay.as_deref(), &cfg) {
        let link = sync::start(relay.clone(), space.clone());
        // The status row names the relay by its host, as it was given.
        let host = relay
            .trim()
            .trim_start_matches("ws://")
            .trim_end_matches('/');
        app.sync(host.to_owned(), link);
    }
    app.run()
}

/// Takes the lock on a space's directory, or stops: another fzn has it open, or a relay holds it.
fn hold(dir: &Path, space: &str) -> Result<store::Lock> {
    match store::Lock::take(dir)? {
        Some(lock) => Ok(lock),
        None => {
            eprintln!("fzn already has {space} open in another window, or a relay holds it");
            std::process::exit(1);
        }
    }
}

/// A space opened for an importer to write, which holds its lock.
fn importing(dir: &Path, space: &str, config_dir: &Path) -> Result<store::Store> {
    let writer = store::writer(dir, config_dir)?;
    let key = key::key_for(space, config_dir, dir)?;
    let store = store::Store::open(dir, writer, Seal::new(key), true)?;
    if store.skipped() > 0 {
        eprintln!(
            "{} lines in {} could not be read, and are left as they are",
            store.skipped(),
            dir.display()
        );
    }
    Ok(store)
}
