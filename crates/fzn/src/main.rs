//! fzn: a blank page that remembers what you wrote.

mod config;
mod import;
mod store;
mod sync;
mod system;
mod ui;

use std::path::PathBuf;

use anyhow::{Result, bail};
use fzn_engine::Session;

const USAGE: &str = "\
fzn [--space NAME]                      open your notes
fzn [--space NAME] export               write every note to standard output, one to a line
fzn [--space NAME] import-jsonl FILE    import a fuzzynote 2022 export (made by the Go cmd/export tool)
fzn [--space NAME] import-markdown DIR  import a directory of markdown pages (Silverbullet, Obsidian)
fzn relay [--listen ADDRESS] [--web DIR]  pass notes between your devices
fzn --help

Notes live in spaces: separate sets of notes, one directory each under the root. Plain fzn opens
the space named in ~/.config/fzn/config.toml, which is main to begin with. --space NAME (or -s)
opens another, and creates it if it is new.

The root is --root DIR, else $FZN_ROOT, else the root in the config file, else ~/.fzn. Each device
appends to its own file in a space, so the root can be synced by anything that copies files.

A line of those files is one version of one note, as JSON, and export writes the same lines. To
bring an export into a space, put the file in the space's directory with a name ending .jsonl.

To sync, run fzn relay on a machine your devices can reach, and name that machine on each
device: relay = \"ws://that-machine:3960\" in the config file, or --relay, or $FZN_RELAY. fzn then
syncs whichever space it opens for as long as it is open, and works as before when the relay is
out of reach. A relay serves every space under its root and keeps them as the same files, so
its root can be opened with fzn too. It listens on 127.0.0.1:3960 unless --listen says
otherwise. It has no password and no encryption yet, so only let it listen where everyone who
can connect may read your notes. With --web DIR it also hands out the web app from that
directory, the web directory of this repository once built, so that a browser can open the
relay's address and be one more device.

If fzn says a log has forked, two machines have been writing under one device id. On one of
them, quit fzn, rename that log in the space's directory (ID.jsonl to ID-old.jsonl, say) and
delete the file named device in ~/.config/fzn. Nothing is lost: both logs then sync.

A date in a note is plain text, written 2026-10-16. /today, /tomorrow, /yesterday, a day of the
week such as /fri, and /+3 or /-2 for days from today are quicker ways of typing one: saving the
note writes the date in their place.";

enum Cmd {
    Open,
    Relay,
    Export,
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
    let mut args = std::env::args().skip(1);
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
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            "export" => cmd = Cmd::Export,
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
        let listen = listen.unwrap_or_else(|| format!("127.0.0.1:{}", sync::PORT));
        let relay = sync::Relay::bind(root.clone(), &listen, web.clone())?;
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
            "fzn relay: there is no password and no encryption yet, so anyone who can reach \
             that address can read and write these notes"
        );
        return relay.serve();
    }
    let device = config::device_id(&config_dir)?;
    let space = config::resolve_space(space_flag.as_deref(), &cfg)?;
    let dir = root.join(&space);
    let mut store = store::Store::open(&dir, &device)?;

    match cmd {
        Cmd::ImportJsonl(path) => {
            let n = import::jsonl(&path, &mut store)?;
            let linked = import::link_named(&mut store)?;
            println!(
                "imported {n} notes into {}, and linked {linked} to the notes they name",
                dir.display()
            );
            return Ok(());
        }
        Cmd::ImportMarkdown(pages) => {
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
        Cmd::Export => {
            store.export(&mut std::io::stdout().lock())?;
            return Ok(());
        }
        Cmd::Open | Cmd::Relay => {}
    }

    let status = if store.notes().is_empty() {
        "no notes yet: type and press Enter, or see fzn --help to import".to_owned()
    } else if store.skipped() > 0 {
        format!(
            "{} unreadable lines skipped in {}",
            store.skipped(),
            dir.display()
        )
    } else {
        dir.display().to_string()
    };
    // The space you live in needs no label. Any other one says where you are.
    let title = if space == config::resolve_space(None, &cfg)? {
        "fzn".to_owned()
    } else {
        format!("fzn · {space}")
    };
    // The session is the app. The terminal is one face on it, and the logs are where this
    // device keeps what the session writes.
    let (notes, logs) = store.into_parts();
    let mut session = Session::new(Box::new(system::System), device, cfg.weights.clone(), notes);
    session.say(status);
    // With a relay named, a thread keeps this space's logs in step with it for as long as the
    // app is open. What arrives lands in the logs, and the app finds it there.
    let env_relay = std::env::var("FZN_RELAY").ok();
    let relay = config::resolve_relay(relay_flag.as_deref(), env_relay.as_deref(), &cfg);
    let sync = relay.map(|relay| {
        let news = sync::start(dir.clone(), relay.clone(), space.clone());
        // The status row names the relay by its host, as it was given.
        let host = relay
            .trim()
            .trim_start_matches("ws://")
            .trim_end_matches('/');
        (host.to_owned(), news)
    });
    ui::App::new(session, logs, config::editor(&cfg), title, sync).run()
}
