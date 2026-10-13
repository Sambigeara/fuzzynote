//! fzn: a blank page that remembers what you wrote.

mod config;
mod import;
mod store;
mod ui;

use std::path::PathBuf;

use anyhow::{Result, bail};
use fzn_engine::Engine;

const USAGE: &str = "\
fzn [--space NAME]                      open your notes
fzn [--space NAME] export               write every note to standard output, one to a line
fzn [--space NAME] import-jsonl FILE    import a fuzzynote 2022 export (made by the Go cmd/export tool)
fzn [--space NAME] import-markdown DIR  import a directory of markdown pages (Silverbullet, Obsidian)
fzn --help

Notes live in spaces: separate sets of notes, one directory each under the root. Plain fzn opens
the space named in ~/.config/fzn/config.toml, which is main to begin with. --space NAME (or -s)
opens another, and creates it if it is new.

The root is --root DIR, else $FZN_ROOT, else the root in the config file, else ~/.fzn. Each device
appends to its own file in a space, so the root can be synced by anything that copies files.

A line of those files is one version of one note, as JSON, and export writes the same lines. To
bring an export into a space, put the file in the space's directory with a name ending .jsonl.

A date in a note is plain text, written 2026-10-16. /today, /tomorrow, /yesterday, a day of the
week such as /fri, and /+3 or /-2 for days from today are quicker ways of typing one: saving the
note writes the date in their place.";

enum Cmd {
    Open,
    Export,
    ImportJsonl(PathBuf),
    ImportMarkdown(PathBuf),
}

fn parse_args() -> Result<(Option<String>, Option<String>, Cmd)> {
    let mut args = std::env::args().skip(1);
    let mut root = None;
    let mut space = None;
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
    Ok((root, space, cmd))
}

fn main() -> Result<()> {
    let (root_flag, space_flag, cmd) = parse_args()?;
    let config_dir = config::config_dir();
    let cfg = config::load_or_create(&config_dir)?;
    let device = config::device_id(&config_dir)?;
    let env_root = std::env::var("FZN_ROOT").ok();
    let root = config::resolve_root(root_flag.as_deref(), env_root.as_deref(), &cfg);
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
        Cmd::Open => {}
    }

    let mut engine = Engine::with_weights(cfg.weights.clone());
    for record in store.records() {
        engine.upsert(ui::note_from(record));
    }
    let status = if store.is_empty() {
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
    ui::App::new(engine, store, config::editor(&cfg), title, status).run()
}
