//! fzn: a blank page that remembers what you wrote.

mod config;
mod import;
mod store;
mod ui;

use std::path::PathBuf;

use anyhow::{Result, bail};
use fzn_engine::Engine;

const USAGE: &str = "\
fzn [--root DIR]                      open your notes
fzn [--root DIR] import-jsonl FILE    import a fuzzynote 2022 export (made by the Go cmd/export tool)
fzn [--root DIR] import-markdown DIR  import a directory of markdown pages
fzn --help

Notes live in DIR, else $FZN_ROOT, else the root in ~/.config/fzn/config.toml, else ~/.fzn.
Each device appends to its own file there, so the directory can be synced by anything that copies files.";

enum Cmd {
    Open,
    ImportJsonl(PathBuf),
    ImportMarkdown(PathBuf),
}

fn parse_args() -> Result<(Option<String>, Cmd)> {
    let mut args = std::env::args().skip(1);
    let mut root = None;
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
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
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
    Ok((root, cmd))
}

fn main() -> Result<()> {
    let (root_flag, cmd) = parse_args()?;
    let config_dir = config::config_dir();
    let cfg = config::load_or_create(&config_dir)?;
    let device = config::device_id(&config_dir)?;
    let env_root = std::env::var("FZN_ROOT").ok();
    let root = config::resolve_root(root_flag.as_deref(), env_root.as_deref(), &cfg);
    let mut store = store::Store::open(&root, &device)?;

    match cmd {
        Cmd::ImportJsonl(path) => {
            let n = import::jsonl(&path, &mut store)?;
            println!("imported {n} notes into {}", root.display());
            return Ok(());
        }
        Cmd::ImportMarkdown(dir) => {
            let n = import::markdown(&dir, &mut store)?;
            println!("imported {n} pages into {}", root.display());
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
            root.display()
        )
    } else {
        root.display().to_string()
    };
    ui::App::new(engine, store, config::editor(&cfg), status).run()
}
