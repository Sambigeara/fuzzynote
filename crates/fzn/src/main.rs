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
mod smaller;
mod store;
mod sync;
mod system;
mod ui;
mod web_files;
mod wire;

use std::io::{IsTerminal as _, Read as _, Write as _};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow, bail};
use fzn_engine::{Seal, image};

const USAGE: &str = "\
fzn [--space NAME]                      open your notes
fzn [--space NAME] query LINE           write what the line finds, one note to a line
fzn [--space NAME] export               write every note, one to a line, oldest first
fzn [--space NAME] apply [FILE]         write the notes given, from FILE or standard input
fzn [--space NAME] tick ID [ID ...]     tick these tasks, by the ids query writes
fzn [--space NAME] untick ID [ID ...]   bring these ticked tasks back
fzn [--space NAME] write TEXT [--in LINE]...  write a note, in the place the lines go into
fzn [--space NAME] image FILE [FILE ...]  keep images in the space, and write a reference to each
fzn [--space NAME] image fzn:ID         write an image the space holds to standard output
fzn [--space NAME] import-jsonl FILE    import such a file, or a fuzzynote 2022 export
fzn [--space NAME] import-markdown DIR  import a directory of markdown pages (Silverbullet, Obsidian)
fzn [--space NAME] key                  type the space's passphrase again
fzn relay [--listen ADDRESS] [--web DIR] [--images SIZE]  pass notes between your devices
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
tick, untick, write and the importers work beside them. image keeps images beside them and
writes no note; an image goes to the relay once a note refers to it and fzn is open here. A
relay, not copying files, is how a space gets from one machine to another. With a relay named
and no window open on the space, query, export, apply, tick, untick and write sync with it once
before they read or write, and send what they wrote before they end; a relay out of reach is
said on one line, not synced with HOST, and they go on with this machine's copy.

Every space has a passphrase. The first time fzn opens a space on a machine it asks for it,
twice if the space is new, and keeps the key it makes from it in ~/.config/fzn/keys. Each note is
sealed with that key before it is written, so neither the files nor a relay can be read without
it. A forgotten passphrase is lost notes: there is no reset. If it was mistyped, fzn key asks
again. $FZN_PASSPHRASE answers in place of the question, for scripts.

query writes the notes a line finds, typed as on the line, one plain line of JSON each, readable
without fzn: every one of them, archived ones too, in the order the app lists them. With nothing
to rank by that is the space's order; on a line with a span it is oldest first by when each was
written, or with [x] by when each was done, and newest first with a - before the span; otherwise
it is best first. fzn query '#house /-mon..' is everything tagged #house written since last
Monday, fzn query '[ ]' the open tasks, fzn query '[x] /-mon.. #work' the tasks in #work done
since last Monday, whenever they were written, and fzn query '-..' every note newest first.
export writes every live note the same way, oldest first. Neither writes anything, and jq orders
and picks from either.

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

write writes TEXT as a new note, as Enter on the line writes it: date words and /task are
written out and [[brackets]] become links, and the note goes at the top of the place the --in
lines go into, with that place's labels and linked to its note. From the top of the space,
each --in LINE in turn is typed on the line and gone into as Tab goes: a line of labels into
each label, and any other line into the one note it names, so --in '#house' --in '[ ]' is the
place --in '#house [ ]' is. fzn write 'Bleed the radiators /task' --in '#house' writes [ ] Bleed
the radiators #house, and with --in 'Boiler overhaul' instead the note is linked to the note
named Boiler overhaul. A line that goes nowhere (it names no note, several, or the place already
gone into) is said and nothing is written, exit 1; so is TEXT that is only labels, which names a
place and not a note. A label is a place whether or not a note carries it yet, as in the app,
so --in '#hoose' writes the note in #hoose. The note written is said back as query writes it, so
a mistyped label shows there. Run twice, write writes two notes. It takes the key kept for the
space as apply does, and writes no body: a body is written with apply.

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
no TLS, so let it listen only on a private network, or behind a proxy that has it, which the
devices then name as wss://host, or wss://NAME:TOKEN@host. It also hands out the web app, which
is built into fzn, so that a browser can open the relay's address and be one more device. With
--web DIR it hands out the page in that directory instead, the web directory of this
repository, which is how the page is worked on.

If fzn says a log has forked, two machines have been writing under one writer id, usually
because a space's directory was copied from one to the other. Nothing is lost, and nothing is
yours to do: each machine starts a new log of its own, which retires the shared one, and every
note both wrote is kept.

A date in a note is plain text, written 2026-10-16. /today, /tomorrow, /yesterday, a day of the
week such as /fri, and /+3 or /-2 for days from today are quicker ways of typing one: saving the
note writes the date in their place. /-mon is the last Monday before today.

A note whose head starts [ ] is a task. Archiving it ticks it, [x], and bringing it back unticks
it; a new note written starting [x] is done as it is written. Every line says when its note was
archived, which for a task is when it was ticked, as done, and null while it is not.

On the line, words find and everything else holds: \"a phrase\" must be in a note, #tag, @person
and a day must be on it, [ ] holds the open tasks and [x] the done ones, archived or not, and a
span such as /-mon 18:00.., 2026-10-01..2026-10-05 or ..2026-10-05 holds the notes written in it,
or with [x] the tasks done in it; .. alone is all time. ~ at the start of the line finds heads by
their letters in order: ~bsv finds Boiler service.";

/// What `fzn query` with nothing after it is told.
const QUERY_NEEDS: &str = "query needs a line, as typed in the app";

/// What `fzn write` with no text, or only spaces, is told.
const WRITE_NEEDS: &str = "write needs the text of a note, as typed on the line";

/// What `--in` with no line after it, or only spaces, is told.
const IN_NEEDS: &str = "--in needs a line, as typed in the app";

/// What `--images` with no size after it, or one that is not a size, is told.
const IMAGES_TAKES: &str = "--images takes a size, as 512M or 2G";

/// The most bytes images may take on a relay unless `--images` says otherwise: 512 MiB.
const IMAGES_DEFAULT: u64 = 512 << 20;

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
    /// A note written as the line writes it, in the place these lines go into.
    Write {
        text: String,
        places: Vec<String>,
    },
    ImportJsonl(PathBuf),
    ImportMarkdown(PathBuf),
    /// Files to keep as images, `-` for standard input, or one image's address to write out.
    Image(Vec<String>),
}

/// What the command line said, besides the command.
#[derive(Debug, PartialEq)]
struct Args {
    root: Option<String>,
    space: Option<String>,
    relay: Option<String>,
    listen: Option<String>,
    web: Option<PathBuf>,
    /// The most bytes images may take on a relay.
    images: Option<u64>,
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
        images: None,
        cmd: Cmd::Open,
    };
    // After `write`, its text and each `--in` with its line, in any order among the flags. Every
    // word that is not a flag is the text, which may start with one dash, as a line may.
    let (mut writing, mut text, mut places) = (false, None, Vec::new());
    while let Some(arg) = args.next() {
        let flag = arg.starts_with("--") || matches!(arg.as_str(), "-s" | "-h" | "-V");
        if writing && !flag {
            if text.is_some() {
                return Err(usage(format!(
                    "write takes one text, and {arg:?} is a second; quote the note as one"
                )));
            }
            text = Some(arg);
            continue;
        }
        match arg.as_str() {
            "--root" => parsed.root = Some(value(&mut args, "--root needs a directory")?),
            "-s" | "--space" => parsed.space = Some(value(&mut args, "--space needs a name")?),
            "--relay" => parsed.relay = Some(value(&mut args, "--relay needs an address")?),
            "--listen" => parsed.listen = Some(value(&mut args, "--listen needs an address")?),
            "--web" => parsed.web = Some(value(&mut args, "--web needs a directory")?.into()),
            "--images" => {
                let size = value(&mut args, IMAGES_TAKES)?;
                parsed.images = Some(size_of(&size).ok_or_else(|| usage(IMAGES_TAKES))?);
            }
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
            "write" => writing = true,
            "image" => {
                let given =
                    std::iter::from_fn(|| args.next_if(|arg| arg == "-" || !arg.starts_with('-')));
                parsed.cmd = Cmd::Image(given.collect());
            }
            "--in" if writing => {
                let line = value(&mut args, IN_NEEDS)?;
                if line.trim().is_empty() {
                    return Err(usage(IN_NEEDS));
                }
                if line.contains(['\n', '\r']) {
                    return Err(usage("--in takes one line, as typed in the app"));
                }
                places.push(line);
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
    // Asked for the manual, or the version, the rest is not read for sense.
    if writing && !matches!(parsed.cmd, Cmd::Help | Cmd::Version) {
        let text = text
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| usage(WRITE_NEEDS))?;
        if text.contains(['\n', '\r']) {
            return Err(usage(
                "write takes one line, a note's head; a body is written with apply",
            ));
        }
        parsed.cmd = Cmd::Write { text, places };
    }
    if let Cmd::Image(given) = &parsed.cmd {
        let out = given.iter().any(|arg| image::id_in(arg).is_some());
        if given.is_empty() {
            return Err(usage(
                "image needs a file to keep, or an image's address to write out",
            ));
        }
        if out && given.len() > 1 {
            return Err(usage(
                "image takes files to keep, or one image's address to write out, not both",
            ));
        }
        if given.iter().filter(|arg| *arg == "-").count() > 1 {
            return Err(usage(
                "image reads one image from standard input, so - is given once",
            ));
        }
    }
    Ok(parsed)
}

/// A size as `--images` takes one: decimal digits, then `K`, `M` or `G` for so many KiB, MiB or
/// GiB, or nothing for bytes.
fn size_of(text: &str) -> Option<u64> {
    let (digits, shift) = match text.as_bytes().last()? {
        b'K' => (&text[..text.len() - 1], 10),
        b'M' => (&text[..text.len() - 1], 20),
        b'G' => (&text[..text.len() - 1], 30),
        _ => (text, 0),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok()?.checked_mul(1 << shift)
}

/// How the room for images is shared among the names a relay serves.
fn said_share(names: u64, share: u64, most: u64) -> String {
    match names {
        1 => "one name is served here, and may take it all".to_owned(),
        n => format!(
            "{n} names are served here, and each may take {}",
            said_size(share, most)
        ),
    }
}

/// A size as the relay says one: whole MiB, rounded down, or KiB where the most is under 1 MiB.
fn said_size(bytes: u64, most: u64) -> String {
    if most < 1 << 20 {
        format!("{} KiB", bytes >> 10)
    } else {
        format!("{} MiB", bytes >> 20)
    }
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
        images,
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
        return serve(
            root,
            config_dir,
            listen,
            web,
            images.unwrap_or(IMAGES_DEFAULT),
        );
    }
    let space = config::resolve_space(space_flag.as_deref(), &cfg)?;
    let dir = root.join(&space);
    // A relay's address is read before anything is asked for, so that one that is not an
    // address is said first. The app syncs with it, and so do the commands that read and write
    // notes; the importers and key leave it be.
    let syncs = matches!(
        cmd,
        Cmd::Open
            | Cmd::Query(_)
            | Cmd::Export
            | Cmd::Apply(_)
            | Cmd::Tick { .. }
            | Cmd::Write { .. }
    );
    let env_relay = std::env::var("FZN_RELAY").ok();
    let named =
        config::resolve_relay(relay_flag.as_deref(), env_relay.as_deref(), &cfg).filter(|_| syncs);
    let dial = named.as_deref().map(sync::dial_of).transpose()?;

    match cmd {
        Cmd::Key => {
            key::forget(&space, &config_dir)?;
            key::key_for(&space, &config_dir, &dir)?;
            println!("the key for {space} was replaced");
        }
        Cmd::Query(line) => {
            let store = reading(&dir, &space, &config_dir, dial.as_ref())?;
            store.query(&line, cfg.weights.clone(), &mut std::io::stdout().lock())?;
        }
        Cmd::Export => {
            let store = reading(&dir, &space, &config_dir, dial.as_ref())?;
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
            // Nothing given is nothing to do, and needs no key, and what is not a note is said
            // before the relay is asked for anything.
            let given = store::given(&input)?;
            if given.is_empty() {
                return Ok(());
            }
            let key = kept_key(&space, &config_dir)?;
            let (beside, not_synced) =
                store::Beside::open(&dir, &space, &config_dir, key, dial.as_ref())?;
            say(not_synced);
            say(beside.apply(given, &mut std::io::stdout().lock())?);
        }
        Cmd::Tick { ids, done } => {
            let key = kept_key(&space, &config_dir)?;
            let (beside, not_synced) =
                store::Beside::open(&dir, &space, &config_dir, key, dial.as_ref())?;
            say(not_synced);
            say(beside.tick(&ids, done, &mut std::io::stdout().lock())?);
        }
        Cmd::Write { text, places } => {
            let key = kept_key(&space, &config_dir)?;
            let (beside, not_synced) =
                store::Beside::open(&dir, &space, &config_dir, key, dial.as_ref())?;
            say(not_synced);
            say(beside.write(&places, &text, &mut std::io::stdout().lock())?);
        }
        Cmd::Image(given) => match given.iter().find_map(|arg| image::id_in(arg)) {
            Some(id) => image_out(&dir, &space, &config_dir, id)?,
            None => image_in(&dir, &space, &config_dir, &given)?,
        },
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
            // to it, on a thread of its own, for as long as it is open. The status row names it
            // by its host alone, never by the token its address may carry.
            if let (Some(relay), Some(dial)) = (named, dial) {
                app.relay(dial.shown, move || {
                    sync::start(relay.clone(), space.clone())
                })?;
            }
            app.run()?;
        }
        Cmd::Help | Cmd::Version | Cmd::Relay | Cmd::RelayAdd(_) | Cmd::RelayToken => {
            unreachable!("answered above")
        }
    }
    Ok(())
}

/// Keeps images in a space, each made smaller by the rule every face keeps to, and writes the
/// reference to each, one a line in the order given, to be read into a body. All or none: every
/// image is read, made smaller and sealed before any is kept, so one that cannot be is said and
/// nothing is kept. It writes no log, so it takes the space's shared lock and no slot, and it
/// syncs nothing: a window here offers an image to the relay once a note refers to it.
fn image_in(dir: &Path, space: &str, config_dir: &Path, given: &[String]) -> Result<()> {
    if given.iter().any(|arg| arg == "-") && std::io::stdin().is_terminal() {
        return Err(usage("image - reads an image from a pipe"));
    }
    let key = kept_key(space, config_dir)?;
    let _in_use = store::Lock::share(dir, space)?;
    let seal = Seal::new(key.clone());
    // Opened only to check that the key opens what is there, so that no image is sealed under
    // a key the space's notes are not.
    store::Store::open(dir, String::new(), Seal::new(key))?;
    let mut sealed = Vec::new();
    for arg in given {
        // Read no further than the most a file may be, and a byte past it to know it is more.
        let mut bytes = Vec::new();
        let name = if arg == "-" {
            std::io::stdin()
                .take(smaller::FILE_MOST + 1)
                .read_to_end(&mut bytes)
                .context("read standard input")?;
            "standard input"
        } else {
            std::fs::File::open(arg)
                .and_then(|file| file.take(smaller::FILE_MOST + 1).read_to_end(&mut bytes))
                .with_context(|| format!("read {arg}"))?;
            arg.as_str()
        };
        if bytes.len() as u64 > smaller::FILE_MOST {
            bail!("{name} is larger than fzn reads, which is 64 MiB");
        }
        let plain = smaller::smaller(&bytes).map_err(|why| match why {
            smaller::Smaller::NotAnImage => {
                anyhow!("{name} is not an image fzn can read: a JPEG, PNG, WebP or GIF")
            }
            smaller::Smaller::TooLarge => anyhow!("{name} is more than 2 MiB even made smaller"),
            smaller::Smaller::TooBig => anyhow!(
                "{name} is larger than fzn reads: 100 million pixels, 32768 on a side, and \
                 256 MiB once read"
            ),
        })?;
        sealed.push(image::seal(&seal, &plain, &system::System).map_err(|why| anyhow!(why))?);
    }
    let logs = store::Logs::open(dir)?;
    for (id, bytes) in &sealed {
        logs.keep_image(id, bytes)?;
    }
    let mut out = std::io::stdout().lock();
    for (id, _) in &sealed {
        writeln!(out, "{}", image::reference(id))?;
    }
    Ok(())
}

/// Writes the image a space holds under an id to standard output, never to a terminal, opened
/// with the space's key and checked against its id.
fn image_out(dir: &Path, space: &str, config_dir: &Path, id: &str) -> Result<()> {
    if std::io::stdout().is_terminal() {
        return Err(usage(format!(
            "image writes an image to a file or a pipe, not to the terminal: \
             fzn image fzn:{id} > boiler.jpg"
        )));
    }
    let key = kept_key(space, config_dir)?;
    let sealed = store::Logs::open(dir)?
        .image(id)?
        .ok_or_else(|| anyhow!("fzn:{id} is not on this machine"))?;
    let plain = image::open(&Seal::new(key), id, &sealed)
        .ok_or_else(|| anyhow!("fzn:{id} here does not open with this space's key"))?;
    let mut out = std::io::stdout().lock();
    out.write_all(&plain)
        .and_then(|()| out.flush())
        .context("write standard output")?;
    Ok(())
}

/// Serves every space under the root to whoever connects, until stopped.
fn serve(
    root: PathBuf,
    config_dir: PathBuf,
    listen: Option<String>,
    web: Option<PathBuf>,
    images: u64,
) -> Result<()> {
    // A relay is nobody's device: it writes nothing of its own, so it has no id.
    let listen = listen.unwrap_or_else(|| format!("127.0.0.1:{}", wire::PORT));
    let relay = relay::Relay::bind(
        root.clone(),
        config_dir.clone(),
        &listen,
        web.clone(),
        images,
    )?;
    println!("fzn relay: {}", version());
    println!(
        "fzn relay: passing on the spaces under {} to the devices that connect to {}",
        root.display(),
        relay.address()?
    );
    let (most, used, names, share) = relay.images();
    println!(
        "fzn relay: images take at most {} here, and {} are taken; {}",
        said_size(most, most),
        said_size(used, most),
        said_share(names, share, most)
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

/// A space opened to be read, as `query` and `export` read it, once it is synced with the relay
/// named.
fn reading(
    dir: &Path,
    space: &str,
    config_dir: &Path,
    relay: Option<&sync::Dial>,
) -> Result<store::Store> {
    let key = key::key_for(space, config_dir, dir)?;
    let (store, not_synced) = synced(dir, space, config_dir, key, relay)?;
    say(not_synced);
    Ok(store)
}

/// Syncs a space with the relay named, if one is, and then reads it, which takes no lock and
/// leaves a torn line for whoever appends to its log to cut. What goes wrong in the directory
/// while syncing, a relay here holding it or no slot free, is one more reason it is not synced,
/// and the read goes on without, so that what is written out is what it would have been.
fn synced(
    dir: &Path,
    space: &str,
    config_dir: &Path,
    key: fzn_engine::Key,
    relay: Option<&sync::Dial>,
) -> Result<(store::Store, Option<String>)> {
    let not_synced = relay.and_then(|relay| {
        store::sync(dir, space, config_dir, key.clone(), relay)
            .unwrap_or_else(|why| Some(format!("not synced with {}: {why:#}", relay.shown)))
    });
    let store = store::Store::open(dir, String::new(), Seal::new(key))?;
    Ok((store, not_synced))
}

/// Says why a command is not synced with the relay, when it is not, on a line of its own; the
/// command goes on with this machine's copy.
fn say(not_synced: Option<String>) {
    if let Some(why) = not_synced {
        eprintln!("fzn: {why}");
    }
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
            cmd(&["query", "-.."]),
            Cmd::Query("-..".into()),
            "a line may start with a dash"
        );
        assert_eq!(
            refused(&["query", "#house", "#garden"]),
            "unknown argument \"#garden\"; see fzn --help"
        );
    }

    #[test]
    fn write_takes_one_line_of_text_and_the_lines_that_make_its_place() {
        let write = |text: &str, places: &[&str]| Cmd::Write {
            text: text.into(),
            places: places.iter().map(|&place| place.into()).collect(),
        };
        assert_eq!(
            cmd(&["write", "Bleed the radiators"]),
            write("Bleed the radiators", &[])
        );
        assert_eq!(
            cmd(&[
                "write",
                "Bleed the radiators /task",
                "--in",
                "#house",
                "--in",
                "Boiler"
            ]),
            write("Bleed the radiators /task", &["#house", "Boiler"])
        );
        assert_eq!(
            cmd(&["write", "--in", "#house [ ]", "Order oil"]),
            write("Order oil", &["#house [ ]"])
        );
        assert_eq!(
            cmd(&["write", "-2 tonight, cover the roses"]),
            write("-2 tonight, cover the roses", &[]),
            "a text may start with a dash"
        );
        // The space and the places may come anywhere after write, before the text or after it.
        for args in [
            &["write", "Order oil", "--space", "garden", "--in", "#house"][..],
            &["write", "--in", "#house", "-s", "garden", "Order oil"],
            &["write", "--space", "garden", "--in", "#house", "Order oil"],
        ] {
            let args = parsed(args).unwrap();
            assert_eq!(
                (args.space.as_deref(), args.cmd),
                (Some("garden"), write("Order oil", &["#house"]))
            );
        }
        assert_eq!(cmd(&["write", "--help"]), Cmd::Help);
        assert_eq!(cmd(&["write", "Order oil", "-h"]), Cmd::Help);
        let needs = "write needs the text of a note, as typed on the line";
        assert_eq!(refused(&["write"]), needs);
        assert_eq!(refused(&["write", "  "]), needs);
        assert_eq!(refused(&["write", "--in", "#house"]), needs);
        let in_needs = "--in needs a line, as typed in the app";
        assert_eq!(refused(&["write", "Boiler", "--in"]), in_needs);
        assert_eq!(refused(&["write", "Boiler", "--in", ""]), in_needs);
        assert_eq!(refused(&["write", "Boiler", "--in", "  "]), in_needs);
        assert_eq!(
            refused(&["write", "Boiler", "--in", "#house\n[ ]"]),
            "--in takes one line, as typed in the app"
        );
        assert_eq!(
            refused(&["--in", "#house", "write", "Boiler"]),
            "unknown argument \"--in\"; see fzn --help",
            "--in is write's"
        );
        assert_eq!(
            refused(&["write", "Order", "oil"]),
            "write takes one text, and \"oil\" is a second; quote the note as one"
        );
        assert_eq!(
            refused(&["write", "Boiler\nService it in May"]),
            "write takes one line, a note's head; a body is written with apply"
        );
    }

    #[test]
    fn image_takes_files_or_one_images_address_and_not_both() {
        let id = "4f998e75f22477b2519998d0731eeec8";
        let address = format!("fzn:{id}");
        let given = |args: &[&str]| Cmd::Image(args.iter().map(|&arg| arg.into()).collect());
        assert_eq!(
            cmd(&["image", "boiler.jpg", "garden.png"]),
            given(&["boiler.jpg", "garden.png"])
        );
        assert_eq!(cmd(&["image", "-"]), given(&["-"]));
        assert_eq!(cmd(&["image", &address]), given(&[&address]));
        let reference = format!("![](fzn:{id})");
        assert_eq!(cmd(&["image", &reference]), given(&[&reference]));
        let args = parsed(&["image", "boiler.jpg", "--space", "garden"]).unwrap();
        assert_eq!(
            (args.space.as_deref(), args.cmd),
            (Some("garden"), given(&["boiler.jpg"]))
        );
        assert_eq!(cmd(&["image", "--help"]), Cmd::Help);

        let both = "image takes files to keep, or one image's address to write out, not both";
        assert_eq!(refused(&["image", "boiler.jpg", &address]), both);
        assert_eq!(refused(&["image", &address, &reference]), both);
        assert_eq!(
            refused(&["image", "-", "boiler.jpg", "-"]),
            "image reads one image from standard input, so - is given once"
        );
        assert_eq!(
            refused(&["image"]),
            "image needs a file to keep, or an image's address to write out"
        );
    }

    #[test]
    fn relay_images_takes_a_size_as_512m_or_2g() {
        let images = |args: &[&str]| parsed(args).unwrap().images;
        assert_eq!(images(&["relay"]), None);
        assert_eq!(images(&["relay", "--images", "512M"]), Some(512 << 20));
        assert_eq!(images(&["relay", "--images", "2G"]), Some(2 << 30));
        assert_eq!(images(&["--images", "64K", "relay"]), Some(64 << 10));
        assert_eq!(images(&["relay", "--images", "1000"]), Some(1000));
        assert_eq!(images(&["relay", "--images", "0"]), Some(0));
        let takes = "--images takes a size, as 512M or 2G";
        for bad in ["", "M", "2T", "-1", "1.5G", "2 G", "99999999999999999999G"] {
            assert_eq!(refused(&["relay", "--images", bad]), takes, "{bad:?}");
        }
        assert_eq!(refused(&["relay", "--images"]), takes);
        assert_eq!(said_size(512 << 20, 512 << 20), "512 MiB");
        assert_eq!(said_size((3 << 20) + 5, 512 << 20), "3 MiB");
        assert_eq!(said_size(300 << 10, 512 << 10), "300 KiB");
        assert_eq!(
            said_share(2, 256 << 20, 512 << 20),
            "2 names are served here, and each may take 256 MiB"
        );
        assert_eq!(IMAGES_DEFAULT, 536_870_912);
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

    #[test]
    fn query_and_export_with_no_relay_read_while_the_directory_is_held_alone() {
        let dir = store::tests::temp_dir();
        let (space, config) = (dir.join("main"), dir.join("config"));
        let key = fzn_engine::Key::from_bytes([7; 32]);
        let mut writer =
            store::Store::open(&space, "3f9a01c2".into(), Seal::new(key.clone())).unwrap();
        writer
            .put(fzn_engine::Record::new("oil", "Order oil #house"))
            .unwrap();
        let held = std::fs::File::create(space.join(".lock")).unwrap();
        held.try_lock().unwrap();

        let (store, not_synced) = synced(&space, "main", &config, key.clone(), None).unwrap();
        assert_eq!(not_synced, None);
        let mut out = Vec::new();
        assert_eq!(store.export(&mut out).unwrap(), 1);
        let (store, _) = synced(&space, "main", &config, key.clone(), None).unwrap();
        let n = store
            .query("#house", Default::default(), &mut Vec::new())
            .unwrap();
        assert_eq!(n, 1);

        // With a relay named, the directory's trouble is one more reason it is not synced, and
        // the read goes on all the same.
        let relay = sync::dial_of("ws://nas.local").unwrap();
        let (store, not_synced) = synced(&space, "main", &config, key, Some(&relay)).unwrap();
        let why = not_synced.unwrap();
        assert!(why.starts_with("not synced with nas.local: "), "{why}");
        assert!(why.contains("holds main"), "{why}");
        assert_eq!(store.export(&mut Vec::new()).unwrap(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
