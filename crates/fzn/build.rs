//! Builds the page into fzn, so that a relay is one file to install and the page and the policy
//! that serves it cannot be at different versions, and says which commit fzn is built from.
//!
//! The page is the web directory's listed files (src/web_files.rs, which the relay reads too),
//! never the whole directory, since the bundles' sources and their node_modules live beside it.
//! Each file goes in with a gzip twin made here, so the relay sends it small with no compressor
//! of its own. Without web/pkg there is no page to build in: a plain build says so and carries
//! none, and a release, which sets FZN_PAGE=required, refuses.

#[path = "src/web_files.rs"]
#[allow(dead_code)]
mod web_files;

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;

fn main() {
    let manifest = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("set by cargo"));
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("set by cargo"));
    println!("cargo:rerun-if-changed=src/web_files.rs");
    stamp(&manifest);
    let web = manifest.join("../../web");
    let page = build_in(&web, &out);
    fs::write(out.join("page.rs"), page).expect("write page.rs");
}

/// What `fzn --version` says after the version: `FZN_BUILD` when it is set, else the commit, with
/// `-dirty` if the tree was changed, and today's date.
fn stamp(manifest: &Path) {
    println!("cargo:rerun-if-env-changed=FZN_BUILD");
    if env::var("FZN_BUILD").is_ok_and(|b| !b.is_empty()) {
        return;
    }
    let git = |args: &[&str]| {
        // No optional locks, so that `git status` never takes the index's lock from under a
        // commit being made while an editor builds.
        Command::new("git")
            .arg("--no-optional-locks")
            .arg("-C")
            .arg(manifest)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
    };
    let (Some(commit), Some(changes)) = (
        git(&["rev-parse", "--short=9", "HEAD"]),
        git(&["status", "--porcelain", "--untracked-files=no"]),
    ) else {
        return;
    };
    // A new commit is a new stamp. Edits to files are not watched, so -dirty is as of the last
    // time this ran; a release sets FZN_BUILD and does not depend on it.
    let mut watched = vec![
        "HEAD".to_owned(),
        "index".to_owned(),
        "packed-refs".to_owned(),
    ];
    watched.extend(git(&["symbolic-ref", "-q", "HEAD"]));
    for name in watched {
        if let Some(path) = git(&["rev-parse", "--git-path", &name]) {
            println!("cargo:rerun-if-changed={}", manifest.join(path).display());
        }
    }
    let dirty = if changes.is_empty() { "" } else { "-dirty" };
    println!("cargo:rustc-env=FZN_BUILD={commit}{dirty} {}", today());
}

/// Today in UTC as 2026-10-16, from the days since 1970 by the civil calendar's arithmetic.
fn today() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let days = (secs / 86_400) as i64 + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let mp = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// When anything under a directory was last changed.
fn newest(dir: &Path) -> Option<SystemTime> {
    fs::read_dir(dir)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let meta = entry.metadata().ok()?;
            if meta.is_dir() {
                newest(&entry.path())
            } else {
                meta.modified().ok()
            }
        })
        .max()
}

/// The source of the table the relay serves the page from: each file's path, its bytes, its gzip
/// twin when that is smaller, and its tag, with `index.html` carrying the build.
fn build_in(web: &Path, out: &Path) -> String {
    println!("cargo:rerun-if-env-changed=FZN_PAGE");
    let built = web.join("pkg/fzn_web_bg.wasm").is_file() && web.join("pkg/fzn_web.js").is_file();
    if !built {
        if env::var("FZN_PAGE").is_ok_and(|p| p == "required") {
            panic!("the page is not built: run web/build.sh");
        }
        println!(
            "cargo:warning=the page is not built (web/build.sh), so this fzn relay serves one \
             only with --web"
        );
        // Watched whole until the page is built, so that building it is noticed.
        println!("cargo:rerun-if-changed={}", web.display());
        return "pub const BUILD: &str = \"\";\n\
                pub static FILES: &[File] = &[];\n"
            .to_owned();
    }

    // A page built from an older engine than the one beside it would put a page and a relay of
    // different versions into one binary, so a page older than the sources it is built from is
    // said. release.sh builds the page itself first.
    let sources = [
        web.join("../crates/fzn-engine"),
        web.join("../crates/fzn-web"),
    ];
    for crate_dir in &sources {
        println!("cargo:rerun-if-changed={}", crate_dir.display());
    }
    let wasm = fs::metadata(web.join("pkg/fzn_web_bg.wasm")).and_then(|m| m.modified());
    if let Ok(wasm) = wasm
        && sources
            .iter()
            .filter_map(|dir| newest(dir))
            .any(|t| t > wasm)
    {
        println!(
            "cargo:warning=the page in web/pkg is older than the engine's sources, so this fzn \
             carries a page from an older engine: run web/build.sh"
        );
    }

    // The listed files at the top, and the directories the rest are in, but not web/ itself,
    // whose scan would take in node_modules.
    let paths = web_files::listed(web);
    for path in paths.iter().filter(|p| !p.contains('/')) {
        println!("cargo:rerun-if-changed={}", web.join(path).display());
    }
    for dir in ["pkg", "vendor"] {
        println!("cargo:rerun-if-changed={}", web.join(dir).display());
    }
    let files: Vec<(String, Vec<u8>)> = paths
        .into_iter()
        .map(|path| {
            let bytes = fs::read(web.join(&path)).unwrap_or_else(|e| panic!("read {path}: {e}"));
            (path, bytes)
        })
        .collect();
    let build = web_files::build_of(files.iter().map(|(p, b)| (p.as_str(), b.as_slice())));

    let mut table = format!(
        "pub const BUILD: &str = {build:?};\n\
         pub static FILES: &[File] = &[\n"
    );
    for (path, bytes) in files {
        let flat = path.replace('/', "_");
        let source = if path == "index.html" {
            let filled = out.join("index.html");
            fs::write(&filled, web_files::with_build(&bytes, &build)).expect("write index.html");
            filled
        } else {
            web.join(&path).canonicalize().expect("a listed file")
        };
        let body = fs::read(&source).expect("read what was listed");
        let mut gz = GzEncoder::new(Vec::new(), Compression::best());
        gz.write_all(&body).expect("gzip in memory");
        let gzipped = gz.finish().expect("gzip in memory");
        let twin = if gzipped.len() < body.len() {
            let twin = out.join(format!("{flat}.gz"));
            fs::write(&twin, gzipped).expect("write a gzip twin");
            format!("Some(include_bytes!({:?}))", twin.display().to_string())
        } else {
            "None".to_owned()
        };
        let _ = writeln!(
            table,
            "    ({path:?}, include_bytes!({:?}), {twin}, {:?}),",
            source.display().to_string(),
            web_files::etag_of(&body)
        );
    }
    table.push_str("];\n");
    table
}
