//! One-off importers: the 2022 fuzzynote export, and a directory of markdown pages.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use fzn_engine::text;
use serde::Deserialize;

use crate::store::{Record, Store};

#[derive(Deserialize)]
struct FznItem {
    key: String,
    line: String,
    #[serde(default)]
    note: String,
    #[serde(default)]
    hidden: bool,
    #[serde(default)]
    created_unix_nano: i64,
    #[serde(default)]
    updated_unix_nano: i64,
}

/// Imports the JSON Lines file written by the Go `cmd/export` tool. Re-importing is harmless.
pub fn jsonl(path: &Path, store: &mut Store) -> Result<usize> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut imported = 0;
    for (i, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let item: FznItem =
            serde_json::from_str(&line).with_context(|| format!("{}:{}", path.display(), i + 1))?;
        let text = if item.note.trim().is_empty() {
            item.line
        } else {
            format!("{}\n{}", item.line, item.note)
        };
        let secs = |nanos: i64| (nanos > 0).then_some(nanos / 1_000_000_000);
        let created = secs(item.created_unix_nano);
        let record = Record {
            id: format!("fzn:{}", item.key),
            text,
            created,
            updated: secs(item.updated_unix_nano).or(created),
            archived: item.hidden,
            deleted: false,
            device: String::new(),
        };
        if put_if_new(store, record)? {
            imported += 1;
        }
    }
    Ok(imported)
}

/// Imports a directory of markdown pages. Each page becomes a note headed by its name, as in
/// Silverbullet or Obsidian. Re-importing is harmless.
pub fn markdown(dir: &Path, store: &mut Store) -> Result<usize> {
    let mut files = Vec::new();
    collect_md(dir, &mut files)?;
    let mut imported = 0;
    for path in &files {
        let rel = path.strip_prefix(dir)?.with_extension("");
        let name = rel.to_string_lossy().replace('\\', "/");
        let content =
            fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let base = rel
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let created = text::facts(&base)
            .dates
            .first()
            .map(|&day| i64::from(day) * 86_400);
        let updated = fs::metadata(path)?
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64);
        let record = Record {
            id: format!("md:{name}"),
            text: format!("{name}\n{content}"),
            created,
            updated: updated.or(created),
            archived: false,
            deleted: false,
            device: String::new(),
        };
        if put_if_new(store, record)? {
            imported += 1;
        }
    }
    Ok(imported)
}

fn collect_md(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if name.starts_with('.') {
            continue;
        }
        if path.is_dir() {
            collect_md(&path, out)?;
        } else if path.extension().is_some_and(|x| x == "md") {
            out.push(path);
        }
    }
    out.sort();
    Ok(())
}

fn put_if_new(store: &mut Store, record: Record) -> Result<bool> {
    if store.version(&record.id).is_some_and(|current| {
        current.deleted || (current.text == record.text && current.archived == record.archived)
    }) {
        return Ok(false);
    }
    store.put(record)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::random_u64;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("fzn-import-test-{:x}", random_u64()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn imports_the_fzn_export_once() {
        let dir = temp_dir();
        let file = dir.join("notes.jsonl");
        fs::write(
            &file,
            concat!(
                "{\"index\":0,\"key\":\"1:2\",\"line\":\"shopping milk\",\"hidden\":true,\"created_unix_nano\":1642000000000000000}\n",
                "\n",
                "{\"index\":1,\"key\":\"1:3\",\"line\":\"a head\",\"note\":\"and a body\",\"hidden\":false}\n",
            ),
        )
        .unwrap();
        let mut store = Store::open(&dir.join("space"), "dev").unwrap();
        assert_eq!(jsonl(&file, &mut store).unwrap(), 2);
        assert_eq!(
            jsonl(&file, &mut store).unwrap(),
            0,
            "re-import adds nothing"
        );
        let a = store.get("fzn:1:2").unwrap();
        assert!(a.archived);
        assert_eq!(a.created, Some(1_642_000_000));
        assert_eq!(a.updated, Some(1_642_000_000));
        let b = store.get("fzn:1:3").unwrap();
        assert_eq!(b.text, "a head\nand a body");
        assert_eq!(b.updated, None);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn imports_markdown_pages_headed_by_their_name() {
        let dir = temp_dir();
        let pages = dir.join("pages");
        fs::create_dir_all(pages.join("Journal")).unwrap();
        fs::create_dir_all(pages.join(".hidden")).unwrap();
        fs::write(
            pages.join("Journal").join("2026-09-05.md"),
            "dentist at 3\n",
        )
        .unwrap();
        fs::write(
            pages.join("Plan.md"),
            "# Plan\nsee [[Journal/2026-09-05]]\n",
        )
        .unwrap();
        fs::write(pages.join(".hidden").join("x.md"), "secret").unwrap();
        fs::write(pages.join("notes.txt"), "not markdown").unwrap();
        let mut store = Store::open(&dir.join("space"), "dev").unwrap();
        assert_eq!(markdown(&pages, &mut store).unwrap(), 2);
        let journal = store.get("md:Journal/2026-09-05").unwrap();
        assert_eq!(journal.text, "Journal/2026-09-05\ndentist at 3\n");
        assert_eq!(
            journal.created,
            Some(fzn_engine::time::day_from_civil(2026, 9, 5) as i64 * 86_400)
        );
        assert!(journal.updated.is_some());
        assert!(store.get("md:Plan").unwrap().created.is_none());
        fs::remove_dir_all(&dir).unwrap();
    }
}
