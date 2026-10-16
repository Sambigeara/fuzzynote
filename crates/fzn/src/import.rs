//! Importers: a file of lines, either fzn's own export or a fuzzynote 2022 export, and a
//! directory of markdown pages.
//!
//! They write the writer's log directly, as sealed lines, and only while the app is closed: the
//! lock on the space's directory, which the importer holds, guarantees that.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use fzn_engine::{Day, Engine, Moment, Record, links_on_save, text, time};
use serde::Deserialize;

use crate::store::Store;
use crate::system;

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

/// Imports a file of JSON lines: what `fzn export` writes, a line a note, or a fuzzynote 2022
/// export. Re-importing is harmless.
pub fn jsonl(path: &Path, store: &mut Store) -> Result<usize> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut imported = 0;
    for (i, line) in BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        // A note as fzn writes it comes in as it is, under its own id.
        if let Some(record) = Record::read(line.as_bytes()) {
            if put_imported(store, record)? {
                imported += 1;
            }
            continue;
        }
        let item: FznItem =
            serde_json::from_str(&line).with_context(|| format!("{}:{}", path.display(), i + 1))?;
        let text = if item.note.trim().is_empty() {
            item.line
        } else {
            format!("{}\n{}", item.line, item.note)
        };
        let secs = |nanos: i64| (nanos > 0).then(|| system::local(nanos / 1_000_000_000));
        let created = secs(item.created_unix_nano);
        let record = Record {
            id: format!("fzn:{}", item.key),
            text,
            links: Vec::new(),
            created,
            updated: secs(item.updated_unix_nano).or(created),
            stamp: None,
            after: None,
            placed: None,
            archived: item.hidden,
            deleted: false,
            device: String::new(),
        };
        if put_imported(store, record)? {
            imported += 1;
        }
    }
    Ok(imported)
}

/// What importing a directory of markdown pages did.
pub struct Imported {
    pub pages: usize,
    /// Notes written. One page can make many.
    pub notes: usize,
    /// Pages left out because they are programs for the app they came from, not notes.
    pub skipped: Vec<String>,
}

/// Midday, in seconds, for a note whose day is known and whose time is not.
const MIDDAY: i64 = 12 * 3600;

/// Imports a directory of markdown pages, as Silverbullet or Obsidian keep them, as the notes
/// they would have been had they been written here. Re-importing is harmless.
///
/// - A page named only for a date or a time, a journal page say, has no title of its own. Each
///   thing written on it becomes a note of that day, and nothing is kept for the page.
/// - A page that is nothing but a list becomes a note, and each item a note about it.
/// - Any other page is a document. It stays whole, headed by its name.
///
/// On the first two kinds, a task is a note at any depth, headed by its box, `[ ]` or `[x]`:
/// archived if it was ticked, and about the note it sat under. Other lines under an item are its
/// body. A bullet holding only tags hands them to what is under it. A note carries the tags in
/// force where it was written: its page's, such a bullet's, its parent's. They go on the line
/// under its head, and so do the tags its own line started with, so that a head is what was said
/// and not how it was filed.
///
/// A folder is a tag, unless all it holds is named for dates, which makes it a calendar and not
/// a subject. Tags in front matter are tags, and a `date` there is the day. A `[[page]]` is
/// rewritten to the head its page has now, or to the date if the page was a day. Pages that are
/// programs for Silverbullet are skipped.
///
/// Importing again brings a note up to date with its page, unless its text was changed here
/// since the page was. What was done to a note here is kept: its links, where it sits, whether it
/// is archived, and that it was deleted.
pub fn markdown(dir: &Path, store: &mut Store) -> Result<Imported> {
    let mut files = Vec::new();
    collect_md(dir, &mut files)?;
    let names: Vec<String> = files
        .iter()
        .map(|path| Ok(page_name(path.strip_prefix(dir)?)))
        .collect::<Result<_>>()?;
    let calendars = calendars(&names);

    let mut skipped = Vec::new();
    let mut pages = Vec::new();
    for (path, name) in files.iter().zip(names) {
        let content =
            fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        if is_program(&content) {
            skipped.push(name);
            continue;
        }
        let modified = fs::metadata(path)?
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| system::local(d.as_secs() as i64));
        pages.push(Page::read(name, &content, modified, &calendars));
    }

    // Two pages that would get the same head keep their whole names, so each can be named.
    let mut heads: HashMap<String, usize> = HashMap::new();
    for page in &pages {
        if let Some(head) = &page.head {
            *heads.entry(text::normalise_head(head)).or_default() += 1;
        }
    }
    let mut targets = HashMap::new();
    for page in &mut pages {
        if let Some(head) = &mut page.head {
            if heads[&text::normalise_head(head)] > 1 {
                head.clone_from(&page.name);
            }
            targets.insert(text::normalise_head(&page.name), Target::Note(head.clone()));
        } else if let Some(day) = page.day {
            targets.insert(text::normalise_head(&page.name), Target::Day(day));
        }
    }

    let mut notes = 0;
    for page in &pages {
        for mut record in page.records() {
            record.text = relink(&record.text, &targets);
            if put_imported(store, record)? {
                notes += 1;
            }
        }
    }
    Ok(Imported {
        pages: pages.len(),
        notes,
        skipped,
    })
}

/// What a `[[page]]` points at once the pages are notes.
enum Target {
    /// A note, by its head.
    Note(String),
    /// A page that was a day, and is now only its date.
    Day(Day),
}

/// Rewrites each `[[page]]` to name what the page became.
fn relink(text: &str, targets: &HashMap<String, Target>) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find("[[") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("]]").filter(|&c| !after[..c].contains('\n')) else {
            out.push_str("[[");
            rest = after;
            continue;
        };
        let inner = &after[..close];
        let (page, alias) = match inner.split_once('|') {
            Some((page, alias)) => (page, Some(alias)),
            None => (inner, None),
        };
        let section = page.split('#').next().unwrap_or(page);
        let target = targets
            .get(&text::normalise_head(page))
            .or_else(|| targets.get(&text::normalise_head(section)));
        match (target, alias) {
            (Some(Target::Note(head)), Some(alias)) => out.push_str(&format!("[[{head}|{alias}]]")),
            (Some(Target::Note(head)), None) => out.push_str(&format!("[[{head}]]")),
            (Some(Target::Day(day)), _) => out.push_str(&time::format_day(*day)),
            (None, _) => out.push_str(&rest[open..open + 2 + close + 2]),
        }
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    out
}

/// A page's path under the directory, without its extension: what a `[[link]]` calls it.
fn page_name(rel: &Path) -> String {
    rel.with_extension("").to_string_lossy().replace('\\', "/")
}

/// The folders that hold nothing but pages and folders named for dates or times.
fn calendars(names: &[String]) -> HashSet<String> {
    let mut plain = HashSet::new();
    let mut folders = HashSet::new();
    for name in names {
        let parts: Vec<&str> = name.split('/').collect();
        for i in 1..parts.len() {
            let folder = parts[..i].join("/");
            if date_name(parts[i]).is_none() && time_name(parts[i]).is_none() {
                plain.insert(folder.clone());
            }
            folders.insert(folder);
        }
    }
    folders.retain(|folder| !plain.contains(folder));
    folders
}

fn date_at(b: &[u8]) -> Option<Day> {
    let num = |from: usize, n: usize| {
        b.get(from..from + n)?.iter().try_fold(0u32, |acc, c| {
            c.is_ascii_digit().then(|| acc * 10 + u32::from(c - b'0'))
        })
    };
    let (year, month, day) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
    (b[4] == b'-' && b[7] == b'-' && (1..=12).contains(&month) && (1..=31).contains(&day))
        .then(|| time::day_from_civil(year as i32, month, day))
}

/// The first date written anywhere in `s`, as `2026-09-05`.
fn date_in(s: &str) -> Option<Day> {
    let b = s.as_bytes();
    (0..b.len().saturating_sub(9)).find_map(|i| date_at(&b[i..i + 10]))
}

/// The date, if `s` is a date and nothing else.
fn date_name(s: &str) -> Option<Day> {
    (s.len() == 10).then(|| date_at(s.as_bytes())).flatten()
}

/// Seconds into the day, if `s` is a time and nothing else, written `16-09-24`.
fn time_name(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let num = |from: usize| {
        (b[from].is_ascii_digit() && b[from + 1].is_ascii_digit())
            .then(|| i64::from(b[from] - b'0') * 10 + i64::from(b[from + 1] - b'0'))
    };
    if b.len() != 8 || b[2] != b'-' || b[5] != b'-' {
        return None;
    }
    let (h, m, s) = (num(0)?, num(3)?, num(6)?);
    (h < 24 && m < 60 && s < 60).then_some(h * 3600 + m * 60 + s)
}

/// A name as a tag: lowercased, spaces as hyphens, and nothing a tag cannot hold.
fn tag_of(name: &str) -> String {
    let tag: String = name
        .trim()
        .chars()
        .filter_map(|c| match c {
            c if c.is_alphanumeric() || matches!(c, '_' | '-' | '/') => Some(c),
            c if c.is_whitespace() => Some('-'),
            _ => None,
        })
        .flat_map(char::to_lowercase)
        .collect();
    tag.trim_matches(['-', '/']).to_owned()
}

/// Tags as a line of text: `#a #b`.
fn tag_line<'a>(tags: impl IntoIterator<Item = &'a String>) -> String {
    let tags: Vec<String> = tags.into_iter().map(|tag| format!("#{tag}")).collect();
    tags.join(" ")
}

/// Splits a line into the tags it starts with, as they were written, and what follows them.
fn leading_tags(line: &str) -> (Vec<&str>, &str) {
    let mut tags = Vec::new();
    let mut rest = line.trim_start();
    while let Some(word) = rest.split_whitespace().next() {
        if !text::only_tags(word) {
            break;
        }
        tags.push(word);
        rest = rest[word.len()..].trim_start();
    }
    (tags, rest)
}

/// The text of a heading, if the line is one.
fn heading(line: &str) -> Option<&str> {
    let rest = line.trim_start_matches('#');
    ((1..=6).contains(&(line.len() - rest.len())) && rest.starts_with(' ')).then(|| rest.trim())
}

/// Whether a page is a program for Silverbullet: its configuration, or a page of queries.
fn is_program(content: &str) -> bool {
    let mut fenced = false;
    content.lines().any(|line| {
        let line = line.trim_start();
        if let Some(info) = line.strip_prefix("```") {
            fenced = !fenced;
            fenced && info.starts_with("space-")
        } else {
            !fenced && line.starts_with("${")
        }
    })
}

/// A page read, and what it is to become.
struct Page {
    name: String,
    /// What heads the note the page becomes. A page with none is not kept: what was written on
    /// it becomes notes of its day.
    head: Option<String>,
    tags: Vec<String>,
    /// Front matter other than tags and the date, which a page that stays a note keeps.
    front: Vec<String>,
    /// The page, below its front matter and the heading that became its head.
    lines: Vec<String>,
    day: Option<Day>,
    /// Seconds into the day, for a page named for a time.
    time: Option<i64>,
    modified: Option<Moment>,
}

impl Page {
    fn read(
        name: String,
        content: &str,
        modified: Option<Moment>,
        calendars: &HashSet<String>,
    ) -> Self {
        let mut lines: Vec<&str> = content.lines().collect();
        let mut tags = Vec::new();
        let mut front = Vec::new();
        let mut day = None;
        if lines.first().is_some_and(|l| l.trim() == "---")
            && let Some(close) = lines[1..].iter().position(|l| l.trim() == "---")
        {
            let mut listing_tags = false;
            for line in &lines[1..=close] {
                if let Some(value) = line.strip_prefix("tags:") {
                    listing_tags = value.trim().is_empty();
                    tags.extend(value.split([' ', ',']).map(tag_of));
                } else if let Some(item) = line.trim_start().strip_prefix("- ")
                    && listing_tags
                {
                    tags.push(tag_of(item));
                } else if let Some(value) = line.strip_prefix("date:") {
                    listing_tags = false;
                    day = date_in(value);
                } else {
                    listing_tags = false;
                    front.push((*line).to_owned());
                }
            }
            lines.drain(..close + 2);
        }

        // Folders are subjects, so tags. A calendar is not a subject, and neither is a tag that
        // only repeats its name.
        let parts: Vec<&str> = name.split('/').collect();
        let (base, folders) = parts.split_last().unwrap_or((&"", &[]));
        let mut not_subjects = Vec::new();
        for (i, folder) in folders.iter().enumerate() {
            if let Some(d) = date_name(folder) {
                day = day.or(Some(d));
            } else if calendars.contains(&parts[..=i].join("/")) {
                not_subjects.push(tag_of(folder));
            } else {
                tags.push(tag_of(folder));
            }
        }
        tags.retain(|tag| !tag.is_empty() && !not_subjects.contains(tag));
        let mut seen = HashSet::new();
        tags.retain(|tag| seen.insert(tag.clone()));

        // A page named only for a date or a time takes its head from a title at its top, and has
        // no head if there is none.
        let named_for_when = date_name(base).is_some() || time_name(base).is_some();
        let head = if named_for_when {
            let top = lines.iter().position(|l| !l.trim().is_empty());
            let title = top.and_then(|i| lines[i].strip_prefix("# ").map(|t| (i, t.trim())));
            title.map(|(i, title)| {
                let title = title.to_owned();
                lines.drain(..=i);
                title
            })
        } else {
            Some((*base).to_owned())
        };
        Self {
            head,
            tags,
            front,
            lines: lines.into_iter().map(str::to_owned).collect(),
            day: day.or_else(|| date_in(base)),
            time: time_name(base),
            modified,
            name,
        }
    }

    fn records(&self) -> Vec<Record> {
        let record = |id: String, text: String, when: (Option<Moment>, Option<Moment>)| Record {
            id,
            text,
            links: Vec::new(),
            created: when.0,
            updated: when.1.or(when.0),
            stamp: None,
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: String::new(),
        };
        let written = self
            .day
            .map(|day| system::on(day, self.time.unwrap_or(MIDDAY)));
        let nodes = nodes(&rows(&self.lines), &mut 0, None);
        let id = format!("md:{}", self.name);
        let mut lifted = Lifted {
            page: &self.name,
            notes: Vec::new(),
        };

        let Some(head) = &self.head else {
            // The page was a day. What was written on it is of that day, in the order it had.
            lifted.all(&nodes, &self.tags, None);
            let when = written.or(self.modified);
            return lifted.records(|i| {
                let when = when.map(|m| m.plus_secs(-(i as i64)));
                (when, when)
            });
        };

        let mut text = head.clone();
        if !self.tags.is_empty() {
            text.push('\n');
            text.push_str(&tag_line(&self.tags));
        }
        if !self.front.is_empty() {
            text.push_str(&format!("\n---\n{}\n---", self.front.join("\n")));
        }
        let when = (written, self.modified);
        if !nodes.is_empty() && nodes.iter().all(|n| matches!(n, Node::Item(_))) {
            // The page was a list. Each item is a note about it, in the order the list had.
            lifted.all(&nodes, &self.tags, Some(&id));
            let mut records = vec![record(id, text, when)];
            records.extend(lifted.records(|i| {
                let updated = when.1.or(when.0).map(|m| m.plus_secs(-(i as i64) - 1));
                (when.0, updated)
            }));
            return records;
        }
        let body = self.lines.join("\n");
        let body = body.trim_end().trim_start_matches(['\n', '\r']);
        if !body.is_empty() {
            text.push('\n');
            text.push_str(body);
        }
        vec![record(id, text, when)]
    }
}

/// One line of a page: how far in it starts, and what it says. For a list item, `task` says
/// whether it is a task, and whether that is done.
struct Row {
    indent: usize,
    item: bool,
    task: Option<bool>,
    text: String,
}

fn rows(lines: &[String]) -> Vec<Row> {
    let width = |c: char| if c == '\t' { 4 } else { 1 };
    let mut rows = Vec::new();
    // How far in the open code fence starts. Inside one, every line is kept as it is.
    let mut fence: Option<usize> = None;
    for line in lines {
        let content = line.trim_start();
        let indent: usize = line[..line.len() - content.len()].chars().map(width).sum();
        let mut row = Row {
            indent,
            item: false,
            task: None,
            text: content.trim_end().to_owned(),
        };
        if let Some(at) = fence {
            row.text = format!("{}{}", " ".repeat(indent.saturating_sub(at)), row.text);
            row.indent = at;
            if content.starts_with("```") {
                fence = None;
            }
        } else if matches!(content.trim_end(), "" | "-" | "*" | "+") {
            // A blank line, or a list marker with nothing after it.
            continue;
        } else if content.starts_with("```") {
            fence = Some(indent);
        } else if let Some(rest) = bullet(content) {
            let (task, rest) = match rest.get(..3) {
                Some("[ ]") => (Some(false), &rest[3..]),
                Some("[x]" | "[X]") => (Some(true), &rest[3..]),
                _ => (None, rest),
            };
            row.item = true;
            row.task = task;
            row.text = rest.trim().to_owned();
        }
        rows.push(row);
    }
    rows
}

/// What follows a list marker, if the line starts with one: `-`, `*`, `+`, `1.` or `1)`.
fn bullet(line: &str) -> Option<&str> {
    let digits = line.bytes().take_while(u8::is_ascii_digit).count();
    let marker = match line.as_bytes().get(digits)? {
        b'-' | b'*' | b'+' if digits == 0 => 1,
        b'.' | b')' if (1..=3).contains(&digits) => digits + 1,
        _ => return None,
    };
    line[marker..].strip_prefix(' ')
}

struct Item {
    text: String,
    task: Option<bool>,
    under: Vec<Node>,
}

enum Node {
    Item(Item),
    /// A line that is not a list item: a paragraph, a heading, a quote, code.
    Text(String),
}

/// Builds the tree of a list from its rows: what is further in than an item is under it.
fn nodes(rows: &[Row], at: &mut usize, parent: Option<usize>) -> Vec<Node> {
    let mut out = Vec::new();
    while let Some(row) = rows.get(*at) {
        if parent.is_some_and(|parent| row.indent <= parent) {
            break;
        }
        *at += 1;
        out.push(if row.item {
            Node::Item(Item {
                text: row.text.clone(),
                task: row.task,
                under: nodes(rows, at, Some(row.indent)),
            })
        } else {
            Node::Text(row.text.clone())
        });
    }
    out
}

/// A note lifted out of a page.
struct Draft {
    text: String,
    archived: bool,
    /// The note it is about, if any.
    about: Option<String>,
}

/// The notes lifted out of one page, in the order they were written on it.
struct Lifted<'a> {
    page: &'a str,
    notes: Vec<Draft>,
}

impl Lifted<'_> {
    fn id(&self, index: usize) -> String {
        format!("md:{}#{}", self.page, index + 1)
    }

    /// Lifts a run of nodes that sit side by side. An item is a note. A heading is a note that
    /// takes what follows it up to the next heading, and any other line takes the lines after it.
    fn all(&mut self, nodes: &[Node], tags: &[String], about: Option<&str>) {
        let mut i = 0;
        while let Some(node) = nodes.get(i) {
            i += 1;
            let line = match node {
                Node::Item(item) => {
                    self.item(item, tags, about);
                    continue;
                }
                Node::Text(line) => line,
            };
            let title = heading(line);
            let ends = |n: &Node| match n {
                Node::Text(line) => heading(line).is_some(),
                Node::Item(_) => title.is_none(),
            };
            let end = nodes[i..]
                .iter()
                .position(ends)
                .map_or(nodes.len(), |n| i + n);
            self.note(title.unwrap_or(line), tags, None, about, &nodes[i..end]);
            i = end;
        }
    }

    fn item(&mut self, item: &Item, tags: &[String], about: Option<&str>) {
        let holds_items = item.under.iter().any(|n| matches!(n, Node::Item(_)));
        if item.task.is_none() && holds_items && text::only_tags(&item.text) {
            let mut tags = tags.to_vec();
            for tag in text::facts(&item.text).tags {
                if !tags.contains(&tag) {
                    tags.push(tag);
                }
            }
            self.all(&item.under, &tags, about);
        } else {
            self.note(&item.text, tags, item.task, about, &item.under);
        }
    }

    /// A note, with what is under it as its body. The tasks under it are notes about it instead.
    /// Its tags go on the line under the head: the ones in force that it does not carry itself,
    /// then the ones its line started with. A line of nothing but tags is left as it is. A task
    /// is headed by its box, and archived if it is ticked.
    fn note(
        &mut self,
        head: &str,
        tags: &[String],
        task: Option<bool>,
        about: Option<&str>,
        under: &[Node],
    ) {
        let index = self.notes.len();
        self.notes.push(Draft {
            text: String::new(),
            archived: task == Some(true),
            about: about.map(str::to_owned),
        });
        let (line, started_with) = match leading_tags(head) {
            (_, "") => (head, Vec::new()),
            (started_with, rest) => (rest, started_with),
        };
        let mut in_force = tags.to_vec();
        for tag in text::facts(head).tags {
            if !in_force.contains(&tag) {
                in_force.push(tag);
            }
        }
        let mut body = String::new();
        self.body(under, 0, &mut body, &in_force, &self.id(index));
        // A box with nothing after it heads nothing, as it did before boxes were kept.
        let mut text = match task {
            Some(done) if !line.is_empty() => text::tick(line, done),
            _ => line.to_owned(),
        };
        let own = text::facts(&format!("{head}\n{body}")).tags;
        let mut labels = tag_line(tags.iter().filter(|tag| !own.contains(tag)));
        for tag in started_with {
            if !labels.is_empty() {
                labels.push(' ');
            }
            labels.push_str(tag);
        }
        for part in [labels.as_str(), body.trim_end()] {
            if !part.is_empty() {
                text.push('\n');
                text.push_str(part);
            }
        }
        self.notes[index].text = text;
    }

    fn body(&mut self, nodes: &[Node], depth: usize, body: &mut String, tags: &[String], id: &str) {
        for node in nodes {
            match node {
                Node::Item(item) if item.task.is_some() => self.item(item, tags, Some(id)),
                Node::Item(item) => {
                    body.push_str(&format!("{}- {}\n", "  ".repeat(depth), item.text));
                    self.body(&item.under, depth + 1, body, tags, id);
                }
                Node::Text(line) => body.push_str(&format!("{}{line}\n", "  ".repeat(depth))),
            }
        }
    }

    /// The notes as records, each given its times by its place on the page.
    fn records(self, when: impl Fn(usize) -> (Option<Moment>, Option<Moment>)) -> Vec<Record> {
        let ids: Vec<String> = (0..self.notes.len()).map(|i| self.id(i)).collect();
        self.notes
            .into_iter()
            .zip(ids)
            .enumerate()
            .map(|(i, (note, id))| {
                let (created, updated) = when(i);
                Record {
                    id,
                    text: note.text,
                    links: note.about.into_iter().collect(),
                    created,
                    updated,
                    stamp: None,
                    after: None,
                    placed: None,
                    archived: note.archived,
                    deleted: false,
                    device: String::new(),
                }
            })
            .collect()
    }
}

/// Links every note in the space to the notes its text names in `[[brackets]]`, as saving it in
/// the app would. Pages name each other before all of them are in, so this runs once they are.
/// Returns how many notes gained a link.
pub fn link_named(store: &mut Store) -> Result<usize> {
    let mut engine = Engine::new();
    for record in store.notes().records() {
        engine.upsert(record.note());
    }
    let relinked: Vec<Record> = store
        .notes()
        .records()
        .filter_map(|r| {
            let links = links_on_save(&engine, &r.id, "", &r.text, &r.links);
            (links != r.links).then(|| Record {
                links,
                stamp: Some(store.next_stamp(&r.id)),
                ..r.clone()
            })
        })
        .collect();
    for record in &relinked {
        store.put(record.clone())?;
    }
    Ok(relinked.len())
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

/// Writes an imported note, unless that would undo something done here. A note deleted here
/// stays deleted, and a text changed here after its source last changed is kept. Otherwise the
/// text becomes the import's, and the note keeps its links and whether it is archived. It keeps
/// where it sits too, unless what is imported was placed later, which a page never is: its
/// versions have no `placed`.
fn put_imported(store: &mut Store, mut record: Record) -> Result<bool> {
    if let Some(held) = store.notes().version(&record.id) {
        if held.deleted || held.text == record.text || held.updated > record.updated {
            return Ok(false);
        }
        record.archived = held.archived;
        let about = std::mem::replace(&mut record.links, held.links.clone());
        for link in about {
            if !record.links.contains(&link) {
                record.links.push(link);
            }
        }
        record.stamp = Some(store.next_stamp(&record.id));
    }
    store.put(record)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::{seal, temp_dir};

    fn open(space: &Path) -> Store {
        Store::open(space, "dev".into(), seal()).unwrap()
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
        let mut store = open(&dir.join("space"));
        assert_eq!(jsonl(&file, &mut store).unwrap(), 2);
        assert_eq!(
            jsonl(&file, &mut store).unwrap(),
            0,
            "re-import adds nothing"
        );
        let a = store.notes().get("fzn:1:2").unwrap();
        assert!(a.archived);
        assert_eq!(a.created.map(|m| m.secs()), Some(1_642_000_000));
        assert_eq!(a.updated, a.created);
        let b = store.notes().get("fzn:1:3").unwrap();
        assert_eq!(b.text, "a head\nand a body");
        assert_eq!(b.updated, None);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_export_is_brought_into_another_space_by_importing_it() {
        let dir = temp_dir();
        let mut from = open(&dir.join("a"));
        let note = |id: &str, text: &str, secs: i64| Record {
            id: id.into(),
            text: text.into(),
            links: Vec::new(),
            created: Some(Moment::utc(secs)),
            updated: Some(Moment::utc(secs)),
            stamp: None,
            after: None,
            placed: None,
            archived: false,
            deleted: false,
            device: String::new(),
        };
        from.put(note("n1", "Boiler", 10)).unwrap();
        from.put(Record {
            archived: true,
            links: vec!["n1".into()],
            ..note("n2", "House", 20)
        })
        .unwrap();
        let file = dir.join("export.jsonl");
        from.export(&mut fs::File::create(&file).unwrap()).unwrap();

        let mut to = open(&dir.join("b"));
        assert_eq!(jsonl(&file, &mut to).unwrap(), 2);
        assert_eq!(jsonl(&file, &mut to).unwrap(), 0, "re-import adds nothing");
        let house = to.notes().get("n2").unwrap();
        assert!(house.archived);
        assert_eq!(house.links, ["n1"]);
        assert_eq!(house.device, "dev");
        assert_eq!(to.notes().get("n1").unwrap().text, "Boiler");
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A small space of pages of every kind the importer tells apart.
    fn pages(dir: &Path) -> PathBuf {
        let pages = dir.join("pages");
        for (name, content) in [
            (
                "Journal/2026-09-05.md",
                "---\ntags: journal\ndate: 2026-09-05\n---\n\
                 * #garden\n  * Planted the beans\n    * two rows\n    * [ ] water them [priority: high]\n  * [x] Order compost\n\
                 - #house Called the plumber about [[Projects/Boiler]]\n  > he can come Friday\n\
                 - [ ] #errands Renew passport\n\
                 ## Reading\n- finished the book\n- see [[Journal/2026-09-04]]\n",
            ),
            ("Journal/2026-09-04.md", "- walked\n- #someday\n- [ ]\n"),
            (
                "Projects/Boiler.md",
                "---\ntags: house\n---\n\n* [ ] get three quotes\n* [X] find the manual\n\n- Old notes\n  - [[Projects/Heating Plan|the plan]]\n- \n-\n",
            ),
            (
                "Projects/Heating Plan.md",
                "---\ntags:\n  - house\n  - plan\nstatus: draft\n---\n# Heating\n\nReplace the boiler by winter.\n\n- [ ] decide on a heat pump\n",
            ),
            (
                "Projects/reviews/2026-09-07.md",
                "# Review: first week\n\n## Done\n- [[Projects/Boiler]] quotes\n",
            ),
            (
                "Inbox/2026-09-05/16-09-24.md",
                "- [Seed catalogue](https://example.com/seeds)\n  - from the garden centre\n",
            ),
            ("index.md", "# Home\n${query[[from index.tag \"page\"]]}\n"),
            ("CONFIG.md", "```space-lua\nconfig.set(\"x\", 1)\n```\n"),
            (".hidden/x.md", "secret"),
            ("notes.txt", "not markdown"),
        ] {
            let path = pages.join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        }
        pages
    }

    #[test]
    fn a_page_named_for_a_day_becomes_the_notes_written_on_it() {
        let dir = temp_dir();
        let mut store = open(&dir.join("space"));
        let done = markdown(&pages(&dir), &mut store).unwrap();
        assert_eq!((done.pages, done.notes), (6, 16));
        assert_eq!(done.skipped, ["CONFIG", "index"]);
        link_named(&mut store).unwrap();

        assert!(store.notes().get("md:Journal/2026-09-05").is_none());
        let note = |n: usize| {
            store
                .notes()
                .get(&format!("md:Journal/2026-09-05#{n}"))
                .unwrap()
        };
        let day = time::day_from_civil(2026, 9, 5);

        // A bullet holding only a tag hands it down. The task under the beans is a note about
        // them, headed by its box and with their tag, and the rest of what is under them is
        // their body.
        assert_eq!(note(1).text, "Planted the beans\n#garden\n- two rows");
        assert_eq!(note(2).text, "[ ] water them [priority: high]\n#garden");
        assert_eq!(note(2).links, ["md:Journal/2026-09-05#1"]);
        assert!(!note(2).archived);
        assert_eq!(note(3).text, "[x] Order compost\n#garden");
        assert!(note(3).archived, "ticked, so archived");
        assert!(note(3).links.is_empty());

        // A tag the line started with goes under the head too. A link to a page is a link to
        // the note it became, under the head it has now.
        assert_eq!(
            note(4).text,
            "Called the plumber about [[Boiler]]\n#house\n> he can come Friday"
        );
        assert_eq!(note(4).links, ["md:Projects/Boiler"]);
        assert_eq!(note(5).text, "[ ] Renew passport\n#errands");

        // A heading takes what follows it, and a link to a day is the date.
        assert_eq!(
            note(6).text,
            "Reading\n- finished the book\n- see 2026-09-04"
        );
        assert!(store.notes().get("md:Journal/2026-09-05#7").is_none());

        // All of that day, in the order the page had, which a list shows newest first.
        for n in 1..=6 {
            assert_eq!(note(n).created.unwrap().day(), day);
            assert_eq!(note(n).created, note(n).updated);
        }
        assert!(note(1).updated > note(2).updated && note(5).updated > note(6).updated);
        assert_eq!(
            store.notes().get("md:Journal/2026-09-04#1").unwrap().text,
            "walked"
        );
        assert_eq!(
            store.notes().get("md:Journal/2026-09-04#2").unwrap().text,
            "#someday",
            "a line of nothing but tags is left as it is"
        );
        let empty = store.notes().get("md:Journal/2026-09-04#3").unwrap();
        assert_eq!(
            text::task(&empty.text),
            None,
            "a box with nothing after it heads nothing"
        );

        // A page named for a time is the note written then.
        let seeds = store.notes().get("md:Inbox/2026-09-05/16-09-24#1").unwrap();
        assert_eq!(
            seeds.text,
            "[Seed catalogue](https://example.com/seeds)\n- from the garden centre"
        );
        assert_eq!(
            seeds.created,
            Some(system::on(day, 16 * 3600 + 9 * 60 + 24))
        );

        assert_eq!(markdown(&pages(&dir), &mut store).unwrap().notes, 0);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn importing_again_brings_notes_up_to_date_and_keeps_what_was_done_here() {
        let dir = temp_dir();
        let pages = pages(&dir);
        let mut store = open(&dir.join("space"));
        markdown(&pages, &mut store).unwrap();
        link_named(&mut store).unwrap();
        let held = |store: &Store, id: &str| store.notes().get(id).unwrap().clone();

        // Here: one note is rewritten, one is linked, archived and moved, one is deleted.
        let passport = held(&store, "md:Journal/2026-09-05#5");
        store
            .put(Record {
                text: "Renew passport, and the driving licence".into(),
                updated: Some(system::now()),
                stamp: Some(store.next_stamp(&passport.id)),
                ..passport
            })
            .unwrap();
        let walked = held(&store, "md:Journal/2026-09-04#1");
        store
            .put(Record {
                links: vec!["md:Projects/Boiler".into()],
                archived: true,
                stamp: Some(store.next_stamp(&walked.id)),
                after: Some("md:Journal/2026-09-04#2".into()),
                placed: Some(system::now()),
                ..walked
            })
            .unwrap();
        let seeds = held(&store, "md:Inbox/2026-09-05/16-09-24#1");
        store
            .put(Record {
                deleted: true,
                stamp: Some(store.next_stamp(&seeds.id)),
                ..seeds
            })
            .unwrap();
        // An import from before boxes took a task's off, and it was archived here since, which
        // leaves its text as the page last gave it.
        let quotes = held(&store, "md:Projects/Boiler#1");
        store
            .put(Record {
                text: quotes.text.replacen("[ ] ", "", 1),
                archived: true,
                stamp: Some(store.next_stamp(&quotes.id)),
                ..quotes
            })
            .unwrap();

        // There: every one of those pages says something new.
        fs::write(pages.join("Journal/2026-09-04.md"), "- walked far\n").unwrap();
        fs::write(
            pages.join("Inbox/2026-09-05/16-09-24.md"),
            "- other seeds\n",
        )
        .unwrap();
        let journal = pages.join("Journal/2026-09-05.md");
        let page = fs::read_to_string(&journal).unwrap();
        fs::write(&journal, page.replace("Renew passport", "Renew it")).unwrap();

        assert_eq!(markdown(&pages, &mut store).unwrap().notes, 2);
        let walked = held(&store, "md:Journal/2026-09-04#1");
        assert_eq!(walked.text, "walked far");
        assert!(walked.archived);
        assert_eq!(walked.links, ["md:Projects/Boiler"]);
        assert_eq!(walked.after.as_deref(), Some("md:Journal/2026-09-04#2"));
        let quotes = held(&store, "md:Projects/Boiler#1");
        assert_eq!(quotes.text, "[ ] get three quotes\n#house #projects");
        assert!(quotes.archived, "its box is back, and it is still archived");
        assert_eq!(
            held(&store, "md:Journal/2026-09-05#5").text,
            "Renew passport, and the driving licence",
            "changed here since, so kept"
        );
        assert!(
            store
                .notes()
                .get("md:Inbox/2026-09-05/16-09-24#1")
                .is_none()
        );
        let reopened = open(&dir.join("space"));
        assert_eq!(held(&reopened, "md:Journal/2026-09-04#1"), walked);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_list_becomes_notes_about_its_page_and_a_document_stays_whole() {
        let dir = temp_dir();
        let mut store = open(&dir.join("space"));
        markdown(&pages(&dir), &mut store).unwrap();
        link_named(&mut store).unwrap();

        // The folder and the front matter are tags. Each item is about the page, in its order.
        let boiler = store.notes().get("md:Projects/Boiler").unwrap();
        assert_eq!(boiler.text, "Boiler\n#house #projects");
        let item = |n: usize| {
            store
                .notes()
                .get(&format!("md:Projects/Boiler#{n}"))
                .unwrap()
        };
        assert_eq!(item(1).text, "[ ] get three quotes\n#house #projects");
        assert_eq!(item(1).links, ["md:Projects/Boiler"]);
        assert_eq!(item(2).text, "[x] find the manual\n#house #projects");
        assert!(!item(1).archived && item(2).archived);
        assert_eq!(
            item(3).text,
            "Old notes\n#house #projects\n- [[Heating Plan|the plan]]"
        );
        assert_eq!(
            item(3).links,
            ["md:Projects/Boiler", "md:Projects/Heating Plan"]
        );
        assert!(boiler.updated > item(1).updated && item(1).updated > item(2).updated);
        assert!(
            store.notes().get("md:Projects/Boiler#4").is_none(),
            "an empty bullet is not a note"
        );

        // A document keeps its text, tasks included, and the front matter that is not tags.
        let plan = store.notes().get("md:Projects/Heating Plan").unwrap();
        assert_eq!(
            plan.text,
            "Heating Plan\n#house #plan #projects\n---\nstatus: draft\n---\n# Heating\n\n\
             Replace the boiler by winter.\n\n- [ ] decide on a heat pump"
        );
        assert!(plan.created.is_none());
        assert!(store.notes().get("md:Projects/Heating Plan#1").is_none());

        // A page named for a date is headed by its title, and a calendar folder is not a tag.
        let review = store.notes().get("md:Projects/reviews/2026-09-07").unwrap();
        assert_eq!(
            review.text,
            "Review: first week\n#projects\n## Done\n- [[Boiler]] quotes"
        );
        assert_eq!(
            review.created.unwrap().day(),
            time::day_from_civil(2026, 9, 7)
        );
        assert_eq!(review.links, ["md:Projects/Boiler"]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn pages_that_would_share_a_head_keep_their_names() {
        let dir = temp_dir();
        let pages = dir.join("pages");
        for name in ["work/Plan.md", "home/Plan.md"] {
            fs::create_dir_all(pages.join(name).parent().unwrap()).unwrap();
            fs::write(pages.join(name), "the plan\n\nsee [[work/Plan]]\n").unwrap();
        }
        let mut store = open(&dir.join("space"));
        markdown(&pages, &mut store).unwrap();
        assert_eq!(
            store.notes().get("md:home/Plan").unwrap().text,
            "home/Plan\n#home\nthe plan\n\nsee [[work/Plan]]"
        );
        assert!(
            store
                .notes()
                .get("md:work/Plan")
                .unwrap()
                .text
                .starts_with("work/Plan\n#work\n")
        );
        fs::remove_dir_all(&dir).unwrap();
    }
}
