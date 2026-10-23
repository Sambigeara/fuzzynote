//! A list in the space's order drawn as a tree: each note under the note it is under, what Enter
//! writes there, where a move goes, and what comes of a row that goes.

use super::*;
use crate::session::list::Drawn;

/// The radiators of a house: a note with four rooms under it, three notes under the first room
/// and a task under one of those, and a note beside it, each ending with `tag`, and below them a
/// garden that is not in the house. Each is put before the one above it, so that the list reads
/// top to bottom as the notes are listed here.
fn valves(s: &mut Session, tag: &str) {
    put(s, "garden", "Mow the garden", &[], 900);
    let notes: [(&str, &str, &[&str]); 10] = [
        ("key", "[ ] Buy a radiator key", &["bleed"]),
        ("fit", "[ ] Fit the new valve on the landing", &["upstairs"]),
        ("bleed", "[ ] Bleed the bedroom radiator", &["upstairs"]),
        ("check", "Check the pressure first", &["upstairs"]),
        ("loft", "loft", &["valves"]),
        ("hallway", "hallway", &["valves"]),
        ("downstairs", "downstairs", &["valves"]),
        ("upstairs", "upstairs", &["valves"]),
        ("oil", "Order heating oil", &[]),
        ("valves", "Replace the radiator valves", &[]),
    ];
    for (at, (id, head, links)) in notes.into_iter().enumerate() {
        put(s, id, &format!("{head}{tag}"), links, 1_000 + at as i64);
    }
}

/// The radiators as `#house` draws them, with nothing on the line.
const HOUSE: [&str; 10] = [
    "Replace the radiator valves",
    "  upstairs",
    "    Check the pressure first",
    "    [ ] Bleed the bedroom radiator",
    "      [ ] Buy a radiator key",
    "    [ ] Fit the new valve on the landing",
    "  downstairs",
    "  hallway",
    "  loft",
    "Order heating oil",
];

/// What the list in `#house` would be, made now.
fn remade(s: &mut Session) -> Vec<String> {
    s.act(Action::Select(None));
    drawn(s)
}

#[test]
fn the_top_draws_each_note_under_the_note_it_is_under() {
    let mut s = session();
    valves(&mut s, "");
    assert_eq!(drawn(&s)[..HOUSE.len()], HOUSE);
    assert_eq!(drawn(&s)[HOUSE.len()..], ["Mow the garden"]);
    let depths: Vec<usize> = s.rows.iter().map(|r| r.depth).collect();
    assert_eq!(depths, [0, 1, 2, 2, 3, 2, 1, 1, 1, 0, 0]);
    let json = serde_json::to_value(&s.rows[1]).unwrap();
    assert_eq!(json["depth"], 1, "the depth crosses to the page");
    assert!(json.get("under").is_none() && json.get("level").is_none());
}

#[test]
fn a_label_place_draws_its_notes_as_a_tree_and_its_rows_hide_the_label() {
    let mut s = session();
    valves(&mut s, " #house");
    go_to(&mut s, "#house");
    assert_eq!(drawn(&s), HOUSE);
    assert!(s.rows.iter().all(|r| r.linked && r.reasons.is_empty()));

    // A line of nothing but what holds draws among what holds it.
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "#house");
    assert!(drawn(&s).iter().all(|row| row.ends_with(" #house")));
    assert_eq!(s.rows.iter().map(|r| r.depth).max(), Some(3));
}

#[test]
fn a_notes_place_is_a_tree_rooted_at_the_note_you_are_in() {
    let mut s = session();
    valves(&mut s, " #house");
    go_to(&mut s, "#house");
    go_into(&mut s, "Replace the radiator valves #house");
    assert_eq!(
        drawn(&s),
        [
            "upstairs",
            "  Check the pressure first",
            "  [ ] Bleed the bedroom radiator",
            "    [ ] Buy a radiator key",
            "  [ ] Fit the new valve on the landing",
            "downstairs",
            "hallway",
            "loft",
        ]
    );
}

#[test]
fn the_open_tasks_draw_a_task_under_a_task_and_one_under_a_note_at_the_top_with_its_arrow() {
    let mut s = session();
    valves(&mut s, " #house");
    let tasks = [
        "[ ] Bleed the bedroom radiator  → upstairs",
        "  [ ] Buy a radiator key",
        "[ ] Fit the new valve on the landing  → upstairs",
    ];
    go_to(&mut s, "#house");
    go_to(&mut s, "[ ]");
    assert_eq!(path(&s), ["#house", "[ ]"]);
    assert_eq!(drawn(&s), tasks);
    s.act(Action::Back);
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "[ ]");
    let at_top: Vec<String> = drawn(&s)
        .iter()
        .map(|row| row.replace(" #house", ""))
        .collect();
    assert_eq!(at_top, tasks, "on the line at the top too");
}

#[test]
fn words_and_spans_list_flat_with_their_arrows() {
    let mut s = session();
    valves(&mut s, "");
    type_in(&mut s, "valve");
    assert!(s.rows.iter().all(|r| r.depth == 0));
    assert!(
        drawn(&s).contains(&"[ ] Fit the new valve on the landing  → upstairs".to_owned()),
        "{:?}",
        drawn(&s)
    );
    clear(&mut s);
    type_in(&mut s, "..");
    assert!(s.rows.iter().all(|r| r.depth == 0));
    assert!(drawn(&s).contains(&"upstairs  → Replace the radiator valves".to_owned()));
    assert_eq!(s.in_order(), 0, "and nothing there is moved");
}

#[test]
fn a_note_under_two_listed_notes_is_drawn_once_under_the_first_it_links_to() {
    let mut s = session();
    put(&mut s, "upstairs", "upstairs", &["boiler"], 1_000);
    put(&mut s, "valves", "Replace the radiator valves", &[], 2_000);
    put(&mut s, "boiler", "Boiler", &[], 3_000);
    let mut bleed = s.notes.get("upstairs").unwrap().clone();
    bleed.links = vec!["valves".into(), "boiler".into()];
    s.commit(bleed);
    s.refresh(true);
    assert_eq!(
        drawn(&s),
        [
            "Boiler",
            "Replace the radiator valves",
            "  upstairs  → Boiler"
        ]
    );
}

#[test]
fn a_ring_is_drawn_from_its_note_that_comes_first_and_that_one_keeps_its_arrow() {
    let mut s = session();
    put(&mut s, "pump", "Pump", &["boiler"], 1_000);
    put(&mut s, "seal", "New seal", &["pump"], 2_000);
    put(&mut s, "boiler", "Boiler", &["pump"], 3_000);
    put(&mut s, "oil", "Order oil", &[], 4_000);
    assert_eq!(
        drawn(&s),
        ["Order oil", "Boiler  → Pump", "  Pump", "    New seal"]
    );
    assert_eq!(
        s.query("").len(),
        4,
        "nothing is lost from a command either"
    );
}

#[test]
fn a_note_deeper_than_the_fourth_level_is_drawn_at_it_with_its_arrow() {
    let mut s = session();
    let chain = [
        "Boiler overhaul",
        "[ ] Bleed the radiators",
        "[ ] Buy a radiator key",
        "Ask at the hardware shop",
        "Ring the plumber",
        "Ask for Tuesday",
    ];
    for (at, head) in chain.into_iter().enumerate() {
        let links: Vec<String> = (at > 0)
            .then(|| format!("n{}", at - 1))
            .into_iter()
            .collect();
        let links: Vec<&str> = links.iter().map(String::as_str).collect();
        put(&mut s, &format!("n{at}"), head, &links, 1_000 + at as i64);
    }
    assert_eq!(
        drawn(&s),
        [
            "Boiler overhaul",
            "  [ ] Bleed the radiators",
            "    [ ] Buy a radiator key",
            "      Ask at the hardware shop",
            "      Ring the plumber  → Ask at the hardware shop",
            "      Ask for Tuesday  → Ring the plumber",
        ]
    );
}

#[test]
fn a_note_under_one_not_listed_is_drawn_under_the_nearest_listed_note_above_and_names_it() {
    let mut s = session();
    valves(&mut s, "");
    let mut bleed = s.notes.get("bleed").unwrap().clone();
    bleed.archived = true;
    s.commit(bleed);
    s.refresh(true);
    assert_eq!(
        drawn(&s)[1..5],
        [
            "  upstairs",
            "    Check the pressure first",
            "    [ ] Buy a radiator key  → [ ] Bleed the bedroom radiator",
            "    [ ] Fit the new valve on the landing",
        ],
        "walked through the archived note"
    );
    s.act(Action::ShowArchived);
    assert_eq!(
        drawn(&s)[..HOUSE.len()],
        HOUSE,
        "and under it while it is shown"
    );
}

#[test]
fn being_in_a_place_says_a_note_links_here_and_what_the_drawing_cannot_say_is_said() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &["manual", "plumber"], 1_000);
    put(&mut s, "manual", "the manual", &[], 2_000);
    put(&mut s, "plumber", "the plumber", &["boiler"], 3_000);
    put(&mut s, "pump", "Pump", &["boiler"], 4_000);
    go_to(&mut s, "Boiler");
    let reasons: Vec<(&str, Vec<&str>)> = s
        .rows
        .iter()
        .map(|r| {
            (
                r.head.as_str(),
                r.reasons.iter().map(String::as_str).collect(),
            )
        })
        .collect();
    assert_eq!(
        reasons,
        [
            ("Pump", vec![]),
            ("the plumber", vec!["linked both ways"]),
            ("the manual", vec!["linked from here"]),
        ]
    );
}

#[test]
fn enter_opens_the_first_of_the_notes_under_a_row_or_one_beside_it_and_each_stays_put() {
    let mut s = session();
    valves(&mut s, " #house");
    go_to(&mut s, "#house");

    select(&mut s, "upstairs #house");
    s.act(Action::Write);
    let row = s.selected().unwrap();
    assert_eq!((row.depth, row.under.as_deref()), (2, Some("upstairs")));
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    edited(&mut s, "Drain the system #house");
    let drain = s.field.as_ref().unwrap().id.clone();

    select(&mut s, "downstairs #house");
    s.act(Action::Write);
    edited(&mut s, "kitchen #house");
    s.act(Action::Write);
    edited(&mut s, "utility #house");
    let utility = s.field.as_ref().unwrap().id.clone();
    let written = [
        "Replace the radiator valves",
        "  upstairs",
        "    Drain the system",
        "    Check the pressure first",
        "    [ ] Bleed the bedroom radiator",
        "      [ ] Buy a radiator key",
        "    [ ] Fit the new valve on the landing",
        "  downstairs",
        "  kitchen",
        "  utility",
        "  hallway",
        "  loft",
        "Order heating oil",
    ];
    assert_eq!(drawn(&s), written, "drawn where each was opened");
    assert_eq!(
        remade(&mut s),
        written,
        "and there when the list is made again"
    );
    assert_eq!(s.notes.get(&drain).unwrap().links, ["upstairs"]);
    assert_eq!(s.notes.get(&utility).unwrap().links, ["valves"]);
    assert_eq!(
        s.notes.get(&utility).unwrap().text,
        "utility #house",
        "with the path's labels as ever"
    );

    // A place on another device, made from the notes alone, draws them the same.
    let mut notes = Notes::new();
    for record in s.notes.records() {
        notes.apply(record.clone());
    }
    let mut there = Session::new(
        Box::new(Stopped::default()),
        "3f9a01c2",
        Weights::default(),
        notes,
    );
    go_to(&mut there, "#house");
    assert_eq!(drawn(&there), written);
}

#[test]
fn enter_on_an_empty_last_row_takes_it_out_a_level_where_it_stands() {
    let mut s = session();
    valves(&mut s, " #house");
    go_to(&mut s, "#house");

    // In the middle of the notes beside it, it stays, and nothing is offered for it.
    select(&mut s, "hallway #house");
    s.act(Action::Write);
    assert!(!labels(&s).contains(&"out a level"));
    s.act(Action::Write);
    assert_eq!(s.selected().unwrap().depth, 1);
    s.act(Action::Select(None));

    select(&mut s, "loft #house");
    s.act(Action::Write);
    assert_eq!(s.selected().unwrap().depth, 1);
    assert_eq!(labels(&s)[0], "out a level");
    assert_eq!(s.view().hints[0].action, Action::Write);
    s.act(Action::Write);
    let row = s.selected().unwrap();
    assert_eq!((row.depth, row.under.as_deref()), (0, None));
    assert_eq!(
        drawn(&s)[8..],
        ["  loft", "", "Order heating oil"],
        "where it stood"
    );
    assert!(
        !labels(&s).contains(&"out a level"),
        "at the top there is no level to go out of"
    );
    let undo = s.undo.len();
    s.act(Action::Write);
    assert_eq!(s.undo.len(), undo, "and Enter does nothing, as it did");
    edited(&mut s, "Bleed the boiler #house");
    let boiler = s.field.as_ref().unwrap().id.clone();
    assert!(s.notes.get(&boiler).unwrap().links.is_empty());
    assert_eq!(
        remade(&mut s)[8..],
        ["  loft", "Bleed the boiler", "Order heating oil"]
    );
}

#[test]
fn a_row_taken_out_a_level_after_it_was_typed_in_and_cleared_links_where_it_now_is() {
    let mut s = session();
    valves(&mut s, "");
    select(&mut s, "[ ] Buy a radiator key");
    s.act(Action::Write);
    assert_eq!(s.selected().unwrap().depth, 3);
    edited(&mut s, "a");
    edited(&mut s, "");
    s.act(Action::Write);
    assert_eq!(s.selected().unwrap().depth, 2);
    edited(&mut s, "Open the valve");
    let id = s.field.as_ref().unwrap().id.clone();
    assert_eq!(s.notes.get(&id).unwrap().links, ["upstairs"]);
}

#[test]
fn a_move_goes_among_the_notes_beside_a_row_and_what_is_under_it_follows() {
    let mut s = session();
    valves(&mut s, " #house");
    go_to(&mut s, "#house");

    select(&mut s, "upstairs #house");
    assert!(
        !labels(&s).contains(&"move up"),
        "it is the first under its note"
    );
    assert!(labels(&s).contains(&"move down"));
    s.act(Action::Shift(1));
    assert_eq!(s.selected().unwrap().head, "upstairs #house");
    assert_eq!(
        drawn(&s)[1..7],
        [
            "  downstairs",
            "  upstairs",
            "    Check the pressure first",
            "    [ ] Bleed the bedroom radiator",
            "      [ ] Buy a radiator key",
            "    [ ] Fit the new valve on the landing",
        ]
    );
    let undo = s.undo.len();
    select(&mut s, "[ ] Buy a radiator key #house");
    assert!(!labels(&s).iter().any(|l| l.starts_with("move")));
    assert!(kept(&s.act(Action::Shift(-1))).is_empty());
    assert_eq!(s.undo.len(), undo, "nothing beside it to move past");

    // At the top a note moves past whole notes and all under them.
    select(&mut s, "Order heating oil #house");
    assert!(labels(&s).contains(&"move up") && !labels(&s).contains(&"move down"));
    s.act(Action::Shift(-1));
    assert_eq!(
        remade(&mut s),
        [
            "Order heating oil",
            "Replace the radiator valves",
            "  downstairs",
            "  upstairs",
            "    Check the pressure first",
            "    [ ] Bleed the bedroom radiator",
            "      [ ] Buy a radiator key",
            "    [ ] Fit the new valve on the landing",
            "  hallway",
            "  loft",
        ]
    );
}

#[test]
fn the_rows_under_a_note_ticked_come_up_a_level_where_they_stand_as_the_list_would_have_them() {
    let mut s = session();
    valves(&mut s, " #house");
    go_to(&mut s, "#house");
    select(&mut s, "[ ] Bleed the bedroom radiator #house");
    s.act(Action::Archive);
    let ticked = [
        "  upstairs",
        "    Check the pressure first",
        "    [ ] Buy a radiator key  → [x] Bleed the bedroom radiator",
        "    [ ] Fit the new valve on the landing",
    ];
    assert_eq!(drawn(&s)[1..5], ticked);
    assert_eq!(
        s.selected().unwrap().head,
        "[ ] Buy a radiator key #house",
        "the cursor on the row that took its place"
    );
    assert_eq!(remade(&mut s)[1..5], ticked);
}

#[test]
fn the_rows_under_a_note_archived_or_deleted_come_up_a_level_where_they_stand() {
    let mut s = session();
    valves(&mut s, " #house");
    go_to(&mut s, "#house");
    select(&mut s, "upstairs #house");
    s.act(Action::Archive);
    let archived = [
        "Replace the radiator valves",
        "  Check the pressure first  → upstairs",
        "  [ ] Bleed the bedroom radiator  → upstairs",
        "    [ ] Buy a radiator key",
        "  [ ] Fit the new valve on the landing  → upstairs",
        "  downstairs",
    ];
    assert_eq!(drawn(&s)[..6], archived);
    assert_eq!(remade(&mut s)[..6], archived);
    s.act(Action::Undo);
    assert_eq!(remade(&mut s), HOUSE);

    select(&mut s, "upstairs #house");
    s.act(Action::Delete);
    assert_eq!(
        drawn(&s)[..6],
        [
            "Replace the radiator valves",
            "  Check the pressure first",
            "  [ ] Bleed the bedroom radiator",
            "    [ ] Buy a radiator key",
            "  [ ] Fit the new valve on the landing",
            "  downstairs",
        ],
        "with no arrow to a note that is gone"
    );
    // Made again, what was under it is under nothing it was, and at the top.
    let again = remade(&mut s);
    assert!(
        again.contains(&"Check the pressure first".to_owned()),
        "{again:?}"
    );
    s.act(Action::Undo);
    assert_eq!(remade(&mut s), HOUSE, "undo puts them back under it");
}

#[test]
fn a_command_lists_the_notes_in_the_order_the_app_draws_them() {
    let mut s = session();
    valves(&mut s, " #house");
    let heads: Vec<String> = s
        .query("#house")
        .iter()
        .map(|r| r.head().replace(" #house", ""))
        .collect();
    let drawn: Vec<&str> = HOUSE.iter().map(|row| row.trim_start()).collect();
    assert_eq!(heads, drawn);
}

/// A space of notes, each as its id, its links, whether it is archived and the note it is placed
/// after, opened on a session.
fn space(notes: &[(String, Vec<String>, bool, Option<String>)]) -> Session {
    let mut kept = Notes::new();
    for (at, (id, links, archived, after)) in notes.iter().enumerate() {
        kept.apply(Record {
            links: links.clone(),
            archived: *archived,
            after: after.clone(),
            created: Some(Moment::utc(1_000 + at as i64)),
            updated: Some(Moment::utc(1_000 + at as i64)),
            ..Record::new(id.clone(), format!("note {id}"))
        });
    }
    Session::new(
        Box::new(Stopped::default()),
        "dev",
        Weights::default(),
        kept,
    )
}

/// The tree a session draws of what is not archived and `holds` keeps, from `root`, cut at
/// `limit`: each note's id, depth, the note it is under and the note it stands at.
fn drawn_tree(
    s: &Session,
    holds: fn(&str) -> bool,
    root: Option<&str>,
    limit: usize,
) -> Vec<(String, usize, Option<String>, String)> {
    let listed = |r: &Record| !r.deleted && !r.archived && holds(&r.id);
    s.tree(&listed, root, limit)
        .into_iter()
        .map(|(r, level, under, stands): Drawn| {
            let under = under.map(|u| u.id.clone());
            (r.id.clone(), level, under, stands.id.clone())
        })
        .collect()
}

/// Asserts that a list cut anywhere draws what the whole list draws up to there.
fn cut_as_whole(s: &Session, holds: fn(&str) -> bool, root: Option<&str>) {
    let whole = drawn_tree(s, holds, root, usize::MAX);
    for k in 1..=whole.len() + 2 {
        let cut = drawn_tree(s, holds, root, k);
        assert_eq!(cut, whole[..k.min(whole.len())], "cut at {k}");
    }
}

#[test]
fn a_list_cut_anywhere_draws_what_the_whole_list_draws_up_to_there() {
    // A listed note under an archived one, which links back to it before the note it is under.
    let note = |id: &str, links: &[&str], archived: bool| {
        let links = links.iter().map(|&l| l.to_owned()).collect();
        (id.to_owned(), links, archived, None)
    };
    let s = space(&[
        note("n5", &[], false),
        note("n1", &[], true),
        note("n7", &["n2", "n1", "n5"], true),
        note("n2", &["n7"], false),
        note("n3", &["n2"], false),
    ]);
    assert_eq!(drawn_tree(&s, |_| true, None, usize::MAX).len(), 3);
    cut_as_whole(&s, |_| true, None);

    // Spaces made at random, with archived notes, rings, notes under several notes and notes
    // placed after others, cut anywhere, at the top and from a note, with and without a line.
    let mut seed = 7_u64;
    let mut next = move |n: u64| {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) % n
    };
    let some: fn(&str) -> bool = |id| !id.ends_with('3');
    for _ in 0..400 {
        let n = 4 + next(14);
        let mut notes = Vec::new();
        for i in 0..n {
            let mut links: Vec<String> = Vec::new();
            for _ in 0..next(4) {
                let link = format!("n{}", next(n));
                if link != format!("n{i}") && !links.contains(&link) {
                    links.push(link);
                }
            }
            let after = (next(3) == 0).then(|| format!("n{}", next(n)));
            notes.push((format!("n{i}"), links, next(4) == 0, after));
        }
        let s = space(&notes);
        let root = format!("n{}", next(n));
        for holds in [|_: &str| true, some] {
            cut_as_whole(&s, holds, None);
            cut_as_whole(&s, holds, Some(&root));
        }
    }
}

/// The radiators in `#house` with the bleeding ticked, so that the key stands where it stood.
fn ticked(s: &mut Session) {
    valves(s, " #house");
    go_to(s, "#house");
    select(s, "[ ] Bleed the bedroom radiator #house");
    s.act(Action::Archive);
    s.act(Action::Select(None));
}

#[test]
fn a_move_past_a_row_standing_where_a_ticked_note_stood_goes_past_it() {
    let mut s = session();
    ticked(&mut s);
    select(&mut s, "[ ] Fit the new valve on the landing #house");
    s.act(Action::Shift(-1));
    assert_ne!(s.notes.get("fit").unwrap().after.as_deref(), Some("fit"));
    assert_eq!(s.selected().unwrap().id, "fit");
    let up = [
        "    Check the pressure first",
        "    [ ] Fit the new valve on the landing",
        "    [ ] Buy a radiator key  → [x] Bleed the bedroom radiator",
    ];
    assert_eq!(drawn(&s)[2..5], up);
    assert_eq!(remade(&mut s)[2..5], up);

    // The row standing where the ticked note stood moves by it.
    select(&mut s, "[ ] Buy a radiator key #house");
    s.act(Action::Shift(-1));
    assert_eq!(s.status, "moved");
    assert_eq!(s.selected().unwrap().id, "key");
    let up = [
        "    Check the pressure first",
        "    [ ] Buy a radiator key  → [x] Bleed the bedroom radiator",
        "    [ ] Fit the new valve on the landing",
    ];
    assert_eq!(remade(&mut s)[2..5], up);
    select(&mut s, "[ ] Buy a radiator key #house");
    s.act(Action::Shift(1));
    assert_eq!(
        remade(&mut s)[3],
        "    [ ] Fit the new valve on the landing"
    );
}

#[test]
fn a_row_opened_beside_one_standing_where_a_ticked_note_stood_stays_there() {
    let mut s = session();
    ticked(&mut s);
    select(&mut s, "[ ] Buy a radiator key #house");
    s.act(Action::Write);
    edited(&mut s, "Drain it #house");
    let written = [
        "    Check the pressure first",
        "    [ ] Buy a radiator key  → [x] Bleed the bedroom radiator",
        "    Drain it",
        "    [ ] Fit the new valve on the landing",
    ];
    assert_eq!(drawn(&s)[2..6], written);
    assert_eq!(remade(&mut s)[2..6], written);
}

#[test]
fn a_note_written_under_one_the_place_only_links_to_is_in_the_place_too() {
    let mut s = session();
    put(&mut s, "pump", "Pump", &[], 1_000);
    put(&mut s, "seal", "New seal", &["pump"], 2_000);
    put(&mut s, "boiler", "Boiler", &["pump", "seal"], 3_000);
    go_to(&mut s, "Boiler");
    assert_eq!(drawn(&s), ["Pump", "  New seal"]);
    select(&mut s, "Pump");
    s.act(Action::Write);
    edited(&mut s, "Gasket");
    let gasket = s.field.as_ref().unwrap().id.clone();
    assert_eq!(s.notes.get(&gasket).unwrap().links, ["pump", "boiler"]);
    assert_eq!(remade(&mut s), ["Pump", "  Gasket", "  New seal"]);

    // Under a note that is under the place, it links to that note alone.
    put(&mut s, "valve", "Valve", &["boiler"], 4_000);
    put(&mut s, "washer", "Washer", &["valve"], 5_000);
    select(&mut s, "Valve");
    s.act(Action::Write);
    edited(&mut s, "Order a spare");
    let spare = s.field.as_ref().unwrap().id.clone();
    assert_eq!(s.notes.get(&spare).unwrap().links, ["valve"]);
}

#[test]
fn a_row_being_written_under_a_note_deleted_elsewhere_is_written_under_what_that_was_under() {
    let mut s = session();
    valves(&mut s, "");
    select(&mut s, "loft");
    s.act(Action::Write);
    assert_eq!(s.selected().unwrap().under.as_deref(), Some("valves"));
    let loft = s.notes.get("loft").unwrap().clone();
    let downstairs = s.notes.get("downstairs").unwrap().clone();
    // The note the new row is under goes, and so does its row.
    let mut gone = theirs("valves", "Replace the radiator valves", 60);
    gone.deleted = true;
    s.absorb([gone]);
    assert_eq!(s.selected().unwrap().under, None);
    edited(&mut s, "Bleed the boiler");
    let id = s.field.as_ref().unwrap().id.clone();
    assert!(s.notes.get(&id).unwrap().links.is_empty());
    assert!(loft.links == ["valves"] && downstairs.links == ["valves"]);
}
