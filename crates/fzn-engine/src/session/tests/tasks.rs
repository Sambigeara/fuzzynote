//! Tasks: the box as a label and a place, and ticking, under the cursor or by id.

use super::*;

/// Two open tasks in `#house` and one outside it, one done and one archived without its tick,
/// and a note with a task in its body, which is not one.
fn tasks(s: &mut Session) {
    put(s, "oil", "[ ] Order oil #house", &[], 1_000);
    put(s, "radiators", "[ ] Bleed the radiators #house", &[], 2_000);
    put(s, "chimney", "[x] Sweep the chimney #house", &[], 3_000);
    put(s, "shed", "[ ] Paint the shed #house", &[], 4_000);
    put(s, "stamps", "[ ] Buy stamps", &[], 5_000);
    put(
        s,
        "boiler",
        "Boiler #house\n- [ ] get three quotes",
        &[],
        6_000,
    );
    for id in ["chimney", "shed"] {
        let held = s.notes.get(id).unwrap().clone();
        s.commit(Record {
            archived: true,
            ..held
        });
    }
    s.refresh(true);
}

#[test]
fn the_box_on_the_line_holds_the_open_tasks_and_the_rest_of_the_line_still_applies() {
    let mut s = session();
    tasks(&mut s);
    let open = [
        "[ ] Buy stamps",
        "[ ] Bleed the radiators #house",
        "[ ] Order oil #house",
    ];
    for typed in ["[ ]", "[ ] a"] {
        clear(&mut s);
        type_in(&mut s, typed);
        assert_eq!(heads(&s), open, "{typed:?}");
    }
    clear(&mut s);
    type_in(&mut s, "[ ] #house");
    assert_eq!(heads(&s), open[1..]);
    clear(&mut s);
    type_in(&mut s, "[ ] oil");
    assert_eq!(heads(&s), ["[ ] Order oil #house"]);
    let exported: Vec<&str> = s.query("[ ]").iter().map(|r| r.id.as_str()).collect();
    assert_eq!(exported, ["stamps", "radiators", "oil"]);
}

#[test]
fn the_box_is_a_place_and_a_new_row_there_opens_with_it_before_the_caret() {
    let mut s = session();
    tasks(&mut s);
    type_in(&mut s, "#house [ ]");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "[ ]"]);
    assert_eq!(
        heads(&s),
        ["[ ] Bleed the radiators #house", "[ ] Order oil #house"]
    );

    // The box first, then the caret, then the other labels.
    assert!(s.act(Action::Write).is_empty());
    let fresh = |s: &Session| s.view().field.map(|f| (f.text.to_owned(), f.fresh));
    assert_eq!(fresh(&s), Some(("[ ]  #house".to_owned(), Some(4))));
    for left in ["[ ] ", "[ ] #house", "[ ]   #house", "[ ]  #ho", "[", ""] {
        assert!(
            edited(&mut s, left).is_empty(),
            "nothing of the writer's: {left:?}"
        );
    }
    let typed = edited(&mut s, "[ ] B #house");
    let [Effect::Keep(kept)] = &typed[..] else {
        panic!("one version: {typed:?}");
    };
    assert_eq!(kept.text, "[ ] B #house");
    s.act(Action::Undo);
    assert_eq!(fresh(&s), Some(("[ ]  #house".to_owned(), Some(4))));
    s.act(Action::Erase);
    assert_eq!(s.cursor, None, "Backspace where it opened takes it away");

    type_in(&mut s, "Fix the gate");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(s.notes.get(&new).unwrap().text, "[ ] Fix the gate #house");
    s.act(Action::Select(None));
    type_in(&mut s, "[ ] Prune the roses");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(
        s.notes.get(&new).unwrap().text,
        "[ ] Prune the roses #house",
        "never two boxes"
    );

    // Gone into the other way round, it is one place all the same.
    s.act(Action::Select(None));
    s.act(Action::Back);
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "[ ]");
    s.act(Action::GoIn);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["[ ]", "#house"]);
    s.act(Action::Write);
    assert_eq!(fresh(&s), Some(("[ ]  #house".to_owned(), Some(4))));
}

#[test]
fn while_archived_notes_are_shown_the_open_box_holds_the_done_tasks_among_the_open_ones() {
    let mut s = session();
    tasks(&mut s);
    let open = ["[ ] Bleed the radiators #house", "[ ] Order oil #house"];
    let every = [
        "[ ] Paint the shed #house",
        "[x] Sweep the chimney #house",
        "[ ] Bleed the radiators #house",
        "[ ] Order oil #house",
    ];
    go_to(&mut s, "#house [ ]");
    assert_eq!(path(&s), ["#house", "[ ]"]);
    assert_eq!(heads(&s), open);
    s.act(Action::ShowArchived);
    assert_eq!(heads(&s), every, "in the space's order");
    let archived: Vec<bool> = s.rows.iter().map(|r| r.archived).collect();
    assert_eq!(archived, [true, true, false, false]);
    s.act(Action::ShowArchived);
    assert_eq!(heads(&s), open, "and gone again once archived is hidden");

    // On the line at the top it is the same, typed either way, and words find among them.
    s.act(Action::Back);
    s.act(Action::Back);
    s.act(Action::ShowArchived);
    for typed in ["[ ] #house", "/task #house"] {
        clear(&mut s);
        type_in(&mut s, typed);
        assert_eq!(heads(&s), every, "{typed:?}");
    }
    clear(&mut s);
    type_in(&mut s, "[ ] chimney");
    assert_eq!(heads(&s), ["[x] Sweep the chimney #house"]);
}

#[test]
fn the_open_box_typed_inside_a_place_narrows_to_every_task_there_while_archived_notes_are_shown() {
    let mut s = session();
    tasks(&mut s);
    go_to(&mut s, "#house");
    s.act(Action::ShowArchived);
    type_in(&mut s, "[ ]");
    assert_eq!(
        heads(&s),
        [
            "[ ] Paint the shed #house",
            "[x] Sweep the chimney #house",
            "[ ] Bleed the radiators #house",
            "[ ] Order oil #house",
        ]
    );
    s.act(Action::ShowArchived);
    assert_eq!(
        heads(&s),
        ["[ ] Bleed the radiators #house", "[ ] Order oil #house"]
    );
}

#[test]
fn the_open_box_at_the_top_lists_the_done_tasks_while_archived_is_shown_and_not_once_hidden() {
    let mut s = session();
    tasks(&mut s);
    type_in(&mut s, "[ ] #house");
    s.act(Action::ShowArchived);
    assert!(heads(&s).contains(&"[x] Sweep the chimney #house"));
    s.act(Action::ShowArchived);
    assert_eq!(
        heads(&s),
        ["[ ] Bleed the radiators #house", "[ ] Order oil #house"]
    );
}

#[test]
fn a_box_the_writer_typed_stays_as_typed_whichever_box_the_place_has_and_whatever_is_shown() {
    let mut s = session();
    tasks(&mut s);
    let held = |s: &Session, head: &str| {
        let r = s
            .notes
            .order()
            .into_iter()
            .find(|r| r.head() == head)
            .unwrap();
        (r.archived, r.done.is_some())
    };

    // Ticked in the open tasks, with archived hidden it is done and goes from the list.
    go_to(&mut s, "#house [ ]");
    type_in(&mut s, "[x] Called the plumber");
    s.act(Action::Write);
    assert_eq!(held(&s, "[x] Called the plumber #house"), (true, true));
    assert!(!heads(&s).contains(&"[x] Called the plumber #house"));

    // With archived shown it is done the same, and stays, ticked.
    s.act(Action::ShowArchived);
    type_in(&mut s, "[x] Drained the tank");
    s.act(Action::Write);
    assert_eq!(held(&s, "[x] Drained the tank #house"), (true, true));
    let row = s.selected().unwrap();
    assert_eq!(
        (row.head.as_str(), row.task),
        ("[x] Drained the tank #house", Some(true))
    );
    s.act(Action::ShowArchived);

    // Open in the done tasks, it stays open.
    s.act(Action::Select(None));
    s.act(Action::Back);
    s.act(Action::Back);
    go_to(&mut s, "#house [x]");
    type_in(&mut s, "[ ] Clean the windows");
    s.act(Action::Write);
    assert_eq!(held(&s, "[ ] Clean the windows #house"), (false, false));
}

#[test]
fn a_task_archived_with_its_box_open_is_listed_in_the_open_box_while_archived_notes_are_shown() {
    let mut s = session();
    tasks(&mut s);
    type_in(&mut s, "[ ]");
    assert!(!heads(&s).contains(&"[ ] Paint the shed #house"));
    s.act(Action::ShowArchived);
    assert!(heads(&s).contains(&"[ ] Paint the shed #house"));
}

#[test]
fn ticking_in_the_open_box_while_archived_notes_are_shown_leaves_the_row_where_it_is_ticked() {
    let mut s = session();
    tasks(&mut s);
    go_to(&mut s, "#house [ ]");
    select(&mut s, "[ ] Order oil #house");
    s.act(Action::Archive);
    assert_eq!(
        heads(&s),
        ["[ ] Bleed the radiators #house"],
        "hidden, it goes as it always did"
    );

    s.act(Action::ShowArchived);
    select(&mut s, "[ ] Bleed the radiators #house");
    let at = s.cursor;
    s.act(Action::Archive);
    assert_eq!(s.status, "ticked");
    assert_eq!(s.cursor, at, "the cursor stays on it");
    let row = s.selected().unwrap();
    assert_eq!(
        (row.head.as_str(), row.task, row.archived),
        ("[x] Bleed the radiators #house", Some(true), true)
    );
    s.act(Action::Select(None));
    assert_eq!(
        heads(&s),
        [
            "[ ] Paint the shed #house",
            "[x] Sweep the chimney #house",
            "[x] Bleed the radiators #house",
            "[x] Order oil #house",
        ]
    );
}

#[test]
fn a_command_asking_for_the_open_box_gets_only_the_open_tasks_whatever_the_face_shows() {
    let mut s = session();
    tasks(&mut s);
    s.act(Action::ShowArchived);
    let written: Vec<&str> = s
        .query("[ ] #house")
        .iter()
        .map(|r| r.id.as_str())
        .collect();
    assert_eq!(written, ["radiators", "oil"]);
}

#[test]
fn the_done_box_lists_archived_notes_without_making_the_open_box_every_task() {
    let mut s = session();
    tasks(&mut s);
    type_in(&mut s, "[ ] [x]");
    assert!(heads(&s).is_empty(), "no note is both open and done");
    go_to(&mut s, "[x]");
    type_in(&mut s, "[ ]");
    assert!(heads(&s).is_empty(), "nor in the done tasks");

    // With archived shown the open box is every task, so beside the done box it is those.
    s.act(Action::ShowArchived);
    assert_eq!(heads(&s), ["[x] Sweep the chimney #house"]);
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "[ ] [x]");
    assert_eq!(heads(&s), ["[x] Sweep the chimney #house"]);
}

#[test]
fn ticking_a_task_writes_its_box_and_archives_it_in_one_version_and_undo_puts_back_both() {
    let mut s = session();
    tasks(&mut s);
    let mut log = Vec::new();
    select(&mut s, "[ ] Order oil #house");
    assert!(labels(&s).contains(&"tick"));
    let ticked = s.act(Action::Archive);
    let [Effect::Keep(kept)] = &ticked[..] else {
        panic!("one version: {ticked:?}");
    };
    assert_eq!(
        (kept.text.as_str(), kept.archived),
        ("[x] Order oil #house", true)
    );
    assert_eq!(
        kept.updated,
        Some(NOON),
        "the box is in the text, so it is an edit"
    );
    assert_eq!(s.status, "ticked");
    assert!(!heads(&s).contains(&"[x] Order oil #house"));
    log.extend(ticked);

    let undone = s.act(Action::Undo);
    let [Effect::Keep(kept)] = &undone[..] else {
        panic!("one version: {undone:?}");
    };
    assert_eq!(
        (kept.text.as_str(), kept.archived),
        ("[ ] Order oil #house", false)
    );
    assert_eq!(kept.updated, Some(Moment::utc(1_000)));
    log.extend(undone);
    log.extend(s.act(Action::Redo));
    for notes in [reopened(&log), reopened(log.iter().rev())] {
        let oil = notes.get("oil").unwrap();
        assert_eq!(
            (oil.text.as_str(), oil.archived),
            ("[x] Order oil #house", true)
        );
    }

    // Shown, a done task is unticked as it is brought back, and so is one archived without
    // its tick.
    s.act(Action::Select(None));
    s.act(Action::ShowArchived);
    for (head, unticked) in [
        ("[x] Order oil #house", "[ ] Order oil #house"),
        ("[ ] Paint the shed #house", "[ ] Paint the shed #house"),
    ] {
        select(&mut s, head);
        assert!(labels(&s).contains(&"untick"));
        s.act(Action::Archive);
        assert_eq!(s.status, "unticked");
        let id = s.selected().unwrap().id.clone();
        let held = s.notes.get(&id).unwrap();
        assert_eq!((held.text.as_str(), held.archived), (unticked, false));
        s.act(Action::Select(None));
    }

    // Anything else is archived as it always was, a box in its body or not.
    select(&mut s, "Boiler #house");
    assert!(labels(&s).contains(&"archive"));
    s.act(Action::Archive);
    assert_eq!(s.status, "archived");
    assert_eq!(
        s.notes.get("boiler").unwrap().text,
        "Boiler #house\n- [ ] get three quotes"
    );
}

#[test]
fn ticking_by_id_writes_what_ctrl_v_writes_and_nothing_unless_every_id_can_be() {
    let ids = |ids: &[&str]| -> Vec<String> { ids.iter().map(|&id| id.to_owned()).collect() };
    let mut by_key = session();
    tasks(&mut by_key);
    select(&mut by_key, "[ ] Order oil #house");
    let mut s = session();
    tasks(&mut s);
    clear(&mut s);
    assert_eq!(
        s.tick(&ids(&["oil"]), true),
        Ok(by_key.act(Action::Archive)),
        "the version Ctrl-V writes"
    );

    // An id that names nothing, a note that is not a task, and a task that archiving would
    // untick rather than tick, as it would the shed, its box open but archived, or the other
    // way round: each is said, and nothing else on the list is written.
    for (list, done, why) in [
        (
            &["radiators", "gutter"][..],
            true,
            "no note has the id gutter",
        ),
        (&["radiators", "boiler"], true, "boiler is not an open task"),
        (&["radiators", "shed"], true, "shed is not an open task"),
        (
            &["chimney", "radiators"],
            false,
            "radiators is not a done task",
        ),
    ] {
        assert_eq!(s.tick(&ids(list), done), Err(why.to_owned()));
        assert_eq!(
            s.notes.get("radiators").unwrap().text,
            "[ ] Bleed the radiators #house"
        );
    }

    // One version a note, however often it is named; and back.
    let named = ["radiators", "chimney", "radiators", "shed"];
    let ticked = s.tick(&ids(&named[..1]), true).unwrap();
    assert_eq!(ticked.len(), 1);
    let unticked = s.tick(&ids(&named), false).unwrap();
    let written: Vec<(&str, bool)> = unticked
        .iter()
        .map(|effect| match effect {
            Effect::Keep(version) => (version.text.as_str(), version.archived),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        written,
        [
            ("[ ] Bleed the radiators #house", false),
            ("[ ] Sweep the chimney #house", false),
            ("[ ] Paint the shed #house", false),
        ]
    );
    assert!(heads(&s).contains(&"[ ] Bleed the radiators #house"));
}

#[test]
fn a_row_says_whether_its_note_is_a_task_and_whether_it_is_ticked() {
    let mut s = session();
    put(&mut s, "oil", "[ ] Order oil", &[], 1_000);
    put(&mut s, "bled", "[x] Bleed the radiators", &[], 2_000);
    put(
        &mut s,
        "boiler",
        "Boiler\n[ ] a box in a body is not a task",
        &[],
        3_000,
    );
    let task = |s: &Session, id: &str| s.rows.iter().find(|r| r.id == id).unwrap().task;
    assert_eq!(task(&s, "oil"), Some(false));
    assert_eq!(task(&s, "bled"), Some(true));
    assert_eq!(task(&s, "boiler"), None);

    // Ticked under the cursor, the row says so at once.
    select(&mut s, "[ ] Order oil");
    s.act(Action::Archive);
    s.act(Action::ShowArchived);
    assert_eq!(task(&s, "oil"), Some(true));
}

#[test]
fn the_done_box_on_the_line_lists_the_done_tasks_though_they_are_archived() {
    let mut s = session();
    tasks(&mut s);
    put(&mut s, "gate", "[x] Mend the gate", &[], 7_000);
    clear(&mut s);
    assert!(!heads(&s).contains(&"[x] Sweep the chimney #house"));
    type_in(&mut s, "[x]");
    assert_eq!(
        heads(&s),
        ["[x] Mend the gate", "[x] Sweep the chimney #house"],
        "archived or not, and not the shed, archived with its box open"
    );
    clear(&mut s);
    type_in(&mut s, "[x] #house");
    assert_eq!(heads(&s), ["[x] Sweep the chimney #house"]);
    clear(&mut s);
    type_in(&mut s, "[x] chimney");
    assert_eq!(
        heads(&s),
        ["[x] Sweep the chimney #house"],
        "words find among them"
    );
    clear(&mut s);
    type_in(&mut s, "\"[x]\"");
    assert_eq!(
        heads(&s),
        ["[x] Mend the gate"],
        "the phrase still asks nothing more"
    );
    let exported: Vec<&str> = s.query("[x]").iter().map(|r| r.id.as_str()).collect();
    assert_eq!(exported, ["gate", "chimney"]);
}

#[test]
fn the_done_box_is_a_place_and_a_note_written_there_is_a_task_done_now() {
    let mut s = session();
    tasks(&mut s);
    type_in(&mut s, "#house [x]");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "[x]"]);
    assert_eq!(heads(&s), ["[x] Sweep the chimney #house"]);

    s.act(Action::Write);
    let fresh = |s: &Session| s.view().field.map(|f| (f.text.to_owned(), f.fresh));
    assert_eq!(fresh(&s), Some(("[x]  #house".to_owned(), Some(4))));
    let typed = edited(&mut s, "[x] F #house");
    let [Effect::Keep(kept)] = &typed[..] else {
        panic!("one version: {typed:?}");
    };
    assert_eq!(
        (kept.text.as_str(), kept.archived, kept.done),
        ("[x] F #house", true, kept.stamp),
        "done from its first version"
    );
    edited(&mut s, "[x] Fixed the gate latch #house");
    s.act(Action::Select(None));
    assert!(heads(&s).contains(&"[x] Fixed the gate latch #house"));

    // Written from the line it is the same, and so in the box alone.
    type_in(&mut s, "Swept the patio");
    s.act(Action::Write);
    let id = s.selected().unwrap().id.clone();
    let held = s.notes.get(&id).unwrap();
    assert_eq!(
        (held.text.as_str(), held.archived, held.done),
        ("[x] Swept the patio #house", true, held.stamp)
    );
    s.act(Action::Select(None));
    s.act(Action::Back);
    s.act(Action::Back);
    go_to(&mut s, "[x]");
    assert_eq!(path(&s), ["[x]"]);
    s.act(Action::Write);
    assert_eq!(fresh(&s), Some(("[x] ".to_owned(), Some(4))));

    // Among the open tasks a note written is open, as it always was.
    s.act(Action::Select(None));
    s.act(Action::Back);
    go_to(&mut s, "[ ]");
    type_in(&mut s, "Clean the windows");
    s.act(Action::Write);
    let id = s.selected().unwrap().id.clone();
    let held = s.notes.get(&id).unwrap();
    assert_eq!((held.archived, held.done), (false, None));
}

#[test]
fn a_new_note_headed_by_a_ticked_box_is_done_wherever_it_is_written_and_only_while_it_is() {
    let mut s = session();
    tasks(&mut s);
    let done = |s: &Session, id: &str| {
        let held = s.notes.get(id).unwrap();
        (held.archived, held.done.is_some())
    };

    // Typed on the line at the top and written with Enter.
    type_in(&mut s, "[x] Called the plumber");
    let written = s.act(Action::Write);
    let called = kept(&written)[0].id.clone();
    assert_eq!(done(&s, &called), (true, true));
    assert!(
        !heads(&s).contains(&"[x] Called the plumber"),
        "done, so archived, and out of the list until asked for"
    );

    // In a new row at the top, from its first version, and not once the box is taken out
    // before the cursor leaves.
    s.act(Action::Select(None));
    s.act(Action::Write);
    edited(&mut s, "[x] W");
    let washed = s.selected().unwrap().id.clone();
    assert_eq!(done(&s, &washed), (true, true));
    edited(&mut s, "Washed the car");
    assert_eq!(done(&s, &washed), (false, false));
    s.act(Action::Select(None));
    assert_eq!(
        heads(&s)[0],
        "Washed the car",
        "listed at the top, as an open note is"
    );

    // In the done tasks, where the row opens with the box, the same.
    go_to(&mut s, "[x]");
    s.act(Action::Write);
    edited(&mut s, "[x] S");
    let id = s.selected().unwrap().id.clone();
    assert_eq!(done(&s, &id), (true, true));
    edited(&mut s, "S");
    assert_eq!(done(&s, &id), (false, false));
    s.act(Action::Select(None));
    s.act(Action::Back);
    clear(&mut s);
    assert_eq!(heads(&s)[0], "S");

    // A note written before is ticked only by the key: a box typed into it changes its text.
    select(&mut s, "Washed the car");
    edited(&mut s, "[x] Washed the car");
    s.act(Action::Select(None));
    assert_eq!(done(&s, &washed), (false, false));
}

#[test]
fn a_line_of_both_boxes_names_no_place() {
    let mut s = session();
    tasks(&mut s);
    type_in(&mut s, "[x] [ ]");
    assert!(!labels(&s).contains(&"go in"));
    s.act(Action::GoIn);
    assert!(path(&s).is_empty());
}

#[test]
fn two_devices_agree_on_when_a_task_was_done_whatever_order_their_versions_arrive_in() {
    let at = |secs: i64| Some(NOON.plus_secs(secs));
    let version =
        |text: &str, archived: bool, done: Option<Moment>, secs: i64, device: &str| Record {
            text: text.into(),
            archived,
            done,
            updated: at(secs),
            stamp: at(secs),
            device: device.into(),
            ..theirs("oil", "[ ] Order oil", 0)
        };
    let ticked = version("[x] Order oil", true, at(10), 10, "dev-a");
    let unticked = version("[ ] Order oil", false, None, 20, "dev-b");
    let edited = version("[ ] Order heating oil", false, None, 20, "dev-b");
    let ticked_later = version("[x] Order oil", true, at(30), 30, "dev-a");
    // A build from before `done` ticking it later still, saying nothing of when.
    let old = Record::read(
        format!(
            r#"{{"v":3,"id":"oil","text":"[x] Order oil","created":"2026-10-13T12:00:00Z","updated":"{0}","stamp":"{0}","archived":true,"device":"dev-c"}}"#,
            "2026-10-13T12:00:40Z"
        )
        .as_bytes(),
    )
    .unwrap();
    for (pair, text, archived, done) in [
        ([&ticked, &unticked], "[ ] Order oil", false, None),
        ([&ticked, &edited], "[ ] Order heating oil", false, None),
        ([&edited, &ticked_later], "[x] Order oil", true, at(30)),
        ([&ticked, &old], "[x] Order oil", true, at(40)),
    ] {
        for order in [[pair[0], pair[1]], [pair[1], pair[0]]] {
            let mut s = session();
            s.absorb([theirs("oil", "[ ] Order oil", 0)]);
            s.absorb(order.map(Record::clone));
            let held = s.notes.get("oil").unwrap();
            assert_eq!(
                (held.text.as_str(), held.archived, held.done),
                (text, archived, done),
                "{:?}",
                order.map(|r| &r.device)
            );
            assert!(
                held.archived || held.done.is_none(),
                "never done while not archived"
            );
        }
    }
}

#[test]
fn a_span_with_the_done_box_holds_the_tasks_done_in_it_whenever_they_were_written() {
    let mut s = session();
    let weeks_ago = |weeks: i64| NOON.secs() - weeks * 7 * 86_400;
    put(
        &mut s,
        "fence",
        "[ ] Fix the fence #work",
        &[],
        weeks_ago(3),
    );
    put(
        &mut s,
        "gate",
        "[ ] Paint the gate #work",
        &[],
        weeks_ago(3),
    );
    put(&mut s, "shed", "[ ] Tidy the shed", &[], weeks_ago(3));
    put(
        &mut s,
        "report",
        "Wrote the report #work",
        &[],
        NOON.secs() - 3_600,
    );
    // The gate was ticked last week, the fence and the shed now.
    let gate = s.notes.get("gate").unwrap().clone();
    s.commit(Record {
        text: "[x] Paint the gate #work".into(),
        archived: true,
        done: Some(Moment::utc(weeks_ago(1))),
        ..gate
    });
    for id in ["fence", "shed"] {
        assert!(s.tick(&[id.to_owned()], true).is_ok());
    }
    clear(&mut s);
    type_in(&mut s, "[x] /-mon.. #work ");
    assert_eq!(heads(&s), ["[x] Fix the fence #work"]);
    type_in(&mut s, "fence");
    assert_eq!(
        heads(&s),
        ["[x] Fix the fence #work"],
        "and with words, ranked"
    );
    let exported: Vec<&str> = s
        .query("[x] /-mon..")
        .iter()
        .map(|r| r.id.as_str())
        .collect();
    assert_eq!(exported.len(), 2);
    assert!(!exported.contains(&"gate"));

    // Without the box a span is when notes were written, archived or not.
    clear(&mut s);
    s.act(Action::ShowArchived);
    type_in(&mut s, "/-mon.. #work");
    assert_eq!(heads(&s), ["Wrote the report #work"]);
    s.act(Action::ShowArchived);

    // Inside the done tasks a span measures when they were done too.
    go_to(&mut s, "[x]");
    type_in(&mut s, "/-mon..");
    assert_eq!(heads(&s).len(), 2);
    assert!(!heads(&s).contains(&"[x] Paint the gate #work"));

    // A box ticked by hand and never archived was done when it last changed, at the latest.
    s.act(Action::Back);
    clear(&mut s);
    put(&mut s, "hedge", "[x] Trim the hedge", &[], NOON.secs() - 60);
    type_in(&mut s, "[x] /today..");
    assert!(heads(&s).contains(&"[x] Trim the hedge"));
}

#[test]
fn ticking_sets_when_the_task_was_done_an_edit_keeps_it_and_unticking_clears_it() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Order oil #house");
    let ticked = s.act(Action::Archive);
    let [Effect::Keep(kept)] = &ticked[..] else {
        panic!("one version: {ticked:?}");
    };
    let done = kept.done;
    assert!(done.is_some());
    assert_eq!(done, kept.stamp, "the moment of the version that ticked it");

    // Edited while done, it was done when it was.
    s.act(Action::Select(None));
    s.act(Action::ShowArchived);
    select(&mut s, "[x] Order oil #house");
    let edit = edited(&mut s, "[x] Order heating oil #house");
    let [Effect::Keep(kept)] = &edit[..] else {
        panic!("one version: {edit:?}");
    };
    assert_ne!(kept.stamp, done);
    assert_eq!(kept.done, done);

    s.act(Action::Select(None));
    select(&mut s, "[x] Order heating oil #house");
    s.act(Action::Archive);
    assert_eq!(
        s.notes.get("oil").unwrap().done,
        None,
        "unticked, so not done"
    );
    s.act(Action::Undo);
    assert_eq!(
        s.notes.get("oil").unwrap().done,
        done,
        "undo puts back when"
    );

    // Anything archived is done when it was archived, and a tick by id is the key's.
    s.act(Action::Select(None));
    select(&mut s, "Boiler #house");
    s.act(Action::Archive);
    assert!(s.notes.get("boiler").unwrap().done.is_some());
    let by_id = s.tick(&["radiators".to_owned()], true).unwrap();
    let [Effect::Keep(kept)] = &by_id[..] else {
        panic!("one version: {by_id:?}");
    };
    assert_eq!((kept.archived, kept.done), (true, kept.stamp));
    let back = s.tick(&["radiators".to_owned()], false).unwrap();
    let [Effect::Keep(kept)] = &back[..] else {
        panic!("one version: {back:?}");
    };
    assert_eq!(kept.done, None);
}

/// An open task at work under review, another at work, and a training app's note.
fn fence(s: &mut Session) {
    put(s, "fence", "[ ] Fix the fence #work #review", &[], 1_000);
    put(s, "shed", "[ ] Paint the shed #work", &[], 2_000);
    put(s, "app", "the training app #work", &[], 3_000);
}

#[test]
fn a_note_written_in_an_open_task_among_the_open_tasks_is_listed_back_among_them() {
    let mut s = session();
    fence(&mut s);
    type_in(&mut s, "#work #review [ ]");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#work", "#review", "[ ]"]);
    go_into(&mut s, "[ ] Fix the fence #work #review");
    assert_eq!(path(&s), ["#work", "#review", "[ ]", "[ ] Fix the fence"]);

    s.act(Action::Write);
    let fresh = |s: &Session| s.view().field.map(|f| (f.text.to_owned(), f.fresh));
    assert_eq!(fresh(&s), Some(("[ ]  #work #review".to_owned(), Some(4))));
    edited(&mut s, "[ ] Order fence posts #work #review");
    let posts = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&posts).unwrap().links, ["fence"]);

    s.act(Action::Back);
    assert_eq!(path(&s), ["#work", "#review", "[ ]"]);
    assert_eq!(
        heads(&s),
        [
            "[ ] Order fence posts #work #review",
            "[ ] Fix the fence #work #review"
        ]
    );

    select(&mut s, "[ ] Fix the fence #work #review");
    s.act(Action::Archive);
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["[ ] Order fence posts #work #review"]);
    let posts = s.notes.get(&posts).unwrap();
    assert_eq!(
        (posts.text.as_str(), posts.archived),
        ("[ ] Order fence posts #work #review", false),
        "ticking the task leaves what is under it open"
    );
}

#[test]
fn a_box_taken_out_of_a_new_row_stays_out_and_the_note_stays_under_its_task() {
    let mut s = session();
    fence(&mut s);
    go_to(&mut s, "#work [ ]");
    go_into(&mut s, "[ ] Fix the fence #work #review");
    s.act(Action::Write);
    assert_eq!(field(&s), Some(("[ ]  #work".to_owned(), true, false)));
    edited(&mut s, "Measure the gap #work");
    let gap = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    let held = s.notes.get(&gap).unwrap();
    assert_eq!(held.text, "Measure the gap #work");
    assert_eq!(held.links, ["fence"]);
    assert_eq!(heads(&s), ["Measure the gap #work"]);

    s.act(Action::Back);
    assert!(
        !heads(&s).contains(&"Measure the gap #work"),
        "not a task, so not among the open tasks"
    );
    s.act(Action::Back);
    clear(&mut s);
    assert!(heads(&s).contains(&"Measure the gap #work"));
}

#[test]
fn a_note_written_under_a_done_task_among_the_done_tasks_is_done_and_listed_where_it_was_written() {
    let mut s = session();
    fence(&mut s);
    select(&mut s, "[ ] Fix the fence #work #review");
    s.act(Action::Archive);
    s.act(Action::Select(None));
    go_to(&mut s, "#work [x]");
    assert_eq!(heads(&s), ["[x] Fix the fence #work #review"]);
    go_into(&mut s, "[x] Fix the fence #work #review");
    assert_eq!(
        path(&s),
        ["#work", "[x]", "[x] Fix the fence #work #review"]
    );

    type_in(&mut s, "Return the post driver");
    s.act(Action::Write);
    let driver = s.selected().unwrap().id.clone();
    let held = s.notes.get(&driver).unwrap();
    assert_eq!(held.text, "[x] Return the post driver #work");
    assert!(
        held.archived && held.done.is_some(),
        "done as it is written"
    );
    s.act(Action::Select(None));
    assert_eq!(
        heads(&s),
        ["[x] Return the post driver #work"],
        "listed where it was written, though it is archived"
    );

    // A span there measures when things were done, as with `[x]` on the line.
    type_in(&mut s, "/today..");
    assert_eq!(heads(&s), ["[x] Return the post driver #work"]);
    s.act(Action::Back);
    assert_eq!(path(&s), ["#work", "[x]"]);
    assert!(heads(&s).contains(&"[x] Return the post driver #work"));
}

#[test]
fn the_box_nearest_the_end_of_the_path_is_the_one_a_note_inherits() {
    let mut s = session();
    fence(&mut s);
    go_to(&mut s, "#work [ ]");
    go_into(&mut s, "[ ] Fix the fence #work #review");
    go_to(&mut s, "[x]");
    assert_eq!(
        path(&s),
        ["#work", "[ ]", "[ ] Fix the fence #work #review", "[x]"]
    );
    s.act(Action::Write);
    assert_eq!(field(&s), Some(("[x]  #work".to_owned(), true, false)));
    s.act(Action::Select(None));

    s.act(Action::Back);
    clear(&mut s);
    go_to(&mut s, "#review");
    assert_eq!(
        path(&s),
        ["#work", "[ ]", "[ ] Fix the fence #work #review", "#review"]
    );
    s.act(Action::Write);
    assert_eq!(
        field(&s),
        Some(("[ ]  #work #review".to_owned(), true, false)),
        "with no box after the note, the one before it"
    );
    s.act(Action::Select(None));

    // A box the writer typed is theirs, whichever box the path has.
    type_in(&mut s, "[x] Bought the posts");
    let written = s.act(Action::Write);
    let [held] = kept(&written)[..] else {
        panic!("one version: {written:?}");
    };
    assert_eq!(held.text, "[x] Bought the posts #work #review");
    assert_eq!(held.links, ["fence"]);
    assert!(held.archived, "done, and so not listed among what is not");
}
