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
    s.act(Action::ShowArchived);
    assert_eq!(heads(&s), open, "a task archived is not open");
    s.act(Action::ShowArchived);
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
