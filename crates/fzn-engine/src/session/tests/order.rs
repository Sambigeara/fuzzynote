//! Where a note sits: placed under the row it was opened from, kept there on every device, and
//! moved a row at a time.

use super::*;

/// Writes a note in a new row under the one the cursor is on, or at the top from the line,
/// and comes back to the line. Gives back what was asked to be kept.
fn under(s: &mut Session, text: &str) -> Vec<Effect> {
    let mut log = s.act(Action::Write);
    log.extend(edited(s, text));
    log.extend(s.act(Action::Select(None)));
    log
}

/// Another device's session, given the versions this one asked to be kept.
fn elsewhere<'a>(effects: impl IntoIterator<Item = &'a Effect>) -> Session {
    Session::new(
        Box::new(Stopped::default()),
        "3f9a01c2",
        Weights::default(),
        reopened(effects),
    )
}

#[test]
fn a_note_written_under_a_row_stays_under_it_here_and_on_another_device() {
    let mut s = session();
    let mut log = Vec::new();
    for text in ["Boiler", "House", "a garden"] {
        log.extend(add(&mut s, text));
        log.extend(s.act(Action::Select(None)));
    }
    assert_eq!(heads(&s), ["a garden", "House", "Boiler"]);
    log.extend(s.act(Action::Move(2)));
    log.extend(under(&mut s, "Order oil"));
    let placed = ["a garden", "House", "Order oil", "Boiler"];
    assert_eq!(heads(&s), placed, "back on the line, it has not moved");
    let oil = s.rows[2].id.clone();
    assert_eq!(s.notes.get(&oil).unwrap().after, Some(s.rows[1].id.clone()));
    assert_eq!(heads(&elsewhere(&log)), placed);
}

#[test]
fn a_note_opened_or_written_on_the_line_goes_to_the_top() {
    let mut s = session();
    three(&mut s);
    let opened = under(&mut s, "Order oil");
    assert_eq!(heads(&s), ["Order oil", "Boiler", "House", "a garden"]);
    let [first, ..] = kept(&opened)[..] else {
        panic!("{opened:?}");
    };
    assert!(
        first.after.is_none() && first.placed.is_some(),
        "placed from its first version, so placed at once everywhere"
    );
    add(&mut s, "Bleed the radiators");
    s.act(Action::Select(None));
    assert_eq!(heads(&s)[..2], ["Bleed the radiators", "Order oil"]);
}

#[test]
fn a_note_moves_a_row_down_or_up_with_the_cursor_on_it() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "Boiler");
    assert!(!labels(&s).contains(&"move up"), "nowhere up to go");
    s.act(Action::Shift(1));
    assert_eq!(heads(&s), ["House", "Boiler", "a garden"]);
    assert_eq!(s.selected().unwrap().head, "Boiler");
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
    s.act(Action::Shift(-1));
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);

    select(&mut s, "a garden");
    assert!(!labels(&s).contains(&"move down"), "nowhere down to go");
    s.act(Action::Shift(-1));
    assert_eq!(heads(&s), ["Boiler", "a garden", "House"]);
    s.act(Action::Shift(-1));
    assert_eq!(heads(&s), ["a garden", "Boiler", "House"]);
    assert_eq!(s.notes.get("garden").unwrap().after, None, "at the top");

    let undo = s.undo.len();
    assert!(
        s.act(Action::Shift(-1)).is_empty(),
        "the top moved up writes nothing"
    );
    select(&mut s, "House");
    assert!(s.act(Action::Shift(1)).is_empty());
    assert_eq!(s.undo.len(), undo);
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["a garden", "Boiler", "House"]);

    // What the line finds is in the order of its scores, so nothing there is moved.
    type_in(&mut s, "garden");
    s.act(Action::Move(1));
    assert!(!labels(&s).iter().any(|l| l.starts_with("move")));
    assert!(s.act(Action::Shift(1)).is_empty());
}

#[test]
fn a_note_moves_among_the_facts_of_a_place() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "house", "House", &["boiler"], 2_000);
    put(&mut s, "garden", "a garden", &["boiler"], 3_000);
    put(&mut s, "oil", "Order oil", &["boiler"], 4_000);
    go_into(&mut s, "Boiler");
    assert_eq!(heads(&s), ["Order oil", "a garden", "House"]);
    select(&mut s, "House");
    s.act(Action::Shift(-1));
    assert_eq!(heads(&s), ["Order oil", "House", "a garden"]);
    s.act(Action::Shift(-1));
    assert_eq!(heads(&s), ["House", "Order oil", "a garden"]);
    assert_eq!(s.selected().unwrap().head, "House");
    select(&mut s, "a garden");
    assert!(!labels(&s).contains(&"move down"), "the facts end here");
    s.act(Action::Back);
    assert_eq!(heads(&s), ["Boiler", "House", "Order oil", "a garden"]);
    let order: Vec<&str> = s.notes.order().iter().map(|r| r.head()).collect();
    assert_eq!(order, ["House", "Order oil", "a garden", "Boiler"]);
}

#[test]
fn a_note_moves_alone_and_undo_puts_back_what_was_written_under_it_too() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    under(&mut s, "Order oil");
    select(&mut s, "House");
    under(&mut s, "Bleed the radiators");
    let written = [
        "Boiler",
        "House",
        "Bleed the radiators",
        "Order oil",
        "a garden",
    ];
    assert_eq!(heads(&s), written);

    select(&mut s, "House");
    let undo = s.undo.len();
    let moved = s.act(Action::Shift(-1));
    assert_eq!(
        kept(&moved).len(),
        3,
        "the note and the two placed after it"
    );
    assert_eq!(s.undo.len(), undo + 1, "one change");
    s.act(Action::Select(None));
    assert_eq!(
        heads(&s),
        [
            "House",
            "Boiler",
            "Bleed the radiators",
            "Order oil",
            "a garden"
        ],
        "the run under it stays where it was, in its order"
    );

    select(&mut s, "House");
    s.act(Action::Undo);
    assert_eq!(heads(&s), written);
    assert_eq!(s.selected().unwrap().head, "House");
    assert_eq!(s.undo.len(), undo);
    s.act(Action::Redo);
    assert_eq!(
        heads(&s),
        [
            "House",
            "Boiler",
            "Bleed the radiators",
            "Order oil",
            "a garden"
        ]
    );

    // Down past what was written under it, it swaps with the row below and no more.
    s.act(Action::Undo);
    s.act(Action::Select(None));
    select(&mut s, "House");
    s.act(Action::Shift(1));
    assert_eq!(
        heads(&s),
        [
            "Boiler",
            "Bleed the radiators",
            "House",
            "Order oil",
            "a garden"
        ]
    );
}

#[test]
fn a_note_placed_after_one_deleted_or_archived_keeps_its_place() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    under(&mut s, "Order oil");
    select(&mut s, "Boiler");
    under(&mut s, "Bleed the radiators");
    assert_eq!(
        heads(&s),
        [
            "Boiler",
            "Bleed the radiators",
            "House",
            "Order oil",
            "a garden"
        ]
    );
    select(&mut s, "House");
    s.act(Action::Delete);
    select(&mut s, "Boiler");
    s.act(Action::Archive);
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["Bleed the radiators", "Order oil", "a garden"]);
    s.act(Action::ShowArchived);
    assert_eq!(
        heads(&s),
        ["Boiler", "Bleed the radiators", "Order oil", "a garden"]
    );
}

#[test]
fn a_move_or_an_edit_carries_the_other_side_of_the_note_as_it_was_written() {
    // Two other devices wrote the note and placed it in the same second, and this device has
    // heard from only one of them.
    let first = Record {
        after: Some("house".into()),
        ..theirs("oil", "Order oil", 60)
    };
    let second = Record {
        text: "Order oil by Friday".into(),
        after: Some("garden".into()),
        device: "3f9a01c3".into(),
        ..first.clone()
    };
    let merged = |versions: &[&Record]| {
        let mut notes = Notes::new();
        for version in versions {
            notes.apply((*version).clone());
        }
        notes.get("oil").cloned().unwrap()
    };
    let both = merged(&[&first, &second]);
    for change in [Action::Shift(-1), Action::Edited("Order oil, paid".into())] {
        let mut s = session();
        three(&mut s);
        s.absorb([first.clone()]);
        select(&mut s, "Order oil");
        let effects = s.act(change.clone());
        let [version] = kept(&effects)[..] else {
            panic!("{effects:?}");
        };
        let all = merged(&[&first, &second, version]);
        match change {
            Action::Shift(_) => assert_eq!(all.text, both.text, "a move is not an edit"),
            _ => assert_eq!(all.after, both.after, "an edit is not a move"),
        }
    }
}

#[test]
fn a_fact_moved_up_goes_past_the_fact_above_it_and_nothing_else() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "oil", "Order oil", &["boiler"], 2_000);
    put(&mut s, "garden", "a garden", &["boiler"], 3_000);
    put(&mut s, "house", "House", &[], 4_000);
    put(
        &mut s,
        "radiators",
        "Bleed the radiators",
        &["boiler"],
        5_000,
    );
    go_into(&mut s, "Boiler");
    assert_eq!(
        heads(&s)[..3],
        ["Bleed the radiators", "a garden", "Order oil"]
    );
    select(&mut s, "Order oil");
    s.act(Action::Shift(-1));
    assert_eq!(
        heads(&s)[..3],
        ["Bleed the radiators", "Order oil", "a garden"]
    );
    let order: Vec<&str> = s.notes.order().iter().map(|r| r.head()).collect();
    assert_eq!(
        order,
        [
            "Bleed the radiators",
            "House",
            "Order oil",
            "a garden",
            "Boiler"
        ],
        "it has not jumped the note between the two facts"
    );
}

#[test]
fn a_note_gone_elsewhere_is_not_moved() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 1_000);
    put(&mut s, "boiler", "Boiler", &[], 2_000);
    s.act(Action::Move(1));
    let gone = Record {
        deleted: true,
        stamp: Some(NOON.plus_secs(120)),
        device: "3f9a01c2".into(),
        ..s.notes.get("boiler").unwrap().clone()
    };
    s.absorb([gone]);
    assert!(kept(&s.act(Action::Shift(1))).is_empty());
}

#[test]
fn undoing_a_move_leaves_what_another_device_has_written_since() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    s.act(Action::Shift(1));
    s.act(Action::Select(None));
    let edit = Record {
        text: "House insurance".into(),
        stamp: Some(NOON.plus_secs(60)),
        device: "3f9a01c2".into(),
        ..s.notes.get("house").unwrap().clone()
    };
    s.absorb([edit]);
    s.act(Action::Undo);
    assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
}

#[test]
fn putting_back_a_head_leaves_where_another_device_has_moved_its_note() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "a garden");
    edited(&mut s, "a garden, dug over");
    let moved = Record {
        after: Some("boiler".into()),
        placed: Some(NOON.plus_secs(60)),
        ..s.notes.get("garden").unwrap().clone()
    };
    s.absorb([moved]);
    s.act(Action::Undo);
    assert_eq!(field(&s).unwrap().0, "a garden");
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["Boiler", "a garden", "House"]);
}
