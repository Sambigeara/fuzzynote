//! A row whose note goes from the list, wherever that was done: ticked, archived or deleted,
//! here or on another device. Its row goes where it stands and the rest of the list stays as it
//! was, so that what is shown is never behind what is held and the walk down the list is not
//! upset by what is put into it.

use super::*;

/// Four open tasks, the last put first.
fn tasks(s: &mut Session) {
    put(s, "fence", "[ ] Mend the fence", &[], 1_000);
    put(s, "oil", "[ ] Order oil", &[], 2_000);
    put(s, "gate", "[ ] Fix the gate", &[], 3_000);
    put(s, "boiler", "[ ] Boiler service", &[], 4_000);
    s.act(Action::Select(None));
}

/// A note as it is held here, changed on another device a minute or two after midday.
fn elsewhere(s: &Session, id: &str, at: i64, change: impl FnOnce(&mut Record)) -> Record {
    let mut record = s.notes.get(id).unwrap().clone();
    change(&mut record);
    record.updated = Some(NOON.plus_secs(at));
    record.stamp = Some(NOON.plus_secs(at));
    record.device = "3f9a01c2".into();
    record
}

/// A task ticked on another device, as ticking it here would: its box and its flag together.
fn ticked(s: &Session, id: &str, at: i64, done: bool) -> Record {
    elsewhere(s, id, at, |r| {
        r.text = crate::text::tick(&r.text, done);
        r.archived = done;
    })
}

#[test]
fn a_note_ticked_elsewhere_takes_its_row_away_and_the_rest_stay_where_they_were() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Boiler service");
    s.take_caret();
    let oil = ticked(&s, "oil", 60, true);
    assert!(s.absorb([oil, theirs("shed", "[ ] Paint the shed", 60)]));
    assert_eq!(
        heads(&s),
        [
            "[ ] Boiler service",
            "[ ] Fix the gate",
            "[ ] Mend the fence"
        ],
        "a note new from elsewhere waits for the line"
    );
    assert_eq!(s.selected().unwrap().id, "boiler");
    assert_eq!(
        s.take_caret(),
        Caret::default(),
        "the caret stays where it was"
    );
    assert_eq!(s.status, "2 changed elsewhere");

    // A row going above the cursor leaves the cursor on its note.
    select(&mut s, "[ ] Mend the fence");
    let boiler = ticked(&s, "boiler", 120, true);
    s.absorb([boiler]);
    assert_eq!(heads(&s), ["[ ] Fix the gate", "[ ] Mend the fence"]);
    assert_eq!(s.selected().unwrap().id, "fence");

    s.act(Action::Select(None));
    assert_eq!(
        heads(&s),
        [
            "[ ] Paint the shed",
            "[ ] Fix the gate",
            "[ ] Mend the fence"
        ]
    );
}

#[test]
fn the_row_under_the_cursor_ticked_elsewhere_goes_and_the_cursor_takes_the_row_in_its_place() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    s.take_caret();
    let gate = ticked(&s, "gate", 60, true);
    s.absorb([gate]);
    assert_eq!(
        heads(&s),
        ["[ ] Boiler service", "[ ] Order oil", "[ ] Mend the fence"]
    );
    assert_eq!(field(&s), Some(("[ ] Order oil".into(), false, false)));
    assert_eq!(
        s.take_caret(),
        Caret {
            line: None,
            head: Some("[ ] Order oil".len()),
        }
    );

    // The last row's place is taken by the row above it.
    select(&mut s, "[ ] Mend the fence");
    let fence = ticked(&s, "fence", 120, true);
    s.absorb([fence]);
    assert_eq!(s.selected().unwrap().id, "oil");

    // With no row left the cursor is on the line, and the list is made again.
    s.act(Action::Type("oil".into()));
    s.act(Action::Move(1));
    assert_eq!(heads(&s), ["[ ] Order oil"]);
    let oil = ticked(&s, "oil", 180, true);
    s.absorb([oil]);
    assert_eq!((s.cursor, heads(&s)), (None, Vec::<&str>::new()));
}

#[test]
fn a_row_being_written_in_stays_until_the_cursor_leaves_it() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    edited(&mut s, "[ ] Fix the gate latch");
    let gate = ticked(&s, "gate", 60, true);
    s.absorb([gate]);
    assert_eq!(
        heads(&s),
        [
            "[ ] Boiler service",
            "[ ] Fix the gate latch",
            "[ ] Order oil",
            "[ ] Mend the fence"
        ],
        "the row is not taken from under the hands"
    );
    assert_eq!(field(&s).unwrap().0, "[ ] Fix the gate latch");
    s.act(Action::Move(1));
    assert_eq!(
        heads(&s),
        ["[ ] Boiler service", "[ ] Order oil", "[ ] Mend the fence"]
    );
    assert_eq!(
        s.selected().unwrap().id,
        "oil",
        "the arrow lands on the row that was under it"
    );

    // A body open is being written in too, closed or not, until the cursor leaves.
    s.act(Action::Open);
    let oil = ticked(&s, "oil", 120, true);
    s.absorb([oil]);
    assert_eq!(s.selected().unwrap().id, "oil");
    s.act(Action::Close);
    assert_eq!(s.selected().unwrap().id, "oil");
    s.act(Action::Move(-1));
    assert_eq!(heads(&s), ["[ ] Boiler service", "[ ] Mend the fence"]);
    assert_eq!(s.selected().unwrap().id, "boiler");
}

#[test]
fn a_row_going_under_the_hands_stays_if_its_note_is_brought_back() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    edited(&mut s, "[ ] Fix the gate latch");
    let done = ticked(&s, "gate", 60, true);
    s.absorb([done]);
    let open = ticked(&s, "gate", 120, false);
    s.absorb([open]);
    s.act(Action::Move(1));
    assert_eq!(heads(&s).len(), 4);
    assert_eq!(s.selected().unwrap().id, "oil");
}

#[test]
fn a_note_changed_elsewhere_is_drawn_again_where_it_stands() {
    let mut s = session();
    tasks(&mut s);
    s.act(Action::Type("[ ]".into()));
    select(&mut s, "[ ] Boiler service");
    let oil = elsewhere(&s, "oil", 60, |r| r.text = "[ ] Order heating oil".into());
    s.absorb([oil]);
    assert_eq!(
        heads(&s),
        [
            "[ ] Boiler service",
            "[ ] Fix the gate",
            "[ ] Order heating oil",
            "[ ] Mend the fence"
        ]
    );

    // An edit never takes a row away, even one that leaves the note without what the line holds:
    // it stays until the list is made again, as one typed here does.
    let gate = elsewhere(&s, "gate", 120, |r| r.text = "Fix the gate".into());
    s.absorb([gate]);
    let row = s.rows.iter().find(|r| r.id == "gate").unwrap();
    assert_eq!((row.head.as_str(), row.task), ("Fix the gate", None));

    // With what is archived shown, a note ticked elsewhere stays, drawn ticked.
    s.act(Action::Type(String::new()));
    s.act(Action::ShowArchived);
    select(&mut s, "[ ] Boiler service");
    let fence = ticked(&s, "fence", 180, true);
    s.absorb([fence]);
    let row = s.rows.iter().find(|r| r.id == "fence").unwrap();
    assert_eq!(
        (row.head.as_str(), row.task, row.archived),
        ("[x] Mend the fence", Some(true), true)
    );
}

#[test]
fn a_tick_here_takes_only_its_row_and_what_arrived_waits_for_the_line() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    s.absorb([theirs("shed", "[ ] Paint the shed", 60)]);
    s.act(Action::Archive);
    assert_eq!(s.status, "ticked");
    assert_eq!(
        heads(&s),
        ["[ ] Boiler service", "[ ] Order oil", "[ ] Mend the fence"]
    );
    assert_eq!(s.selected().unwrap().id, "oil");

    // Undo brings it back with the cursor on it, and redo takes only its row again.
    s.act(Action::Undo);
    assert_eq!(s.selected().unwrap().id, "gate");
    let listed = heads(&s).len();
    s.act(Action::Redo);
    assert_eq!(heads(&s).len(), listed - 1);
    assert!(!heads(&s).contains(&"[ ] Fix the gate"));
    assert_eq!(s.selected().unwrap().id, "oil");
}

#[test]
fn unticking_among_the_done_tasks_takes_the_row_the_list_would_no_longer_have() {
    let mut s = session();
    tasks(&mut s);
    for head in ["[ ] Fix the gate", "[ ] Order oil"] {
        select(&mut s, head);
        s.act(Action::Archive);
    }
    s.act(Action::Type("[x]".into()));
    assert_eq!(heads(&s), ["[x] Fix the gate", "[x] Order oil"]);
    select(&mut s, "[x] Fix the gate");
    s.act(Action::Archive);
    assert_eq!(s.status, "unticked");
    assert_eq!(heads(&s), ["[x] Order oil"]);
    assert_eq!(s.selected().unwrap().id, "oil");
}

#[test]
fn a_row_going_under_the_hands_stays_if_putting_it_back_brings_its_note_back() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    edited(&mut s, "[ ] Fix the gate latch");
    let gate = ticked(&s, "gate", 60, true);
    s.absorb([gate]);
    s.act(Action::Undo);
    assert_eq!(field(&s).unwrap().0, "[ ] Fix the gate");
    assert!(!s.notes.get("gate").unwrap().archived);
    s.act(Action::Move(1));
    assert_eq!(heads(&s).len(), 4);
    assert_eq!(s.selected().unwrap().id, "oil");
}

#[test]
fn a_head_typed_into_stays_in_hand_once_what_was_typed_is_written_out_until_it_is_put_back() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    edited(&mut s, "[ ] Fix the gate latch");
    s.act(Action::Keep);
    let gate = ticked(&s, "gate", 60, true);
    s.absorb([gate]);
    assert_eq!(s.selected().unwrap().id, "gate");

    // Put back, it is no longer being written in, and goes as an untouched row does.
    select(&mut s, "[ ] Order oil");
    edited(&mut s, "[ ] Order heating oil");
    s.act(Action::Undo);
    let oil = ticked(&s, "oil", 120, true);
    s.absorb([oil]);
    assert_eq!(s.selected().unwrap().id, "fence");
}

#[test]
fn a_row_going_under_the_hands_goes_however_the_cursor_leaves_it() {
    // Tapped away from, the cursor lands on the row tapped.
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    edited(&mut s, "[ ] Fix the gate latch");
    let gate = ticked(&s, "gate", 60, true);
    s.absorb([gate]);
    select(&mut s, "[ ] Mend the fence");
    assert_eq!(
        heads(&s),
        ["[ ] Boiler service", "[ ] Order oil", "[ ] Mend the fence"]
    );
    assert_eq!(s.selected().unwrap().id, "fence");

    // Enter opens a row under it, and the cursor stays in the new row.
    select(&mut s, "[ ] Order oil");
    edited(&mut s, "[ ] Order heating oil");
    let oil = ticked(&s, "oil", 120, true);
    s.absorb([oil]);
    s.act(Action::Write);
    assert_eq!(heads(&s), ["[ ] Boiler service", "", "[ ] Mend the fence"]);
    assert_eq!(field(&s), Some((String::new(), true, false)));
}

#[test]
fn a_note_written_done_here_is_not_taken_away_by_an_edit_from_elsewhere() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Boiler service");
    s.act(Action::Write);
    edited(&mut s, "[x] Paid the boiler bill");
    s.act(Action::Move(1));
    let id = s.rows[1].id.clone();
    assert!(s.notes.get(&id).unwrap().archived, "done as it was written");
    let paid = elsewhere(&s, &id, 60, |r| {
        r.text = "[x] Paid the boiler bill in full".into()
    });
    s.absorb([paid]);
    assert_eq!(heads(&s)[1], "[x] Paid the boiler bill in full");
}

#[test]
fn a_note_written_from_the_line_and_taken_back_takes_only_its_row() {
    let mut s = session();
    tasks(&mut s);
    select(&mut s, "[ ] Fix the gate");
    s.absorb([theirs("shed", "[ ] Paint the shed", 60)]);
    s.act(Action::Select(None));
    add(&mut s, "[ ] Sweep the path");
    select(&mut s, "[ ] Order oil");
    s.absorb([theirs("roof", "[ ] Fix the roof tiles", 120)]);
    select(&mut s, "[ ] Sweep the path");
    s.act(Action::Undo);
    assert!(!heads(&s).contains(&"[ ] Sweep the path"));
    assert!(
        !heads(&s).contains(&"[ ] Fix the roof tiles"),
        "what arrived still waits for the line"
    );
    assert_eq!(s.selected().unwrap().head, "[ ] Paint the shed");
}
