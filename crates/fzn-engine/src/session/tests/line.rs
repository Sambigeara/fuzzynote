//! The line, the cursor and the wire: searching, going in and back, links, what is offered, and
//! the JSON a face sends and reads.

use super::*;

/// A hub with a neighbourhood of three: two notes that share words with the draft below, and
/// one that shares nothing with it.
fn hub_with_neighbours(s: &mut Session) {
    put(
        s,
        "profile",
        "Profile\ntrainingapp numbers and plan",
        &[],
        2_000,
    );
    put(s, "squats", "Squat log\ntrainingapp sets", &["hub"], 2_500);
    put(s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
    put(s, "near", "trainingapp plan ideas", &[], 1_000);
}

const DRAFT: &str = "the plan needs a deload week";

#[test]
fn midday_is_when_the_tests_say_it_is() {
    assert_eq!(NOON.to_string(), "2026-10-13T12:00:00.000Z");
    assert_eq!(time::weekday(NOON.day()), 1);
}

#[test]
fn archiving_keeps_the_date_and_hides_the_note_until_asked_for() {
    let mut s = session();
    let mut log = Vec::new();
    put(&mut s, "old", "kettlebell swings", &[], 1_000_000);
    log.extend(s.act(Action::Move(1)));
    log.extend(s.act(Action::Archive));
    let held = s.notes.get("old").unwrap();
    assert!(held.archived);
    assert_eq!(
        held.updated,
        Some(Moment::utc(1_000_000)),
        "archiving is not an edit"
    );
    assert_eq!(held.device, "dev");

    assert!(heads(&s).is_empty());
    s.act(Action::Type("kettlebell".into()));
    assert!(heads(&s).is_empty(), "nor does typing find it");
    s.act(Action::ShowArchived);
    assert_eq!(heads(&s), ["kettlebell swings"]);
    assert!(s.view().archived_shown);

    // Undo lands in the same second as the change it reverses, and still has to win,
    // whatever order the three versions are read back in.
    log.extend(s.act(Action::Undo));
    assert!(!s.notes.get("old").unwrap().archived);
    assert_eq!(log.len(), 3, "each change asked for one version to be kept");
    for notes in [reopened(&log), reopened(log.iter().rev())] {
        assert!(!notes.get("old").unwrap().archived);
        assert_eq!(
            notes.get("old").unwrap().updated,
            Some(Moment::utc(1_000_000))
        );
    }
}

#[test]
fn the_cursor_moves_between_the_line_and_the_list() {
    let mut s = session();
    s.act(Action::Move(1));
    assert_eq!(s.cursor, None, "nothing listed, so nowhere to go");
    put(&mut s, "c", "kettlebell halo", &[], 1_000);
    put(&mut s, "b", "kettlebell press", &[], 2_000);
    put(&mut s, "a", "kettlebell swings", &[], 3_000);
    s.act(Action::Move(1));
    assert_eq!(s.cursor, Some(0));
    s.act(Action::Move(10));
    assert_eq!(s.cursor, Some(2), "no further than the last note");
    s.act(Action::Move(-1));
    assert_eq!(s.cursor, Some(1));
    s.act(Action::Move(-10));
    assert_eq!(s.cursor, None, "up from the first note is the line");

    s.act(Action::Select(Some(2)));
    assert_eq!(s.cursor, Some(2));
    s.act(Action::Select(Some(3)));
    assert_eq!(s.cursor, Some(2), "a row that is not there is not gone to");
    s.act(Action::Type("press".into()));
    assert_eq!(s.cursor, None, "the line's action goes to the line");
    assert_eq!(heads(&s), ["kettlebell press"]);
}

#[test]
fn stepping_back_out_of_a_note_lands_on_the_line() {
    let mut s = session();
    add(&mut s, "kettlebell swings");
    add(&mut s, "kettlebell press");
    s.act(Action::Type("kettlebell".into()));
    s.act(Action::Move(1));
    s.act(Action::GoIn);
    assert_eq!(s.path.len(), 1);
    assert_eq!(s.line, "");
    s.act(Action::Back);
    assert!(s.path.is_empty());
    assert_eq!(s.line, "kettlebell");
    assert_eq!(
        s.cursor, None,
        "so that what is asked next is asked of the line"
    );
    s.act(Action::Back);
    assert_eq!(s.line, "kettlebell", "with nowhere to step back to");
}

#[test]
fn going_into_a_note_already_on_the_path_goes_back_to_it() {
    let mut s = session();
    put(
        &mut s,
        "squats",
        "Squat log\ntrainingapp sets",
        &["hub"],
        1_000,
    );
    put(
        &mut s,
        "profile",
        "Profile\ntrainingapp numbers",
        &["squats"],
        2_000,
    );
    put(&mut s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);

    type_in(&mut s, "trai");
    go_into(&mut s, "trainingapp");
    type_in(&mut s, "pro");
    go_into(&mut s, "Profile");
    type_in(&mut s, "squat");
    go_into(&mut s, "Squat log");
    assert_eq!(path(&s), ["trainingapp", "Profile", "Squat log"]);
    assert!(
        heads(&s).contains(&"trainingapp"),
        "the log links to the hub, so it is listed again"
    );

    go_into(&mut s, "trainingapp");
    assert_eq!(path(&s), ["trainingapp"], "back to it, not a fourth step");
    assert_eq!(s.line, "pro", "the line as it was at that depth");
    assert_eq!(s.cursor, None);

    // With something on the line, the line stays as it is.
    clear(&mut s);
    go_into(&mut s, "Profile");
    type_in(&mut s, "trai");
    go_into(&mut s, "trainingapp");
    assert_eq!(path(&s), ["trainingapp"]);
    assert_eq!(s.line, "trai");
}

#[test]
fn what_is_written_elsewhere_shows_up() {
    let mut s = session();
    let mine = add(&mut s, "mine");
    assert!(!s.absorb(Vec::new()), "nothing new");
    let kept: Vec<Record> = reopened(&mine).records().cloned().collect();
    assert_eq!(kept.len(), 1);
    assert!(
        !s.absorb(kept),
        "its own versions, read back from the log, are not news"
    );

    let theirs = Record {
        created: Some(NOON),
        updated: Some(NOON.plus_secs(60)),
        stamp: Some(NOON),
        device: "other".into(),
        ..Record::new("theirs", "written elsewhere")
    };
    s.act(Action::Move(1));
    assert!(s.absorb([theirs.clone()]));
    assert_eq!(heads(&s), ["mine"], "the list holds still under the cursor");
    assert_eq!(s.status, "1 changed elsewhere");
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["written elsewhere", "mine"]);
    s.act(Action::Type("elsewhere".into()));
    assert_eq!(heads(&s), ["written elsewhere"], "and the engine knows it");

    assert!(s.absorb([Record {
        deleted: true,
        stamp: Some(NOON.plus_secs(1)),
        ..theirs
    }]));
    assert!(heads(&s).is_empty(), "from the line, at once");
}

#[test]
fn a_note_written_around_another_is_linked_by_id_and_listed_first() {
    let mut s = session();
    put(
        &mut s,
        "profile",
        "Profile\ntrainingapp numbers and plan",
        &[],
        2_000,
    );
    put(&mut s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
    put(&mut s, "near", "trainingapp plan ideas", &[], 9_000_000_000);
    go_into(&mut s, "trainingapp");
    assert_eq!(heads(&s), ["Profile"]);

    let kept = add(&mut s, "some new note");
    assert_eq!(
        heads(&s),
        ["some new note", "Profile"],
        "what is linked, the last written on top, and nothing the engine suggests"
    );
    assert!(s.rows.iter().all(|r| r.linked));
    assert!(s.rows[0].reasons[0].starts_with("links here"));
    assert!(s.rows[1].reasons[0].starts_with("linked from here"));
    let new = s.notes.get(&s.rows[0].id).unwrap().clone();
    assert_eq!(new.text, "some new note", "the text is what was typed");
    assert_eq!(new.links, ["hub"]);
    assert_eq!((new.created, new.updated), (Some(NOON), Some(NOON)));
    assert_eq!(
        kept.last(),
        Some(&Effect::Keep(new.clone())),
        "and that is the version asked to be kept"
    );
    assert_eq!(s.line, "");
    assert_eq!(s.status, "created");

    // Renaming the hub breaks nothing, and from outside the note says what it is linked to.
    let hub = s.notes.get("hub").unwrap().clone();
    s.commit(Record {
        text: "Training App\nthe plan".into(),
        ..hub
    });
    s.refresh(false);
    assert_eq!(heads(&s)[..2], ["some new note", "Profile"]);
    s.act(Action::Back);
    let row = s.rows.iter().find(|r| r.head == "some new note").unwrap();
    assert_eq!(row.links, ["Training App"]);
}

#[test]
fn linking_and_unlinking_the_note_under_the_cursor() {
    let mut s = session();
    put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);
    put(
        &mut s,
        "profile",
        "Profile\ntrainingapp numbers",
        &[],
        2_000,
    );
    let hub = "trainingapp\nthe plan, see [[Profile]]";
    put(&mut s, "hub", hub, &["profile"], 3_000);
    go_into(&mut s, "trainingapp");
    assert_eq!(heads(&s), ["Profile"]);
    type_in(&mut s, "trainingapp ");
    assert_eq!(heads(&s), ["Profile", "trainingapp plan ideas"]);
    assert_eq!(
        s.rows.iter().map(|r| r.linked).collect::<Vec<_>>(),
        [true, false]
    );

    select(&mut s, "trainingapp plan ideas");
    s.act(Action::Link);
    assert_eq!(s.notes.get("near").unwrap().links, ["hub"]);
    assert_eq!(
        s.notes.get("near").unwrap().updated,
        Some(Moment::utc(1_000))
    );
    assert!(s.rows.iter().all(|r| r.linked), "now in the place");
    assert_eq!(s.selected().unwrap().head, "trainingapp plan ideas");

    s.act(Action::Link);
    assert!(s.notes.get("near").unwrap().links.is_empty());
    assert!(!s.selected().unwrap().linked);
    assert_eq!(s.status, "unlinked trainingapp plan ideas");

    // Any link comes off the same way, whichever note holds it and however it was made.
    select(&mut s, "Profile");
    s.act(Action::Link);
    assert!(s.notes.get("hub").unwrap().links.is_empty());
    assert!(!s.selected().unwrap().linked);
    assert!(s.status.contains("still in the text"), "{}", s.status);
    s.act(Action::Select(None));
    clear(&mut s);
    assert!(heads(&s).is_empty(), "and nothing is in the place now");
}

#[test]
fn brackets_typed_into_a_note_link_it_by_id() {
    let mut s = session();
    put(&mut s, "prog", "Kettlebell Programme\nweek one", &[], 2_000);
    let typed = "follow [[kettlebell programme]] and [[Nowhere]] today";
    add(&mut s, typed);
    let new = s.rows[0].id.clone();
    let held = s.notes.get(&new).unwrap();
    assert_eq!(held.links, ["prog"]);
    assert_eq!(held.text, typed, "the text is left as typed");
    go_into(&mut s, "Kettlebell Programme");
    assert!(s.rows[0].linked && s.rows[0].reasons[0].starts_with("links here"));

    // Renamed, the target is still linked, and the note shows its new head.
    let prog = s.notes.get("prog").unwrap().clone();
    s.commit(Record {
        text: "Strength Programme\nweek one".into(),
        ..prog
    });
    s.act(Action::Back);
    let row = s.rows.iter().find(|r| r.id == new).unwrap();
    assert_eq!(row.links, ["Strength Programme"]);
}

#[test]
fn linking_outside_a_note_does_nothing_but_say_so() {
    let mut s = session();
    put(&mut s, "a", "kettlebell swings", &[], 2_000);
    put(&mut s, "b", "kettlebell press", &[], 1_000);
    type_in(&mut s, "kettlebell day");
    select(&mut s, "kettlebell swings");
    s.act(Action::Link);
    assert!(s.notes.records().all(|r| r.links.is_empty()));
    assert_eq!(s.undo.len(), 2, "nothing was written");
    assert!(s.status.starts_with("go into a note first"), "{}", s.status);
    assert_eq!(s.line, "kettlebell day", "the line is left alone");

    s.act(Action::Select(None));
    s.act(Action::Write);
    let new = s
        .notes
        .records()
        .find(|r| r.text == "kettlebell day")
        .unwrap();
    assert!(
        new.links.is_empty(),
        "nor is anything kept for the note being written"
    );
}

#[test]
fn coming_back_never_throws_away_what_you_typed() {
    let mut s = session();
    put(&mut s, "hub", "trainingapp\nthe plan", &[], 3_000);
    put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);

    // Gone into from an empty line.
    go_into(&mut s, "trainingapp");
    type_in(&mut s, "deload next week");
    s.act(Action::Back);
    assert!(s.path.is_empty());
    assert_eq!(s.line, "deload next week");

    // Gone into by a search.
    clear(&mut s);
    type_in(&mut s, "trai");
    go_into(&mut s, "trainingapp");
    type_in(&mut s, "deload next week");
    s.act(Action::Back);
    assert_eq!(s.line, "deload next week", "the draft, not the search");
    assert_eq!(s.cursor, None);

    // With the line emptied, the search comes back.
    clear(&mut s);
    type_in(&mut s, "trai");
    go_into(&mut s, "trainingapp");
    type_in(&mut s, "oops");
    clear(&mut s);
    s.act(Action::Back);
    assert_eq!(s.line, "trai");
}

#[test]
fn a_search_clears_on_the_way_in_and_comes_back_on_the_way_out() {
    let mut s = session();
    put(&mut s, "hub", "trainingapp\nthe plan", &[], 3_000);
    put(&mut s, "near", "trainingapp plan ideas", &["hub"], 1_000);
    type_in(&mut s, "trai");
    go_into(&mut s, "trainingapp");
    assert_eq!(s.path.len(), 1);
    assert_eq!(s.line, "");
    assert_eq!(heads(&s), ["trainingapp plan ideas"]);

    s.act(Action::Back);
    assert!(s.path.is_empty());
    assert_eq!(s.line, "trai");
    assert_eq!(s.cursor, None, "on the line");
}

#[test]
fn the_name_of_a_note_finds_what_was_written_in_it() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "quote", "engineer quoted 240", &["boiler"], 2_000);
    put(
        &mut s,
        "pressure",
        "pressure dropped again",
        &["boiler"],
        3_000,
    );
    put(&mut s, "far", "cake recipe", &[], 4_000);

    type_in(&mut s, "boiler");
    assert_eq!(heads(&s)[0], "Boiler", "the note so named comes first");
    assert_eq!(s.rows[0].reasons[0], "its name", "{:?}", s.rows[0].reasons);
    assert!(s.rows[0].reasons.len() > 1, "and then what it scored for");
    assert_eq!(heads(&s).len(), 3);
    let row = s.rows.iter().find(|r| r.id == "quote").unwrap();
    assert!(
        row.reasons.iter().any(|r| r.contains("about Boiler")),
        "{:?}",
        row.reasons
    );
    assert_eq!(row.links, ["Boiler"], "and the row says so too");

    // Going into a note found that way was a search for it, though it does not say the word.
    go_into(&mut s, "pressure dropped again");
    assert_eq!(s.path.len(), 1);
    assert_eq!(s.line, "");
    s.act(Action::Back);
    assert_eq!(s.line, "boiler");

    // So was one found by a word of its own and the name of what it is about.
    clear(&mut s);
    type_in(&mut s, "boiler quoted");
    assert_eq!(heads(&s)[0], "engineer quoted 240");
    s.act(Action::Move(1));
    s.act(Action::GoIn);
    assert_eq!(s.line, "");
    s.act(Action::Back);

    // Something new said about the boiler is a draft, and goes in with you.
    clear(&mut s);
    type_in(&mut s, "boiler needs a service");
    go_into(&mut s, "Boiler");
    assert_eq!(s.line, "boiler needs a service");
}

#[test]
fn a_draft_comes_with_you_and_is_written_where_you_land() {
    let mut s = session();
    hub_with_neighbours(&mut s);
    go_into(&mut s, "trainingapp");
    assert_eq!(heads(&s), ["Squat log", "Profile"]);
    s.act(Action::Back);

    type_in(&mut s, DRAFT);
    go_into(&mut s, "trainingapp");
    assert_eq!(s.path.len(), 1);
    assert_eq!(s.line, DRAFT, "the draft came along");
    assert_eq!(s.cursor, None);
    assert_eq!(
        heads(&s),
        ["Profile", "trainingapp plan ideas"],
        "the neighbourhood narrowed by the draft, what is in the place first"
    );

    s.act(Action::Write);
    let new = s.notes.records().find(|r| r.text == DRAFT).unwrap();
    assert_eq!(new.links, ["hub"]);
    assert_eq!(s.line, "");
    assert_eq!(heads(&s)[0], DRAFT, "at the top of the place");

    s.act(Action::Back);
    assert!(s.path.is_empty());
    assert_eq!(s.line, "", "the draft was written, so it is not put back");
}

#[test]
fn a_draft_taken_into_a_note_and_back_out_is_still_on_the_line() {
    let mut s = session();
    hub_with_neighbours(&mut s);
    type_in(&mut s, DRAFT);
    go_into(&mut s, "trainingapp");
    type_in(&mut s, "!");
    s.act(Action::Back);
    assert!(s.path.is_empty());
    assert_eq!(s.line, format!("{DRAFT}!"), "as it is now, not as it was");
    assert_eq!(s.cursor, None);
    assert!(
        heads(&s).contains(&"trainingapp"),
        "and what it finds is listed"
    );
}

#[test]
fn only_what_applies_is_offered() {
    let mut s = session();
    assert_eq!(s.view().notes, 0);
    assert_eq!(
        labels(&s),
        ["new note", "show archived"],
        "nothing listed, so nothing to pick"
    );
    put(
        &mut s,
        "profile",
        "Profile\ntrainingapp numbers",
        &[],
        2_000,
    );
    assert_eq!(s.view().notes, 1);
    put(
        &mut s,
        "video",
        "Squat form video\nhttps://example.com/squat for trainingapp",
        &["hub"],
        2_500,
    );
    put(&mut s, "near", "trainingapp plan ideas", &[], 1_000);
    put(&mut s, "hub", "trainingapp\nthe plan", &["profile"], 3_000);
    s.say("saved");
    assert_eq!((s.view().status, s.view().notes), ("saved", 4));
    assert_eq!(
        labels(&s),
        ["new note", "pick a note", "show archived", "undo"]
    );
    assert_eq!(s.view().hints[1].action, Action::Move(1));
    type_in(&mut s, "squat");
    assert_eq!(labels(&s)[0], "write", "with something on the line");
    clear(&mut s);
    select(&mut s, "trainingapp");
    assert_eq!(
        labels(&s),
        [
            "new note",
            "body",
            "editor",
            "go in",
            "archive",
            "delete",
            "copy",
            "move down",
            "undo",
            "up"
        ]
    );
    assert_eq!(
        s.view().hints.last().unwrap().action,
        Action::Select(None),
        "the way back is last"
    );

    s.act(Action::GoIn);
    assert_eq!(
        labels(&s),
        [
            "new note here",
            "pick a note",
            "show archived",
            "undo",
            "back"
        ]
    );
    assert_eq!(s.view().hints.last().unwrap().action, Action::Back);
    type_in(&mut s, "squat");
    assert_eq!(labels(&s)[0], "write here");
    clear(&mut s);
    select(&mut s, "Profile");
    assert_eq!(
        labels(&s),
        [
            "new note here",
            "body",
            "editor",
            "go in",
            "unlink",
            "archive",
            "delete",
            "copy",
            "move up",
            "undo",
            "up"
        ]
    );
    select(&mut s, "Squat form video");
    assert_eq!(
        labels(&s),
        [
            "new note here",
            "body",
            "editor",
            "go in",
            "unlink",
            "archive",
            "delete",
            "copy",
            "open URL",
            "move down",
            "undo",
            "up"
        ]
    );
    // What is not in the place is found by typing, and can be linked there.
    s.act(Action::Select(None));
    type_in(&mut s, "plan");
    select(&mut s, "trainingapp plan ideas");
    assert_eq!(
        labels(&s)[..5],
        ["new note here", "body", "editor", "go in", "link"]
    );

    // The most there is to offer: a linked, archived note with a URL, with undo and redo to
    // hand. Under the top of the facts it could be moved up as well.
    s.act(Action::Select(None));
    clear(&mut s);
    s.act(Action::ShowArchived);
    select(&mut s, "Squat form video");
    s.act(Action::Archive);
    s.act(Action::Select(None));
    type_in(&mut s, "plan");
    select(&mut s, "trainingapp plan ideas");
    s.act(Action::Link);
    s.act(Action::Undo);
    s.act(Action::Select(None));
    clear(&mut s);
    assert_eq!(
        labels(&s),
        [
            "new note here",
            "pick a note",
            "hide archived",
            "undo",
            "redo",
            "back"
        ]
    );
    select(&mut s, "Squat form video");
    let all = [
        "new note here",
        "body",
        "editor",
        "go in",
        "unlink",
        "unarchive",
        "delete",
        "copy",
        "open URL",
        "move down",
        "undo",
        "redo",
        "up",
    ];
    assert_eq!(labels(&s), all);

    // A changed head is put back rather than undone. It is written as it is typed, which
    // leaves nothing to redo.
    s.act(Action::Edited(
        "Squat form video, slowed down\nhttps://example.com/squat for trainingapp".into(),
    ));
    assert_eq!(
        labels(&s),
        [
            "new note here",
            "body",
            "editor",
            "go in",
            "unlink",
            "unarchive",
            "delete",
            "copy",
            "open URL",
            "move down",
            "put back",
            "up"
        ]
    );
    s.act(Action::Undo);
    let without_redo: Vec<&str> = all.into_iter().filter(|&l| l != "redo").collect();
    assert_eq!(labels(&s), without_redo);

    // An open body offers what is about the note, and closing it is the way back. Putting
    // back is offered only once there is something to put back.
    s.act(Action::Open);
    assert_eq!(labels(&s), ["delete", "done"]);
    assert_eq!(s.view().hints.last().unwrap().action, Action::Close);
    s.act(Action::Edited("Squat form video\nslowed down".into()));
    assert_eq!(labels(&s), ["put back", "delete", "done"]);
    s.act(Action::Undo);
    assert_eq!(labels(&s), ["delete", "done"]);
    s.act(Action::Close);
    s.act(Action::Select(None));
    s.act(Action::Write);
    assert_eq!(labels(&s), ["body", "editor", "up"], "a new note");
    s.act(Action::Edited("deload".into()));
    assert_eq!(
        labels(&s),
        [
            "new note here",
            "body",
            "editor",
            "go in",
            "unlink",
            "archive",
            "delete",
            "copy",
            "move down",
            "put back",
            "up"
        ],
        "a note, once it has text"
    );
}

#[test]
fn a_date_word_searches_as_its_date_and_is_written_as_one() {
    let mut s = session();
    add(&mut s, "dentist 2026-10-16");
    add(&mut s, "something else entirely");

    // On the line the word stays as typed, the view says the date it stands for, and it
    // already holds the notes of that day.
    type_in(&mut s, "/fri");
    assert_eq!(s.line, "/fri");
    assert_eq!(s.view().dates, ["Fri 2026-10-16"]);
    assert_eq!(heads(&s), ["dentist 2026-10-16"]);

    // Going into the note it found is a search, so the line clears.
    s.act(Action::Move(1));
    s.act(Action::GoIn);
    assert_eq!(s.line, "");
    s.act(Action::Back);
    assert_eq!(s.line, "/fri");

    // Writing the line writes the date.
    add(&mut s, "book the car in /tom, not /today's");
    assert!(heads(&s).contains(&"book the car in 2026-10-14, not /today's"));
    s.act(Action::Type("no dates here /home/sam".into()));
    assert!(s.view().dates.is_empty());
}

#[test]
fn an_edit_goes_out_as_a_request_and_comes_back_as_a_save() {
    let mut s = session();
    put(&mut s, "prog", "Kettlebell Programme\nweek one", &[], 2_000);
    put(&mut s, "plan", "the plan", &[], 3_000);
    assert!(
        s.act(Action::Editor)
            .iter()
            .all(|e| matches!(e, Effect::Keep(_))),
        "on the line there is no note to edit"
    );
    select(&mut s, "the plan");
    assert_eq!(
        s.act(Action::Editor),
        [Effect::Editor {
            id: "plan".into(),
            text: "the plan".into()
        }]
    );
    assert!(!s.view().field.unwrap().whole, "the editor is not the body");
    let save = |s: &mut Session, text: &str| {
        s.act(Action::Save {
            id: "plan".into(),
            text: text.into(),
        })
    };

    assert!(save(&mut s, "the plan\n").is_empty());
    assert_eq!(
        s.status, "unchanged",
        "an editor's last newline is not a change"
    );

    let kept = save(
        &mut s,
        "The plan\nfollow [[kettlebell programme]] from /tom\n",
    );
    let held = s.notes.get("plan").unwrap().clone();
    assert_eq!(kept, [Effect::Keep(held.clone())]);
    assert_eq!(
        held.text,
        "The plan\nfollow [[kettlebell programme]] from 2026-10-14"
    );
    assert_eq!(held.links, ["prog"]);
    assert_eq!(held.updated, Some(NOON), "an edit is dated");
    assert_eq!(held.created, Some(Moment::utc(3_000)));
    assert_eq!(s.status, "saved");
    assert_eq!(
        s.selected().unwrap().head,
        "The plan",
        "the cursor stays on the note"
    );

    // Taking the brackets out takes the link away, and undo puts both back. Undo takes back
    // a visit, so this is a visit of its own.
    s.act(Action::Select(None));
    select(&mut s, "The plan");
    save(&mut s, "The plan");
    assert!(s.notes.get("plan").unwrap().links.is_empty());
    s.act(Action::Undo);
    assert_eq!(s.notes.get("plan").unwrap().links, ["prog"]);

    s.act(Action::Delete);
    assert!(save(&mut s, "too late").is_empty());
    assert!(s.status.starts_with("that note is gone"), "{}", s.status);
}

#[test]
fn copying_and_opening_are_left_to_the_face() {
    let mut s = session();
    put(
        &mut s,
        "video",
        "Squat form video\nhttps://example.com/squat",
        &[],
        2_000,
    );
    put(&mut s, "plan", "the plan", &[], 3_000);
    assert!(
        s.act(Action::Copy)
            .iter()
            .all(|e| matches!(e, Effect::Keep(_))),
        "on the line there is no note to copy from"
    );

    select(&mut s, "the plan");
    assert_eq!(s.act(Action::Copy), [Effect::Copy("the plan".into())]);
    assert_eq!(s.status, "copied head: the plan");
    assert!(s.act(Action::OpenUrl).is_empty());
    assert_eq!(s.status, "no URL in that note");

    select(&mut s, "Squat form video");
    let url = "https://example.com/squat";
    assert_eq!(s.act(Action::Copy), [Effect::Copy(url.into())]);
    assert_eq!(s.status, format!("copied URL: {url}"));
    assert_eq!(s.act(Action::OpenUrl), [Effect::Open(url.into())]);
    assert_eq!(s.status, format!("opened {url}"));
}

#[test]
fn deleting_and_undoing_ask_for_a_version_each() {
    let mut s = session();
    let written = add(&mut s, "kettlebell swings");
    s.act(Action::Move(1));
    let deleted = s.act(Action::Delete);
    assert!(heads(&s).is_empty());
    assert_eq!(s.view().notes, 0);
    assert!(matches!(&deleted[..], [Effect::Keep(r)] if r.deleted));

    let undone = s.act(Action::Undo);
    assert_eq!(heads(&s), ["kettlebell swings"]);
    assert_eq!(s.status, "undid: kettlebell swings");
    let redone = s.act(Action::Redo);
    assert!(heads(&s).is_empty());
    assert!(s.act(Action::Redo).is_empty());
    assert_eq!(s.status, "nothing to redo");

    let log: Vec<Effect> = [written, deleted, undone, redone].concat();
    assert_eq!(log.len(), 4);
    assert!(reopened(&log).is_empty());
    assert!(reopened(log.iter().rev()).is_empty());
}

#[test]
fn a_space_opens_with_the_notes_it_is_given() {
    let mut notes = Notes::new();
    for (id, text, updated, archived) in [
        ("a", "kettlebell swings", 2_000, false),
        ("b", "kettlebell press", 3_000, false),
        ("c", "kettlebell halo", 4_000, true),
    ] {
        notes.apply(Record {
            updated: Some(Moment::utc(updated)),
            archived,
            device: "other".into(),
            ..Record::new(id, text)
        });
    }
    let mut s = Session::new(
        Box::new(Stopped::default()),
        "dev",
        Weights::default(),
        notes,
    );
    assert_eq!(heads(&s), ["kettlebell press", "kettlebell swings"]);
    assert_eq!(s.view().notes, 3, "archived notes are still notes");
    s.act(Action::Type("swi".into()));
    assert_eq!(heads(&s), ["kettlebell swings"], "and they are indexed");
}

#[test]
fn actions_and_the_view_go_over_a_wire_as_json() {
    for (action, json) in [
        (Action::Write, r#""write""#),
        (Action::GoIn, r#""go_in""#),
        (Action::ShowArchived, r#""show_archived""#),
        (Action::Open, r#""open""#),
        (Action::Close, r#""close""#),
        (Action::Editor, r#""editor""#),
        (Action::Erase, r#""erase""#),
        (Action::Keep, r#""keep""#),
        (Action::Edited("a\nb".into()), r#"{"edited":"a\nb"}"#),
        (Action::Type("a line".into()), r#"{"type":"a line"}"#),
        (Action::Move(-1), r#"{"move":-1}"#),
        (Action::Select(None), r#"{"select":null}"#),
        (Action::Select(Some(3)), r#"{"select":3}"#),
        (
            Action::Restore(vec![
                KeptStep::Note {
                    note: "n1".into(),
                    search: Some("boi".into()),
                },
                KeptStep::Label {
                    label: "#house".into(),
                    search: None,
                },
            ]),
            r##"{"restore":[{"note":"n1","search":"boi"},{"label":"#house"}]}"##,
        ),
        (
            Action::Save {
                id: "n1".into(),
                text: "a\nb".into(),
            },
            r#"{"save":{"id":"n1","text":"a\nb"}}"#,
        ),
    ] {
        assert_eq!(serde_json::to_string(&action).unwrap(), json);
        assert_eq!(serde_json::from_str::<Action>(json).unwrap(), action);
    }

    let mut s = session();
    put(&mut s, "hub", "Boiler\nthe plan", &[], 3_000);
    s.act(Action::Move(1));
    let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
    assert_eq!(view["line"], "");
    assert_eq!(view["cursor"], 0);
    assert_eq!(view["rows"][0]["head"], "Boiler");
    assert_eq!(view["rows"][0]["has_body"], true);
    assert_eq!(view["rows"][0]["shown"], serde_json::Value::Null);
    assert!(view["rows"][0].get("created").is_none());
    assert_eq!(view["rows"][0]["task"], serde_json::Value::Null);
    assert_eq!(view["finds_from"], serde_json::Value::Null);
    assert_eq!(view["notes"], 1);
    assert_eq!(view["here"], serde_json::Value::Null);
    assert_eq!(
        view["field"],
        serde_json::json!({
            "text": "Boiler\nthe plan",
            "fresh": null,
            "whole": false,
            "created": "1970-01-01T00:50:00.000Z"
        })
    );
    assert!(view.get("open").is_none());
    // A hint's action is what to send back to ask for it.
    assert_eq!(view["hints"][0]["label"], "new note");
    assert_eq!(view["hints"][0]["action"], "write");
    assert_eq!(view["hints"][1]["action"], "open");
    let last = view["hints"].as_array().unwrap().last().unwrap();
    assert_eq!(last["action"], serde_json::json!({"select": null}));

    s.act(Action::Open);
    let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
    assert_eq!(view["field"]["whole"], true);
    s.act(Action::GoIn);
    let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
    assert_eq!(
        view["here"],
        serde_json::json!({"id": "hub", "text": "Boiler\nthe plan"})
    );
    assert_eq!(view["field"], serde_json::Value::Null);

    let effect = Effect::Editor {
        id: "hub".into(),
        text: "Boiler".into(),
    };
    assert_eq!(
        serde_json::to_string(&effect).unwrap(),
        r#"{"editor":{"id":"hub","text":"Boiler"}}"#
    );
}

#[test]
fn only_the_actions_that_can_change_a_note_write() {
    let writes = [
        Action::Write,
        Action::Open,
        Action::Edited("Boiler: bleed the radiators".into()),
        Action::Editor,
        Action::Close,
        Action::Keep,
        Action::Save {
            id: "n1".into(),
            text: "Boiler: bleed the radiators".into(),
        },
        Action::Archive,
        Action::Delete,
        Action::Link,
        Action::Undo,
        Action::Redo,
    ];
    let reads = [
        Action::Type("boiler".into()),
        Action::Move(1),
        Action::Select(None),
        Action::GoIn,
        Action::Back,
        Action::Copy,
        Action::OpenUrl,
        Action::ShowArchived,
        Action::ShowScores,
        Action::Restore(Vec::new()),
    ];
    for action in writes {
        assert!(action.writes(), "{action:?}");
    }
    for action in reads {
        assert!(!action.writes(), "{action:?}");
    }
}

#[test]
fn a_new_writer_names_the_versions_written_after_it() {
    let devices = |effects: Vec<Effect>| -> Vec<String> {
        effects
            .into_iter()
            .filter_map(|e| match e {
                Effect::Keep(r) => Some(r.device),
                _ => None,
            })
            .collect()
    };
    let mut s = session();
    assert_eq!(s.device(), "dev");
    assert_eq!(devices(add(&mut s, "plant the garlic")), ["dev"]);
    s.writes_as("3f9a01c2");
    assert_eq!(s.device(), "3f9a01c2");
    assert_eq!(devices(add(&mut s, "net the brassicas")), ["3f9a01c2"]);
}

#[test]
fn a_space_filled_from_nothing_says_nothing_and_what_changes_after_is_said() {
    let mut s = session();
    s.absorb([theirs("boiler", "Boiler", 60), theirs("house", "House", 61)]);
    assert_eq!(s.status, "");
    assert_eq!(heads(&s), ["House", "Boiler"]);
    // A space arrives a log at a time, and every log of it is the space arriving.
    s.absorb([theirs("pump", "Pump", 62)]);
    assert_eq!(s.status, "");

    // Once something has been asked of it, what arrives is news.
    s.act(Action::Move(1));
    s.act(Action::Select(None));
    s.absorb([theirs("garden", "a garden", 63)]);
    assert_eq!(s.status, "1 changed elsewhere");

    // A session that opens with notes is not being filled.
    let mut notes = Notes::new();
    notes.apply(theirs("boiler", "Boiler", 60));
    let mut s = Session::new(
        Box::new(Stopped::default()),
        "dev",
        Weights::default(),
        notes,
    );
    s.absorb([theirs("house", "House", 61)]);
    assert_eq!(s.status, "1 changed elsewhere");
}

#[test]
fn the_task_word_on_the_line_holds_the_open_tasks_goes_in_as_the_box_and_writes_a_task() {
    let mut s = session();
    put(&mut s, "oil", "[ ] Order oil #house", &[], 1_000);
    put(&mut s, "stamps", "[ ] Buy stamps", &[], 2_000);
    put(&mut s, "boiler", "Boiler #house", &[], 3_000);
    s.act(Action::Select(None));

    for typed in ["/task #house", "/ta #house", "/tas #house "] {
        clear(&mut s);
        type_in(&mut s, typed);
        assert_eq!(s.line, typed, "the word stays as typed");
        assert_eq!(heads(&s), ["[ ] Order oil #house"], "{typed:?}");
        assert_eq!(s.view().dates, ["[ ]"], "the line says what it stands for");
    }

    // Tab goes in as it does on `[ ] #house`, one label after the other.
    clear(&mut s);
    type_in(&mut s, "/task #house");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["[ ]", "#house"]);
    assert_eq!(heads(&s), ["[ ] Order oil #house"]);
    s.act(Action::Back);
    s.act(Action::Back);

    // Enter writes a note whose head is a task, as `[ ] Boiler` on the line does.
    clear(&mut s);
    add(&mut s, "Bleed the radiators /task");
    let new = s.selected().unwrap().id.clone();
    assert_eq!(s.notes.get(&new).unwrap().text, "[ ] Bleed the radiators");
    assert!(!s.notes.get(&new).unwrap().archived);
    s.act(Action::Select(None));
    // Alone it is the label `[ ]` alone is, so Enter goes into the open tasks as Tab does and
    // writes nothing.
    let before = s.notes.len();
    add(&mut s, "/task");
    assert_eq!(path(&s), ["[ ]"]);
    assert_eq!(s.notes.len(), before);
    s.act(Action::Back);
    clear(&mut s);
    add(&mut s, "[ ]");
    assert_eq!(path(&s), ["[ ]"]);
    assert_eq!(s.notes.len(), before);
    s.act(Action::Back);

    // `/t` is the start of date words too, and is neither.
    s.act(Action::Select(None));
    clear(&mut s);
    type_in(&mut s, "/t");
    assert!(s.view().dates.is_empty());
}

/// Two notes under `#house #boiler`, with the cursor back on an empty line.
fn house_and_boiler(s: &mut Session) {
    put(
        s,
        "radiators",
        "Bleed the radiators #house #boiler",
        &[],
        1_000,
    );
    put(s, "oil", "Order oil #house #boiler", &[], 2_000);
    s.act(Action::Select(None));
    clear(s);
}

#[test]
fn enter_on_a_line_of_labels_goes_into_them_as_tab_does() {
    let mut s = session();
    house_and_boiler(&mut s);
    let before = s.notes.len();
    add(&mut s, "#house #boiler");
    assert_eq!(path(&s), ["#house", "#boiler"]);
    assert_eq!(s.line, "");
    assert_eq!(s.notes.len(), before, "nothing was written");
    assert_eq!(heads(&s).len(), 2);
}

#[test]
fn enter_on_a_line_of_labels_that_names_nowhere_writes_nothing() {
    let mut s = session();
    house_and_boiler(&mut s);
    let before = s.notes.len();
    add(&mut s, "#house");
    assert_eq!(path(&s), ["#house"]);
    add(&mut s, "#house");
    assert_eq!(path(&s), ["#house"]);
    assert_eq!(s.notes.len(), before);
    s.act(Action::Back);
    clear(&mut s);
    add(&mut s, "[ ] [x]");
    assert_eq!(s.notes.len(), before);
}

#[test]
fn inside_a_place_enter_on_a_label_goes_into_it_too() {
    let mut s = session();
    house_and_boiler(&mut s);
    add(&mut s, "#house");
    add(&mut s, "#boiler");
    assert_eq!(path(&s), ["#house", "#boiler"]);
}

#[test]
fn the_line_of_labels_offers_going_in_and_not_writing() {
    let mut s = session();
    house_and_boiler(&mut s);
    type_in(&mut s, "#house #boiler");
    let offered: Vec<Action> = s.view().hints.iter().map(|h| h.action.clone()).collect();
    assert!(offered.contains(&Action::GoIn));
    assert!(!offered.contains(&Action::Write));
}

#[test]
fn a_line_with_words_spans_or_phrases_as_well_as_labels_is_still_written_by_enter() {
    let mut s = session();
    house_and_boiler(&mut s);
    for line in [
        "#house ..",
        "#house 2026-10-01..",
        "#house \"oil\"",
        "#house ~oil",
        "#house a",
        "Order oil #house",
    ] {
        s.act(Action::Select(None));
        clear(&mut s);
        let before = s.notes.len();
        add(&mut s, line);
        assert_eq!(s.notes.len(), before + 1, "{line} is written");
        assert!(path(&s).is_empty(), "{line} goes nowhere");
    }
}
