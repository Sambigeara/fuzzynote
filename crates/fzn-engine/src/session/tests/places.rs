//! Labels and places: a tag, a person or a day gone into, a note and the labels after it as one
//! place, and what a note written there inherits.

use super::*;

#[test]
fn a_tag_on_the_line_is_gone_into_and_lists_every_note_carrying_it() {
    let mut s = session();
    house(&mut s);
    let mut old = s.notes.get("roof").unwrap().clone();
    old.id = "fence".into();
    old.text = "paint the fence #house".into();
    old.archived = true;
    s.commit(old);

    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house"]);
    assert_eq!(s.line, "", "it was a search for the place");
    assert!(s.view().here.is_none(), "a label has no note to read");
    assert_eq!(
        heads(&s),
        [
            "fix the roof tiles #house",
            "clear the gutter #house",
            "Boiler"
        ],
        "every note carrying it, last written on top, and nothing else that says it"
    );
    assert_eq!(linked(&s), [true, true, true]);
    assert!(
        s.rows.iter().all(|r| r.reasons.is_empty()),
        "what is in a place needs no reason"
    );

    s.act(Action::ShowArchived);
    assert!(heads(&s).contains(&"paint the fence #house"));
    assert!(s.rows.iter().find(|r| r.id == "fence").unwrap().linked);

    s.act(Action::Back);
    assert!(path(&s).is_empty());
    assert_eq!(s.line, "#house", "and the search comes back on the way out");
}

#[test]
fn labels_gone_into_in_one_tab_step_back_as_if_each_had_been_typed_and_gone_into() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house #garden");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "#garden"]);
    assert_eq!(s.line, "");
    let kept = s.steps();

    s.act(Action::Back);
    assert_eq!(path(&s), ["#house"]);
    assert_eq!(s.line, "#garden", "the search for that step alone");
    clear(&mut s);
    s.act(Action::Back);
    assert!(path(&s).is_empty());
    assert_eq!(s.line, "#house");

    // The same as going in a label at a time.
    clear(&mut s);
    go_to(&mut s, "#house");
    go_to(&mut s, "#garden");
    assert_eq!(s.steps(), kept);

    // And so the same after a reload.
    let mut again = session();
    house(&mut again);
    again.act(Action::Restore(kept));
    again.act(Action::Back);
    assert_eq!(
        (path(&again), again.line.as_str()),
        (vec!["#house"], "#garden")
    );
}

#[test]
fn a_label_put_back_on_the_way_out_is_as_it_is_written_and_reads_as_the_same_label() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house /task /fri");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "[ ]", "2026-10-16"]);

    s.act(Action::Back);
    assert_eq!(s.line, "2026-10-16", "a day word comes back as its day");
    s.act(Action::GoIn);
    assert_eq!(
        path(&s),
        ["#house", "[ ]", "2026-10-16"],
        "and goes in again"
    );

    s.act(Action::Back);
    clear(&mut s);
    s.act(Action::Back);
    assert_eq!(s.line, "[ ]", "the task word comes back as its box");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "[ ]"]);
}

#[test]
fn a_label_typed_twice_is_one_place_and_gives_back_the_line_as_typed() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house #House");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house"]);
    s.act(Action::Back);
    assert_eq!(s.line, "#house #House");
}

#[test]
fn a_label_only_gone_back_to_or_passed_over_puts_nothing_back() {
    let mut s = session();
    house(&mut s);
    go_to(&mut s, "#house");
    go_to(&mut s, "/today");
    type_in(&mut s, "#house @joe");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "2026-10-13", "@joe"]);
    s.act(Action::Back);
    assert_eq!(s.line, "@joe", "not #house, which was passed over");

    clear(&mut s);
    go_to(&mut s, "boiler");
    type_in(&mut s, "#house @joe");
    s.act(Action::GoIn);
    assert_eq!(
        path(&s),
        ["#house", "@joe"],
        "#house before the note is gone back to"
    );
    s.act(Action::Back);
    assert_eq!(path(&s), ["#house"]);
    assert_eq!(s.line, "@joe");
    clear(&mut s);
    s.act(Action::Back);
    assert_eq!(s.line, "#house", "what it was gone into with before");
}

#[test]
fn typing_inside_a_tag_narrows_its_neighbourhood() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    type_in(&mut s, "roof");
    assert_eq!(heads(&s), ["fix the roof tiles #house"]);
    assert_eq!(linked(&s), [true]);
    clear(&mut s);
    type_in(&mut s, "move");
    assert_eq!(heads(&s), ["house move plan"]);
    assert_eq!(linked(&s), [false]);
    clear(&mut s);
    type_in(&mut s, "cake");
    assert!(heads(&s).is_empty(), "nothing outside the neighbourhood");
}

#[test]
fn every_note_carrying_a_tag_is_listed_whatever_the_limit_and_typing_keeps_those_that_match() {
    let mut s = session();
    put(&mut s, "tyres", "pump the tyres #house", &[], 1_000);
    s.absorb((0..=PLACE_LIMIT).map(|i| theirs(&format!("leaves-{i:04}"), "rake #house", 1)));
    s.absorb((0..=TOP_LIMIT).map(|i| theirs(&format!("pump-{i:04}"), "pump it", 1)));
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    assert_eq!(linked(&s), [true; PLACE_LIMIT + 2]);

    // However many notes elsewhere say it better, a note carrying the tag that says it stays.
    type_in(&mut s, "pump");
    assert_eq!(heads(&s), ["pump the tyres #house"]);
    assert_eq!(linked(&s), [true]);
}

#[test]
fn a_note_written_from_the_line_inside_a_tag_ends_with_it_and_never_twice() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);

    type_in(&mut s, "bleed the radiators");
    s.act(Action::Write);
    let new = s.selected().unwrap().clone();
    let held = s.notes.get(&new.id).unwrap();
    assert_eq!(held.text, "bleed the radiators #house");
    assert!(held.links.is_empty(), "a label is not a note to link to");
    assert!(new.linked);
    assert_eq!(
        heads(&s)[0],
        "bleed the radiators #house",
        "first, as newest"
    );

    for typed in ["#house call the plumber", "call the roofer #House"] {
        s.act(Action::Select(None));
        type_in(&mut s, typed);
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(
            s.notes.get(&new).unwrap().text,
            typed,
            "it carries it already"
        );
    }
}

#[test]
fn a_new_row_inside_a_tag_opens_with_it_and_its_first_letter_writes_it_there() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);

    assert!(s.act(Action::Write).is_empty(), "nothing is written yet");
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    assert!(!s.unwritten(), "the label alone is nothing to write");
    let id = s.field.as_ref().unwrap().id.clone();

    // The first letter goes before the label, where the caret is, and is written with it.
    let typed = edited(&mut s, "B #house");
    let [Effect::Keep(kept)] = &typed[..] else {
        panic!("one version: {typed:?}");
    };
    assert_eq!(kept.text, "B #house");
    assert_eq!(field(&s), Some(("B #house".to_owned(), false, false)));

    edited(&mut s, "Bleed the radiators /fri #house");
    let undo = s.undo.len();
    s.act(Action::Move(1));
    assert_eq!(
        s.notes.get(&id).unwrap().text,
        "Bleed the radiators 2026-10-16 #house",
        "date words are written out on leaving, and nothing is added"
    );
    assert_eq!(s.undo.len(), undo, "still one change to undo");
    s.act(Action::Select(None));
    assert_eq!(heads(&s)[0], "Bleed the radiators 2026-10-16 #house");
    assert!(s.rows[0].linked);

    // Only a note made here inherits: one edited here is left as it was written.
    type_in(&mut s, "move");
    select(&mut s, "house move plan");
    edited(&mut s, "house move plan, after the winter");
    s.act(Action::Select(None));
    assert_eq!(
        s.notes.get("plan").unwrap().text,
        "house move plan, after the winter"
    );
}

#[test]
fn a_new_row_inside_a_tag_with_only_the_tag_in_it_is_empty() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    let listed = heads(&s).len();

    // Left as it opened, it goes, and nothing was ever written.
    s.act(Action::Write);
    let offered = labels(&s);
    assert!(!offered.contains(&"go in") && !offered.contains(&"new note here"));
    for action in [Action::Write, Action::GoIn, Action::Archive, Action::Copy] {
        assert!(s.act(action).is_empty());
    }
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    assert!(s.act(Action::Select(None)).is_empty());
    assert_eq!(heads(&s).len(), listed, "no row remains");

    // Backspace at its start takes it away, as it would an empty row.
    s.act(Action::Move(1));
    s.act(Action::Write);
    assert_eq!(heads(&s).len(), listed + 1);
    assert!(s.act(Action::Erase).is_empty());
    assert_eq!(heads(&s).len(), listed);
    assert_eq!(s.cursor, Some(0), "up to the row it was opened under");

    // The editor leaving only the label leaves no note either.
    s.act(Action::Write);
    let id = s.field.as_ref().unwrap().id.clone();
    assert!(
        s.act(Action::Save {
            id,
            text: " #house\n".into()
        })
        .is_empty()
    );
    assert_eq!(heads(&s).len(), listed);

    // Nor does a body that pushes the label under an empty head, as Enter at its start does.
    s.act(Action::Write);
    s.act(Action::Open);
    edited(&mut s, "\n #house");
    assert!(s.act(Action::Close).is_empty());
    assert!(s.act(Action::Select(None)).is_empty());
    assert_eq!(heads(&s).len(), listed);
}

#[test]
fn a_new_row_inside_a_tag_put_back_or_cleared_is_as_it_opened() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);

    s.act(Action::Write);
    edited(&mut s, "order oil #house");
    let id = s.field.as_ref().unwrap().id.clone();
    assert!(s.notes.get(&id).is_some());
    s.act(Action::Undo);
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    assert!(s.notes.get(&id).is_none(), "its version is taken back");

    // Written out, then cleared back to its label and written out, it is as it opened.
    edited(&mut s, "order oil #house");
    s.act(Action::Close);
    assert_eq!(s.notes.get(&id).unwrap().text, "order oil #house");
    edited(&mut s, " #house");
    s.act(Action::Close);
    assert!(s.notes.get(&id).is_none());
    edited(&mut s, "order logs #house");
    s.act(Action::Move(1));
    assert_eq!(s.notes.get(&id).unwrap().text, "order logs #house");

    // The label taken out is out: what is seen is what is written. Taken out a letter at a
    // time from its end, what is left of it is no more the writer's than the whole of it.
    s.act(Action::Select(None));
    s.act(Action::Write);
    for left in (0..7).rev() {
        assert!(edited(&mut s, &" #house"[..left]).is_empty(), "{left}");
    }
    edited(&mut s, "sweep the chimney");
    let id = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&id).unwrap().text, "sweep the chimney");
}

#[test]
fn a_note_written_at_the_end_of_a_path_carries_every_label_on_it_and_links_to_its_last_note() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    type_in(&mut s, "boil");
    assert!(labels(&s).contains(&"go in"), "the line names one note");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "Boiler"]);
    assert_eq!(s.line, "");
    assert_eq!(s.view().here.unwrap().id, "boiler");

    type_in(&mut s, "bleed the radiators");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    let held = s.notes.get(&new).unwrap();
    assert_eq!(held.text, "bleed the radiators #house");
    assert_eq!(held.links, ["boiler"]);

    s.act(Action::Write);
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    edited(&mut s, "top up the pressure #house");
    let id = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&id).unwrap().text, "top up the pressure #house");
    assert_eq!(s.notes.get(&id).unwrap().links, ["boiler"]);

    // Labels after the note and before it, in the order of the path, the box at the start.
    go_to(&mut s, "[ ]");
    assert_eq!(path(&s), ["#house", "Boiler", "[ ]"]);
    s.act(Action::Write);
    assert_eq!(field(&s), Some(("[ ]  #house".to_owned(), true, false)));
    s.act(Action::Select(None));
    s.act(Action::Back);
    clear(&mut s);
    go_to(&mut s, "@joe");
    type_in(&mut s, "Ring about the flue");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    let held = s.notes.get(&new).unwrap();
    assert_eq!(held.text, "Ring about the flue #house @joe");
    assert_eq!(held.links, ["boiler"]);

    s.act(Action::Back);
    s.act(Action::Back);
    assert_eq!(path(&s), ["#house"]);
    assert_eq!(s.line, "boil", "the search that found the note");
}

#[test]
fn a_label_already_on_the_path_is_gone_back_to() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    type_in(&mut s, "#house");
    assert!(
        !labels(&s).contains(&"go in"),
        "the place you are in is not offered"
    );
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house"], "and is not gone into again");
    assert_eq!(s.line, "#house");

    clear(&mut s);
    type_in(&mut s, "boiler");
    s.act(Action::GoIn);
    type_in(&mut s, "#HOUSE");
    s.act(Action::GoIn);
    assert_eq!(
        path(&s),
        ["#house"],
        "back to it, in whatever case it is typed"
    );
    assert_eq!(
        s.line, "#HOUSE",
        "with something on the line, as for a note"
    );

    // Alone on the line it is gone back to though the place has it, being one with the
    // labels gone into after it.
    go_to(&mut s, "/today");
    assert_eq!(path(&s), ["#house", "2026-10-13"]);
    type_in(&mut s, "#house");
    assert!(labels(&s).contains(&"go in"));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house"], "back to it");
}

#[test]
fn going_in_is_offered_on_the_line_only_when_it_names_a_place_and_clear_comes_before_back() {
    let mut s = session();
    house(&mut s);
    let last = |s: &Session| s.view().hints.last().map(|h| (h.label, h.action.clone()));
    assert!(!labels(&s).contains(&"go in"));
    assert_ne!(labels(&s).last(), Some(&"clear"), "nothing to clear");

    for (typed, names) in [
        ("cake rec", true),
        ("cake recipe and", false),
        ("cake", false),
        ("#house", true),
        ("#house roof", false),
        ("@joe", true),
        ("/fri", true),
        ("/fri dentist", false),
    ] {
        clear(&mut s);
        type_in(&mut s, typed);
        assert_eq!(labels(&s).contains(&"go in"), names, "{typed}");
        assert_eq!(
            last(&s),
            Some(("clear", Action::Type(String::new()))),
            "{typed}"
        );
    }

    // What does not name one place is not gone into.
    clear(&mut s);
    type_in(&mut s, "cake recipe and");
    s.act(Action::GoIn);
    assert!(path(&s).is_empty());
    assert_eq!(s.line, "cake recipe and");
    put(&mut s, "cake2", "Cake Recipe", &[], 6_000);
    clear(&mut s);
    type_in(&mut s, "cake rec");
    assert!(!labels(&s).contains(&"go in"), "two notes share the name");
    s.act(Action::GoIn);
    assert!(path(&s).is_empty());

    clear(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    assert_eq!(last(&s), Some(("back", Action::Back)));
    type_in(&mut s, "roof");
    assert_eq!(last(&s), Some(("clear", Action::Type(String::new()))));
    assert!(!labels(&s).contains(&"back"));
    s.act(Action::Type(String::new()));
    assert_eq!(path(&s), ["#house"], "clearing stays where it is");
    assert_eq!(last(&s), Some(("back", Action::Back)));
}

#[test]
fn a_person_and_a_day_are_places_too() {
    let mut s = session();
    let friday = NOON.plus_secs(3 * 86_400).secs();
    put(&mut s, "ladder", "@Joe has the ladder", &[], 1_000);
    put(&mut s, "call", "call @joe about the boiler", &[], 2_000);
    put(&mut s, "dentist", "dentist 2026-10-16", &[], 3_000);
    put(&mut s, "keys", "pick up the keys", &[], friday);
    put(&mut s, "cake", "cake recipe", &[], 4_000);

    type_in(&mut s, "@joe");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["@joe"]);
    assert_eq!(
        heads(&s),
        ["call @joe about the boiler", "@Joe has the ladder"]
    );
    type_in(&mut s, "fix the gate");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(s.notes.get(&new).unwrap().text, "fix the gate @joe");
    s.act(Action::Back);
    assert_eq!(s.line, "@joe");

    clear(&mut s);
    type_in(&mut s, "/fri");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["2026-10-16"], "a day as it is written");
    assert_eq!(
        heads(&s),
        ["pick up the keys", "dentist 2026-10-16"],
        "written on the day, or mentioning it"
    );
    type_in(&mut s, "post the parcel");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(
        s.notes.get(&new).unwrap().text,
        "post the parcel 2026-10-16"
    );
    type_in(&mut s, "dentist again /fri");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(s.notes.get(&new).unwrap().text, "dentist again 2026-10-16");
    s.act(Action::Select(None));
    s.act(Action::Back);
    assert_eq!(s.line, "/fri", "the line as it was typed");
}

#[test]
fn inside_a_place_the_note_the_line_names_is_listed_first_and_gone_into() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);

    type_in(&mut s, "cake rec");
    assert_eq!(heads(&s), ["cake recipe"], "outside the tag, but named");
    assert_eq!(s.rows[0].reasons[0], "its name");
    assert!(!s.rows[0].linked, "named, not a fact of the place");
    assert!(labels(&s).contains(&"go in"));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "cake recipe"], "where the list said");

    s.act(Action::Back);
    for (typed, listed) in [
        ("roof", &["fix the roof tiles #house"][..]),
        ("cake", &[][..]),
    ] {
        clear(&mut s);
        type_in(&mut s, typed);
        assert_eq!(heads(&s), listed, "{typed} names nothing");
        assert!(!labels(&s).contains(&"go in"), "{typed}");
    }
}

#[test]
fn a_note_written_on_the_day_carries_it_and_one_in_another_day_has_it_written_in() {
    let mut s = session();
    let friday = NOON.plus_secs(3 * 86_400).secs();
    put(&mut s, "keys", "pick up the keys", &[], friday);

    type_in(&mut s, "/today");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["2026-10-13"]);
    type_in(&mut s, "water the plants");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(s.notes.get(&new).unwrap().text, "water the plants");
    s.act(Action::Select(None));
    s.act(Action::Write);
    assert_eq!(
        field(&s),
        Some((String::new(), true, false)),
        "nothing to add"
    );
    edited(&mut s, "feed the cat");
    let id = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&id).unwrap().text, "feed the cat");
    assert_eq!(
        heads(&s)[..2],
        ["feed the cat", "water the plants"],
        "both written on it, so both carry it"
    );

    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "/fri");
    s.act(Action::GoIn);
    let keys = s.rows.iter().find(|r| r.id == "keys").unwrap();
    assert!(
        keys.reasons.iter().all(|r| !r.contains("apart")),
        "a note of the day is near it, not near today: {:?}",
        keys.reasons
    );
    type_in(&mut s, "post the parcel");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(
        s.notes.get(&new).unwrap().text,
        "post the parcel 2026-10-16"
    );
    s.act(Action::Write);
    assert_eq!(field(&s), Some((" 2026-10-16".to_owned(), true, false)));
    edited(&mut s, "order oil 2026-10-16");
    let id = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&id).unwrap().text, "order oil 2026-10-16");
}

#[test]
fn today_gone_into_from_a_tag_is_todays_notes_in_it_and_what_is_written_there_carries_both() {
    let mut s = session();
    house(&mut s);
    put(&mut s, "boots", "dry the boots #house", &[], NOON.secs());
    put(&mut s, "plants", "water the plants", &[], NOON.secs());

    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    type_in(&mut s, "/today");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "2026-10-13"]);
    assert_eq!(
        heads(&s),
        ["dry the boots #house"],
        "not the tag's notes of other days, nor today's without the tag"
    );
    for (typed, listed, fact) in [
        ("boots", "dry the boots #house", true),
        ("plants", "water the plants", false),
        ("gutter", "clear the gutter #house", false),
    ] {
        type_in(&mut s, typed);
        assert_eq!(heads(&s), [listed], "the line narrows the two as one");
        assert_eq!(linked(&s), [fact], "{typed}");
        clear(&mut s);
    }

    type_in(&mut s, "Order oil");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(
        s.notes.get(&new).unwrap().text,
        "Order oil #house",
        "the tag written in, and the day carried by being written on it"
    );
    assert!(s.rows[0].linked, "and it is a fact of the place");

    s.act(Action::Select(None));
    s.act(Action::Write);
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    edited(&mut s, "Bleed radiators #house");
    let id = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&id).unwrap().text, "Bleed radiators #house");

    s.act(Action::Back);
    assert_eq!(path(&s), ["#house"], "back one rung at a time");
    assert_eq!(s.line, "/today");
}

#[test]
fn labels_gone_into_one_after_another_open_a_new_row_with_each_in_the_order_of_the_path() {
    let mut s = session();
    house(&mut s);
    put(&mut s, "ladder", "@joe has the ladder #house", &[], 1_500);
    put(&mut s, "gate", "@joe fixed the gate", &[], 1_600);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    type_in(&mut s, "@joe");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "@joe"]);
    assert_eq!(heads(&s), ["@joe has the ladder #house"]);

    s.act(Action::Write);
    assert_eq!(field(&s), Some((" #house @joe".to_owned(), true, false)));
    for left in (0..12).rev() {
        assert!(
            edited(&mut s, &" #house @joe"[..left]).is_empty(),
            "what is left of the labels is not the writer's: {left}"
        );
    }
    s.act(Action::Select(None));
    for (typed, written) in [
        ("Order oil", "Order oil #house @joe"),
        ("ask @Joe about the oil", "ask @Joe about the oil #house"),
    ] {
        s.act(Action::Select(None));
        type_in(&mut s, typed);
        s.act(Action::Write);
        let new = s.selected().unwrap().id.clone();
        assert_eq!(s.notes.get(&new).unwrap().text, written);
    }

    select(&mut s, "@joe has the ladder #house");
    s.act(Action::Link);
    assert_eq!(
        s.status,
        "nothing links to #house @joe; what you write here carries them"
    );

    s.act(Action::Select(None));
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "/fri");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "2026-10-16"]);
    s.act(Action::Write);
    assert_eq!(
        field(&s),
        Some((" #house 2026-10-16".to_owned(), true, false))
    );
}

#[test]
fn with_nothing_on_the_line_a_place_lists_only_its_facts_in_the_order_kept_and_no_scores() {
    let mut s = session();
    boiler(&mut s);
    put(&mut s, "gauge", "Boiler pressure gauge", &[], NOON.secs());
    go_to(&mut s, "Boiler");
    assert_eq!(path(&s), ["Boiler"]);
    assert_eq!(
        heads(&s),
        ["Pressure dropped #house", "Service booked"],
        "what is linked, and not the notes that share its words or its tag"
    );
    for row in &s.rows {
        assert!(row.linked && row.score.is_none());
        assert!(
            row.reasons.is_empty(),
            "nothing scored, and that it links here goes without saying"
        );
    }
    s.act(Action::Select(Some(0)));
    s.act(Action::Shift(1));
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["Service booked", "Pressure dropped #house"]);

    go_to(&mut s, "#house");
    assert_eq!(path(&s), ["Boiler", "#house"]);
    go_to(&mut s, "Pressure dropped #house");
    assert_eq!(heads(&s), ["Boiler"]);
    assert_eq!(s.rows[0].reasons, ["linked from here"]);
}

#[test]
fn typing_in_a_place_lists_the_facts_that_match_before_what_it_finds() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler\nthe old one", &[], 1_000);
    put(
        &mut s,
        "visit",
        "Engineer visit\npressure low",
        &["boiler"],
        2_000,
    );
    put(&mut s, "gauge", "Boiler pressure gauge", &[], 3_000);
    put(&mut s, "cake", "cake recipe", &[], 4_000);
    go_to(&mut s, "Boiler");
    assert_eq!(heads(&s), ["Engineer visit"]);
    assert_eq!(
        s.view().finds_from,
        None,
        "a place with nothing typed lists only its facts"
    );

    type_in(&mut s, "pressure");
    assert_eq!(
        heads(&s),
        ["Engineer visit", "Boiler pressure gauge"],
        "the fact first, though the find has the word in its head"
    );
    assert_eq!(linked(&s), [true, false]);
    assert_eq!(
        s.view().finds_from,
        Some(1),
        "where the faces set the finds apart"
    );

    // A fact that does not match leaves only finds, and nothing to set apart.
    clear(&mut s);
    type_in(&mut s, "gauge");
    assert_eq!(heads(&s), ["Boiler pressure gauge"]);
    assert_eq!(s.view().finds_from, None);
    clear(&mut s);
    type_in(&mut s, "pressure");
    assert!(
        s.rows
            .iter()
            .all(|r| r.score.is_some() && !r.reasons.is_empty())
    );
}

#[test]
fn a_note_and_the_labels_gone_into_after_it_are_one_place() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    go_to(&mut s, "/today");
    assert_eq!(path(&s), ["Boiler", "2026-10-13"]);
    assert_eq!(
        heads(&s),
        ["Pressure dropped #house"],
        "Boiler's notes from today"
    );
    assert_eq!(
        s.view().here.unwrap().id,
        "boiler",
        "its note is read there"
    );

    // What is written there links to the note and is of the day by being written on it.
    s.act(Action::Write);
    assert_eq!(field(&s), Some((String::new(), true, false)));
    let id = s.field.as_ref().unwrap().id.clone();
    edited(&mut s, "Bleed the radiators");
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&id).unwrap().links, ["boiler"]);
    assert_eq!(
        heads(&s),
        ["Bleed the radiators", "Pressure dropped #house"]
    );

    // Ctrl-L there is the note's, as in its own place.
    select(&mut s, "Bleed the radiators");
    assert!(labels(&s).contains(&"unlink"));
    s.act(Action::Link);
    assert!(s.notes.get(&id).unwrap().links.is_empty());
    assert_eq!(heads(&s), ["Pressure dropped #house"], "out of the place");
    s.act(Action::Select(None));
    type_in(&mut s, "oil");
    select(&mut s, "Order oil #house");
    assert!(labels(&s).contains(&"link"));

    // A note linked to it on another day is found there, and is not of the place.
    go_to(&mut s, "Boiler");
    go_to(&mut s, "/today");
    type_in(&mut s, "booked");
    assert_eq!(heads(&s), ["Service booked"]);
    assert_eq!(linked(&s), [false]);
    select(&mut s, "Service booked");
    assert!(labels(&s).contains(&"unlink"));

    // With a tag after it, what is in it carries the tag, and a new note carries it too.
    go_to(&mut s, "Boiler");
    go_to(&mut s, "#house");
    assert_eq!(path(&s), ["Boiler", "#house"]);
    assert_eq!(heads(&s), ["Pressure dropped #house"]);
    s.act(Action::Write);
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    s.act(Action::Select(None));
    type_in(&mut s, "Check the valve");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    let held = s.notes.get(&new).unwrap();
    assert_eq!(held.text, "Check the valve #house");
    assert_eq!(held.links, ["boiler"]);

    // A note gone into from there is a new place, and what is written in it still carries the
    // tag. The path names it as its row read there.
    go_to(&mut s, "Pressure dropped #house");
    assert_eq!(path(&s), ["Boiler", "#house", "Pressure dropped"]);
    s.act(Action::Write);
    assert_eq!(field(&s), Some((" #house".to_owned(), true, false)));
    assert_eq!(heads(&s)[1..], ["Boiler"]);
}

#[test]
fn the_note_of_a_place_with_labels_after_it_is_listed_when_named_and_gone_back_to() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    type_in(&mut s, "Boiler");
    assert!(
        !labels(&s).contains(&"go in") && !heads(&s).contains(&"Boiler"),
        "in its own place it names where you are"
    );

    go_to(&mut s, "/today");
    type_in(&mut s, "Boiler");
    assert_eq!(heads(&s)[0], "Boiler", "where Tab goes");
    assert_eq!(s.rows[0].reasons[0], "its name");
    assert!(!s.rows[0].linked, "named, not a fact of the place");
    assert!(labels(&s).contains(&"go in"));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["Boiler"], "back to it");

    // Its row has nothing to link to.
    go_to(&mut s, "/today");
    type_in(&mut s, "Boiler");
    select(&mut s, "Boiler");
    assert!(!labels(&s).contains(&"link") && !labels(&s).contains(&"unlink"));
    let undo = s.undo.len();
    s.act(Action::Link);
    assert_eq!(s.undo.len(), undo, "nothing was written");
    assert!(s.notes.get("boiler").unwrap().links.is_empty());
}

#[test]
fn a_note_lists_the_same_however_it_was_reached_and_writes_the_path_it_was_reached_by() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "#house");
    go_to(&mut s, "Boiler");
    assert_eq!(path(&s), ["#house", "Boiler"]);
    assert_eq!(
        heads(&s),
        ["Pressure dropped #house", "Service booked"],
        "Boiler's notes, with the tag or without"
    );
    s.act(Action::Write);
    assert_eq!(
        field(&s),
        Some((" #house".to_owned(), true, false)),
        "the tag it was reached by"
    );
    s.act(Action::Select(None));
    type_in(&mut s, "Call the engineer");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    let held = s.notes.get(&new).unwrap();
    assert_eq!(held.text, "Call the engineer #house");
    assert_eq!(held.links, ["boiler"]);

    // Today after the note is the place Boiler then today is.
    go_to(&mut s, "/today");
    assert_eq!(path(&s), ["#house", "Boiler", "2026-10-13"]);
    let here = heads(&s).join("|");
    s.act(Action::Back);
    s.act(Action::Back);
    s.act(Action::Back);
    go_to(&mut s, "Boiler");
    go_to(&mut s, "/today");
    assert_eq!(heads(&s).join("|"), here);
    assert_eq!(here, "Call the engineer #house|Pressure dropped #house");

    // Reached without the tag, a note written there is written without it.
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "Ask about the warranty");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(s.notes.get(&new).unwrap().text, "Ask about the warranty");
}

#[test]
fn what_carries_a_label_is_listed_whatever_the_weights_give_it() {
    let nothing = Weights {
        lexical: 0.0,
        explicit: 0.0,
        temporal: 0.0,
        ..Weights::default()
    };
    let mut s = Session::new(Box::new(Stopped::default()), "dev", nothing, Notes::new());
    put(&mut s, "dentist", "dentist 2026-10-16", &[], 3_000);
    put(&mut s, "gutter", "clear the gutter #house", &[], 2_000);
    type_in(&mut s, "/fri");
    s.act(Action::GoIn);
    assert_eq!(heads(&s), ["dentist 2026-10-16"]);
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    assert_eq!(heads(&s), ["clear the gutter #house"]);
    assert_eq!(linked(&s), [true]);
}

#[test]
fn linking_inside_a_label_does_nothing_but_say_so() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house");
    s.act(Action::GoIn);
    for (typed, head) in [("", "Boiler"), ("move", "house move plan")] {
        s.act(Action::Select(None));
        clear(&mut s);
        type_in(&mut s, typed);
        select(&mut s, head);
        assert!(!labels(&s).contains(&"link") && !labels(&s).contains(&"unlink"));
        let undo = s.undo.len();
        s.act(Action::Link);
        assert_eq!(s.undo.len(), undo, "nothing was written");
        assert!(
            s.status.starts_with("nothing links to #house"),
            "{}",
            s.status
        );
    }
}

#[test]
fn going_into_a_note_the_line_holds_was_a_search_and_a_draft_carrying_a_tag_is_not() {
    let mut s = session();
    house(&mut s);
    for typed in ["~fr", "\"roof tiles\"", "#house roof"] {
        clear(&mut s);
        type_in(&mut s, typed);
        go_into(&mut s, "fix the roof tiles #house");
        assert_eq!(s.line, "", "{typed} was a search");
        s.act(Action::Back);
        assert_eq!(s.line, typed);
    }
    clear(&mut s);
    type_in(&mut s, "boiler needs bleeding #house");
    go_into(&mut s, "Boiler");
    assert_eq!(
        s.line, "boiler needs bleeding #house",
        "a draft, carried in to be written"
    );
}

#[test]
fn the_finds_begin_after_every_fact_however_the_named_rows_mix() {
    let mut s = session();
    put(&mut s, "hub", "Boiler #house", &[], 1_000);
    put(&mut s, "a", "Oil", &["hub"], 2_000);
    put(&mut s, "b", "Oil", &[], 3_000);
    put(&mut s, "c", "oil tank check", &["hub"], 4_000);
    put(&mut s, "d", "oil boiler service #house", &[], 5_000);
    go_to(&mut s, "boiler");
    type_in(&mut s, "oil");
    let rows: Vec<_> = s.rows.iter().map(|r| (r.id.as_str(), r.linked)).collect();
    // Both notes named come first, the fact before the other, and then the place's facts.
    assert_eq!(rows, [("a", true), ("b", false), ("c", true), ("d", false)]);
    assert_eq!(s.view().finds_from, Some(3));

    // A new row opened above the finds does not move where they begin.
    s.act(Action::Move(1));
    s.act(Action::Write);
    assert_eq!(s.rows[s.view().finds_from.unwrap()].id, "d");
}

#[test]
fn the_note_you_are_in_opens_from_the_line_and_closes_back_onto_it() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    assert_eq!(path(&s), ["Boiler"]);
    let before: Vec<String> = heads(&s).into_iter().map(str::to_owned).collect();
    assert!(
        !before.contains(&"Boiler".to_owned()),
        "where you are is not listed"
    );

    // It is put under the cursor, first, as the note the line names is listed.
    assert!(s.act(Action::Open).is_empty());
    assert_eq!(field(&s), Some(("Boiler\n#house".into(), false, true)));
    assert_eq!((s.cursor, s.rows[0].id.as_str()), (Some(0), "boiler"));
    assert_eq!(heads(&s)[1..], before);
    assert!(
        s.act(Action::Close).is_empty(),
        "nothing changed, nothing written"
    );
    assert_eq!((s.cursor, field(&s)), (None, None), "back on the line");
    assert_eq!(heads(&s), before, "and the list is as it was");

    // What is written there is written when it closes, as a row's body is.
    s.act(Action::Open);
    edited(&mut s, "Boiler\n#house\nthe combi under the stairs");
    let done = s.act(Action::Close);
    assert_eq!(kept(&done).len(), 1);
    assert_eq!((s.cursor, field(&s)), (None, None));
    assert_eq!(heads(&s), before);
    assert_eq!(
        s.view().here.map(|here| here.text),
        Some("Boiler\n#house\nthe combi under the stairs")
    );
    assert_eq!(path(&s), ["Boiler"], "still in it");

    // Leaving it for another row, as a press on the page does, takes its row away too.
    s.act(Action::Open);
    s.act(Action::Select(Some(2)));
    assert_eq!(heads(&s), before);
    assert_eq!(s.rows[s.cursor.unwrap()].head, before[1]);
}

#[test]
fn the_note_you_are_in_listed_by_its_name_is_opened_where_it_is_listed() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    go_to(&mut s, "#house");
    type_in(&mut s, "Boiler");
    assert_eq!(s.rows[0].id, "boiler", "the line names it");
    let listed = s.rows.len();
    s.act(Action::Open);
    assert_eq!(
        (s.cursor, s.rows.len()),
        (Some(0), listed),
        "not listed twice"
    );
    s.act(Action::Close);
    assert_eq!((s.cursor, s.rows.len()), (None, listed));
    assert_eq!(s.rows[0].id, "boiler", "its row stays, being listed");
}

#[test]
fn on_the_line_inside_labels_alone_or_at_the_top_there_is_still_nothing_to_open() {
    let mut s = session();
    boiler(&mut s);
    s.act(Action::Select(None));
    for at in ["the top", "#house"] {
        if at != "the top" {
            go_to(&mut s, at);
        }
        let before: Vec<String> = heads(&s).into_iter().map(str::to_owned).collect();
        assert!(s.act(Action::Open).is_empty(), "{at}");
        assert!(s.act(Action::Editor).is_empty(), "{at}");
        assert_eq!((s.cursor, field(&s)), (None, None), "{at}");
        assert_eq!(heads(&s), before, "{at}");
        assert!(
            !labels(&s).iter().any(|&l| l == "body" || l == "editor"),
            "{at}: {:?}",
            labels(&s)
        );
    }
}

#[test]
fn the_body_and_the_editor_are_offered_on_the_line_only_in_a_note() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    assert_eq!(
        labels(&s),
        [
            "new note here",
            "body",
            "editor",
            "pick a note",
            "show archived",
            "undo",
            "back"
        ]
    );
    // Opened from the line it offers what a row's body does.
    s.act(Action::Open);
    assert_eq!(labels(&s), ["delete", "done"]);
    edited(&mut s, "Boiler\n#house\nthe combi");
    assert_eq!(labels(&s), ["put back", "delete", "done"]);
    s.act(Action::Close);
    assert_eq!(labels(&s)[..3], ["new note here", "body", "editor"]);
}

#[test]
fn the_editor_is_handed_the_note_you_are_in_from_the_line_and_the_cursor_stays_there() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    assert_eq!(
        s.act(Action::Editor),
        [Effect::Editor {
            id: "boiler".into(),
            text: "Boiler\n#house".into(),
        }]
    );
    assert_eq!(s.cursor, None);
    let saved = s.act(Action::Save {
        id: "boiler".into(),
        text: "Boiler\n#house\nthe combi".into(),
    });
    assert_eq!(kept(&saved).len(), 1);
    assert_eq!((s.cursor, field(&s)), (None, None));
    assert_eq!(
        s.view().here.map(|here| here.text),
        Some("Boiler\n#house\nthe combi")
    );
}

/// Whether the session has stepped out of Boiler, deleted, onto the line with what it says.
fn stepped_out(s: &Session, line: &str) {
    assert!(s.notes.get("boiler").is_none(), "deleted");
    assert!(path(s).is_empty(), "out to just before it");
    assert_eq!((s.line.as_str(), s.cursor, field(s)), (line, None, None));
    assert_eq!(s.view().status, "deleted · undo brings it back");
}

#[test]
fn the_note_you_are_in_deleted_from_its_body_opened_from_the_line_steps_out_of_it() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    s.act(Action::Open);
    s.act(Action::Delete);
    stepped_out(&s, "Boiler");

    // Undo brings it back, under the cursor where the line finds it.
    s.act(Action::Undo);
    assert!(s.notes.get("boiler").is_some());
    assert_eq!(s.selected().map(|r| r.id.as_str()), Some("boiler"));
}

#[test]
fn the_note_you_are_in_deleted_from_its_row_steps_out_of_it_and_keeps_the_line() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    go_to(&mut s, "#house");
    type_in(&mut s, "Boiler");
    assert_eq!(s.rows[0].id, "boiler", "the line names it");
    s.act(Action::Select(Some(0)));
    assert!(labels(&s).contains(&"delete"));
    s.act(Action::Delete);
    stepped_out(&s, "Boiler");
}

#[test]
fn the_note_you_are_in_emptied_from_the_line_is_deleted_and_steps_out_of_it() {
    let mut s = session();
    boiler(&mut s);

    // Its body cleared in the sheet and closed.
    go_to(&mut s, "Boiler");
    s.act(Action::Open);
    edited(&mut s, "");
    let done = s.act(Action::Close);
    assert!(kept(&done).iter().any(|r| r.id == "boiler" && r.deleted));
    stepped_out(&s, "Boiler");
    s.act(Action::Undo);

    // An editor that leaves nothing.
    go_to(&mut s, "Boiler");
    s.act(Action::Editor);
    s.act(Action::Save {
        id: "boiler".into(),
        text: "\n".into(),
    });
    stepped_out(&s, "Boiler");
    s.act(Action::Undo);
    assert!(s.notes.get("boiler").is_some(), "undo brings it back");
}

#[test]
fn the_note_you_are_in_deleted_elsewhere_steps_out_of_it_once_the_cursor_is_on_the_line() {
    let mut s = session();
    boiler(&mut s);
    let gone = |s: &Session, at: i64| Record {
        deleted: !s.notes.version("boiler").unwrap().deleted,
        stamp: Some(NOON.plus_secs(at)),
        device: "3f9a01c2".into(),
        ..s.notes.version("boiler").unwrap().clone()
    };
    go_to(&mut s, "#house");
    go_into(&mut s, "Boiler");
    assert_eq!(path(&s), ["#house", "Boiler"]);
    assert!(s.absorb([gone(&s, 60)]));
    assert_eq!(path(&s), ["#house"], "out to just before it");
    assert!(s.view().here.is_none());

    // While the cursor walks the list, the list holds still, the path with it, until the
    // cursor is back on the line.
    s.absorb([gone(&s, 120)]);
    go_into(&mut s, "Boiler");
    s.act(Action::Move(1));
    s.absorb([gone(&s, 180)]);
    assert_eq!(path(&s), ["#house", "Boiler"]);

    // Nor does what is done on the row pull it away, walking on or writing in its body, and
    // what was written there is kept as the cursor leaves for the line.
    s.act(Action::Move(1));
    s.act(Action::Open);
    let written = format!("{}\nfor tuesday", field(&s).unwrap().0);
    edited(&mut s, &written);
    assert_eq!(path(&s), ["#house", "Boiler"]);
    assert_eq!(
        field(&s).map(|(text, _, whole)| (text, whole)),
        Some((written.clone(), true))
    );
    let effects = s.act(Action::Select(None));
    assert_eq!(path(&s), ["#house"]);
    assert!(
        kept(&effects).iter().any(|r| r.text == written),
        "the body is not lost"
    );
}

#[test]
fn two_steps_out_of_labels_gone_into_in_one_tab_end_with_the_outer_one_on_the_line() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house #garden");
    s.act(Action::GoIn);
    s.act(Action::Back);
    assert_eq!((path(&s), s.line.as_str()), (vec!["#house"], "#garden"));
    s.act(Action::Back);
    assert!(path(&s).is_empty());
    assert_eq!(
        s.line, "#house",
        "as two steps out of one made a label at a time"
    );
}

#[test]
fn a_line_changed_after_a_step_out_put_it_back_is_kept_by_the_next() {
    let mut s = session();
    house(&mut s);
    type_in(&mut s, "#house #garden");
    s.act(Action::GoIn);
    s.act(Action::Back);
    type_in(&mut s, " shed");
    s.act(Action::Back);
    assert!(path(&s).is_empty());
    assert_eq!(s.line, "#garden shed");

    // Typed again as it was put back, it is the writer's all the same.
    clear(&mut s);
    type_in(&mut s, "#house #garden");
    s.act(Action::GoIn);
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "#garden");
    s.act(Action::Back);
    assert!(path(&s).is_empty());
    assert_eq!(s.line, "#garden");
}

/// Each row as it reads off the cursor, with its arrow and what it says.
fn read(s: &Session) -> Vec<(&str, Vec<&str>, Vec<&str>)> {
    s.rows
        .iter()
        .map(|r| {
            (
                r.shown.as_deref().unwrap_or(&r.head),
                r.links.iter().map(String::as_str).collect(),
                r.reasons.iter().map(String::as_str).collect(),
            )
        })
        .collect()
}

#[test]
fn what_is_written_under_a_task_stays_in_every_place_stepped_back_through() {
    let mut s = session();
    put(&mut s, "overhaul", "Boiler overhaul #house", &[], 1_000);
    put(&mut s, "oil", "Order oil #house", &[], 2_000);
    go_to(&mut s, "#house");
    go_to(&mut s, "boiler overhaul");
    go_to(&mut s, "[ ]");
    assert_eq!(path(&s), ["#house", "Boiler overhaul", "[ ]"]);

    s.act(Action::Write);
    assert_eq!(field(&s), Some(("[ ]  #house".to_owned(), true, false)));
    edited(&mut s, "[ ] Bleed the radiators #house");
    let bleed = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&bleed).unwrap().links, ["overhaul"]);

    go_into(&mut s, "[ ] Bleed the radiators #house");
    assert_eq!(
        path(&s),
        [
            "#house",
            "Boiler overhaul",
            "[ ]",
            "[ ] Bleed the radiators"
        ]
    );
    s.act(Action::Write);
    assert_eq!(field(&s), Some(("[ ]  #house".to_owned(), true, false)));
    edited(&mut s, "[ ] Buy a radiator key #house");
    let key = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    let held = s.notes.get(&key).unwrap();
    assert_eq!(held.text, "[ ] Buy a radiator key #house");
    assert_eq!(held.links, [bleed.as_str()]);

    s.act(Action::Back);
    assert_eq!(path(&s), ["#house", "Boiler overhaul", "[ ]"]);
    assert_eq!(s.line, "");
    assert_eq!(
        drawn(&s),
        ["[ ] Bleed the radiators", "  [ ] Buy a radiator key"]
    );

    s.act(Action::Back);
    assert_eq!(path(&s), ["#house", "Boiler overhaul"]);
    assert_eq!(s.line, "[ ]");
    clear(&mut s);
    assert_eq!(
        drawn(&s),
        ["[ ] Bleed the radiators", "  [ ] Buy a radiator key"]
    );

    s.act(Action::Back);
    assert_eq!(path(&s), ["#house"]);
    assert_eq!(s.line, "boiler overhaul");
    clear(&mut s);
    assert_eq!(
        drawn(&s),
        [
            "Order oil",
            "Boiler overhaul",
            "  [ ] Bleed the radiators",
            "    [ ] Buy a radiator key"
        ],
        "all three by their tag"
    );
}

/// House, with a boiler overhaul in it, a task under the overhaul and a task under that, and a
/// pressure gauge that is not in it.
fn overhaul(s: &mut Session) {
    put(s, "house", "House", &[], 1_000);
    put(s, "overhaul", "Boiler overhaul", &["house"], 2_000);
    put(s, "bleed", "[ ] Bleed the radiators", &["overhaul"], 3_000);
    put(s, "key", "[ ] Buy a radiator key", &["bleed"], 4_000);
    put(s, "gauge", "Pressure gauge", &[], 5_000);
}

#[test]
fn unlinking_a_note_takes_what_is_under_it_out_of_the_place_with_it() {
    let mut s = session();
    overhaul(&mut s);
    go_to(&mut s, "House");
    assert_eq!(
        drawn(&s),
        [
            "Boiler overhaul",
            "  [ ] Bleed the radiators",
            "    [ ] Buy a radiator key"
        ]
    );
    select(&mut s, "Boiler overhaul");
    assert!(labels(&s).contains(&"unlink"));
    s.act(Action::Link);
    assert!(s.notes.get("overhaul").unwrap().links.is_empty());
    assert!(heads(&s).is_empty(), "and all that is under it");

    type_in(&mut s, "boiler overhaul");
    select(&mut s, "Boiler overhaul");
    assert!(labels(&s).contains(&"link"));
    s.act(Action::Link);
    s.act(Action::Select(None));
    clear(&mut s);
    assert_eq!(heads(&s).len(), 3, "all of it back");
}

#[test]
fn the_link_chord_offers_nothing_on_a_row_under_another_note_and_says_why() {
    let mut s = session();
    overhaul(&mut s);
    go_to(&mut s, "House");
    for typed in ["", "radiator key"] {
        s.act(Action::Select(None));
        clear(&mut s);
        type_in(&mut s, typed);
        select(&mut s, "[ ] Buy a radiator key");
        assert!(!labels(&s).contains(&"link") && !labels(&s).contains(&"unlink"));
        let undo = s.undo.len();
        s.act(Action::Link);
        assert_eq!(s.undo.len(), undo, "nothing was written");
        assert_eq!(
            s.status, "it is under [ ] Bleed the radiators; unlink it there",
            "named, since a row under the cursor may not show its arrow"
        );
    }
    s.act(Action::Select(None));
    clear(&mut s);
    select(&mut s, "Boiler overhaul");
    assert!(labels(&s).contains(&"unlink"));
}

#[test]
fn a_delete_in_the_middle_takes_what_was_under_it_where_it_stands_and_undo_puts_it_back() {
    let mut s = session();
    overhaul(&mut s);
    put(&mut s, "porch", "Sweep the porch", &["house"], 6_000);
    put(&mut s, "boots", "Boots by the door", &["porch"], 7_000);
    go_to(&mut s, "House");
    let all = heads(&s).join("|");
    select(&mut s, "Boiler overhaul");
    s.act(Action::Delete);
    assert_eq!(
        heads(&s),
        ["Sweep the porch", "Boots by the door"],
        "what was in the place only by being under it goes with it"
    );
    assert_eq!(
        s.selected().map(|r| r.head.as_str()),
        Some("Boots by the door"),
        "the cursor on the row that took the last one's place"
    );
    assert_eq!(s.notes.get("bleed").unwrap().links, ["overhaul"]);

    s.act(Action::Undo);
    s.act(Action::Select(None));
    assert_eq!(heads(&s).join("|"), all, "undo puts the chain back");
}

#[test]
fn an_arrow_is_drawn_again_when_the_note_it_names_changes() {
    let mut s = session();
    overhaul(&mut s);
    go_to(&mut s, "Boiler overhaul");
    let key = |s: &Session| s.rows.iter().find(|r| r.id == "key").unwrap().links.clone();
    assert!(key(&s).is_empty(), "drawn under what it names");
    s.act(Action::Type("radiator".into()));
    assert_eq!(key(&s), ["[ ] Bleed the radiators"]);
    select(&mut s, "[ ] Bleed the radiators");
    s.act(Action::Archive);
    assert_eq!(
        key(&s),
        ["[x] Bleed the radiators"],
        "ticked, with the cursor still on the list"
    );

    // A version from elsewhere is followed too.
    s.absorb([theirs("bleed", "[x] Bleed every radiator", 60)]);
    assert_eq!(key(&s), ["[x] Bleed every radiator"]);
}

#[test]
fn a_note_gone_into_by_name_from_a_place_is_not_under_it_and_its_search_comes_back() {
    let mut s = session();
    overhaul(&mut s);
    go_to(&mut s, "House");
    go_to(&mut s, "pressure gauge");
    assert_eq!(path(&s), ["House", "Pressure gauge"]);
    type_in(&mut s, "Replace the dial");
    s.act(Action::Write);
    let dial = s.selected().unwrap().id.clone();
    assert_eq!(s.notes.get(&dial).unwrap().links, ["gauge"]);

    s.act(Action::Select(None));
    s.act(Action::Back);
    assert_eq!(s.line, "pressure gauge");
    assert_eq!(heads(&s), ["Pressure gauge"]);
    assert_eq!(linked(&s), [false], "found, and not in the place");
    s.act(Action::GoIn);
    assert_eq!(heads(&s), ["Replace the dial"], "where the search leads");

    s.act(Action::Back);
    clear(&mut s);
    assert_eq!(
        heads(&s),
        [
            "Boiler overhaul",
            "[ ] Bleed the radiators",
            "[ ] Buy a radiator key"
        ],
        "neither is under House"
    );
}

#[test]
fn a_name_finds_one_hop_down_and_going_in_shows_every_depth() {
    let mut s = session();
    overhaul(&mut s);
    type_in(&mut s, "boiler overhaul ");
    assert_eq!(heads(&s), ["Boiler overhaul", "[ ] Bleed the radiators"]);
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["Boiler overhaul"]);
    assert_eq!(
        read(&s),
        [
            ("[ ] Bleed the radiators", vec![], vec![]),
            ("[ ] Buy a radiator key", vec![], vec![]),
            ("House", vec![], vec!["linked from here"]),
        ]
    );
    assert_eq!(
        drawn(&s),
        [
            "[ ] Bleed the radiators",
            "  [ ] Buy a radiator key",
            "House"
        ]
    );
}

#[test]
fn the_link_chord_asks_of_the_notes_as_they_are_now_and_not_of_the_row() {
    let mut s = session();
    overhaul(&mut s);
    go_to(&mut s, "House");
    select(&mut s, "[ ] Buy a radiator key");
    assert!(!labels(&s).contains(&"link"));

    // Elsewhere the task it is under is taken out of the overhaul. Its row holds still, and is no
    // longer in the place.
    s.absorb([theirs("bleed", "[ ] Bleed the radiators", 60)]);
    assert!(s.selected().is_some_and(|r| r.id == "key" && r.linked));
    assert!(labels(&s).contains(&"link"), "{:?}", labels(&s));
    s.act(Action::Link);
    assert_eq!(s.notes.get("key").unwrap().links, ["bleed", "house"]);
    assert_eq!(s.status, "linked [ ] Buy a radiator key");
}

#[test]
fn unlinking_a_note_still_under_the_place_through_another_says_so() {
    let mut s = session();
    overhaul(&mut s);
    put(
        &mut s,
        "key",
        "[ ] Buy a radiator key",
        &["bleed", "house"],
        4_000,
    );
    go_to(&mut s, "House");
    select(&mut s, "[ ] Buy a radiator key");
    assert!(labels(&s).contains(&"unlink"));
    s.act(Action::Link);
    assert_eq!(s.notes.get("key").unwrap().links, ["bleed"]);
    assert!(heads(&s).contains(&"[ ] Buy a radiator key"), "it stays");
    assert_eq!(
        s.status,
        "unlinked [ ] Buy a radiator key · still under [ ] Bleed the radiators"
    );
}

#[test]
fn a_reason_and_a_status_name_a_note_as_its_row_reads() {
    let mut s = session();
    put(&mut s, "overhaul", "Boiler overhaul #house", &[], 1_000);
    put(
        &mut s,
        "ring",
        "Ring the engineer #house",
        &["overhaul"],
        2_000,
    );
    put(&mut s, "flue", "Check the flue #house", &["ring"], 3_000);

    // At the top nothing is on the path, and a reason leaves off what the row's own head ends
    // with, as its arrow does.
    type_in(&mut s, "boiler overhaul ");
    let ring = s.rows.iter().find(|r| r.id == "ring").unwrap();
    assert_eq!(ring.links, ["Boiler overhaul"]);
    assert!(
        ring.reasons
            .iter()
            .any(|r| r.contains("about Boiler overhaul"))
            && ring.reasons.iter().all(|r| !r.contains("#house")),
        "{:?}",
        ring.reasons
    );
    clear(&mut s);

    go_to(&mut s, "#house");
    go_to(&mut s, "boiler overhaul");
    type_in(&mut s, "engineer");
    let flue = s.rows.iter().find(|r| r.id == "flue").unwrap();
    assert!(
        flue.reasons
            .iter()
            .any(|r| r.contains("about Ring the engineer"))
            && flue.reasons.iter().all(|r| !r.contains("#house")),
        "{:?}",
        flue.reasons
    );
    clear(&mut s);
    type_in(&mut s, "boiler overhaul ");
    let ring = s.rows.iter().find(|r| r.id == "ring").unwrap();
    assert!(
        ring.reasons
            .iter()
            .any(|r| r.ends_with("about Boiler overhaul")),
        "the place's own note, which has no arrow: {:?}",
        ring.reasons
    );

    s.act(Action::Select(None));
    clear(&mut s);
    select(&mut s, "Ring the engineer #house");
    s.act(Action::Link);
    assert_eq!(s.status, "unlinked Ring the engineer");
    s.act(Action::Undo);
    assert_eq!(s.status, "undid: Ring the engineer");
}

#[test]
fn a_link_followed_from_an_open_body_writes_the_body_and_goes_into_the_note_it_names() {
    let mut s = session();
    boiler(&mut s);
    select(&mut s, "Order oil #house");
    s.act(Action::Open);
    edited(&mut s, "Order oil #house\nask about [[boiler]] first");
    s.act(Action::Follow("boiler".into()));
    assert_eq!(
        path(&s),
        ["Boiler"],
        "named in any case, as brackets name it"
    );
    assert_eq!(
        s.notes.get("oil").unwrap().text,
        "Order oil #house\nask about [[boiler]] first",
        "the body is written as it is left"
    );
    assert_eq!(s.notes.get("oil").unwrap().links, ["boiler"]);
    s.act(Action::Back);
    assert!(path(&s).is_empty());

    // A name for no note, or one two notes share, goes nowhere, leaves the body open, and says
    // why.
    select(&mut s, "Order oil #house");
    s.act(Action::Open);
    s.act(Action::Follow("Nowhere".into()));
    assert!(field(&s).is_some_and(|(_, _, whole)| whole));
    assert_eq!(
        (path(&s).len(), s.status.as_str()),
        (0, "no note is called Nowhere")
    );
    put(&mut s, "g1", "Garage", &[], NOON.secs());
    put(&mut s, "g2", "garage", &[], NOON.secs());
    s.act(Action::Follow("Garage".into()));
    assert_eq!(
        (path(&s).len(), s.status.as_str()),
        (0, "2 notes are called Garage")
    );
}

#[test]
fn stepping_back_out_of_a_note_gone_into_from_its_row_puts_the_cursor_back_on_that_row() {
    let mut s = session();
    boiler(&mut s);
    let order: Vec<String> = heads(&s).into_iter().map(String::from).collect();
    go_into(&mut s, "Pressure dropped #house");
    assert_eq!(path(&s), ["Pressure dropped #house"]);
    assert_eq!(s.cursor, None, "inside, the cursor is on the line");
    s.act(Action::Back);
    assert_eq!(heads(&s), order, "the list is as it was");
    assert_eq!(
        s.selected().map(|r| r.head.as_str()),
        Some("Pressure dropped #house")
    );

    // From a search, the search comes back with the cursor on the row it found.
    s.act(Action::Select(None));
    type_in(&mut s, "oil");
    go_into(&mut s, "Order oil #house");
    s.act(Action::Back);
    assert_eq!(s.line, "oil");
    s.act(Action::Back);
    assert_eq!(s.line, "oil", "with nowhere to step back to");
    assert_eq!(
        s.selected().map(|r| r.head.as_str()),
        Some("Order oil #house")
    );

    // A note gone into from the line is stepped out of onto the line, as before.
    go_to(&mut s, "Boiler");
    assert_eq!(path(&s), ["Boiler"]);
    s.act(Action::Back);
    assert_eq!(s.cursor, None);
}
