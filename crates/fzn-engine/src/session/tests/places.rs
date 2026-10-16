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
    s.act(Action::Edit);
    edited(&mut s, "\n #house");
    assert!(s.act(Action::Done).is_empty());
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
    s.act(Action::Done);
    assert_eq!(s.notes.get(&id).unwrap().text, "order oil #house");
    edited(&mut s, " #house");
    s.act(Action::Done);
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
fn inside_a_tag_and_then_a_note_a_new_note_links_to_the_note_and_gets_no_tag() {
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
    assert_eq!(held.text, "bleed the radiators");
    assert_eq!(held.links, ["boiler"]);

    s.act(Action::Write);
    edited(&mut s, "top up the pressure");
    let id = s.field.as_ref().unwrap().id.clone();
    s.act(Action::Select(None));
    assert_eq!(s.notes.get(&id).unwrap().text, "top up the pressure");
    assert_eq!(s.notes.get(&id).unwrap().links, ["boiler"]);

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
        assert_eq!(
            row.reasons,
            ["links here"],
            "how it is linked, and nothing scored"
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

    type_in(&mut s, "pressure");
    assert_eq!(
        heads(&s),
        ["Engineer visit", "Boiler pressure gauge"],
        "the fact first, though the find has the word in its head"
    );
    assert_eq!(linked(&s), [true, false]);
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

    // Ctrl-L there is the note's, as in its own zoom.
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

    // A note gone into from there starts a new place.
    go_to(&mut s, "Pressure dropped #house");
    assert_eq!(path(&s), ["Boiler", "#house", "Pressure dropped #house"]);
    s.act(Action::Write);
    assert_eq!(field(&s), Some((String::new(), true, false)));
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
        "in its own zoom it names where you are"
    );

    go_to(&mut s, "/today");
    type_in(&mut s, "Boiler");
    assert_eq!(heads(&s)[0], "Boiler", "where Right goes");
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
fn the_labels_before_a_note_are_only_how_it_was_reached() {
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
    assert_eq!(field(&s), Some((String::new(), true, false)), "no tag");
    s.act(Action::Select(None));
    type_in(&mut s, "Call the engineer");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    let held = s.notes.get(&new).unwrap();
    assert_eq!(held.text, "Call the engineer");
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
    assert_eq!(here, "Call the engineer|Pressure dropped #house");
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
