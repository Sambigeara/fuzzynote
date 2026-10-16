//! The field: writing in place, a visit to a row at a time, and what undo makes of it.

use super::*;

#[test]
fn enter_on_an_empty_line_opens_a_note_to_write_in_place_linked_to_where_you_are() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "house", "House", &["boiler"], 2_000);
    go_into(&mut s, "Boiler");
    assert_eq!(heads(&s), ["House"]);

    assert!(s.act(Action::Write).is_empty(), "nothing is written yet");
    assert_eq!(field(&s), Some((String::new(), true, false)));
    assert_eq!(heads(&s), ["", "House"], "a new row at the top");
    assert!(s.rows[0].linked, "it will be linked to where you are");
    assert_eq!(s.cursor, Some(0));
    assert_eq!(s.line, "");
    assert_eq!(s.view().here.unwrap().text, "Boiler");

    let typed = edited(&mut s, "pressure dropped to 0.8\nbleed the radiators /fri");
    let [new] = kept(&typed)[..] else {
        panic!("{typed:?}");
    };
    assert_eq!(new.id, "new-0001", "the id it was given when it opened");
    assert_eq!(
        new.text, "pressure dropped to 0.8\nbleed the radiators /fri",
        "as it was typed"
    );
    assert_eq!(new.links, ["boiler"]);
    assert_eq!((new.created, new.updated), (Some(NOON), Some(NOON)));
    let done = s.act(Action::Close);
    let [new] = kept(&done)[..] else {
        panic!("{done:?}");
    };
    assert_eq!(
        new.text, "pressure dropped to 0.8\nbleed the radiators 2026-10-16",
        "and its date word written out"
    );
    assert_eq!(
        field(&s),
        Some((new.text.clone(), false, false)),
        "the cursor stays on it, a note now"
    );
    assert_eq!(heads(&s), ["pressure dropped to 0.8", "House"]);
    assert!(s.rows[0].linked);
    assert!(s.act(Action::Move(-1)).is_empty(), "nothing more to write");

    // Outside a note there is nothing to link to.
    s.act(Action::Back);
    s.act(Action::Write);
    assert!(!s.rows[0].linked);
}

#[test]
fn a_new_row_typed_into_and_cleared_goes_when_left_and_leaves_nothing_to_undo() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    s.say("");
    let undo = s.undo.len();

    s.act(Action::Write);
    let id = s.rows[2].id.clone();
    assert_eq!(kept(&edited(&mut s, "a gar")).len(), 1);
    assert!(
        edited(&mut s, " \n").is_empty(),
        "an empty head is not written"
    );
    assert_eq!(s.notes.get(&id).unwrap().text, "a gar");

    let left = s.act(Action::Move(1));
    assert!(
        matches!(&kept(&left)[..], [r] if r.id == id && r.deleted),
        "it was on every device, so it is taken off them: {left:?}"
    );
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"], "no row");
    assert_eq!(
        s.selected().unwrap().head,
        "a garden",
        "the row that was under it"
    );
    assert_eq!(s.status, "");
    assert_eq!(s.undo.len(), undo, "nothing to undo");
    assert_eq!(s.notes.len(), 3);

    // One never typed into writes nothing, and is not left behind by going anywhere else.
    s.act(Action::Select(None));
    s.act(Action::Write);
    assert!(s.act(Action::Move(1)).is_empty());
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
    assert_eq!(s.selected().unwrap().head, "Boiler");
}

#[test]
fn a_chord_on_a_new_row_with_nothing_in_it_does_nothing_and_leaves_the_row() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    s.act(Action::Write);
    let id = s.rows[2].id.clone();
    for action in [
        Action::GoIn,
        Action::Archive,
        Action::Link,
        Action::Copy,
        Action::OpenUrl,
    ] {
        let name = format!("{action:?}");
        assert!(s.act(action).is_empty(), "{name} asked nothing of the face");
        assert_eq!(s.cursor, Some(2), "{name} left the cursor where it was");
        assert_eq!(s.rows[2].id, id, "{name} left the row");
        assert_eq!(s.field.as_ref().map(|f| f.id.as_str()), Some(id.as_str()));
        assert!(path(&s).is_empty(), "{name} went nowhere");
    }
    assert_eq!(s.notes.len(), 3, "nothing was written");
}

#[test]
fn a_note_edited_in_place_is_written_at_each_change_and_done_writes_out_its_date_words() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler service", &[], 1_000);
    s.act(Action::Move(1));
    assert_eq!(field(&s), Some(("Boiler service".into(), false, false)));
    for text in [
        "Boiler service b",
        "Boiler service booked",
        "Boiler service booked /fri",
    ] {
        assert_eq!(kept(&edited(&mut s, text)).len(), 1, "a version a change");
    }
    assert_eq!(
        s.notes.get("boiler").unwrap().text,
        "Boiler service booked /fri",
        "as it was typed"
    );

    let done = s.act(Action::Close);
    let [held] = kept(&done)[..] else {
        panic!("{done:?}");
    };
    assert_eq!(held.text, "Boiler service booked 2026-10-16");
    assert_eq!(held.updated, Some(NOON));
    assert_eq!(s.status, "saved");
    assert_eq!(
        field(&s),
        Some(("Boiler service booked 2026-10-16".into(), false, false)),
        "written, and the cursor stays"
    );
    assert_eq!(
        s.selected().unwrap().head,
        "Boiler service booked 2026-10-16"
    );

    // Leaving writes nothing more and says nothing.
    s.say("");
    assert!(s.act(Action::Move(-1)).is_empty());
    assert_eq!(s.status, "");

    // Undo takes back the visit, all of it.
    s.act(Action::Undo);
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler service");
}

#[test]
fn escape_keeps_what_was_typed_and_undo_puts_it_back() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler service", &[], 1_000);
    s.act(Action::Move(1));
    assert_eq!(kept(&edited(&mut s, "Boiler serviced")).len(), 1);
    let undone = s.act(Action::Undo);
    assert_eq!(
        kept(&undone)[0].text,
        "Boiler service",
        "putting back writes the head as it was"
    );
    assert_eq!(s.status, "put back as it was");
    assert_eq!(field(&s), Some(("Boiler service".into(), false, false)));
    assert!(s.act(Action::Select(None)).is_empty());
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler service");

    s.act(Action::Move(1));
    edited(&mut s, "Boiler serviced");
    let up = s.act(Action::Select(None));
    assert!(kept(&up).is_empty(), "kept as it was typed");
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
}

#[test]
fn moving_off_a_row_keeps_what_was_typed_and_writes_nothing_more() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 2_000);
    put(&mut s, "boiler", "Boiler", &[], 3_000);
    s.act(Action::Move(1));
    assert_eq!(
        kept(&edited(&mut s, "Boiler serviced"))[0].text,
        "Boiler serviced"
    );
    assert!(kept(&s.act(Action::Move(1))).is_empty());
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
    assert_eq!(field(&s), Some(("House".into(), false, false)));
    assert_eq!(s.selected().unwrap().head, "House");

    // The line's action goes to the line.
    edited(&mut s, "House insurance");
    assert!(kept(&s.act(Action::Type("garden".into()))).is_empty());
    assert_eq!(s.notes.get("house").unwrap().text, "House insurance");
    assert_eq!((s.line.as_str(), s.cursor), ("garden", None));
    assert_eq!(field(&s), None);
}

#[test]
fn a_new_note_is_written_as_it_is_typed_and_the_next_is_an_enter_away() {
    let mut s = session();
    for text in ["plant the garlic", "net the brassicas"] {
        s.act(Action::Write);
        assert_eq!(kept(&edited(&mut s, text)).len(), 1);
        assert!(kept(&s.act(Action::Move(-1))).is_empty());
        assert_eq!(
            (s.cursor, s.line.as_str()),
            (None, ""),
            "on the line, for the next"
        );
        assert!(heads(&s).contains(&text));
    }
    assert_eq!(s.notes.len(), 2);
}

#[test]
fn a_burst_of_keystrokes_on_one_head_in_one_second_keeps_its_stamp_within_that_many_milliseconds_of_the_clock()
 {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 3_000);
    select(&mut s, "Boiler");
    // A key at a time with the clock stopped: each is a version, stamped a millisecond past
    // the last, where a second a key put the head as many seconds ahead.
    let head = "Boiler: service due, book it before win";
    let mut stamps = Vec::new();
    for end in "Boiler".len()..=head.len() {
        for version in kept(&edited(&mut s, &head[..end])) {
            stamps.extend(version.stamp);
        }
    }
    assert_eq!(stamps.len(), head.len() - "Boiler".len());
    assert!(stamps.windows(2).all(|two| two[0] < two[1]));
    let ahead = stamps.last().unwrap().ms - NOON.ms;
    assert!((0..=stamps.len() as i64).contains(&ahead), "{ahead} ms");
}

#[test]
fn a_note_emptied_in_place_is_deleted_and_undo_brings_it_back() {
    let mut s = session();
    put(&mut s, "house", "House\nthe deeds", &[], 2_000);
    put(&mut s, "boiler", "Boiler", &[], 3_000);
    select(&mut s, "House");
    edited(&mut s, "");
    let done = s.act(Action::Close);
    assert!(matches!(&kept(&done)[..], [r] if r.deleted && r.text == "House\nthe deeds"));
    assert_eq!(s.status, "deleted · undo brings it back");
    assert_eq!(heads(&s), ["Boiler"]);

    s.act(Action::Undo);
    assert_eq!(heads(&s), ["Boiler", "House"]);

    // Deleting from the emptied row deletes it once, and nothing next to it goes with it.
    select(&mut s, "House");
    edited(&mut s, "");
    let deleted = s.act(Action::Delete);
    assert_eq!(kept(&deleted).len(), 1);
    assert_eq!(heads(&s), ["Boiler"]);
}

#[test]
fn a_note_gone_elsewhere_keeps_its_head_under_the_cursor_and_leaves_without_a_word() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 1_000);
    put(&mut s, "boiler", "Boiler", &[], 2_000);
    assert_eq!(heads(&s), ["Boiler", "House"]);
    s.act(Action::Move(2));
    assert_eq!(s.selected().unwrap().head, "House");

    // Boiler is deleted elsewhere while the cursor walks the list, which holds still.
    let mut gone = s.notes.get("boiler").unwrap().clone();
    gone.deleted = true;
    gone.stamp = Some(NOON.plus_secs(120));
    gone.device = "other".into();
    assert!(s.absorb([gone]));
    assert_eq!(heads(&s), ["Boiler", "House"], "the list holds still");

    // Landing on the row that is left shows its head, not nothing; leaving it again, with
    // nothing typed, says nothing, since nothing was lost.
    s.say("");
    s.act(Action::Move(-1));
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
    assert!(s.act(Action::Move(-1)).is_empty());
    assert_eq!(s.status, "");
    assert_eq!((s.cursor, heads(&s)), (None, vec!["House"]));
}

#[test]
fn undo_and_redo_on_an_empty_new_note_do_nothing_and_undo_on_one_typed_into_empties_it() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 1_000);
    s.say("");
    let undo = s.undo.len();
    s.act(Action::Write);
    assert!(s.act(Action::Undo).is_empty());
    assert!(s.act(Action::Redo).is_empty());
    assert_eq!(
        field(&s),
        Some((String::new(), true, false)),
        "still a new note"
    );
    assert_eq!(heads(&s), ["", "House"]);
    assert_eq!(s.status, "");

    // One typed into is written, and putting it back takes the version off again and
    // leaves the row empty to write in.
    edited(&mut s, "a garden");
    assert_eq!(s.notes.len(), 2);
    let undone = s.act(Action::Undo);
    assert!(matches!(&kept(&undone)[..], [r] if r.deleted));
    assert_eq!(field(&s), Some((String::new(), true, false)));
    assert_eq!(heads(&s), ["", "House"]);
    assert_eq!(s.view().field.unwrap().updated, None);
    assert_eq!(s.notes.len(), 1);
    assert_eq!(s.undo.len(), undo);
}

#[test]
fn what_arrives_while_the_cursor_is_on_the_list_is_listed_when_it_comes_back_to_the_line() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    s.act(Action::Move(1));
    assert!(s.absorb([theirs("house", "House", 60)]));
    assert_eq!(s.status, "1 changed elsewhere");
    assert_eq!(heads(&s), ["Boiler"], "the list waits");
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
    assert!(s.act(Action::Select(None)).is_empty());
    assert_eq!(heads(&s), ["House", "Boiler"], "and comes with the line");

    select(&mut s, "Boiler");
    assert!(s.absorb([theirs("garden", "a garden", 120)]));
    assert_eq!(kept(&edited(&mut s, "Boiler serviced")).len(), 1);
    assert_eq!(
        heads(&s),
        ["House", "Boiler serviced"],
        "the list still waits"
    );
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["a garden", "House", "Boiler serviced"]);
}

#[test]
fn the_editor_writes_a_draft() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    go_into(&mut s, "Boiler");
    s.act(Action::Write);
    let id = s.rows[0].id.clone();
    assert_eq!(
        s.act(Action::Editor),
        [Effect::Editor {
            id: id.clone(),
            text: String::new()
        }]
    );
    assert!(field(&s).is_some(), "the draft waits for the editor");
    let saved = s.act(Action::Save {
        id: id.clone(),
        text: "bleed the radiators\n".into(),
    });
    let [new] = kept(&saved)[..] else {
        panic!("{saved:?}");
    };
    assert_eq!(
        (new.id.as_str(), new.text.as_str()),
        (id.as_str(), "bleed the radiators")
    );
    assert_eq!(new.links, ["boiler"]);
    assert_eq!(s.status, "created");
    assert_eq!(
        field(&s),
        Some(("bleed the radiators".into(), false, false))
    );
    assert_eq!(s.cursor, Some(0));

    // An editor that leaves nothing leaves no note, and the cursor goes back to the row the
    // new note was opened under.
    s.act(Action::Write);
    let id = s.rows[1].id.clone();
    s.act(Action::Editor);
    assert!(
        s.act(Action::Save {
            id,
            text: "\n".into()
        })
        .is_empty()
    );
    assert_eq!(heads(&s), ["bleed the radiators"]);
    assert_eq!(s.cursor, Some(0));
    assert_eq!(
        field(&s),
        Some(("bleed the radiators".into(), false, false))
    );

    // From the line, the cursor goes back to the line.
    s.act(Action::Select(None));
    s.act(Action::Write);
    let id = s.rows[0].id.clone();
    s.act(Action::Editor);
    s.act(Action::Save {
        id,
        text: String::new(),
    });
    assert_eq!((s.cursor, field(&s)), (None, None));
}

#[test]
fn an_empty_file_back_from_the_editor_deletes_the_note() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
    s.act(Action::Move(1));
    s.act(Action::Editor);
    let saved = s.act(Action::Save {
        id: "boiler".into(),
        text: " \n\n".into(),
    });
    assert!(matches!(&kept(&saved)[..], [r] if r.deleted));
    assert_eq!(s.status, "deleted · undo brings it back");
    assert!(heads(&s).is_empty());
    s.act(Action::Undo);
    assert_eq!(heads(&s), ["Boiler"]);
}

#[test]
fn the_head_emptied_under_a_body_gives_the_bodys_first_line_as_the_head() {
    let mut s = session();
    put(
        &mut s,
        "boiler",
        "Boiler\nthe combi under the stairs\nserviced in spring",
        &[],
        1_000,
    );
    s.act(Action::Move(1));
    edited(
        &mut s,
        "\n  \nthe combi under the stairs\nserviced in spring\n",
    );
    let done = s.act(Action::Close);
    assert_eq!(
        kept(&done)[0].text,
        "the combi under the stairs\nserviced in spring"
    );
    assert_eq!(heads(&s), ["the combi under the stairs"]);
    assert!(s.rows[0].has_body);
}

#[test]
fn typing_into_a_head_writes_each_change_and_leaving_writes_nothing_more() {
    let mut s = session();
    three(&mut s);
    let undo = s.undo.len();
    assert!(s.act(Action::Move(1)).is_empty());
    assert_eq!(
        field(&s),
        Some(("Boiler".into(), false, false)),
        "nothing pressed to begin"
    );
    assert!(!s.unwritten());
    for text in ["Boiler s", "Boiler serviced"] {
        let typed = edited(&mut s, text);
        let [held] = kept(&typed)[..] else {
            panic!("{typed:?}");
        };
        assert_eq!(held.text, text);
        assert_eq!(s.notes.get("boiler").unwrap().text, text);
        assert_eq!(s.selected().unwrap().head, text, "the row is put right");
        assert!(!s.unwritten());
    }
    assert!(kept(&s.act(Action::Move(1))).is_empty());
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
    assert!(!s.unwritten());
    assert_eq!(s.undo.len(), undo + 1, "one entry for the visit");
}

#[test]
fn down_through_a_list_of_unchanged_heads_writes_nothing() {
    let mut s = session();
    three(&mut s);
    let undo = s.undo.len();
    for by in [1, 1, 1, 1, -1, -1, -1, -1] {
        assert!(s.act(Action::Move(by)).is_empty());
    }
    assert_eq!(s.cursor, None);
    assert_eq!(s.undo.len(), undo);
}

#[test]
fn a_head_written_mid_list_stays_where_it_is_and_down_lands_on_the_row_below_it() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    assert_eq!(kept(&edited(&mut s, "House insurance")).len(), 1);
    assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
    s.act(Action::Move(1));
    assert_eq!(s.selected().unwrap().head, "a garden");
    assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
}

#[test]
fn an_old_note_edited_stays_where_it_is_kept_when_the_cursor_comes_back_to_the_line() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "a garden");
    let place = s.notes.get("garden").map(|r| (r.after.clone(), r.placed));
    let typed = edited(&mut s, "a garden, dug over");
    let [edit] = kept(&typed)[..] else {
        panic!("{typed:?}");
    };
    assert_eq!(
        Some((edit.after.clone(), edit.placed)),
        place,
        "an edit is not a move"
    );
    s.act(Action::Move(-1));
    assert_eq!(heads(&s), ["Boiler", "House", "a garden, dug over"]);
    s.act(Action::Move(-1));
    s.act(Action::Move(-1));
    assert_eq!(s.cursor, None);
    assert_eq!(heads(&s), ["Boiler", "House", "a garden, dug over"]);
}

#[test]
fn undo_on_a_changed_head_puts_it_back_and_on_an_unchanged_one_takes_back_the_last_version() {
    let mut s = session();
    three(&mut s);
    s.act(Action::Move(1));
    edited(&mut s, "Boiler serviced");
    let put_back = s.act(Action::Undo);
    assert_eq!(kept(&put_back)[0].text, "Boiler");
    assert_eq!(s.status, "put back as it was");
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));

    edited(&mut s, "Boiler serviced");
    s.act(Action::Close);
    let undone = s.act(Action::Undo);
    assert_eq!(kept(&undone)[0].text, "Boiler");
    assert_eq!(s.status, "undid: Boiler serviced");
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
}

#[test]
fn a_chord_on_a_head_typed_into_acts_on_the_note_as_it_reads() {
    let mut s = session();
    three(&mut s);
    let undo = s.undo.len();
    s.act(Action::Move(1));
    assert_eq!(kept(&edited(&mut s, "Boiler serviced")).len(), 1);
    let archived = s.act(Action::Archive);
    let [archive] = kept(&archived)[..] else {
        panic!("{archived:?}");
    };
    assert_eq!(
        (archive.text.as_str(), archive.archived),
        ("Boiler serviced", true)
    );
    assert_eq!(s.undo.len(), undo + 2);
    assert_eq!(heads(&s), ["House", "a garden"]);
}

#[test]
fn the_body_opens_on_the_row_and_closes_onto_its_head() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
    s.act(Action::Move(1));
    assert!(s.act(Action::Open).is_empty());
    assert_eq!(field(&s), Some(("Boiler\nthe combi".into(), false, true)));
    assert!(edited(&mut s, "Boiler\nthe combi under the stairs").is_empty());
    let done = s.act(Action::Close);
    assert_eq!(kept(&done).len(), 1);
    assert_eq!(
        field(&s),
        Some(("Boiler\nthe combi under the stairs".into(), false, false))
    );
    assert_eq!(s.cursor, Some(0), "the cursor stays on the row");
    assert!(s.act(Action::Close).is_empty(), "written once");
}

#[test]
fn the_editor_is_handed_the_head_as_it_now_reads_and_what_it_leaves_is_one_version() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
    s.act(Action::Move(1));
    let undo = s.undo.len();
    edited(&mut s, "Boiler serviced\nthe combi");
    assert_eq!(
        s.act(Action::Editor),
        [Effect::Editor {
            id: "boiler".into(),
            text: "Boiler serviced\nthe combi".into()
        }]
    );
    let saved = s.act(Action::Save {
        id: "boiler".into(),
        text: "Boiler serviced\nthe combi\nin spring\n".into(),
    });
    let [held] = kept(&saved)[..] else {
        panic!("{saved:?}");
    };
    assert_eq!(held.text, "Boiler serviced\nthe combi\nin spring");
    assert_eq!(s.undo.len(), undo + 1);
    assert_eq!(s.status, "saved");
    assert!(!s.unwritten());
}

#[test]
fn deleting_a_new_note_with_text_deletes_it_and_an_empty_one_goes_with_nothing_written() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 2_000);
    s.act(Action::Select(None));
    s.act(Action::Write);
    edited(&mut s, "a garden");
    let deleted = s.act(Action::Delete);
    assert!(matches!(&kept(&deleted)[..], [r] if r.deleted && r.text == "a garden"));
    assert_eq!(heads(&s), ["House"]);
    assert_eq!(s.status, "deleted · undo brings it back");
    s.act(Action::Undo);
    assert_eq!(heads(&s), ["a garden", "House"], "a note like any other");

    s.act(Action::Select(None));
    s.say("");
    let undo = s.undo.len();
    s.act(Action::Write);
    assert!(s.act(Action::Delete).is_empty());
    assert_eq!(heads(&s), ["a garden", "House"]);
    assert_eq!((s.cursor, field(&s)), (None, None));
    assert_eq!(s.notes.len(), 2);
    assert_eq!(s.undo.len(), undo);
    assert_eq!(s.status, "");
}

#[test]
fn going_into_a_new_note_with_text_goes_into_the_note_it_is() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 2_000);
    s.act(Action::Select(None));
    s.act(Action::Write);
    assert_eq!(kept(&edited(&mut s, "a garden")).len(), 1);
    assert!(kept(&s.act(Action::GoIn)).is_empty(), "written already");
    assert_eq!(path(&s), ["a garden"]);

    // One with no text has nowhere to go in to, and the row stays to be written in.
    s.act(Action::Back);
    s.act(Action::Write);
    assert!(s.act(Action::GoIn).is_empty());
    assert!(path(&s).is_empty());
    assert_eq!(s.cursor, Some(0));
}

#[test]
fn a_version_of_the_note_under_the_cursor_shows_while_its_head_is_unchanged_and_waits_while_it_is_not()
 {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    s.act(Action::Move(1));
    assert!(s.absorb([theirs("boiler", "Boiler\nbled the radiators", 60)]));
    assert_eq!(
        field(&s),
        Some(("Boiler\nbled the radiators".into(), false, false))
    );

    edited(&mut s, "Boiler serviced");
    assert!(s.absorb([theirs("boiler", "Boiler\nbled them twice", 120)]));
    assert_eq!(field(&s), Some(("Boiler serviced".into(), false, false)));
    let up = s.act(Action::Select(None));
    assert_eq!(kept(&up)[0].text, "Boiler serviced", "written past theirs");
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler serviced");
}

#[test]
fn enter_on_a_row_opens_a_new_note_under_it() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "swings", "kettlebell swings", &["boiler"], 2_000);
    put(&mut s, "garden", "a garden", &["boiler"], 3_000);
    put(&mut s, "house", "House", &["boiler"], 4_000);
    go_into(&mut s, "Boiler");
    assert_eq!(heads(&s), ["House", "a garden", "kettlebell swings"]);
    type_in(&mut s, "");
    select(&mut s, "a garden");
    s.say("");

    assert!(s.act(Action::Write).is_empty(), "nothing is written yet");
    assert_eq!(heads(&s), ["House", "a garden", "", "kettlebell swings"]);
    assert_eq!(s.cursor, Some(2));
    assert_eq!(field(&s), Some((String::new(), true, false)));
    assert!(s.rows[2].linked, "it will be linked to where you are");

    let typed = edited(&mut s, "bleed the radiators");
    let [new] = kept(&typed)[..] else {
        panic!("{typed:?}");
    };
    assert_eq!(new.text, "bleed the radiators");
    assert_eq!(new.links, ["boiler"]);
    assert_eq!(new.created, Some(NOON), "when Enter was pressed");
    assert_eq!(s.status, "");
    assert!(s.act(Action::Select(None)).is_empty());
    assert_eq!(
        (s.cursor, s.line.as_str()),
        (None, ""),
        "the line is untouched"
    );
}

#[test]
fn enter_on_a_row_typed_into_writes_nothing_more_and_on_one_emptied_deletes_it() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    assert_eq!(kept(&edited(&mut s, "House insurance")).len(), 1);
    assert!(s.act(Action::Write).is_empty());
    assert_eq!(heads(&s), ["Boiler", "House insurance", "", "a garden"]);
    assert_eq!(s.cursor, Some(2));
    assert_eq!(field(&s), Some((String::new(), true, false)));

    // A head emptied deletes its note, and the new note opens where its row was.
    select(&mut s, "Boiler");
    assert!(edited(&mut s, "").is_empty());
    let entered = s.act(Action::Write);
    assert!(matches!(&kept(&entered)[..], [r] if r.deleted));
    assert_eq!(s.cursor, Some(0));
    assert_eq!(field(&s), Some((String::new(), true, false)));
}

#[test]
fn enter_on_a_new_note_with_text_opens_another_under_it() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 1_000);
    s.act(Action::Write);
    assert_eq!(kept(&edited(&mut s, "plant the garlic")).len(), 1);
    assert!(s.act(Action::Write).is_empty());
    assert_eq!(heads(&s), ["plant the garlic", "", "House"]);
    assert_eq!(s.cursor, Some(1));
    assert_eq!(kept(&edited(&mut s, "net the brassicas")).len(), 1);
    assert!(s.act(Action::Select(None)).is_empty());
    assert_eq!(s.notes.len(), 3);
    assert_eq!(s.cursor, None);
}

#[test]
fn enter_on_an_empty_new_note_does_nothing() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    s.act(Action::Write);
    assert!(s.act(Action::Write).is_empty());
    assert_eq!(heads(&s), ["Boiler", "House", "", "a garden"]);
    assert_eq!(s.cursor, Some(2));
    assert_eq!(field(&s), Some((String::new(), true, false)));
}

#[test]
fn an_empty_new_note_left_by_an_arrow_goes_and_the_arrow_lands_past_it() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    edited(&mut s, "House insurance");
    s.act(Action::Write);
    assert!(s.act(Action::Move(-1)).is_empty());
    assert_eq!(s.selected().unwrap().head, "House insurance");
    assert_eq!(
        heads(&s),
        ["Boiler", "House insurance", "a garden"],
        "the list holds still"
    );

    s.act(Action::Write);
    assert!(s.act(Action::Move(1)).is_empty());
    assert_eq!(
        s.selected().unwrap().head,
        "a garden",
        "the row that was under it"
    );
    assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);

    // Under the last row, down has nowhere further to go than the last row.
    s.act(Action::Write);
    s.act(Action::Move(1));
    assert_eq!(s.selected().unwrap().head, "a garden");

    // At the top, up is the line, and there the list is put in order, where an edit has
    // not moved anything.
    s.act(Action::Select(None));
    s.act(Action::Write);
    assert_eq!(s.cursor, Some(0));
    s.act(Action::Move(-1));
    assert_eq!((s.cursor, field(&s)), (None, None));
    assert_eq!(heads(&s), ["Boiler", "House insurance", "a garden"]);
}

#[test]
fn down_off_an_empty_new_note_at_the_top_lands_on_the_first_row_even_when_the_list_is_stale() {
    let mut s = session();
    three(&mut s);
    s.act(Action::Write);
    assert_eq!(s.cursor, Some(0));
    // A version arriving while the cursor is on the list waits for the line.
    assert!(s.absorb([Record {
        created: Some(NOON),
        updated: Some(NOON),
        stamp: Some(NOON),
        device: "other".into(),
        ..Record::new("theirs", "written elsewhere")
    }]));
    s.act(Action::Move(1));
    assert_eq!(
        s.selected().unwrap().head,
        "Boiler",
        "the row that was under it"
    );
    assert_eq!(
        heads(&s),
        ["Boiler", "House", "a garden"],
        "and the list holds still"
    );
    s.act(Action::Select(None));
    assert!(
        heads(&s).contains(&"written elsewhere"),
        "put in order once the cursor is on the line"
    );
}

#[test]
fn backspace_on_an_empty_new_row_takes_it_away_and_lands_on_the_row_above() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 1_000);
    put(&mut s, "boiler", "Boiler", &[], 2_000);
    s.act(Action::Select(Some(0)));
    let undo = s.undo.len();
    s.act(Action::Write);
    assert_eq!(s.cursor, Some(1));
    assert!(s.act(Action::Erase).is_empty(), "nothing is written");
    assert_eq!(s.cursor, Some(0));
    assert_eq!(heads(&s), ["Boiler", "House"]);
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
    assert_eq!(s.notes.len(), 2);
    assert_eq!(s.undo.len(), undo);
}

#[test]
fn backspace_on_an_empty_new_row_at_the_top_lands_on_the_line() {
    let mut s = session();
    three(&mut s);
    s.act(Action::Write);
    assert_eq!(s.cursor, Some(0));
    assert!(s.act(Action::Erase).is_empty());
    assert_eq!((s.cursor, field(&s)), (None, None));
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
}

#[test]
fn backspace_on_a_head_emptied_deletes_the_note_and_steps_up() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 1_000);
    put(&mut s, "boiler", "Boiler", &[], 2_000);
    select(&mut s, "House");
    edited(&mut s, "");
    let erased = s.act(Action::Erase);
    assert!(matches!(&kept(&erased)[..], [r] if r.id == "house" && r.deleted));
    assert_eq!(s.status, "deleted · undo brings it back");
    assert_eq!(s.cursor, Some(0));
    assert_eq!(heads(&s), ["Boiler"]);
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));

    s.act(Action::Undo);
    s.act(Action::Select(None));
    assert_eq!(heads(&s), ["Boiler", "House"], "undo brings it back");
}

#[test]
fn backspace_on_an_empty_head_over_a_body_leaves_the_note() {
    let mut s = session();
    put(&mut s, "house", "House\nthe plan", &[], 1_000);
    select(&mut s, "House");
    edited(&mut s, "\nthe plan");
    assert!(s.act(Action::Erase).is_empty());
    assert_eq!(s.notes.get("house").unwrap().text, "House\nthe plan");
    assert_eq!(s.cursor, Some(0));
    assert_eq!(s.status, "it has a body, so it stays");
}

#[test]
fn backspace_with_the_body_open_is_not_the_engines() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    s.act(Action::Open);
    edited(&mut s, "");
    assert!(s.act(Action::Erase).is_empty());
    assert_eq!(field(&s), Some((String::new(), false, true)));
    assert_eq!(s.cursor, Some(1));
    assert!(!s.notes.get("house").unwrap().deleted);
}

#[test]
fn backspace_at_the_start_of_a_head_with_text_leaves_it() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    assert!(s.act(Action::Erase).is_empty());
    assert_eq!(s.cursor, Some(1));
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
}

#[test]
fn backspace_on_the_line_does_nothing() {
    let mut s = session();
    three(&mut s);
    assert!(s.act(Action::Erase).is_empty());
    assert_eq!((s.cursor, field(&s)), (None, None));
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
    assert_eq!(s.notes.len(), 3);
}

#[test]
fn backspace_on_an_empty_new_row_at_the_top_puts_a_stale_list_in_order() {
    let mut s = session();
    three(&mut s);
    s.act(Action::Write);
    // A version arriving while the cursor is on the list waits for the line.
    assert!(s.absorb([theirs("theirs", "written elsewhere", 0)]));
    assert!(!heads(&s).contains(&"written elsewhere"));
    s.act(Action::Erase);
    assert_eq!(s.cursor, None);
    assert!(
        heads(&s).contains(&"written elsewhere"),
        "put in order once the cursor is on the line"
    );
}

#[test]
fn discarding_a_new_note_cleared_under_a_row_lands_on_the_row_above_and_leaves_nothing_to_undo() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    let undo = s.undo.len();
    s.act(Action::Write);
    edited(&mut s, "a garden shed");
    edited(&mut s, "");
    let discarded = s.act(Action::Delete);
    assert!(matches!(&kept(&discarded)[..], [r] if r.deleted));
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
    assert_eq!(s.selected().unwrap().head, "House");
    assert_eq!(field(&s), Some(("House".into(), false, false)));
    assert_eq!(s.undo.len(), undo);

    // Leaving one empty for the line goes to the line.
    s.act(Action::Write);
    s.act(Action::Select(None));
    assert_eq!(
        (s.cursor, heads(&s)),
        (None, vec!["Boiler", "House", "a garden"])
    );
}

#[test]
fn a_row_offers_a_new_note_first_and_an_empty_new_note_does_not() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    assert_eq!(s.view().hints[0].action, Action::Write);
    assert_eq!(labels(&s)[0], "new note");
    s.act(Action::Write);
    assert_eq!(labels(&s), ["body", "editor", "up"]);
    edited(&mut s, "a garden shed");
    assert_eq!(labels(&s)[0], "new note");
}

#[test]
fn typing_into_a_new_row_writes_the_note_at_its_first_letter_and_each_letter_after() {
    let mut s = session();
    put(&mut s, "house", "House", &[], 1_000);
    s.act(Action::Select(None));
    s.say("");
    let undo = s.undo.len();
    s.act(Action::Write);
    let id = s.rows[0].id.clone();
    assert_eq!(
        s.view().field.unwrap().updated,
        None,
        "no date until it is written"
    );

    let mut versions = Vec::new();
    for text in ["B", "Bo"] {
        versions.extend(edited(&mut s, text));
    }
    let texts: Vec<&str> = kept(&versions).iter().map(|r| r.text.as_str()).collect();
    assert_eq!(texts, ["B", "Bo"], "a version a letter");
    let held = s.notes.get(&id).unwrap();
    assert_eq!(held.text, "Bo");
    assert_eq!(held.created, Some(NOON), "when Enter was pressed");
    assert_eq!(s.rows[0].head, "Bo");
    assert_eq!(
        s.view().field.unwrap().updated,
        Some(NOON),
        "the field has its date"
    );
    assert_eq!(
        s.status, "",
        "the date is the sign; the status says nothing"
    );
    assert_eq!(field(&s), Some(("Bo".into(), false, false)), "a note now");
    assert_eq!(s.undo.len(), undo + 1, "one entry for the visit");

    // Once left, one undo takes the note away entirely.
    assert!(s.act(Action::Select(None)).is_empty());
    s.act(Action::Undo);
    assert!(s.notes.get(&id).is_none());
    assert_eq!(heads(&s), ["House"]);
    assert_eq!(s.undo.len(), undo);
}

#[test]
fn a_head_cleared_to_be_typed_again_is_not_deleted_while_the_cursor_is_on_it() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "Boiler");
    assert!(edited(&mut s, "").is_empty(), "nothing is written");
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler");
    assert_eq!(
        s.selected().unwrap().head,
        "Boiler",
        "the row reads as it did"
    );
    assert!(s.unwritten(), "leaving now would delete it");
    let typed = edited(&mut s, "House");
    assert!(matches!(&kept(&typed)[..], [r] if r.id == "boiler" && r.text == "House"));
    assert!(!s.unwritten());
}

#[test]
fn a_date_word_is_kept_as_typed_and_written_out_when_the_cursor_leaves() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    let typed = edited(&mut s, "House viewing /tom");
    assert_eq!(
        kept(&typed)[0].text,
        "House viewing /tom",
        "not made a date mid-word"
    );
    assert!(s.unwritten());
    let left = s.act(Action::Move(1));
    assert_eq!(kept(&left)[0].text, "House viewing 2026-10-14");
    assert_eq!(
        s.notes.get("house").unwrap().text,
        "House viewing 2026-10-14"
    );

    // One undo afterwards brings back the head as it was before the visit.
    s.act(Action::Select(None));
    s.act(Action::Undo);
    assert_eq!(s.notes.get("house").unwrap().text, "House");
}

#[test]
fn put_back_writes_the_head_as_it_was_when_the_cursor_landed_and_leaves_nothing_to_undo() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "house", "House", &[], 2_000);
    let undo = s.undo.len();
    select(&mut s, "Boiler");
    edited(&mut s, "Boiler s");
    edited(&mut s, "Boiler serviced");
    let put_back = s.act(Action::Undo);
    let [back] = kept(&put_back)[..] else {
        panic!("{put_back:?}");
    };
    assert_eq!(
        (back.text.as_str(), back.updated),
        ("Boiler", Some(Moment::utc(1_000))),
        "as it was, date and all"
    );
    assert_eq!(s.status, "put back as it was");
    assert_eq!(field(&s), Some(("Boiler".into(), false, false)));
    assert_eq!(heads(&s), ["House", "Boiler"], "the list does not move");
    assert_eq!(s.undo.len(), undo, "the visit left nothing to undo");

    // Undo after leaving takes back what was done before the visit.
    s.act(Action::Select(None));
    s.act(Action::Undo);
    assert!(s.notes.get("house").is_none());
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler");
}

#[test]
fn putting_back_over_a_version_from_elsewhere_is_a_change_that_undo_takes_back() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    select(&mut s, "Boiler");
    edited(&mut s, "Boiler s");
    s.absorb([theirs("boiler", "Boiler serviced in spring", 60)]);
    s.act(Action::Undo);
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler");

    s.act(Action::Select(None));
    s.act(Action::Undo);
    assert_eq!(
        s.notes.get("boiler").unwrap().text,
        "Boiler serviced in spring",
        "what was written elsewhere is not lost to the put back"
    );
}

#[test]
fn a_new_row_cleared_with_its_body_open_is_taken_off_every_device_when_the_body_closes() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    let undo = s.undo.len();
    s.act(Action::Write);
    let id = s.rows[2].id.clone();
    assert_eq!(kept(&edited(&mut s, "a gar")).len(), 1);
    s.act(Action::Open);
    assert!(
        edited(&mut s, "").is_empty(),
        "a body is written when it closes"
    );

    let done = s.act(Action::Close);
    assert!(
        matches!(&kept(&done)[..], [r] if r.id == id && r.deleted),
        "it was on every device, so it is taken off them: {done:?}"
    );
    assert_eq!(
        field(&s),
        Some((String::new(), true, false)),
        "an empty row again"
    );
    let row = s.selected().unwrap();
    assert_eq!(row.head, "");
    assert_eq!(s.view().field.unwrap().updated, None);
    assert_eq!(s.undo.len(), undo, "nothing to undo");

    // A letter typed there writes it again, and leaving it empty writes nothing more.
    assert_eq!(kept(&edited(&mut s, "B")).len(), 1);
    edited(&mut s, "");
    s.act(Action::Close);
    assert!(s.notes.get(&id).is_none());
    assert!(s.act(Action::Move(1)).is_empty());
    assert_eq!(heads(&s), ["Boiler", "House", "a garden"]);
}

#[test]
fn undo_on_a_new_note_after_its_body_closes_takes_the_note_away() {
    let mut s = session();
    s.act(Action::Write);
    let id = s.rows[0].id.clone();
    edited(&mut s, "Boiler");
    s.act(Action::Open);
    edited(&mut s, "Boiler\nthe combi under the stairs");
    s.act(Action::Close);
    assert!(
        !s.hints().iter().any(|h| h.label == "put back"),
        "nothing to put back once the body is written"
    );

    let undone = s.act(Action::Undo);
    assert!(matches!(&kept(&undone)[..], [r] if r.id == id && r.deleted));
    assert!(s.notes.get(&id).is_none());
}

#[test]
fn redo_with_nothing_to_redo_leaves_a_visit_one_entry_to_undo() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    let undo = s.undo.len();
    edited(&mut s, "House plan");
    s.act(Action::Redo);
    assert_eq!(s.status, "nothing to redo");
    assert_eq!(
        field(&s),
        Some(("House plan".into(), false, false)),
        "still on the row"
    );
    edited(&mut s, "House plans");
    s.act(Action::Select(None));
    assert_eq!(s.undo.len(), undo + 1);

    s.act(Action::Undo);
    assert_eq!(s.notes.get("house").unwrap().text, "House");
}

#[test]
fn the_body_is_not_written_as_it_is_typed_but_when_it_closes() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler\nthe combi", &[], 1_000);
    s.act(Action::Move(1));
    s.act(Action::Open);
    for text in ["Boiler\nthe combi u", "Boiler\nthe combi under the stairs"] {
        assert!(edited(&mut s, text).is_empty());
    }
    assert_eq!(s.notes.get("boiler").unwrap().text, "Boiler\nthe combi");
    assert!(s.unwritten());
    let done = s.act(Action::Close);
    assert_eq!(kept(&done)[0].text, "Boiler\nthe combi under the stairs");
    assert_eq!(s.status, "saved");
    assert!(!s.unwritten());
}

#[test]
fn a_keystroke_that_leaves_the_text_as_it_is_held_writes_nothing() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    assert!(edited(&mut s, "House").is_empty());
    assert!(
        edited(&mut s, "House\n").is_empty(),
        "a newline at the end is not a change"
    );
    assert_eq!(kept(&edited(&mut s, "Houses")).len(), 1);
    assert!(edited(&mut s, "Houses").is_empty());
}

#[test]
fn enter_with_text_on_the_line_takes_the_cursor_to_the_note_it_wrote() {
    let mut s = session();
    three(&mut s);
    type_in(&mut s, "a garden shed");
    s.act(Action::Write);
    let id = s
        .notes
        .records()
        .find(|r| r.text == "a garden shed")
        .unwrap()
        .id
        .clone();
    assert_eq!(s.selected().unwrap().id, id);
    assert_eq!(field(&s), Some(("a garden shed".into(), false, false)));
    assert_eq!(s.line, "");
    assert_eq!(s.status, "created");

    // Enter there opens a row under it, so a run of notes starts from the line.
    s.act(Action::Write);
    assert_eq!(s.cursor, Some(1));
    assert_eq!(field(&s), Some((String::new(), true, false)));
}

#[test]
fn enter_with_text_on_the_line_takes_the_cursor_to_the_note_it_wrote_in_a_zoom_too() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "house", "House", &["boiler"], 2_000);
    go_into(&mut s, "Boiler");
    type_in(&mut s, "bleed the radiators");
    s.act(Action::Write);
    let new = s.selected().unwrap();
    assert_eq!(new.head, "bleed the radiators");
    assert!(new.linked);
    assert_eq!(s.notes.get(&new.id).unwrap().links, ["boiler"]);
    assert_eq!(s.line, "");
}

#[test]
fn keep_writes_the_field_as_it_stands_and_stays_where_it_is() {
    let mut s = session();
    three(&mut s);
    put(&mut s, "boiler", "Boiler\nthe combi", &[], 4_000);
    s.act(Action::Select(None));

    // A head's date words are written out, and the cursor stays on it.
    select(&mut s, "House");
    edited(&mut s, "House viewing /tom");
    let now = s.act(Action::Keep);
    assert_eq!(kept(&now)[0].text, "House viewing 2026-10-14");
    assert_eq!(s.selected().unwrap().id, "house");
    assert_eq!(field(&s).unwrap().0, "House viewing 2026-10-14");
    assert!(s.act(Action::Keep).is_empty(), "nothing more to keep");

    // An open body is kept as a version and stays open.
    select(&mut s, "Boiler");
    s.act(Action::Open);
    edited(&mut s, "Boiler\nthe combi under the stairs");
    let now = s.act(Action::Keep);
    assert_eq!(kept(&now)[0].text, "Boiler\nthe combi under the stairs");
    assert_eq!(
        field(&s),
        Some(("Boiler\nthe combi under the stairs".into(), false, true))
    );
    assert!(!s.unwritten());

    // A new note with nothing in it has nothing to keep, and stays a row to write in.
    s.act(Action::Close);
    s.act(Action::Write);
    assert!(s.act(Action::Keep).is_empty());
    assert!(field(&s).unwrap().1);

    // On the line there is nothing to keep.
    s.act(Action::Select(None));
    assert!(s.act(Action::Keep).is_empty());
}

#[test]
fn keep_leaves_a_head_emptied_or_cleared_to_be_typed_again_as_it_is_held() {
    let mut s = session();
    put(&mut s, "oil", "Order oil\nfrom the depot", &[], 1_000);
    put(&mut s, "pump", "Pump", &[], 2_000);
    s.act(Action::Select(None));

    select(&mut s, "Pump");
    edited(&mut s, "");
    assert!(s.act(Action::Keep).is_empty());
    assert_eq!(s.notes.get("pump").unwrap().text, "Pump");
    assert_eq!(s.selected().unwrap().id, "pump");
    assert_eq!(field(&s).unwrap().0, "");

    select(&mut s, "Order oil");
    edited(&mut s, "\nfrom the depot");
    assert!(s.act(Action::Keep).is_empty());
    assert_eq!(
        s.notes.get("oil").unwrap().text,
        "Order oil\nfrom the depot"
    );
    assert_eq!(s.selected().unwrap().id, "oil");
}

#[test]
fn keep_leaves_an_open_body_emptied_as_it_is_held_and_open() {
    let mut s = session();
    put(&mut s, "oil", "Order oil\nfrom the depot", &[], 1_000);
    select(&mut s, "Order oil");
    s.act(Action::Open);
    edited(&mut s, "");
    assert!(s.act(Action::Keep).is_empty());
    assert_eq!(
        s.notes.get("oil").unwrap().text,
        "Order oil\nfrom the depot"
    );
    assert_eq!(field(&s), Some((String::new(), false, true)));
}
