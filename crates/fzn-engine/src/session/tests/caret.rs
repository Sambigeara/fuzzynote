//! Where the session puts the caret. It says so whenever it, and not the person's typing,
//! changed what the caret is in, so that two faces do not each work it out.

use super::*;

fn head(at: usize) -> Caret {
    Caret {
        line: None,
        head: Some(at),
    }
}

#[test]
fn landing_on_a_row_puts_the_caret_at_the_end_of_its_head() {
    let mut s = session();
    three(&mut s);
    s.take_caret();
    s.act(Action::Move(1));
    assert_eq!(s.take_caret(), head("Boiler".len()));
    s.act(Action::Move(1));
    assert_eq!(s.take_caret(), head("House".len()));
    // Up off the first row is the line, which nothing changed.
    s.act(Action::Move(-2));
    assert_eq!(s.take_caret(), Caret::default());
}

#[test]
fn typing_never_moves_the_caret() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "Boiler");
    s.take_caret();
    edited(&mut s, "Boiler service");
    assert_eq!(s.take_caret(), Caret::default(), "a head typed into");
    s.act(Action::Select(None));
    type_in(&mut s, "boi");
    assert_eq!(s.take_caret(), Caret::default(), "the line typed into");
    clear(&mut s);
    assert_eq!(s.take_caret(), Caret::default(), "the line cleared");
}

#[test]
fn a_new_row_has_the_caret_where_its_labels_leave_room() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "Boiler");
    s.act(Action::Write);
    assert_eq!(field(&s), Some((String::new(), true, false)));
    assert_eq!(s.take_caret(), head(0));

    // In the open tasks of a tag, after the box and before the tag.
    let mut s = session();
    house(&mut s);
    go_to(&mut s, "[ ] #house");
    s.take_caret();
    s.act(Action::Write);
    assert_eq!(field(&s), Some(("[ ]  #house".into(), true, false)));
    assert_eq!(s.take_caret(), head("[ ] ".len()));

    // Typed into and put back to its labels, it has the caret there again.
    edited(&mut s, "[ ] Order oil #house");
    s.take_caret();
    s.act(Action::Undo);
    assert_eq!(field(&s), Some(("[ ]  #house".into(), true, false)));
    assert_eq!(s.take_caret(), head("[ ] ".len()));
}

#[test]
fn a_head_the_session_changes_has_the_caret_at_its_end() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "Boiler");

    // Its words written out, after what they became.
    edited(&mut s, "Boiler /task");
    s.act(Action::Write);
    assert_eq!(s.take_caret(), head("[ ] Boiler".len()));

    // Put back, after what it was.
    edited(&mut s, "[ ] Boiler service booked");
    s.act(Action::Undo);
    assert_eq!(field(&s).unwrap().0, "[ ] Boiler");
    assert_eq!(s.take_caret(), head("[ ] Boiler".len()));

    // Ticked.
    s.act(Action::ShowArchived);
    s.take_caret();
    s.act(Action::Archive);
    assert_eq!(field(&s).unwrap().0, "[x] Boiler");
    assert_eq!(s.take_caret(), head("[x] Boiler".len()));

    // Copied, which changes nothing, leaves it where the face has it.
    s.act(Action::Copy);
    assert_eq!(s.take_caret(), Caret::default());
}

#[test]
fn a_body_opening_or_closing_puts_the_caret_at_the_end_of_the_head_and_an_open_one_is_the_editors()
{
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    s.take_caret();
    s.act(Action::Open);
    assert_eq!(s.take_caret(), head("House".len()));
    edited(&mut s, "House\nviewing on /fri");
    assert_eq!(s.take_caret(), Caret::default());
    // The date word is written out under the editor's caret, which is the editor's to keep.
    s.act(Action::Keep);
    assert_eq!(field(&s).unwrap().0, "House\nviewing on 2026-10-16");
    assert_eq!(s.take_caret(), Caret::default());
    s.act(Action::Close);
    assert_eq!(s.take_caret(), head("House".len()));
}

#[test]
fn a_line_the_session_changes_has_the_caret_at_its_end() {
    let mut s = session();
    three(&mut s);
    type_in(&mut s, "Order oil");
    s.take_caret();
    s.act(Action::Write);
    assert_eq!(
        s.take_caret(),
        Caret {
            line: Some(0),
            head: Some("Order oil".len()),
        },
        "the line written is cleared, and the note it became landed on"
    );
}

#[test]
fn a_version_from_elsewhere_moves_the_caret_only_when_it_changes_the_head_under_it() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "Boiler");
    s.take_caret();
    s.absorb([theirs("house", "House viewing", 60)]);
    assert_eq!(s.take_caret(), Caret::default(), "another note");
    s.absorb([theirs("boiler", "Boiler serviced", 120)]);
    assert_eq!(field(&s).unwrap().0, "Boiler serviced");
    assert_eq!(s.take_caret(), head("Boiler serviced".len()));

    // A head typed into here keeps what was typed, and so its caret.
    edited(&mut s, "Boiler serviced in spring");
    s.absorb([theirs("boiler", "Boiler serviced twice", 180)]);
    assert_eq!(field(&s).unwrap().0, "Boiler serviced in spring");
    assert_eq!(s.take_caret(), Caret::default());
}

#[test]
fn a_change_under_the_head_leaves_the_caret_where_it_was() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    s.take_caret();
    s.absorb([theirs("house", "House\nviewing on friday", 60)]);
    assert_eq!(field(&s).unwrap().0, "House\nviewing on friday");
    assert_eq!(s.take_caret(), Caret::default(), "a body from elsewhere");

    let text = "House\nviewing on friday, keys from the agent".to_owned();
    s.act(Action::Save {
        id: "house".into(),
        text,
    });
    assert_eq!(s.take_caret(), Caret::default(), "a body from the editor");

    // An editor that changes the head is a head changed under the caret.
    let text = "House viewing\nviewing on friday, keys from the agent".to_owned();
    s.act(Action::Save {
        id: "house".into(),
        text,
    });
    assert_eq!(s.take_caret(), head("House viewing".len()));
}

#[test]
fn a_head_kept_with_its_words_written_out_has_the_caret_at_its_end() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "House");
    edited(&mut s, "House viewing /fri");
    s.act(Action::Keep);
    assert_eq!(field(&s).unwrap().0, "House viewing 2026-10-16");
    assert_eq!(s.take_caret(), head("House viewing 2026-10-16".len()));
}

#[test]
fn backspace_on_an_empty_row_lands_on_the_row_above_with_the_caret_at_its_end() {
    let mut s = session();
    three(&mut s);
    select(&mut s, "Boiler");
    s.act(Action::Write);
    s.take_caret();
    s.act(Action::Erase);
    assert_eq!(s.selected().unwrap().id, "boiler");
    assert_eq!(s.take_caret(), head("Boiler".len()));
}

#[test]
fn going_into_what_the_line_names_clears_it_with_the_caret_at_its_start() {
    let mut s = session();
    three(&mut s);
    type_in(&mut s, "Boiler");
    s.take_caret();
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["Boiler"]);
    assert_eq!(
        s.take_caret(),
        Caret {
            line: Some(0),
            head: None,
        }
    );
}
