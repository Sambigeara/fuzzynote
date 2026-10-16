//! The line's forms: what holds, a span, labels one after another, and letters, and how each
//! lists.

use super::*;

#[test]
fn a_line_of_nothing_but_what_holds_lists_exactly_that_in_the_spaces_order() {
    let mut s = session();
    house(&mut s);
    let carrying = [
        "fix the roof tiles #house",
        "clear the gutter #house",
        "Boiler",
    ];
    for typed in ["#house", "#ho", "#house a"] {
        clear(&mut s);
        type_in(&mut s, typed);
        assert_eq!(
            heads(&s),
            carrying,
            "{typed}: what carries it in the order kept, and not what says its word"
        );
        assert!(
            s.rows
                .iter()
                .all(|r| r.reasons.is_empty() && r.score.is_none())
        );
    }
    select(&mut s, "clear the gutter #house");
    assert!(labels(&s).contains(&"move up") && labels(&s).contains(&"move down"));

    s.act(Action::Select(None));
    clear(&mut s);
    type_in(&mut s, "#house roof");
    assert_eq!(
        heads(&s),
        ["fix the roof tiles #house"],
        "words rank among them"
    );
    assert!(!s.rows[0].reasons.is_empty());
    clear(&mut s);
    type_in(&mut s, "#house cake");
    assert!(heads(&s).is_empty(), "and find nothing that does not hold");
}

#[test]
fn a_line_of_labels_is_gone_into_one_after_another() {
    let mut s = session();
    house(&mut s);
    put(&mut s, "boots", "dry the boots #house", &[], NOON.secs());
    type_in(&mut s, "#house /today");
    assert!(labels(&s).contains(&"go in"));
    s.act(Action::GoIn);
    assert_eq!(
        path(&s),
        ["#house", "2026-10-13"],
        "one place, as if Tab after each"
    );
    assert_eq!(s.line, "");
    assert_eq!(heads(&s), ["dry the boots #house"]);

    type_in(&mut s, "#house @joe");
    s.act(Action::GoIn);
    assert_eq!(
        path(&s),
        ["#house", "2026-10-13", "@joe"],
        "of several, a label the place has already is passed over"
    );

    // Out a step at a time, each line coming back on the way out of the first place it
    // went into.
    s.act(Action::Back);
    assert_eq!(path(&s), ["#house", "2026-10-13"]);
    assert_eq!(s.line, "#house @joe");
    clear(&mut s);
    s.act(Action::Back);
    assert_eq!((path(&s), s.line.as_str()), (vec!["#house"], ""));
    s.act(Action::Back);
    assert!(path(&s).is_empty());
    assert_eq!(s.line, "#house /today");
}

#[test]
fn a_span_is_said_beside_the_line_and_is_not_a_place() {
    let mut s = session();
    // Monday 16:00 and 20:00, the day before the tests' noon.
    put(
        &mut s,
        "early",
        "bled the radiators",
        &[],
        NOON.secs() - 20 * 3_600,
    );
    put(&mut s, "late", "ordered oil", &[], NOON.secs() - 16 * 3_600);
    type_in(&mut s, "/-mon 18:00..");
    assert_eq!(s.view().dates, ["Mon 2026-10-12 18:00 – now"]);
    assert_eq!(heads(&s), ["ordered oil"], "written since then");
    clear(&mut s);
    type_in(&mut s, "#house /-mon..");
    assert!(!labels(&s).contains(&"go in"), "a span is not a place");
    s.act(Action::GoIn);
    assert!(path(&s).is_empty());
    assert_eq!(s.line, "#house /-mon..");

    // Enter writes the line as it reads, its date words written out.
    clear(&mut s);
    type_in(&mut s, "ask about “the leak” /-mon 18:00..");
    s.act(Action::Write);
    let new = s.selected().unwrap().id.clone();
    assert_eq!(
        s.notes.get(&new).unwrap().text,
        "ask about “the leak” 2026-10-12 18:00.."
    );
}

#[test]
fn inside_a_place_what_the_line_holds_narrows_its_facts_and_its_finds_alike() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "Boiler");
    for (typed, listed, facts) in [
        ("#house", &["Pressure dropped #house"][..], &[true][..]),
        ("/yesterday", &["Service booked"], &[true]),
        ("oil /today", &["Order oil #house"], &[false]),
        ("oil /yesterday", &[], &[]),
    ] {
        clear(&mut s);
        type_in(&mut s, typed);
        assert_eq!(heads(&s), listed, "{typed}");
        assert_eq!(linked(&s), facts, "{typed}");
    }
}

#[test]
fn letters_rank_by_what_they_score_then_the_shorter_head_then_the_spaces_order() {
    let mut s = session();
    put(&mut s, "list", "bs list", &[], 500);
    put(&mut s, "shed", "Boiler shed", &[], 1_000);
    put(&mut s, "sheds", "Boiler sheds", &[], 2_000);
    put(&mut s, "shop", "Bottle shop!", &[], 3_000);
    put(&mut s, "body", "Notes\nboiler shed in the body", &[], 4_000);
    type_in(&mut s, "~bs");
    assert_eq!(
        heads(&s),
        ["bs list", "Boiler shed", "Bottle shop!", "Boiler sheds"]
    );
    assert_eq!(s.rows[0].reasons, ["letters: bs"]);

    clear(&mut s);
    select(&mut s, "Boiler sheds");
    s.act(Action::Shift(-1));
    s.act(Action::Select(None));
    type_in(&mut s, "~bs");
    assert_eq!(
        heads(&s)[2..],
        ["Boiler sheds", "Bottle shop!"],
        "a tie goes by the space's order"
    );
}
