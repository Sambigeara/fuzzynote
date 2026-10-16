//! A row in a place shows its head without the place's labels at its end.

use super::*;

/// What each row shows when the cursor is not on it, by its head.
fn shown<'a>(s: &'a Session, head: &str) -> Option<&'a str> {
    let row = s.rows.iter().find(|r| r.head == head).unwrap();
    row.shown.as_deref()
}

#[test]
fn a_row_in_a_place_hides_the_places_labels_at_the_end_of_its_head() {
    let mut s = session();
    put(&mut s, "oil", "Order oil #house", &[], 1_000);
    put(&mut s, "pump", "Pump #House", &[], 2_000);
    put(
        &mut s,
        "gutter",
        "clear the gutter #house, soon",
        &[],
        3_000,
    );
    put(
        &mut s,
        "plumber",
        "Call the plumber @joe #house",
        &[],
        4_000,
    );
    put(&mut s, "bins", "Bins out 2026-10-12 #house", &[], 5_000);
    put(&mut s, "middle", "the #house plan", &[], 6_000);

    go_to(&mut s, "#house");
    assert_eq!(shown(&s, "Order oil #house"), Some("Order oil"));
    assert_eq!(shown(&s, "Pump #House"), Some("Pump"), "in any case");
    assert_eq!(shown(&s, "clear the gutter #house, soon"), None);
    assert_eq!(shown(&s, "the #house plan"), None, "only at the end");
    // A label that is not the place's stops the taking off.
    assert_eq!(
        shown(&s, "Call the plumber @joe #house"),
        Some("Call the plumber @joe")
    );
    assert_eq!(
        shown(&s, "Bins out 2026-10-12 #house"),
        Some("Bins out 2026-10-12")
    );

    // Labels gone into one after another are taken off in any order, a day as it is written.
    go_to(&mut s, "@joe");
    assert_eq!(
        shown(&s, "Call the plumber @joe #house"),
        Some("Call the plumber")
    );
    s.act(Action::Back);
    clear(&mut s);
    go_to(&mut s, "2026-10-12");
    assert_eq!(shown(&s, "Bins out 2026-10-12 #house"), Some("Bins out"));
}

#[test]
fn the_box_stays_and_a_head_of_only_its_labels_shows_as_it_is() {
    let mut s = session();
    put(&mut s, "oil", "[ ] Order oil #house", &[], 1_000);
    put(&mut s, "boxed", "[ ] #house", &[], 2_000);
    put(&mut s, "bare", "#house", &[], 3_000);
    go_to(&mut s, "#house");
    assert_eq!(shown(&s, "[ ] Order oil #house"), Some("[ ] Order oil"));
    assert_eq!(shown(&s, "[ ] #house"), None);
    assert_eq!(shown(&s, "#house"), None);
}

#[test]
fn a_note_with_a_body_keeps_its_head_whole_in_a_place() {
    let mut s = session();
    put(
        &mut s,
        "oil",
        "Order oil #house\nfrom the depot",
        &[],
        1_000,
    );
    put(&mut s, "boiler", "Boiler\n#house", &[], 2_000);
    go_to(&mut s, "#house");
    assert_eq!(shown(&s, "Order oil #house"), None);
    assert_eq!(shown(&s, "Boiler"), None);
}

#[test]
fn at_the_top_and_with_a_label_only_typed_nothing_is_hidden() {
    let mut s = session();
    put(&mut s, "oil", "Order oil #house", &[], 1_000);
    assert_eq!(shown(&s, "Order oil #house"), None);
    type_in(&mut s, "#house");
    assert_eq!(shown(&s, "Order oil #house"), None);
}

#[test]
fn labels_before_the_note_on_the_path_still_show_and_the_path_reads_as_the_row_did() {
    let mut s = session();
    put(&mut s, "boiler", "# Boiler #house", &[], 1_000);
    put(&mut s, "pump", "Pump #house", &["boiler"], 2_000);
    go_to(&mut s, "#house");
    assert_eq!(shown(&s, "# Boiler #house"), Some("# Boiler"));
    go_into(&mut s, "# Boiler #house");
    // The heading mark goes too, as a rendered heading reads.
    assert_eq!(path(&s), ["#house", "Boiler"]);
    assert_eq!(shown(&s, "Pump #house"), None);

    // Named from the line, a note is named on the path as its row reads in the place too.
    s.act(Action::Back);
    s.act(Action::Back);
    clear(&mut s);
    go_to(&mut s, "#house");
    type_in(&mut s, "pump");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "Pump"]);
}

#[test]
fn the_row_under_the_cursor_shows_its_text_and_hiding_changes_nothing_else() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "oil", "Order oil #house", &["boiler"], 2_000);
    type_in(&mut s, "oil");
    let top = s.rows.iter().find(|r| r.id == "oil").unwrap().clone();
    clear(&mut s);
    go_to(&mut s, "#house");
    type_in(&mut s, "oil");
    let here = s.rows.iter().find(|r| r.id == "oil").unwrap().clone();
    assert_eq!(here.shown.as_deref(), Some("Order oil"));
    assert_eq!(
        (&here.head, &here.links, &here.reasons),
        (&top.head, &top.links, &top.reasons)
    );
    select(&mut s, "Order oil #house");
    assert_eq!(s.view().field.unwrap().text, "Order oil #house");
}

#[test]
fn a_head_is_named_without_the_labels_that_end_it() {
    let mut s = session();
    put(&mut s, "oil", "Order oil #house", &[], 1_000);
    put(&mut s, "other", "Order more oil", &[], 2_000);
    go_to(&mut s, "#house");
    // The row reads `Order oil`, and typing that names it, so going in goes where it reads.
    type_in(&mut s, "order oil");
    assert_eq!(heads(&s)[0], "Order oil #house");
    assert!(s.rows[0].reasons.iter().any(|r| r == "its name"));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "Order oil"]);

    // It is named with its labels too, and anywhere.
    s.act(Action::Back);
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "order oil house");
    assert!(s.view().hints.iter().any(|h| h.action == Action::GoIn));
}

#[test]
fn a_head_typed_in_a_place_and_left_shows_without_the_label() {
    let mut s = session();
    put(&mut s, "oil", "Order oil #house", &[], 1_000);
    go_to(&mut s, "#house");
    s.act(Action::Write);
    edited(&mut s, "Bleed the radiators #house");
    s.act(Action::Move(1));
    let row = s.rows.iter().find(|r| r.head.starts_with("Bleed")).unwrap();
    assert_eq!(row.shown.as_deref(), Some("Bleed the radiators"));
}

#[test]
fn a_head_without_labels_is_named_before_one_that_only_ends_the_same_way() {
    let mut s = session();
    put(&mut s, "plain", "Order oil", &[], 1_000);
    put(&mut s, "garden", "Order oil #garden", &[], 2_000);
    type_in(&mut s, "order oil");
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["Order oil"]);
}

#[test]
fn a_hub_is_named_before_the_dated_notes_that_share_its_name() {
    let mut s = session();
    put(&mut s, "hub", "Standup", &[], 1_000);
    put(&mut s, "d1", "Standup 2026-10-12", &[], 2_000);
    put(&mut s, "d2", "Standup 2026-10-13", &[], 3_000);
    type_in(&mut s, "standup");
    assert_eq!(heads(&s)[0], "Standup");
    let named = s
        .rows
        .iter()
        .filter(|r| r.reasons.iter().any(|r| r == "its name"));
    assert_eq!(named.count(), 1);
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["Standup"]);
}

#[test]
fn in_a_place_a_name_without_labels_goes_into_the_note_whose_row_reads_so() {
    let mut s = session();
    put(&mut s, "house", "Order oil #house", &[], 1_000);
    put(&mut s, "garden", "Order oil #garden", &[], 2_000);
    go_to(&mut s, "#house");
    type_in(&mut s, "order oil");
    assert!(s.view().hints.iter().any(|h| h.action == Action::GoIn));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "Order oil"]);

    // At the top both read so, and neither is gone into.
    s.act(Action::Back);
    s.act(Action::Back);
    clear(&mut s);
    type_in(&mut s, "order oil");
    assert!(!s.view().hints.iter().any(|h| h.action == Action::GoIn));
}

#[test]
fn in_a_place_the_list_says_its_name_only_where_tab_goes() {
    let mut s = session();
    put(&mut s, "garden", "Order oil #garden", &[], 1_000);
    go_to(&mut s, "#house");
    type_in(&mut s, "order oil");
    assert!(
        !s.rows
            .iter()
            .any(|r| r.reasons.iter().any(|r| r == "its name"))
    );
    assert!(!s.view().hints.iter().any(|h| h.action == Action::GoIn));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house"]);

    // Once a note carries the place's labels, it is listed first as named, and gone into.
    put(&mut s, "house", "Order oil #house", &[], 2_000);
    clear(&mut s);
    type_in(&mut s, "order oil");
    assert_eq!(heads(&s)[0], "Order oil #house");
    assert!(named(&s, "Order oil #house"));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#house", "Order oil"]);
}

#[test]
fn a_head_of_only_labels_and_a_heading_mark_shows_as_it_is() {
    let mut s = session();
    put(&mut s, "mark", "# #house", &[], 1_000);
    put(&mut s, "plans", "## Plans #house", &[], 2_000);
    go_to(&mut s, "#house");
    assert_eq!(shown(&s, "# #house"), None);
    assert_eq!(shown(&s, "## Plans #house"), Some("## Plans"));
    go_into(&mut s, "## Plans #house");
    assert_eq!(path(&s), ["#house", "Plans"]);
}

#[test]
fn a_body_of_only_tags_is_no_body_so_the_head_still_hides_the_place() {
    let mut s = session();
    put(&mut s, "oil", "Order oil #house\n#garden", &[], 1_000);
    go_to(&mut s, "#house");
    assert_eq!(shown(&s, "Order oil #house"), Some("Order oil"));
}

/// Whether the row with this head says it is named by the line.
fn named(s: &Session, head: &str) -> bool {
    let row = s.rows.iter().find(|r| r.head == head).unwrap();
    row.reasons.iter().any(|r| r == "its name")
}

#[test]
fn a_whole_head_archived_does_not_keep_the_name_from_a_note_that_can_be_seen() {
    let mut s = session();
    put(&mut s, "plain", "Order oil", &[], 1_000);
    select(&mut s, "Order oil");
    s.act(Action::Archive);
    put(&mut s, "house", "Order oil #house", &[], 2_000);
    s.act(Action::Select(None));
    type_in(&mut s, "order oil");
    // The list and going in agree: the note named is listed first, saying so, and gone into.
    assert_eq!(heads(&s)[0], "Order oil #house");
    assert!(named(&s, "Order oil #house"));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["Order oil #house"]);
}

#[test]
fn a_whole_head_that_does_not_hold_the_line_does_not_keep_the_name_either() {
    let mut s = session();
    put(&mut s, "plain", "Order oil", &[], 1_000);
    put(&mut s, "task", "[ ] Order oil #house", &[], 2_000);
    type_in(&mut s, "[ ] order oil");
    assert_eq!(heads(&s), ["[ ] Order oil #house"]);
    assert!(named(&s, "[ ] Order oil #house"));
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["[ ] Order oil #house"]);
}
