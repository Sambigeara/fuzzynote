//! The path given as steps, and a face put back where it kept them.

use super::*;

fn note(id: &str, search: Option<&str>) -> KeptStep {
    KeptStep::Note {
        note: id.into(),
        search: search.map(str::to_owned),
    }
}

fn label(written: &str, search: Option<&str>) -> KeptStep {
    KeptStep::Label {
        label: written.into(),
        search: search.map(str::to_owned),
    }
}

#[test]
fn the_path_is_given_as_steps_and_restored_from_them() {
    let mut s = session();
    boiler(&mut s);
    go_to(&mut s, "boi");
    go_to(&mut s, "#house");
    let steps = s.steps();
    assert_eq!(
        steps,
        [note("boiler", Some("boi")), label("#house", Some("#house"))]
    );
    assert_eq!(
        serde_json::to_string(&steps).unwrap(),
        r##"[{"note":"boiler","search":"boi"},{"label":"#house","search":"#house"}]"##
    );

    // A session opened afresh, as a page is after a reload, is put back in the same place.
    let mut again = session();
    boiler(&mut again);
    type_in(&mut again, "pump");
    again.act(Action::Restore(steps));
    assert_eq!(path(&again), ["Boiler", "#house"]);
    assert_eq!(again.line, "");
    assert_eq!(again.cursor, None);
    assert_eq!(heads(&again), heads(&s));

    // The searches come back on the way out, as they would have.
    again.act(Action::Back);
    assert_eq!(again.line, "#house");
    clear(&mut again);
    again.act(Action::Back);
    assert_eq!(again.line, "boi");
    assert!(path(&again).is_empty());
}

#[test]
fn a_step_whose_note_has_gone_or_whose_label_is_not_one_is_passed_over_and_none_is_added_twice() {
    let mut s = session();
    boiler(&mut s);
    s.act(Action::Restore(vec![
        note("gone", None),
        label("boiler", None),
        label("#house /today", None),
        label("#house", None),
        label("#House", None),
        note("boiler", None),
        note("boiler", None),
    ]));
    assert_eq!(path(&s), ["#house", "Boiler"]);
    assert_eq!(s.steps(), [label("#house", None), note("boiler", None)]);

    // A day is kept as it is written, and comes back so.
    go_to(&mut s, "/today");
    let steps = s.steps();
    assert_eq!(steps[2], label("2026-10-13", Some("/today")));
    s.act(Action::Restore(steps));
    assert_eq!(path(&s), ["#house", "Boiler", "2026-10-13"]);
}

#[test]
fn restoring_writes_out_the_field_first() {
    let mut s = session();
    boiler(&mut s);
    select(&mut s, "Water the plants");
    edited(&mut s, "Water the plants /tom");
    let effects = s.act(Action::Restore(vec![label("#house", None)]));
    assert_eq!(kept(&effects)[0].text, "Water the plants 2026-10-14");
    assert_eq!(path(&s), ["#house"]);
    assert!(s.view().field.is_none());
}
