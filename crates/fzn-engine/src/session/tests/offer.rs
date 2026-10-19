//! What is offered while a tag or a person is typed, and taking it: on the line and in a head,
//! opened by typing and closed by anything else.

use super::*;

/// Food on three notes, the fox on two, the house and Joe on one each.
fn tagged(s: &mut Session) {
    put(s, "bread", "Bake bread #food", &[], 1_000);
    put(s, "soup", "Leek soup #food", &[], 2_000);
    put(s, "den", "A den under the shed #fox", &[], 3_000);
    put(s, "jam", "Plum jam #food", &[], 4_000);
    put(s, "tracks", "Tracks by the pond #fox", &[], 5_000);
    put(s, "boiler", "Boiler service #house @joe", &[], 6_000);
    s.act(Action::Select(None));
}

/// Types onto the end of the line and says the caret is at its end, as a face does.
fn typed(s: &mut Session, text: &str) {
    type_in(s, text);
    let at = s.line.chars().count();
    s.act(Action::At(at));
}

/// What is offered, as the view gives it: the words, and the one chosen.
fn offered(s: &Session) -> Option<(Vec<String>, usize)> {
    s.view()
        .offer
        .map(|offer| (offer.words.clone(), offer.chosen))
}

fn words(s: &Session) -> Vec<String> {
    offered(s).map(|(words, _)| words).unwrap_or_default()
}

#[test]
fn a_tag_being_typed_is_offered_the_tags_of_the_space_that_it_starts() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#");
    assert_eq!(offered(&s), None, "a mark alone starts nothing yet");
    typed(&mut s, "f");
    assert_eq!(words(&s), ["#food", "#fox"]);
    typed(&mut s, "o");
    assert_eq!(offered(&s), Some((vec!["#food".into(), "#fox".into()], 0)));
    assert_eq!(labels(&s), ["take", "choose", "close"]);
    assert_eq!(s.view().hints.last().unwrap().action, Action::Dismiss);
    typed(&mut s, "x");
    assert_eq!(
        offered(&s),
        None,
        "the word itself alone is nothing to offer"
    );
    clear(&mut s);
    typed(&mut s, "#FO");
    assert_eq!(
        words(&s),
        ["#food", "#fox"],
        "in any case, as the line holds them"
    );
    clear(&mut s);
    typed(&mut s, "#fo ");
    assert_eq!(offered(&s), None, "a space ends the word");
    typed(&mut s, "#ho");
    assert_eq!(words(&s), ["#house"], "a word after another");
}

#[test]
fn a_person_being_typed_is_offered_the_people_and_not_the_tags() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "@j");
    assert_eq!(words(&s), ["@joe"]);
    clear(&mut s);
    typed(&mut s, "#j");
    assert_eq!(offered(&s), None);
    clear(&mut s);
    typed(&mut s, "oil#fo");
    assert_eq!(offered(&s), None, "a mark inside a word is not one");
}

#[test]
fn what_is_offered_is_the_most_carried_first_then_the_newest_then_by_name_and_five_at_most() {
    let mut s = session();
    put(&mut s, "a", "#garden", &[], 1_000);
    put(&mut s, "b", "#garden", &[], 2_000);
    put(&mut s, "c", "#gate", &[], 3_000);
    put(&mut s, "d", "#gas", &[], 4_000);
    put(&mut s, "e", "#gap #gale", &[], 5_000);
    put(&mut s, "f", "#gaze", &[], 900);
    put(&mut s, "g", "#gab", &[], 800);
    s.act(Action::Select(None));
    typed(&mut s, "#ga");
    assert_eq!(words(&s), ["#garden", "#gale", "#gap", "#gas", "#gate"]);
}

#[test]
fn an_archived_note_still_counts_for_what_it_carries() {
    let mut s = session();
    put(&mut s, "a", "Plum jam #food", &[], 1_000);
    put(&mut s, "b", "Fox tracks #fox", &[], 2_000);
    put(&mut s, "c", "Bake bread #food", &[], 3_000);
    select(&mut s, "Bake bread #food");
    s.act(Action::Archive);
    s.act(Action::Select(None));
    typed(&mut s, "#fo");
    assert_eq!(words(&s), ["#food", "#fox"]);
}

#[test]
fn nothing_is_offered_unless_the_caret_is_at_the_end_of_the_word() {
    let mut s = session();
    tagged(&mut s);
    type_in(&mut s, "#fo oil");
    s.act(Action::At(3));
    assert_eq!(words(&s), ["#food", "#fox"], "a word with more after it");
    type_in(&mut s, "x");
    s.act(Action::At(2));
    assert_eq!(offered(&s), None, "inside the word");
}

#[test]
fn tab_takes_the_chosen_word_with_a_space_after_it_and_the_caret_after_the_space() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#fo");
    s.take_caret();
    s.act(Action::Take(None));
    assert_eq!(s.line, "#food ");
    assert_eq!(
        s.take_caret(),
        Caret {
            line: Some(6),
            head: None
        }
    );
    assert_eq!(offered(&s), None);
    assert_eq!(heads(&s).len(), 3, "the line holds what it now reads");
    // So typing it and two Tabs go into it.
    s.act(Action::GoIn);
    assert_eq!(path(&s), ["#food"]);
}

#[test]
fn a_word_taken_in_the_middle_of_the_line_keeps_what_follows_and_the_space_before_it() {
    let mut s = session();
    tagged(&mut s);
    type_in(&mut s, "#fo oil");
    s.act(Action::At(3));
    s.act(Action::Choose(1));
    s.act(Action::Take(None));
    assert_eq!(s.line, "#fox oil");
    assert_eq!(s.take_caret().line, Some(5));
}

#[test]
fn a_word_asked_for_by_its_place_is_taken_whichever_is_chosen() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#fo");
    s.act(Action::Take(Some(1)));
    assert_eq!(s.line, "#fox ");
    typed(&mut s, "#fo");
    s.act(Action::Take(Some(7)));
    assert_eq!(s.line, "#fox #fo", "a place past the end takes nothing");
    assert_eq!(offered(&s), None);
}

#[test]
fn the_arrows_choose_and_stop_at_either_end() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#fo");
    s.act(Action::Choose(-1));
    assert_eq!(offered(&s).unwrap().1, 0);
    s.act(Action::Choose(1));
    s.act(Action::Choose(1));
    assert_eq!(offered(&s).unwrap().1, 1);
    s.act(Action::Take(None));
    assert_eq!(s.line, "#fox ");
}

#[test]
fn typing_more_or_taking_letters_back_offers_again_and_taking_the_mark_closes_it() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#fo");
    s.act(Action::Choose(1));
    typed(&mut s, "o");
    assert_eq!(
        offered(&s),
        Some((vec!["#food".into()], 0)),
        "chosen from the first"
    );
    s.act(Action::Type("#fo".into()));
    s.act(Action::At(3));
    assert_eq!(words(&s), ["#food", "#fox"]);
    s.act(Action::Type("#".into()));
    s.act(Action::At(1));
    assert_eq!(offered(&s), None);
}

#[test]
fn escape_closes_what_is_offered_until_the_word_is_typed_into_again() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#fo");
    let line = s.line.clone();
    s.act(Action::Dismiss);
    assert_eq!(offered(&s), None);
    assert_eq!(s.line, line, "and nothing else");
    s.act(Action::At(3));
    assert_eq!(offered(&s), None, "the caret said again opens nothing");
    typed(&mut s, "o");
    assert_eq!(words(&s), ["#food"]);
}

#[test]
fn moving_the_caret_off_the_end_of_the_word_closes_what_is_offered_and_back_does_not_open_it() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#fo");
    s.act(Action::At(3));
    assert_eq!(
        words(&s),
        ["#food", "#fox"],
        "a caret said where it was keeps it"
    );
    s.act(Action::At(1));
    assert_eq!(offered(&s), None);
    s.act(Action::At(3));
    assert_eq!(offered(&s), None);
}

#[test]
fn anything_else_asked_for_closes_what_is_offered() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "#fo");
    s.act(Action::Move(1));
    assert_eq!(offered(&s), None, "the cursor off the line");
    s.act(Action::Select(None));
    s.act(Action::Type("#fo".into()));
    assert_eq!(offered(&s), None, "typed, until the caret is said");
    s.act(Action::At(3));
    s.act(Action::ShowArchived);
    assert_eq!(offered(&s), None);
}

#[test]
fn a_head_being_written_is_offered_the_tags_its_own_word_not_counted() {
    let mut s = session();
    tagged(&mut s);
    select(&mut s, "Tracks by the pond #fox");
    edited(&mut s, "Tracks by the pond #fo");
    s.act(Action::At("Tracks by the pond #fo".chars().count()));
    assert_eq!(
        words(&s),
        ["#food", "#fox"],
        "the fox on one note now, not two, and #fo not at all"
    );
    s.take_caret();
    let effects = s.act(Action::Take(None));
    assert_eq!(field(&s).unwrap().0, "Tracks by the pond #food ");
    assert_eq!(s.take_caret().head, Some("Tracks by the pond #food ".len()));
    assert_eq!(kept(&effects)[0].text, "Tracks by the pond #food ");
    assert!(
        labels(&s).contains(&"put back"),
        "it is one more change to the visit"
    );
}

#[test]
fn a_word_taken_in_a_head_keeps_the_body_under_it() {
    let mut s = session();
    tagged(&mut s);
    put(&mut s, "walk", "Pond walk\nmuddy by the gate", &[], 7_000);
    select(&mut s, "Pond walk");
    edited(&mut s, "Pond walk #fo\nmuddy by the gate");
    s.act(Action::At("Pond walk #fo".len()));
    s.act(Action::Take(None));
    assert_eq!(field(&s).unwrap().0, "Pond walk #food \nmuddy by the gate");
}

#[test]
fn an_open_body_is_offered_nothing() {
    let mut s = session();
    tagged(&mut s);
    put(&mut s, "walk", "Pond walk", &[], 7_000);
    select(&mut s, "Pond walk");
    s.act(Action::Open);
    edited(&mut s, "Pond walk #fo");
    s.act(Action::At("Pond walk #fo".len()));
    assert_eq!(offered(&s), None);
}

#[test]
fn a_change_from_elsewhere_to_the_head_closes_what_is_offered_and_any_other_does_not() {
    let mut s = session();
    tagged(&mut s);
    put(&mut s, "walk", "Pond walk #fo", &[], 7_000);
    select(&mut s, "Pond walk #fo");
    // Typed into and back as it was, the head is as it landed, so a version from elsewhere
    // shows in it.
    edited(&mut s, "Pond walk #f");
    edited(&mut s, "Pond walk #fo");
    s.act(Action::At("Pond walk #fo".len()));
    assert_eq!(words(&s), ["#food", "#fox"]);
    s.absorb([theirs("jam", "Plum jam #food, set", 60)]);
    assert_eq!(words(&s), ["#food", "#fox"], "another note changed");
    s.absorb([theirs("walk", "Pond walk by the shed", 120)]);
    assert_eq!(field(&s).unwrap().0, "Pond walk by the shed");
    assert_eq!(offered(&s), None);
}

#[test]
fn what_is_offered_goes_over_the_wire_as_its_words_the_one_chosen_and_where_the_word_starts() {
    let mut s = session();
    tagged(&mut s);
    let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
    assert_eq!(view["offer"], serde_json::Value::Null);
    typed(&mut s, "oil #fo");
    s.act(Action::Choose(1));
    let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
    assert_eq!(
        view["offer"],
        serde_json::json!({"words": ["#food", "#fox"], "chosen": 1, "from": 4})
    );
    assert_eq!(
        view["hints"][0]["action"],
        serde_json::json!({"take": null})
    );
    assert_eq!(view["hints"][1]["action"], serde_json::json!({"choose": 1}));
    assert_eq!(view["hints"][2]["action"], "dismiss");
}
