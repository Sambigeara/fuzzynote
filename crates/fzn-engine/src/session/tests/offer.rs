//! What is offered while a tag or a person is typed, or a note's head inside brackets, and taking
//! it: on the line and in a head, opened by typing and closed by anything else.

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
fn a_tag_written_only_in_code_is_not_offered_and_nothing_is_offered_inside_code() {
    let mut s = session();
    tagged(&mut s);
    put(
        &mut s,
        "fence",
        "Paint the fence `#fab`\n```\n#fan\n```",
        &[],
        7_000,
    );
    s.act(Action::Select(None));
    typed(&mut s, "#f");
    assert_eq!(words(&s), ["#food", "#fox"], "a tag in code is not one");
    clear(&mut s);
    typed(&mut s, "`ls #fo");
    assert_eq!(offered(&s), None, "the word is in code still being typed");
    clear(&mut s);
    typed(&mut s, "`ls` #fo");
    assert_eq!(
        words(&s),
        ["#food", "#fox"],
        "once the code is closed it is offered again"
    );
}

#[test]
fn a_tag_after_a_bang_on_the_line_is_offered_and_taken_after_the_bang() {
    let mut s = session();
    tagged(&mut s);
    typed(&mut s, "!#fo");
    assert_eq!(words(&s), ["#food", "#fox"]);
    s.act(Action::Take(None));
    assert_eq!(s.line, "!#food ");
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

/// A boiler linked to twice, its service once, the radiators not at all, and a note about the
/// house that links to nothing.
fn headed(s: &mut Session) {
    put(s, "boiler", "Boiler", &[], 1_000);
    put(s, "service", "Boiler service", &[], 2_000);
    put(s, "radiators", "Bleed the radiators", &[], 3_000);
    put(s, "pressure", "Pressure dropped", &["boiler"], 4_000);
    put(s, "oil", "Order oil", &["boiler", "service"], 5_000);
    put(s, "house", "House", &[], 6_000);
    s.act(Action::Select(None));
}

#[test]
fn a_head_being_typed_in_brackets_is_offered_the_heads_it_starts_the_most_linked_to_first() {
    let mut s = session();
    headed(&mut s);
    typed(&mut s, "Call about [[");
    assert_eq!(offered(&s), None, "brackets alone start nothing yet");
    typed(&mut s, "b");
    assert_eq!(
        words(&s),
        ["Boiler", "Boiler service", "Bleed the radiators"]
    );
    assert_eq!(labels(&s), ["take", "choose", "close"]);
    typed(&mut s, "OILER  ");
    assert_eq!(
        words(&s),
        ["Boiler service"],
        "in any case, spaces as one, and a space typed after a word ends it"
    );
    clear(&mut s);
    typed(&mut s, "[[boiler s");
    assert_eq!(words(&s), ["Boiler service"]);
    typed(&mut s, "ervice");
    assert_eq!(
        offered(&s),
        None,
        "the head typed whole is nothing to offer"
    );
}

#[test]
fn a_head_is_offered_only_inside_brackets_still_open_and_out_of_code() {
    let mut s = session();
    headed(&mut s);
    typed(&mut s, "[[Boiler]] b");
    assert_eq!(offered(&s), None, "brackets closed");
    clear(&mut s);
    typed(&mut s, "[[Boiler]] [[b");
    assert_eq!(words(&s).len(), 3, "brackets opened after closed ones");
    clear(&mut s);
    typed(&mut s, "[[oil [[b");
    assert_eq!(offered(&s), None, "brackets inside brackets");
    clear(&mut s);
    typed(&mut s, "`[[b");
    assert_eq!(offered(&s), None, "inside code still being typed");
    clear(&mut s);
    typed(&mut s, "`[[` b");
    assert_eq!(offered(&s), None, "brackets in code are not brackets");
    clear(&mut s);
    typed(&mut s, "`ls` [[b");
    assert_eq!(words(&s).len(), 3, "once the code is closed");
    clear(&mut s);
    typed(&mut s, "[[#ho");
    assert_eq!(offered(&s), None, "no tag is offered inside brackets");
    clear(&mut s);
    type_in(&mut s, "[[boiler");
    s.act(Action::At(4));
    assert_eq!(offered(&s), None, "the caret in the middle of a word");
}

#[test]
fn a_caret_walked_into_brackets_opens_nothing_and_escape_closes_until_typed_into() {
    let mut s = session();
    headed(&mut s);
    typed(&mut s, "[[bo");
    s.act(Action::At(2));
    s.act(Action::At(4));
    assert_eq!(offered(&s), None, "the caret walked back to the end of it");
    typed(&mut s, "i");
    assert_eq!(words(&s), ["Boiler", "Boiler service"]);
    s.act(Action::Dismiss);
    assert_eq!(offered(&s), None);
    s.act(Action::At(5));
    assert_eq!(offered(&s), None, "the caret said again opens nothing");
    typed(&mut s, "l");
    assert_eq!(words(&s), ["Boiler", "Boiler service"]);
}

#[test]
fn heads_are_ranked_by_how_often_they_are_linked_to_then_the_newest_then_by_head_five_at_most() {
    let mut s = session();
    put(&mut s, "a", "Garden gate", &[], 1_000);
    put(&mut s, "b", "Garden shed", &[], 2_000);
    put(&mut s, "c", "Garden bench", &[], 3_000);
    put(&mut s, "d", "Garden wall", &[], 4_000);
    put(&mut s, "e", "Garden pond", &[], 900);
    put(&mut s, "f", "Garden path", &[], 900);
    put(&mut s, "g", "Mend it", &["f"], 5_000);
    s.act(Action::Select(None));
    typed(&mut s, "[[gar");
    assert_eq!(
        words(&s),
        [
            "Garden path",
            "Garden wall",
            "Garden bench",
            "Garden shed",
            "Garden gate"
        ]
    );
}

#[test]
fn an_archived_note_is_offered_and_its_links_count() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "bench", "Bench", &[], 3_000);
    put(&mut s, "note", "Once a year", &["boiler"], 4_000);
    select(&mut s, "Boiler");
    s.act(Action::Archive);
    s.act(Action::Select(None));
    typed(&mut s, "[[b");
    assert_eq!(words(&s), ["Boiler", "Bench"]);
}

#[test]
fn the_note_being_written_is_not_offered_and_its_own_links_do_not_count() {
    let mut s = session();
    put(&mut s, "boiler", "Boiler", &[], 1_000);
    put(&mut s, "service", "Boiler service", &[], 500);
    put(&mut s, "call", "Call about it", &["service"], 2_000);
    put(&mut s, "board", "Boiler board", &[], 3_000);
    select(&mut s, "Boiler board");
    edited(&mut s, "Boiler board [[boi");
    s.act(Action::At("Boiler board [[boi".chars().count()));
    assert_eq!(words(&s), ["Boiler service", "Boiler"]);
    edited(&mut s, "Boiler board");
    select(&mut s, "Call about it");
    edited(&mut s, "Call about it [[boi");
    s.act(Action::At("Call about it [[boi".chars().count()));
    assert_eq!(
        words(&s),
        ["Boiler board", "Boiler", "Boiler service"],
        "the service linked to only by the note being written"
    );
}

#[test]
fn notes_sharing_a_head_are_offered_once_as_the_newest_writes_it() {
    let mut s = session();
    put(&mut s, "a", "boiler", &[], 1_000);
    put(&mut s, "b", "Boiler  ", &[], 2_000);
    put(&mut s, "c", "Boiler   service", &[], 3_000);
    s.act(Action::Select(None));
    typed(&mut s, "[[b");
    assert_eq!(words(&s), ["Boiler service", "Boiler"]);
}

#[test]
fn a_head_brackets_could_not_name_is_not_offered() {
    let mut s = session();
    put(&mut s, "a", "Boiler service #house", &[], 1_000);
    put(&mut s, "b", "Boiler | gas", &[], 2_000);
    put(&mut s, "c", "Boiler [x]]", &[], 3_000);
    put(&mut s, "d", "Boiler `make`", &[], 4_000);
    s.act(Action::Select(None));
    typed(&mut s, "[[b");
    assert_eq!(words(&s), ["Boiler `make`"]);
}

#[test]
fn taking_a_head_writes_it_whole_closes_the_brackets_and_puts_the_caret_after_them() {
    let mut s = session();
    headed(&mut s);
    typed(&mut s, "Call about [[boi");
    s.take_caret();
    s.act(Action::Take(None));
    assert_eq!(s.line, "Call about [[Boiler]]");
    assert_eq!(s.take_caret().line, Some("Call about [[Boiler]]".len()));
    assert_eq!(offered(&s), None);

    clear(&mut s);
    type_in(&mut s, "Call about [[boi]] soon");
    s.act(Action::At("Call about [[boi".len()));
    s.act(Action::Choose(1));
    s.act(Action::Take(None));
    assert_eq!(
        s.line, "Call about [[Boiler service]] soon",
        "brackets already there are not written twice"
    );
    assert_eq!(
        s.take_caret().line,
        Some("Call about [[Boiler service]]".len())
    );

    clear(&mut s);
    type_in(&mut s, "Call about [[boi soon");
    s.act(Action::At("Call about [[boi".len()));
    s.act(Action::Take(None));
    assert_eq!(s.line, "Call about [[Boiler]] soon");
}

#[test]
fn a_head_taken_into_a_head_and_saved_becomes_a_link() {
    let mut s = session();
    headed(&mut s);
    select(&mut s, "House");
    edited(&mut s, "House [[serv\nkeep it warm");
    s.act(Action::At("House [[serv".chars().count()));
    assert_eq!(offered(&s), None, "only a head that starts with it");
    edited(&mut s, "House [[boiler s\nkeep it warm");
    s.act(Action::At("House [[boiler s".chars().count()));
    let effects = s.act(Action::Take(None));
    assert_eq!(
        field(&s).unwrap().0,
        "House [[Boiler service]]\nkeep it warm"
    );
    assert_eq!(s.take_caret().head, Some("House [[Boiler service]]".len()));
    assert_eq!(kept(&effects)[0].links, ["service"]);
}

#[test]
fn a_head_taken_on_the_line_and_written_becomes_a_link() {
    let mut s = session();
    headed(&mut s);
    typed(&mut s, "Call about [[bl");
    s.act(Action::Take(None));
    let effects = s.act(Action::Write);
    let new = kept(&effects)
        .into_iter()
        .find(|r| r.text == "Call about [[Bleed the radiators]]")
        .unwrap()
        .clone();
    assert_eq!(new.links, ["radiators"]);
}

#[test]
fn an_offered_head_goes_over_the_wire_as_a_tag_does_from_where_the_head_starts() {
    let mut s = session();
    headed(&mut s);
    typed(&mut s, "oil [[bl");
    let view: serde_json::Value = serde_json::to_value(s.view()).unwrap();
    assert_eq!(
        view["offer"],
        serde_json::json!({"words": ["Bleed the radiators"], "chosen": 0, "from": 6})
    );
}

#[test]
fn a_long_head_is_drawn_cut_and_taken_whole() {
    let mut s = session();
    let long = "Boiler service booked with the engineer for the second week of the month";
    put(&mut s, "long", long, &[], 1_000);
    put(&mut s, "boiler", "Boiler", &[], 2_000);
    s.act(Action::Select(None));
    typed(&mut s, "[[boiler ");
    assert_eq!(words(&s), [text::fit(long, 40)]);
    s.act(Action::Take(None));
    assert_eq!(s.line, format!("[[{long}]]"));
}
