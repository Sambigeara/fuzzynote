//! Notes written from outside: `fzn apply`'s rules.

use super::*;

/// A boiler, an open task linked to it, a done one, and a pump.
fn house(s: &mut Session) {
    put(s, "pump", "Pump", &[], 500);
    put(s, "boiler", "Boiler #house", &[], 1_000);
    put(s, "oil", "[ ] Order oil #house", &["boiler"], 2_000);
    put(s, "chimney", "[x] Sweep the chimney #house", &[], 3_000);
    let held = s.notes.get("chimney").unwrap().clone();
    s.commit(Record {
        archived: true,
        ..held
    });
    clear(s);
    s.out.clear();
}

/// A note as `fzn query` writes it, read back as a line given to apply.
fn line(s: &Session, id: &str) -> Given {
    let line = s.notes.version(id).unwrap().line().unwrap();
    serde_json::from_str(&line).unwrap()
}

fn text(s: &Session, id: &str) -> String {
    s.notes.version(id).unwrap().text.clone()
}

#[test]
fn a_round_trip_through_apply_writes_nothing() {
    let mut s = session();
    house(&mut s);
    let lines: Vec<Given> = s.query("#house").iter().map(|r| line(&s, &r.id)).collect();
    assert_eq!(lines.len(), 3);
    let held: Vec<Record> = ["oil", "chimney", "boiler"]
        .iter()
        .map(|id| s.notes.version(id).unwrap().clone())
        .collect();
    let (notes, effects) = s.apply(lines).unwrap();
    assert!(kept(&effects).is_empty(), "{effects:?}");
    let mut ids: Vec<&str> = notes.iter().map(|r| r.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["boiler", "chimney", "oil"]);
    for record in held {
        assert_eq!(s.notes.version(&record.id), Some(&record));
    }
}

#[test]
fn apply_writes_one_version_a_note_that_differs_and_nothing_unless_every_line_can_be() {
    let mut s = session();
    house(&mut s);
    let renamed = Given {
        text: Some("Boiler service #house".into()),
        ..line(&s, "boiler")
    };
    let unknown = Given {
        id: Some("gutter".into()),
        text: Some("Clear the gutter".into()),
        ..Given::default()
    };
    assert_eq!(
        s.apply(vec![renamed.clone(), unknown]),
        Err("no note has the id gutter".to_owned())
    );
    assert_eq!(text(&s, "boiler"), "Boiler #house", "nothing was written");
    assert!(s.out.is_empty());

    assert_eq!(
        s.apply(vec![renamed.clone(), renamed.clone()]),
        Err("boiler is given twice".to_owned())
    );

    let emptied = Given {
        id: Some("oil".into()),
        text: Some("\n\n".into()),
        ..Given::default()
    };
    assert_eq!(
        s.apply(vec![renamed.clone(), emptied]),
        Err("oil has no text; delete it instead".to_owned())
    );

    let oil = line(&s, "oil");
    let (notes, effects) = s.apply(vec![renamed, oil]).unwrap();
    let written = kept(&effects);
    assert_eq!(written.len(), 1, "the oil was given as it is");
    assert_eq!(written[0].text, "Boiler service #house");
    assert_eq!(written[0].updated, Some(NOON));
    assert_eq!(written[0].created, Some(Moment::utc(1_000)), "as held");
    assert_eq!(
        notes.iter().map(|r| r.text.as_str()).collect::<Vec<_>>(),
        ["Boiler service #house", "[ ] Order oil #house"],
        "every note given, as it now stands, in the order given"
    );

    // A line with only an id and a field changes that field, and a date word in it is written
    // out as the editor writes it.
    let (_, effects) = s
        .apply(vec![Given {
            id: Some("boiler".into()),
            text: Some("Boiler service /tomorrow #house".into()),
            ..Given::default()
        }])
        .unwrap();
    assert_eq!(kept(&effects)[0].text, "Boiler service 2026-10-14 #house");
    assert_eq!(
        kept(&effects)[0].links,
        s.notes.get("boiler").unwrap().links
    );

    // Deleted is a tombstone, as Ctrl-D writes, and a deleted note is not brought back.
    let gone = Given {
        deleted: Some(true),
        ..line(&s, "chimney")
    };
    let (notes, _) = s.apply(vec![gone]).unwrap();
    assert!(notes[0].deleted && s.notes.get("chimney").is_none());
    let back = Given {
        id: Some("chimney".into()),
        text: Some("[x] Sweep the chimney #house".into()),
        ..Given::default()
    };
    assert_eq!(s.apply(vec![back]), Err("chimney was deleted".to_owned()));
}

#[test]
fn apply_keeps_a_tasks_box_and_flag_together_whichever_changed() {
    let mut s = session();
    house(&mut s);
    let archived = |s: &Session, id: &str| s.notes.version(id).unwrap().archived;

    // The box ticked in the text archives it, as Ctrl-V would.
    let (_, effects) = s
        .apply(vec![Given {
            text: Some("[x] Order oil #house".into()),
            ..line(&s, "oil")
        }])
        .unwrap();
    assert_eq!(kept(&effects).len(), 1);
    assert!(archived(&s, "oil"));

    // The flag cleared with the box as it was unticks the box.
    s.apply(vec![Given {
        archived: Some(false),
        ..line(&s, "oil")
    }])
    .unwrap();
    assert_eq!(text(&s, "oil"), "[ ] Order oil #house");
    assert!(!archived(&s, "oil"));

    // Both changed and agreeing is the same.
    s.apply(vec![Given {
        text: Some("[x] Order oil #house".into()),
        archived: Some(true),
        ..line(&s, "oil")
    }])
    .unwrap();
    assert!(archived(&s, "oil"));

    // Held disagreeing, as a task archived without its tick, either side changed alone puts it
    // right, and both changed and disagreeing still is said.
    let held = s.notes.get("oil").unwrap().clone();
    s.commit(Record {
        text: "[ ] Order oil #house".into(),
        ..held
    });
    s.out.clear();
    let untouched = line(&s, "oil");
    assert!(kept(&s.apply(vec![untouched.clone()]).unwrap().1).is_empty());
    assert_eq!(
        s.apply(vec![Given {
            text: Some("[x] Order oil #house".into()),
            archived: Some(false),
            ..untouched.clone()
        }]),
        Err("oil's box and archived flag disagree".to_owned())
    );
    s.apply(vec![Given {
        archived: Some(false),
        ..untouched
    }])
    .unwrap();
    assert_eq!(text(&s, "oil"), "[ ] Order oil #house");
    assert!(!archived(&s, "oil"));

    // A note that is no task is archived as given, and a new note's box decides.
    s.apply(vec![Given {
        archived: Some(true),
        ..line(&s, "boiler")
    }])
    .unwrap();
    assert_eq!(text(&s, "boiler"), "Boiler #house");
    assert!(archived(&s, "boiler"));
    let (notes, _) = s
        .apply(vec![Given {
            text: Some("[x] Bleed the radiators #house".into()),
            ..Given::default()
        }])
        .unwrap();
    assert!(notes[0].archived);
    assert_eq!(
        s.apply(vec![Given {
            text: Some("[x] Paint the shed".into()),
            archived: Some(false),
            ..Given::default()
        }]),
        Err("line 1's box and archived flag disagree".to_owned())
    );
}

#[test]
fn a_query_line_says_when_a_note_was_done_and_apply_keeps_that_as_fzns_own() {
    let mut s = session();
    house(&mut s);
    let lines: Vec<String> = s
        .query("#house")
        .iter()
        .map(|r| r.line().unwrap())
        .collect();
    let of = |id: &str| {
        lines
            .iter()
            .find(|line| line.contains(&format!(r#""id":"{id}""#)))
            .unwrap()
            .clone()
    };
    let done = s.notes.get("chimney").unwrap().done.unwrap();
    assert!(of("chimney").contains(r#""archived":true,"done":"2026-10-13T12:00:00.001Z","#));
    assert!(of("oil").contains(r#""archived":false,"done":null,"#));

    // A moment given is not taken, as `updated` is not: the line reads as it was read.
    let given = |line: String| serde_json::from_str::<Given>(&line).unwrap();
    let redated = of("chimney").replace(
        r#""done":"2026-10-13T12:00:00.001Z""#,
        r#""done":"2026-10-12T09:00:00.000Z""#,
    );
    let (_, effects) = s.apply(vec![given(redated)]).unwrap();
    assert!(kept(&effects).is_empty(), "{effects:?}");
    assert_eq!(s.notes.get("chimney").unwrap().done, Some(done));

    // Ticked from outside it is done now, and unticked it is not done.
    let ticked = of("oil").replace("[ ] Order oil", "[x] Order oil");
    let (notes, _) = s.apply(vec![given(ticked)]).unwrap();
    assert!(notes[0].archived);
    assert_eq!(notes[0].done, notes[0].stamp);
    let (notes, _) = s
        .apply(vec![Given {
            archived: Some(false),
            ..line(&s, "chimney")
        }])
        .unwrap();
    assert_eq!(
        (notes[0].text.as_str(), notes[0].done),
        ("[ ] Sweep the chimney #house", None)
    );
}

#[test]
fn apply_refuses_a_note_changed_since_it_was_read() {
    let mut s = session();
    house(&mut s);
    let read = line(&s, "boiler");
    put(&mut s, "boiler", "Boiler serviced #house", &[], 4_000);
    s.out.clear();
    let edited = Given {
        text: Some("Boiler, new pump #house".into()),
        ..read
    };
    assert_eq!(
        s.apply(vec![edited.clone()]),
        Err("boiler has changed since it was read".to_owned())
    );
    assert_eq!(text(&s, "boiler"), "Boiler serviced #house");
    let unstamped = Given {
        stamp: None,
        ..edited
    };
    s.apply(vec![unstamped]).unwrap();
    assert_eq!(text(&s, "boiler"), "Boiler, new pump #house");
}

#[test]
fn apply_follows_brackets_as_the_app_does() {
    let mut s = session();
    house(&mut s);

    // A text changed links what its brackets name, as saving it does, whatever links are given;
    // links given with the text as held are taken as given; one added that names no note here is
    // said.
    let (notes, _) = s
        .apply(vec![Given {
            text: Some("[ ] Order oil for the [[Pump]] #house".into()),
            links: Some(Vec::new()),
            ..line(&s, "oil")
        }])
        .unwrap();
    assert_eq!(notes[0].links, ["pump"]);
    let (notes, _) = s
        .apply(vec![Given {
            links: Some(vec!["boiler".into(), "chimney".into(), "chimney".into()]),
            ..line(&s, "oil")
        }])
        .unwrap();
    assert_eq!(notes[0].links, ["boiler", "chimney"]);
    assert_eq!(
        s.apply(vec![Given {
            links: Some(vec!["gutter".into()]),
            ..line(&s, "oil")
        }]),
        Err("oil links to gutter, which is no note here".to_owned())
    );
}

#[test]
fn a_line_with_no_id_is_a_new_note_and_an_unknown_id_is_refused() {
    let mut s = session();
    house(&mut s);
    let (notes, effects) = s
        .apply(vec![Given {
            id: Some(String::new()),
            text: Some("Bleed the radiators, see [[Pump]]".into()),
            ..Given::default()
        }])
        .unwrap();
    let new = &notes[0];
    assert_eq!(new.id, "new-0001");
    assert_eq!(new.links, ["pump"]);
    assert_eq!((new.created, new.after.as_deref()), (Some(NOON), None));
    assert_eq!(kept(&effects).len(), 1);
    assert_eq!(s.notes.order()[0].id, "new-0001", "at the top");

    for (given, why) in [
        (Given::default(), "line 1 is a new note with no text"),
        (
            Given {
                text: Some("Rake the leaves".into()),
                deleted: Some(true),
                ..Given::default()
            },
            "line 1 is a new note, and cannot be deleted",
        ),
        (
            Given {
                id: Some("leaves".into()),
                text: Some("Rake the leaves".into()),
                ..Given::default()
            },
            "no note has the id leaves",
        ),
    ] {
        assert_eq!(s.apply(vec![given]), Err(why.to_owned()));
    }
}

#[test]
fn apply_does_not_move_a_note() {
    let mut s = session();
    house(&mut s);
    let moved = Given {
        after: Some(Some("chimney".into())),
        ..line(&s, "boiler")
    };
    assert_eq!(
        s.apply(vec![moved]),
        Err("moving boiler is not done from outside yet".to_owned())
    );
    let at_the_top = Given {
        after: Some(None),
        text: Some("Boiler #house #garden".into()),
        ..line(&s, "boiler")
    };
    s.apply(vec![at_the_top]).unwrap();
    assert_eq!(text(&s, "boiler"), "Boiler #house #garden");
    let placed = Given {
        text: Some("Rake the leaves".into()),
        after: Some(Some("boiler".into())),
        ..Given::default()
    };
    assert_eq!(
        s.apply(vec![placed]),
        Err("line 1 places a new note, which is not done from outside yet".to_owned())
    );
}

#[test]
fn a_line_given_reads_what_it_names_and_leaves_what_is_fzns() {
    let given: Given = serde_json::from_str(
        r#"{"v":3,"id":"boiler","text":"Boiler","after":null,"device":"3f9a01c2","placed":null,"created":"2026-10-13T12:00:00Z","done":"2026-10-13T12:00:00Z"}"#,
    )
    .unwrap();
    assert_eq!(
        given,
        Given {
            id: Some("boiler".into()),
            text: Some("Boiler".into()),
            device: Some("3f9a01c2".into()),
            after: Some(None),
            placed: Some(None),
            ..Given::default()
        }
    );
    let given: Given = serde_json::from_str(r#"{"id":"boiler"}"#).unwrap();
    assert_eq!((given.after, given.placed), (None, None));
}

#[test]
fn a_link_held_to_a_deleted_note_is_kept_and_only_a_link_added_must_name_a_note() {
    let mut s = session();
    house(&mut s);
    let held = s.notes.get("boiler").unwrap().clone();
    s.commit(Record {
        deleted: true,
        ..held
    });
    s.out.clear();
    let lines: Vec<Given> = s.query("#house").iter().map(|r| line(&s, &r.id)).collect();
    let (notes, effects) = s.apply(lines).unwrap();
    assert!(kept(&effects).is_empty(), "{effects:?}");
    let oil = notes.iter().find(|r| r.id == "oil").unwrap();
    assert_eq!(oil.links, ["boiler"]);
    assert_eq!(
        s.apply(vec![Given {
            links: Some(vec!["boiler".into(), "gutter".into()]),
            ..line(&s, "oil")
        }]),
        Err("oil links to gutter, which is no note here".to_owned())
    );
}

#[test]
fn a_text_given_as_held_writes_nothing_though_its_brackets_now_name_a_note() {
    let mut s = session();
    house(&mut s);
    put(&mut s, "plan", "The plan, see [[Valve]]", &[], 4_000);
    put(&mut s, "valve", "Valve", &[], 5_000);
    s.out.clear();
    let (notes, effects) = s.apply(vec![line(&s, "plan")]).unwrap();
    assert!(kept(&effects).is_empty(), "{effects:?}");
    assert!(notes[0].links.is_empty());
}

#[test]
fn a_note_moved_since_it_was_read_keeps_its_place_and_takes_the_text_given() {
    let mut s = session();
    house(&mut s);
    let read = line(&s, "boiler");
    let held = s.notes.get("boiler").unwrap().clone();
    s.commit(Record {
        after: Some("chimney".into()),
        placed: Some(NOON),
        ..held
    });
    s.out.clear();
    let (notes, _) = s
        .apply(vec![Given {
            text: Some("Boiler serviced #house".into()),
            ..read.clone()
        }])
        .unwrap();
    assert_eq!(notes[0].text, "Boiler serviced #house");
    assert_eq!(
        notes[0].after.as_deref(),
        Some("chimney"),
        "where it was moved"
    );

    // An after changed with the place as read is a move asked for.
    assert_eq!(
        s.apply(vec![Given {
            after: Some(Some("oil".into())),
            ..line(&s, "boiler")
        }]),
        Err("moving boiler is not done from outside yet".to_owned())
    );
}

#[test]
fn a_stamp_is_the_same_version_in_any_zone_and_another_writers_is_not() {
    let mut s = session();
    house(&mut s);
    let read = line(&s, "boiler");
    let stamp = read.stamp.unwrap();
    let elsewhere = Given {
        text: Some("Boiler serviced #house".into()),
        stamp: Some(Moment {
            offset: 3_600,
            ..stamp
        }),
        ..read.clone()
    };
    assert!(s.apply(vec![elsewhere]).is_ok());
    let read = line(&s, "boiler");
    let other = Given {
        text: Some("Boiler, new pump #house".into()),
        device: Some("3f9a01c2".into()),
        ..read
    };
    assert_eq!(
        s.apply(vec![other]),
        Err("boiler has changed since it was read".to_owned())
    );
}
