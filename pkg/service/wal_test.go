package service

import (
	//"runtime"
	"testing"
)

const (
	walDirPattern = "wal_%v.db"
)

func TestEventEquality(t *testing.T) {
	t.Run("Check event comparisons", func(t *testing.T) {
		var lamport int64
		id := uuid(1)
		event1 := EventLog{
			VectorClock: map[uuid]int64{
				id: lamport,
			},
			UUID:      id,
			EventType: AddEvent,
		}

		event2 := EventLog{
			VectorClock: map[uuid]int64{
				id: lamport + 1,
			},
			UUID:      id,
			EventType: AddEvent,
		}

		equality := checkEquality(event1, event2)
		if equality != leftEventOlder {
			t.Fatalf("Expected left event to be older")
		}

		equality = checkEquality(event2, event1)
		if equality != rightEventOlder {
			t.Fatalf("Expected right event to be older")
		}

		equality = checkEquality(event1, event1)
		if equality != eventsEqual {
			t.Fatalf("Expected events to be equal")
		}
	})
}

func TestMerge(t *testing.T) {
	repo, clearUp := setupRepo()
	repoUUID := uuid(1)
	repo.uuid = repoUUID

	exit := make(chan struct{})
	elChan := make(chan []EventLog)
	go func() {
		el := []EventLog{}
		for {
			select {
			case e := <-repo.eventsChan:
				el = append(el, e)
			case <-exit:
				elChan <- el
				return
			}
		}
	}()
	//t.Logf("%v", e)

	repo.Add("", nil, nil)
	n0 := repo.Root
	repo.Update("a", n0)

	repo.Add("", nil, n0)
	n1 := n0.parent
	repo.Update("b", n1)

	repo.Add("", nil, n1)
	n2 := n1.parent
	repo.Update("c", n2)

	// matchChild would usually be set during the Match() call, but set here manually
	n2.matchChild = n1
	repo.MoveUp(n2)

	repo.Delete(n0)

	go func() {
		exit <- struct{}{}
	}()
	correctEl := <-elChan

	// Clear up this repo state prior to running tests below - we only wanted the event log
	clearUp()

	checkFn := func(t *testing.T, n *ListItem) {
		// We expect two items in the list as follows: n2 -> n1
		if n.key != n2.key {
			t.Errorf("first item key is incorrect")
		}
		if n.rawLine != n2.rawLine {
			t.Errorf("first item rawLine is incorrect")
		}
		if n.parent.key != n1.key {
			t.Errorf("second item key is incorrect")
		}
		if n.parent.rawLine != n1.rawLine {
			t.Errorf("second item rawLine is incorrect")
		}

		if n.child != nil {
			t.Errorf("first item child should be nil")
		}
		if n.parent.parent != nil {
			t.Errorf("second item parent should be nil")
		}
		if n.parent.child.key != n2.key {
			t.Errorf("second item child should be first item")
		}
	}

	t.Run("Replay in order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		el := make([]EventLog, len(correctEl))
		copy(el, correctEl)

		repo.Replay(el)

		checkFn(t, repo.Root)
	})
	t.Run("Replay adds in reverse order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		el := make([]EventLog, len(correctEl))
		copy(el[0:2], correctEl[4:6]) // c
		copy(el[2:4], correctEl[2:4]) // b
		copy(el[4:6], correctEl[0:2]) // a
		copy(el[6:], correctEl[6:])   // move + delete

		//runtime.Breakpoint()
		repo.Replay(el)

		checkFn(t, repo.Root)
	})
	t.Run("Replay delete first", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		// TODO the below
		el := make([]EventLog, len(correctEl))
		copy(el[0:1], correctEl[7:8]) // delete
		copy(el[1:8], correctEl[0:7])

		repo.Replay(el)

		checkFn(t, repo.Root)
	})
	t.Run("Replay move first", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		// TODO the below
		el := make([]EventLog, len(correctEl))
		copy(el[0:1], correctEl[6:7]) // delete
		copy(el[1:7], correctEl[0:6])
		copy(el[7:8], correctEl[7:8]) // delete

		repo.Replay(el)

		checkFn(t, repo.Root)
	})
	t.Run("Replay in reverse", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		el := make([]EventLog, len(correctEl))
		copy(el, correctEl)

		// Reverse the log
		for i, j := 0, len(el)-1; i < j; i, j = i+1, j-1 {
			el[i], el[j] = el[j], el[i]
		}

		repo.Replay(el)

		checkFn(t, repo.Root)
	})
}

//func TestWalFilter(t *testing.T) {
//    t.Run("Check includes matching item", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 matchTerm,
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 2 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching and non matching items", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 matchTerm,
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "",
//            ListItemCreationTime: creationTime + 1,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 2 {
//            t.Fatalf("Matched wal should not include the non matching event")
//        }
//    })
//    t.Run("Check includes previously matching item", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 matchTerm,
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "",
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 3 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check doesn't include non matching items", func(t *testing.T) {
//        // We currently only operate on full matches
//        matchTerm := "fobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foobar",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "",
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 0 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item mid line", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 fmt.Sprintf("something something %s", matchTerm),
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 2 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item after updates", func(t *testing.T) {
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "f",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foob",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fooba",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foobar",
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune("foobar")
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 7 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item no add with update", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//                Line:                 matchTerm,
//            },
//        }

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 1 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item after post replay updates", func(t *testing.T) {
//        localWalFile := NewLocalFileWalFile(rootDir)
//        webTokenStore := NewFileWebTokenStore(rootDir)
//        os.Mkdir(rootDir, os.ModePerm)
//        defer clearUp()
//        repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "f",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foo",
//            ListItemCreationTime: creationTime,
//        })

//        repo.log = &[]EventLog{}
//        repo.Replay(&el)
//        repo.Match([][]rune{}, true, "", 0, 0)
//        matches := repo.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo" {
//            t.Fatalf("The item line should be %s", "foo")
//        }

//        eventTime++
//        newEl := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            UpdateEvent,
//                Line:                 "foo ",
//                ListItemCreationTime: creationTime,
//            },
//        }
//        repo.Replay(&newEl)
//        repo.Match([][]rune{}, true, "", 0, 0)
//        matches = repo.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo " {
//            t.Fatalf("The item line should be %s", "foo ")
//        }

//        localWalFile.pushMatchTerm = []rune("foo")
//        matchedWal := getMatchedWal(repo.log, localWalFile)
//        if len(*matchedWal) != 5 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item after remote flushes further matching updates", func(t *testing.T) {
//        os.Mkdir(rootDir, os.ModePerm)
//        os.Mkdir(otherRootDir, os.ModePerm)
//        defer clearUp()

//        // Both repos will talk to the same walfile, but we'll have to instantiate separately, as repo1
//        // needs to set explicit match params
//        walFile1 := NewLocalFileWalFile(rootDir)
//        webTokenStore1 := NewFileWebTokenStore(rootDir)
//        repo1 := NewDBListRepo(walFile1, webTokenStore1, testPushFrequency, testPushFrequency)

//        walFile2 := NewLocalFileWalFile(otherRootDir)
//        webTokenStore2 := NewFileWebTokenStore(otherRootDir)
//        repo2 := NewDBListRepo(walFile2, webTokenStore2, testPushFrequency, testPushFrequency)

//        // Create copy
//        filteredWalFile := NewLocalFileWalFile(otherRootDir)
//        filteredWalFile.pushMatchTerm = []rune("foo")
//        repo1.AddWalFile(filteredWalFile)

//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "f",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foo",
//            ListItemCreationTime: creationTime,
//        })

//        // repo1 pushes filtered wal to shared walfile
//        repo1.push(&el, filteredWalFile, "")
//        // repo2 pulls from shared walfile
//        filteredEl, _ := repo2.pull([]WalFile{walFile2})

//        // After replay, the remote repo should see a single matched item
//        repo2.Replay(filteredEl)
//        repo2.Match([][]rune{}, true, "", 0, 0)
//        matches := repo2.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo" {
//            t.Fatalf("The item line should be %s", "foo ")
//        }

//        // Add another update event in original repo
//        eventTime++
//        el = []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            UpdateEvent,
//                Line:                 "foo ",
//                ListItemCreationTime: creationTime,
//            },
//        }

//        repo1.push(&el, filteredWalFile, "")
//        filteredEl, _ = repo2.pull([]WalFile{walFile2})
//        repo2.Replay(filteredEl)
//        repo2.Match([][]rune{}, true, "", 0, 0)
//        matches = repo2.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo " {
//            t.Fatalf("The item line should be %s", "foo ")
//        }
//    })
//}
