package service

import (
	"fmt"
	"testing"
)

func permutationsOfEvents(arr []EventLog) chan []EventLog {
	var helper func([]EventLog, int)
	ch := make(chan []EventLog)

	helper = func(arr []EventLog, n int) {
		if n == 1 {
			tmp := make([]EventLog, len(arr))
			copy(tmp, arr)
			ch <- tmp
		} else {
			for i := 0; i < n; i++ {
				helper(arr, n-1)
				if n%2 == 1 {
					tmp := arr[i]
					arr[i] = arr[n-1]
					arr[n-1] = tmp
				} else {
					tmp := arr[0]
					arr[0] = arr[n-1]
					arr[n-1] = tmp
				}
			}
		}
	}
	go func() {
		helper(arr, len(arr))
		close(ch)
	}()
	return ch
}

func TestCRDTEventEquality(t *testing.T) {
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

func TestCRDTProcessEvent(t *testing.T) {
	t.Run("Test update event", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		vc := map[uuid]int64{
			1: 1,
		}
		key := "1"
		repo.processEventLog(EventLog{
			VectorClock: vc,
			EventType:   UpdateEvent,
			ListItemKey: key,
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 1
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item := matches[0]
		if item.key != key {
			t.Fatalf("item should have key: %s", key)
		}
	})
	t.Run("Test update two linked events in order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		nodeKey0 := "1"
		nodeKey1 := "2"
		vc0 := map[uuid]int64{
			1: 1,
		}
		vc1 := map[uuid]int64{
			1: 2,
		}
		repo.processEventLog(EventLog{
			VectorClock: vc0,
			EventType:   UpdateEvent,
			ListItemKey: nodeKey0,
		})
		repo.processEventLog(EventLog{
			VectorClock:       vc1,
			EventType:         UpdateEvent,
			ListItemKey:       nodeKey1,
			TargetListItemKey: nodeKey0,
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 2
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item1 := matches[0]
		item2 := matches[1]
		if item1.key != nodeKey0 {
			t.Fatalf("node0 should have key: %s", nodeKey0)
		}
		if item2.key != nodeKey1 {
			t.Fatalf("node1 should have key: %s", nodeKey1)
		}
	})
	t.Run("Test update two linked events reverse order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		nodeKey0 := "1"
		nodeKey1 := "2"

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       nodeKey1,
			TargetListItemKey: nodeKey0,
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:   UpdateEvent,
			ListItemKey: nodeKey0,
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 2
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item1 := matches[0]
		item2 := matches[1]
		if item1.key != nodeKey0 {
			t.Fatalf("node0 should have key: %s", nodeKey0)
		}
		if item2.key != nodeKey1 {
			t.Fatalf("node1 should have key: %s", nodeKey1)
		}
	})
	t.Run("Test update two events targeting root", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		nodeKey0 := "1"
		nodeKey1 := "2"
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:   UpdateEvent,
			ListItemKey: nodeKey0,
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:   UpdateEvent,
			ListItemKey: nodeKey1,
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 2
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item1 := matches[0]
		item2 := matches[1]
		if item1.key != nodeKey1 {
			t.Fatalf("node0 should have key: %s", nodeKey1)
		}
		if item2.key != nodeKey0 {
			t.Fatalf("node1 should have key: %s", nodeKey0)
		}
	})
	t.Run("Test two partial linked lists with eventual merge", func(t *testing.T) {
		// End result should be items with ordered keys:
		// 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8
		// but we will add in the order, with sequential vector clocks:
		// 1, 3, 2, 4
		// 8, 6, 7, 5

		repo, clearUp := setupRepo()
		defer clearUp()

		i := int64(1)
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "3",
			TargetListItemKey: "2",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "4",
			TargetListItemKey: "3",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "8",
			TargetListItemKey: "7",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "6",
			TargetListItemKey: "5",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "7",
			TargetListItemKey: "6",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "5",
			TargetListItemKey: "4",
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 8
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		for i, n := range matches {
			if n.key != fmt.Sprintf("%d", i+1) {
				t.Fatalf("expected listItem key %d but got %s", i+1, n.key)
			}
		}
	})
	t.Run("Test updates and delete from linked list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "2",
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 1
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item := matches[0]
		if item.key != "1" {
			t.Fatalf("the wrong item was deleted from the linked list")
		}
	})
	t.Run("Test updates and delete root", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 1
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item := matches[0]
		if item.key != "2" {
			t.Fatalf("the wrong item was deleted from the linked list")
		}
	})
	t.Run("Test delete before updates", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		// throw a duplicate delete in for good measure
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 1
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item := matches[0]
		if item.key != "2" {
			t.Fatalf("the wrong item was deleted from the linked list")
		}
	})
	t.Run("Test position two updates targetting deleted previous root item", func(t *testing.T) {
		// Add 1 <- 2 <- 3
		// Delete 1
		// Position 1 <- 3
		// should end up with: 3 <- 2, tree node key should be 1
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "3",
			TargetListItemKey: "2",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 4,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 5,
			},
			EventType:         PositionEvent,
			ListItemKey:       "3",
			TargetListItemKey: "1",
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 2
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item1 := matches[0]
		item2 := matches[1]
		if item1.key != "3" {
			t.Fatalf("root item has the wrong key")
		}
		if item2.key != "2" {
			t.Fatalf("root item has the wrong key")
		}
	})
	t.Run("Test position two updates targetting deleted previous central item", func(t *testing.T) {
		// Add 1 <- 2 <- 3 <- 4
		// Delete 1, 2
		// Position 2 <- 4
		// should end up with: 4 <- 3, tree node key should be 2
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "3",
			TargetListItemKey: "2",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 4,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "4",
			TargetListItemKey: "3",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 5,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 6,
			},
			EventType:   DeleteEvent,
			ListItemKey: "2",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 7,
			},
			EventType:         PositionEvent,
			ListItemKey:       "4",
			TargetListItemKey: "2",
		})

		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 2
		if l := len(matches); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		item1 := matches[0]
		item2 := matches[1]

		expectedKey := "4"
		if k := item1.key; k != expectedKey {
			t.Fatalf("item1 key should be %s but is %s", expectedKey, k)
		}

		expectedKey = "3"
		if k := item2.key; k != expectedKey {
			t.Fatalf("item2 key should be %s but is %s", expectedKey, k)
		}
	})
}

func TestCRDTAllPermsMix(t *testing.T) {
	repo, clearUp := setupRepo()
	repo.isTest = true
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

	k, _ := repo.Add("", nil, nil)
	nA := repo.listItemCache[k]
	repo.Update("a", nA)

	k, _ = repo.Add("b", nil, nA)
	nB := repo.listItemCache[k]

	k, _ = repo.Add("c", nil, nB)
	nC := repo.listItemCache[k]

	repo.Match([][]rune{}, false, "", 0, 0)
	repo.MoveUp(nC)

	repo.Match([][]rune{}, false, "", 0, 0)
	repo.Delete(nA)

	go func() {
		exit <- struct{}{}
	}()
	correctEl := <-elChan

	// Clear up this repo state prior to running tests below - we only wanted the event log
	clearUp()

	checkFn := func(t *testing.T, repo *DBListRepo) bool {
		// We expect two items in the list as follows: n2 ("c") -> n1 ("b")
		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		success := true
		if len(matches) != 2 {
			t.Errorf("all nodes should exist")
			return false
		}

		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]

		if item1.key != nC.key {
			t.Errorf("first item key is incorrect")
			success = false
		}
		if item1.rawLine != nC.rawLine {
			t.Errorf("first item rawLine is incorrect")
			success = false
		}
		if item2.key != nB.key {
			t.Errorf("second item key is incorrect")
			success = false
		}
		if item2.rawLine != nB.rawLine {
			t.Errorf("second item rawLine is incorrect")
			success = false
		}
		return success
	}
	t.Run("All permutations", func(t *testing.T) {
		repo, clearUp = setupRepo()
		defer clearUp()
		var i int
		for p := range permutationsOfEvents(correctEl) {
			// We can't rely on fresh repos each iterations here because OS+file management lags behind and
			// causes inconsistencies. Therefore, use the same repo and refresh state
			repo.listItemCache = make(map[string]*ListItem)
			repo.addEventSet = make(map[string]EventLog)
			repo.deleteEventSet = make(map[string]EventLog)
			repo.positionEventSet = make(map[string]EventLog)

			// 8! == 40320
			//if i == 120 {
			//runtime.Breakpoint()
			//}

			repo.Replay(p)

			if !checkFn(t, repo) {
				t.Log("failed on iteration: ", i)
				return
			}
			i++
		}
	})
}

func TestCRDTAllPermsMoves(t *testing.T) {
	repo, clearUp := setupRepo()
	repo.isTest = true
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

	k, _ := repo.Add("a", nil, nil)
	nA := repo.listItemCache[k]

	k, _ = repo.Add("b", nil, nA)
	nB := repo.listItemCache[k]

	k, _ = repo.Add("c", nil, nB)
	nC := repo.listItemCache[k]

	repo.Match([][]rune{}, false, "", 0, 0)
	repo.MoveDown(nA)

	repo.Match([][]rune{}, false, "", 0, 0)
	repo.MoveDown(nB)

	go func() {
		exit <- struct{}{}
	}()
	correctEl := <-elChan

	// Clear up this repo state prior to running tests below - we only wanted the event log
	clearUp()

	checkFn := func(t *testing.T, repo *DBListRepo) bool {
		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		success := true
		if len(matches) != 3 {
			t.Errorf("all nodes should exist")
			return false
		}

		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		if item1.key != nA.key {
			t.Errorf("first item key is incorrect")
			success = false
		}
		if item1.rawLine != nA.rawLine {
			t.Errorf("first item rawLine is incorrect")
			success = false
		}
		if item2.key != nB.key {
			t.Errorf("second item key is incorrect")
			success = false
		}
		if item2.rawLine != nB.rawLine {
			t.Errorf("second item rawLine is incorrect")
			success = false
		}
		if item3.key != nC.key {
			t.Errorf("third item key is incorrect")
			success = false
		}
		if item3.rawLine != nC.rawLine {
			t.Errorf("third item rawLine is incorrect")
			success = false
		}
		return success
	}

	t.Run("All permutations", func(t *testing.T) {
		repo, clearUp = setupRepo()
		defer clearUp()
		var i int
		for p := range permutationsOfEvents(correctEl) {
			// We can't rely on fresh repos each iterations here because OS+file management lags behind and
			// causes inconsistencies. Therefore, use the same repo and refresh state
			repo.listItemCache = make(map[string]*ListItem)
			repo.addEventSet = make(map[string]EventLog)
			repo.deleteEventSet = make(map[string]EventLog)
			repo.positionEventSet = make(map[string]EventLog)
			repo.crdt = newTree()

			repo.Replay(p)

			if !checkFn(t, repo) {
				t.Log("failed on iteration: ", i)
				return
			}
			i++
		}
	})
}

func TestCRDTAllPermsDeletes(t *testing.T) {
	repo, clearUp := setupRepo()
	repo.isTest = true
	repo.uuid = uuid(1)

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

	k, _ := repo.Add("a", nil, nil)
	nA := repo.listItemCache[k]

	k, _ = repo.Add("b", nil, nA)
	nB := repo.listItemCache[k]

	k, _ = repo.Add("c", nil, nB)
	nC := repo.listItemCache[k]

	k, _ = repo.Add("d", nil, nC)
	nD := repo.listItemCache[k]

	repo.Match([][]rune{}, false, "", 0, 0)
	repo.Delete(nA)
	repo.Match([][]rune{}, false, "", 0, 0)
	repo.Delete(nD)

	go func() {
		exit <- struct{}{}
	}()
	correctEl := <-elChan

	// Clear up this repo state prior to running tests below - we only wanted the event log
	clearUp()

	checkFn := func(t *testing.T, repo *DBListRepo) bool {
		matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

		success := true
		if len(matches) != 2 {
			t.Errorf("all nodes should exist")
			return false
		}

		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]

		if item1.key != nB.key {
			t.Errorf("first item key is incorrect")
		}
		if item1.rawLine != nB.rawLine {
			t.Errorf("first item rawLine is incorrect")
		}
		if item2.key != nC.key {
			t.Errorf("second item key is incorrect")
		}
		if item2.rawLine != nC.rawLine {
			t.Errorf("second item rawLine is incorrect")
		}

		return success
	}

	t.Run("All permutations", func(t *testing.T) {
		repo, clearUp = setupRepo()
		defer clearUp()
		var i int
		for p := range permutationsOfEvents(correctEl) {
			// We can't rely on fresh repos each iterations here because OS+file management lags behind and
			// causes inconsistencies. Therefore, use the same repo and refresh state
			repo.listItemCache = make(map[string]*ListItem)
			repo.addEventSet = make(map[string]EventLog)
			repo.deleteEventSet = make(map[string]EventLog)
			repo.positionEventSet = make(map[string]EventLog)
			repo.crdt = newTree()

			repo.Replay(p)

			if !checkFn(t, repo) {
				t.Log("failed on iteration: ", i)
				return
			}
			i++
		}
	})
}

//func TestCRDTMergeDeletesReal(t *testing.T) {
//    repo, clearUp := setupRepo()
//    repoUUID := uuid(1)
//    repo.uuid = repoUUID

//    exit := make(chan struct{})
//    elChan := make(chan []EventLog)
//    go func() {
//        el := []EventLog{}
//        for {
//            select {
//            case e := <-repo.eventsChan:
//                el = append(el, e)
//            case <-exit:
//                elChan <- el
//                return
//            }
//        }
//    }()

//    k, _ := repo.Add("a", nil, nil)
//    nA := repo.listItemCache[k]

//    k, _ = repo.Add("b", nil, nA)
//    nB := repo.listItemCache[k]

//    k, _ = repo.Add("c", nil, nB)
//    nC := repo.listItemCache[k]

//    nA.matchParent = nB
//    repo.Delete(nA)

//    k, _ = repo.Add("d", nil, nB)
//    nD := repo.listItemCache[k]

//    k, _ = repo.Add("e", nil, nD)
//    nE := repo.listItemCache[k]

//    k, _ = repo.Add("f", nil, nB)
//    nF := repo.listItemCache[k]

//    k, _ = repo.Add("g", nil, nF)
//    nG := repo.listItemCache[k]

//    nD.matchParent = nE
//    nD.matchChild = nG
//    repo.Delete(nD)

//    go func() {
//        exit <- struct{}{}
//    }()
//    correctEl := <-elChan

//    // Clear up this repo state prior to running tests below - we only wanted the event log
//    clearUp()

//    checkFn := func(t *testing.T, repo *DBListRepo) bool {
//        matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

//        success := true
//        if len(matches) != 5 {
//            t.Errorf("all nodes should exist")
//            return false
//        }

//        item1 := repo.matchListItems[matches[0].key]
//        item2 := repo.matchListItems[matches[1].key]
//        item3 := repo.matchListItems[matches[2].key]
//        item4 := repo.matchListItems[matches[3].key]
//        item5 := repo.matchListItems[matches[4].key]

//        if item1.key != nB.key {
//            t.Errorf("item1 key is incorrect")
//            success = false
//        }
//        if item1.rawLine != nB.rawLine {
//            t.Errorf("item1 rawLine is incorrect")
//            success = false
//        }
//        if item2.key != nF.key {
//            t.Errorf("item2 key is incorrect")
//            success = false
//        }
//        if item2.rawLine != nF.rawLine {
//            t.Errorf("item2 rawLine is incorrect")
//            success = false
//        }
//        if item3.key != nG.key {
//            t.Errorf("item3 key is incorrect")
//            success = false
//        }
//        if item3.rawLine != nG.rawLine {
//            t.Errorf("item3 rawLine is incorrect")
//            success = false
//        }
//        if item4.key != nE.key {
//            t.Errorf("item4 key is incorrect")
//            success = false
//        }
//        if item4.rawLine != nE.rawLine {
//            t.Errorf("item4 rawLine is incorrect")
//            success = false
//        }
//        if item5.key != nC.key {
//            t.Errorf("item5 key is incorrect")
//            success = false
//        }
//        if item5.rawLine != nC.rawLine {
//            t.Errorf("item5 rawLine is incorrect")
//            success = false
//        }
//        return success
//    }

//    t.Run("All permutations", func(t *testing.T) {
//        repo, clearUp = setupRepo()
//        defer clearUp()
//        //for i, p := range permutationsOfEvents(correctEl) {
//        var i int
//        for p := range permutationsOfEvents(correctEl) {
//            // We can't rely on fresh repos each iterations here because OS+file management lags behind and
//            // causes inconsistencies. Therefore, use the same repo and refresh state
//            repo.listItemCache = make(map[string]*ListItem)
//            repo.addEventSet = make(map[string]EventLog)
//            repo.deleteEventSet = make(map[string]EventLog)
//            repo.positionEventSet = make(map[string]EventLog)
//            repo.crdt = newTree()

//            //if i == 5040 {
//            //if i == 0 {
//            //runtime.Breakpoint()
//            //}

//            repo.Replay(p)

//            if !checkFn(t, repo) {
//                t.Log("failed on iteration: ", i)
//                return
//            }
//            i++
//        }
//    })
//}

//func TestCRDTMergeAddsReal(t *testing.T) {
//    repo, clearUp := setupRepo()
//    repoUUID := uuid(1)
//    repo.uuid = repoUUID

//    exit := make(chan struct{})
//    elChan := make(chan []EventLog)
//    go func() {
//        el := []EventLog{}
//        for {
//            select {
//            case e := <-repo.eventsChan:
//                el = append(el, e)
//            case <-exit:
//                elChan <- el
//                return
//            }
//        }
//    }()

//    k, _ := repo.Add("a", nil, nil)
//    nA := repo.listItemCache[k]

//    k, _ = repo.Add("b", nil, nA)
//    nB := repo.listItemCache[k]

//    k, _ = repo.Add("", nil, nA)
//    nC := repo.listItemCache[k]

//    nC.matchChild = nA // this would usually be set in `Match`
//    repo.Update("c", nC)

//    go func() {
//        exit <- struct{}{}
//    }()
//    correctEl := <-elChan

//    // Clear up this repo state prior to running tests below - we only wanted the event log
//    clearUp()

//    checkFn := func(t *testing.T, repo *DBListRepo) bool {
//        matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)

//        success := true
//        if len(matches) != 3 {
//            t.Errorf("all nodes should exist")
//            success = false
//        }

//        item1 := repo.matchListItems[matches[0].key]
//        item2 := repo.matchListItems[matches[1].key]
//        item3 := repo.matchListItems[matches[2].key]

//        if item1.key != nA.key {
//            t.Errorf("item1 key is incorrect")
//            success = false
//        }
//        if item1.rawLine != nA.rawLine {
//            t.Errorf("item1 rawLine is incorrect")
//            success = false
//        }
//        if item2.key != nC.key {
//            t.Errorf("item2 key is incorrect")
//            success = false
//        }
//        if item2.rawLine != nC.rawLine {
//            t.Errorf("item2 rawLine is incorrect")
//            success = false
//        }
//        if item3.key != nB.key {
//            t.Errorf("item3 key is incorrect")
//            success = false
//        }
//        if item3.rawLine != nB.rawLine {
//            t.Errorf("item3 rawLine is incorrect")
//            success = false
//        }

//        return success
//    }

//    t.Run("All permutations", func(t *testing.T) {
//        repo, clearUp = setupRepo()
//        defer clearUp()
//        for i, p := range permutationsOfEvents(correctEl) {
//            // We can't rely on fresh repos each iterations here because OS+file management lags behind and
//            // causes inconsistencies. Therefore, use the same repo and refresh state
//            repo.listItemCache = make(map[string]*ListItem)
//            repo.addEventSet = make(map[string]EventLog)
//            repo.deleteEventSet = make(map[string]EventLog)
//            repo.positionEventSet = make(map[string]EventLog)

//            if i == 2 {
//                //runtime.Breakpoint()
//            }

//            //runtime.Breakpoint()
//            repo.Replay(p)

//            //items := []*ListItem{}
//            //for i := range repo.getListItems() {
//            //    items = append(items, i)
//            //}

//            if !checkFn(t, repo) {
//                t.Log("failed on iteration: ", i)
//                return
//            }
//        }
//    })
//}

//func permutationsOfEventLogs(arr [][]EventLog) [][][]EventLog {
//    var helper func([][]EventLog, int)
//    res := [][][]EventLog{}

//    helper = func(arr [][]EventLog, n int) {
//        if n == 1 {
//            tmp := make([][]EventLog, len(arr))
//            copy(tmp, arr)
//            res = append(res, tmp)
//        } else {
//            for i := 0; i < n; i++ {
//                helper(arr, n-1)
//                if n%2 == 1 {
//                    tmp := arr[i]
//                    arr[i] = arr[n-1]
//                    arr[n-1] = tmp
//                } else {
//                    tmp := arr[0]
//                    arr[0] = arr[n-1]
//                    arr[n-1] = tmp
//                }
//            }
//        }
//    }
//    helper(arr, len(arr))
//    return res
//}

//func TestCRDTMergeReal(t *testing.T) {
//    // The following are event logs generated by a real world scenario with inconsistent merge results
//    el1 := []EventLog{
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 1,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:1",
//            TargetListItemKey: "",
//            Line:              "",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 2,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:1",
//            TargetListItemKey: "",
//            Line:              "a",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 3,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "",
//        },
//    }
//    el2 := []EventLog{
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 4,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "h",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 5,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "he",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 6,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hel",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 7,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hell",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 8,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 9,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello ",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 10,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello h",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 11,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello ho",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 12,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 13,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how ",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 14,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how a",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 15,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how ar",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 16,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how are",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 17,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how are ",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 18,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how are y",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 19,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how are yo",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 20,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how are you",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 21,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:21",
//            TargetListItemKey: "1:3",
//            Line:              "",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 22,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:21",
//            TargetListItemKey: "1:3",
//            Line:              "b",
//        },
//    }
//    el3 := []EventLog{
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 23,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:23",
//            TargetListItemKey: "1:21",
//            Line:              "",
//        },
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 24,
//            },
//            EventType:         UpdateEvent,
//            ListItemKey:       "1:23",
//            TargetListItemKey: "1:21",
//            Line:              "c",
//        },
//    }
//    el4 := []EventLog{
//        {
//            UUID: 1,
//            VectorClock: map[uuid]int64{
//                1: 25,
//            },
//            EventType:         DeleteEvent,
//            ListItemKey:       "1:3",
//            TargetListItemKey: "1:1",
//            Line:              "hello how are you",
//        },
//    }

//    checkFn := func(t *testing.T, matches []ListItem) {
//        // We expect two items in the list as follows: n2 ("c") -> n1 ("b")
//        // We expect 3 nodes, "a" -> "b" -> "c"
//        n1 := matches[0]
//        n2 := matches[1]
//        n3 := matches[2]

//        if n1.key != "1:1" {
//            t.Errorf("node 1 key should be 1:1 but is %s", n1.key)
//        }
//        if n1.rawLine != "a" {
//            t.Errorf("node 1 line should be a but is %s", n1.rawLine)
//        }

//        if n2.key != "1:21" {
//            t.Errorf("node 2 key should be 1:21 but is %s", n2.key)
//        }
//        if n2.rawLine != "b" {
//            t.Errorf("node 2 line should be b but is %s", n2.rawLine)
//        }

//        if n3.key != "1:23" {
//            t.Errorf("node 3 key should be 1:23 but is %s", n3.key)
//        }
//        if n3.rawLine != "c" {
//            t.Errorf("node 3 line should be c but is %s", n3.rawLine)
//        }
//    }

//    t.Run("All permutations", func(t *testing.T) {
//        repo, clearUp := setupRepo()
//        el := [][]EventLog{el1, el2, el3, el4}
//        for _, arr := range permutationsOfEventLogs(el) {
//            // We can't rely on fresh repos each iterations here because OS+file management lags behind and
//            // causes inconsistencies. Therefore, use the same repo and refresh state
//            repo.listItemCache = make(map[string]*ListItem)
//            repo.addEventSet = make(map[string]EventLog)
//            repo.deleteEventSet = make(map[string]EventLog)
//            repo.positionEventSet = make(map[string]EventLog)

//            el := make([]EventLog, 25)
//            i := 0
//            for _, partialEl := range arr {
//                l := len(partialEl)
//                copy(el[i:i+l], partialEl)
//                i += l
//            }

//            repo.Replay(el)

//            matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)
//            checkFn(t, matches)
//        }
//        clearUp()
//    })
//}

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
