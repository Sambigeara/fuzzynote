package service

import (
	"testing"
)

func TestCRDTLog(t *testing.T) {
	t.Run("Test single event", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		id := uuid(1)
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})

		expected := []EventLog{
			{
				UUID:             id,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := 1
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test two unique events", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		id := uuid(1)
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})

		expected := []EventLog{
			{
				UUID:             id,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             id,
				LamportTimestamp: 2,
				EventType:        UpdateEvent,
				ListItemKey:      "1:2",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := 2
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		for i := range expected {
			if checkEquality(expected[i], generated[i]) != eventsEqual {
				t.Fatalf("items at idx %d should be equal", i)
			}
		}
	})
	t.Run("Test two unique events reversed", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		id := uuid(1)
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})

		expected := []EventLog{
			{
				UUID:             id,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             id,
				LamportTimestamp: 2,
				EventType:        UpdateEvent,
				ListItemKey:      "1:2",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := 2
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		for i := range expected {
			if checkEquality(expected[i], generated[i]) != eventsEqual {
				t.Fatalf("items at idx %d should be equal", i)
			}
		}
	})
	t.Run("Test override event", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		id := uuid(1)
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})

		expected := []EventLog{
			{
				UUID:             id,
				LamportTimestamp: 2,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := 1
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test override event reversed", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		id := uuid(1)
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})

		expected := []EventLog{
			{
				UUID:             id,
				LamportTimestamp: 2,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := 1
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test two equal events", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		id := uuid(1)
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             id,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})

		expected := []EventLog{
			{
				UUID:             id,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := 1
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test updates multiple uuid", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})
		// Colliding lamport
		repo.processEventLog(EventLog{
			UUID:             2,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "2:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 3,
			EventType:        UpdateEvent,
			ListItemKey:      "1:3",
		})
		repo.processEventLog(EventLog{
			UUID:             2,
			LamportTimestamp: 4,
			EventType:        UpdateEvent,
			ListItemKey:      "1:4",
		})
		// override
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 5,
			EventType:        UpdateEvent,
			ListItemKey:      "1:3",
		})

		expected := []EventLog{
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 2,
				EventType:        UpdateEvent,
				ListItemKey:      "1:2",
			},
			{
				UUID:             2,
				LamportTimestamp: 2,
				EventType:        UpdateEvent,
				ListItemKey:      "2:2",
			},
			{
				UUID:             2,
				LamportTimestamp: 4,
				EventType:        UpdateEvent,
				ListItemKey:      "1:4",
			},
			{
				UUID:             1,
				LamportTimestamp: 5,
				EventType:        UpdateEvent,
				ListItemKey:      "1:3",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test delete removes update before position", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        PositionEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        PositionEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 3,
			EventType:        DeleteEvent,
			ListItemKey:      "1:2",
		})

		expected := []EventLog{
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        PositionEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 2,
				EventType:        PositionEvent,
				ListItemKey:      "1:2",
			},
			{
				UUID:             1,
				LamportTimestamp: 3,
				EventType:        DeleteEvent,
				ListItemKey:      "1:2",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test delete removes update after position", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        PositionEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        PositionEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 3,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 4,
			EventType:        DeleteEvent,
			ListItemKey:      "1:2",
		})

		expected := []EventLog{
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        PositionEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 2,
				EventType:        PositionEvent,
				ListItemKey:      "1:2",
			},
			{
				UUID:             1,
				LamportTimestamp: 3,
				EventType:        DeleteEvent,
				ListItemKey:      "1:2",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test old delete ignored", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        PositionEvent,
			ListItemKey:      "1:1",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        PositionEvent,
			ListItemKey:      "1:2",
		})
		// Different client UUID which will resolve as later
		repo.processEventLog(EventLog{
			UUID:             2,
			LamportTimestamp: 3,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		})
		repo.processEventLog(EventLog{
			UUID:             1,
			LamportTimestamp: 3,
			EventType:        DeleteEvent,
			ListItemKey:      "1:2",
		})

		expected := []EventLog{
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        PositionEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 2,
				EventType:        PositionEvent,
				ListItemKey:      "1:2",
			},
			{
				UUID:             1,
				LamportTimestamp: 3,
				EventType:        DeleteEvent,
				ListItemKey:      "1:2",
			},
			{
				UUID:             2,
				LamportTimestamp: 3,
				EventType:        UpdateEvent,
				ListItemKey:      "1:2",
			},
		}

		generated := repo.crdt.getEventLog()

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
	t.Run("Test old updates after deletes", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		el := []EventLog{
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        PositionEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 2,
				EventType:        DeleteEvent,
				ListItemKey:      "1:1",
			},
			{
				UUID:             1,
				LamportTimestamp: 1,
				EventType:        UpdateEvent,
				ListItemKey:      "1:1",
			},
		}

		//runtime.Breakpoint()
		repo.Replay(el)

		expected := el[:2]
		generated := repo.crdt.getEventLog()

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d bue has %d", expectedLen, l)
		}

		if checkEquality(expected[0], generated[0]) != eventsEqual {
			t.Fatalf("items should be equal")
		}
	})
}

func getLog() []EventLog {
	return []EventLog{
		{
			UUID:             1,
			LamportTimestamp: 1,
			EventType:        UpdateEvent,
			ListItemKey:      "1:1",
		},
		{
			UUID:             1,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "1:2",
		},
		// Colliding lamport
		{
			UUID:             2,
			LamportTimestamp: 2,
			EventType:        UpdateEvent,
			ListItemKey:      "2:2",
		},
		{
			UUID:             2,
			LamportTimestamp: 3,
			EventType:        UpdateEvent,
			ListItemKey:      "1:4",
		},
		// override
		{
			UUID:             1,
			LamportTimestamp: 4,
			EventType:        UpdateEvent,
			ListItemKey:      "1:3",
		},
	}
}

// Sync logs are returned in reverse order, without the matching event (only matching in the case of colliding
// lamport clocks). This function is to make result set comparisons easier
func reverseLogRemoveDuplicate(el []EventLog, m EventLog) []EventLog {
	r := []EventLog{}
	for i := len(el) - 1; i >= 0; i-- {
		if i < 0 {
			break
		}
		e := el[i]
		if checkEquality(e, m) != eventsEqual {
			r = append(r, e)
		}
	}
	return r
}

func TestCRDTSync(t *testing.T) {
	t.Run("Test events after known", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		log := getLog()

		for _, e := range log {
			repo.processEventLog(e)
		}

		syncEvent := log[3]
		expected := reverseLogRemoveDuplicate(log[4:], syncEvent)

		generated, _ := repo.crdt.sync(syncEvent)

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		for i := range expected {
			if checkEquality(expected[i], generated[i]) != eventsEqual {
				t.Fatalf("items at idx %d should be equal", i)
			}
		}
	})
	t.Run("Test events same most recent", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		log := getLog()

		for _, e := range log {
			repo.processEventLog(e)
		}

		syncEvent := log[len(log)-1]

		generated, _ := repo.crdt.sync(syncEvent)

		expectedLen := 0
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}
	})
	t.Run("Test events later unknown sync event", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		log := getLog()

		for _, e := range log {
			repo.processEventLog(e)
		}

		syncEvent := EventLog{
			UUID:             2,
			LamportTimestamp: 5,
			EventType:        UpdateEvent,
			ListItemKey:      "5",
		}
		expected := reverseLogRemoveDuplicate(log, syncEvent)

		generated, _ := repo.crdt.sync(syncEvent)

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		for i := range expected {
			if checkEquality(expected[i], generated[i]) != eventsEqual {
				t.Fatalf("items at idx %d should be equal", i)
			}
		}
	})
	t.Run("Test events earlier unknown sync event", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		log := getLog()

		for _, e := range log {
			repo.processEventLog(e)
		}

		syncEvent := EventLog{
			UUID:             3,
			LamportTimestamp: 5,
			EventType:        UpdateEvent,
			ListItemKey:      "3:5",
		}
		expected := reverseLogRemoveDuplicate(log, syncEvent)

		generated, _ := repo.crdt.sync(syncEvent)

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		for i := range expected {
			if checkEquality(expected[i], generated[i]) != eventsEqual {
				t.Fatalf("items at idx %d should be equal", i)
			}
		}
	})
	t.Run("Test events known sync event lamport collision", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		log := getLog()

		for _, e := range log {
			repo.processEventLog(e)
		}

		syncEvent := log[2]
		expected := reverseLogRemoveDuplicate(log[1:], syncEvent) // it'll traverse backwards until the lamport changes

		generated, _ := repo.crdt.sync(syncEvent)

		expectedLen := len(expected)
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
		}

		for i := range expected {
			if checkEquality(expected[i], generated[i]) != eventsEqual {
				t.Fatalf("items at idx %d should be equal", i)
			}
		}
	})
}
