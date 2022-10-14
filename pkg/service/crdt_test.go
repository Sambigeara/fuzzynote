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

		expectedLen := 5
		if l := len(generated); l != expectedLen {
			t.Fatalf("log should have len %d but has %d", expectedLen, l)
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

func TestCRDTSync(t *testing.T) {
	t.Run("Test events after known", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		log := getLog()

		for _, e := range log {
			repo.processEventLog(e)
		}

		syncEvent := log[3]
		expected := log[4:]

		generated := repo.crdt.sync(syncEvent)

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

		generated := repo.crdt.sync(syncEvent)

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
		expected := log

		generated := repo.crdt.sync(syncEvent)

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
		expected := log

		generated := repo.crdt.sync(syncEvent)

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
		expected := log[1:] // it'll traverse backwards until the lamport changes

		generated := repo.crdt.sync(syncEvent)

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
