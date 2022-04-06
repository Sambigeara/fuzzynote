package service

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"testing"
	//"encoding/binary"
	//"time"
	//"github.com/google/go-cmp/cmp"
)

const (
	walDirPattern = "wal_%v.db"
)

func TestEventEquality(t *testing.T) {
	t.Run("Check event comparisons", func(t *testing.T) {
		var lamport int64
		uuid := uuid(1)
		event1 := EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        AddEvent,
		}

		event2 := EventLog{
			LamportTimestamp: lamport + 1,
			UUID:             uuid,
			EventType:        AddEvent,
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

func TestWalCompact(t *testing.T) {
	t.Skip("Deletion in compaction is currently broken and disabled")
	t.Run("Check removes all including delete", func(t *testing.T) {
		uuid := uuid(1)
		var lamport int64
		el := []EventLog{
			{
				LamportTimestamp: lamport,
				UUID:             uuid,
				EventType:        AddEvent,
			},
		}
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveUpEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        DeleteEvent,
		})

		compactedWal, _ := compact(el)
		if len(compactedWal) != 0 {
			t.Fatalf("Compacted wal should be empty")
		}
	})
	t.Run("Check removes all updates before most recent matching update pair", func(t *testing.T) {
		uuid := uuid(1)
		var lamport int64
		oldNote := []byte("old note")
		newNote := []byte("new note")
		el := []EventLog{
			{
				LamportTimestamp: lamport,
				UUID:             uuid,
				EventType:        AddEvent,
			},
		}
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
			Note:             oldNote,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveUpEvent,
		})
		lamport++
		// This should remain
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		// This should also remain
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
			Note:             newNote,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveDownEvent,
		})

		compactedWal, _ := compact(el)
		checkResult := func() {
			if len(compactedWal) != 5 {
				t.Fatalf("Expected %d events in compacted wal but had %d", 4, len(compactedWal))
			}

			if compactedWal[0].EventType != AddEvent {
				t.Fatalf("First event should be the original AddEvent")
			}
			if compactedWal[1].EventType != MoveUpEvent {
				t.Fatalf("Second event should be a moveUpEvent")
			}
			if compactedWal[2].EventType != UpdateEvent {
				t.Fatalf("Third event should be an UpdateEvent")
			}
			if compactedWal[2].Note != nil {
				t.Fatalf("Third event Update should have a nil Note")
			}
			if compactedWal[3].EventType != UpdateEvent {
				t.Fatalf("Fourth event should be an UpdateEvent")
			}
			if &compactedWal[3].Note != &newNote {
				t.Fatalf("Fourth event should be have a note attached")
			}
			if compactedWal[4].EventType != MoveDownEvent {
				t.Fatalf("Fifth event should be a moveDownEvent")
			}
		}
		checkResult()
		// Run again to ensure idempotency
		checkResult()
	})
	t.Run("Check add and move wals remain untouched", func(t *testing.T) {
		uuid := uuid(1)
		var lamport int64
		el := []EventLog{
			{
				LamportTimestamp: lamport,
				UUID:             uuid,
				EventType:        AddEvent,
			},
		}
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveUpEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveDownEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveDownEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        HideEvent,
		})
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        ShowEvent,
		})

		compactedWal, _ := compact(el)
		if len(compactedWal) != 6 {
			t.Fatalf("Compacted wal should be untouched")
		}
	})
	t.Run("Check wal equality check", func(t *testing.T) {
		uuid := uuid(1)
		var lamport int64
		oldNote := []byte("old note")
		newNote := []byte("new note")
		el := []EventLog{
			{
				LamportTimestamp: lamport,
				UUID:             uuid,
				EventType:        AddEvent,
			},
		}
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
			Note:             oldNote,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveUpEvent,
		})
		lamport++
		// This should remain
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
		})
		lamport++
		// This should also remain
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        UpdateEvent,
			Note:             newNote,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid,
			EventType:        MoveDownEvent,
		})

		compactedWal, _ := compact(el)

		testRootA, _, _ := checkWalIntegrity(el)
		testRootB, _, _ := checkWalIntegrity(compactedWal)
		if !listsAreEquivalent(testRootA, testRootB) {
			t.Fatal("Wals should be equivalent")
		}
	})
	t.Run("Check wal equality check remote origin add", func(t *testing.T) {
		uuid1 := uuid(1)
		var lamport int64
		oldNote := []byte("old note")
		newNote := []byte("new note")
		el := []EventLog{
			{
				LamportTimestamp: lamport,
				UUID:             uuid1,
				EventType:        AddEvent,
			},
		}
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid1,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid1,
			EventType:        UpdateEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid1,
			EventType:        UpdateEvent,
			Note:             oldNote,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid1,
			EventType:        MoveUpEvent,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid(2),
			EventType:        AddEvent,
			Line:             "diff origin line",
		})
		lamport++
		// This should remain
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid1,
			EventType:        UpdateEvent,
		})
		lamport++
		// This should also remain
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid1,
			EventType:        UpdateEvent,
			Note:             newNote,
		})
		lamport++
		el = append(el, EventLog{
			LamportTimestamp: lamport,
			UUID:             uuid1,
			EventType:        MoveDownEvent,
		})

		compactedWal, _ := compact(el)

		testRootA, _, _ := checkWalIntegrity(el)
		testRootB, _, _ := checkWalIntegrity(compactedWal)
		if !listsAreEquivalent(testRootA, testRootB) {
			t.Fatal("Wals should be equivalent")
		}
	})
}

func TestWalMerge(t *testing.T) {
	os.Mkdir(rootDir, os.ModePerm)
	t.Run("Start empty db", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()

		repo := NewDBListRepo(localWalFile, webTokenStore)

		if l := len(repo.log); l != 0 {
			t.Fatalf("Expected no events in WAL EventLog but had %d", l)
		}
		if repo.Root != nil {
			t.Fatal("repo.Root should not exist")
		}
	})
	t.Run("Single local WAL merge", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()

		repo := NewDBListRepo(localWalFile, webTokenStore)

		var lamport0, lamport1 int64
		lamport1++

		line0 := "First item"
		line1 := "Second item"

		strUUID := strconv.Itoa(int(repo.uuid))

		wal := []EventLog{
			{
				UUID:              repo.uuid,
				LamportTimestamp:  lamport0,
				EventType:         AddEvent,
				ListItemKey:       strUUID + ":" + strconv.Itoa(int(lamport0)),
				TargetListItemKey: strUUID + ":0",
				Line:              line0,
			},
			{
				UUID:              repo.uuid,
				LamportTimestamp:  lamport1,
				EventType:         AddEvent,
				ListItemKey:       strUUID + ":" + strconv.Itoa(int(lamport1)),
				TargetListItemKey: strUUID + ":" + strconv.Itoa(int(lamport0)),
				Line:              line1,
			},
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		byteWal, _ := buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		eventLog, _, _ := repo.pull(context.Background(), []WalFile{localWalFile})
		repo.Replay(eventLog)

		if l := len(repo.log); l != 2 {
			t.Fatalf("Expected 2 events in WAL EventLog but had %d", l)
		}

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		if l := len(matches); l != 2 {
			t.Fatalf("Expected 2 matches items but had %d", l)
		}

		if repo.log[0].ListItemKey != matches[0].Key() {
			t.Fatal("First match ListItemKey should match first item Key")
		}
		if repo.log[1].ListItemKey != matches[1].Key() {
			t.Fatal("Second match ListItemKey should match second item Key")
		}

		if repo.log[0].EventType != AddEvent {
			t.Fatal("First match item should be of type AddEvent")
		}
		if repo.log[1].EventType != AddEvent {
			t.Fatal("Second match item should be of type AddEvent")
		}
	})
	t.Run("Two WAL file merge", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()

		repo := NewDBListRepo(localWalFile, webTokenStore)

		const (
			lamport0 int64 = iota
			lamport1
			lamport2
			lamport3
			lamport4
			lamport5
			lamport6
			lamport7
		)

		line0 := "First item"
		line2 := "Third item"

		strUUID := strconv.Itoa(int(repo.uuid))

		wal := []EventLog{
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport0,
				EventType:        AddEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
			},
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport1,
				EventType:        UpdateEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
				Line:             line0,
			},
			{
				UUID:              repo.uuid,
				LamportTimestamp:  lamport4,
				EventType:         AddEvent,
				ListItemKey:       strUUID + ":" + strconv.Itoa(int(lamport4)),
				TargetListItemKey: strUUID + ":" + strconv.Itoa(int(lamport0)),
			},
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport5,
				EventType:        UpdateEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport4)),
				Line:             line2,
			},
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		byteWal, _ := buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		remoteUUID := generateUUID()

		line1 := "Second item"
		line3 := "Fourth item"

		wal = []EventLog{
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport2,
				EventType:        AddEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport2)),
			},
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport3,
				EventType:        UpdateEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport2)),
				Line:             line1,
			},
			{
				UUID:              repo.uuid,
				LamportTimestamp:  lamport6,
				EventType:         AddEvent,
				ListItemKey:       strUUID + ":" + strconv.Itoa(int(lamport6)),
				TargetListItemKey: strUUID + ":" + strconv.Itoa(int(lamport2)),
			},
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport7,
				EventType:        UpdateEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport6)),
				Line:             line3,
			},
		}

		walPath = path.Join(rootDir, fmt.Sprintf(walDirPattern, remoteUUID))
		f, _ = os.Create(walPath)
		defer os.Remove(walPath)

		byteWal, _ = buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		eventLog, _, _ := repo.pull(context.Background(), []WalFile{localWalFile})
		repo.Replay(eventLog)

		if l := len(repo.log); l != 8 {
			t.Fatalf("Expected 8 events in WAL EventLog but had %d", l)
		}

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		if l := len(matches); l != 4 {
			t.Fatalf("Expected 4 matched items but had %d", l)
		}

		if repo.log[1].Line != line0 {
			t.Fatal("First match line should match first EventLog")
		}
		if repo.log[3].Line != line1 {
			t.Fatal("Second match line should match second EventLog")
		}
		if repo.log[5].Line != line2 {
			t.Fatal("Third match line should match third EventLog")
		}
		if repo.log[7].Line != line3 {
			t.Fatal("Fourth match line should match fourth EventLog")
		}
	})
	t.Run("Merge, save, reload, delete remote merged item, re-merge, item still deleted", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()

		repo := NewDBListRepo(localWalFile, webTokenStore)

		var lamport int64

		line0 := "First item"

		strUUID := strconv.Itoa(int(repo.uuid))

		wal := []EventLog{
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport,
				EventType:        AddEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport)),
				Line:             line0,
			},
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		byteWal, _ := buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		line1 := "Second item"

		remoteUUID := generateUUID()
		strUUID = strconv.Itoa(int(remoteUUID))

		// lamport timestamp will need to be the same, given the remote will have no knowledge of the
		// local event
		wal = []EventLog{
			{
				UUID:             remoteUUID,
				LamportTimestamp: lamport,
				EventType:        AddEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport)),
				Line:             line1,
			},
		}

		walPath = path.Join(rootDir, fmt.Sprintf(walDirPattern, remoteUUID))
		f, _ = os.Create(walPath)

		byteWal, _ = buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		eventLog, _, _ := repo.pull(context.Background(), []WalFile{localWalFile})
		repo.Replay(eventLog)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		if l := len(matches); l != 2 {
			t.Fatalf("Expected 2 matches items but had %d", l)
		}

		if l := len(repo.log); l != 2 {
			t.Fatalf("Expected 2 events in WAL EventLog but had %d", l)
		}

		if repo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if repo.Root.parent.parent != nil {
			t.Fatal("Oldest item should have no parent")
		}
		if repo.Root != repo.Root.parent.child {
			t.Fatal("Root shoud equal Root.parent.child")
		}

		preSaveLog := make([]EventLog, len(repo.log))
		copy(preSaveLog, repo.log)

		localWalFile = NewLocalFileWalFile(rootDir)
		webTokenStore = NewFileWebTokenStore(rootDir)

		repo = NewDBListRepo(localWalFile, webTokenStore)

		// This is the only test that requires this as we're calling ListRepo CRUD actions on it
		go func() {
			repo.Start(newTestClient())
		}()

		eventLog, _, _ = repo.pull(context.Background(), []WalFile{localWalFile})
		repo.Replay(eventLog)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(matches))
		}

		if l := len(repo.log); l != 2 {
			t.Fatalf("Expected 2 events in WAL EventLog but had %d", l)
		}

		for i := range [2]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := repo.log[i]
			if !checkEventLogEquality(oldLogItem, newLogItem) {
				t.Fatalf("Old log item %v does not equal new log item %v at index %d", oldLogItem, newLogItem, i)
			}
		}

		if repo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if repo.Root.parent.parent != nil {
			t.Fatal("Oldest item should have no parent")
		}
		if repo.Root != repo.Root.parent.child {
			t.Fatal("Root shoud equal Root.parent.child")
		}

		repo.Delete(repo.Root.parent)

		if l := len(repo.log); l != 3 {
			t.Fatalf("Expected 3 events in WAL EventLog after Delete, but had %d", l)
		}

		eventLog, _, _ = repo.pull(context.Background(), []WalFile{localWalFile})
		repo.Replay(eventLog)

		preSaveLogB = make([]EventLog, len(repo.log))
		copy(preSaveLog, repo.log)

		// Re-write the same remote WAL
		f, _ = os.Create(walPath)
		byteWal, _ = buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		eventLog, _, _ = repo.pull(context.Background(), []WalFile{localWalFile})
		repo.Replay(eventLog)

		for i := range [3]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := repo.log[i]
			if !checkEventLogEquality(oldLogItem, newLogItem) {
				t.Fatalf("Old log item %v does not equal new log item %v at index %d", oldLogItem, newLogItem, i)
			}
		}

		// Event log should still be len == 3 as the second log was pre-existing
		if l := len(repo.log); l != 3 {
			t.Fatalf("Expected 3 events in WAL EventLog but had %d", l)
		}

		if repo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if repo.Root.parent != nil {
			t.Fatal("Remaining single item should have no parent")
		}
	})
	t.Run("Two WAL file duplicate merge, Delete item in one, Update same in other", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()

		repo := NewDBListRepo(localWalFile, webTokenStore)

		const (
			lamport0 int64 = iota
			lamport1
			lamport2
			lamport3
		)

		line0 := "First item"
		line1 := "Updated item"

		strUUID := strconv.Itoa(int(repo.uuid))

		wal := []EventLog{
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport0,
				EventType:        AddEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
			},
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport1,
				EventType:        UpdateEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
				Line:             line0,
			},
			// Deviates here
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport2,
				EventType:        DeleteEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
			},
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		byteWal, _ := buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		remoteUUID := generateUUID()
		strUUID = strconv.Itoa(int(remoteUUID))

		wal = []EventLog{
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport0,
				EventType:        AddEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
			},
			{
				UUID:             repo.uuid,
				LamportTimestamp: lamport1,
				EventType:        UpdateEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
				Line:             line0,
			},
			// Deviates here, but lamport timestamp will remain the same
			{
				UUID:             remoteUUID,
				LamportTimestamp: lamport2,
				EventType:        UpdateEvent,
				ListItemKey:      strUUID + ":" + strconv.Itoa(int(lamport0)),
				Line:             line1,
			},
		}

		walPath = path.Join(rootDir, fmt.Sprintf(walDirPattern, remoteUUID))
		f, _ = os.Create(walPath)
		defer os.Remove(walPath)

		byteWal, _ = buildByteWal(wal)
		f.Write(byteWal.Bytes())
		f.Close()

		eventLog, _, _ := repo.pull(context.Background(), []WalFile{localWalFile})
		repo.Replay(eventLog)

		if l := len(repo.log); l != 4 {
			t.Fatalf("Expected 4 events in WAL EventLog but had %d", l)
		}

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		if l := len(matches); l != 1 {
			t.Fatalf("Expected 1 matches items but had %d", l)
		}

		if repo.log[0].EventType != AddEvent {
			t.Fatal("First event should be of type AddEvent")
		}
		if repo.log[1].EventType != UpdateEvent {
			t.Fatal("Second event should be of type UpdateEvent")
		}
		if repo.log[2].EventType != DeleteEvent {
			t.Fatal("Third event should be of type DeleteEvent")
		}
		if repo.log[3].EventType != UpdateEvent {
			t.Fatal("Fourth event should be of type UpdateEvent")
		}
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

//func TestWalReplay(t *testing.T) {
//    os.Mkdir(rootDir, os.ModePerm)
//    os.Mkdir(rootDir, os.ModePerm)
//    defer clearUp()
//    repo := NewDBListRepo(
//        NewLocalFileWalFile(rootDir),
//        NewFileWebTokenStore(rootDir),
//        testPushFrequency,
//        testPushFrequency,
//    )
//    t.Run("Check add creates item", func(t *testing.T) {
//        line := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//                Line:                 line,
//            },
//        }

//        repo.log = &[]EventLog{}
//        repo.Replay(&el)
//        repo.Match([][]rune{}, true, "", 0, 0)
//        matches := repo.matchListItems

//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != line {
//            t.Fatalf("The item line should be %s", line)
//        }
//    })
//    t.Run("Check update creates item", func(t *testing.T) {
//        line := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            UpdateEvent,
//                ListItemCreationTime: creationTime,
//                Line:                 line,
//            },
//        }

//        repo.log = &[]EventLog{}
//        repo.Replay(&el)
//        repo.Match([][]rune{}, true, "", 0, 0)
//        matches := repo.matchListItems

//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != line {
//            t.Fatalf("The item line should be %s", line)
//        }
//    })
//    t.Run("Check merge of updates replays correctly", func(t *testing.T) {
//        line := "foo"
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
//            t.Fatalf("The item line should be %s", line)
//        }

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
//            t.Fatalf("The item line should be %s", line)
//        }
//    })
//}
