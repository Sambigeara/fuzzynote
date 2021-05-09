package service

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const walDirPattern = "wal_%v.db"

func TestEventEquality(t *testing.T) {
	t.Run("Check event comparisons", func(t *testing.T) {
		time1 := time.Now().UnixNano()
		uuid := uuid(1)
		event1 := EventLog{
			UnixNanoTime: time1,
			UUID:         uuid,
			EventType:    AddEvent,
		}

		time2 := time1 + 1
		event2 := EventLog{
			UnixNanoTime: time2,
			UUID:         uuid,
			EventType:    AddEvent,
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
	t.Run("Check removes all before delete", func(t *testing.T) {
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		el := []EventLog{
			EventLog{
				UnixNanoTime: eventTime,
				UUID:         uuid,
				EventType:    AddEvent,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    UpdateEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    UpdateEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    MoveUpEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    DeleteEvent,
		})

		compactedWal := *(compact(&el))
		if len(compactedWal) != 1 {
			t.Fatalf("Compacted wal should only have the delete event remaining")
		}

		if compactedWal[0].EventType != DeleteEvent {
			t.Fatalf("Compacted wal should only have the delete event remaining")
		}
	})
	t.Run("Check removes all updates before most recent update", func(t *testing.T) {
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		el := []EventLog{
			EventLog{
				UnixNanoTime: eventTime,
				UUID:         uuid,
				EventType:    AddEvent,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    UpdateEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    UpdateEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    MoveUpEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    UpdateEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    MoveDownEvent,
		})

		compactedWal := *(compact(&el))
		if len(compactedWal) != 4 {
			t.Fatalf("Compacted wal should only have the most recent UpdateEvent, the move events and the original AddEvent")
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
		if compactedWal[3].EventType != MoveDownEvent {
			t.Fatalf("Third event should be a moveDownEvent")
		}
	})
	t.Run("Check add and move wals remain untouched", func(t *testing.T) {
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		el := []EventLog{
			EventLog{
				UnixNanoTime: eventTime,
				UUID:         uuid,
				EventType:    AddEvent,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    MoveUpEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    MoveDownEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    MoveDownEvent,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    HideEvent,
		})
		el = append(el, EventLog{
			UnixNanoTime: eventTime,
			UUID:         uuid,
			EventType:    ShowEvent,
		})

		compactedWal := *(compact(&el))
		if len(compactedWal) != 6 {
			t.Fatalf("Compacted wal should be untouched")
		}
	})
}

func TestWalMerge(t *testing.T) {
	testWalChan := generateProcessingWalChan()
	t.Run("Start empty db", func(t *testing.T) {
		localWalFile := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		repo := NewDBListRepo(rootDir, localWalFile, testPushFrequency)
		os.Mkdir(rootDir, os.ModePerm)
		f, _ := os.Create(rootPath)
		defer f.Close()
		defer clearUp()

		repo.Start(testWalChan)

		if len(*repo.log) != 0 {
			t.Fatalf("Expected no events in WAL EventLog but had %d", len(*repo.log))
		}
		if repo.Root != nil {
			t.Fatalf("repo.Root should not exist")
		}
	})
	t.Run("Single local WAL merge", func(t *testing.T) {
		localWalFile := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		repo := NewDBListRepo(rootDir, localWalFile, testPushFrequency)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp()

		repo.Start(testWalChan)

		now := time.Now().UnixNano()

		line0 := []byte("First item")
		line1 := []byte("Second item")
		data := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now,
				EventType:                  AddEvent,
				LineLength:                 uint64(len(line0)),
				NoteLength:                 0,
			},
			line0,
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       2,
				TargetListItemCreationTime: 1,
				EventTime:                  now + 1,
				EventType:                  AddEvent,
				LineLength:                 uint64(len(line1)),
				NoteLength:                 0,
			},
			line1,
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range data {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		eventLog, _ := pull(localWalFile)
		repo.Replay(eventLog)

		if len(*repo.log) != 2 {
			t.Fatalf("Expected 2 events in WAL EventLog but had %d", len(*repo.log))
		}

		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*repo.log))
		}

		if (*repo.log)[0].ListItemCreationTime != matches[0].creationTime {
			t.Fatal("First match ListItemCreationTime should match first EventLog")
		}
		if (*repo.log)[1].ListItemCreationTime != matches[1].creationTime {
			t.Fatal("Second match ListItemCreationTime should match second EventLog")
		}

		if (*repo.log)[0].EventType != AddEvent {
			t.Fatal("First match item should be of type AddEvent")
		}
		if (*repo.log)[1].EventType != AddEvent {
			t.Fatal("Second match item should be of type AddEvent")
		}
	})
	t.Run("Two WAL file merge", func(t *testing.T) {
		localWalFile := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		repo := NewDBListRepo(rootDir, localWalFile, testPushFrequency)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp()

		repo.Start(testWalChan)

		now0 := time.Now().UnixNano()
		now1 := now0 + 1
		now2 := now1 + 1
		now3 := now2 + 1
		now4 := now3 + 1
		now5 := now4 + 1
		now6 := now5 + 1
		now7 := now6 + 1

		line0 := []byte("First item")
		line2 := []byte("Third item")
		localData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now0,
				EventType:                  AddEvent,
				LineLength:                 0,
				NoteLength:                 0,
			},
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now1,
				EventType:                  UpdateEvent,
				LineLength:                 uint64(len(line0)),
				NoteLength:                 0,
			},
			line0,
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       2,
				TargetListItemCreationTime: 1,
				EventTime:                  now4,
				EventType:                  AddEvent,
				LineLength:                 0,
				NoteLength:                 0,
			},
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       2,
				TargetListItemCreationTime: 1,
				EventTime:                  now5,
				EventType:                  UpdateEvent,
				LineLength:                 uint64(len(line2)),
				NoteLength:                 0,
			},
			line2,
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range localData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		remoteUUID := generateUUID()

		line1 := []byte("Second item")
		line3 := []byte("Fourth item")
		remoteData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:                       remoteUUID,
				TargetUUID:                 remoteUUID,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now2,
				EventType:                  AddEvent,
				LineLength:                 0,
				NoteLength:                 0,
			},
			walItemSchema2{
				UUID:                       remoteUUID,
				TargetUUID:                 remoteUUID,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now3,
				EventType:                  UpdateEvent,
				LineLength:                 uint64(len(line1)),
				NoteLength:                 0,
			},
			line1,
			walItemSchema2{
				UUID:                       remoteUUID,
				TargetUUID:                 remoteUUID,
				ListItemCreationTime:       2,
				TargetListItemCreationTime: 1,
				EventTime:                  now6,
				EventType:                  AddEvent,
				LineLength:                 0,
				NoteLength:                 0,
			},
			walItemSchema2{
				UUID:                       remoteUUID,
				TargetUUID:                 remoteUUID,
				ListItemCreationTime:       2,
				TargetListItemCreationTime: 1,
				EventTime:                  now7,
				EventType:                  UpdateEvent,
				LineLength:                 uint64(len(line3)),
				NoteLength:                 0,
			},
			line3,
		}

		walPath = path.Join(rootDir, fmt.Sprintf(walDirPattern, remoteUUID))
		f, _ = os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		eventLog, _ := pull(localWalFile)
		repo.Replay(eventLog)

		if len(*repo.log) != 8 {
			t.Fatalf("Expected 8 events in WAL EventLog but had %d", len(*repo.log))
		}

		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems
		if len(matches) != 4 {
			t.Fatalf("Expected 4 matches items but had %d", len(*repo.log))
		}

		if (*repo.log)[1].Line != string(line0) {
			t.Fatal("First match line should match first EventLog")
		}
		if (*repo.log)[3].Line != string(line1) {
			t.Fatal("Second match line should match second EventLog")
		}
		if (*repo.log)[5].Line != string(line2) {
			t.Fatal("Third match line should match third EventLog")
		}
		if (*repo.log)[7].Line != string(line3) {
			t.Fatal("Fourth match line should match fourth EventLog")
		}
	})
	t.Run("Merge, save, reload, delete remote merged item, re-merge, item still deleted", func(t *testing.T) {
		localWalFile := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		repo := NewDBListRepo(rootDir, localWalFile, testPushFrequency)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp()

		repo.Start(testWalChan)

		now0 := time.Now().UnixNano() - 10 // `-10` Otherwise delete "happens" before these times
		now1 := now0 + 1

		line0 := []byte("First item")
		localData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now0,
				EventType:                  AddEvent,
				LineLength:                 uint64(len(line0)),
				NoteLength:                 0,
			},
			line0,
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range localData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		remoteUUID := generateUUID()

		line1 := []byte("Second item")
		remoteData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:                       remoteUUID,
				TargetUUID:                 remoteUUID,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now1,
				EventType:                  AddEvent,
				LineLength:                 uint64(len(line1)),
				NoteLength:                 0,
			},
			line1,
		}

		walPath = path.Join(rootDir, fmt.Sprintf(walDirPattern, remoteUUID))
		f, _ = os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		eventLog, _ := pull(localWalFile)
		repo.Replay(eventLog)

		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*repo.log))
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

		preSaveLog := *repo.log
		localWalFile = NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		repo = NewDBListRepo(rootDir, localWalFile, testPushFrequency)
		defer clearUp()
		repo.Start(testWalChan)
		localWalFile = NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		localWalFile.processedPartialWals = make(map[string]struct{})
		eventLog, _ = pull(localWalFile)
		repo.Replay(eventLog)

		repo.Match([][]rune{}, true, "")
		matches = repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(matches))
		}

		if len(*repo.log) != 2 {
			t.Fatalf("Expected 2 events in WAL EventLog but had %d", len(*repo.log))
		}

		for i := range [2]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := (*repo.log)[i]
			if !(cmp.Equal(oldLogItem, newLogItem, cmp.AllowUnexported(oldLogItem, newLogItem))) {
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

		repo.Delete(1)
		eventLog, _ = pull(localWalFile)
		repo.Replay(eventLog)
		preSaveLog = *repo.log

		// Re-write the same remote WAL
		f, _ = os.Create(walPath)
		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		eventLog, _ = pull(localWalFile)
		repo.Replay(eventLog)

		for i := range [3]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := (*repo.log)[i]
			// `cmp.Equal` doesn't like function comparisons but they're not relevant for this test, so nullify
			if !(cmp.Equal(oldLogItem, newLogItem, cmp.AllowUnexported(oldLogItem, newLogItem))) {
				t.Fatalf("Old log item %v does not equal new log item %v at index %d", oldLogItem, newLogItem, i)
			}
		}

		// Event log should still be len == 3 as the second log was pre-existing
		if len(*repo.log) != 3 {
			t.Fatalf("Expected 3 events in WAL EventLog but had %d", len(*repo.log))
		}

		if repo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if repo.Root.parent != nil {
			t.Fatal("Remaining single item should have no parent")
		}
	})
	t.Run("Two WAL file duplicate merge, Delete item in one, Update same in other", func(t *testing.T) {
		localWalFile := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		repo := NewDBListRepo(rootDir, localWalFile, testPushFrequency)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp()

		repo.Start(testWalChan)

		now0 := time.Now().UnixNano()
		now1 := now0 + 1
		now2 := now1 + 1
		now3 := now2 + 1

		line0 := []byte("First item")
		line1 := []byte("Updated item")
		localData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now0,
				EventType:                  AddEvent,
				LineLength:                 0,
				NoteLength:                 0,
			},
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now1,
				EventType:                  UpdateEvent,
				LineLength:                 uint64(len(line0)),
				NoteLength:                 0,
			},
			line0,
			// Deviates here
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now2,
				EventType:                  DeleteEvent,
				LineLength:                 0,
				NoteLength:                 0,
			},
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range localData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		remoteData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now0,
				EventType:                  AddEvent,
				LineLength:                 0,
				NoteLength:                 0,
			},
			walItemSchema2{
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now1,
				EventType:                  UpdateEvent,
				LineLength:                 uint64(len(line0)),
				NoteLength:                 0,
			},
			line0,
			// Deviates here
			walItemSchema2{
				// UUID will be same as item.originUUID
				UUID:                       repo.uuid,
				TargetUUID:                 repo.uuid,
				ListItemCreationTime:       1,
				TargetListItemCreationTime: 0,
				EventTime:                  now3,
				EventType:                  UpdateEvent,
				LineLength:                 uint64(len(line1)),
				NoteLength:                 0,
			},
			line1,
		}

		remoteUUID := generateUUID()
		walPath = path.Join(rootDir, fmt.Sprintf(walDirPattern, remoteUUID))
		f, _ = os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		eventLog, _ := pull(localWalFile)
		repo.Replay(eventLog)

		if len(*repo.log) != 4 {
			t.Fatalf("Expected 4 events in WAL EventLog but had %d", len(*repo.log))
		}

		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems
		if len(matches) != 1 {
			t.Fatalf("Expected 1 matches items but had %d", len(*repo.log))
		}

		if (*repo.log)[0].EventType != AddEvent {
			t.Fatal("First event should be of type AddEvent")
		}
		if (*repo.log)[1].EventType != UpdateEvent {
			t.Fatal("First event should be of type AddEvent")
		}
		if (*repo.log)[2].EventType != DeleteEvent {
			t.Fatal("First event should be of type AddEvent")
		}
		if (*repo.log)[3].EventType != UpdateEvent {
			t.Fatal("First event should be of type UpdateEvent")
		}
	})
}

func TestWalFilter(t *testing.T) {
	t.Run("Check includes matching item", func(t *testing.T) {
		matchTerm := "foobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 matchTerm,
			ListItemCreationTime: creationTime,
		})

		wf := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		wf.pushMatchTerm = []rune(matchTerm)
		matchedWal := getMatchedWal(&el, wf)
		if len(*matchedWal) != 2 {
			t.Fatalf("Matched wal should have the same number of events")
		}
	})
	t.Run("Check includes matching and non matching items", func(t *testing.T) {
		matchTerm := "foobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 matchTerm,
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "",
			ListItemCreationTime: creationTime + 1,
		})

		wf := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		wf.pushMatchTerm = []rune(matchTerm)
		matchedWal := getMatchedWal(&el, wf)
		if len(*matchedWal) != 2 {
			t.Fatalf("Matched wal should not include the non matching event")
		}
	})
	t.Run("Check includes previously matching item", func(t *testing.T) {
		matchTerm := "foobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 matchTerm,
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "",
			ListItemCreationTime: creationTime,
		})

		wf := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		wf.pushMatchTerm = []rune(matchTerm)
		matchedWal := getMatchedWal(&el, wf)
		if len(*matchedWal) != 3 {
			t.Fatalf("Matched wal should have the same number of events")
		}
	})
	t.Run("Check doesn't include non matching items", func(t *testing.T) {
		// We currently only operate on full matches
		matchTerm := "fobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "foobar",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "",
			ListItemCreationTime: creationTime,
		})

		wf := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		wf.pushMatchTerm = []rune(matchTerm)
		matchedWal := getMatchedWal(&el, wf)
		if len(*matchedWal) != 0 {
			t.Fatalf("Matched wal should have the same number of events")
		}
	})
	t.Run("Check includes matching item mid line", func(t *testing.T) {
		matchTerm := "foobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 fmt.Sprintf("something something %s", matchTerm),
			ListItemCreationTime: creationTime,
		})

		wf := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		wf.pushMatchTerm = []rune(matchTerm)
		matchedWal := getMatchedWal(&el, wf)
		if len(*matchedWal) != 2 {
			t.Fatalf("Matched wal should have the same number of events")
		}
	})
	t.Run("Check includes matching item after updates", func(t *testing.T) {
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "f",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "fo",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "foo",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "foob",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "fooba",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "foobar",
			ListItemCreationTime: creationTime,
		})

		wf := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		wf.pushMatchTerm = []rune("foobar")
		matchedWal := getMatchedWal(&el, wf)
		if len(*matchedWal) != 7 {
			t.Fatalf("Matched wal should have the same number of events")
		}
	})
	t.Run("Check includes matching item no add with update", func(t *testing.T) {
		matchTerm := "foobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
				Line:                 matchTerm,
			},
		}

		wf := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		wf.pushMatchTerm = []rune(matchTerm)
		matchedWal := getMatchedWal(&el, wf)
		if len(*matchedWal) != 1 {
			t.Fatalf("Matched wal should have the same number of events")
		}
	})
	t.Run("Check includes matching item after post replay updates", func(t *testing.T) {
		localWalFile := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		repo := NewDBListRepo(rootDir, localWalFile, testPushFrequency)
		defer clearUp()
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "f",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "fo",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "foo",
			ListItemCreationTime: creationTime,
		})

		repo.log = &[]EventLog{}
		repo.Replay(&el)
		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems
		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != "foo" {
			t.Fatalf("The item line should be %s", "foo")
		}

		eventTime++
		newEl := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            UpdateEvent,
				Line:                 "foo ",
				ListItemCreationTime: creationTime,
			},
		}
		repo.Replay(&newEl)
		repo.Match([][]rune{}, true, "")
		matches = repo.matchListItems
		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != "foo " {
			t.Fatalf("The item line should be %s", "foo ")
		}

		localWalFile.pushMatchTerm = []rune("foo")
		matchedWal := getMatchedWal(repo.log, localWalFile)
		if len(*matchedWal) != 5 {
			t.Fatalf("Matched wal should have the same number of events")
		}
	})
	t.Run("Check includes matching item after remote flushes further matching updates", func(t *testing.T) {
		walFile1 := NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir)
		// Both repos will talk to the same walfile, but we'll have to instantiate separately, as repo1
		// needs to set explicit match params
		repo1 := NewDBListRepo(rootDir, walFile1, testPushFrequency)
		defer clearUp()
		walFile2 := NewLocalWalFile(testPushFrequency, testPushFrequency, otherRootDir)
		// Create copy
		filteredWalFile := NewLocalWalFile(testPushFrequency, testPushFrequency, otherRootDir)
		filteredWalFile.pushMatchTerm = []rune("foo")
		repo1.RegisterWalFile(filteredWalFile)
		repo2 := NewDBListRepo(otherRootDir, walFile2, testPushFrequency)

		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "f",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "fo",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "foo",
			ListItemCreationTime: creationTime,
		})

		// repo1 pushes filtered wal to shared walfile
		repo1.push(&el, filteredWalFile)
		// repo2 pulls from shared walfile
		filteredEl, _ := pull(walFile2)
		// After replay, the remote repo should see a single matched item
		repo2.Replay(filteredEl)
		repo2.Match([][]rune{}, true, "")
		matches := repo2.matchListItems
		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != "foo" {
			t.Fatalf("The item line should be %s", "foo ")
		}

		// Add another update event in original repo
		eventTime++
		el = []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            UpdateEvent,
				Line:                 "foo ",
				ListItemCreationTime: creationTime,
			},
		}

		repo1.push(&el, filteredWalFile)
		filteredEl, _ = pull(walFile2)
		repo2.Replay(filteredEl)
		repo2.Match([][]rune{}, true, "")
		matches = repo2.matchListItems
		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != "foo " {
			t.Fatalf("The item line should be %s", "foo ")
		}
	})
}

func TestWalReplay(t *testing.T) {
	repo := NewDBListRepo(rootDir, NewLocalWalFile(testPushFrequency, testPushFrequency, rootDir), testPushFrequency)
	defer clearUp()
	t.Run("Check add creates item", func(t *testing.T) {
		line := "foobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
				Line:                 line,
			},
		}

		repo.log = &[]EventLog{}
		repo.Replay(&el)
		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems

		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != line {
			t.Fatalf("The item line should be %s", line)
		}
	})
	t.Run("Check update creates item", func(t *testing.T) {
		line := "foobar"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            UpdateEvent,
				ListItemCreationTime: creationTime,
				Line:                 line,
			},
		}

		repo.log = &[]EventLog{}
		repo.Replay(&el)
		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems

		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != line {
			t.Fatalf("The item line should be %s", line)
		}
	})
	t.Run("Check merge of updates replays correctly", func(t *testing.T) {
		line := "foo"
		uuid := uuid(1)
		eventTime := time.Now().UnixNano()
		creationTime := eventTime
		el := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            AddEvent,
				ListItemCreationTime: creationTime,
			},
		}
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "f",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "fo",
			ListItemCreationTime: creationTime,
		})
		eventTime++
		el = append(el, EventLog{
			UnixNanoTime:         eventTime,
			UUID:                 uuid,
			EventType:            UpdateEvent,
			Line:                 "foo",
			ListItemCreationTime: creationTime,
		})

		repo.log = &[]EventLog{}
		repo.Replay(&el)
		repo.Match([][]rune{}, true, "")
		matches := repo.matchListItems

		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != "foo" {
			t.Fatalf("The item line should be %s", line)
		}

		newEl := []EventLog{
			EventLog{
				UnixNanoTime:         eventTime,
				UUID:                 uuid,
				EventType:            UpdateEvent,
				Line:                 "foo ",
				ListItemCreationTime: creationTime,
			},
		}
		repo.Replay(&newEl)
		repo.Match([][]rune{}, true, "")
		matches = repo.matchListItems

		if len(matches) != 1 {
			t.Fatalf("There should be one matched item")
		}
		if matches[0].Line != "foo " {
			t.Fatalf("The item line should be %s", line)
		}
	})
}
