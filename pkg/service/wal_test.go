package service

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	//"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const walDirPattern = "wal_%v.db"

func TestEventEquality(t *testing.T) {
	t.Run("Check event comparisons", func(t *testing.T) {
		time1 := time.Now().UnixNano()
		uuid := uuid(1)
		event1 := eventLog{
			unixNanoTime: time1,
			uuid:         uuid,
			eventType:    addEvent,
		}

		time2 := time1 + 1
		event2 := eventLog{
			unixNanoTime: time2,
			uuid:         uuid,
			eventType:    addEvent,
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

func TestWalMerge(t *testing.T) {
	t.Run("Start empty db", func(t *testing.T) {
		repo := NewMockDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		f, _ := os.Create(rootPath)
		defer f.Close()
		defer clearUp(repo)

		repo.Load()

		if len(*repo.wal.fullLog) != 0 {
			t.Fatalf("Expected no events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}
		if repo.Root != nil {
			t.Fatalf("repo.Root should not exist")
		}
	})
	t.Run("Load from primary.db", func(t *testing.T) {
		repo := NewMockDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		f, _ := os.Create(rootPath)
		defer f.Close()
		defer clearUp(repo)

		line0 := []byte("First item")
		line1 := []byte("Second item")
		data := []interface{}{
			uint16(3), // Schema type
			generateUUID(),
			uint64(3), // nextListItemID
			listItemSchema1{
				1,
				0,
				uint64(len(line0)),
				0,
			},
			line0,
			listItemSchema1{
				2,
				0,
				uint64(len(line1)),
				0,
			},
			line1,
		}

		for _, v := range data {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		repo.Load()

		if len(*repo.wal.fullLog) != 2 {
			t.Fatalf("Expected 2 events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*repo.wal.fullLog))
		}

		if (*repo.wal.fullLog)[0].listItemID != matches[0].id {
			t.Fatal("First match listItemID should match first eventLog")
		}
		if (*repo.wal.fullLog)[1].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match second eventLog")
		}

		if (*repo.wal.fullLog)[0].eventType != addEvent {
			t.Fatal("First match item should be of type addEvent")
		}
		if (*repo.wal.fullLog)[1].eventType != addEvent {
			t.Fatal("Second match item should be of type addEvent")
		}
	})
	t.Run("Load from primary.db with hidden", func(t *testing.T) {
		repo := NewMockDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		f, _ := os.Create(rootPath)
		defer f.Close()
		defer clearUp(repo)

		line0 := []byte("First item")
		line1 := []byte("Second item")
		data := []interface{}{
			uint16(3), // Schema type
			generateUUID(),
			uint64(3), // nextListItemID
			listItemSchema1{
				1,
				0,
				uint64(len(line0)),
				0,
			},
			line0,
			listItemSchema1{
				2,
				hidden, // Set hidden bit in metadata
				uint64(len(line1)),
				0,
			},
			line1,
		}

		for _, v := range data {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		repo.Load()

		if len(*repo.wal.fullLog) != 3 {
			t.Fatalf("Expected 3 events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*repo.wal.fullLog))
		}

		if (*repo.wal.fullLog)[0].listItemID != matches[0].id {
			t.Fatal("First match listItemID should match first eventLog")
		}
		if (*repo.wal.fullLog)[1].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match first eventLog")
		}
		if (*repo.wal.fullLog)[2].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match first eventLog")
		}

		if (*repo.wal.fullLog)[0].eventType != addEvent {
			t.Fatal("First match item should be of type addEvent")
		}
		if (*repo.wal.fullLog)[1].eventType != addEvent {
			t.Fatal("Second match item should be of type addEvent")
		}
		if (*repo.wal.fullLog)[2].eventType != hideEvent {
			t.Fatal("Third match item should be of type hideEvent")
		}
	})
	t.Run("Single local WAL merge", func(t *testing.T) {
		repo := NewMockDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp(repo)

		repo.Load()

		now := time.Now().UnixNano()

		line0 := []byte("First item")
		line1 := []byte("Second item")
		data := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now,
				EventType:        addEvent,
				LineLength:       uint64(len(line0)),
				NoteLength:       0,
			},
			line0,
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       2,
				TargetListItemID: 1,
				UnixTime:         now + 1,
				EventType:        addEvent,
				LineLength:       uint64(len(line1)),
				NoteLength:       0,
			},
			line1,
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.wal.uuid))
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range data {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		//runtime.Breakpoint()
		repo.Refresh(nil, nil, true)

		if len(*repo.wal.fullLog) != 2 {
			t.Fatalf("Expected 2 events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*repo.wal.fullLog))
		}

		if (*repo.wal.fullLog)[0].listItemID != matches[0].id {
			t.Fatal("First match listItemID should match first eventLog")
		}
		if (*repo.wal.fullLog)[1].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match second eventLog")
		}

		if (*repo.wal.fullLog)[0].eventType != addEvent {
			t.Fatal("First match item should be of type addEvent")
		}
		if (*repo.wal.fullLog)[1].eventType != addEvent {
			t.Fatal("Second match item should be of type addEvent")
		}
	})
	t.Run("Two WAL file merge", func(t *testing.T) {
		repo := NewMockDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp(repo)

		repo.Load()

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
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now0,
				EventType:        addEvent,
				LineLength:       0,
				NoteLength:       0,
			},
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now1,
				EventType:        updateEvent,
				LineLength:       uint64(len(line0)),
				NoteLength:       0,
			},
			line0,
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       2,
				TargetListItemID: 1,
				UnixTime:         now4,
				EventType:        addEvent,
				LineLength:       0,
				NoteLength:       0,
			},
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       2,
				TargetListItemID: 1,
				UnixTime:         now5,
				EventType:        updateEvent,
				LineLength:       uint64(len(line2)),
				NoteLength:       0,
			},
			line2,
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.wal.uuid))
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
				UUID:             remoteUUID,
				TargetUUID:       remoteUUID,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now2,
				EventType:        addEvent,
				LineLength:       0,
				NoteLength:       0,
			},
			walItemSchema2{
				UUID:             remoteUUID,
				TargetUUID:       remoteUUID,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now3,
				EventType:        updateEvent,
				LineLength:       uint64(len(line1)),
				NoteLength:       0,
			},
			line1,
			walItemSchema2{
				UUID:             remoteUUID,
				TargetUUID:       remoteUUID,
				ListItemID:       2,
				TargetListItemID: 1,
				UnixTime:         now6,
				EventType:        addEvent,
				LineLength:       0,
				NoteLength:       0,
			},
			walItemSchema2{
				UUID:             remoteUUID,
				TargetUUID:       remoteUUID,
				ListItemID:       2,
				TargetListItemID: 1,
				UnixTime:         now7,
				EventType:        updateEvent,
				LineLength:       uint64(len(line3)),
				NoteLength:       0,
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

		repo.Refresh(nil, nil, true)

		if len(*repo.wal.fullLog) != 8 {
			t.Fatalf("Expected 8 events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		if len(matches) != 4 {
			t.Fatalf("Expected 4 matches items but had %d", len(*repo.wal.fullLog))
		}

		if (*repo.wal.fullLog)[1].line != string(line0) {
			t.Fatal("First match line should match first eventLog")
		}
		if (*repo.wal.fullLog)[3].line != string(line1) {
			t.Fatal("Second match line should match second eventLog")
		}
		if (*repo.wal.fullLog)[5].line != string(line2) {
			t.Fatal("Third match line should match third eventLog")
		}
		if (*repo.wal.fullLog)[7].line != string(line3) {
			t.Fatal("Fourth match line should match fourth eventLog")
		}
	})
	t.Run("Merge, save, reload, delete remote merged item, re-merge, item still deleted", func(t *testing.T) {
		repo := NewMockDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp(repo)

		repo.Load()

		now0 := time.Now().UnixNano() - 10 // `-10` Otherwise delete "happens" before these times
		now1 := now0 + 1

		line0 := []byte("First item")
		localData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now0,
				EventType:        addEvent,
				LineLength:       uint64(len(line0)),
				NoteLength:       0,
			},
			line0,
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.wal.uuid))
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
				UUID:             remoteUUID,
				TargetUUID:       remoteUUID,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now1,
				EventType:        addEvent,
				LineLength:       uint64(len(line1)),
				NoteLength:       0,
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

		repo.Refresh(nil, nil, true)

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*repo.wal.fullLog))
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

		preSaveLog := *repo.wal.fullLog
		repo = NewMockDBListRepo(rootDir)
		repo.Load()

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(matches))
		}

		if len(*repo.wal.fullLog) != 2 {
			t.Fatalf("Expected 2 events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}

		for i := range [2]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := (*repo.wal.fullLog)[i]
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

		repo.Delete(1, func() {})
		repo.Refresh(nil, nil, true)
		preSaveLog = *repo.wal.fullLog

		// Re-write the same remote WAL
		f, _ = os.Create(walPath)
		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		repo.Refresh(nil, nil, true)

		for i := range [3]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := (*repo.wal.fullLog)[i]
			// `cmp.Equal` doesn't like function comparisons but they're not relevant for this test, so nullify
			oldLogItem.callback = nil
			newLogItem.callback = nil
			if !(cmp.Equal(oldLogItem, newLogItem, cmp.AllowUnexported(oldLogItem, newLogItem))) {
				t.Fatalf("Old log item %v does not equal new log item %v at index %d", oldLogItem, newLogItem, i)
			}
		}

		// Event log should still be len == 3 as the second log was pre-existing
		if len(*repo.wal.fullLog) != 3 {
			t.Fatalf("Expected 3 events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}

		if repo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if repo.Root.parent != nil {
			t.Fatal("Remaining single item should have no parent")
		}
	})
	t.Run("Two WAL file duplicate merge, Delete item in one, Update same in other", func(t *testing.T) {
		repo := NewMockDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		os.Create(rootPath)
		defer clearUp(repo)

		repo.Load()

		now0 := time.Now().UnixNano()
		now1 := now0 + 1
		now2 := now1 + 1
		now3 := now2 + 1

		line0 := []byte("First item")
		line1 := []byte("Updated item")
		localData := []interface{}{
			latestWalSchemaID,
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now0,
				EventType:        addEvent,
				LineLength:       0,
				NoteLength:       0,
			},
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now1,
				EventType:        updateEvent,
				LineLength:       uint64(len(line0)),
				NoteLength:       0,
			},
			line0,
			// Deviates here
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now2,
				EventType:        deleteEvent,
				LineLength:       0,
				NoteLength:       0,
			},
		}

		walPath := path.Join(rootDir, fmt.Sprintf(walDirPattern, repo.wal.uuid))
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
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now0,
				EventType:        addEvent,
				LineLength:       0,
				NoteLength:       0,
			},
			walItemSchema2{
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now1,
				EventType:        updateEvent,
				LineLength:       uint64(len(line0)),
				NoteLength:       0,
			},
			line0,
			// Deviates here
			walItemSchema2{
				// UUID will be same as item.originUUID
				UUID:             repo.wal.uuid,
				TargetUUID:       repo.wal.uuid,
				ListItemID:       1,
				TargetListItemID: 0,
				UnixTime:         now3,
				EventType:        updateEvent,
				LineLength:       uint64(len(line1)),
				NoteLength:       0,
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

		repo.Refresh(nil, nil, true)

		if len(*repo.wal.fullLog) != 4 {
			t.Fatalf("Expected 4 events in WAL eventLog but had %d", len(*repo.wal.fullLog))
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		if len(matches) != 1 {
			t.Fatalf("Expected 1 matches items but had %d", len(*repo.wal.fullLog))
		}

		if (*repo.wal.fullLog)[0].eventType != addEvent {
			t.Fatal("First event should be of type addEvent")
		}
		if (*repo.wal.fullLog)[1].eventType != updateEvent {
			t.Fatal("First event should be of type addEvent")
		}
		if (*repo.wal.fullLog)[2].eventType != deleteEvent {
			t.Fatal("First event should be of type addEvent")
		}
		if (*repo.wal.fullLog)[3].eventType != updateEvent {
			t.Fatal("First event should be of type updateEvent")
		}
	})
}
