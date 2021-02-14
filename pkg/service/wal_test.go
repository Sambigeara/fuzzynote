package service

import (
	"encoding/binary"
	"fmt"
	"os"
	//"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	rootPath = "file_to_delete"
)

func TestWalMerge(t *testing.T) {
	t.Run("Start empty db", func(t *testing.T) {
		walEventLogger := NewWalEventLogger()
		walFile := NewWalFile(rootPath, walDirPattern, walEventLogger)
		fileDS := NewFileDataStore(rootPath, "", walFile)
		fileDS.uuid = fileDS.generateUUID()
		listRepo := NewDBListRepo(NewDbEventLogger(), walEventLogger)

		f, _ := os.Create(rootPath)
		f.Close()
		defer os.Remove(rootPath)

		fileDS.Load(listRepo)
		fileDS.Save(nil, []*ListItem{}, listRepo.NextID)

		if len(*walEventLogger.log) != 0 {
			t.Fatalf("Expected no events in WAL eventLog but had %d", len(*walEventLogger.log))
		}
		if listRepo.Root != nil {
			t.Fatalf("listRepo.Root should not exist")
		}
	})
	t.Run("Load from primary.db", func(t *testing.T) {
		walEventLogger := NewWalEventLogger()
		walFile := NewWalFile(rootPath, walDirPattern, walEventLogger)
		fileDS := NewFileDataStore(rootPath, "", walFile)
		listRepo := NewDBListRepo(NewDbEventLogger(), walEventLogger)

		line0 := []byte("First item")
		line1 := []byte("Second item")
		data := []interface{}{
			uint16(3), // Schema type
			fileDS.generateUUID(),
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

		f, _ := os.Create(rootPath)
		defer os.Remove(rootPath)

		for _, v := range data {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		//runtime.Breakpoint()
		fileDS.Load(listRepo)

		if len(*walEventLogger.log) != 2 {
			t.Fatalf("Expected 2 events in WAL eventLog but had %d", len(*walEventLogger.log))
		}

		listRepo.Match([][]rune{}, -1, true)
		matches := listRepo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*walEventLogger.log))
		}

		if (*walEventLogger.log)[0].listItemID != matches[0].id {
			t.Fatal("First match listItemID should match first eventLog")
		}
		if (*walEventLogger.log)[1].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match second eventLog")
		}

		if (*walEventLogger.log)[0].eventType != addEvent {
			t.Fatal("First match item should be of type addEvent")
		}
		if (*walEventLogger.log)[1].eventType != addEvent {
			t.Fatal("Second match item should be of type addEvent")
		}
	})
	t.Run("Load from primary.db with hidden", func(t *testing.T) {
		walEventLogger := NewWalEventLogger()
		walFile := NewWalFile(rootPath, walDirPattern, walEventLogger)
		fileDS := NewFileDataStore(rootPath, "", walFile)
		listRepo := NewDBListRepo(NewDbEventLogger(), walEventLogger)

		line0 := []byte("First item")
		line1 := []byte("Second item")
		data := []interface{}{
			uint16(3), // Schema type
			fileDS.generateUUID(),
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

		f, _ := os.Create(rootPath)
		defer os.Remove(rootPath)

		for _, v := range data {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		//runtime.Breakpoint()
		fileDS.Load(listRepo)

		if len(*walEventLogger.log) != 3 {
			t.Fatalf("Expected 3 events in WAL eventLog but had %d", len(*walEventLogger.log))
		}

		listRepo.Match([][]rune{}, -1, true)
		matches := listRepo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*walEventLogger.log))
		}

		if (*walEventLogger.log)[0].listItemID != matches[0].id {
			t.Fatal("First match listItemID should match first eventLog")
		}
		if (*walEventLogger.log)[1].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match first eventLog")
		}
		if (*walEventLogger.log)[2].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match first eventLog")
		}

		if (*walEventLogger.log)[0].eventType != addEvent {
			t.Fatal("First match item should be of type addEvent")
		}
		if (*walEventLogger.log)[1].eventType != addEvent {
			t.Fatal("Second match item should be of type addEvent")
		}
		if (*walEventLogger.log)[2].eventType != visibilityEvent {
			t.Fatal("Third match item should be of type toggleVisibility")
		}
	})
	t.Run("Single local WAL merge", func(t *testing.T) {
		walEventLogger := NewWalEventLogger()
		walFile := NewWalFile(rootPath, walDirPattern, walEventLogger)
		fileDS := NewFileDataStore(rootPath, "", walFile)
		listRepo := NewDBListRepo(NewDbEventLogger(), walEventLogger)
		defer os.Remove(rootPath)

		// Load and Save the fileDS to instantiate
		fileDS.Load(listRepo)
		fileDS.Save(nil, []*ListItem{}, listRepo.NextID)

		now := time.Now().Unix()

		line0 := []byte("First item")
		line1 := []byte("Second item")
		data := []interface{}{
			walItemSchema1{
				UUID:            fileDS.uuid,
				LogID:           1,
				ListItemID:      1,
				ChildListItemID: 0,
				UnixTime:        now,
				EventType:       addEvent,
				LineLength:      uint64(len(line0)),
				NoteLength:      0,
			},
			line0,
			walItemSchema1{
				UUID:            fileDS.uuid,
				LogID:           2,
				ListItemID:      2,
				ChildListItemID: 1,
				UnixTime:        now + 1,
				EventType:       addEvent,
				LineLength:      uint64(len(line1)),
				NoteLength:      0,
			},
			line1,
		}

		walPath := fmt.Sprintf(walDirPattern, fileDS.uuid)
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range data {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		fileDS.Load(listRepo)

		if len(*walEventLogger.log) != 2 {
			t.Fatalf("Expected 2 events in WAL eventLog but had %d", len(*walEventLogger.log))
		}

		listRepo.Match([][]rune{}, -1, true)
		matches := listRepo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*walEventLogger.log))
		}

		if (*walEventLogger.log)[0].listItemID != matches[0].id {
			t.Fatal("First match listItemID should match first eventLog")
		}
		if (*walEventLogger.log)[1].listItemID != matches[1].id {
			t.Fatal("Second match listItemID should match second eventLog")
		}

		if (*walEventLogger.log)[0].eventType != addEvent {
			t.Fatal("First match item should be of type addEvent")
		}
		if (*walEventLogger.log)[1].eventType != addEvent {
			t.Fatal("Second match item should be of type addEvent")
		}
	})
	t.Run("Two WAL file merge", func(t *testing.T) {
		walEventLogger := NewWalEventLogger()
		walFile := NewWalFile(rootPath, walDirPattern, walEventLogger)
		fileDS := NewFileDataStore(rootPath, "", walFile)
		listRepo := NewDBListRepo(NewDbEventLogger(), walEventLogger)
		defer os.Remove(rootPath)

		// Load and Save the fileDS to instantiate
		fileDS.Load(listRepo)
		fileDS.Save(nil, []*ListItem{}, listRepo.NextID)

		now0 := time.Now().Unix()
		now1 := now0 + 1
		now2 := now1 + 1
		now3 := now2 + 1

		line0 := []byte("First item")
		line2 := []byte("Third item")
		localData := []interface{}{
			walItemSchema1{
				UUID:            fileDS.uuid,
				LogID:           1,
				ListItemID:      1,
				ChildListItemID: 0,
				UnixTime:        now0,
				EventType:       addEvent,
				LineLength:      0,
				NoteLength:      0,
			},
			walItemSchema1{
				UUID:            fileDS.uuid,
				LogID:           2,
				ListItemID:      1,
				ChildListItemID: 0,
				UnixTime:        now0,
				EventType:       updateEvent,
				LineLength:      uint64(len(line0)),
				NoteLength:      0,
			},
			line0,
			walItemSchema1{
				UUID:            fileDS.uuid,
				LogID:           3,
				ListItemID:      2,
				ChildListItemID: 1,
				UnixTime:        now2,
				EventType:       addEvent,
				LineLength:      0,
				NoteLength:      0,
			},
			walItemSchema1{
				UUID:            fileDS.uuid,
				LogID:           4,
				ListItemID:      2,
				ChildListItemID: 1,
				UnixTime:        now2,
				EventType:       updateEvent,
				LineLength:      uint64(len(line2)),
				NoteLength:      0,
			},
			line2,
		}

		walPath := fmt.Sprintf(walDirPattern, fileDS.uuid)
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range localData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		remoteUUID := fileDS.generateUUID()

		line1 := []byte("Second item")
		line3 := []byte("Fourth item")
		remoteData := []interface{}{
			walItemSchema1{
				UUID:            remoteUUID,
				LogID:           1,
				ListItemID:      1,
				ChildListItemID: 0,
				UnixTime:        now1,
				EventType:       addEvent,
				LineLength:      0,
				NoteLength:      0,
			},
			walItemSchema1{
				UUID:            remoteUUID,
				LogID:           2,
				ListItemID:      1,
				ChildListItemID: 0,
				UnixTime:        now1,
				EventType:       updateEvent,
				LineLength:      uint64(len(line1)),
				NoteLength:      0,
			},
			line1,
			walItemSchema1{
				UUID:            remoteUUID,
				LogID:           3,
				ListItemID:      2,
				ChildListItemID: 1,
				UnixTime:        now3,
				EventType:       addEvent,
				LineLength:      0,
				NoteLength:      0,
			},
			walItemSchema1{
				UUID:            remoteUUID,
				LogID:           4,
				ListItemID:      2,
				ChildListItemID: 1,
				UnixTime:        now3,
				EventType:       updateEvent,
				LineLength:      uint64(len(line3)),
				NoteLength:      0,
			},
			line3,
		}

		walPath = fmt.Sprintf(walDirPattern, remoteUUID)
		f, _ = os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		//runtime.Breakpoint()
		fileDS.Load(listRepo)

		if len(*walEventLogger.log) != 8 {
			t.Fatalf("Expected 8 events in WAL eventLog but had %d", len(*walEventLogger.log))
		}

		listRepo.Match([][]rune{}, -1, true)
		matches := listRepo.matchListItems
		if len(matches) != 4 {
			t.Fatalf("Expected 4 matches items but had %d", len(*walEventLogger.log))
		}

		if (*walEventLogger.log)[1].redoLine != string(line0) {
			t.Fatal("First match line should match first eventLog")
		}
		if (*walEventLogger.log)[3].redoLine != string(line1) {
			t.Fatal("Second match line should match second eventLog")
		}
		if (*walEventLogger.log)[5].redoLine != string(line2) {
			t.Fatal("Third match line should match third eventLog")
		}
		if (*walEventLogger.log)[7].redoLine != string(line3) {
			t.Fatal("Fourth match line should match fourth eventLog")
		}
	})
	t.Run("Merge, save, reload, delete remote merged item, re-merge, item still deleted", func(t *testing.T) {
		walEventLogger := NewWalEventLogger()
		walFile := NewWalFile(rootPath, walDirPattern, walEventLogger)
		fileDS := NewFileDataStore(rootPath, "", walFile)
		listRepo := NewDBListRepo(NewDbEventLogger(), walEventLogger)
		defer os.Remove(rootPath)

		// Load and Save the fileDS to instantiate
		fileDS.Load(listRepo)
		fileDS.Save(nil, []*ListItem{}, listRepo.NextID)

		now0 := time.Now().Unix() - 10 // `-10` Otherwise delete "happens" before these times
		now1 := now0 + 1

		line0 := []byte("First item")
		localData := []interface{}{
			walItemSchema1{
				UUID:            fileDS.uuid,
				LogID:           1,
				ListItemID:      1,
				ChildListItemID: 0,
				UnixTime:        now0,
				EventType:       addEvent,
				LineLength:      uint64(len(line0)),
				NoteLength:      0,
			},
			line0,
		}

		walPath := fmt.Sprintf(walDirPattern, fileDS.uuid)
		f, _ := os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range localData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		remoteUUID := fileDS.generateUUID()

		line1 := []byte("Second item")
		remoteData := []interface{}{
			walItemSchema1{
				UUID:            remoteUUID,
				LogID:           2,
				ListItemID:      1,
				ChildListItemID: 0,
				UnixTime:        now1,
				EventType:       addEvent,
				LineLength:      uint64(len(line1)),
				NoteLength:      0,
			},
			line1,
		}

		walPath = fmt.Sprintf(walDirPattern, remoteUUID)
		f, _ = os.Create(walPath)
		defer os.Remove(walPath)

		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		fileDS.Load(listRepo)

		listRepo.Match([][]rune{}, -1, true)
		matches := listRepo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(*walEventLogger.log))
		}

		if listRepo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if listRepo.Root.parent.parent != nil {
			t.Fatal("Oldest item should have no parent")
		}
		if listRepo.Root != listRepo.Root.parent.child {
			t.Fatal("Root shoud equal Root.parent.child")
		}

		preSaveLog := *walEventLogger.log

		// Save and reload to ensure consistency in event log after write and read to/from disk
		//runtime.Breakpoint()
		fileDS.Save(nil, []*ListItem{}, listRepo.NextID)
		listRepo = NewDBListRepo(NewDbEventLogger(), walEventLogger)
		fileDS.Load(listRepo)

		listRepo.Match([][]rune{}, -1, true)
		matches = listRepo.matchListItems
		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches items but had %d", len(matches))
		}

		if len(*walEventLogger.log) != 2 {
			t.Fatalf("Expected 2 events in WAL eventLog but had %d", len(*walEventLogger.log))
		}

		for i := range [2]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := (*walEventLogger.log)[i]
			if !(cmp.Equal(oldLogItem, newLogItem, cmp.AllowUnexported(oldLogItem, newLogItem))) {
				t.Fatalf("Old log item %v does not equal new log item %v at index %d", oldLogItem, newLogItem, i)
			}
		}

		if listRepo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if listRepo.Root.parent.parent != nil {
			t.Fatal("Oldest item should have no parent")
		}
		if listRepo.Root != listRepo.Root.parent.child {
			t.Fatal("Root shoud equal Root.parent.child")
		}

		listRepo.Delete(1)

		preSaveLog = *walEventLogger.log

		// Flush the merged WAL to disk
		fileDS.Save(nil, []*ListItem{}, listRepo.NextID)

		// Re-write the same remote WAL
		f, _ = os.Create(walPath)
		for _, v := range remoteData {
			err := binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.Close()

		//runtime.Breakpoint()
		fileDS.Load(listRepo)

		for i := range [3]int{} {
			oldLogItem := preSaveLog[i]
			newLogItem := (*walEventLogger.log)[i]
			if !(cmp.Equal(oldLogItem, newLogItem, cmp.AllowUnexported(oldLogItem, newLogItem))) {
				t.Fatalf("Old log item %v does not equal new log item %v at index %d", oldLogItem, newLogItem, i)
			}
		}

		// Event log should still be len == 3 as the second log was pre-existing
		if len(*walEventLogger.log) != 3 {
			t.Fatalf("Expected 3 events in WAL eventLog but had %d", len(*walEventLogger.log))
		}

		if listRepo.Root.child != nil {
			t.Fatal("Root should have no child")
		}
		if listRepo.Root.parent.parent != nil {
			t.Fatal("Oldest item should have no parent")
		}
		if listRepo.Root != listRepo.Root.parent.child {
			t.Fatal("Root shoud equal Root.parent.child")
		}
	})
}
