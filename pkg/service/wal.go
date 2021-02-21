package service

import (
	"encoding/binary"
	//"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	//"runtime"
	"time"
	"unsafe"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 1

type walItemSchema1 struct {
	UUID             uuid
	ListItemID       uint64
	TargetListItemID uint64
	UnixTime         int64
	EventType        eventType
	LineLength       uint64
	NoteLength       uint64
}

// Ordering of these enums are VERY IMPORTANT as they're used for comparisons when resolving WAL merge conflicts
// (although there has to be nanosecond level collisions in order for this to be relevant)
const (
	nullEvent eventType = iota
	addEvent
	updateEvent
	moveUpEvent
	moveDownEvent
	showEvent
	hideEvent
	deleteEvent
)

type eventLog struct {
	uuid             uuid
	listItemID       uint64 // auto-incrementing ID, unique for a given DB UUID
	targetListItemID uint64
	unixNanoTime     int64
	eventType        eventType
	line             string
	note             *[]byte
}

func (r *DBListRepo) callFunctionForEventLog(e eventLog) (*ListItem, error) {
	item := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.listItemID)]
	targetItem := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.targetListItemID)]

	var err error
	switch e.eventType {
	case addEvent:
		item, err = r.add(e.line, e.note, targetItem, e.uuid)
		item.id = e.listItemID
		r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, item.id)] = item
	case deleteEvent:
		err = r.del(item)
		delete(r.wal.listItemTracker, fmt.Sprintf("%d:%d", e.uuid, e.listItemID))
	case updateEvent:
		// We have to cover an edge case here which occurs when merging two remote WALs. If the following occurs:
		// - wal1 creates item A
		// - wal2 copies wal1
		// - wal2 deletes item A
		// - wal1 updates item A
		// - wal1 copies wal2
		// We will end up with an attempted Update on a nonexistent item.
		// In this case, we will Add an item back in with the updated content
		// NOTE A side effect of this will be that the re-added item will be at the top of the list as it
		// becomes tricky to deal with child IDs
		if item != nil {
			item, err = r.update(e.line, e.note, item)
		} else {
			addEl := e
			addEl.eventType = addEvent
			item, err = r.callFunctionForEventLog(addEl)
		}
	case moveUpEvent:
		newChild := targetItem.child
		newParent := targetItem
		err = r.moveItem(item, newChild, newParent)
	case moveDownEvent:
		newChild := targetItem
		newParent := targetItem.parent
		err = r.moveItem(item, newChild, newParent)
	case showEvent:
		err = r.setVisibility(item, true)
	case hideEvent:
		err = r.setVisibility(item, false)
	}
	return item, err
}

func (r *DBListRepo) add(line string, note *[]byte, childItem *ListItem, uuid uuid) (*ListItem, error) {
	if note == nil {
		note = &[]byte{}
	}
	newItem := &ListItem{
		Line:       line,
		id:         r.NextID,
		child:      childItem,
		Note:       note,
		originUUID: uuid,
	}
	r.NextID++

	// If `child` is nil, it's the first item in the list so set as root and return
	if childItem == nil {
		oldRoot := r.Root
		r.Root = newItem
		if oldRoot != nil {
			newItem.parent = oldRoot
			oldRoot.child = newItem
		}
		return newItem, nil
	}

	if childItem.parent != nil {
		childItem.parent.child = newItem
		newItem.parent = childItem.parent
	}
	childItem.parent = newItem

	return newItem, nil
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) update(line string, note *[]byte, listItem *ListItem) (*ListItem, error) {
	line = r.parseOperatorGroups(line)
	listItem.Line = line
	listItem.Note = note
	return listItem, nil
}

func (r *DBListRepo) del(item *ListItem) error {
	if item.child != nil {
		item.child.parent = item.parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		r.Root = item.parent
	}

	if item.parent != nil {
		item.parent.child = item.child
	}

	return nil
}

func (r *DBListRepo) moveItem(item *ListItem, newChild *ListItem, newParent *ListItem) error {
	// Close off gap from source location (for whole dataset)
	if item.child != nil {
		item.child.parent = item.parent
	}
	if item.parent != nil {
		item.parent.child = item.child
	}

	// Insert item into new position based on Matched pointers
	item.child = newChild
	item.parent = newParent

	// Update pointers at target location
	if newParent != nil {
		newParent.child = item
	}
	if newChild != nil {
		newChild.parent = item
	}

	// Update root if required
	// for loop because it might have to traverse multiple times
	for r.Root.child != nil {
		r.Root = r.Root.child
	}
	return nil
}

func (r *DBListRepo) setVisibility(item *ListItem, isVisible bool) error {
	item.IsHidden = !isVisible
	return nil
}

func (w *Wal) replay(r *DBListRepo, primaryRoot *ListItem) error {
	// TODO remove this temp measure
	// To deal with legacy pre-WAL versions, if there are WAL files present, build an initial one based
	// on the state of the `primary.db` and return that. It will involve a number of Add and setVisibility
	// events
	if len(*w.log) == 0 {
		var err error
		w.log, err = buildWalFromPrimary(w.uuid, primaryRoot)
		if err != nil {
			return err
		}
	}

	// If still no events, return nil
	if len(*w.log) == 0 {
		return nil
	}

	// At the moment, we're bypassing the primary.db entirely, so we track the maxID from the WAL
	// and then set the global NextID afterwards, to avoid wastage.
	nextID := uint64(1)

	for _, e := range *w.log {
		item, _ := r.callFunctionForEventLog(e)

		if nextID <= item.id {
			nextID = item.id + 1
		}
	}

	r.NextID = nextID

	return nil
}

func buildWalFromPrimary(uuid uuid, item *ListItem) (*[]eventLog, error) {
	primaryLogs := []eventLog{}
	// We need to implement a nasty and non-guaranteed hack here, but it's only super
	// temporary. In order to ensure uniqueness, each record needs to have it's own UUID.
	// We don't know how many records there will be without traversing, and because I'm
	// lazy, it's easier just to preset `now` to 1 year ago and increment a nanosecond
	// at a time
	now := time.Now().AddDate(-1, 0, 0).UnixNano()

	for item != nil {
		var targetListItemID uint64
		if item.child != nil {
			targetListItemID = item.child.id
		}
		el := eventLog{
			uuid:             uuid,
			listItemID:       item.id,
			targetListItemID: targetListItemID,
			unixNanoTime:     now,
			eventType:        addEvent,
			line:             item.Line,
			note:             item.Note,
		}
		primaryLogs = append(primaryLogs, el)

		if item.IsHidden {
			now++
			el.eventType = hideEvent
			el.unixNanoTime = now
			primaryLogs = append(primaryLogs, el)
		}
		now++
		item = item.parent
	}

	return &primaryLogs, nil
}

func getNextEventLogFromWalFile(f *os.File) (eventLog, error) {
	item := walItemSchema1{}
	el := eventLog{}

	err := binary.Read(f, binary.LittleEndian, &item)
	if err != nil {
		return el, err
	}

	// ptr is initially set to nil as any "add" events wont have corresponding ptrs, so we deal with this post-merge
	el.listItemID = item.ListItemID
	el.targetListItemID = item.TargetListItemID
	el.unixNanoTime = item.UnixTime
	el.uuid = item.UUID
	el.eventType = item.EventType

	line := make([]byte, item.LineLength)
	err = binary.Read(f, binary.LittleEndian, &line)
	if err != nil {
		return el, err
	}
	el.line = string(line)

	if item.NoteLength > 0 {
		note := make([]byte, item.NoteLength)
		err = binary.Read(f, binary.LittleEndian, &note)
		if err != nil {
			return el, err
		}
		el.note = &note
	}

	return el, nil
}

func (w *Wal) load() error {
	localWalFilePath := fmt.Sprintf(w.walPathPattern, w.uuid)

	// Initially, we want to create a single merged eventLog for all non-local WAL files
	// To do this, we find and open each file, and traverse through each simultaneously with an N-pointer approach
	remoteFiles := make(map[*os.File]struct{}) // Effectively a set implementation (struct{} cheaper than bool}
	fileNames, err := filepath.Glob(fmt.Sprintf(w.walPathPattern, "*"))
	if err != nil {
		panic(err)
	}

	for _, fileName := range fileNames {
		f, err := os.OpenFile(fileName, os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}

		// The first two bytes of each file represents the file schema ID. For now this means nothing
		// so we can seek forwards 2 bytes
		f.Seek(int64(unsafe.Sizeof(latestWalSchemaID)), io.SeekStart)

		remoteFiles[f] = struct{}{}
		defer f.Close()
		if fileName != localWalFilePath {
			defer os.Remove(fileName)
		}
	}

	// This maps the open remote WAL file to the current head of that file, useful when comparing each head to
	// determine order of single output
	curEventLogs := make(map[*os.File]eventLog)
	mergedRemoteLogs := []eventLog{}

	// We keep track of open files to determine how many current eventLogs to store in the map
	// Initially, we grab the first item from each open file to populate curEventLogs
	// We then pop the oldest item and append it to the merged logs, and replace with another item from that file.
	// We repeat this until end of file.
	// When we close a file, we reduce remoteFiles so we know to compare on fewer items.
	//runtime.Breakpoint()
	for f := range remoteFiles {
		e, err := getNextEventLogFromWalFile(f)
		if err != nil {
			switch err {
			case io.EOF:
				continue
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on remote WAL merge:", err)
				return err
			default:
				return err
			}
		}
		curEventLogs[f] = e
	}

	for len(curEventLogs) > 0 {
		// Get the current oldest item, and the file that it resides in
		var curFile *os.File = nil
		oldest := eventLog{}

		for f := range remoteFiles {
			if oldest.eventType == nullEvent {
				// Deal with initial iteration
				oldest = curEventLogs[f]
				curFile = f
			} else {
				e := curEventLogs[f]
				if e.unixNanoTime < oldest.unixNanoTime {
					oldest = e
					curFile = f
				} else if e.unixNanoTime == oldest.unixNanoTime {
					// If unixNanoTime's are the same, order by uuid and then eventType to ensure consistent ordering
					if e.uuid < oldest.uuid {
						oldest = e
						curFile = f
					} else if e.uuid == oldest.uuid {
						if e.eventType < oldest.eventType {
							oldest = e
							curFile = f
						}
					}
				}
			}
		}

		// Because we have guaranteed ordering by unixNanoTime -> uuid -> eventType, we can compare the current "oldest"
		// against the head of the mergedRemoteLogs - if they share the three attributes above (strictly only uuid
		// and rowID) we can ignore. Otherwise, we append to the logs
		if oldest.eventType != nullEvent {
			if len(mergedRemoteLogs) > 0 {
				head := mergedRemoteLogs[len(mergedRemoteLogs)-1]
				// This is an inverse (De Morgan's Law) of: IGNORE if UUIDs and unixNanoTime's are the same
				if head.uuid != oldest.uuid || head.unixNanoTime != oldest.unixNanoTime {
					mergedRemoteLogs = append(mergedRemoteLogs, oldest)
				}
			} else {
				mergedRemoteLogs = append(mergedRemoteLogs, oldest)
			}
		}

		// Retrieve the next log from that file. If EOF, remove it from the list of remoteFiles
		nextEvent, err := getNextEventLogFromWalFile(curFile)
		if err != nil {
			if err == io.EOF {
				delete(curEventLogs, curFile)
			} else {
				fmt.Println("binary.Read failed on remote WAL merge:", err)
				return err
			}
		} else {
			if nextEvent.eventType != nullEvent {
				curEventLogs[curFile] = nextEvent
			}
		}
	}

	w.log = &mergedRemoteLogs

	return nil

	// Then we want to merge with the local WAL. There are two scenarios to handle here... TODO
}

func (w *Wal) save() error {
	walFilePath := fmt.Sprintf(w.walPathPattern, w.uuid)

	f, err := os.Create(walFilePath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	// Write the schema ID
	err = binary.Write(f, binary.LittleEndian, latestWalSchemaID)

	for _, item := range *w.log {
		lenLine := uint64(len([]byte(item.line)))
		var lenNote uint64
		if item.note != nil {
			lenNote = uint64(len(*(item.note)))
		}
		i := walItemSchema1{
			UUID:             item.uuid,
			ListItemID:       item.listItemID,
			TargetListItemID: item.targetListItemID,
			UnixTime:         item.unixNanoTime,
			EventType:        item.eventType,
			LineLength:       lenLine,
			NoteLength:       lenNote,
		}
		data := []interface{}{
			i,
			[]byte(item.line),
		}
		if item.note != nil {
			data = append(data, item.note)
		}
		for _, v := range data {
			err = binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				fmt.Printf("binary.Write failed when writing field for WAL log item %v: %s\n", v, err)
				log.Fatal(err)
				return err
			}
		}
	}

	return nil
}
