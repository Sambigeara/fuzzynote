package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"
	//"unsafe"
	//"errors"
	//"runtime"

	"github.com/rogpeppe/go-internal/lockedfile"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 2

// TODO these don't need to be public attributes
type walItemSchema1 struct {
	UUID             uuid
	ListItemID       uint64
	TargetListItemID uint64
	UnixTime         int64
	EventType        eventType
	LineLength       uint64
	NoteLength       uint64
}

type walItemSchema2 struct {
	UUID             uuid
	TargetUUID       uuid
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
	cursorMoveEvent
)

type eventLog struct {
	uuid             uuid
	listItemID       uint64 // auto-incrementing ID, unique for a given DB UUID
	targetUUID       uuid
	targetListItemID uint64
	unixNanoTime     int64
	eventType        eventType
	line             string
	note             *[]byte
	//curRoot          *ListItem
	callback func()
}

func (r *DBListRepo) CallFunctionForEventLog(root *ListItem, e eventLog) (*ListItem, *ListItem, error) {
	item := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.listItemID)]
	targetItem := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.targetUUID, e.targetListItemID)]

	// When we're calling this function on initial WAL merge and load, we may come across
	// orphaned items. There MIGHT be a valid case to keep events around if the eventType
	// is Update. Item will obviously never exist for Add. For all other eventTypes,
	// we should just skip the event and return
	// However, skip this check for cursorMoveEvent, as they are a special case
	if e.eventType != cursorMoveEvent && item == nil && e.eventType != addEvent && e.eventType != updateEvent {
		return root, nil, nil
	}

	var err error
	switch e.eventType {
	case addEvent:
		root, item, err = r.wal.add(root, e.line, e.note, targetItem, e.uuid)
		item.id = e.listItemID
		r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, item.id)] = item
		//r.NextID++
		if item.id >= r.NextID {
			r.NextID = item.id + 1
		}
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
			item, err = r.wal.update(e.line, e.note, item)
		} else {
			addEl := e
			addEl.eventType = addEvent
			root, item, err = r.CallFunctionForEventLog(root, addEl)
		}
	case deleteEvent:
		root, err = r.wal.del(root, item)
		delete(r.wal.listItemTracker, fmt.Sprintf("%d:%d", e.uuid, e.listItemID))
	case moveUpEvent:
		// If targetItem is nil, we avoid the callback and thus avoid the cursor move event
		if targetItem == nil {
			return root, nil, nil
		}
		newChild := targetItem.child
		newParent := targetItem
		root, err = r.wal.moveItem(root, item, newChild, newParent)
	case moveDownEvent:
		if targetItem == nil {
			return root, nil, nil
		}
		newChild := targetItem
		newParent := targetItem.parent
		root, err = r.wal.moveItem(root, item, newChild, newParent)
	case showEvent:
		err = r.wal.setVisibility(item, true)
	case hideEvent:
		err = r.wal.setVisibility(item, false)
	}
	// TODO make this better
	// Callback adds to the cursor movement channel to be actioned by the client
	e.callback()
	return root, item, err
}

func (w *Wal) add(root *ListItem, line string, note *[]byte, childItem *ListItem, uuid uuid) (*ListItem, *ListItem, error) {
	if note == nil {
		note = &[]byte{}
	}
	newItem := &ListItem{
		Line:       line,
		child:      childItem,
		Note:       note,
		originUUID: uuid,
	}

	// If `child` is nil, it's the first item in the list so set as root and return
	if childItem == nil {
		oldRoot := root
		root = newItem
		if oldRoot != nil {
			newItem.parent = oldRoot
			oldRoot.child = newItem
		}
		return root, newItem, nil
	}

	if childItem.parent != nil {
		childItem.parent.child = newItem
		newItem.parent = childItem.parent
	}
	childItem.parent = newItem

	return root, newItem, nil
}

// Update will update the line or note of an existing ListItem
func (w *Wal) update(line string, note *[]byte, listItem *ListItem) (*ListItem, error) {
	line = parseOperatorGroups(line)
	listItem.Line = line
	listItem.Note = note
	return listItem, nil
}

func (w *Wal) del(root *ListItem, item *ListItem) (*ListItem, error) {
	if item.child != nil {
		item.child.parent = item.parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		root = item.parent
	}

	if item.parent != nil {
		item.parent.child = item.child
	}

	return root, nil
}

func (w *Wal) moveItem(root *ListItem, item *ListItem, newChild *ListItem, newParent *ListItem) (*ListItem, error) {
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
	for root.child != nil {
		root = root.child
	}
	return root, nil
}

func (w *Wal) setVisibility(item *ListItem, isVisible bool) error {
	item.IsHidden = !isVisible
	return nil
}

func (r *DBListRepo) replay(root *ListItem, primaryRoot *ListItem) (*ListItem, uint64, error) {
	// At the moment, we're bypassing the primary.db entirely, so we track the maxID from the WAL
	// and then set the global NextID afterwards, to avoid wastage.
	// TODO look at this??
	nextID := uint64(1)

	// TODO remove this temp measure
	// To deal with legacy pre-WAL versions, if there are WAL files present, build an initial one based
	// on the state of the `primary.db` and return that. It will involve a number of Add and setVisibility
	// events
	if len(r.wal.merge(r.wal.fullLog, r.wal.log)) == 0 {
		var err error
		r.wal.fullLog, err = buildWalFromPrimary(r.wal.uuid, primaryRoot)
		if err != nil {
			return root, nextID, err
		}
	}

	fullLog := r.wal.merge(r.wal.fullLog, r.wal.log)
	// If still no events, return nil
	if len(fullLog) == 0 {
		return root, nextID, nil
	}

	//runtime.Breakpoint()
	for _, e := range fullLog {
		var item *ListItem
		// Stub out the cursor callback
		// TODO move to deserialisation?
		e.callback = func() {}
		root, item, _ = r.CallFunctionForEventLog(root, e)

		// Item can be nil in the distributed merge orphaned item cases
		if item != nil {
			if nextID <= item.id {
				nextID = item.id + 1
			}
		}
	}

	return root, nextID, nil
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

func getNextEventLogFromWalFile(f *os.File, schemaVersionID uint16) (*eventLog, error) {
	el := eventLog{}
	if schemaVersionID == uint16(1) {
		item := walItemSchema1{}
		err := binary.Read(f, binary.LittleEndian, &item)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		el.line = string(line)

		if item.NoteLength > 0 {
			note := make([]byte, item.NoteLength)
			err = binary.Read(f, binary.LittleEndian, &note)
			if err != nil {
				return nil, err
			}
			el.note = &note
		}
	} else if schemaVersionID == uint16(2) {
		item := walItemSchema2{}
		err := binary.Read(f, binary.LittleEndian, &item)
		if err != nil {
			return nil, err
		}
		el.listItemID = item.ListItemID
		el.targetListItemID = item.TargetListItemID
		el.unixNanoTime = item.UnixTime
		el.uuid = item.UUID
		el.targetUUID = item.TargetUUID
		el.eventType = item.EventType

		// TODO remove, this is roughly covering a broken previous build
		if el.targetUUID == uuid(0) {
			el.targetUUID = item.UUID
		}

		line := make([]byte, item.LineLength)
		err = binary.Read(f, binary.LittleEndian, &line)
		if err != nil {
			return nil, err
		}
		el.line = string(line)

		if item.NoteLength > 0 {
			note := make([]byte, item.NoteLength)
			err = binary.Read(f, binary.LittleEndian, &note)
			if err != nil {
				return nil, err
			}
			el.note = &note
		}
	}
	return &el, nil
}

func flush(f *os.File, walLog *[]eventLog) error {
	// Write the schema ID
	err := binary.Write(f, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		return err
	}

	for _, item := range *walLog {
		lenLine := uint64(len([]byte(item.line)))
		var lenNote uint64
		if item.note != nil {
			lenNote = uint64(len(*(item.note)))
		}
		i := walItemSchema2{
			UUID:             item.uuid,
			TargetUUID:       item.targetUUID,
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

func (w *Wal) buildFromFile(f *os.File) ([]eventLog, error) {
	// The first two bytes of each file represents the file schema ID. For now this means nothing
	// so we can seek forwards 2 bytes
	//f.Seek(int64(unsafe.Sizeof(latestWalSchemaID)), io.SeekStart)

	el := []eventLog{}
	var walSchemaVersionID uint16
	err := binary.Read(f, binary.LittleEndian, &walSchemaVersionID)
	if err != nil {
		if err == io.EOF {
			return el, nil
		} else {
			log.Fatal(err)
			return el, err
		}
	}

	for {
		e, err := getNextEventLogFromWalFile(f, walSchemaVersionID)
		if err != nil {
			switch err {
			case io.EOF:
				return el, nil
			//case io.ErrUnexpectedEOF:
			default:
				fmt.Println("binary.Read failed on remote WAL sync:", err)
				return el, err
			}
		}
		el = append(el, *e)
	}
}

const (
	eventsEqual int = iota
	leftEventOlder
	rightEventOlder
)

func checkEquality(event1 eventLog, event2 eventLog) int {
	// TODO I think BODMAS covers us here so remove the parantheses (check first)
	if event1.unixNanoTime < event2.unixNanoTime ||
		event1.unixNanoTime == event2.unixNanoTime && event1.uuid < event2.uuid ||
		event1.unixNanoTime == event2.unixNanoTime && event1.uuid == event2.uuid && event1.eventType < event2.eventType {
		return leftEventOlder
	} else if event2.unixNanoTime < event1.unixNanoTime ||
		event2.unixNanoTime == event1.unixNanoTime && event2.uuid < event1.uuid ||
		event2.unixNanoTime == event1.unixNanoTime && event2.uuid == event1.uuid && event2.eventType < event1.eventType {
		return rightEventOlder
	}
	return eventsEqual
}

func (w *Wal) merge(wal1 *[]eventLog, wal2 *[]eventLog) []eventLog {
	//if len(*wal1) == 0 {
	//    return wal2
	//} else if len(*wal2) == 0 {
	//    return wal1
	//}

	// Adopt a two pointer approach
	i, j := 0, 0
	mergedEl := []eventLog{}
	// We can use an empty log here because it will never be equal to in the checkEquality calls below
	lastEvent := eventLog{}
	for i < len(*wal1) || j < len(*wal2) {
		if len(mergedEl) > 0 {
			lastEvent = mergedEl[len(mergedEl)-1]
		}
		if i == len(*wal1) {
			// Ignore duplicates (compare with current head of the array
			if len(mergedEl) == 0 || checkEquality((*wal2)[j], lastEvent) != eventsEqual {
				mergedEl = append(mergedEl, (*wal2)[j])
			}
			j++
		} else if j == len(*wal2) {
			// Ignore duplicates (compare with current head of the array
			if len(mergedEl) == 0 || checkEquality((*wal1)[i], lastEvent) != eventsEqual {
				mergedEl = append(mergedEl, (*wal1)[i])
			}
			i++
		} else {
			switch checkEquality((*wal1)[i], (*wal2)[j]) {
			case leftEventOlder:
				if len(mergedEl) == 0 || checkEquality((*wal1)[i], lastEvent) != eventsEqual {
					mergedEl = append(mergedEl, (*wal1)[i])
				}
				i++
			case rightEventOlder:
				if len(mergedEl) == 0 || checkEquality((*wal2)[j], lastEvent) != eventsEqual {
					mergedEl = append(mergedEl, (*wal2)[j])
				}
				j++
			case eventsEqual:
				// At this point, we only want to guarantee an increment on ONE of the two pointers
				if i < len(*wal1) {
					i++
				} else {
					j++
				}
			}
		}
	}
	return mergedEl
}

// TODO decouple this from the Wal as it should almost be a pure function
func (w *Wal) sync(fullSync bool) (*[]eventLog, *[]eventLog, error) {

	//
	// LOAD AND MERGE ALL WALs
	//

	mutFile, err := lockedfile.Create(w.syncFilePath)
	if err != nil {
		log.Fatalf("Error creating wal sync lock: %s\n", err)
	}
	defer mutFile.Close()
	//mutFile := lockedfile.MutexAt(w.syncFilePath)
	//unlockFunc, err := mutFile.Lock()
	//defer unlockFunc()
	//if err != nil {
	//    log.Fatalf("Error creating wal sync lock: %s\n", err)
	//}

	localWalFilePath := fmt.Sprintf(w.walPathPattern, w.uuid)

	var localWalFile *os.File

	// Avoid mutating underyling logs before the process is complete.
	// Therefore, dereference into copies, who's pointers will be reassigned to w later on
	localWal := *w.log
	fullLog := *w.fullLog

	if fullSync {
		// On initial load, sync will be called on no WAL files, we we need to create here with the O_CREATE flag
		localWalFile, err = os.OpenFile(localWalFilePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, nil, err
		}
		defer localWalFile.Close()

		// If w.fullLog is not empty, then it'll be consistent with the state of the file, so skip this read.
		// The read will only be required on Load, not on Save (when we close the app)
		// TODO decouple `sync` into `Load` and `Finish` functions
		if len(fullLog) == 0 {
			fullLog, err = w.buildFromFile(localWalFile)
			if err != nil {
				return nil, nil, err
			}
		}

		localWal = w.merge(&fullLog, &localWal)
	}

	fileNames, err := filepath.Glob(fmt.Sprintf(w.walPathPattern, "*"))
	if err != nil {
		return nil, nil, err
	}

	// TODO refactor
	// On partial syncs, to avoid file reads completely, iterate over the names, and if
	// they're all already processed, return
	if !fullSync && len(fileNames) > 1 && len(localWal) == 0 {
		returnEarly := true
		i := 0
		for i < len(fileNames) && returnEarly {
			fileName := fileNames[i]
			if _, exists := w.processedPartialWals[fileName]; !exists && fileName != localWalFilePath {
				returnEarly = false
			}
			i++
		}
		if returnEarly {
			return w.log, w.fullLog, nil
		}
	}

	mergedWal := []eventLog{}

	// Load all WAL files into arrays
	for _, fileName := range fileNames {
		if fileName != localWalFilePath {
			f, err := os.Open(fileName)
			if err != nil {
				return nil, nil, err
			}
			wal, err := w.buildFromFile(f)
			if err != nil {
				return nil, nil, err
			}
			// Add to the processed cache
			w.processedPartialWals[fileName] = struct{}{}
			// Merge all WALs
			mergedWal = w.merge(&mergedWal, &wal)
			f.Close()
			if fullSync {
				os.Remove(fileName)
			}
		}
	}

	flushPartial := true
	if !fullSync {
		// Before we merge in the localWal, we should check to see if the merged logs are
		// the same length the as recent local logs. If so, there's no point flushing any
		// new random files as they'll add additional files to merge with no difference to
		// the outcome
		if len(localWal) == len(mergedWal) {
			flushPartial = false
		}
	}

	mergedWal = w.merge(&mergedWal, &localWal)

	//
	// SAVE AND FLUSH TO A SINGLE WAL
	//

	if fullSync {
		// In the case of full syncs, we want to override the "base" WAL (aka the one representing the
		// full log of events, and named after the static UUID for the app/db
		localWalFile.Truncate(0)
		localWalFile.Seek(0, io.SeekStart)
		err = flush(localWalFile, &mergedWal)
		if err != nil {
			return nil, nil, err
		}
		// We also want to append the recent w.log to w.fullLog, and refresh (empty) the log
		return &[]eventLog{}, &mergedWal, nil
	} else {
		// Otherwise, we leave the base WAL untouched, generate a temp UUID and flush the partial WAL
		// to disk
		//go func() {
		if flushPartial {
			randomWal := fmt.Sprintf(w.walPathPattern, generateUUID())
			partialWalFile, err := os.Create(randomWal)
			if err != nil {
				return nil, nil, err
			}
			defer partialWalFile.Close()
			// Flush the log rather than merged events to avoid duplication between partial WALs
			err = flush(partialWalFile, w.log)
		}
		return &mergedWal, &fullLog, nil
		//}()
	}
}
