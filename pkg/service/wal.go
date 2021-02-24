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
	"unsafe"

	"github.com/alexflint/go-filemutex"
	//"errors"
	//"runtime"
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
	next             *eventLog
}

func (w *Wal) callFunctionForEventLog(root *ListItem, e eventLog) (*ListItem, *ListItem, error) {
	item := w.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.listItemID)]
	targetItem := w.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.targetListItemID)]

	// When we're calling this function on initial WAL merge and load, we may come across
	// orphaned items. There MIGHT be a valid case to keep events around if the eventType
	// is Update. Item will obviously never exist for Add. For all other eventTypes,
	// we should just skip the event and return
	if item == nil && e.eventType != addEvent && e.eventType != updateEvent {
		return nil, nil, nil
	}

	var err error
	switch e.eventType {
	case addEvent:
		root, item, err = w.add(root, e.line, e.note, targetItem, e.uuid)
		item.id = e.listItemID
		w.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, item.id)] = item
	case deleteEvent:
		root, err = w.del(root, item)
		delete(w.listItemTracker, fmt.Sprintf("%d:%d", e.uuid, e.listItemID))
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
			item, err = w.update(e.line, e.note, item)
		} else {
			addEl := e
			addEl.eventType = addEvent
			root, item, err = w.callFunctionForEventLog(root, addEl)
		}
	case moveUpEvent:
		if targetItem == nil {
			return nil, nil, nil
		}
		newChild := targetItem.child
		newParent := targetItem
		root, err = w.moveItem(root, item, newChild, newParent)
	case moveDownEvent:
		if targetItem == nil {
			return nil, nil, nil
		}
		newChild := targetItem
		newParent := targetItem.parent
		root, err = w.moveItem(root, item, newChild, newParent)
	case showEvent:
		err = w.setVisibility(item, true)
	case hideEvent:
		err = w.setVisibility(item, false)
	}
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

func (w *Wal) replay(root *ListItem, primaryRoot *ListItem) (*ListItem, uint64, error) {
	// At the moment, we're bypassing the primary.db entirely, so we track the maxID from the WAL
	// and then set the global NextID afterwards, to avoid wastage.
	nextID := uint64(1)

	// TODO remove this temp measure
	// To deal with legacy pre-WAL versions, if there are WAL files present, build an initial one based
	// on the state of the `primary.db` and return that. It will involve a number of Add and setVisibility
	// events
	if len(*w.log) == 0 {
		var err error
		w.log, err = buildWalFromPrimary(w.uuid, primaryRoot)
		if err != nil {
			return root, nextID, err
		}
	}

	// If still no events, return nil
	if len(*w.log) == 0 {
		return root, nextID, nil
	}

	for _, e := range *w.log {
		var item *ListItem
		root, item, _ = w.callFunctionForEventLog(root, e)

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

func getNextEventLogFromWalFile(f *os.File) (*eventLog, error) {
	item := walItemSchema1{}
	el := eventLog{}

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

	return &el, nil
}

func (w *Wal) flush(f *os.File, walLog *[]eventLog) error {
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

func (w *Wal) buildFromFile(f *os.File) (*eventLog, error) {
	// The first two bytes of each file represents the file schema ID. For now this means nothing
	// so we can seek forwards 2 bytes
	f.Seek(int64(unsafe.Sizeof(latestWalSchemaID)), io.SeekStart)

	// We will store and represent the root eventLog for each remote
	root, err := getNextEventLogFromWalFile(f)
	if err != nil {
		switch err {
		case io.EOF:
			return root, nil
		case io.ErrUnexpectedEOF:
		default:
			fmt.Println("binary.Read failed on remote WAL sync:", err)
			return nil, err
		}
	}
	cur := root
	for cur != nil {
		e, err := getNextEventLogFromWalFile(f)
		if err != nil {
			switch err {
			case io.EOF:
				break
			case io.ErrUnexpectedEOF:
			default:
				fmt.Println("binary.Read failed on remote WAL sync:", err)
				return root, err
			}
		}
		cur.next = e
		cur = cur.next
	}
	return root, nil
}

func (w *Wal) mergeAll(wal1 *eventLog, wal2 *eventLog) (*eventLog, error) {
	if wal1 == nil {
		return wal2, nil
	} else if wal2 == nil {
		return wal1, nil
	}

	var err error
	// TODO I think BODMAS covers us here so remove the parantheses (check first)
	if wal1.unixNanoTime < wal2.unixNanoTime ||
		(wal1.unixNanoTime == wal2.unixNanoTime && wal1.uuid < wal2.uuid) ||
		(wal1.unixNanoTime == wal2.unixNanoTime && wal1.uuid == wal2.uuid && wal1.eventType < wal2.eventType) {
		wal1.next, err = w.merge(wal1.next, wal2)
		return wal1, err
	}
	wal2.next, err = w.merge(wal2.next, wal1)
	return wal2, err
}

func (w *Wal) dedup(root *eventLog) (*eventLog, error) {
	if root == nil {
		return nil, nil
	}
	cur := root
	// For each log, inspect the next. If the next is equal to current, set next.next to next (to ignore the equal)
	for cur != nil {
		if cur.next != nil {
			if cur.unixNanoTime == cur.next.unixNanoTime &&
				cur.uuid == cur.next.uuid &&
				cur.eventType == cur.eventType {
				newEl := cur.next.next
				cur.next = nil
				cur.next = newEl
			}
		}
		cur = cur.next
	}
	return root, nil
}

func (w *Wal) merge(wal1 *eventLog, wal2 *eventLog) (*eventLog, error) {
	root, err := w.mergeAll(wal1, wal2)
	if err != nil {
		return nil, err
	}
	return w.dedup(root)
}

func (w *Wal) sync() (*[]eventLog, error) {
	//
	// LOAD AND MERGE ALL WALs
	//

	w.syncMutex.Lock()
	defer w.syncMutex.Unlock()

	mut, err := filemutex.New(w.syncFilePath)
	if err != nil {
		log.Fatalf("Error creating wal sync lock: %s\n", err)
	}

	mut.Lock()
	defer mut.Unlock()

	localWalFilePath := fmt.Sprintf(w.walPathPattern, w.uuid)
	// On initial load, sync will be called on no WAL files, we we need to create here with the O_CREATE flag
	localWalFile, err := os.OpenFile(localWalFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return &[]eventLog{}, err
	}
	defer localWalFile.Close()

	// Traverse through the current wal log to build a linked-list
	var existingLocalRoot *eventLog
	if len(*w.log) > 0 {
		existingLocalRoot = &((*w.log)[0])
		cur := existingLocalRoot
		for i := 1; i < len(*w.log); i++ {
			cur.next = &(*w.log)[i]
			cur = cur.next
		}
	}

	// We will store and represent the root eventLog for each remote
	localWalRoot, err := w.buildFromFile(localWalFile)
	if err != nil {
		return &[]eventLog{}, err
	}

	// Merge the in-memory log state with the file state (for the local WAL)
	localWalRoot, err = w.merge(localWalRoot, existingLocalRoot)
	if err != nil {
		return &[]eventLog{}, err
	}

	walRoots := []*eventLog{}
	if localWalRoot != nil {
		walRoots = append(walRoots, localWalRoot)
	}

	fileNames, err := filepath.Glob(fmt.Sprintf(w.walPathPattern, "*"))
	if err != nil {
		return &[]eventLog{}, err
	}

	// Unpack all WAL files into linked-list WALs
	for _, fileName := range fileNames {
		if fileName != localWalFilePath {
			f, err := os.Open(fileName)
			if err != nil {
				return &[]eventLog{}, err
			}
			wal, err := w.buildFromFile(f)
			if err != nil {
				return &[]eventLog{}, err
			}
			walRoots = append(walRoots, wal)
			f.Close()
			os.Remove(fileName)
		}
	}

	// Merge all WALs in place into localWal
	for i := 1; i < len(walRoots); i++ {
		localWalRoot, err = w.merge(localWalRoot, walRoots[i])
		if err != nil {
			return &[]eventLog{}, err
		}
	}

	// TODO eventually we should operate on and merge lists, rather than reading into linked
	// lists and coverting to arrays. This will give us some speedup. But for now, this works

	// Generate a list of eventLogs by traversing through the linked lists
	//runtime.Breakpoint()
	mergedRemoteLogs := []eventLog{}
	cur := localWalRoot
	for cur != nil {
		mergedRemoteLogs = append(mergedRemoteLogs, *cur)
		cur = cur.next
	}
	//runtime.Breakpoint()

	//
	// SAVE AND FLUSH TO A SINGLE WAL
	//

	localWalFile.Truncate(0)
	localWalFile.Seek(0, io.SeekStart)

	err = w.flush(localWalFile, &mergedRemoteLogs)
	return &mergedRemoteLogs, err
}
