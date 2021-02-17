package service

import (
	"fmt"
	//"runtime"
	//"os"
	"time"
)

// TransactionLogger represents an interface to log and changes to the state in the application
// This enables use-cases such as undo/redo and a write-ahead log
type TransactionLogger interface {
	addLog(e eventType, item *ListItem, newLine string, newNote *[]byte) error
}

type eventType uint16

const (
	nullEvent eventType = iota
	addEvent
	deleteEvent
	updateEvent
	moveUpEvent
	moveDownEvent
	visibilityEvent
)

// OppositeEvent returns the `undoing` event for a given type, e.g. delete an added item
var oppositeEvent = map[eventType]eventType{
	addEvent:        deleteEvent,
	deleteEvent:     addEvent,
	updateEvent:     updateEvent,
	moveUpEvent:     moveDownEvent,
	moveDownEvent:   moveUpEvent,
	visibilityEvent: visibilityEvent,
}

// logFuncs represent the undo and redo functions related to a particular transaction event
type eventLog struct {
	uuid            uuid   // only relevant for the WalEventLogger, but store in generic eventLog in-mem for consistency
	listItemID      uint64 // auto-incrementing ID, unique for a given DB UUID
	childListItemID uint64
	logID           uint64 // auto-incrementing ID, unique for a given WAL
	unixTime        int64
	eventType       eventType
	ptr             *ListItem
	undoLine        string
	undoNote        *[]byte
	redoLine        string
	redoNote        *[]byte
}

// DbEventLogger implements the TransactionLogger interface for the in-mem undo/redo mechanism
type DbEventLogger struct {
	curIdx int // Last index is latest/most recent in history (appends on new events)
	log    []eventLog
}

// WalEventLogger implements the TransactionLogger interface for the write-ahead log
type WalEventLogger struct {
	uuid uuid
	log  *[]eventLog
}

// NewDbEventLogger Returns a new instance of DbEventLogger
func NewDbEventLogger() *DbEventLogger {
	el := eventLog{
		eventType: nullEvent,
		ptr:       nil,
		undoLine:  "",
		undoNote:  nil,
		redoLine:  "",
		redoNote:  nil,
	}
	return &DbEventLogger{0, []eventLog{el}}
}

// NewWalEventLogger Returns a new instance of WalEventLogger
func NewWalEventLogger() *WalEventLogger {
	return &WalEventLogger{log: &[]eventLog{}}
}

func (l *DbEventLogger) addLog(e eventType, item *ListItem, newLine string, newNote *[]byte) error {
	ev := eventLog{
		eventType: e,
		ptr:       item,
		undoLine:  item.Line,
		undoNote:  item.Note,
		redoLine:  newLine,
		redoNote:  newNote,
	}
	// Truncate the event log, so when we Undo and then do something new, the previous Redo events
	// are overwritten
	l.log = l.log[:l.curIdx+1]

	// Append to log
	l.log = append(l.log, ev)

	l.curIdx++

	return nil
}

func (l *WalEventLogger) addLog(e eventType, item *ListItem, newLine string, newNote *[]byte) error {
	// Base next id off of previous. It's a bit brute-force, but will enforce uniqueness in a single WAL
	// which is our end goal here
	nextID := uint64(1)
	if len(*l.log) > 0 {
		nextID = (*l.log)[len(*l.log)-1].logID + 1
	}
	ev := eventLog{
		logID:      nextID,
		uuid:       l.uuid,
		unixTime:   time.Now().Unix(),
		eventType:  e,
		listItemID: item.id,
		redoLine:   newLine,
		redoNote:   newNote,
	}
	if item.child != nil {
		ev.childListItemID = item.child.id
	}

	// Append to log
	*l.log = append(*l.log, ev)

	return nil
}

func (w *Wal) replayWalEvents(r *DBListRepo, primaryRoot *ListItem) error {
	// TODO remove this temp measure
	// To deal with legacy pre-WAL versions, if there are WAL files present, build an initial one based
	// on the state of the `primary.db` and return that. It will involve a number of Add and toggleVisibility
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

	// TODO sort this out
	// At the moment, we're bypassing the primary.db entirely, so we track the maxID from the WAL
	// and then set the global NextID afterwards, to avoid wastage.
	nextID := uint64(1)

	//listItemTracker := make(map[string]*ListItem)
	//runtime.Breakpoint()
	for _, e := range *w.log {
		//item := listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.listItemID)]
		//child := listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.childListItemID)]

		//r.callFunctionForEventLog(e)
		item, _ := r.callFunctionForEventLog(e)

		// This only applies when we Add a new event and want the ID to be consistent with the incoming
		// events - required for retrieving the correct listItem from the listItemTracker map
		//item.id = e.listItemID

		// Update tracker for any newly created listItems
		//listItemTracker[fmt.Sprintf("%d:%d", e.uuid, item.id)] = item

		if nextID <= item.id {
			nextID = item.id + 1
		}
	}

	r.NextID = nextID

	return nil
}

func callFunctionForEventLog(r *DBListRepo, ev eventType, item *ListItem, child *ListItem, line string, note *[]byte) (*ListItem, error) {
	var err error
	switch ev {
	case addEvent:
		item, err = add(r, line, note, child, item)
	case deleteEvent:
		err = del(r, item)
	case updateEvent:
		_, err = update(r, line, note, item)
	case moveUpEvent:
		_, err = moveUp(r, item)
	case moveDownEvent:
		_, err = moveDown(r, item)
	case visibilityEvent:
		err = toggleVisibility(r, item)
	}
	// Return item only relevant for Add()
	return item, err
}

func getListItemKey(uuid uuid, listItemID uint64) listItemKey {
	return listItemKey(fmt.Sprintf(listItemKeyPattern, uuid, listItemID))
}

func add(r *DBListRepo, line string, note *[]byte, childItem *ListItem, newItem *ListItem) (*ListItem, error) {
	if note == nil {
		note = &[]byte{}
	}
	if newItem == nil {
		newItem = &ListItem{
			Line:  line,
			id:    r.NextID,
			child: childItem,
			Note:  note,
		}
		r.NextID++
	}

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

	r.listItemMap[getListItemKey(r.wal.uuid, newItem.id)] = newItem

	return newItem, nil
}

// Update will update the line or note of an existing ListItem
func update(r *DBListRepo, line string, note *[]byte, listItem *ListItem) (*ListItem, error) {
	line = r.parseOperatorGroups(line)
	listItem.Line = line
	listItem.Note = note
	return listItem, nil
}

func del(r *DBListRepo, item *ListItem) error {
	if item.child != nil {
		item.child.parent = item.parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		r.Root = item.parent
	}

	if item.parent != nil {
		item.parent.child = item.child
	}

	r.PendingDeletions = append(r.PendingDeletions, item)

	delete(r.listItemMap, getListItemKey(r.walLogger.uuid, item.id))

	return nil
}

func moveItem(r *DBListRepo, item *ListItem, newChild *ListItem, newParent *ListItem) error {
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
	for r.Root.child != nil {
		r.Root = r.Root.child
	}
	return nil
}

func moveUp(r *DBListRepo, item *ListItem) (bool, error) {
	var targetItem *ListItem
	if item.matchChild != nil {
		targetItem = item.matchChild
	} else {
		// matchChild will only be null in this context on initial startup with loading
		// from the WAL
		targetItem = item.child
	}
	if targetItem == nil {
		return false, nil
	}

	newChild := targetItem.child
	newParent := targetItem
	err := moveItem(r, item, newChild, newParent)
	return true, err
}

func moveDown(r *DBListRepo, item *ListItem) (bool, error) {
	var targetItem *ListItem
	if item.matchParent != nil {
		targetItem = item.matchParent
	} else {
		// matchParent will only be null in this context on initial startup with loading
		// from the WAL
		targetItem = item.parent
	}
	if targetItem == nil {
		return false, nil
	}

	newChild := targetItem
	newParent := targetItem.parent
	err := moveItem(r, item, newChild, newParent)
	return true, err
}

func toggleVisibility(r *DBListRepo, item *ListItem) error {
	item.IsHidden = !item.IsHidden
	return nil
}
