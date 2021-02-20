package service

import (
	"fmt"
	//"runtime"
	//"os"
)

type eventType uint16

// OppositeEvent returns the `undoing` event for a given type, e.g. delete an added item
var oppositeEvent = map[eventType]eventType{
	addEvent:        deleteEvent,
	deleteEvent:     addEvent,
	updateEvent:     updateEvent,
	moveUpEvent:     moveDownEvent,
	moveDownEvent:   moveUpEvent,
	visibilityEvent: visibilityEvent,
}

type undoEventLog struct {
	uuid      uuid
	eventType eventType
	ptr       *ListItem
	undoLine  string
	undoNote  *[]byte
	redoLine  string
	redoNote  *[]byte
}

// DbEventLogger is used for in-mem undo/redo mechanism
type DbEventLogger struct {
	curIdx int // Last index is latest/most recent in history (appends on new events)
	log    []undoEventLog
}

// NewDbEventLogger Returns a new instance of DbEventLogger
func NewDbEventLogger() *DbEventLogger {
	el := undoEventLog{
		eventType: nullEvent,
		ptr:       nil,
		undoLine:  "",
		undoNote:  nil,
		redoLine:  "",
		redoNote:  nil,
	}
	return &DbEventLogger{0, []undoEventLog{el}}
}

func (r *DBListRepo) addUndoLog(e eventType, item *ListItem, newLine string, newNote *[]byte) error {
	ev := undoEventLog{
		uuid:      r.wal.uuid,
		eventType: e,
		ptr:       item,
		undoLine:  item.Line,
		undoNote:  item.Note,
		redoLine:  newLine,
		redoNote:  newNote,
	}
	// Truncate the event log, so when we Undo and then do something new, the previous Redo events
	// are overwritten
	r.eventLogger.log = r.eventLogger.log[:r.eventLogger.curIdx+1]

	// Append to log
	r.eventLogger.log = append(r.eventLogger.log, ev)

	r.eventLogger.curIdx++

	return nil
}

// TODO use this
func getListItemKey(uuid uuid, listItemID uint64) listItemKey {
	return listItemKey(fmt.Sprintf(listItemKeyPattern, uuid, listItemID))
}

func add(r *DBListRepo, line string, note *[]byte, childItem *ListItem) (*ListItem, error) {
	if note == nil {
		note = &[]byte{}
	}
	newItem := &ListItem{
		Line:  line,
		id:    r.NextID,
		child: childItem,
		Note:  note,
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
