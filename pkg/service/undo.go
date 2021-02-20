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
