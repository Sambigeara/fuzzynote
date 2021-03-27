package service

import (
//"fmt"
//"runtime"
//"os"
)

type eventType uint16

// OppositeEvent returns the `undoing` event for a given type, e.g. delete an added item
var oppositeEvent = map[eventType]eventType{
	addEvent:      deleteEvent,
	deleteEvent:   addEvent,
	updateEvent:   updateEvent,
	moveUpEvent:   moveDownEvent,
	moveDownEvent: moveUpEvent,
	showEvent:     hideEvent,
	hideEvent:     showEvent,
}

type undoEventLog struct {
	uuid                       uuid
	targetUUID                 uuid
	eventType                  eventType
	listItemCreationTime       int64
	targetListItemCreationTime int64
	undoLine                   *string
	undoNote                   *[]byte
	redoLine                   *string
	redoNote                   *[]byte
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
		undoLine:  nil,
		undoNote:  nil,
		redoLine:  nil,
		redoNote:  nil,
	}
	return &DbEventLogger{0, []undoEventLog{el}}
}

func (r *DBListRepo) addUndoLog(e eventType, creationTime int64, targetCreationTime int64, originUUID uuid, targetUUID uuid, oldLine *string, oldNote *[]byte, newLine *string, newNote *[]byte) error {
	ev := undoEventLog{
		uuid:                       originUUID,
		targetUUID:                 targetUUID,
		eventType:                  e,
		listItemCreationTime:       creationTime,
		targetListItemCreationTime: targetCreationTime,
		undoLine:                   oldLine,
		undoNote:                   oldNote,
		redoLine:                   newLine,
		redoNote:                   newNote,
	}
	// Truncate the event log, so when we Undo and then do something new, the previous Redo events
	// are overwritten
	r.eventLogger.log = r.eventLogger.log[:r.eventLogger.curIdx+1]

	// Append to log
	r.eventLogger.log = append(r.eventLogger.log, ev)

	r.eventLogger.curIdx++

	return nil
}
