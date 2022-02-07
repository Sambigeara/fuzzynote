package service

import (
//"fmt"
//"runtime"
//"os"
)

// oppositeEvent returns the `undoing` event for a given type, e.g. delete an added item
var oppositeEvent = map[EventType]EventType{
	AddEvent:      DeleteEvent,
	DeleteEvent:   AddEvent,
	UpdateEvent:   UpdateEvent,
	MoveUpEvent:   MoveDownEvent,
	MoveDownEvent: MoveUpEvent,
	ShowEvent:     HideEvent,
	HideEvent:     ShowEvent,
}

type undoEventLog struct {
	uuid                       uuid
	targetUUID                 uuid
	eventType                  EventType
	listItemCreationTime       int64
	targetListItemCreationTime int64
	undoLine                   string
	undoNote                   *[]byte
	redoLine                   string
	redoNote                   *[]byte
}

type undoLog struct {
	oppEvent EventLog // The event required to undo the corresponding original event
	event    EventLog // The original event that was triggered on the client interface call (Add/Update/etc)
}

// DbEventLogger is used for in-mem undo/redo mechanism
type DbEventLogger struct {
	curIdx int // Last index is latest/most recent in history (appends on new events)
	log    []undoLog
}

// NewDbEventLogger Returns a new instance of DbEventLogger
func NewDbEventLogger() *DbEventLogger {
	return &DbEventLogger{
		log: []undoLog{undoLog{
			oppEvent: EventLog{
				EventType: NullEvent,
			},
			event: EventLog{
				EventType: NullEvent,
			},
		}},
	}
}

func (r *DBListRepo) addUndoLog(oppEvent EventLog, originalEvent EventLog) error {
	ul := undoLog{
		oppEvent: oppEvent,
		event:    originalEvent,
	}
	// Truncate the event log, so when we Undo and then do something new, the previous Redo events
	// are overwritten
	r.eventLogger.log = r.eventLogger.log[:r.eventLogger.curIdx+1]

	// Append to log
	r.eventLogger.log = append(r.eventLogger.log, ul)

	r.eventLogger.curIdx++

	return nil
}
