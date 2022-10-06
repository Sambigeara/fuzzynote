package service

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

type undoLog struct {
	oppEvents []EventLog // The events required to undo the corresponding original events
	events    []EventLog // The original events that were triggered on the client interface call (Add/Update/etc)
}

// DbEventLogger is used for in-mem undo/redo mechanism
type DbEventLogger struct {
	curIdx int // Last index is latest/most recent in history (appends on new events)
	log    []undoLog
}

// NewDbEventLogger Returns a new instance of DbEventLogger
func NewDbEventLogger() *DbEventLogger {
	return &DbEventLogger{
		log: []undoLog{
			{
				oppEvents: []EventLog{
					{
						EventType: NullEvent,
					},
				},
				events: []EventLog{
					{
						EventType: NullEvent,
					},
				},
			},
		},
	}
}

func (r *DBListRepo) addUndoLogs(oppEvents []EventLog, originalEvents []EventLog) error {
	ul := undoLog{
		oppEvents: oppEvents,
		events:    originalEvents,
	}
	// Truncate the event log, so when we Undo and then do something new, the previous Redo events
	// are overwritten
	r.eventLogger.log = r.eventLogger.log[:r.eventLogger.curIdx+1]

	// Append to log
	r.eventLogger.log = append(r.eventLogger.log, ul)

	r.eventLogger.curIdx++

	return nil
}
