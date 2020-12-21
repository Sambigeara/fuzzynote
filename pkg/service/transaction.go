package service

// TransactionLogger represents an interface to log and changes to the state in the application
// This enables use-cases such as undo/redo and a write-ahead log
type TransactionLogger interface {
	addLog(e eventType, item *ListItem, newLine string, newNote *[]byte) error
}

type eventType string

const (
	nullEvent       eventType = "null"
	addEvent        eventType = "add"
	deleteEvent     eventType = "delete"
	updateEvent     eventType = "update"
	moveUpEvent     eventType = "moveUp"
	moveDownEvent   eventType = "moveDown"
	visibilityEvent eventType = "toggleVisibility"
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
	eventType eventType
	ptr       *ListItem
	undoLine  string
	undoNote  *[]byte
	redoLine  string
	redoNote  *[]byte
}

// DbEventLogger implements the TransactionLogger interface
type DbEventLogger struct {
	curIdx int // Last index is latest/most recent in history (appends on new events)
	log    []eventLog
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

func (r *DBListRepo) callFunctionForEventLog(ev eventType, ptr *ListItem, line string, note *[]byte) error {
	var err error
	switch ev {
	case addEvent:
		_, err = r.add(ptr.Line, ptr.Note, ptr.child, ptr)
	case deleteEvent:
		err = r.del(ptr)
	case updateEvent:
		err = r.update(line, note, ptr)
	case moveUpEvent:
		_, err = r.moveUp(ptr)
	case moveDownEvent:
		_, err = r.moveDown(ptr)
	case visibilityEvent:
		err = r.toggleVisibility(ptr)
	}
	return err
}
