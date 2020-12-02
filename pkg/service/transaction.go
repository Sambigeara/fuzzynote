package service

// TransactionLogger represents an interface to log and changes to the state in the application
// This enables use-cases such as undo/redo and a write-ahead log
type TransactionLogger interface {
	addLog(e eventType, item *ListItem, newLine string, newNote *[]byte) error
}

type eventType string

const (
	nullEvent     eventType = "null"
	addEvent      eventType = "add"
	deleteEvent   eventType = "delete"
	updateEvent   eventType = "update"
	moveUpEvent   eventType = "moveUp"
	moveDownEvent eventType = "moveDown"
)

// OppositeEvent returns the `undoing` event for a given type, e.g. delete an added item
var oppositeEvent = map[eventType]eventType{
	addEvent:      deleteEvent,
	deleteEvent:   addEvent,
	updateEvent:   updateEvent,
	moveUpEvent:   moveDownEvent,
	moveDownEvent: moveUpEvent,
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

func (l *DbEventLogger) addLog(e eventType, item *ListItem, newLine string, newNote *[]byte) error {
	ev := eventLog{
		eventType: e,
		ptr:       item,
		undoLine:  item.Line,
		undoNote:  item.Note,
		redoLine:  newLine,
		redoNote:  newNote,
	}
	// PREPEND newest items to the log
	//l.log = append([]eventLog{ev}, l.log...)
	// Append to log
	l.log = append(l.log, ev)
	//fmt.Println(l.log)
	return nil
}
