package service

import (
	"context"
	"fmt"
	"os"
	"path"
)

var eventNameMap = map[EventType]string{
	NullEvent:     "NullEvent",
	AddEvent:      "AddEvent",
	UpdateEvent:   "UpdateEvent",
	MoveUpEvent:   "MoveUpEvent",
	MoveDownEvent: "MoveDownEvent",
	ShowEvent:     "ShowEvent",
	HideEvent:     "HideEvent",
	DeleteEvent:   "DeleteEvent",
}

// DebugWriteEventsToFile is used for debug purposes. It prints all events for the given uuid/lamportTimestamp
// combination to file, for inspection
func (r *DBListRepo) DebugWriteEventsToFile(filename string, key string) {
	el, _, _ := r.pull(context.Background(), []WalFile{r.LocalWalFile})

	f, err := os.OpenFile(path.Join(filename, "results_"+key), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for _, e := range el {
		if e.ListItemKey == key {
			f.WriteString(fmt.Sprintf("[%s] [%s] [%s] [%v]\n", eventNameMap[e.EventType], e.key(), e.Line, e.Friends.Emails))
		}
	}
}
