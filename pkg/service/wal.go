package service

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	//"runtime"
	"time"
	//"path"
	"path/filepath"
)

// WalFile is a file representation of the Wal interface
type WalFile struct {
	rootPath          string
	walPathPattern    string
	latestWalSchemaID uint16
	logger            *WalEventLogger
}

const latestWalSchemaID uint16 = 1

// NewWalFile instantiates and returns a new WalFile instance
func NewWalFile(rootPath string, walPathPattern string, logger *WalEventLogger) *WalFile {
	return &WalFile{
		rootPath:          rootPath,
		walPathPattern:    walPathPattern,
		latestWalSchemaID: latestWalSchemaID,
		logger:            logger,
	}
}

type walHeader struct {
	schemaID fileSchemaID
}

type walItemSchema1 struct {
	UUID            uuid
	LogID           uint64
	ListItemID      uint64
	ChildListItemID uint64
	UnixTime        int64
	EventType       eventType
	LineLength      uint64
	NoteLength      uint64
}

func buildWalFromPrimary(uuid uuid, item *ListItem) (*[]eventLog, error) {
	primaryLogs := []eventLog{}
	now := time.Now().Unix()

	nextLogID := uint64(1)
	for item != nil {
		var childListItemID uint64
		if item.child != nil {
			childListItemID = item.child.id
		}
		el := eventLog{
			uuid:            uuid,
			listItemID:      item.id,
			childListItemID: childListItemID,
			logID:           nextLogID,
			unixTime:        now,
			eventType:       addEvent,
			redoLine:        item.Line,
			redoNote:        item.Note,
		}
		primaryLogs = append(primaryLogs, el)
		nextLogID++

		if item.IsHidden {
			el.eventType = visibilityEvent
			el.logID = nextLogID
			primaryLogs = append(primaryLogs, el)
			nextLogID++
		}
		item = item.parent
	}

	return &primaryLogs, nil
}

func getNextEventLogFromWalFile(f *os.File) (eventLog, error) {
	item := walItemSchema1{}
	el := eventLog{}

	err := binary.Read(f, binary.LittleEndian, &item)
	if err != nil {
		return el, err
	}

	// ptr is initially set to nil as any "add" events wont have corresponding ptrs, so we deal with this post-merge
	el.listItemID = item.ListItemID
	el.childListItemID = item.ChildListItemID
	el.logID = item.LogID
	el.unixTime = item.UnixTime
	el.uuid = item.UUID
	el.eventType = item.EventType

	line := make([]byte, item.LineLength)
	err = binary.Read(f, binary.LittleEndian, &line)
	if err != nil {
		return el, err
	}
	el.redoLine = string(line)

	if item.NoteLength > 0 {
		note := make([]byte, item.NoteLength)
		err = binary.Read(f, binary.LittleEndian, &note)
		if err != nil {
			return el, err
		}
		el.redoNote = &note
	}

	return el, nil
}

func (w *Wal) loadWal() error {
	localWalFilePath := fmt.Sprintf(w.walPathPattern, w.uuid)

	// Initially, we want to create a single merged eventLog for all non-local WAL files
	// To do this, we find and open each file, and traverse through each simultaneously with an N-pointer approach
	remoteFiles := make(map[*os.File]struct{}) // Effectively a set implementation (struct{} cheaper than bool}
	fileNames, err := filepath.Glob(fmt.Sprintf(w.walPathPattern, "*"))
	if err != nil {
		panic(err)
	}

	for _, fileName := range fileNames {
		f, err := os.OpenFile(fileName, os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		remoteFiles[f] = struct{}{}
		defer f.Close()
		if fileName != localWalFilePath {
			defer os.Remove(fileName)
		}
	}

	// This maps the open remote WAL file to the current head of that file, useful when comparing each head to
	// determine order of single output
	curEventLogs := make(map[*os.File]eventLog)
	mergedRemoteLogs := []eventLog{}

	// We keep track of open files to determine how many current eventLogs to store in the map
	// Initially, we grab the first item from each open file to populate curEventLogs
	// We then pop the oldest item and append it to the merged logs, and replace with another item from that file.
	// We repeat this until end of file.
	// When we close a file, we reduce remoteFiles so we know to compare on fewer items.
	for f := range remoteFiles {
		e, err := getNextEventLogFromWalFile(f)
		if err != nil {
			switch err {
			case io.EOF:
				continue
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on remote WAL merge:", err)
				return err
			default:
				return err
			}
		}
		curEventLogs[f] = e
	}

	for len(curEventLogs) > 0 {
		// Get the current oldest item, and the file that it resides in
		var curFile *os.File = nil
		oldest := eventLog{}

		for f := range remoteFiles {
			if oldest.eventType == nullEvent {
				// Deal with initial iteration
				oldest = curEventLogs[f]
				curFile = f
			} else {
				e := curEventLogs[f]
				if e.unixTime < oldest.unixTime {
					oldest = e
					curFile = f
				} else if e.unixTime == oldest.unixTime {
					// If unixTime's are the same, order by uuid and then logID to ensure consistent ordering
					if e.uuid < oldest.uuid {
						oldest = e
						curFile = f
					} else if e.uuid == oldest.uuid {
						if e.logID < oldest.logID {
							oldest = e
							curFile = f
						}
					}
				}
			}
		}

		// Because we have guaranteed ordering by unixTime -> uuid -> logID, we can compare the current "oldest"
		// against the head of the mergedRemoteLogs - if they share the three attributes above (strictly only uuid
		// and rowID) we can ignore. Otherwise, we append to the logs
		if oldest.eventType != nullEvent {
			if len(mergedRemoteLogs) > 0 {
				head := mergedRemoteLogs[len(mergedRemoteLogs)-1]
				if head.uuid == oldest.uuid && head.logID == oldest.logID {
					if head.unixTime != oldest.unixTime {
						panic(errors.New("Log item's should have the same unixTime"))
					}
				} else {
					// TODO deal with duplication
					mergedRemoteLogs = append(mergedRemoteLogs, oldest)
				}
			} else {
				mergedRemoteLogs = append(mergedRemoteLogs, oldest)
			}
		}

		// Retrieve the next log from that file. If EOF, remove it from the list of remoteFiles
		nextEvent, err := getNextEventLogFromWalFile(curFile)
		if err != nil {
			if err == io.EOF {
				delete(curEventLogs, curFile)
			} else {
				fmt.Println("binary.Read failed on remote WAL merge:", err)
				return err
			}
		} else {
			if nextEvent.eventType != nullEvent {
				curEventLogs[curFile] = nextEvent
			}
		}
	}

	w.log = &mergedRemoteLogs

	return nil

	// Then we want to merge with the local WAL. There are two scenarios to handle here... TODO
}

func (w *Wal) saveWal() error {
	walFilePath := fmt.Sprintf(w.walPathPattern, w.uuid)

	f, err := os.Create(walFilePath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	for _, item := range *w.log {
		lenLine := uint64(len([]byte(item.redoLine)))
		var lenNote uint64
		if item.redoNote != nil {
			lenNote = uint64(len(*(item.redoNote)))
		}
		i := walItemSchema1{
			UUID:            item.uuid,
			LogID:           item.logID,
			ListItemID:      item.listItemID,
			ChildListItemID: item.childListItemID,
			UnixTime:        item.unixTime,
			EventType:       item.eventType,
			LineLength:      lenLine,
			NoteLength:      lenNote,
		}
		data := []interface{}{
			i,
			[]byte(item.redoLine),
		}
		if item.redoNote != nil {
			data = append(data, item.redoNote)
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
