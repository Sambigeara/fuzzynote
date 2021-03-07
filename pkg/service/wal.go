package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"time"
	//"runtime"

	"github.com/rogpeppe/go-internal/lockedfile"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 2

// Wal manages the state of the WAL, via all update functions and replay functionality
type Wal struct {
	uuid                 uuid
	log                  *[]eventLog // log represents a fresh set of events (unique from the historical log below)
	fullLog              *[]eventLog // fullLog is a historical log of events
	listItemTracker      map[string]*ListItem
	processedPartialWals map[string]struct{}
	walPathPattern       string
	latestWalSchemaID    uint16
	localWalFile         *localWalFile
	remoteWalFiles       []walFile
}

func generateUUID() uuid {
	return uuid(rand.Uint32())
}

func NewWal(rootDir string) *Wal {
	return &Wal{
		uuid:                 generateUUID(),
		walPathPattern:       path.Join(rootDir, walFilePattern),
		latestWalSchemaID:    latestWalSchemaID,
		log:                  &[]eventLog{},
		fullLog:              &[]eventLog{},
		listItemTracker:      make(map[string]*ListItem),
		processedPartialWals: make(map[string]struct{}),
		localWalFile:         newLocalWalFile(rootDir),
		remoteWalFiles:       []walFile{},
	}
}

// TODO these don't need to be public attributes
type walItemSchema1 struct {
	UUID             uuid
	TargetUUID       uuid
	ListItemID       uint64
	TargetListItemID uint64
	UnixTime         int64
	EventType        eventType
	LineLength       uint64
	NoteLength       uint64
}

// Ordering of these enums are VERY IMPORTANT as they're used for comparisons when resolving WAL merge conflicts
// (although there has to be nanosecond level collisions in order for this to be relevant)
const (
	nullEvent eventType = iota
	addEvent
	updateEvent
	moveUpEvent
	moveDownEvent
	showEvent
	hideEvent
	deleteEvent
)

type eventLog struct {
	uuid             uuid
	listItemID       uint64 // auto-incrementing ID, unique for a given DB UUID
	targetUUID       uuid
	targetListItemID uint64
	unixNanoTime     int64
	eventType        eventType
	line             string
	note             *[]byte
}

// WalFile offers a generic interface into local or remote filesystems
type walFile interface {
	lock() error
	unlock() error
	getFileNames(string) ([]string, error)
	generateLogFromFile(string) ([]eventLog, error)
	removeFile(string) error
	flush(string, *[]eventLog) error
}

type localWalFile struct {
	syncFilePath string
	fileMutex    *lockedfile.File
}

func newLocalWalFile(rootDir string) *localWalFile {
	return &localWalFile{
		syncFilePath: path.Join(rootDir, syncFile),
	}
}

func (wf *localWalFile) lock() error {
	var err error
	wf.fileMutex, err = lockedfile.Create(wf.syncFilePath)
	if err != nil {
		return fmt.Errorf("error creating wal sync lock: %s", err)
	}
	return nil
}

func (wf *localWalFile) unlock() error {
	wf.fileMutex.Close()
	return nil
}

func (wf *localWalFile) getFileNames(walPathPattern string) ([]string, error) {
	fileNames, err := filepath.Glob(fmt.Sprintf(walPathPattern, "*"))
	if err != nil {
		return []string{}, err
	}
	return fileNames, nil
}

func (wf *localWalFile) generateLogFromFile(fileName string) ([]eventLog, error) {
	wal := []eventLog{}
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		return wal, err
	}
	wal, err = buildFromFile(f)
	if err != nil {
		return wal, err
	}
	return wal, nil
}

func (wf *localWalFile) removeFile(fileName string) error {
	return os.Remove(fileName)
}

func (wf *localWalFile) flush(fileName string, walLog *[]eventLog) error {
	// TODO make generic function with walFile specific functions retrieving specific io.Writer
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to beginning of file just in case
	f.Truncate(0)
	f.Seek(0, io.SeekStart)

	// Write the schema ID
	err = binary.Write(f, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		return err
	}

	for _, item := range *walLog {
		lenLine := uint64(len([]byte(item.line)))
		var lenNote uint64
		if item.note != nil {
			lenNote = uint64(len(*(item.note)))
		}
		i := walItemSchema1{
			UUID:             item.uuid,
			TargetUUID:       item.targetUUID,
			ListItemID:       item.listItemID,
			TargetListItemID: item.targetListItemID,
			UnixTime:         item.unixNanoTime,
			EventType:        item.eventType,
			LineLength:       lenLine,
			NoteLength:       lenNote,
		}
		data := []interface{}{
			i,
			[]byte(item.line),
		}
		if item.note != nil {
			data = append(data, item.note)
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

func (r *DBListRepo) CallFunctionForEventLog(root *ListItem, e eventLog) (*ListItem, *ListItem, error) {
	item := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.listItemID)]
	targetItem := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.targetUUID, e.targetListItemID)]

	// When we're calling this function on initial WAL merge and load, we may come across
	// orphaned items. There MIGHT be a valid case to keep events around if the eventType
	// is Update. Item will obviously never exist for Add. For all other eventTypes,
	// we should just skip the event and return
	if item == nil && e.eventType != addEvent && e.eventType != updateEvent {
		return root, nil, nil
	}

	var err error
	switch e.eventType {
	case addEvent:
		root, item, err = r.wal.add(root, e.line, e.note, targetItem, e.uuid)
		item.id = e.listItemID
		r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, item.id)] = item
		if item.id >= r.NextID {
			r.NextID = item.id + 1
		}
	case updateEvent:
		// We have to cover an edge case here which occurs when merging two remote WALs. If the following occurs:
		// - wal1 creates item A
		// - wal2 copies wal1
		// - wal2 deletes item A
		// - wal1 updates item A
		// - wal1 copies wal2
		// We will end up with an attempted Update on a nonexistent item.
		// In this case, we will Add an item back in with the updated content
		// NOTE A side effect of this will be that the re-added item will be at the top of the list as it
		// becomes tricky to deal with child IDs
		if item != nil {
			item, err = r.wal.update(e.line, e.note, item)
		} else {
			addEl := e
			addEl.eventType = addEvent
			root, item, err = r.CallFunctionForEventLog(root, addEl)
		}
	case deleteEvent:
		root, err = r.wal.del(root, item)
		delete(r.wal.listItemTracker, fmt.Sprintf("%d:%d", e.uuid, e.listItemID))
	case moveUpEvent:
		// If targetItem is nil, we avoid the callback and thus avoid the cursor move event
		if targetItem == nil {
			return root, nil, nil
		}
		newChild := targetItem.child
		newParent := targetItem
		root, err = r.wal.moveItem(root, item, newChild, newParent)
	case moveDownEvent:
		if targetItem == nil {
			return root, nil, nil
		}
		newChild := targetItem
		newParent := targetItem.parent
		root, err = r.wal.moveItem(root, item, newChild, newParent)
	case showEvent:
		err = r.wal.setVisibility(item, true)
	case hideEvent:
		err = r.wal.setVisibility(item, false)
	}
	return root, item, err
}

func (w *Wal) add(root *ListItem, line string, note *[]byte, childItem *ListItem, uuid uuid) (*ListItem, *ListItem, error) {
	if note == nil {
		note = &[]byte{}
	}
	newItem := &ListItem{
		Line:       line,
		child:      childItem,
		Note:       note,
		originUUID: uuid,
	}

	// If `child` is nil, it's the first item in the list so set as root and return
	if childItem == nil {
		oldRoot := root
		root = newItem
		if oldRoot != nil {
			newItem.parent = oldRoot
			oldRoot.child = newItem
		}
		return root, newItem, nil
	}

	if childItem.parent != nil {
		childItem.parent.child = newItem
		newItem.parent = childItem.parent
	}
	childItem.parent = newItem

	return root, newItem, nil
}

// Update will update the line or note of an existing ListItem
func (w *Wal) update(line string, note *[]byte, listItem *ListItem) (*ListItem, error) {
	listItem.Line = line
	listItem.Note = note
	return listItem, nil
}

func (w *Wal) del(root *ListItem, item *ListItem) (*ListItem, error) {
	if item.child != nil {
		item.child.parent = item.parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		root = item.parent
	}

	if item.parent != nil {
		item.parent.child = item.child
	}

	return root, nil
}

func (w *Wal) moveItem(root *ListItem, item *ListItem, newChild *ListItem, newParent *ListItem) (*ListItem, error) {
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
	// for loop because it might have to traverse multiple times
	for root.child != nil {
		root = root.child
	}
	return root, nil
}

func (w *Wal) setVisibility(item *ListItem, isVisible bool) error {
	item.IsHidden = !isVisible
	return nil
}

func (r *DBListRepo) replay(root *ListItem, log *[]eventLog, fullLog *[]eventLog) (*ListItem, uint64, *[]eventLog, *[]eventLog, error) {
	fullLog = merge(fullLog, log)
	// If still no events, return nil
	if len(*fullLog) == 0 {
		return root, uint64(1), log, fullLog, nil
	}

	//runtime.Breakpoint()
	for _, e := range *fullLog {
		root, _, _ = r.CallFunctionForEventLog(root, e)
	}

	return root, r.NextID, log, fullLog, nil
}

func getNextEventLogFromWalFile(f *os.File, schemaVersionID uint16) (*eventLog, error) {
	el := eventLog{}
	// TODO this is a hacky fix. Instantiate the note just in case
	el.note = &[]byte{}

	item := walItemSchema1{}
	err := binary.Read(f, binary.LittleEndian, &item)
	if err != nil {
		return nil, err
	}
	el.listItemID = item.ListItemID
	el.targetListItemID = item.TargetListItemID
	el.unixNanoTime = item.UnixTime
	el.uuid = item.UUID
	el.targetUUID = item.TargetUUID
	el.eventType = item.EventType

	line := make([]byte, item.LineLength)
	err = binary.Read(f, binary.LittleEndian, &line)
	if err != nil {
		return nil, err
	}
	el.line = string(line)

	if item.NoteLength > 0 {
		note := make([]byte, item.NoteLength)
		err = binary.Read(f, binary.LittleEndian, &note)
		if err != nil {
			return nil, err
		}
		el.note = &note
	}
	return &el, nil
}

func buildFromFile(f *os.File) ([]eventLog, error) {
	// The first two bytes of each file represents the file schema ID. For now this means nothing
	// so we can seek forwards 2 bytes
	//f.Seek(int64(unsafe.Sizeof(latestWalSchemaID)), io.SeekStart)

	el := []eventLog{}
	var walSchemaVersionID uint16
	err := binary.Read(f, binary.LittleEndian, &walSchemaVersionID)
	if err != nil {
		if err == io.EOF {
			return el, nil
		} else {
			log.Fatal(err)
			return el, err
		}
	}

	for {
		e, err := getNextEventLogFromWalFile(f, walSchemaVersionID)
		if err != nil {
			switch err {
			case io.EOF:
				return el, nil
			//case io.ErrUnexpectedEOF:
			default:
				fmt.Println("binary.Read failed on remote WAL sync:", err)
				return el, err
			}
		}
		el = append(el, *e)
	}
}

const (
	eventsEqual int = iota
	leftEventOlder
	rightEventOlder
)

func checkEquality(event1 eventLog, event2 eventLog) int {
	if event1.unixNanoTime < event2.unixNanoTime ||
		event1.unixNanoTime == event2.unixNanoTime && event1.uuid < event2.uuid ||
		event1.unixNanoTime == event2.unixNanoTime && event1.uuid == event2.uuid && event1.eventType < event2.eventType {
		return leftEventOlder
	} else if event2.unixNanoTime < event1.unixNanoTime ||
		event2.unixNanoTime == event1.unixNanoTime && event2.uuid < event1.uuid ||
		event2.unixNanoTime == event1.unixNanoTime && event2.uuid == event1.uuid && event2.eventType < event1.eventType {
		return rightEventOlder
	}
	return eventsEqual
}

func merge(wal1 *[]eventLog, wal2 *[]eventLog) *[]eventLog {
	// Adopt a two pointer approach
	i, j := 0, 0
	mergedEl := []eventLog{}
	// We can use an empty log here because it will never be equal to in the checkEquality calls below
	lastEvent := eventLog{}
	for i < len(*wal1) || j < len(*wal2) {
		if len(mergedEl) > 0 {
			lastEvent = mergedEl[len(mergedEl)-1]
		}
		if i == len(*wal1) {
			// Ignore duplicates (compare with current head of the array
			if len(mergedEl) == 0 || checkEquality((*wal2)[j], lastEvent) != eventsEqual {
				mergedEl = append(mergedEl, (*wal2)[j])
			}
			j++
		} else if j == len(*wal2) {
			// Ignore duplicates (compare with current head of the array
			if len(mergedEl) == 0 || checkEquality((*wal1)[i], lastEvent) != eventsEqual {
				mergedEl = append(mergedEl, (*wal1)[i])
			}
			i++
		} else {
			switch checkEquality((*wal1)[i], (*wal2)[j]) {
			case leftEventOlder:
				if len(mergedEl) == 0 || checkEquality((*wal1)[i], lastEvent) != eventsEqual {
					mergedEl = append(mergedEl, (*wal1)[i])
				}
				i++
			case rightEventOlder:
				if len(mergedEl) == 0 || checkEquality((*wal2)[j], lastEvent) != eventsEqual {
					mergedEl = append(mergedEl, (*wal2)[j])
				}
				j++
			case eventsEqual:
				// At this point, we only want to guarantee an increment on ONE of the two pointers
				if i < len(*wal1) {
					i++
				} else {
					j++
				}
			}
		}
	}
	return &mergedEl
}

func (w *Wal) sync(fullSync bool) (*[]eventLog, *[]eventLog, error) {

	//
	// LOAD AND MERGE ALL WALs
	//

	walFiles := append(w.remoteWalFiles, w.localWalFile)

	for _, wf := range walFiles {
		err := wf.lock()
		if err != nil {
			log.Fatal(err)
		}
		defer wf.unlock()
	}

	localWalFilePath := fmt.Sprintf(w.walPathPattern, w.uuid)

	// Avoid mutating underyling logs before the process is complete.
	// Therefore, dereference into copies, who's pointers will be reassigned to w later on
	localWal := *w.log
	fullLog := *w.fullLog

	if fullSync {
		// On initial load, sync will be called on no WAL files, we we need to create here with the O_CREATE flag
		localWalFile, err := os.OpenFile(localWalFilePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, nil, err
		}
		defer localWalFile.Close()

		// If w.fullLog is not empty, then it'll be consistent with the state of the file, so skip this read.
		// The read will only be required on Load, not on Save (when we close the app)
		if len(fullLog) == 0 {
			fullLog, err = buildFromFile(localWalFile)
			if err != nil {
				return nil, nil, err
			}
		}

		localWal = *(merge(&fullLog, &localWal))
	}

	fileNameMap := make(map[walFile][]string)
	//fileNames := []string{}
	nFileNames := 0
	for _, wf := range walFiles {
		fileNames, err := wf.getFileNames(w.walPathPattern)
		if err != nil {
			log.Fatal(err)
		}
		nFileNames += len(fileNames)
		fileNameMap[wf] = append(fileNameMap[wf], fileNames...)
	}

	// On partial syncs, to avoid file reads completely, iterate over the names, and if
	// they're all already processed, return
	if !fullSync && nFileNames > 1 && len(localWal) == 0 {
		returnEarly := true
		for _, fileNames := range fileNameMap {
			i := 0
			for i < len(fileNames) && returnEarly {
				fileName := fileNames[i]
				if _, exists := w.processedPartialWals[fileName]; !exists && fileName != localWalFilePath {
					returnEarly = false
				}
				i++
			}
			if returnEarly {
				return w.log, w.fullLog, nil
			}
		}
	}

	mergedWal := []eventLog{}

	// Load all WAL files into arrays
	for wf, fileNames := range fileNameMap {
		for _, fileName := range fileNames {
			// Avoid localWal
			if fileName != localWalFilePath {
				// Don't bother merging processed files
				if _, exists := w.processedPartialWals[fileName]; !exists {
					wal, err := wf.generateLogFromFile(fileName)
					if err != nil {
						log.Fatal(err)
					}
					// Add to the processed cache
					w.processedPartialWals[fileName] = struct{}{}
					// Merge all WALs
					mergedWal = *(merge(&mergedWal, &wal))
				}
				if fullSync {
					err := wf.removeFile(fileName)
					if err != nil {
						return nil, nil, err
					}
				}
			}
		}
	}

	flushPartial := true
	if !fullSync {
		// Before we merge in the localWal, we should check to see if the merged logs are
		// the same length the as recent local logs. If so, there's no point flushing any
		// new random files as they'll add additional files to merge with no difference to
		// the outcome
		if len(localWal) == len(mergedWal) {
			flushPartial = false
		}
	}

	mergedWal = *(merge(&mergedWal, &localWal))

	//
	// SAVE AND FLUSH TO A SINGLE WAL
	//

	if fullSync {
		for _, wf := range walFiles {
			// In the case of full syncs, we want to override the "base" WAL (aka the one representing the
			// full log of events, and named after the static UUID for the app/db
			err := wf.flush(localWalFilePath, &mergedWal)
			if err != nil {
				return nil, nil, err
			}
			// Clear the processedPartialWals cache as processed files may have changed on full syncs
			w.processedPartialWals = make(map[string]struct{})
		}
		return &[]eventLog{}, &mergedWal, nil
	} else {
		for _, wf := range walFiles {
			// Otherwise, we leave the base WAL untouched, generate a temp UUID and flush the partial WAL
			// to disk
			if flushPartial {
				randomWal := fmt.Sprintf(w.walPathPattern, generateUUID())
				// Flush the log rather than merged events to avoid duplication between partial WALs
				err := wf.flush(randomWal, w.log)
				if err != nil {
					log.Fatal(err)
				}
				// Add it straight to the cache to avoid processing it in the future
				w.processedPartialWals[randomWal] = struct{}{}
			}
		}
		return &mergedWal, &fullLog, nil
	}
}
