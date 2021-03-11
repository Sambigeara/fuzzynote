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
	//"regexp"
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
	processedPartialWals map[WalFile]map[string]struct{}
	latestWalSchemaID    uint16
	pendingRemoteLogs    map[WalFile]*[]eventLog
}

func generateUUID() uuid {
	return uuid(rand.Uint32())
}

func NewWal() *Wal {
	return &Wal{
		uuid:                 generateUUID(),
		latestWalSchemaID:    latestWalSchemaID,
		log:                  &[]eventLog{},
		fullLog:              &[]eventLog{},
		listItemTracker:      make(map[string]*ListItem),
		processedPartialWals: make(map[WalFile]map[string]struct{}),
		pendingRemoteLogs:    make(map[WalFile]*[]eventLog),
	}
}

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
type WalFile interface {
	getRootDir() string
	getLocalRootDir() string
	lock() error
	unlock() error
	getFileNamesMatchingPattern(string) ([]string, error)
	generateLogFromFile(string) ([]eventLog, error)
	removeFile(string) error
	flush(*os.File, string) error
}

type localWalFile struct {
	//syncFilePath string
	fileMutex *lockedfile.File
	rootDir   string
	//processedPartialWals map[string]struct{}
}

func NewLocalWalFile(rootDir string) *localWalFile {
	return &localWalFile{
		//syncFilePath: path.Join(rootDir, syncFile),
		rootDir: rootDir,
		//processedPartialWals: make(map[string]struct{}),
	}
}

func (wf *localWalFile) getRootDir() string {
	return wf.rootDir
}

func (wf *localWalFile) getLocalRootDir() string {
	return wf.rootDir
}

func (wf *localWalFile) lock() error {
	var err error
	wf.fileMutex, err = lockedfile.Create(path.Join(wf.rootDir, syncFile))
	if err != nil {
		return fmt.Errorf("error creating wal sync lock: %s", err)
	}
	return nil
}

func (wf *localWalFile) unlock() error {
	wf.fileMutex.Close()
	return nil
}

func (wf *localWalFile) getFileNamesMatchingPattern(matchPattern string) ([]string, error) {
	fileNames, err := filepath.Glob(matchPattern)
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

func (wf *localWalFile) flush(f *os.File, fileName string) error {
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
			case io.ErrUnexpectedEOF:
				// Given the distributed concurrent nature of this app, we sometimes pick up partially
				// uploaded files which will fail, but may well be complete later on, therefore just
				// return for now and attempt again later
				// TODO implement a decent retry mech here
				return []eventLog{}, nil
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

func (w *Wal) sync(wf WalFile, fullSync bool) (*[]eventLog, error) {
	//runtime.Breakpoint()
	//localReg, _ := regexp.Compile(fmt.Sprintf(w.walPathPattern, fmt.Sprintf("%v[0-9]+", w.uuid)))

	//
	// LOAD AND MERGE ALL WALs
	//

	// TODO only local wal needs a lock so remove interface functions and implement here
	err := wf.lock()
	if err != nil {
		log.Fatal(err)
	}
	defer wf.unlock()

	// We need any recent local changes to be flushed to all WalFiles, but we only process one at a time.
	// Therefore, propagate any recent local changes to all other WalFiles
	if !fullSync && len(*w.log) > 0 {
		// It will always exist (due to pre-instantiation) but might be empty
		for otherWalFile, otherLog := range w.pendingRemoteLogs {
			if otherWalFile != wf {
				w.pendingRemoteLogs[otherWalFile] = merge(otherLog, w.log)
			}
		}
	}

	filePathPattern := path.Join(wf.getRootDir(), walFilePattern)

	allFileNames, err := wf.getFileNamesMatchingPattern(fmt.Sprintf(filePathPattern, "*"))
	if err != nil {
		log.Fatal(err)
	}
	newFileNames := []string{}
	// TODO put the processedPartialWals map on the WalFile and check this within getFileNamesMatchingPattern
	for _, fileName := range allFileNames {
		if _, exists := w.processedPartialWals[wf][fileName]; !exists {
			newFileNames = append(newFileNames, fileName)
		}
	}

	// ONLY remove files prefixed with the local UUID. This is import when handling remote WalFile locations and
	// enables us to handle distributed merge without any distrbuted file mutex mechanisms (etc).
	if fullSync {
		// TODO this does not need to be another call to S3 seeing as we retrieve fileNames above!!
		homeFileNames, err := wf.getFileNamesMatchingPattern(fmt.Sprintf(filePathPattern, fmt.Sprintf("%v*", w.uuid)))
		if err != nil {
			log.Fatal(err)
		}
		for _, fileName := range homeFileNames {
			defer wf.removeFile(fileName)
		}
	}

	// Grab any pending logs early for the comparison below
	pendingLogs, _ := w.pendingRemoteLogs[wf]

	// For partial syncs, if no new files were found, (and there are no new local logs to flush in
	// w.log) we've already processed all of them, so return early
	if !fullSync && len(*w.log) == 0 && len(newFileNames) == 0 && len(*pendingLogs) == 0 {
		return w.fullLog, nil
	}

	newMergedWal := []eventLog{}
	// Iterate over the new files and generate a new log of events
	for _, fileName := range newFileNames {
		newWal, err := wf.generateLogFromFile(fileName)
		if err != nil {
			log.Fatal(err)
		}
		newMergedWal = *(merge(&newMergedWal, &newWal))
		// Add to the processed cache
		w.processedPartialWals[wf][fileName] = struct{}{}
	}
	processedWithNewEvents := *(merge(w.log, merge(w.fullLog, &newMergedWal)))

	//
	// SAVE AND FLUSH TO A SINGLE WAL
	//

	// On full sync, we want to flush the product of all merged logs to a new file as we're effectively
	// aggregating all. We then remove all other files
	var eventsToFlush []eventLog
	if fullSync {
		eventsToFlush = processedWithNewEvents
	} else {
		eventsToFlush = *w.log
		// Grab any aggregated pending local changes
		eventsToFlush = *(merge(&eventsToFlush, pendingLogs))
		w.pendingRemoteLogs[wf] = &[]eventLog{}
	}
	// TODO move this to an explicit location outside of the function
	w.log = &[]eventLog{}

	// The only time we don't want to flush is when eventsToFlush is empty and we're not doing a full sync
	if fullSync || len(eventsToFlush) > 0 {
		// We need to use the local WalFile rootDir here
		randomUUID := generateUUID()
		// We need to generate the file within the local root (which is guaranteed to exist) but then flush
		// to the correct remote prefix
		randomLocalWal := fmt.Sprintf(path.Join(wf.getLocalRootDir(), walFilePattern), fmt.Sprintf("%v%v", w.uuid, randomUUID))
		randomWal := fmt.Sprintf(path.Join(wf.getRootDir(), walFilePattern), fmt.Sprintf("%v%v", w.uuid, randomUUID))
		f, err := os.OpenFile(randomLocalWal, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		// Seek to beginning of file just in case
		f.Truncate(0)
		f.Seek(0, io.SeekStart)

		// Write the schema ID
		err = binary.Write(f, binary.LittleEndian, latestWalSchemaID)
		if err != nil {
			log.Fatal(err)
		}

		for _, item := range eventsToFlush {
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
				}
			}
		}
		err = wf.flush(f, randomWal)
		if err != nil {
			log.Fatal(err)
		}
		// Add it straight to the cache to avoid processing it in the future
		w.processedPartialWals[wf][randomWal] = struct{}{}
	}

	return &processedWithNewEvents, nil
}
