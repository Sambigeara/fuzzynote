package service

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"time"

	//"sync"
	//"regexp"
	//"runtime"

	"github.com/rogpeppe/go-internal/lockedfile"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 1

// Wal manages the state of the WAL, via all update functions and replay functionality
type Wal struct {
	uuid              uuid
	log               *[]EventLog // log represents a fresh set of events (unique from the historical log below)
	fullLog           *[]EventLog // fullLog is a historical log of events
	listItemTracker   map[string]*ListItem
	latestWalSchemaID uint16
	walFiles          []WalFile
	eventsChan        chan EventLog
}

func generateUUID() uuid {
	return uuid(rand.Uint32())
}

func NewWal() *Wal {
	wal := Wal{
		uuid:              generateUUID(),
		latestWalSchemaID: latestWalSchemaID,
		log:               &[]EventLog{},
		fullLog:           &[]EventLog{},
		listItemTracker:   make(map[string]*ListItem),
		eventsChan:        make(chan EventLog),
		//walFiles:             []WalFile{},
	}
	return &wal
}

type walItemSchema1 struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	EventTime                  int64
	EventType                  eventType
	LineLength                 uint64
	NoteLength                 uint64
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

type EventLog struct {
	uuid                       uuid
	targetUUID                 uuid
	listItemCreationTime       int64
	targetListItemCreationTime int64
	unixNanoTime               int64
	eventType                  eventType
	line                       string
	note                       *[]byte
}

// WalFile offers a generic interface into local or remote filesystems
type WalFile interface {
	getRootDir() string
	lock() error
	unlock() error
	getFileNamesMatchingPattern(string) ([]string, error)
	generateLogFromFile(string) ([]EventLog, error)
	removeFile(string) error
	flush(*bytes.Buffer, string) error
	isPartialWalProcessed(string) bool
	setProcessedPartialWals(string)
	awaitPull()
	awaitGather()
	stopTickers()
}

type localWalFile struct {
	fileMutex            *lockedfile.File
	rootDir              string
	RefreshTicker        *time.Ticker
	GatherTicker         *time.Ticker
	processedPartialWals map[string]struct{}
	//syncFilePath string
}

func NewLocalWalFile(refreshFrequency uint16, gatherFrequency uint16, rootDir string) *localWalFile {
	return &localWalFile{
		rootDir:              rootDir,
		RefreshTicker:        time.NewTicker(time.Millisecond * time.Duration(refreshFrequency)),
		GatherTicker:         time.NewTicker(time.Millisecond * time.Duration(gatherFrequency)),
		processedPartialWals: make(map[string]struct{}),
		//syncFilePath: path.Join(rootDir, syncFile),
	}
}

func (wf *localWalFile) getRootDir() string {
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

func (wf *localWalFile) generateLogFromFile(fileName string) ([]EventLog, error) {
	wal := []EventLog{}
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		return wal, err
	}

	var b []byte
	b, err = ioutil.ReadFile(fileName)
	buf := bytes.NewBuffer(b)
	wal, err = buildFromFile(buf)
	if err != nil {
		return wal, err
	}
	return wal, nil
}

func (wf *localWalFile) removeFile(fileName string) error {
	return os.Remove(fileName)
}

func (wf *localWalFile) flush(b *bytes.Buffer, fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	f.Write(b.Bytes())
	return nil
}

func (wf *localWalFile) isPartialWalProcessed(fileName string) bool {
	_, exists := wf.processedPartialWals[fileName]
	return exists
}

func (wf *localWalFile) setProcessedPartialWals(fileName string) {
	wf.processedPartialWals[fileName] = struct{}{}
}

func (wf *localWalFile) awaitPull() {
	<-wf.RefreshTicker.C
}

func (wf *localWalFile) awaitGather() {
	<-wf.GatherTicker.C
}

func (wf *localWalFile) stopTickers() {
	wf.RefreshTicker.Stop()
	wf.GatherTicker.Stop()
}

func (r *DBListRepo) CallFunctionForEventLog(root *ListItem, e EventLog) (*ListItem, *ListItem, error) {
	item := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.listItemCreationTime)]
	targetItem := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.targetUUID, e.targetListItemCreationTime)]

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
		root, item, err = r.wal.add(root, e.listItemCreationTime, e.line, e.note, targetItem, e.uuid)
		r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, item.creationTime)] = item
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
		delete(r.wal.listItemTracker, fmt.Sprintf("%d:%d", e.uuid, e.listItemCreationTime))
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

func (w *Wal) add(root *ListItem, creationTime int64, line string, note *[]byte, childItem *ListItem, uuid uuid) (*ListItem, *ListItem, error) {
	if note == nil {
		note = &[]byte{}
	}
	newItem := &ListItem{
		originUUID:   uuid,
		creationTime: creationTime,
		child:        childItem,
		Line:         line,
		Note:         note,
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

func (r *DBListRepo) Replay(partialWal *[]EventLog) error {
	// Merge with any new local events which may have occurred during sync
	r.wal.log = merge(r.wal.log, partialWal)
	// If no events, do nothing and return nil
	if len(*r.wal.log) == 0 {
		return nil
	}

	var root *ListItem
	for _, e := range *r.wal.log {
		// We need to pass a fresh null root and leave the old r.Root intact for the function
		// caller logic, because dragons lie within
		root, _, _ = r.CallFunctionForEventLog(root, e)
	}

	r.Root = root
	return nil
}

func getNextEventLogFromWalFile(b *bytes.Buffer, schemaVersionID uint16) (*EventLog, error) {
	el := EventLog{}
	// TODO this is a hacky fix. Instantiate the note just in case
	el.note = &[]byte{}

	item := walItemSchema1{}
	err := binary.Read(b, binary.LittleEndian, &item)
	if err != nil {
		return nil, err
	}
	el.listItemCreationTime = item.ListItemCreationTime
	el.targetListItemCreationTime = item.TargetListItemCreationTime
	el.unixNanoTime = item.EventTime
	el.uuid = item.UUID
	el.targetUUID = item.TargetUUID
	el.eventType = item.EventType

	line := make([]byte, item.LineLength)
	err = binary.Read(b, binary.LittleEndian, &line)
	if err != nil {
		return nil, err
	}
	el.line = string(line)

	if item.NoteLength > 0 {
		note := make([]byte, item.NoteLength)
		err = binary.Read(b, binary.LittleEndian, &note)
		if err != nil {
			return nil, err
		}
		el.note = &note
	}
	return &el, nil
}

func buildFromFile(b *bytes.Buffer) ([]EventLog, error) {
	// The first two bytes of each file represents the file schema ID. For now this means nothing
	// so we can seek forwards 2 bytes
	//f.Seek(int64(unsafe.Sizeof(latestWalSchemaID)), io.SeekStart)

	el := []EventLog{}
	var walSchemaVersionID uint16
	err := binary.Read(b, binary.LittleEndian, &walSchemaVersionID)
	if err != nil {
		if err == io.EOF {
			return el, nil
		} else {
			log.Fatal(err)
			return el, err
		}
	}

	for {
		e, err := getNextEventLogFromWalFile(b, walSchemaVersionID)
		if err != nil {
			switch err {
			case io.EOF:
				return el, nil
			case io.ErrUnexpectedEOF:
				// Given the distributed concurrent nature of this app, we sometimes pick up partially
				// uploaded files which will fail, but may well be complete later on, therefore just
				// return for now and attempt again later
				// TODO implement a decent retry mech here
				return []EventLog{}, nil
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

func checkEquality(event1 EventLog, event2 EventLog) int {
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

func merge(wal1 *[]EventLog, wal2 *[]EventLog) *[]EventLog {
	mergedEl := []EventLog{}
	if len(*wal1) == 0 && len(*wal2) == 0 {
		return &mergedEl
	} else if len(*wal1) == 0 {
		return wal2
	} else if len(*wal2) == 0 {
		return wal1
	}

	// Before merging, check to see that the the most recent from one wal isn't older than the oldest from another.
	// If that is the case, append the newer to the older and return.
	if checkEquality((*wal1)[0], (*wal2)[len(*wal2)-1]) == rightEventOlder {
		mergedEl = append(*wal2, *wal1...)
		return &mergedEl
	} else if checkEquality((*wal2)[0], (*wal1)[len(*wal1)-1]) == rightEventOlder {
		mergedEl = append(*wal1, *wal2...)
		return &mergedEl
	}

	// Adopt a two pointer approach
	i, j := 0, 0
	// We can use an empty log here because it will never be equal to in the checkEquality calls below
	lastEvent := EventLog{}
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

func compact(wal *[]EventLog) *[]EventLog {
	// Traverse from most recent to most distant logs. Omit events in the following scenarios:
	// - Any events preceding a deleteEvent
	// - Any updateEvent preceding the most recent updateEvent
	// WARNING: Omitting addEvents/moveEvents leads to weird behaviour during Replay at the mo. Be warned.
	keysToPurge := make(map[string]bool)
	compactedWal := []EventLog{}
	for i := len(*wal) - 1; i >= 0; i-- {
		e := (*wal)[i]
		key := fmt.Sprintf("%d:%d", e.uuid, e.listItemCreationTime)
		if isDelete, purged := keysToPurge[key]; purged && (isDelete || e.eventType == updateEvent) {
			continue
		}
		if e.eventType == updateEvent {
			keysToPurge[key] = false
		} else if e.eventType == deleteEvent {
			keysToPurge[key] = true
		}
		// We need to reverse the list, but prepending is horribly inefficient, so append and reverse before
		// returning
		compactedWal = append(compactedWal, e)
	}
	// Reverse
	for i, j := 0, len(compactedWal)-1; i < j; i, j = i+1, j-1 {
		compactedWal[i], compactedWal[j] = compactedWal[j], compactedWal[i]
	}
	return &compactedWal
}

func pull(wf WalFile, walChan chan *[]EventLog) error {
	filePathPattern := path.Join(wf.getRootDir(), walFilePattern)
	allFileNames, err := wf.getFileNamesMatchingPattern(fmt.Sprintf(filePathPattern, "*"))
	if err != nil {
		log.Fatal(err)
	}

	newMergedWal := []EventLog{}
	for _, fileName := range allFileNames {
		if !wf.isPartialWalProcessed(fileName) {
			newWal, err := wf.generateLogFromFile(fileName)
			if err != nil {
				log.Fatal(err)
			}
			newMergedWal = *(merge(&newMergedWal, &newWal))
			// Add to the processed cache
			wf.setProcessedPartialWals(fileName)
		}
	}
	walChan <- &newMergedWal

	return nil
}

func (w *Wal) buildPartialWal(wal *[]EventLog) (*bytes.Buffer, error) {
	var b bytes.Buffer
	// Write the schema ID
	err := binary.Write(&b, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		log.Fatal(err)
	}

	for _, item := range *wal {
		lenLine := uint64(len([]byte(item.line)))
		var lenNote uint64
		if item.note != nil {
			lenNote = uint64(len(*(item.note)))
		}
		i := walItemSchema1{
			UUID:                       item.uuid,
			TargetUUID:                 item.targetUUID,
			ListItemCreationTime:       item.listItemCreationTime,
			TargetListItemCreationTime: item.targetListItemCreationTime,
			EventTime:                  item.unixNanoTime,
			EventType:                  item.eventType,
			LineLength:                 lenLine,
			NoteLength:                 lenNote,
		}
		data := []interface{}{
			i,
			[]byte(item.line),
		}
		if item.note != nil {
			data = append(data, item.note)
		}
		for _, v := range data {
			err = binary.Write(&b, binary.LittleEndian, v)
			if err != nil {
				fmt.Printf("binary.Write failed when writing field for WAL log item %v: %s\n", v, err)
				log.Fatal(err)
			}
		}
	}
	return &b, nil
}

func (w *Wal) push(el *[]EventLog, wf WalFile) error {
	var b bytes.Buffer
	// Write the schema ID
	err := binary.Write(&b, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		log.Fatal(err)
	}

	for _, item := range *el {
		lenLine := uint64(len([]byte(item.line)))
		var lenNote uint64
		if item.note != nil {
			lenNote = uint64(len(*(item.note)))
		}
		i := walItemSchema1{
			UUID:                       item.uuid,
			TargetUUID:                 item.targetUUID,
			ListItemCreationTime:       item.listItemCreationTime,
			TargetListItemCreationTime: item.targetListItemCreationTime,
			EventTime:                  item.unixNanoTime,
			EventType:                  item.eventType,
			LineLength:                 lenLine,
			NoteLength:                 lenNote,
		}
		data := []interface{}{
			i,
			[]byte(item.line),
		}
		if item.note != nil {
			data = append(data, item.note)
		}
		for _, v := range data {
			err = binary.Write(&b, binary.LittleEndian, v)
			if err != nil {
				fmt.Printf("binary.Write failed when writing field for WAL log item %v: %s\n", v, err)
				log.Fatal(err)
			}
		}
	}

	randomUUID := fmt.Sprintf("%v%v", w.uuid, generateUUID())
	randomWal := fmt.Sprintf(path.Join(wf.getRootDir(), walFilePattern), randomUUID)
	go func(wf WalFile) {
		err := wf.flush(&b, randomWal)
		if err != nil {
			log.Fatal(err)
		}
		// Add it straight to the cache to avoid processing it in the future
		wf.setProcessedPartialWals(randomWal)
	}(wf)

	return nil
}

// gather up all WALs in the WalFile matching the local UUID into a single new Wal, and attempt
// to delete the old ones
func (w *Wal) gather(wf WalFile) error {
	filePathPattern := path.Join(wf.getRootDir(), fmt.Sprintf(walFilePattern, fmt.Sprintf("%v*", w.uuid)))
	originFiles, err := wf.getFileNamesMatchingPattern(filePathPattern)
	if err != nil {
		log.Fatal(err)
	}

	// If there's only 1 file, there's no point gathering them, so return
	if len(originFiles) <= 1 {
		return nil
	}

	// Gather origin files
	mergedWal := []EventLog{}
	for _, fileName := range originFiles {
		wal, err := wf.generateLogFromFile(fileName)
		if err != nil {
			log.Fatal(err)
		}
		mergedWal = *(merge(&mergedWal, &wal))
	}

	// Flush the gathered Wal to ALL walfiles
	// TODO actual proper error handling in push
	// Use a WaitGroup so we can run all pushes async but then assert on completion before deleting files
	// from origin walfile
	//var wg sync.WaitGroup
	//for _, tempWf := range w.walFiles {
	//    wg.Add(1)
	//    go func(tempWf WalFile) error {
	if err := w.push(&mergedWal, wf); err != nil {
		return err
	}
	//wg.Done()
	//return nil
	//    }(tempWf)
	//}
	//wg.Wait()

	// Schedule a delete on the files
	for _, fileName := range originFiles {
		// TODO proper error handling here too
		wf.removeFile(fileName)
	}

	return nil
}

func (w *Wal) startSync(walChan chan *[]EventLog) error {
	// Run a concurrent/synchronous pull on all walfiles individually on app start
	// This also gathers any new Wals that may have appeared locally whilst the app was off
	// Push these to all walfiles
	startUpWalChan := make(chan *[]EventLog)

	// The following loop watches for any wals pulled on load within the first go func in the
	// main walfile loop below
	go func() {
		for {
			el := <-startUpWalChan
			for _, wf := range w.walFiles {
				go func(wf WalFile) {
					w.push(el, wf)
				}(wf)
			}
		}
	}()

	for _, wf := range w.walFiles {
		// Pull all wals on startup into a channel to push to all walfiles in the loop above
		go func(wf WalFile) error {
			if err := pull(wf, startUpWalChan); err != nil {
				return err
			}
			return nil
		}(wf)

		// Schedule async pull from walfiles individually
		go func(wf WalFile) error {
			for {
				wf.awaitPull()
				if err := pull(wf, walChan); err != nil {
					return err
				}
			}
		}(wf)

		// Schedule gather tasks
		go func(wf WalFile) error {
			for {
				wf.awaitGather()
				if err := w.gather(wf); err != nil {
					return err
				}
			}
		}(wf)
	}

	// Push to all WalFiles
	go func() {
		for {
			ev := <-w.eventsChan
			// Push same event to all WalFiles
			el := []EventLog{ev}
			for _, wf := range w.walFiles {
				go func() {
					w.push(&el, wf)
				}()
			}
		}
	}()

	return nil
}

func (w *Wal) finish() error {
	for _, wf := range w.walFiles {
		wf.stopTickers()
	}
	return nil
}
