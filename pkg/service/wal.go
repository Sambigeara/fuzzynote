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
	"sync"
	"time"
	//b64 "encoding/base64"

	"nhooyr.io/websocket"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 2

func generateUUID() uuid {
	return uuid(rand.Uint32())
}

// TODO remove
type walItemSchema1 struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	EventTime                  int64
	EventType                  EventType
	LineLength                 uint64
	NoteLength                 uint64
}

type walItemSchema2 struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	EventTime                  int64
	EventType                  EventType
	LineLength                 uint64
	NoteExists                 bool
	NoteLength                 uint64
}

// Ordering of these enums are VERY IMPORTANT as they're used for comparisons when resolving WAL merge conflicts
// (although there has to be nanosecond level collisions in order for this to be relevant)
const (
	NullEvent EventType = iota
	AddEvent
	UpdateEvent
	MoveUpEvent
	MoveDownEvent
	ShowEvent
	HideEvent
	DeleteEvent
)

type EventLog struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string
	Note                       *[]byte
}

func (e *EventLog) getKeys() (string, string) {
	key := fmt.Sprintf("%d:%d", e.UUID, e.ListItemCreationTime)
	targetKey := fmt.Sprintf("%d:%d", e.TargetUUID, e.TargetListItemCreationTime)
	return key, targetKey
}

// WalFile offers a generic interface into local or remote filesystems
type WalFile interface {
	// TODO surely these should just implement Push/Pull/Gather????
	GetUUID() string
	GetRoot() string
	GetMatchingWals(string) ([]string, error)
	GetWal(string) ([]EventLog, error)
	RemoveWals([]string) error
	Flush(*bytes.Buffer, string) error

	// TODO these probably don't need to be interface functions
	SetProcessedPartialWals(string)
	IsPartialWalProcessed(string) bool
	SetProcessedEvent(string)
	IsEventProcessed(string) bool

	AwaitPull()
	AwaitGather()
	StopTickers()

	GetMode() string
	GetPushMatchTerm() []rune
}

type LocalWalFile interface {
	Load(interface{}) (uint32, error)
	Stop(uint32, interface{}) error

	WalFile
}

type LocalFileWalFile struct {
	rootDir                  string
	RefreshTicker            *time.Ticker
	GatherTicker             *time.Ticker
	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
	mode                     string
	pushMatchTerm            []rune
	processedEventLock       *sync.Mutex
	processedEventMap        map[string]struct{}
}

func NewLocalFileWalFile(refreshFrequency uint16, gatherFrequency uint16, rootDir string) *LocalFileWalFile {
	return &LocalFileWalFile{
		rootDir:                  rootDir,
		RefreshTicker:            time.NewTicker(time.Millisecond * time.Duration(refreshFrequency)),
		GatherTicker:             time.NewTicker(time.Millisecond * time.Duration(gatherFrequency)),
		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
		mode:                     ModeSync,
		pushMatchTerm:            []rune{},
		processedEventLock:       &sync.Mutex{},
		processedEventMap:        make(map[string]struct{}),
	}
}

func (wf *LocalFileWalFile) flushPrimary(f *os.File, uuid uuid) error {
	// Truncate and move to start of file just in case
	f.Truncate(0)
	f.Seek(0, io.SeekStart)

	// Write the file header to the start of the file
	fileHeader := fileHeader{
		SchemaID: latestFileSchemaID,
		UUID:     uuid,
	}
	err := binary.Write(f, binary.LittleEndian, &fileHeader)
	if err != nil {
		fmt.Println("binary.Write failed when writing fileHeader:", err)
		log.Fatal(err)
		return err
	}
	return nil
}

// Load retrieves UUID, instantiates the app and flushes to disk if required
func (wf *LocalFileWalFile) Load(ctx interface{}) (uint32, error) {
	rootPath := path.Join(wf.rootDir, rootFileName)
	f, err := os.OpenFile(rootPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return 0, err
	}
	defer f.Close()

	fileHeader := fileHeader{}
	err = binary.Read(f, binary.LittleEndian, &fileHeader)
	uuid := generateUUID()
	if err != nil {
		// For initial load cases (first time an app is run) to beat an edge case race condition
		// (loading two apps in a fresh root without saves) we need to flush state to the primary.db
		// file. This prevents initial apps getting confused and generating different WAL UUIDs (thus
		// ultimately leading to data loss)
		if err == io.EOF {
			wf.flushPrimary(f, uuid)
		} else {
			log.Fatal(err)
			return 0, err
		}
	}

	// We can now override uuid as it's been read from the file
	if fileHeader.UUID != 0 {
		uuid = fileHeader.UUID
	}

	return uint32(uuid), nil
}

func (wf *LocalFileWalFile) Stop(uid uint32, ctx interface{}) error {
	rootPath := path.Join(wf.rootDir, rootFileName)
	f, err := os.Create(rootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	err = wf.flushPrimary(f, uuid(uid))
	if err != nil {
		return err
	}

	return nil
}

func (wf *LocalFileWalFile) GetUUID() string {
	// TODO this is a stub function for now, refactor out
	// knowledge of UUID is only relevant for WebWalFiles
	return ""
}

func (wf *LocalFileWalFile) GetRoot() string {
	return wf.rootDir
}

func (wf *LocalFileWalFile) GetMatchingWals(matchPattern string) ([]string, error) {
	fileNames, err := filepath.Glob(matchPattern)
	if err != nil {
		return []string{}, err
	}
	return fileNames, nil
}

func (wf *LocalFileWalFile) GetWal(fileName string) ([]EventLog, error) {
	wal := []EventLog{}
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		return wal, err
	}

	var b []byte
	b, err = ioutil.ReadFile(fileName)
	buf := bytes.NewBuffer(b)
	wal, err = BuildFromFile(buf)
	if err != nil {
		return wal, err
	}
	return wal, nil
}

func (wf *LocalFileWalFile) RemoveWals(fileNames []string) error {
	for _, f := range fileNames {
		os.Remove(f)
	}
	return nil
}

func (wf *LocalFileWalFile) Flush(b *bytes.Buffer, fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	f.Write(b.Bytes())
	return nil
}

func (wf *LocalFileWalFile) SetProcessedPartialWals(fileName string) {
	wf.processedPartialWalsLock.Lock()
	defer wf.processedPartialWalsLock.Unlock()
	wf.processedPartialWals[fileName] = struct{}{}
}

func (wf *LocalFileWalFile) IsPartialWalProcessed(fileName string) bool {
	wf.processedPartialWalsLock.Lock()
	defer wf.processedPartialWalsLock.Unlock()
	_, exists := wf.processedPartialWals[fileName]
	return exists
}

func (wf *LocalFileWalFile) AwaitPull() {
	<-wf.RefreshTicker.C
}

func (wf *LocalFileWalFile) AwaitGather() {
	<-wf.GatherTicker.C
}

func (wf *LocalFileWalFile) StopTickers() {
	wf.RefreshTicker.Stop()
	wf.GatherTicker.Stop()
}

func (wf *LocalFileWalFile) GetMode() string {
	return wf.mode
}

func (wf *LocalFileWalFile) GetPushMatchTerm() []rune {
	return wf.pushMatchTerm
}

func (wf *LocalFileWalFile) SetProcessedEvent(fileName string) {
	// TODO these are currently duplicated across walfiles, think of a more graceful
	// boundary for reuse
	wf.processedEventLock.Lock()
	defer wf.processedEventLock.Unlock()
	wf.processedEventMap[fileName] = struct{}{}
}

func (wf *LocalFileWalFile) IsEventProcessed(fileName string) bool {
	wf.processedEventLock.Lock()
	defer wf.processedEventLock.Unlock()
	_, exists := wf.processedEventMap[fileName]
	return exists
}

func (r *DBListRepo) CallFunctionForEventLog(root *ListItem, e EventLog) (*ListItem, *ListItem, error) {
	key, targetKey := e.getKeys()
	item := r.listItemTracker[key]
	targetItem := r.listItemTracker[targetKey]

	// When we're calling this function on initial WAL merge and load, we may come across
	// orphaned items. There MIGHT be a valid case to keep events around if the EventType
	// is Update. Item will obviously never exist for Add. For all other eventTypes,
	// we should just skip the event and return
	if item == nil && e.EventType != AddEvent && e.EventType != UpdateEvent {
		return root, item, nil
	}

	var err error
	switch e.EventType {
	case AddEvent:
		root, item, err = r.add(root, e.ListItemCreationTime, e.Line, e.Note, targetItem, e.UUID)
		r.listItemTracker[key] = item
	case UpdateEvent:
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
			item, err = r.update(e.Line, e.Note, item)
		} else {
			addEl := e
			addEl.EventType = AddEvent
			root, item, err = r.CallFunctionForEventLog(root, addEl)
		}
	case DeleteEvent:
		root, err = r.del(root, item)
		delete(r.listItemTracker, key)
	case MoveUpEvent:
		if targetItem == nil {
			return root, item, err
		}
		root, item, err = r.move(root, item, targetItem.child)
		// Need to override the listItemTracker to ensure pointers are correct
		r.listItemTracker[key] = item
	case MoveDownEvent:
		if targetItem == nil {
			return root, item, err
		}
		root, item, err = r.move(root, item, targetItem)
		// Need to override the listItemTracker to ensure pointers are correct
		r.listItemTracker[key] = item
	case ShowEvent:
		err = r.setVisibility(item, true)
	case HideEvent:
		err = r.setVisibility(item, false)
	}
	return root, item, err
}

func (r *DBListRepo) add(root *ListItem, creationTime int64, line string, note *[]byte, childItem *ListItem, uuid uuid) (*ListItem, *ListItem, error) {
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
func (r *DBListRepo) update(line string, note *[]byte, listItem *ListItem) (*ListItem, error) {
	// We currently separate Line and Note updates even though they use the same interface
	// This is to reduce wal size and also solves some race conditions for long held open
	// notes, etc
	if note != nil {
		listItem.Note = note
	} else {
		listItem.Line = line
	}
	return listItem, nil
}

func (r *DBListRepo) del(root *ListItem, item *ListItem) (*ListItem, error) {
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

func (r *DBListRepo) move(root *ListItem, item *ListItem, childItem *ListItem) (*ListItem, *ListItem, error) {
	var err error
	root, err = r.del(root, item)
	root, item, err = r.add(root, item.creationTime, item.Line, item.Note, childItem, item.originUUID)
	return root, item, err
}

func (r *DBListRepo) setVisibility(item *ListItem, isVisible bool) error {
	item.IsHidden = !isVisible
	return nil
}

func (r *DBListRepo) Replay(partialWal *[]EventLog) error {
	// No point merging with an empty partialWal
	if len(*partialWal) == 0 {
		return nil
	}

	// Merge with any new local events which may have occurred during sync
	r.log = merge(r.log, partialWal)

	// Clear the listItemTracker for all full Replays
	// This map is also used by the main service interface CRUD endpoints, but we can
	// clear it here because both the CRUD ops and these Replays run in the same loop,
	// so we won't get any contention.
	r.listItemTracker = make(map[string]*ListItem)

	var root *ListItem
	for _, e := range *r.log {
		// We need to pass a fresh null root and leave the old r.Root intact for the function
		// caller logic
		root, _, _ = r.CallFunctionForEventLog(root, e)
	}

	r.Root = root
	return nil
}

func getNextEventLogFromWalFile(b *bytes.Buffer, schemaVersionID uint16) (*EventLog, error) {
	el := EventLog{}

	// TODO remove
	if schemaVersionID == 1 {
		item := walItemSchema1{}
		err := binary.Read(b, binary.LittleEndian, &item)
		if err != nil {
			return nil, err
		}
		el.ListItemCreationTime = item.ListItemCreationTime
		el.TargetListItemCreationTime = item.TargetListItemCreationTime
		el.UnixNanoTime = item.EventTime
		el.UUID = item.UUID
		el.TargetUUID = item.TargetUUID
		el.EventType = item.EventType

		line := make([]byte, item.LineLength)
		err = binary.Read(b, binary.LittleEndian, &line)
		if err != nil {
			return nil, err
		}
		el.Line = string(line)

		if item.NoteLength > 0 {
			note := make([]byte, item.NoteLength)
			err = binary.Read(b, binary.LittleEndian, &note)
			if err != nil {
				return nil, err
			}
			el.Note = &note
		}
	} else {
		item := walItemSchema2{}
		err := binary.Read(b, binary.LittleEndian, &item)
		if err != nil {
			return nil, err
		}
		el.ListItemCreationTime = item.ListItemCreationTime
		el.TargetListItemCreationTime = item.TargetListItemCreationTime
		el.UnixNanoTime = item.EventTime
		el.UUID = item.UUID
		el.TargetUUID = item.TargetUUID
		el.EventType = item.EventType

		line := make([]byte, item.LineLength)
		err = binary.Read(b, binary.LittleEndian, &line)
		if err != nil {
			return nil, err
		}
		el.Line = string(line)

		if item.NoteExists {
			el.Note = &[]byte{}
		}
		if item.NoteLength > 0 {
			note := make([]byte, item.NoteLength)
			err = binary.Read(b, binary.LittleEndian, &note)
			if err != nil {
				return nil, err
			}
			el.Note = &note
		}
	}
	return &el, nil
}

func BuildFromFile(b *bytes.Buffer) ([]EventLog, error) {
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
	if event1.UnixNanoTime < event2.UnixNanoTime ||
		event1.UnixNanoTime == event2.UnixNanoTime && event1.UUID < event2.UUID ||
		event1.UnixNanoTime == event2.UnixNanoTime && event1.UUID == event2.UUID && event1.EventType < event2.EventType {
		return leftEventOlder
	} else if event2.UnixNanoTime < event1.UnixNanoTime ||
		event2.UnixNanoTime == event1.UnixNanoTime && event2.UUID < event1.UUID ||
		event2.UnixNanoTime == event1.UnixNanoTime && event2.UUID == event1.UUID && event2.EventType < event1.EventType {
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
	// - Any events preceding a DeleteEvent
	// - Any UpdateEvent preceding the most recent UpdateEvent
	// WARNING: Omitting addEvents/moveEvents leads to weird behaviour during Replay at the mo. Be warned.
	keysToPurge := make(map[string]bool)
	compactedWal := []EventLog{}
	for i := len(*wal) - 1; i >= 0; i-- {
		e := (*wal)[i]
		key, _ := e.getKeys()
		if isDelete, purged := keysToPurge[key]; purged && (isDelete || e.EventType == UpdateEvent) {
			continue
		}
		if e.EventType == UpdateEvent {
			keysToPurge[key] = false
		} else if e.EventType == DeleteEvent {
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

func (r *DBListRepo) generatePartialView(matchItems []ListItem) error {
	// Now we have our list of matchItems, we need to iterate over them to retrieve all originUUID:creationTime keys
	// which map to the wal entries
	logKeys := make(map[string]struct{})
	for _, item := range matchItems {
		logKeys[item.Key()] = struct{}{}
	}

	// TODO figure out how to deal with this!!!
	// This is a bit of a strange one, but there is an edge case that occurs from addEvents referencing items which
	// do not exist in the partialWal - because the item will not exist, the item will shoot to the top of the list.
	// This is actually expected behaviour (and is idempotent and mergeable), however, for user-friendliness, to ensure
	// expected ordering in the output partial view, we will manually add move* events to the matched items to force them
	// into the expected locations but with all child pointers attached direct. This _does_ have the side-effect of ultimately
	// grouping the items in the origin (if/when the remote wal is merged back in), but this is probably preferred.

	// Now we have our keys, we can iterate over the entire wal, and generate a partial wal containing only eventLogs
	// for ListItems retrieved above.
	// IMPORTANT: we also need to include ALL DeleteEvent logs, as otherwise we may end up with orphaned items in the
	// target view.
	partialWal := []EventLog{}
	for _, e := range *r.log {
		key, _ := e.getKeys()
		if _, exists := logKeys[key]; exists || e.EventType == DeleteEvent {
			partialWal = append(partialWal, e)
		}
	}

	// We now need to generate a temp name (NOT matching the standard wal pattern) and push to it. We can then manually
	// retrieve and handle the wal (for now)
	// Use the current time to generate the name
	b := BuildByteWal(&partialWal)
	viewName := fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), viewFilePattern), time.Now().UnixNano())
	r.LocalWalFile.Flush(b, viewName)
	return nil
}

func pull(wf WalFile) (*[]EventLog, error) {
	filePathPattern := path.Join(wf.GetRoot(), walFilePattern)
	allFileNames, err := wf.GetMatchingWals(fmt.Sprintf(filePathPattern, "*"))
	if err != nil {
		log.Fatal(err)
	}

	newMergedWal := []EventLog{}
	for _, fileName := range allFileNames {
		if !wf.IsPartialWalProcessed(fileName) {
			newWal, err := wf.GetWal(fileName)
			if err != nil {
				log.Fatal(err)
			}
			newMergedWal = *(merge(&newMergedWal, &newWal))

			// TODO refactor this so it's inline rather than a whole new iteration??
			// Ensure all events are cached in the processedEvent cache so any local changes
			// on remote origin items will propagate back out
			for _, e := range newMergedWal {
				k, _ := e.getKeys()
				wf.SetProcessedEvent(k)
			}

			// Add to the processed cache
			wf.SetProcessedPartialWals(fileName)
		}
	}
	return &newMergedWal, nil
}

func BuildByteWal(el *[]EventLog) *bytes.Buffer {
	var b bytes.Buffer
	// Write the schema ID
	err := binary.Write(&b, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		log.Fatal(err)
	}

	// Compact the Wal
	//el = compact(el)

	for _, e := range *el {
		lenLine := uint64(len([]byte(e.Line)))
		var lenNote uint64
		noteExists := false
		if e.Note != nil {
			noteExists = true
			lenNote = uint64(len(*(e.Note)))
		}
		i := walItemSchema2{
			UUID:                       e.UUID,
			TargetUUID:                 e.TargetUUID,
			ListItemCreationTime:       e.ListItemCreationTime,
			TargetListItemCreationTime: e.TargetListItemCreationTime,
			EventTime:                  e.UnixNanoTime,
			EventType:                  e.EventType,
			LineLength:                 lenLine,
			NoteExists:                 noteExists,
			NoteLength:                 lenNote,
		}
		data := []interface{}{
			i,
			[]byte(e.Line),
		}
		if e.Note != nil {
			data = append(data, e.Note)
		}
		for _, v := range data {
			err = binary.Write(&b, binary.LittleEndian, v)
			if err != nil {
				fmt.Printf("binary.Write failed when writing field for WAL log item %v: %s\n", v, err)
				log.Fatal(err)
			}
		}
	}
	return &b
}

func getMatchedWal(el *[]EventLog, wf WalFile) *[]EventLog {
	matchTerm := wf.GetPushMatchTerm()

	if len(matchTerm) == 0 {
		return el
	}
	// Iterate over the entire Wal. If a Line fulfils the Match rules, log the key in a map
	for _, e := range *el {
		// For now (for safety) use full pattern matching
		if isMatch(matchTerm, e.Line, FullMatchPattern) {
			k, _ := e.getKeys()
			wf.SetProcessedEvent(k)
		}
	}

	filteredWal := []EventLog{}
	// Do a second iteration using the map above, and build a Wal which includes any logs which
	// fulfilled the match term at some point in it's history.
	for _, e := range *el {
		k, _ := e.getKeys()
		if wf.IsEventProcessed(k) {
			filteredWal = append(filteredWal, e)
		}
	}
	return &filteredWal
}

func (r *DBListRepo) push(el *[]EventLog, wf WalFile) error {
	// Apply any filtering based on Push match configuration
	el = getMatchedWal(el, wf)

	b := BuildByteWal(el)

	randomUUID := fmt.Sprintf("%v%v", r.uuid, generateUUID())
	randomWal := fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), randomUUID)
	if err := wf.Flush(b, randomWal); err != nil {
		log.Fatal(err)
	}
	// Add it straight to the cache to avoid processing it in the future
	wf.SetProcessedPartialWals(randomWal)

	return nil
}

// gather up all WALs in the WalFile matching the local UUID into a single new Wal, and attempt
// to delete the old ones
func (r *DBListRepo) gather(wf WalFile) error {
	// Handle only origin wals
	// TODO I think switching to this breaks other parts of the syncing process, use with caution
	//filePathPattern := path.Join(wf.GetRoot(), fmt.Sprintf(walFilePattern, fmt.Sprintf("%v*", r.uuid)))
	//originFiles, err := wf.GetMatchingWals(filePathPattern)

	// Handle ALL wals
	filePathPattern := path.Join(wf.GetRoot(), walFilePattern)
	originFiles, err := wf.GetMatchingWals(fmt.Sprintf(filePathPattern, "*"))
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
		wal, err := wf.GetWal(fileName)
		if err != nil {
			log.Fatal(err)
		}
		mergedWal = *(merge(&mergedWal, &wal))
	}

	// Flush the gathered Wal
	if err := r.push(&mergedWal, wf); err != nil {
		return err
	}

	// Schedule a delete on the files
	wf.RemoveWals(originFiles)

	return nil
}

func (r *DBListRepo) flushPartialWals(el []EventLog, sync bool) {
	if len(el) > 0 {
		for _, wf := range r.walFiles {
			if wf.GetMode() == ModeSync || wf.GetMode() == ModePush {
				if sync {
					r.push(&el, wf)
				} else {
					go func(wf WalFile) { r.push(&el, wf) }(wf)
				}
			}
		}
	}
}

func (r *DBListRepo) startSync(walChan chan *[]EventLog) error {
	// Run an initial blocking load from the local walfile (and put onto channel for immediate
	// processing in main loop). Also push to all walFiles (this will get missed in async loop below
	// due to cache, so small amount of duplicated code required).
	var localEl *[]EventLog
	var err error
	if localEl, err = pull(r.LocalWalFile); err != nil {
		return err
	}
	go func() { walChan <- localEl }()
	for _, wf := range r.walFiles {
		if wf != r.LocalWalFile {
			go func(wf WalFile) { r.push(localEl, wf) }(wf)
		}
	}

	for _, wf := range r.walFiles {
		// Trigger initial instant single pull from all walfiles
		go func(wf WalFile) {
			var el *[]EventLog
			if el, err = pull(wf); err != nil {
				log.Fatal(err)
			}
			walChan <- el
		}(wf)

		// And then schedule repeating async pulls
		go func(wf WalFile) {
			for {
				wf.AwaitPull()
				var el *[]EventLog
				if el, err = pull(wf); err != nil {
					log.Fatal(err)
				}
				walChan <- el
			}
		}(wf)

		// Schedule gather tasks
		go func(wf WalFile) {
			for {
				wf.AwaitGather()
				if err := r.gather(wf); err != nil {
					log.Fatal(err)
				}
			}
		}(wf)
	}

	// Consume from the websocket, if available
	//if r.web != nil {
	go func() {
		// TODO Check for stop signal
		for {
			if r.web != nil && r.web.wsConn != nil {
				err := r.web.consumeWebsocket(walChan)
				if err != nil {
					return
				}
			} else {
				// No point tying up CPU for no-op, sleep 5 seconds between attempts to self-heal websocket
				time.Sleep(5 * time.Second)
			}
		}
	}()
	//}

	// Push to all WalFiles
	go func() {
		el := []EventLog{}
		for {
			// The events chan contains single events. We want to aggregate them between intervals
			// and then emit them in batches, for great efficiency gains.
			select {
			case e := <-r.eventsChan:
				// Write in real time to the websocket, if present
				if r.web != nil {
					// TODO this should only iterate over WebWalFiles
					for _, wf := range r.walFiles {
						// TODO getMatchedWal and mode checks should be handled in pushWebsocket maybe
						if wf.GetMode() == ModeSync || wf.GetMode() == ModePush {
							matchedEventLog := getMatchedWal(&[]EventLog{e}, wf)
							if len(*matchedEventLog) > 0 {
								e = (*matchedEventLog)[0]
								r.web.pushWebsocket(e, wf.GetUUID())
							}
						}
					}
				}
				// Consume off of the channel and add to an ephemeral log
				el = append(el, e)
			case <-r.pushTicker.C:
				// On ticks, Flush what we've aggregated to all walfiles, and then reset the
				// ephemeral log. If empty, skip.
				// We pass by reference, so we'll need to create a copy prior to sending to `push`
				// otherwise the underlying el may change before `push` has a chance to process it
				// If we start passing by value later on, this won't be required (as go will pass
				// copies by default, I think).
				elCopy := el
				r.flushPartialWals(elCopy, false)
				el = []EventLog{}
			case <-r.stop:
				r.flushPartialWals(el, true)
				r.stop <- struct{}{}
			}
		}
	}()

	return nil
}

func (r *DBListRepo) finish() error {
	// TODO duplication
	// Retrieve all local wal names
	filePathPattern := path.Join(r.LocalWalFile.GetRoot(), walFilePattern)
	localFileNames, _ := r.LocalWalFile.GetMatchingWals(fmt.Sprintf(filePathPattern, "*"))

	// Flush full log to local walfile
	r.push(r.log, r.LocalWalFile)

	// Delete all redundant local files
	r.LocalWalFile.RemoveWals(localFileNames)

	// Stop tickers
	r.pushTicker.Stop()
	for _, wf := range r.walFiles {
		wf.StopTickers()
	}

	// Flush all unpushed changes to non-local walfiles
	// TODO handle this more gracefully
	r.stop <- struct{}{}
	<-r.stop

	if r.web != nil {
		r.web.wsConn.Close(websocket.StatusNormalClosure, "")
	}
	return nil
}
