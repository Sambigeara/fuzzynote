package service

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 4

func generateUUID() uuid {
	return uuid(rand.Uint32())
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

var (
	errWalIntregrity = errors.New("the wal was forcefully recovered, r.log needs to be purged")
)

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
	GetUUID() string
	GetRoot() string
	GetMatchingWals(string) ([]string, error)
	GetWalBytes(io.Writer, string) error
	RemoveWals([]string) error
	Flush(*bytes.Buffer, string) error

	// TODO these probably don't need to be interface functions
	SetProcessedEvent(string)
	IsEventProcessed(string) bool

	GetMode() string
	GetPushMatchTerm() []rune
}

type LocalWalFile interface {
	Load(interface{}) (uint32, error)
	Stop(uint32, interface{}) error
	SetBaseUUID(uint32, interface{}) error
	Purge(interface{})

	WalFile
}

type LocalFileWalFile struct {
	rootDir            string
	mode               string
	pushMatchTerm      []rune
	processedEventLock *sync.Mutex
	processedEventMap  map[string]struct{}
}

func NewLocalFileWalFile(rootDir string) *LocalFileWalFile {
	return &LocalFileWalFile{
		rootDir:            rootDir,
		mode:               ModeSync,
		pushMatchTerm:      []rune{},
		processedEventLock: &sync.Mutex{},
		processedEventMap:  make(map[string]struct{}),
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

func (wf *LocalFileWalFile) SetBaseUUID(uid uint32, ctx interface{}) error {
	// TODO dedup
	rootPath := path.Join(wf.rootDir, rootFileName)
	f, err := os.OpenFile(rootPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	return wf.flushPrimary(f, uuid(uid))
}

func (wf *LocalFileWalFile) Purge(ctx interface{}) {
	if err := os.RemoveAll(wf.rootDir); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
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
	pullPaths, err := filepath.Glob(matchPattern)
	if err != nil {
		return []string{}, err
	}
	uuids := []string{}
	for _, p := range pullPaths {
		_, fileName := path.Split(p)
		uuid := strings.Split(strings.Split(fileName, "_")[1], ".")[0]
		uuids = append(uuids, uuid)
	}
	return uuids, nil
}

func (wf *LocalFileWalFile) GetWalBytes(w io.Writer, fileName string) error {
	//var b []byte
	fileName = fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), fileName)
	f, err := os.Open(fileName)
	if err != nil {
		return nil
	}
	if _, err := io.Copy(w, f); err != nil {
		return err
	}
	//b, err := ioutil.ReadFile(fileName)
	//if err != nil {
	//    return b, err
	//}
	return nil
}

func (wf *LocalFileWalFile) RemoveWals(fileNames []string) error {
	for _, f := range fileNames {
		os.Remove(fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), f))
	}
	return nil
}

func (wf *LocalFileWalFile) Flush(b *bytes.Buffer, randomUUID string) error {
	fileName := fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), randomUUID)
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	f.Write(b.Bytes())
	return nil
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
	case MoveDownEvent:
		if targetItem == nil {
			return root, item, nil
		}
		fallthrough
	case MoveUpEvent:
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

	fullMerge := true
	// Establish whether or not the oldest event of the partialWal is newer than the most
	// recent of the existing (and therefore already processed) wal.
	// If all events in the partialWal are newer, we can avoid the full merge, and r.Root
	// reset, which is a significant optimisation.
	// This needs to be done pre-merge.
	if len(*r.log) > 0 && checkEquality((*r.log)[len(*r.log)-1], (*partialWal)[0]) == leftEventOlder {
		fullMerge = false
	}

	// Merge with any new local events which may have occurred during sync
	r.log = merge(r.log, partialWal)

	var replayLog *[]EventLog
	var root *ListItem
	if fullMerge {
		// Clear the listItemTracker for all full Replays
		// This map is also used by the main service interface CRUD endpoints, but we can
		// clear it here because both the CRUD ops and these Replays run in the same loop,
		// so we won't get any contention.
		r.listItemTracker = make(map[string]*ListItem)
		replayLog = r.log
	} else {
		replayLog = partialWal
		root = r.Root
	}

	for _, e := range *replayLog {
		// We need to pass a fresh null root and leave the old r.Root intact for the function
		// caller logic
		root, _, _ = r.CallFunctionForEventLog(root, e)
	}

	r.Root = root
	return nil
}

func getNextEventLogFromWalFile(r io.Reader, schemaVersionID uint16) (*EventLog, error) {
	el := EventLog{}

	switch schemaVersionID {
	case 3:
		item := walItemSchema2{}
		err := binary.Read(r, binary.LittleEndian, &item)
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
		err = binary.Read(r, binary.LittleEndian, &line)
		if err != nil {
			return nil, err
		}
		el.Line = string(line)

		if item.NoteExists {
			el.Note = &[]byte{}
		}
		if item.NoteLength > 0 {
			note := make([]byte, item.NoteLength)
			err = binary.Read(r, binary.LittleEndian, &note)
			if err != nil {
				return nil, err
			}
			el.Note = &note
		}
	//case 4:
	//    if err := dec.Decode(&el); err != nil {
	//        return &el, err
	//    }
	default:
		return nil, errors.New("unrecognised wal schema version")
	}
	return &el, nil
}

func buildFromFile(raw io.Reader) (*[]EventLog, error) {
	var el []EventLog
	var walSchemaVersionID uint16
	err := binary.Read(raw, binary.LittleEndian, &walSchemaVersionID)
	if err != nil {
		if err == io.EOF {
			return &el, nil
		}
		return &el, err
	}

	// Versions >=3 of the wal schema is gzipped after the first 2 bytes. Therefore, unzip those bytes
	// prior to passing it to the loop below
	r, w := io.Pipe()
	errChan := make(chan error, 1)
	go func() {
		defer w.Close()
		switch walSchemaVersionID {
		case 3:
			fallthrough
		case 4:
			zr, err := gzip.NewReader(raw)
			if err != nil {
				errChan <- err
			}
			defer zr.Close()
			if _, err := io.Copy(w, zr); err != nil {
				errChan <- err
			}
		default:
			if _, err := io.Copy(w, raw); err != nil {
				errChan <- err
			}
		}
		errChan <- nil
	}()

	// This decoder is only relevant for wals with the most recent schema version (4 presently)
	if walSchemaVersionID <= 3 {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					return &el, err
				}
			default:
				e, err := getNextEventLogFromWalFile(r, walSchemaVersionID)
				if err != nil {
					switch err {
					case io.EOF:
						return &el, nil
					case io.ErrUnexpectedEOF:
						// Given the distributed concurrent nature of this app, we sometimes pick up partially
						// uploaded files which will fail, but may well be complete later on, therefore just
						// return for now and attempt again later
						// TODO implement a decent retry mech here
						return &el, nil
					default:
						return &el, err
					}
				}
				el = append(el, *e)
			}
		}
	} else if walSchemaVersionID == 4 {
		dec := gob.NewDecoder(r)
		if err := dec.Decode(&el); err != nil {
			//return &el, err
			// TODO
			// TODO
			// TODO
			// TODO
			// TODO remove this!!
			// I introduced a buggy version 4, but don't need to worry about the contents of the files
			// if I return err == nil, it'll assume the file was read and empty, and then gather will
			// handle the deletion
			return &el, nil
		}
		if err := <-errChan; err != nil {
			return &el, err
		}
	}

	return &el, err
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
	if len(*wal1) == 0 && len(*wal2) == 0 {
		return &[]EventLog{}
	} else if len(*wal1) == 0 {
		return wal2
	} else if len(*wal2) == 0 {
		return wal1
	}

	// Pre-allocate a slice with the maximum possible items (sum of both lens). Although under many circumstances, it's
	// unlikely we'll fill the capacity, it's far more optimal than each separate append re-allocating to a new slice.
	mergedEl := make([]EventLog, 0, len(*wal1)+len(*wal2))

	// Before merging, check to see that the the most recent from one wal isn't older than the oldest from another.
	// If that is the case, append the newer to the older and return.
	// We append to the newly allocated mergedEl twice, as we can guarantee that the underlying capacity will be enough
	// (so no further allocations are needed)
	if checkEquality((*wal1)[0], (*wal2)[len(*wal2)-1]) == rightEventOlder {
		mergedEl = append(mergedEl, *wal2...)
		mergedEl = append(mergedEl, *wal1...)
		return &mergedEl
	} else if checkEquality((*wal2)[0], (*wal1)[len(*wal1)-1]) == rightEventOlder {
		mergedEl = append(mergedEl, *wal1...)
		mergedEl = append(mergedEl, *wal2...)
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

func areListItemsEqual(a *ListItem, b *ListItem, checkPointers bool) bool {
	// checkPointers prevents recursion
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Line != b.Line ||
		a.Note != b.Note ||
		a.IsHidden != b.IsHidden ||
		a.originUUID != b.originUUID ||
		a.creationTime != b.creationTime {
		return false
	}
	if checkPointers {
		if !areListItemsEqual(a.child, b.child, false) ||
			!areListItemsEqual(a.parent, b.parent, false) ||
			!areListItemsEqual(a.matchChild, b.matchChild, false) ||
			!areListItemsEqual(a.matchParent, b.matchParent, false) {
			return false
		}
	}
	return true
}

func checkListItemPtrs(listItem *ListItem, matchItems []ListItem) error {
	if listItem == nil {
		return nil
	}

	if listItem.child != nil {
		return errors.New("list integrity error: root has a child pointer")
	}

	i := 0
	processedItems := make(map[string]struct{})
	for listItem.parent != nil {
		if !areListItemsEqual(listItem, &matchItems[i], false) {
			return fmt.Errorf("list integrity error: listItem %s does not match the expected position in the match list", listItem.Key())
		}
		if _, exists := processedItems[listItem.Key()]; exists {
			return fmt.Errorf("list integrity error: listItem %s has appeared twice", listItem.Key())
		}
		processedItems[listItem.Key()] = struct{}{}
		listItem = listItem.parent
		i++
	}

	return nil
}

// listsAreEquivalent traverses both test generated list item linked lists (from full and compacted wals) to
// check for equality. It returns `true` if they they are identical lists, or `false` otherwise.
// This is primarily used as a temporary measure to check the correctness of the compaction algo
func listsAreEquivalent(ptrA *ListItem, ptrB *ListItem) bool {
	// Return false if only one is nil
	if (ptrA != nil && ptrB == nil) || (ptrA == nil && ptrB != nil) {
		return false
	}

	// Check root equality
	if !areListItemsEqual(ptrA, ptrB, true) {
		return false
	}

	// Return true if both are nil (areListItemsEqual returns true if both nil so only check one)
	if ptrA == nil {
		return true
	}

	// Iterate over both ll's together and check equality of each item. Return `false` as soon as a pair
	// don't match, or one list is a different length to another
	for ptrA.parent != nil && ptrB.parent != nil {
		ptrA = ptrA.parent
		ptrB = ptrB.parent
		if !areListItemsEqual(ptrA, ptrB, true) {
			return false
		}
	}

	return areListItemsEqual(ptrA, ptrB, true)
}

// NOTE: not in use - debug function
func writePlainWalToFile(wal []EventLog) {
	f, err := os.Create(fmt.Sprintf("debug_%d", time.Now().UnixNano()))
	if err != nil {
		fmt.Println(err)
		f.Close()
		return
	}
	defer f.Close()

	for _, e := range wal {
		fmt.Fprintln(f, e)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func checkWalIntegrity(wal *[]EventLog) (*ListItem, *[]ListItem, error) {
	// Generate a test repo and use it to generate a match set, then inspect the health
	// of said match set.
	testRepo := DBListRepo{
		log:             &[]EventLog{},
		listItemTracker: make(map[string]*ListItem),
	}

	// Use the Replay function to generate the linked lists
	if err := testRepo.Replay(wal); err != nil {
		return nil, nil, err
	}

	// This check traverses from the root node to the last parent and checks the state of the pointer
	// relationships between both. There have previously been edge case wal merge/compaction bugs which resulted
	// in MoveUp events targeting a child, who's child was the original item to be moved (a cyclic pointer bug).
	// This has since been fixed, but to catch other potential cases, we run this check.
	testMatchItems, _, err := testRepo.Match([][]rune{}, true, "", 0, 0)
	if err != nil {
		return nil, nil, errors.New("failed to generate match items for list integrity check")
	}

	if err := checkListItemPtrs(testRepo.Root, testMatchItems); err != nil {
		return nil, &testMatchItems, err
	}

	return testRepo.Root, &testMatchItems, nil
}

// recoverWal is responsible for recovering Wals that are very broken, in a variety of ways.
// It collects as much metadata about each item as it can, from both the existing broken wal and the returned
// match set, and then uses it to rebuild a fresh wal, maintaining as much of the original state as possible -
// specifically with regards to DTs and ordering.
func recoverWal(wal *[]EventLog, matches *[]ListItem) *[]EventLog {
	acknowledgedItems := make(map[string]ListItem)
	listOrder := []string{}

	// Iterate over the match items and add all to the map
	for _, item := range *matches {
		key := item.Key()
		if _, exists := acknowledgedItems[key]; !exists {
			listOrder = append(listOrder, key)
		}
		acknowledgedItems[key] = item
	}

	// Iterate over the wal and collect data, updating the items in the map as we go.
	// We update metadata based on Updates only (Move*s are unimportant here). We honour deletes, and will remove
	// then from the map (although any subsequent updates will put them back in)
	// This cleanup will also dedup Add events if there are cases with duplicates.
	for _, e := range *wal {
		key, _ := e.getKeys()
		item, exists := acknowledgedItems[key]
		switch e.EventType {
		case AddEvent:
			fallthrough
		case UpdateEvent:
			if !exists {
				item = ListItem{
					Line:         e.Line,
					Note:         e.Note,
					originUUID:   e.UUID,
					creationTime: e.ListItemCreationTime,
				}
				if key != item.Key() {
					log.Fatal("ListItem key mismatch during recovery")
				}
			} else {
				if e.EventType == AddEvent {
					item.Line = e.Line
					item.Note = e.Note
				} else {
					// Updates handle Line and Note mutations separately
					if e.Note != nil {
						item.Note = e.Note
					} else {
						item.Line = e.Line
					}
				}
			}
			acknowledgedItems[key] = item
		case HideEvent:
			if exists {
				item.IsHidden = true
				acknowledgedItems[key] = item
			}
		case ShowEvent:
			if exists {
				item.IsHidden = false
				acknowledgedItems[key] = item
			}
		case DeleteEvent:
			delete(acknowledgedItems, key)
		}
	}

	// Now, iterate over the ordered list of item keys in reverse order, and pull the item from the map,
	// generating an AddEvent for each.
	newWal := []EventLog{}
	for i := len(listOrder) - 1; i >= 0; i-- {
		item := acknowledgedItems[listOrder[i]]
		el := EventLog{
			EventType:            AddEvent,
			UUID:                 item.originUUID,
			UnixNanoTime:         item.creationTime, // Use original creation time
			ListItemCreationTime: item.creationTime,
			Line:                 item.Line,
			Note:                 item.Note,
		}
		newWal = append(newWal, el)

		if item.IsHidden {
			el := EventLog{
				EventType:            HideEvent,
				UUID:                 item.originUUID,
				UnixNanoTime:         time.Now().UnixNano(),
				ListItemCreationTime: item.creationTime,
			}
			newWal = append(newWal, el)
		}
	}

	return &newWal
}

func compact(wal *[]EventLog) (*[]EventLog, error) {
	// Check the integrity of the incoming full wal prior to compaction.
	// If broken in some way, call the recovery function.
	testRootA, matchItemsA, err := checkWalIntegrity(wal)
	if err != nil {
		// If we get here, shit's on fire. This function is the equivalent of the fire brigade.
		wal = recoverWal(wal, matchItemsA)
		testRootA, matchItemsA, err = checkWalIntegrity(wal)
		if err != nil {
			log.Fatal("wal recovery failed!")
		}

		return wal, errWalIntregrity
	}

	// Traverse from most recent to most distant logs. Omit events in the following scenarios:
	// NOTE delete event purging is currently disabled
	// - Delete events, and any events preceding a DeleteEvent
	// - Update events in the following circumstances
	//   - Any UpdateEvent with a Note preceding the most recent UpdateEvent with a Note
	//   - Same without a Note
	//
	// Opting to store all Move* events to maintain the most consistent ordering of the output linked list.
	// e.g. it'll attempt to apply oldest -> newest Move*s until the target pointers don't exist.
	//
	// We need to maintain the first of two types of Update events (as per above, separate Line and Note),
	// so generate a separate set for each to tell us if each has occurred
	updateWithNote := make(map[string]struct{})
	updateWithLine := make(map[string]struct{})

	compactedWal := []EventLog{}
	for i := len(*wal) - 1; i >= 0; i-- {
		e := (*wal)[i]
		key, _ := e.getKeys()

		// TODO figure out how to reintegrate full purge of deleted events, whilst guaranteeing consistent
		// state of ListItems. OR purge everything older than X days, so ordering doesn't matter cos users
		// won't see it??
		// Add DeleteEvents straight to the purge set, if there's not any newer update events
		// NOTE it's important that we only continue if ONE OF BOTH UPDATE TYPES IS ALREADY PROCESSED
		//if e.EventType == DeleteEvent {
		//    if _, noteExists := updateWithNote[key]; !noteExists {
		//        if _, lineExists := updateWithLine[key]; !lineExists {
		//            keysToPurge[key] = struct{}{}
		//            continue
		//        }
		//    }
		//}

		if e.EventType == UpdateEvent {
			//Check to see if the UpdateEvent alternative event has occurred
			// Nil `Note` signifies `Line` update
			if e.Note != nil {
				if _, exists := updateWithNote[key]; exists {
					continue
				}
				updateWithNote[key] = struct{}{}
			} else {
				if _, exists := updateWithLine[key]; exists {
					continue
				}
				updateWithLine[key] = struct{}{}
			}
		}

		compactedWal = append(compactedWal, e)
	}
	// Reverse
	for i, j := 0, len(compactedWal)-1; i < j; i, j = i+1, j-1 {
		compactedWal[i], compactedWal[j] = compactedWal[j], compactedWal[i]
	}

	// TODO remove this once confidence with compact is there!
	// This is a circuit breaker which will blow up if compact generates inconsistent results
	testRootB, _, err := checkWalIntegrity(&compactedWal)
	if err != nil {
		log.Fatalf("`compact` caused wal to lose integrity: %s", err)
	}

	if !listsAreEquivalent(testRootA, testRootB) {
		log.Fatal("`compact` generated inconsistent results and things blew up!")
	}
	return &compactedWal, nil
}

// generatePlainTextFile takes the current matchset, and writes the lines separately to a
// local file. Notes are ignored.
func (r *DBListRepo) generatePlainTextFile(matchItems []ListItem) error {
	curWd, err := os.Getwd()
	if err != nil {
		return err
	}
	// Will be in the form `{currentDirectory}/export_1624785401.txt`
	fileName := path.Join(curWd, fmt.Sprintf(exportFilePattern, time.Now().Unix()))
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for _, i := range matchItems {
		if _, err := f.Write([]byte(fmt.Sprintf("%s\n", i.Line))); err != nil {
			return err
		}
	}
	return nil
}

// This function is currently unused
func (r *DBListRepo) generatePartialView(matchItems []ListItem) error {
	wal := []EventLog{}
	//now := time.Now().AddDate(-1, 0, 0).UnixNano()
	now := int64(1) // TODO remove this - it's to ensure consistency to enable file diffs

	// Iterate from oldest to youngest
	for i := len(matchItems) - 1; i >= 0; i-- {
		item := matchItems[i]
		el := EventLog{
			UUID:                       item.originUUID,
			TargetUUID:                 0,
			ListItemCreationTime:       item.creationTime,
			TargetListItemCreationTime: 0,
			UnixNanoTime:               now,
			EventType:                  AddEvent,
			Line:                       item.Line,
			Note:                       item.Note,
		}
		wal = append(wal, el)
		now++

		if item.IsHidden {
			el.EventType = HideEvent
			el.UnixNanoTime = now
			wal = append(wal, el)
			now++
		}
	}

	b := buildByteWal(&wal)
	//viewName := fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), viewFilePattern), time.Now().UnixNano())
	viewName := fmt.Sprintf(viewFilePattern, time.Now().UnixNano())
	r.LocalWalFile.Flush(b, viewName)
	log.Fatalf("N list generated events: %d", len(wal))
	return nil
}

func (r *DBListRepo) setProcessedPartialWals(fileName string) {
	r.processedPartialWalsLock.Lock()
	defer r.processedPartialWalsLock.Unlock()
	r.processedPartialWals[fileName] = struct{}{}
}

func (r *DBListRepo) isPartialWalProcessed(fileName string) bool {
	r.processedPartialWalsLock.Lock()
	defer r.processedPartialWalsLock.Unlock()
	_, exists := r.processedPartialWals[fileName]
	return exists
}

func (r *DBListRepo) pull(walFiles []WalFile) (*[]EventLog, error) {
	//log.Print("Pulling...")
	// To protect against excessive memory usage, and to keep computational complexity consistent and
	// predictable, we now synchronously iterate over each WalFile, generate an io.Pipe into which we
	// read the files, and then consume these in the `buildFromFile` function
	// TODO rename buildFromFile
	newMergedWal := []EventLog{}
	for _, wf := range walFiles {
		var fileNames []string
		var err error
		filePathPattern := path.Join(wf.GetRoot(), walFilePattern)
		fileNames, err = wf.GetMatchingWals(fmt.Sprintf(filePathPattern, "*"))
		if err != nil {
			log.Fatal(err)
		}

		for _, fileName := range fileNames {
			if !r.isPartialWalProcessed(fileName) {
				pr, pw := io.Pipe()
				go func() {
					defer pw.Close()
					if err := wf.GetWalBytes(pw, fileName); err != nil {
						// TODO handle
						//log.Fatal(err)
					}
				}()

				// Add to the processed cache
				// TODO this be done separately after fully merging the wals in case of failure??
				// TODO this is done synchronously, we no longer need this lock
				r.setProcessedPartialWals(fileName)

				// Build new wals
				newWfWal, err := buildFromFile(pr)
				if err != nil {
					// Ignore incompatible files
					continue
				}

				for _, ev := range *newWfWal {
					key, _ := ev.getKeys()
					wf.SetProcessedEvent(key)
				}

				newMergedWal = *(merge(&newMergedWal, newWfWal))
			}
		}
	}

	return &newMergedWal, nil
}

// gather up all WALs in the WalFile matching the local UUID into a single new Wal, and attempt
// to delete the old ones
func (r *DBListRepo) gather(walFiles []WalFile, forceGather bool) (*[]EventLog, error) {
	//log.Print("Gathering...")
	// TODO separate IO/CPU bound ops to optimise
	fullMergedWal := []EventLog{}
	for _, wf := range walFiles {
		// Handle ALL wals
		filePathPattern := path.Join(wf.GetRoot(), walFilePattern)
		originFiles, err := wf.GetMatchingWals(fmt.Sprintf(filePathPattern, "*"))
		if err != nil {
			log.Fatal(err)
		}

		// If there's only 1 file, there's no point gathering them, so skip
		// We need to let it pass for the 0 case (e.g. fresh roots with no existing wals)
		if len(originFiles) == 1 && !forceGather {
			// We still want to process other walFiles, so continue
			continue
		}

		// Gather origin files
		mergedWal := []EventLog{}
		filesToDelete := []string{}
		for _, fileName := range originFiles {
			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				if err := wf.GetWalBytes(pw, fileName); err != nil {
					// TODO handle
					//log.Fatal(err)
				}
			}()

			// Build new wals
			wal, err := buildFromFile(pr)
			if err != nil {
				// Ignore incompatible files
				continue
			}

			// Only delete files which were successfully pulled
			filesToDelete = append(filesToDelete, fileName)
			mergedWal = *(merge(&mergedWal, wal))
		}

		// Merge with entire local log
		mergedWal = *(merge(&mergedWal, r.log))

		// Compact
		compactedWal, err := compact(&mergedWal)
		if err != nil {
			if err == errWalIntregrity {
				// This isn't nice, but saves a larger refactor for what might be entirely unused emergency
				// logic: we need to purge the log on the listRepo, otherwise on `gather` -> `Replay`, the
				// broken log will just be merged back in. We want to remove it entirely.
				r.log = &[]EventLog{}
			} else {
				return nil, err
			}
		}
		mergedWal = *compactedWal

		// Flush the gathered Wal
		if err := r.push(&mergedWal, wf, ""); err != nil {
			log.Fatal(err)
		}

		// Merge into the full wal (which will be returned at the end of the function)
		fullMergedWal = *(merge(&fullMergedWal, &mergedWal))

		// Schedule a delete on the files
		wf.RemoveWals(filesToDelete)
	}

	return &fullMergedWal, nil
}

func buildByteWal(el *[]EventLog) *bytes.Buffer {
	var compressedBuf bytes.Buffer

	// Write the schema ID
	err := binary.Write(&compressedBuf, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		log.Fatal(err)
	}

	//pr, pw := io.Pipe()
	var buf bytes.Buffer
	//enc := gob.NewEncoder(pw)

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(el); err != nil {
		log.Fatal(err) // TODO
	}

	// Then write in the compressed bytes
	zw := gzip.NewWriter(&compressedBuf)
	//if _, err := io.Copy(zw, pr); err != nil {
	//    log.Fatal(err) // TODO
	//}
	_, err = zw.Write(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		log.Fatal(err) // TODO
	}

	return &compressedBuf
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

func (r *DBListRepo) push(el *[]EventLog, wf WalFile, randomUUID string) error {
	// Apply any filtering based on Push match configuration
	el = getMatchedWal(el, wf)

	// Return for empty wals
	if len(*el) == 0 {
		return nil
	}

	b := buildByteWal(el)

	if randomUUID == "" {
		randomUUID = fmt.Sprintf("%v%v", r.uuid, generateUUID())
	}
	// Add it straight to the cache to avoid processing it in the future
	// This needs to be done PRIOR to flushing to avoid race conditions
	// (as pull is done in a separate thread of control, and therefore we might try
	// and pull our own pushed wal)
	r.setProcessedPartialWals(randomUUID)
	if err := wf.Flush(b, randomUUID); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (r *DBListRepo) flushPartialWals(el []EventLog, sync bool) {
	//log.Print("Flushing...")
	if len(el) > 0 {
		randomUUID := fmt.Sprintf("%v%v", r.uuid, generateUUID())
		for _, wf := range r.allWalFiles() {
			if wf.GetMode() == ModeSync {
				if sync {
					// TODO Use waitgroups
					r.push(&el, wf, randomUUID)
				} else {
					go func(wf WalFile) { r.push(&el, wf, randomUUID) }(wf)
				}
			}
		}
	}
}

// TODO keeping this implementation around for reference. It was a (working) attempt at putting
// all CPU heavy stuff in a single loop, but for some reason it performed a _lot_ worst in the wasm
// webapp. However, leaving it here cos it might provide the base of a next attempt.
//func (r *DBListRepo) startSyncNew(walChan chan *[]EventLog) error {
//// We want to trigger a web sync as soon as the web connection has been established
////webSyncTriggerChan := make(chan time.Time)
//fileSyncTriggerChan := make(chan time.Time)
//webConnectionRefreshChan := make(chan time.Time)

//// Trigger initial file sync and web connection creation
//go func() {
//    fileSyncTriggerChan <- time.Time{}
//    webConnectionRefreshChan <- time.Time{}
//}()

//webConnectionRefreshTicker := time.NewTicker(time.Minute * 10)

//// Schedule ongoing wal file syncs
//go func() {
//    for {
//        select {
//        //case t := <-r.webSyncTicker.C:
//        //    webSyncTriggerChan <- t
//        case t := <-r.fileSyncTicker.C:
//            fileSyncTriggerChan <- t
//        case t := <-webConnectionRefreshTicker.C:
//            webConnectionRefreshChan <- t
//        }
//    }
//}()

//// Instantiate the ephemeral event log here. We aggregate websocket push events and then periodically
//// flush them (and set a new empty log) in the `push` step in a separate thread below.
//var tempEventLog []EventLog

//websocketConsumeChan := make(chan *[]EventLog)
//websocketCursorConsumeChan := make(chan cursorMoveEvent)

//var webConnectionRefreshCtx context.Context
//var webConnectionRefreshCancelFn context.CancelFunc

//// Handle websocket push events in the same goroutine
//go func() {
//    for {
//        select {
//        case <-webConnectionRefreshChan:
//            if r.web != nil {
//                // TODO what happens if this hasn't been set? E.g. initial iteration?
//                // Cancel the context from the previous iteration (if there was one) to exit
//                // the websocket consumption goroutine
//                if webConnectionRefreshCancelFn != nil {
//                    webConnectionRefreshCancelFn()
//                }

//                // Close off old websocket connection
//                // Nil check because initial instantiation also occurs async in this loop (previous it was sync on startup)
//                if r.web.wsConn != nil {
//                    r.web.wsConn.Close(websocket.StatusNormalClosure, "")
//                }
//                // Start new one
//                err := r.registerWeb()
//                if err != nil {
//                    log.Print(err)
//                    os.Exit(0)
//                }
//                // Trigger web walfile sync (mostly relevant on initial start)
//                //webSyncTriggerChan <- time.Time{}

//                // To avoid deadlocks between the web refresh and blocking consumeWebsocket reads, we explicitly
//                // define the context which is manually cancelled on each timed iteration of the web refresh
//                webConnectionRefreshCtx, webConnectionRefreshCancelFn = context.WithCancel(context.Background())
//                go func() {
//                    for {
//                        if r.web.wsConn == nil {
//                            // Return to wait on blocking web refresh, to prevent infinite loop
//                            return
//                        }
//                        err := r.web.consumeWebsocket(webConnectionRefreshCtx, websocketConsumeChan, websocketCursorConsumeChan)
//                        if err != nil {
//                            return
//                        }
//                    }
//                }()
//            }
//        case e := <-websocketConsumeChan:
//            walChan <- e
//        case e := <-websocketCursorConsumeChan:
//            r.remoteCursorMoveChan <- e
//        // The events chan contains single events. We want to aggregate them between intervals
//        // and then emit them in batches, for great efficiency gains.
//        case e := <-r.eventsChan:
//            // Write in real time to the websocket, if present
//            if r.web != nil {
//                for _, wf := range r.webWalFiles {
//                    // TODO uuid is a hack to work around the GetUUID stubs I have in place atm:
//                    if wf.GetMode() == ModeSync && wf.GetUUID() != "" {
//                        matchedEventLog := getMatchedWal(&[]EventLog{e}, wf)
//                        if len(*matchedEventLog) > 0 {
//                            // There are only single events, so get the zero index
//                            b := buildByteWal(&[]EventLog{(*matchedEventLog)[0]})
//                            b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())
//                            m := websocketMessage{
//                                Action: "wal",
//                                UUID:   wf.GetUUID(),
//                                Wal:    b64Wal,
//                            }
//                            r.web.pushWebsocket(m)
//                        }
//                    }
//                }
//            }
//            // Add to an ephemeral log
//            tempEventLog = append(tempEventLog, e)
//        // Emit local cursor move events
//        case e := <-r.localCursorMoveChan:
//            // TODO dedup webWalFile ModeSync loop
//            if r.web != nil && r.web.wsConn != nil {
//                for _, wf := range r.webWalFiles {
//                    if wf.GetMode() == ModeSync && wf.GetUUID() != "" {
//                        m := websocketMessage{
//                            Action:       "position",
//                            UUID:         wf.GetUUID(),
//                            Key:          e.listItemKey,
//                            UnixNanoTime: e.unixNanoTime,
//                        }
//                        r.web.pushWebsocket(m)
//                    }
//                }
//            }
//        }
//    }
//}()

//// Run an initial blocking load from the local walfile (and put onto channel for immediate
//// processing in main loop). Also push to all walFiles (this will get missed in async loop below
//// due to cache, so small amount of duplicated code required).
//var localEl *[]EventLog
//var err error
//if localEl, err = r.pull([]WalFile{r.LocalWalFile}); err != nil {
//    return err
//}
//go func() { walChan <- localEl }()

//// Schedule push to all non-local walFiles
//// This is required for flushing new files that have been manually dropped into local root
//// Because we `gather` on close, for most scenarios, we only need to do this if there are > 1 wal files locally.
//// NOTE: this obviously won't work when dropping a single wal file into a fresh root directory, but this is
//// heading into edge cases of edge cases so won't worry about it for now
//localFileNames, err := r.LocalWalFile.GetMatchingWals(fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), walFilePattern), "*"))
//if err != nil {
//    log.Fatal(err)
//}
//if len(localFileNames) > 1 {
//    for _, wf := range r.allWalFiles() {
//        if wf != r.LocalWalFile {
//            go func(wf WalFile) { r.push(localEl, wf, "") }(wf)
//        }
//    }
//}

//// Main sync event loop
////fileWalFiles := append(r.s3WalFiles, r.LocalWalFile)

//// Main "sync" loop. We handle all of the computationally heavy `push`, `pull` and `gather`
//// events in the same thread to try and maintain some explicit control over the computationally
//// heavy operations.
//go func() {
//    i := 0
//    for {
//        var el *[]EventLog
//        select {
//        // Pull tasks
//        //case <-webSyncTriggerChan:
//        //    if el, err = r.pull(r.webWalFiles); err != nil {
//        //        log.Fatal(err)
//        //    }
//        //    walChan <- el
//        //case <-fileSyncTriggerChan:
//        //    if el, err = r.pull(fileWalFiles); err != nil {
//        //        log.Fatal(err)
//        //    }
//        //    walChan <- el
//        case <-fileSyncTriggerChan:
//            if i == 3 {
//                if el, err = r.gather(r.allWalFiles(), false); err != nil {
//                    log.Fatal(err)
//                }
//                i = 0
//            } else {
//                if el, err = r.pull(r.allWalFiles()); err != nil {
//                    log.Fatal(err)
//                }
//            }

//            // On ticks, Flush what we've aggregated to all walfiles, and then reset the
//            // ephemeral log. If empty, skip.
//            // We pass by reference, so we'll need to create a copy prior to sending to `push`
//            // otherwise the underlying el may change before `push` has a chance to process it
//            // If we start passing by value later on, this won't be required (as go will pass
//            // copies by default, I think).
//            elCopy := tempEventLog
//            r.flushPartialWals(elCopy, false)
//            tempEventLog = []EventLog{}
//            //}
//            walChan <- el
//        //case <-r.gatherTicker.C:
//        //if el, err = r.gather(r.allWalFiles(), false); err != nil {
//        //    log.Fatal(err)
//        //}
//        //walChan <- el

//        // Push tasks
//        //case <-r.pushTicker.C:
//        //    // On ticks, Flush what we've aggregated to all walfiles, and then reset the
//        //    // ephemeral log. If empty, skip.
//        //    // We pass by reference, so we'll need to create a copy prior to sending to `push`
//        //    // otherwise the underlying el may change before `push` has a chance to process it
//        //    // If we start passing by value later on, this won't be required (as go will pass
//        //    // copies by default, I think).
//        //    elCopy := tempEventLog
//        //    r.flushPartialWals(elCopy, false)
//        //    tempEventLog = []EventLog{}
//        case <-r.stop:
//            r.flushPartialWals(tempEventLog, true)
//            r.stop <- struct{}{}
//        }
//    }
//}()
//return nil
//}

func (r *DBListRepo) startSync(walChan chan *[]EventLog) error {
	// Create mutex to protect against dropped websocket events when refreshing web connections
	webRefreshMut := sync.RWMutex{}

	syncTriggerChan := make(chan time.Time, 1)
	pushTriggerChan := make(chan time.Time, 1)

	// We want to trigger a web sync as soon as the web connection has been established
	scheduleSync := func() {
		// Attempt to put onto the channel, else pass
		select {
		case syncTriggerChan <- time.Time{}:
		default:
		}
	}
	schedulePush := func() {
		// Unlike schedule above (which can be called from numerous locations), we want to block
		// when attempting to put a time on the channel
		pushTriggerChan <- time.Time{}
	}

	// Prioritise async web start-up to minimise wait time before websocket instantiation
	if r.web != nil {
		// Create a loop responsible for periodic refreshing of web connections and web walfiles.
		go func() {
			for {
				func() {
					webRefreshMut.Lock()
					defer webRefreshMut.Unlock()
					// Close off old websocket connection
					// Nil check because initial instantiation also occurs async in this loop (previous it was sync on startup)
					if r.web.wsConn != nil {
						r.web.wsConn.Close(websocket.StatusNormalClosure, "")
					}
					// Start new one
					err := r.registerWeb()
					if err != nil {
						log.Print(err)
						os.Exit(0)
					}
					// Trigger web walfile sync (mostly relevant on initial start)
					scheduleSync()
				}()

				// To avoid deadlocks between the web refresh and blocking consumeWebsocket reads, we explicitly
				// define the context which is manually cancelled on each timed iteration of the web refresh
				ctx, cancel := context.WithCancel(context.Background())

				go func() {
					for {
						if r.web.wsConn == nil {
							// Return to wait on blocking web refresh, to prevent infinite loop
							return
						}
						err := func() error {
							//webRefreshMut.RLock()
							//defer webRefreshMut.RUnlock()
							err := r.web.consumeWebsocket(ctx, walChan, r.remoteCursorMoveChan)
							if err != nil {
								return err
							}
							return nil
						}()
						if err != nil {
							return
						}
					}
				}()

				// The `Sleep` has to be at the end to allow an initial iteration to occur immediately on startup
				time.Sleep(webRefreshInterval)
				cancel()
			}
		}()

		// Create a loop to deal with any collaborator cursor move events
		go func() {
			for {
				e := <-r.localCursorMoveChan
				// TODO dedup webWalFile ModeSync loop
				if r.web.wsConn != nil {
					for _, wf := range r.webWalFiles {
						if wf.GetMode() == ModeSync && wf.GetUUID() != "" {
							func() {
								m := websocketMessage{
									Action:       "position",
									UUID:         wf.GetUUID(),
									Key:          e.listItemKey,
									UnixNanoTime: e.unixNanoTime,
								}
								webRefreshMut.RLock()
								defer webRefreshMut.RUnlock()
								r.web.pushWebsocket(m)
							}()
						}
					}
				} else {
					time.Sleep(5 * time.Second)
				}
			}
		}()
	} else {
		// We want to wait for the web connection to be established before triggering an intial sync,
		// otherwise we end up waiting the full interval (after local sync) before it pulls down any
		//remote changes.
		// This `else` just covers the case where web is not active and therefore no sync is scheduled
		// in the case above
		go func() {
			scheduleSync()
		}()
	}

	// Start the cycle for pushes
	go func() {
		schedulePush()
	}()

	// Run an initial blocking load from the local walfile (and put onto channel for immediate
	// processing in main loop). Also push to all walFiles (this will get missed in async loop below
	// due to cache, so small amount of duplicated code required).
	var localEl *[]EventLog
	var err error
	if localEl, err = r.pull([]WalFile{r.LocalWalFile}); err != nil {
		return err
	}
	go func() { walChan <- localEl }()

	// Schedule push to all non-local walFiles
	// This is required for flushing new files that have been manually dropped into local root
	// Because we `gather` on close, for most scenarios, we only need to do this if there are > 1 wal files locally.
	// NOTE: this obviously won't work when dropping a single wal file into a fresh root directory, but this is
	// heading into edge cases of edge cases so won't worry about it for now
	localFileNames, err := r.LocalWalFile.GetMatchingWals(fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), walFilePattern), "*"))
	if err != nil {
		log.Fatal(err)
	}
	if len(localFileNames) > 1 {
		for _, wf := range r.allWalFiles() {
			if wf != r.LocalWalFile {
				go func(wf WalFile) { r.push(localEl, wf, "") }(wf)
			}
		}
	}

	// Main sync event loop
	go func() {
		i := 0
		for {
			var el *[]EventLog
			// Every fourth iteration is a `gather`
			select {
			case <-syncTriggerChan:
				if i < 3 {
					if el, err = r.pull(r.allWalFiles()); err != nil {
						log.Fatal(err)
					}
					i++
				} else {
					//case <-r.gatherTicker.C:
					if el, err = r.gather(r.allWalFiles(), false); err != nil {
						log.Fatal(err)
					}
					i = 0
				}
			}
			walChan <- el
			// Rather than relying on a ticker (which will trigger the next cycle if processing time is >= the interval)
			// we set a wait interval from the end of processing. This prevents a vicious circle which could leave the
			// program with it's CPU constantly tied up, which leads to performance degradation.
			// Instead, at the end of the processing cycle, we schedule a wait period after which the next event is put
			// onto the syncTriggerChan
			time.Sleep(time.Second * 30)
			scheduleSync()
		}
	}()

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
					for _, wf := range r.webWalFiles {
						// TODO uuid is a hack to work around the GetUUID stubs I have in place atm:
						if wf.GetMode() == ModeSync && wf.GetUUID() != "" {
							matchedEventLog := getMatchedWal(&[]EventLog{e}, wf)
							if len(*matchedEventLog) > 0 {
								// There are only single events, so get the zero index
								b := buildByteWal(&[]EventLog{(*matchedEventLog)[0]})
								b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())
								m := websocketMessage{
									Action: "wal",
									UUID:   wf.GetUUID(),
									Wal:    b64Wal,
								}
								func() {
									webRefreshMut.RLock()
									defer webRefreshMut.RUnlock()
									r.web.pushWebsocket(m)
								}()
							}
						}
					}
				}
				// Add to an ephemeral log
				el = append(el, e)
			//case <-r.pushTicker.C:
			case <-pushTriggerChan:
				// On ticks, Flush what we've aggregated to all walfiles, and then reset the
				// ephemeral log. If empty, skip.
				// We pass by reference, so we'll need to create a copy prior to sending to `push`
				// otherwise the underlying el may change before `push` has a chance to process it
				// If we start passing by value later on, this won't be required (as go will pass
				// copies by default, I think).
				elCopy := el
				r.flushPartialWals(elCopy, false)
				el = []EventLog{}
				go func() {
					time.Sleep(time.Second * 30)
					schedulePush()
				}()
			case <-r.stop:
				r.flushPartialWals(el, true)
				r.stop <- struct{}{}
			}
		}
	}()

	return nil
}

func (r *DBListRepo) finish() error {
	// Gather the local walfile
	// TODO this is a bit of a "catch-all" convenience hack which guarantees that *all* in mem logs
	// will always be persisted to disk on close. Ideally, we'd just flush to disk whenever we pulled
	// new logs from remotes, but for now, this will do.
	//r.gather([]WalFile{r.LocalWalFile}, true)

	// Stop tickers
	//r.webSyncTicker.Stop()
	//r.fileSyncTicker.Stop()
	//r.syncTicker.Stop()
	//r.pushTicker.Stop()
	//r.gatherTicker.Stop()

	// Flush all unpushed changes to non-local walfiles
	// TODO handle this more gracefully
	r.stop <- struct{}{}
	<-r.stop

	if r.web != nil && r.web.wsConn != nil {
		r.web.wsConn.Close(websocket.StatusNormalClosure, "")
	}
	return nil
}

// BuildWalFromPlainText accepts an io.Reader with line separated plain text, and generates a wal db file
// which is dumped in fzn root directory.
func BuildWalFromPlainText(wf WalFile, r io.Reader, isHidden bool) error {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	el := []EventLog{}

	// any random UUID is fine
	uuid := generateUUID()
	targetListItemCreationTime := int64(0)
	// we need to set a unique UnixNanoTime for each event log, so we take Now() and then increment
	// by one for each new log. This isn't a perfect solution given that the number of lines in the input
	// can be unbounded, and therefore theoretically we could end up generating events in the future,
	// but realistically, this is _highly_ unlikely to occur and I don't think it causes issues anyway.
	unixNanoTime := time.Now().UnixNano()
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		listItemCreationTime := unixNanoTime
		el = append(el, EventLog{
			UUID:                       uuid,
			TargetUUID:                 uuid,
			ListItemCreationTime:       listItemCreationTime,
			TargetListItemCreationTime: targetListItemCreationTime,
			UnixNanoTime:               unixNanoTime,
			EventType:                  AddEvent,
			Line:                       line,
		})
		unixNanoTime++

		if isHidden {
			el = append(el, EventLog{
				UUID:                       uuid,
				TargetUUID:                 uuid,
				ListItemCreationTime:       listItemCreationTime,
				TargetListItemCreationTime: targetListItemCreationTime,
				UnixNanoTime:               unixNanoTime,
				EventType:                  HideEvent,
			})
			unixNanoTime++
		}
		targetListItemCreationTime = listItemCreationTime
	}

	b := buildByteWal(&el)
	wf.Flush(b, fmt.Sprintf("%d", uuid))

	return nil
}
