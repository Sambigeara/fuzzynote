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
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 4

// sync intervals
const (
	pullIntervalSeconds = 5
	pushIntervalSeconds = 5
	gatherWaitDuration  = time.Second * time.Duration(15)
)

var EmailRegex = regexp.MustCompile("[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*")

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

type lineFriends struct {
	isProcessed bool
	offset      int
	emails      map[string]struct{}
}

type EventLog struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string // TODO This represents the raw (un-friends-processed) line
	Note                       *[]byte
	friends                    lineFriends
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
}

type LocalWalFile interface {
	Purge()

	WalFile
}

type LocalFileWalFile struct {
	rootDir            string
	processedEventLock *sync.Mutex
	processedEventMap  map[string]struct{}
}

func NewLocalFileWalFile(rootDir string) *LocalFileWalFile {
	return &LocalFileWalFile{
		rootDir:            rootDir,
		processedEventLock: &sync.Mutex{},
		processedEventMap:  make(map[string]struct{}),
	}
}

func (wf *LocalFileWalFile) Purge() {
	if err := os.RemoveAll(wf.rootDir); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

func (wf *LocalFileWalFile) GetUUID() string {
	return "local"
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

// https://go.dev/play/p/1kbFF8FR-V7
// enforces existence of surrounding boundary character
// match[0] = full match (inc boundaries)
// match[1] = `@email`
// match[2] = `email`
var lineFriendRegex = regexp.MustCompile("(?:^|\\s)(@([a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*))(?:$|\\s)")

func (r *DBListRepo) getFriendsFromLine(line string) map[string]struct{} {
	// (golang?) regex does return overlapping results, which we need in order to ensure space
	// or start/end email boundaries. Therefore we iterate over the line and match/replace any
	// email with (pre|app)ending spaces with a single space

	friends := map[string]struct{}{}

	if len(line) == 0 {
		return friends
	}

	replacedLine := line
	oldReplacedLine := ""
	for replacedLine != oldReplacedLine {
		oldReplacedLine = replacedLine
		if match := lineFriendRegex.FindStringSubmatch(replacedLine); match != nil {
			// If we want to check against the friends cache, we use the email only matching group
			s := match[2]
			friends[s] = struct{}{}
			replacedLine = strings.Replace(replacedLine, s, " ", 1)
		}
	}

	return friends
}

func (r *DBListRepo) getFriendFromConfigLine(line string) string {
	if len(line) == 0 {
		return ""
	}
	match := r.cfgFriendRegex.FindStringSubmatch(line)
	// First submatch is the email regex
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func (r *DBListRepo) repositionActiveFriends(e *EventLog) {
	// TODO
	// TODO
	// TODO
	// TODO
	// TODO WRITE TESTS!!!

	// Empty lines would only be present if the user is not logged in (it shouldn't reach this code path in that
	// circumstance either way, but JIC...)
	if len(e.Line) == 0 {
		return
	}

	if e.friends.isProcessed {
		return
	}

	// If any matched friends are within the current friends cache, cut from the Line and store in a slice
	r.friendsUpdateLock.RLock()
	defer r.friendsUpdateLock.RUnlock()

	// If this is a config line, we only want to hide the owner email, therefore manually set a single key friends map
	var friends map[string]struct{}
	if r.cfgFriendRegex.MatchString(e.Line) {
		friends = map[string]struct{}{
			r.email: struct{}{},
		}
	} else {
		friends = r.getFriendsFromLine(e.Line)
		if len(friends) == 0 {
			return
		}
	}

	friendsToReposition := []string{}
	newLine := e.Line
	for f := range friends {
		if _, isFriend := r.friends[f]; isFriend {
			friendsToReposition = append(friendsToReposition, fmt.Sprintf("@%s", f))
			// Each cut email address needs also remove (up to) one (pre|suf)fixed space, not more.
			newLine = strings.ReplaceAll(newLine, fmt.Sprintf(" @%s", f), "")
			newLine = strings.ReplaceAll(newLine, fmt.Sprintf("@%s ", f), "")
		}
	}

	var sb strings.Builder
	sb.WriteString(newLine)

	// Sort the emails, and then append a space separated string to the end of the Line
	sort.Strings(friendsToReposition)
	for _, f := range friendsToReposition {
		sb.WriteString(" ")
		sb.WriteString(f)
	}

	e.Line = sb.String()

	friendsString := fmt.Sprintf(" %s", strings.Join(friendsToReposition, " "))
	// We need to include `self` in the raw Line, as this will be distributed across all clients who also
	// need to collaborate back to the local client. However, we do _not_ want to include this email in
	// lineFriends.emails, as it's not relevant to the client's Friends() call
	ownerOmitted := make(map[string]struct{})
	for _, f := range friendsToReposition {
		if strings.TrimPrefix(f, "@") != r.email {
			ownerOmitted[f] = struct{}{}
		}
	}
	e.friends = lineFriends{
		isProcessed: true,
		offset:      len(e.Line) - len(friendsString),
		emails:      ownerOmitted,
	}
}

func (r *DBListRepo) generateFriendChangeEvents(e EventLog, item *ListItem) {
	// This method is responsible for detecting changes to the "friends" configuration in order to update
	// local state, and to emit events to the cloud API.
	friendToAdd := ""
	friendToRemove := ""

	var existingLine string
	if item != nil {
		existingLine = item.rawLine
	}

	before := r.getFriendFromConfigLine(existingLine)
	switch e.EventType {
	case AddEvent:
		fallthrough
	case UpdateEvent:
		after := r.getFriendFromConfigLine(e.Line)
		// If before != after, we know that we need to remove the previous entry, and add the new one.
		if before != after {
			friendToRemove = before
			friendToAdd = after
		}
	case DeleteEvent:
		friendToRemove = before
	}

	key, _ := e.getKeys()

	// We now iterate over each friend in the two slices and compare to the cached friends on DBListRepo.
	// If the friend doesn't exist, or the timestamp is more recent than the cached counterpart, we update.
	// We do this on a per-listItem basis to account for duplicate lines.
	func() {
		// We need both additions and deletions to be handled in an atomic fully blocking op, so the update events
		// aren't emitted with pending deletions still present
		r.friendsUpdateLock.Lock()
		defer r.friendsUpdateLock.Unlock()
		if friendToAdd != "" {
			// If the listItem specific friend exists, skip
			var friendItems map[string]int64
			var friendExists bool
			if friendItems, friendExists = r.friends[friendToAdd]; !friendExists {
				friendItems = make(map[string]int64)
				r.friends[friendToAdd] = friendItems
			}

			if dtLastChange, exists := friendItems[key]; !exists || e.UnixNanoTime > dtLastChange {
				r.friends[friendToAdd][key] = e.UnixNanoTime
				// TODO the consumer of the channel below will need to be responsible for adding the walfile locally
				r.AddWalFile(
					&WebWalFile{
						uuid:               friendToAdd,
						processedEventLock: &sync.Mutex{},
						processedEventMap:  make(map[string]struct{}),
						web:                r.web,
					},
					false,
				)
			}
		}
		//for email := range friendsToRemove {
		if friendToRemove != "" {
			// We only delete and emit the cloud event if the friend exists (which it always should tbf)
			// Although we ignore the delete if the event timestamp is older than the latest known cache state.
			if friendItems, friendExists := r.friends[friendToRemove]; friendExists {
				if dtLastChange, exists := friendItems[key]; exists && e.UnixNanoTime > dtLastChange {
					delete(r.friends[friendToRemove], key)
					if len(r.friends[friendToRemove]) == 0 {
						delete(r.friends, friendToRemove)
						r.DeleteWalFile(friendToRemove)
					}
				}
			}
		}
		if (friendToAdd != "" || friendToRemove != "") && r.friendsMostRecentChangeDT < e.UnixNanoTime {
			r.friendsMostRecentChangeDT = e.UnixNanoTime
		}
	}()
}

func (r *DBListRepo) processEventLog(root *ListItem, e *EventLog) (*ListItem, *ListItem, error) {
	if e == nil {
		return root, nil, nil
	}

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

	// At this point, if `web` is active, we inspect the Line in the event log (relevant for `Add` and `Update`)
	// for `@friends`, and if they're present and currently enabled (e.g. in the friends cache), cut them from any
	// central location in the line and append to the end. This is required for later public client APIs (e.g. we only
	// return a slice of the rawLine to clients via the Line() API).
	// This occurs here next to the generateFriendChangeEvents call, as the Line mutations need to act on the "current"
	// state of the friends cache, where "current" can indeed be historical, but current WRT the event log being processed.
	// This means that even if there are inconsistencies in events being transmitted to other clients, due to out-of-date
	// friends caches, distributed state will always be eventually consistent.
	// NOTE: ordering of generateFriendChangeEvents vs repositionActiveFriends is irrelevant, because we will never need to
	// extract friends from the config lines used to generate change events.
	if r.web != nil {
		r.repositionActiveFriends(e)
		r.generateFriendChangeEvents(*e, item)
	}

	var err error
	switch e.EventType {
	case AddEvent:
		if item != nil {
			// 21/11/21: There was a bug caused when a collaborator `Undo` was carried out on a collaborator origin
			// `Delete`, when the `Delete` was not picked up by the local. This resulted in an `Add` being carried out
			// with a duplicate ListItem key, which led to some F'd up behaviour in the match set. This catch covers
			// this case by doing a dumb "if Add on existing item, change to Update". We need to run two updates, as
			// Note and Line updates are individual operations.
			// TODO remove this when `Compact`/wal post-processing is smart enough to iron out these broken logs.
			if e.Note != nil {
				item, err = r.update(e.Line, e.friends, e.Note, item)
			}
			item, err = r.update(e.Line, e.friends, nil, item)
		} else {
			root, item, err = r.add(root, e.ListItemCreationTime, e.Line, e.friends, e.Note, targetItem, e.UUID)
			r.listItemTracker[key] = item
		}
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
			item, err = r.update(e.Line, e.friends, e.Note, item)
		} else {
			addEl := e
			addEl.EventType = AddEvent
			root, item, err = r.processEventLog(root, addEl)
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

func (r *DBListRepo) add(root *ListItem, creationTime int64, line string, friends lineFriends, note *[]byte, childItem *ListItem, uuid uuid) (*ListItem, *ListItem, error) {
	newItem := &ListItem{
		originUUID:   uuid,
		creationTime: creationTime,
		child:        childItem,
		rawLine:      line,
		Note:         note,
		friends:      friends,
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

func (r *DBListRepo) update(line string, friends lineFriends, note *[]byte, listItem *ListItem) (*ListItem, error) {
	// We currently separate Line and Note updates even though they use the same interface
	// This is to reduce wal size and also solves some race conditions for long held open
	// notes, etc
	if note != nil {
		listItem.Note = note
	} else {
		listItem.rawLine = line
		// listItem.friends.emails is a map, which we only ever want to OR with to aggregate (we can only add new emails,
		// not remove any, due to the processedEventMap mechanism elsewhere)
		mergedEmailMap := make(map[string]struct{})
		for e := range listItem.friends.emails {
			mergedEmailMap[e] = struct{}{}
		}
		for e := range friends.emails {
			mergedEmailMap[e] = struct{}{}
		}
		listItem.friends.isProcessed = friends.isProcessed
		listItem.friends.offset = friends.offset
		listItem.friends.emails = mergedEmailMap
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
	isHidden := item.IsHidden
	root, item, err = r.add(root, item.creationTime, item.rawLine, item.friends, item.Note, childItem, item.originUUID)
	if isHidden {
		r.setVisibility(item, false)
	}
	return root, item, err
}

func (r *DBListRepo) setVisibility(item *ListItem, isVisible bool) error {
	item.IsHidden = !isVisible
	return nil
}

// Replay updates listItems based on the current state of the local WAL logs. It generates or updates the linked list
// which is attached to DBListRepo.Root
func (r *DBListRepo) Replay(partialWal []EventLog) error {
	// No point merging with an empty partialWal
	if len(partialWal) == 0 {
		return nil
	}

	fullMerge := true
	// Establish whether or not the oldest event of the partialWal is newer than the most
	// recent of the existing (and therefore already processed) wal.
	// If all events in the partialWal are newer, we can avoid the full merge, and r.Root
	// reset, which is a significant optimisation.
	// This needs to be done pre-merge.
	if len(r.log) > 0 && checkEquality(r.log[len(r.log)-1], partialWal[0]) == leftEventOlder {
		fullMerge = false
	}

	// Merge with any new local events which may have occurred during sync
	r.log = merge(r.log, partialWal)

	var replayLog []EventLog
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

	for _, e := range replayLog {
		// We need to pass a fresh null root and leave the old r.Root intact for the function
		// caller logic
		root, _, _ = r.processEventLog(root, &e)
	}

	// Update processed wal event caches for all local walfiles, plus those based on the friends in each event
	// NOTE: this needs to occur after processEventLog, as that is where the r.friends cache is populated
	// on initial Replay.
	for _, e := range partialWal {
		key, _ := e.getKeys()
		if len(e.Line) > 0 {
			r.friendsUpdateLock.RLock()
			for f := range r.getFriendsFromLine(e.Line) {
				// TODO figure out why this no work when f == r.email
				if _, isFriend := r.friends[f]; isFriend && f != r.email {
					r.allWalFileMut.RLock()
					wf, _ := r.allWalFiles[f]
					r.allWalFileMut.RUnlock()
					wf.SetProcessedEvent(key)
				}
			}
			r.friendsUpdateLock.RUnlock()
		}
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

func buildFromFile(raw io.Reader) ([]EventLog, error) {
	var el []EventLog
	var walSchemaVersionID uint16
	err := binary.Read(raw, binary.LittleEndian, &walSchemaVersionID)
	if err != nil {
		if err == io.EOF {
			return el, nil
		}
		return el, err
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

	if walSchemaVersionID <= 3 {
		for {
			select {
			case err := <-errChan:
				if err != nil {
					return el, err
				}
			default:
				e, err := getNextEventLogFromWalFile(r, walSchemaVersionID)
				if err != nil {
					switch err {
					case io.EOF:
						return el, nil
					case io.ErrUnexpectedEOF:
						// Given the distributed concurrent nature of this app, we sometimes pick up partially
						// uploaded files which will fail, but may well be complete later on, therefore just
						// return for now and attempt again later
						// TODO implement a decent retry mech here
						return el, nil
					default:
						return el, err
					}
				}
				el = append(el, *e)
			}
		}
		// This decoder is only relevant for wals with the most recent schema version (4 presently)
	} else if walSchemaVersionID == 4 {
		dec := gob.NewDecoder(r)
		if err := dec.Decode(&el); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}

		// Instantiate any explicitly empty notes
		for _, e := range el {
			if e.Note != nil && uint8((*e.Note)[0]) == uint8(0) {
				*e.Note = []byte{}
			}
		}
	}

	return el, err
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

func merge(wal1 []EventLog, wal2 []EventLog) []EventLog {
	if len(wal1) == 0 && len(wal2) == 0 {
		return []EventLog{}
	} else if len(wal1) == 0 {
		return wal2
	} else if len(wal2) == 0 {
		return wal1
	}

	// Pre-allocate a slice with the maximum possible items (sum of both lens). Although under many circumstances, it's
	// unlikely we'll fill the capacity, it's far more optimal than each separate append re-allocating to a new slice.
	mergedEl := make([]EventLog, 0, len(wal1)+len(wal2))

	// Before merging, check to see that the the most recent from one wal isn't older than the oldest from another.
	// If that is the case, append the newer to the older and return.
	// We append to the newly allocated mergedEl twice, as we can guarantee that the underlying capacity will be enough
	// (so no further allocations are needed)
	if checkEquality(wal1[0], wal2[len(wal2)-1]) == rightEventOlder {
		mergedEl = append(mergedEl, wal2...)
		mergedEl = append(mergedEl, wal1...)
		return mergedEl
	} else if checkEquality(wal2[0], wal1[len(wal1)-1]) == rightEventOlder {
		mergedEl = append(mergedEl, wal1...)
		mergedEl = append(mergedEl, wal2...)
		return mergedEl
	}

	// Adopt a two pointer approach
	i, j := 0, 0
	// We can use an empty log here because it will never be equal to in the checkEquality calls below
	lastEvent := EventLog{}
	for i < len(wal1) || j < len(wal2) {
		if len(mergedEl) > 0 {
			lastEvent = mergedEl[len(mergedEl)-1]
		}
		if i == len(wal1) {
			// Ignore duplicates (compare with current head of the array
			if len(mergedEl) == 0 || checkEquality(wal2[j], lastEvent) != eventsEqual {
				mergedEl = append(mergedEl, (wal2)[j])
			}
			j++
		} else if j == len(wal2) {
			// Ignore duplicates (compare with current head of the array
			if len(mergedEl) == 0 || checkEquality(wal1[i], lastEvent) != eventsEqual {
				mergedEl = append(mergedEl, (wal1)[i])
			}
			i++
		} else {
			switch checkEquality(wal1[i], wal2[j]) {
			case leftEventOlder:
				if len(mergedEl) == 0 || checkEquality(wal1[i], lastEvent) != eventsEqual {
					mergedEl = append(mergedEl, wal1[i])
				}
				i++
			case rightEventOlder:
				if len(mergedEl) == 0 || checkEquality(wal2[j], lastEvent) != eventsEqual {
					mergedEl = append(mergedEl, wal2[j])
				}
				j++
			case eventsEqual:
				// At this point, we only want to guarantee an increment on ONE of the two pointers
				if i < len(wal1) {
					i++
				} else {
					j++
				}
			}
		}
	}
	return mergedEl
}

func areListItemsEqual(a *ListItem, b *ListItem, checkPointers bool) bool {
	// checkPointers prevents recursion
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.rawLine != b.rawLine ||
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

func checkListItemPtrs(listItem *ListItem, matchItems []*ListItem) error {
	if listItem == nil {
		return nil
	}

	if listItem.child != nil {
		return errors.New("list integrity error: root has a child pointer")
	}

	i := 0
	processedItems := make(map[string]struct{})
	var prev *ListItem
	for listItem.parent != nil {
		// Ensure current.child points to the previous
		// TODO check if this is just duplicating a check in areListItemsEqual below
		if listItem.child != prev {
			return fmt.Errorf("list integrity error: listItem %s child ptr does not point to the expected item", listItem.Key())
		}

		//if !areListItemsEqual(listItem, matchItems[i], false) {
		if listItem != matchItems[i] {
			return fmt.Errorf("list integrity error: listItem %s does not match the expected position in the match list", listItem.Key())
		}

		if _, exists := processedItems[listItem.Key()]; exists {
			return fmt.Errorf("list integrity error: listItem %s has appeared twice", listItem.Key())
		}

		processedItems[listItem.Key()] = struct{}{}
		prev = listItem
		listItem = listItem.parent
		i++
	}

	// Check to see if there are remaining items in the match list
	// NOTE: `i` will have been incremented one more time beyond the final tested index
	if i+1 < len(matchItems) {
		return errors.New("list integrity error: orphaned items in match set")
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

func checkWalIntegrity(wal []EventLog) (*ListItem, []*ListItem, error) {
	// Generate a test repo and use it to generate a match set, then inspect the health
	// of said match set.
	testRepo := DBListRepo{
		log:             []EventLog{},
		listItemTracker: make(map[string]*ListItem),

		webWalFiles:    make(map[string]WalFile),
		allWalFiles:    make(map[string]WalFile),
		syncWalFiles:   make(map[string]WalFile),
		webWalFileMut:  &sync.RWMutex{},
		allWalFileMut:  &sync.RWMutex{},
		syncWalFileMut: &sync.RWMutex{},

		friends:           make(map[string]map[string]int64),
		friendsUpdateLock: &sync.RWMutex{},
	}

	// Use the Replay function to generate the linked lists
	if err := testRepo.Replay(wal); err != nil {
		return nil, nil, err
	}

	// 22/11/21: The Match() function usually returns a slice of ListItem copies as the first argument, which is fine for
	// client operation, as we only reference them for the attached Lines/Notes (and any mutations act on a different
	// slice of ListItem ptrs).
	// In order to allow us to check the state of the ListItems more explicitly, we reference the testRepo.matchListItems
	// which are generated in parallel on Match() calls, as we can then check proper equality between the various items.
	// This isn't ideal as it's not completely consistent with what's returned to the client, but it's a reasonable check
	// for now (and I intend on centralising the logic anyway so we don't maintain two versions of state).
	// TODO, remove this comment when logic is centralised
	_, _, err := testRepo.Match([][]rune{}, true, "", 0, 0)
	if err != nil {
		return nil, nil, errors.New("failed to generate match items for list integrity check")
	}

	// This check traverses from the root node to the last parent and checks the state of the pointer
	// relationships between both. There have previously been edge case wal merge/compaction bugs which resulted
	// in MoveUp events targeting a child, who's child was the original item to be moved (a cyclic pointer bug).
	// This has since been fixed, but to catch other potential cases, we run this check.
	if err := checkListItemPtrs(testRepo.Root, testRepo.matchListItems); err != nil {
		return nil, testRepo.matchListItems, err
	}

	return testRepo.Root, testRepo.matchListItems, nil
}

// recoverWal is responsible for recovering Wals that are very broken, in a variety of ways.
// It collects as much metadata about each item as it can, from both the existing broken wal and the returned
// match set, and then uses it to rebuild a fresh wal, maintaining as much of the original state as possible -
// specifically with regards to DTs and ordering.
func recoverWal(wal []EventLog, matches []*ListItem) []EventLog {
	acknowledgedItems := make(map[string]*ListItem)
	listOrder := []string{}

	// Iterate over the match items and add all to the map
	for _, item := range matches {
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
	for _, e := range wal {
		key, _ := e.getKeys()
		item, exists := acknowledgedItems[key]
		switch e.EventType {
		case AddEvent:
			fallthrough
		case UpdateEvent:
			if !exists {
				item = &ListItem{
					rawLine:      e.Line,
					Note:         e.Note,
					originUUID:   e.UUID,
					creationTime: e.ListItemCreationTime,
				}
				if key != item.Key() {
					log.Fatal("ListItem key mismatch during recovery")
				}
			} else {
				if e.EventType == AddEvent {
					item.rawLine = e.Line
					item.Note = e.Note
				} else {
					// Updates handle Line and Note mutations separately
					if e.Note != nil {
						item.Note = e.Note
					} else {
						item.rawLine = e.Line
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
			Line:                 item.rawLine,
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

	return newWal
}

func compact(wal []EventLog) ([]EventLog, error) {
	// Check the integrity of the incoming full wal prior to compaction.
	// If broken in some way, call the recovery function.
	testRootA, matchItemsA, err := checkWalIntegrity(wal)
	if err != nil {
		// If we get here, shit's on fire. This function is the equivalent of the fire brigade.
		wal = recoverWal(wal, matchItemsA)
		testRootA, _, err = checkWalIntegrity(wal)
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
	for i := len(wal) - 1; i >= 0; i-- {
		e := wal[i]
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
	testRootB, _, err := checkWalIntegrity(compactedWal)
	if err != nil {
		log.Fatalf("`compact` caused wal to lose integrity: %s", err)
	}

	if !listsAreEquivalent(testRootA, testRootB) {
		//sliceA := []ListItem{}
		//node := testRootA
		//for node != nil {
		//    sliceA = append(sliceA, *node)
		//    node = node.parent
		//}
		//generatePlainTextFile(sliceA)

		//sliceB := []ListItem{}
		//node = testRootB
		//for node != nil {
		//    sliceB = append(sliceB, *node)
		//    node = node.parent
		//}
		//generatePlainTextFile(sliceB)

		//return wal, nil
		log.Fatal("`compact` generated inconsistent results and things blew up!")
	}
	return compactedWal, nil
}

// generatePlainTextFile takes the current matchset, and writes the lines separately to a
// local file. Notes are ignored.
func generatePlainTextFile(matchItems []ListItem) error {
	curWd, err := os.Getwd()
	if err != nil {
		return err
	}
	// Will be in the form `{currentDirectory}/export_1624785401.txt`
	fileName := path.Join(curWd, fmt.Sprintf(exportFilePattern, time.Now().UnixNano()))
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for _, i := range matchItems {
		if _, err := f.Write([]byte(fmt.Sprintf("%s\n", i.rawLine))); err != nil {
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
			Line:                       item.rawLine,
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

	b := buildByteWal(wal)
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

func (r *DBListRepo) pull(walFiles []WalFile, walChan chan []EventLog) error {
	//log.Print("Pulling...")
	// Concurrently gather all new wal UUIDs for all walFiles, tracking each in a map against a walFile key
	newWalMutex := sync.Mutex{}
	newWalMap := make(map[WalFile][]string)
	var wg sync.WaitGroup
	for _, wf := range walFiles {
		wg.Add(1)
		go func(wf WalFile) {
			defer wg.Done()
			filePathPattern := path.Join(wf.GetRoot(), walFilePattern)
			newWals, err := wf.GetMatchingWals(fmt.Sprintf(filePathPattern, "*"))
			if err != nil {
				//log.Fatal(err)
				return
			}
			newWalMutex.Lock()
			defer newWalMutex.Unlock()
			newWalMap[wf] = newWals
		}(wf)
	}
	wg.Wait()

	// We then run CPU bound ops in a single thread to mitigate excessive CPU usage (particularly as an N WalFiles
	// is unbounded)
	// We merge all wals before publishing to the walchan as each time the main app event loop consumes from walchan,
	// it blocks user input and creates a poor user experience.
	mergedWal := []EventLog{}
	for wf, newWals := range newWalMap {
		for _, newWal := range newWals {
			if !r.isPartialWalProcessed(newWal) {
				pr, pw := io.Pipe()
				go func() {
					defer pw.Close()
					if err := wf.GetWalBytes(pw, newWal); err != nil {
						// TODO handle
						//log.Fatal(err)
					}
				}()

				// TODO rename buildFromFile
				// Build new wals
				newWfWal, err := buildFromFile(pr)
				if err != nil {
					// Ignore incompatible files
					continue
				}

				mergedWal = merge(mergedWal, newWfWal)

				// Add to the processed cache after we've successfully pulled it
				// TODO strictly we should only set processed once it's successfull merged and displayed to client
				r.setProcessedPartialWals(newWal)
			}
		}
	}

	if len(mergedWal) > 0 {
		go func() {
			walChan <- mergedWal
		}()
	}

	return nil
}

func (r *DBListRepo) gather(walChan chan []EventLog) error {
	//log.Print("Gathering...")
	// Create a new list so we don't have to keep the lock on the mutex for too long
	syncWalFiles := []WalFile{}
	r.syncWalFileMut.RLock()
	for _, wf := range r.syncWalFiles {
		syncWalFiles = append(syncWalFiles, wf)
	}
	r.syncWalFileMut.RUnlock()

	fullMergedWal := []EventLog{}
	localReplayWal := []EventLog{}
	for _, wf := range syncWalFiles {
		// Handle ALL wals
		filePathPattern := path.Join(wf.GetRoot(), walFilePattern)
		originFiles, err := wf.GetMatchingWals(fmt.Sprintf(filePathPattern, "*"))
		if err != nil {
			continue
			//log.Fatal(err)
		}

		// If there's only 1 file, there's no point gathering them, so skip
		// We need to let it pass for the 0 case (e.g. fresh roots with no existing wals)
		if len(originFiles) == 1 {
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

			if !r.isPartialWalProcessed(fileName) {
				localReplayWal = merge(localReplayWal, wal)
			}

			// Only delete files which were successfully pulled
			filesToDelete = append(filesToDelete, fileName)
			mergedWal = merge(mergedWal, wal)
		}

		// Merge with entire local log
		mergedWal = merge(mergedWal, r.log)

		// Compact
		compactedWal, err := compact(mergedWal)
		if err != nil {
			if err == errWalIntregrity {
				// This isn't nice, but saves a larger refactor for what might be entirely unused emergency
				// logic: we need to purge the log on the listRepo, otherwise on `gather` -> `Replay`, the
				// broken log will just be merged back in. We want to remove it entirely.
				r.log = []EventLog{}
			} else {
				return err
			}
		}
		mergedWal = compactedWal

		// Flush the gathered Wal
		if err := r.push(mergedWal, wf, ""); err != nil {
			log.Fatal(err)
		}

		// Merge into the full wal (which will be returned at the end of the function)
		fullMergedWal = merge(fullMergedWal, mergedWal)

		// Schedule a delete on the files
		wf.RemoveWals(filesToDelete)
	}

	if len(fullMergedWal) > 0 {
		// We track if there are any unprocessed wals that we gather, as there's no point replaying a full log
		// (which is costly) if we already have the events locally
		if len(localReplayWal) > 0 {
			go func() {
				walChan <- fullMergedWal
			}()
		}

		// If a push to a non-owned walFile fails, the events will never reach said walFile, as it's the responsible
		// of the owner's client to gather and merge their own walFiles.
		// Therefore, on each gather, we push the aggregated log to all non-owned walFiles (filtered for each).
		randomUUID := fmt.Sprintf("%v%v", r.uuid, generateUUID())

		r.allWalFileMut.RLock()
		r.syncWalFileMut.RLock()

		nonOwnedWalFiles := []WalFile{}
		for name, wf := range r.allWalFiles {
			if _, exists := r.syncWalFiles[name]; !exists {
				nonOwnedWalFiles = append(nonOwnedWalFiles, wf)
			}
		}

		r.allWalFileMut.RUnlock()
		r.syncWalFileMut.RUnlock()

		for _, wf := range nonOwnedWalFiles {
			go func(wf WalFile) {
				r.push(fullMergedWal, wf, randomUUID)
			}(wf)
		}
	}

	return nil
}

func buildByteWal(el []EventLog) *bytes.Buffer {
	var compressedBuf bytes.Buffer

	// Write the schema ID
	err := binary.Write(&compressedBuf, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		log.Fatal(err)
	}

	//pr, pw := io.Pipe()
	var buf bytes.Buffer
	//enc := gob.NewEncoder(pw)

	// If we have empty/non-null notes, we need to explicitly set them to our empty note
	// pattern, otherwise gob will treat a ptr to an empty byte array as null when decoding
	for _, e := range el {
		if e.Note != nil && len(*e.Note) == 0 {
			*e.Note = []byte{0}
		}
	}

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

func (r *DBListRepo) getMatchedWal(el []EventLog, wf WalFile) []EventLog {
	_, isWebRemote := wf.(*WebWalFile)
	walOwnerEmail := wf.GetUUID()

	// Iterate over the entire Wal. If a Line fulfils the Match rules, log the key in a map
	for _, e := range el {
		var isFriend bool
		if len(e.Line) > 0 {
			// Event owner is added to e.Friends by default
			friends := r.getFriendsFromLine(e.Line)
			_, isFriend = friends[walOwnerEmail]
			isFriend = isFriend || walOwnerEmail == r.email
		}
		if !isWebRemote || isFriend {
			k, _ := e.getKeys()
			wf.SetProcessedEvent(k)
		}
	}

	// Only include those events which are/have been shared (this is handled via the event processed
	// cache elsewhere)
	filteredWal := []EventLog{}
	for _, e := range el {
		k, _ := e.getKeys()
		if wf.IsEventProcessed(k) {
			filteredWal = append(filteredWal, e)
		}
	}
	return filteredWal
}

func (r *DBListRepo) push(el []EventLog, wf WalFile, randomUUID string) error {
	// Apply any filtering based on Push match configuration
	el = r.getMatchedWal(el, wf)

	// Return for empty wals
	if len(el) == 0 {
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
		return err
	}

	return nil
}

func (r *DBListRepo) flushPartialWals(el []EventLog) {
	//log.Print("Flushing...")
	if len(el) > 0 {
		randomUUID := fmt.Sprintf("%v%v", r.uuid, generateUUID())
		r.allWalFileMut.RLock()
		defer r.allWalFileMut.RUnlock()
		for _, wf := range r.allWalFiles {
			go func(wf WalFile) {
				r.push(el, wf, randomUUID)
			}(wf)
		}
	}
}

func (r *DBListRepo) emitRemoteUpdate() {
	if r.web != nil && r.web.isActive {
		// We need to wrap the friendsMostRecentChangeDT comparison check, as the friend map update
		// and subsequent friendsMostRecentChangeDT update needs to be an atomic operation
		r.friendsUpdateLock.RLock()
		defer r.friendsUpdateLock.RUnlock()
		if r.friendsLastPushDT == 0 || r.friendsLastPushDT < r.friendsMostRecentChangeDT {
			u, _ := url.Parse(apiURL)
			u.Path = path.Join(u.Path, "remote")

			emails := []string{}
			for e := range r.friends {
				if e != r.email {
					emails = append(emails, e)
				}
			}

			remote := WebRemote{
				Emails:       emails,
				DTLastChange: r.friendsMostRecentChangeDT,
			}

			go func() {
				if err := r.web.PostRemote(&remote, u); err != nil {
					//fmt.Println(err)
					//os.Exit(0)
				}
			}()
			r.friendsLastPushDT = r.friendsMostRecentChangeDT
		}
	}
}

func (r *DBListRepo) startSync(walChan chan []EventLog) error {
	//log.Print("Now with merged wal...")

	syncTriggerChan := make(chan struct{}, 1) // Without the buffered slot, the initial sync on non-web usage is not triggered
	// we use pushAggregateWindowChan to ensure that there is only a single pending push scheduled
	pushAggregateWindowChan := make(chan struct{}, 1)
	pushTriggerChan := make(chan struct{})
	gatherTriggerTimer := time.NewTimer(time.Second * 0)
	// Drain the initial timer, we want to wait for initial user input
	<-gatherTriggerTimer.C

	webPingTicker := time.NewTicker(webPingInterval)
	webRefreshTicker := time.NewTicker(webRefreshInterval)
	// We set the interval to 0 because we want the initial connection establish attempt to occur ASAP
	webRefreshTicker.Reset(0)

	websocketPushEvents := make(chan websocketMessage)

	// Schedule push to all non-local walFiles
	// This is required for flushing new files that have been manually dropped into local root, and to cover other off cases.
	// We block until the web connection is established (or skipped) on initial start, to ensure logs are propagated to all
	// required remotes.
	hasRunInitialSync := false
	triggerInitialSync := func() {
		if hasRunInitialSync {
			return
		}
		// Because we `gather` on close, for most scenarios, we only need to do this if there are > 1 wal files locally.
		// NOTE: this obviously won't work when dropping a single wal file into a fresh root directory, but this is
		// heading into edge cases of edge cases so won't worry about it for now
		localFileNames, err := r.LocalWalFile.GetMatchingWals(fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), walFilePattern), "*"))
		if err != nil {
			log.Fatal(err)
		}
		if len(localFileNames) > 1 {
			func() {
				r.allWalFileMut.RLock()
				defer r.allWalFileMut.RUnlock()
				for _, wf := range r.allWalFiles {
					if wf != r.LocalWalFile {
						go func(wf WalFile) { r.push(r.log, wf, "") }(wf)
					}
				}
			}()
			hasRunInitialSync = true
		}

		// Do an initial remote update to ensure any offline changes are immediately synced to the cloud
		r.emitRemoteUpdate()
	}

	// We want to trigger a web sync as soon as the web connection has been established
	scheduleSync := func(afterSeconds int) {
		// Attempt to put onto the channel, else pass
		if afterSeconds > 0 {
			time.Sleep(time.Second * time.Duration(afterSeconds))
		}
		select {
		case syncTriggerChan <- struct{}{}:
		default:
		}
		triggerInitialSync()
	}
	schedulePush := func() {
		select {
		// Only schedule a post-interval push if there isn't already one pending. pushAggregateWindowChan
		// is responsible for holding pending pushes.
		case pushAggregateWindowChan <- struct{}{}:
			go func() {
				time.Sleep(time.Second * pushIntervalSeconds)
				pushTriggerChan <- struct{}{}
				<-pushAggregateWindowChan
			}()
		default:
		}

		// Each time a push is scheduled, successful or not (e.g. on a keypress), we reset the timer for
		// the next gather trigger (up to the maximum window)
		// TODO this appears to be working as is without the explicit channel drain, but if things start going
		// awry here, this thread has useful context:
		// https://stackoverflow.com/a/58631999
		//if !gatherTriggerTimer.Stop() {
		//    select {
		//    case <-gatherTriggerTimer.C:
		//    default:
		//    }
		//}
		gatherTriggerTimer.Reset(gatherWaitDuration)
	}

	// Prioritise async web start-up to minimise wait time before websocket instantiation
	// Create a loop responsible for periodic refreshing of web connections and web walfiles.
	// TODO only start this goroutine if there is actually a configured web
	if r.web != nil {
		go func() {
			expBackoffInterval := time.Second * 1
			var waitInterval time.Duration
			var ctx context.Context
			var cancel context.CancelFunc
			for {
				select {
				case <-webPingTicker.C:
					// is !isActive, we've already entered the exponential retry backoff below
					if r.web.isActive {
						if _, err := r.web.ping(); err != nil {
							r.web.isActive = false
							webRefreshTicker.Reset(0)
							continue
						}
					}
				case m := <-websocketPushEvents:
					if r.web.isActive {
						r.web.pushWebsocket(m)
					}
				case <-webRefreshTicker.C:
					if cancel != nil {
						cancel()
					}
					// Close off old websocket connection
					// Nil check because initial instantiation also occurs async in this loop (previous it was sync on startup)
					if r.web.wsConn != nil {
						r.web.wsConn.Close(websocket.StatusNormalClosure, "")
					}
					// Start new one
					err := r.registerWeb()
					if err != nil {
						r.web.isActive = false
						waitInterval = expBackoffInterval
						if expBackoffInterval < webRefreshInterval {
							expBackoffInterval *= 2
						}
					} else {
						r.web.isActive = true
						expBackoffInterval = time.Second * 1
						waitInterval = webRefreshInterval
					}
					// Trigger web walfile sync (mostly relevant on initial start)
					scheduleSync(0)

					ctx, cancel = context.WithCancel(context.Background())

					if r.web.isActive {
						go func() {
							for {
								err := func() error {
									err := r.consumeWebsocket(ctx, walChan)
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
					}

					webRefreshTicker.Reset(waitInterval)
				}
			}
		}()

		// Create a loop to deal with any collaborator cursor move events
		go func() {
			for {
				e := <-r.localCursorMoveChan
				func() {
					r.webWalFileMut.RLock()
					defer r.webWalFileMut.RUnlock()
					for _, wf := range r.webWalFiles {
						go func(wf WalFile) {
							websocketPushEvents <- websocketMessage{
								Action:       "position",
								UUID:         wf.GetUUID(),
								Key:          e.listItemKey,
								UnixNanoTime: e.unixNanoTime,
							}
						}(wf)
					}
				}()
			}
		}()
	} else {
		go func() {
			scheduleSync(0)
		}()
	}

	// Run an initial load from the local walfile
	if err := r.pull([]WalFile{r.LocalWalFile}, walChan); err != nil {
		return err
	}

	// Main sync event loop
	go func() {
		for {
			select {
			case <-syncTriggerChan:
				syncWalFiles := []WalFile{}
				r.syncWalFileMut.RLock()
				for _, wf := range r.syncWalFiles {
					syncWalFiles = append(syncWalFiles, wf)
				}
				r.syncWalFileMut.RUnlock()
				if err := r.pull(syncWalFiles, walChan); err != nil {
					log.Fatal(err)
				}
			case <-gatherTriggerTimer.C:
				if err := r.gather(walChan); err != nil {
					log.Fatal(err)
				}
			}

			// Rather than relying on a ticker (which will trigger the next cycle if processing time is >= the interval)
			// we set a wait interval from the end of processing. This prevents a vicious circle which could leave the
			// program with it's CPU constantly tied up, which leads to performance degradation.
			// Instead, at the end of the processing cycle, we schedule a wait period after which the next event is put
			// onto the syncTriggerChan
			go func() {
				scheduleSync(pullIntervalSeconds)
			}()
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
					func() {
						r.webWalFileMut.RLock()
						defer r.webWalFileMut.RUnlock()
						for _, wf := range r.webWalFiles {
							matchedEventLog := r.getMatchedWal([]EventLog{e}, wf)
							if len(matchedEventLog) > 0 {
								// There are only single events, so get the zero index
								b := buildByteWal([]EventLog{matchedEventLog[0]})
								b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())
								go func(wf WalFile) {
									websocketPushEvents <- websocketMessage{
										Action: "wal",
										UUID:   wf.GetUUID(),
										Wal:    b64Wal,
									}
								}(wf)
							}
						}
					}()

					// Emit any remote updates if web active and local changes have occurred
					r.emitRemoteUpdate()
				}
				// Add to an ephemeral log
				el = append(el, e)
				// Trigger an aggregated push (if not already pending)
				schedulePush()
			case <-pushTriggerChan:
				// On ticks, Flush what we've aggregated to all walfiles, and then reset the
				// ephemeral log. If empty, skip.
				r.flushPartialWals(el)
				el = []EventLog{}
			case <-r.stop:
				r.flushPartialWals(el)
				r.stop <- struct{}{}
			}
		}
	}()

	return nil
}

func (r *DBListRepo) finish(purge bool) error {
	// Flush all unpushed changes to non-local walfiles
	// TODO handle this more gracefully
	r.stop <- struct{}{}
	<-r.stop

	// When we pull wals from remotes, we merge into our in-mem logs, but will only flush to local walfile
	// on gather. To ensure we store all logs locally, for now, we can just push the entire in-mem log to
	// the local walfile. We can remove any other files to avoid overuse of local storage.
	// TODO this can definitely be optimised (e.g. only flush a partial log of unpersisted changes, or perhaps
	// track if any new wals have been pulled, etc)
	if !purge {
		localFiles, _ := r.LocalWalFile.GetMatchingWals(fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), walFilePattern), "*"))
		filesToDelete := []string{}
		// Ensure we've actually processed the files before we delete them...
		for _, fileName := range localFiles {
			if r.isPartialWalProcessed(fileName) {
				filesToDelete = append(filesToDelete, fileName)
			}
		}
		r.push(r.log, r.LocalWalFile, "")
		r.LocalWalFile.RemoveWals(filesToDelete)
	} else {
		// If purge is set, we delete everything in the local walfile. This is used primarily in the wasm browser app on logout
		r.LocalWalFile.Purge()
	}

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

	b := buildByteWal(el)
	wf.Flush(b, fmt.Sprintf("%d", uuid))

	return nil
}
