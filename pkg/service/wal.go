package service

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
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
	"strconv"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const latestWalSchemaID uint16 = 5

// sync intervals
const (
	pullIntervalSeconds      = 5
	pushWaitDuration         = time.Second * time.Duration(5)
	gatherWaitDuration       = time.Second * time.Duration(15)
	compactionGatherMultiple = 2
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

type EventType uint16

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

type LineFriends struct {
	IsProcessed bool
	Offset      int
	Emails      []string
	emailsMap   map[string]struct{}
}

type EventLog struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string // TODO This represents the raw (un-friends-processed) line
	Note                       []byte
	Friends                    LineFriends
	key                        string
	targetKey                  string
}

type LineFriendsSchema4 struct {
	IsProcessed bool
	Offset      int
	Emails      map[string]struct{}
}

type EventLogSchema4 struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string // TODO This represents the raw (un-friends-processed) line
	Note                       []byte
	Friends                    LineFriendsSchema4
	key                        string
	targetKey                  string
}

func (e *EventLog) getKeys() (string, string) {
	if e.key == "" {
		e.key = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.ListItemCreationTime))
	}
	if e.targetKey == "" {
		e.targetKey = strconv.Itoa(int(e.TargetUUID)) + ":" + strconv.Itoa(int(e.TargetListItemCreationTime))
	}
	return e.key, e.targetKey
}

func (e *EventLog) emailHasAccess(email string) bool {
	if e.Friends.emailsMap == nil {
		e.Friends.emailsMap = make(map[string]struct{})
		for _, f := range e.Friends.Emails {
			e.Friends.emailsMap[f] = struct{}{}
		}
	}
	_, exists := e.Friends.emailsMap[email]
	return exists
}

// WalFile offers a generic interface into local or remote filesystems
type WalFile interface {
	GetUUID() string
	GetRoot() string
	GetMatchingWals(context.Context, string) ([]string, error)
	GetWalBytes(context.Context, io.Writer, string) error
	RemoveWals(context.Context, []string) error
	Flush(context.Context, *bytes.Buffer, string) error
}

type LocalWalFile interface {
	Purge()

	WalFile
}

type LocalFileWalFile struct {
	rootDir string
}

func NewLocalFileWalFile(rootDir string) *LocalFileWalFile {
	return &LocalFileWalFile{
		rootDir: rootDir,
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

func (wf *LocalFileWalFile) GetMatchingWals(ctx context.Context, matchPattern string) ([]string, error) {
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

func (wf *LocalFileWalFile) GetWalBytes(ctx context.Context, w io.Writer, fileName string) error {
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

func (wf *LocalFileWalFile) RemoveWals(ctx context.Context, fileNames []string) error {
	for _, f := range fileNames {
		os.Remove(fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), f))
	}
	return nil
}

func (wf *LocalFileWalFile) Flush(ctx context.Context, b *bytes.Buffer, randomUUID string) error {
	fileName := fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), randomUUID)
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	f.Write(b.Bytes())
	return nil
}

// https://go.dev/play/p/1kbFF8FR-V7
// enforces existence of surrounding boundary character
// match[0] = full match (inc boundaries)
// match[1] = `@email`
// match[2] = `email`
//var lineFriendRegex = regexp.MustCompile("(?:^|\\s)(@([a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*))(?:$|\\s)")

// getFriendsFromLine returns a map of @friends in the line, that currently exist in the friends cache, and a
// boolean representing whether or not the line is already correctly ordered (e.g. the friends are all appended
// to the end of the line, separated by single whitespace chars)
func (r *DBListRepo) getFriendsFromLine(line string) ([]string, bool) {
	// (golang?) regex does return overlapping results, which we need in order to ensure space
	// or start/end email boundaries. Therefore we iterate over the line and match/replace any
	// email with (pre|app)ending spaces with a single space

	//friends := map[string]struct{}{}

	//if len(line) == 0 {
	//    return friends
	//}

	//replacedLine := line
	//oldReplacedLine := ""
	//for replacedLine != oldReplacedLine {
	//    oldReplacedLine = replacedLine
	//    if match := lineFriendRegex.FindStringSubmatch(replacedLine); match != nil {
	//        // If we want to check against the friends cache, we use the email only matching group
	//        s := match[2]
	//        friends[s] = struct{}{}
	//        replacedLine = strings.Replace(replacedLine, s, " ", 1)
	//    }
	//}

	//return friends

	// Regex ops above (although far more accurate in terms of email matching) are _ridiculously_ expensive,
	// therefore we're going to (for now) default to basic matching of any words starting with "@"
	r.friendsUpdateLock.RLock()
	defer r.friendsUpdateLock.RUnlock()

	hasFoundFriend := false
	isOrdered := true
	friendsMap := map[string]struct{}{}
	for _, w := range strings.Split(line, " ") {
		if len(w) > 1 && rune(w[0]) == '@' && r.friends[w[1:]] != nil {
			// If there are duplicates, the line has not been processed
			if _, exists := friendsMap[w[1:]]; exists {
				isOrdered = false
			}
			friendsMap[w[1:]] = struct{}{}
			hasFoundFriend = true
		} else if hasFoundFriend {
			// If we reach here, the current word is _not_ a friend, but we have previously processed one
			isOrdered = false
		}
	}
	friends := []string{}
	for f := range friendsMap {
		friends = append(friends, f)
	}
	return friends, isOrdered
}

func (r *DBListRepo) getEmailFromConfigLine(line string) string {
	//if len(line) == 0 {
	//    return ""
	//}
	//match := r.cfgFriendRegex.FindStringSubmatch(line)
	//// First submatch is the email regex
	//if len(match) > 1 {
	//    return match[1]
	//}
	//return ""

	// Avoiding expensive regex based ops for now
	var f string
	if words := strings.Split(line, " "); len(words) == 3 && words[0] == "fzn_cfg:friend" && rune(words[2][0]) == '@' && words[2][1:] == r.email {
		f = words[1]
	}
	return f
}

func (r *DBListRepo) repositionActiveFriends(e EventLog) EventLog {
	// SL 2021-12-30: Recent changes mean that _all_ event logs will now store the current state of the line, so this
	// check is only relevant to bypass earlier logs which have nothing to process.
	if len(e.Line) == 0 {
		return e
	}

	if e.Friends.IsProcessed {
		return e
	}

	// If this is a config line, we only want to hide the owner email, therefore manually set a single key friends map
	var friends []string
	//if r.cfgFriendRegex.MatchString(e.Line) {
	if r.getEmailFromConfigLine(e.Line) != "" {
		friends = append(friends, r.email)
	} else {
		// If there are no friends, return early
		if friends, _ = r.getFriendsFromLine(e.Line); len(friends) == 0 {
			e.Friends.IsProcessed = true
			e.Friends.Offset = len(e.Line)
			return e
		}
	}

	newLine := e.Line
	for _, f := range friends {
		atFriend := "@" + f
		// Cover edge case whereby email is typed first and constitutes entire string
		if atFriend == newLine {
			newLine = ""
		} else {
			// Each cut email address needs also remove (up to) one (pre|suf)fixed space, not more.
			newLine = strings.ReplaceAll(newLine, " "+atFriend, "")
			newLine = strings.ReplaceAll(newLine, atFriend+" ", "")
		}
	}

	friendString := ""

	// Sort the emails, and then append a space separated string to the end of the Line
	sort.Strings(friends)
	for _, f := range friends {
		friendString += " @"
		friendString += f
	}

	newLine += friendString
	e.Line = newLine

	e.Friends.IsProcessed = true
	e.Friends.Offset = len(newLine) - len(friendString)
	e.Friends.Emails = friends
	return e
}

func (r *DBListRepo) generateFriendChangeEvents(e EventLog, item *ListItem) {
	// This method is responsible for detecting changes to the "friends" configuration in order to update
	// local state, and to emit events to the cloud API.
	var friendToAdd, friendToRemove string

	var existingLine string
	if item != nil {
		existingLine = item.rawLine
	}

	before := r.getEmailFromConfigLine(existingLine)
	switch e.EventType {
	case AddEvent, UpdateEvent:
		after := r.getEmailFromConfigLine(e.Line)
		// If before != after, we know that we need to remove the previous entry, and add the new one.
		if before != after {
			friendToRemove = before
			friendToAdd = after
		}
	case DeleteEvent:
		friendToRemove = before
	default:
		return
	}

	if friendToRemove == "" && friendToAdd == "" {
		return
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
						uuid: friendToAdd,
						web:  r.web,
					},
					false,
				)
			}
		}
		//for email := range friendsToRemove {
		if friendToRemove != "" && friendToRemove != r.email {
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

func (r *DBListRepo) processEventLog(root *ListItem, e EventLog) (*ListItem, *ListItem, error) {
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

	r.generateFriendChangeEvents(e, item)

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
				item, err = r.update(e.Line, e.Friends, e.Note, item)
			}
			item, err = r.update(e.Line, e.Friends, nil, item)
		} else {
			root, item, err = r.add(root, e.ListItemCreationTime, e.Line, e.Friends, e.Note, targetItem, e.UUID)
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
			item, err = r.update(e.Line, e.Friends, e.Note, item)
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

func (r *DBListRepo) add(root *ListItem, creationTime int64, line string, friends LineFriends, note []byte, childItem *ListItem, uuid uuid) (*ListItem, *ListItem, error) {
	newItem := &ListItem{
		originUUID:   uuid,
		creationTime: creationTime,
		child:        childItem,
		rawLine:      line,
		Note:         note,
		friends:      friends,
		localEmail:   r.email,
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

func (r *DBListRepo) update(line string, friends LineFriends, note []byte, listItem *ListItem) (*ListItem, error) {
	if len(line) > 0 {
		listItem.rawLine = line
		// listItem.friends.emails is a map, which we only ever want to OR with to aggregate (we can only add new emails,
		// not remove any, due to the processedEventMap mechanism elsewhere)
		mergedEmailMap := make(map[string]struct{})
		for _, e := range listItem.friends.Emails {
			mergedEmailMap[e] = struct{}{}
		}
		for _, e := range friends.Emails {
			mergedEmailMap[e] = struct{}{}
		}
		listItem.friends.IsProcessed = friends.IsProcessed
		listItem.friends.Offset = friends.Offset
		emails := []string{}
		for e := range mergedEmailMap {
			if e != r.email {
				emails = append(emails, e)
			}
		}
		sort.Strings(emails)
		listItem.friends.Emails = emails
	} else {
		listItem.Note = note
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
		replayLog = r.log

		// If doing a full merge, we generate checksum and check against the processedWalChecksums cache - if
		// the wal has already been processed, there's no need to replay the events, so we can return early to
		// avoid unnecessary work
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(replayLog); err != nil {
			log.Fatal("Boom")
		}
		checksum := md5.Sum(buf.Bytes())
		if r.isWalChecksumProcessed(checksum) {
			return nil
		}
		r.setProcessedWalChecksum(checksum)

		// Clear the listItemTracker for all full Replays
		// This map is also used by the main service interface CRUD endpoints, but we can
		// clear it here because both the CRUD ops and these Replays run in the same loop,
		// so we won't get any contention.
		r.listItemTracker = make(map[string]*ListItem)

		// The same goes for the `friends` cache.
		// The cache is populated by traversing through the list of events sequentially, so the state
		// of the cache at any point is representative of the intent at _that_ point. Therefore, we reset
		// it on full replays to ensure that events are only triggered for the intended state at the time.
		r.friendsUpdateLock.Lock()
		r.friends = make(map[string]map[string]int64)
		// TODO dedup
		if r.email != "" {
			r.friends[r.email] = make(map[string]int64)
		}
		r.friendsUpdateLock.Unlock()
	} else {
		replayLog = partialWal
		root = r.Root
	}

	for _, e := range replayLog {
		// We need to pass a fresh null root and leave the old r.Root intact for the function
		// caller logic
		root, _, _ = r.processEventLog(root, e)
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
			el.Note = []byte{}
		}
		if item.NoteLength > 0 {
			note := make([]byte, item.NoteLength)
			err = binary.Read(r, binary.LittleEndian, &note)
			if err != nil {
				return nil, err
			}
			el.Note = note
		}
	default:
		return nil, errors.New("unrecognised wal schema version")
	}
	return &el, nil
}

func buildFromFile(raw io.Reader) ([]EventLog, error) {
	var el []EventLog
	var walSchemaVersionID uint16
	if err := binary.Read(raw, binary.LittleEndian, &walSchemaVersionID); err != nil {
		if err == io.EOF {
			return el, nil
		}
		return el, err
	}

	pr, pw := io.Pipe()
	errChan := make(chan error, 1)
	go func() {
		defer pw.Close()
		if walSchemaVersionID < 3 {
			if _, err := io.Copy(pw, raw); err != nil {
				errChan <- err
			}
		} else {
			// Versions >=3 of the wal schema is gzipped after the first 2 bytes. Therefore, unzip those bytes
			// prior to passing it to the loop below
			zr, err := gzip.NewReader(raw)
			if err != nil {
				errChan <- err
			}
			defer zr.Close()
			if _, err := io.Copy(pw, zr); err != nil {
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
				e, err := getNextEventLogFromWalFile(pr, walSchemaVersionID)
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
	} else if walSchemaVersionID == 4 {
		var oel []EventLogSchema4
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&oel); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}
		for _, oe := range oel {
			e := EventLog{
				UUID:                       oe.UUID,
				TargetUUID:                 oe.TargetUUID,
				ListItemCreationTime:       oe.ListItemCreationTime,
				TargetListItemCreationTime: oe.TargetListItemCreationTime,
				UnixNanoTime:               oe.UnixNanoTime,
				EventType:                  oe.EventType,
				Line:                       oe.Line,
				Note:                       oe.Note,
				Friends: LineFriends{
					IsProcessed: oe.Friends.IsProcessed,
					Offset:      oe.Friends.Offset,
					emailsMap:   oe.Friends.Emails,
				},
			}
			for f := range oe.Friends.Emails {
				e.Friends.Emails = append(e.Friends.Emails, f)
			}
			sort.Strings(e.Friends.Emails)
			el = append(el, e)
		}
	} else {
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&el); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}
	}

	return el, nil
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
				mergedEl = append(mergedEl, wal2[j])
			}
			j++
		} else if j == len(wal2) {
			// Ignore duplicates (compare with current head of the array
			if len(mergedEl) == 0 || checkEquality(wal1[i], lastEvent) != eventsEqual {
				mergedEl = append(mergedEl, wal1[i])
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
		string(a.Note) != string(b.Note) ||
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
func (r *DBListRepo) generatePartialView(ctx context.Context, matchItems []ListItem) error {
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
	r.LocalWalFile.Flush(ctx, b, viewName)
	log.Fatalf("N list generated events: %d", len(wal))
	return nil
}

func (r *DBListRepo) setProcessedWalName(fileName string) {
	r.processedWalNameLock.Lock()
	defer r.processedWalNameLock.Unlock()
	r.processedWalNames[fileName] = struct{}{}
}

func (r *DBListRepo) isWalNameProcessed(fileName string) bool {
	r.processedWalNameLock.Lock()
	defer r.processedWalNameLock.Unlock()
	_, exists := r.processedWalNames[fileName]
	return exists
}

func (r *DBListRepo) setProcessedWalChecksum(checksum [16]byte) {
	r.processedWalChecksumLock.Lock()
	defer r.processedWalChecksumLock.Unlock()
	r.processedWalChecksums[checksum] = struct{}{}
}

func (r *DBListRepo) isWalChecksumProcessed(checksum [16]byte) bool {
	r.processedWalChecksumLock.Lock()
	defer r.processedWalChecksumLock.Unlock()
	_, exists := r.processedWalChecksums[checksum]
	return exists
}

func (r *DBListRepo) pull(ctx context.Context, walFiles []WalFile) ([]EventLog, error) {
	// Concurrently gather all new wal UUIDs for all walFiles, tracking each in a map against a walFile key
	newWalMutex := sync.Mutex{}
	newWalMap := make(map[WalFile][]string)
	var wg sync.WaitGroup
	for _, wf := range walFiles {
		wg.Add(1)
		go func(wf WalFile) {
			defer wg.Done()
			filePathPattern := path.Join(wf.GetRoot(), walFilePattern)
			newWals, err := wf.GetMatchingWals(ctx, fmt.Sprintf(filePathPattern, "*"))
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
			if !r.isWalNameProcessed(newWal) {
				pr, pw := io.Pipe()
				go func() {
					defer pw.Close()
					if err := wf.GetWalBytes(ctx, pw, newWal); err != nil {
						// TODO handle
						//log.Fatal(err)
					}
				}()

				// Build new wals
				newWfWal, err := buildFromFile(pr)
				if err != nil {
					// Ignore incompatible files
					continue
				}

				mergedWal = merge(mergedWal, newWfWal)

				// Add to the processed cache after we've successfully pulled it
				// TODO strictly we should only set processed once it's successfull merged and displayed to client
				r.setProcessedWalName(newWal)
			}
		}
	}

	return mergedWal, nil
}

func (r *DBListRepo) gather(ctx context.Context, runCompaction bool) ([]EventLog, error) {
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
		originFiles, err := wf.GetMatchingWals(ctx, fmt.Sprintf(filePathPattern, "*"))
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
				if err := wf.GetWalBytes(ctx, pw, fileName); err != nil {
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

			// TODO put this in a less frequented code path, or remove when we're very confident in the merge algo
			sort.Slice(wal, func(i, j int) bool {
				return checkEquality(wal[i], wal[j]) == leftEventOlder
			})

			// There's no point replaying a full log (which is costly) if we already have the events locally
			if !r.isWalNameProcessed(fileName) {
				localReplayWal = merge(localReplayWal, wal)
			}

			// Only delete files which were successfully pulled
			filesToDelete = append(filesToDelete, fileName)
			mergedWal = merge(mergedWal, wal)
		}

		// Merge with entire local log
		mergedWal = merge(mergedWal, r.log)

		// Compact
		if runCompaction {
			compactedWal, err := compact(mergedWal)
			if err != nil {
				if err == errWalIntregrity {
					// This isn't nice, but saves a larger refactor for what might be entirely unused emergency
					// logic: we need to purge the log on the listRepo, otherwise on `gather` -> `Replay`, the
					// broken log will just be merged back in. We want to remove it entirely.
					r.log = []EventLog{}
				} else {
					return localReplayWal, err
				}
			}
			mergedWal = compactedWal
		}

		// Flush the gathered Wal
		if err := r.push(ctx, mergedWal, wf, ""); err != nil {
			log.Fatal(err)
		}

		// Merge into the full wal (which will be returned at the end of the function)
		fullMergedWal = merge(fullMergedWal, mergedWal)

		// Schedule a delete on the files
		wf.RemoveWals(ctx, filesToDelete)
	}

	if len(fullMergedWal) > 0 {
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
				r.push(ctx, fullMergedWal, wf, randomUUID)
			}(wf)
		}
	}

	return localReplayWal, nil
}

func buildByteWal(el []EventLog) *bytes.Buffer {
	var outputBuf bytes.Buffer

	// Write the schema ID
	if err := binary.Write(&outputBuf, binary.LittleEndian, latestWalSchemaID); err != nil {
		log.Fatal(err)
	}

	// We need to encode the eventLog separately in order to generate a checksum
	var elBuf bytes.Buffer
	enc := gob.NewEncoder(&elBuf)
	if err := enc.Encode(el); err != nil {
		log.Fatal(err) // TODO
	}

	// Then write in the compressed bytes
	zw := gzip.NewWriter(&outputBuf)
	if _, err := zw.Write(elBuf.Bytes()); err != nil {
		log.Fatal(err)
	}

	if err := zw.Close(); err != nil {
		log.Fatal(err) // TODO
	}

	return &outputBuf
}

func (r *DBListRepo) getMatchedWal(el []EventLog, wf WalFile) []EventLog {
	walFileOwnerEmail := wf.GetUUID()
	_, isWebRemote := wf.(*WebWalFile)
	isWalFileOwner := !isWebRemote || (r.email != "" && r.email == walFileOwnerEmail)

	// Only include those events which are/have been shared (this is handled via the event processed
	// cache elsewhere)
	filteredWal := []EventLog{}
	for _, e := range el {
		// Separate conditional here to prevent the need for e.getKeys lookup if not necessary
		if isWalFileOwner {
			filteredWal = append(filteredWal, e)
			continue
		}
		if e.emailHasAccess(walFileOwnerEmail) {
			filteredWal = append(filteredWal, e)
		}
	}
	return filteredWal
}

func (r *DBListRepo) push(ctx context.Context, el []EventLog, wf WalFile, randomUUID string) error {
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
	r.setProcessedWalName(randomUUID)
	if err := wf.Flush(ctx, b, randomUUID); err != nil {
		return err
	}

	return nil
}

func (r *DBListRepo) flushPartialWals(ctx context.Context, el []EventLog) {
	//log.Print("Flushing...")
	if len(el) > 0 {
		randomUUID := fmt.Sprintf("%v%v", r.uuid, generateUUID())
		r.allWalFileMut.RLock()
		defer r.allWalFileMut.RUnlock()
		for _, wf := range r.allWalFiles {
			go func(wf WalFile) {
				r.push(ctx, el, wf, randomUUID)
			}(wf)
		}
	}
}

func (r *DBListRepo) emitRemoteUpdate() {
	if r.web.isActive {
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

func (r *DBListRepo) startSync(ctx context.Context, walChan chan []EventLog) error {
	syncTriggerChan := make(chan struct{})
	// we use pushAggregateWindowChan to ensure that there is only a single pending push scheduled
	//pushAggregateWindowChan := make(chan struct{}, 1)
	//pushTriggerChan := make(chan struct{})
	pushTriggerTimer := time.NewTimer(time.Second * 0)
	gatherTriggerTimer := time.NewTimer(time.Second * 0)
	// Drain the initial timers, we want to wait for initial user input
	<-pushTriggerTimer.C
	<-gatherTriggerTimer.C

	webPingTicker := time.NewTicker(webPingInterval)
	webRefreshTicker := time.NewTicker(webRefreshInterval)
	// We set the interval to 0 because we want the initial connection establish attempt to occur ASAP
	webRefreshTicker.Reset(0)

	websocketPushEvents := make(chan websocketMessage)

	// Schedule push to all non-local walFiles
	// This is required for flushing new files that have been manually dropped into local root, and to cover other odd cases.
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
		localFileNames, err := r.LocalWalFile.GetMatchingWals(ctx, fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), walFilePattern), "*"))
		if err != nil {
			log.Fatal(err)
		}
		if len(localFileNames) > 1 {
			func() {
				r.allWalFileMut.RLock()
				defer r.allWalFileMut.RUnlock()
				for _, wf := range r.allWalFiles {
					if wf != r.LocalWalFile {
						go func(wf WalFile) { r.push(ctx, r.log, wf, "") }(wf)
					}
				}
			}()
		}
		hasRunInitialSync = true

		// Do an initial remote update to ensure any offline changes are immediately synced to the cloud
		r.emitRemoteUpdate()
	}

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
		//select {
		//// Only schedule a post-interval push if there isn't already one pending. pushAggregateWindowChan
		//// is responsible for holding pending pushes.
		//case pushAggregateWindowChan <- struct{}{}:
		//    go func() {
		//        time.Sleep(time.Second * pushIntervalSeconds)
		//        pushTriggerChan <- struct{}{}
		//        <-pushAggregateWindowChan
		//    }()
		//default:
		//}
		pushTriggerTimer.Reset(pushWaitDuration)

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

	// Run an initial load from the local walfile
	localEl, err := r.pull(ctx, []WalFile{r.LocalWalFile})
	if err != nil {
		return err
	}
	walChan <- localEl

	// Main sync event loop
	go func() {
		compactInc := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				var el []EventLog
				var err error
				select {
				case <-syncTriggerChan:
					syncWalFiles := []WalFile{}
					r.syncWalFileMut.RLock()
					for _, wf := range r.syncWalFiles {
						syncWalFiles = append(syncWalFiles, wf)
					}
					r.syncWalFileMut.RUnlock()
					if el, err = r.pull(ctx, syncWalFiles); err != nil {
						log.Fatal(err)
					}
				case <-gatherTriggerTimer.C:
					// Runs compaction every Nth gather
					runCompaction := compactInc > 0 && compactInc%compactionGatherMultiple == 0
					if el, err = r.gather(ctx, runCompaction); err != nil {
						log.Fatal(err)
					}
					compactInc++
				}

				// Rather than relying on a ticker (which will trigger the next cycle if processing time is >= the interval)
				// we set a wait interval from the end of processing. This prevents a vicious circle which could leave the
				// program with it's CPU constantly tied up, which leads to performance degradation.
				// Instead, at the end of the processing cycle, we schedule a wait period after which the next event is put
				// onto the syncTriggerChan
				go func() {
					// we block on walChan publish so it only schedules again after a Replay begins
					if len(el) > 0 {
						walChan <- el
					}
					scheduleSync(pullIntervalSeconds)
				}()
			}
		}
	}()

	// Prioritise async web start-up to minimise wait time before websocket instantiation
	// Create a loop responsible for periodic refreshing of web connections and web walfiles.
	// TODO only start this goroutine if there is actually a configured web
	go func() {
		expBackoffInterval := time.Second * 1
		var waitInterval time.Duration
		var webCtx context.Context
		var webCancel context.CancelFunc
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
				if webCancel != nil {
					webCancel()
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
					switch err.(type) {
					case authFailureError:
						scheduleSync(0) // trigger initial (local) sync cycle before returning
						return          // authFailureError signifies incorrect login details, disable web and run local only mode
					default:
						waitInterval = expBackoffInterval
						if expBackoffInterval < webRefreshInterval {
							expBackoffInterval *= 2
						}
					}
				} else {
					r.web.isActive = true
					expBackoffInterval = time.Second * 1
					waitInterval = webRefreshInterval
				}
				// Trigger web walfile sync (mostly relevant on initial start)
				scheduleSync(0)

				webCtx, webCancel = context.WithCancel(ctx)
				if r.web.isActive {
					go func() {
						for {
							wsEl, err := r.consumeWebsocket(webCtx)
							if err != nil {
								// webCancel() triggers error which we need to handle and return
								// to avoid haywire goroutines with infinite loops and CPU destruction
								return
							}
							// Rather than clogging up numerous goroutines waiting to publish
							// single item event logs to walChan, we attempt to publish to it,
							// but if there's already a wal pending, we fail back and write to
							// an aggregated log, which will then be attempted again on the next
							// incoming event
							// TODO if we get a failed publish and then no more incoming websocket
							// events, the aggregated log will never be published to walChan
							// use a secondary ticker that will wait a given period and flush if
							// needed????
							if len(wsEl) > 0 {
								go func() {
									walChan <- wsEl
								}()
							}
						}
					}()
				}

				webRefreshTicker.Reset(waitInterval)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Create a loop to deal with any collaborator cursor move events
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				e := <-r.localCursorMoveChan
				func() {
					r.webWalFileMut.RLock()
					defer r.webWalFileMut.RUnlock()
					for _, wf := range r.webWalFiles {
						go func(uuid string) {
							websocketPushEvents <- websocketMessage{
								Action:       "position",
								UUID:         uuid,
								Key:          e.listItemKey,
								UnixNanoTime: e.unixNanoTime,
							}
						}(wf.GetUUID())
					}
				}()
			}
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
				if r.web.isActive {
					func() {
						r.webWalFileMut.RLock()
						defer r.webWalFileMut.RUnlock()
						for _, wf := range r.webWalFiles {
							if matchedEventLog := r.getMatchedWal([]EventLog{e}, wf); len(matchedEventLog) > 0 {
								// There are only single events, so get the zero index
								b := buildByteWal(matchedEventLog)
								b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())
								go func(uuid string) {
									websocketPushEvents <- websocketMessage{
										Action: "wal",
										UUID:   uuid,
										Wal:    b64Wal,
									}
								}(wf.GetUUID())
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
			case <-pushTriggerTimer.C:
				// On ticks, Flush what we've aggregated to all walfiles, and then reset the
				// ephemeral log. If empty, skip.
				r.flushPartialWals(ctx, el)
				el = []EventLog{}
			case <-ctx.Done():
				// TODO create a separate timeout here? This is the only case where we don't want the parent
				// context cancellation to cancel the inflight flush
				// TODO ensure completion of this operation before completing?? maybe bring back in the stop chan
				r.flushPartialWals(context.Background(), el)
				return
			}
		}
	}()

	return nil
}

func (r *DBListRepo) finish(purge bool) error {
	// When we pull wals from remotes, we merge into our in-mem logs, but will only flush to local walfile
	// on gather. To ensure we store all logs locally, for now, we can just push the entire in-mem log to
	// the local walfile. We can remove any other files to avoid overuse of local storage.
	// TODO this can definitely be optimised (e.g. only flush a partial log of unpersisted changes, or perhaps
	// track if any new wals have been pulled, etc)
	ctx := context.Background()
	if !purge {
		localFiles, _ := r.LocalWalFile.GetMatchingWals(ctx, fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), walFilePattern), "*"))
		filesToDelete := []string{}
		// Ensure we've actually processed the files before we delete them...
		for _, fileName := range localFiles {
			if r.isWalNameProcessed(fileName) {
				filesToDelete = append(filesToDelete, fileName)
			}
		}
		r.push(ctx, r.log, r.LocalWalFile, "")
		r.LocalWalFile.RemoveWals(ctx, filesToDelete)
	} else {
		// If purge is set, we delete everything in the local walfile. This is used primarily in the wasm browser app on logout
		r.LocalWalFile.Purge()
	}

	if r.web.wsConn != nil {
		r.web.wsConn.Close(websocket.StatusNormalClosure, "")
	}
	return nil
}

// BuildWalFromPlainText accepts an io.Reader with line separated plain text, and generates a wal db file
// which is dumped in fzn root directory.
func BuildWalFromPlainText(ctx context.Context, wf WalFile, r io.Reader, isHidden bool) error {
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
	wf.Flush(ctx, b, fmt.Sprintf("%d", uuid))

	return nil
}
