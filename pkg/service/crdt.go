package service

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"container/list"
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

const latestWalSchemaID uint16 = 6

// sync intervals
const (
	pullIntervalSeconds      = 5
	pushWaitDuration         = time.Second * time.Duration(5)
	gatherWaitDuration       = time.Second * time.Duration(10)
	compactionGatherMultiple = 2
	websocketConsumeDuration = time.Millisecond * 600
	websocketPublishDuration = time.Millisecond * 600
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

type EventType uint16

// Ordering of these enums are VERY IMPORTANT as they're used for comparisons when resolving WAL merge conflicts
const (
	NullEvent EventType = iota
	AddEvent
	UpdateEvent
	MoveUpEvent
	MoveDownEvent
	ShowEvent
	HideEvent
	DeleteEvent
	PositionEvent
)

type LineFriendsSchema4 struct {
	IsProcessed bool
	Offset      int
	Emails      map[string]struct{}
}

type EventLogSchema4 struct {
	UUID, TargetUUID           uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string
	Note                       []byte
	Friends                    LineFriendsSchema4
	key, targetKey             string
}

type LineFriends struct {
	IsProcessed bool
	Offset      int
	Emails      []string
	emailsMap   map[string]struct{}
}

type EventLogSchema5 struct {
	UUID, TargetUUID           uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string
	Note                       []byte
	Friends                    LineFriends
	key, targetKey             string
}

type EventLog struct {
	UUID                           uuid // origin UUID
	VectorClock                    map[uuid]int64
	EventType                      EventType
	ListItemKey, TargetListItemKey string
	Line                           string
	Note                           []byte
	IsHidden                       bool
	Friends                        LineFriends
	cachedKey                      string
	eventDLL                       *list.List
}

func (r *DBListRepo) newEventLog(t EventType, incrementLocalVector bool) EventLog {
	if incrementLocalVector {
		r.incLocalVectorDT()
	}
	e := EventLog{
		UUID:      r.uuid,
		EventType: t,
		eventDLL:  list.New(),
	}
	// make a copy of the map
	e.VectorClock = r.getLocalVectorClockCopy()
	return e
}

func (r *DBListRepo) newEventLogFromListItem(t EventType, item *ListItem, incrementLocalVector bool) EventLog {
	e := r.newEventLog(t, incrementLocalVector)
	e.ListItemKey = item.key
	if item.matchChild != nil {
		e.TargetListItemKey = item.matchChild.key
	}
	e.Line = item.rawLine
	e.Note = item.Note
	e.IsHidden = item.IsHidden
	return e
}

func (r *DBListRepo) getLocalVectorClockCopy() map[uuid]int64 {
	clock := make(map[uuid]int64)
	for k, v := range r.vectorClock {
		clock[k] = v
	}
	return clock
}

func (e *EventLog) setVectorDT(id uuid, dt int64) {
	e.VectorClock[id] = dt
}

func (e *EventLog) getLocalVectorDT() int64 {
	return e.VectorClock[e.UUID]
}

func (e *EventLog) setLocalVectorDT(dt int64) {
	e.setVectorDT(e.UUID, dt)
}

func (e *EventLog) key() string {
	if e.cachedKey == "" {
		e.cachedKey = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.getLocalVectorDT()))
	}
	return e.cachedKey
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
		return err
	}
	defer f.Close()
	if _, err := f.Write(b.Bytes()); err != nil {
		return err
	}
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
func (r *DBListRepo) getFriendsFromLine(line string, existingFriends []string) ([]string, bool) {
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

	// add existing friends, if there are any
	for _, f := range existingFriends {
		friendsMap[f] = struct{}{}
	}

	for _, w := range strings.Split(line, " ") {
		// We only force lower case when comparing to the friends cache, as we need to maintain case (for now)
		// for purposes of matching on the string for repositiong. The email is lower-cased after repositioning.
		if len(w) > 1 && rune(w[0]) == '@' && r.friends[strings.ToLower(w[1:])] != nil {
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

func (r *DBListRepo) checkIfConfigLine(line string) ([]string, bool) {
	words := strings.Fields(line)
	if len(words) == 3 && words[0] == "fzn_cfg:friend" && rune(words[2][0]) == '@' && words[2][1:] == r.email {
		return words, true
	}
	return words, false
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
	if words, isConfig := r.checkIfConfigLine(line); isConfig {
		f = strings.ToLower(words[1])
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
		// Retrieve existing friends from the item, if it already exists. This prevents the following bug:
		// - Item is shared with `@foo@bar.com`
		// - `fzn_cfg:friend foo@bar.com` is removed
		// - Update is made on previously shared line
		// - `@foo@bar.com` is appended to the Line() portion of the string, because the friend is no longer present in
		//   the r.friends cache
		var existingFriends []string
		if item, exists := r.listItemCache[e.ListItemKey]; exists {
			existingFriends = item.friends.Emails
		}
		// If there are no friends, return early
		friends, _ = r.getFriendsFromLine(e.Line, existingFriends)
		if len(friends) == 0 {
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
	for i, f := range friends {
		friendString += " @"
		lowerF := strings.ToLower(f)
		friendString += lowerF
		friends[i] = lowerF // override the friends slice to ensure lower case
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
				// If the friend does not exist, add to the end of the friendsOrdered slice
				r.friendsOrdered = append(r.friendsOrdered, friendToAdd)
			}

			if dtLastChange, exists := friendItems[e.ListItemKey]; !exists || e.getLocalVectorDT() > dtLastChange {
				r.friends[friendToAdd][e.ListItemKey] = e.getLocalVectorDT()
				r.AddWalFile(
					&WebWalFile{
						uuid: friendToAdd,
						web:  r.web,
					},
					false,
				)
			}
		}
		if friendToRemove != "" && friendToRemove != r.email {
			// We only delete and emit the cloud event if the friend exists (which it always should tbf)
			// Although we ignore the delete if the event timestamp is older than the latest known cache state.
			if friendItems, friendExists := r.friends[friendToRemove]; friendExists {
				if dtLastChange, exists := friendItems[e.ListItemKey]; exists && e.getLocalVectorDT() > dtLastChange {
					delete(r.friends[friendToRemove], e.ListItemKey)
					if len(r.friends[friendToRemove]) == 0 {
						delete(r.friends, friendToRemove)
						// remove the friend from the friendsOrdered list
						for i, f := range r.friendsOrdered {
							if f == friendToRemove {
								r.friendsOrdered = append(r.friendsOrdered[:i], r.friendsOrdered[i+1:]...)
								break
							}
						}
						r.DeleteWalFile(friendToRemove)
					}
				}
			}
		}
		if (friendToAdd != "" || friendToRemove != "") && r.friendsMostRecentChangeDT < e.getLocalVectorDT() {
			r.friendsMostRecentChangeDT = e.getLocalVectorDT()
		}
	}()
}

func (r *DBListRepo) getOrCreateListItem(key string) *ListItem {
	if key == "" {
		return nil
	}
	item, exists := r.listItemCache[key]
	if exists {
		return item
	}
	item = &ListItem{
		key: key,
	}
	r.listItemCache[key] = item
	return item
}

func vectorClockBefore(a, b map[uuid]int64) bool {
	// for one event to have happened before another event, all the elements of its vector need to be
	// smaller or equal to the matching elements in the other vector.
	// therefore, if _any_ of the cached event counters are <= the remote counterpart, we need
	// to process the event. Or in other words, if all of the cached event counters are greater than
	// the counterpart, we can skip the event

	isEqual := len(a) == len(b)

	for id, aDT := range a {
		if bDT, bCachedExists := b[id]; !bCachedExists || aDT > bDT {
			return false
		} else if isEqual && aDT < bDT {
			isEqual = false
		}
	}

	if isEqual {
		return false
	}

	return true
}

func (a *EventLog) before(b EventLog) bool {
	return vectorClockBefore(a.VectorClock, b.VectorClock)
}

func (r *DBListRepo) itemIsLive(item *ListItem) bool {
	var k string
	if item != nil {
		k = item.key
	}

	latestAddEvent, isAdded := r.addEventSet[k]
	latestDeleteEvent, isDeleted := r.deleteEventSet[k]
	if isAdded && (!isDeleted || (isAdded && latestDeleteEvent.before(latestAddEvent))) {
		return true
	}
	return false
}

func (r *DBListRepo) processEventLog(e EventLog) (*ListItem, error) {
	item := r.getOrCreateListItem(e.ListItemKey)

	var eventCache map[string]EventLog
	switch e.EventType {
	case UpdateEvent:
		eventCache = r.addEventSet
	case DeleteEvent:
		eventCache = r.deleteEventSet
	case PositionEvent:
		eventCache = r.positionEventSet
	}

	// Check the event cache and skip if the event is older than the most-recently processed
	if ce, exists := eventCache[e.ListItemKey]; exists {
		if e.before(ce) {
			return item, nil
		}
	}

	// Add the event to the cache after the pre-existence checks above
	eventCache[e.ListItemKey] = e

	// Iterate over all counters in the event clock and update the local vector representation for each newer one
	// IMPORTANT: this needs to occur **after** the equality check above
	for id, remoteDT := range e.VectorClock {
		if localDT, exists := r.getVectorDT(id); !exists || localDT < remoteDT {
			r.setVectorDT(id, remoteDT)
		}
	}

	r.generateFriendChangeEvents(e, item)

	var err error
	switch e.EventType {
	case UpdateEvent:
		err = updateItemFromEvent(item, e, r.email)
	//case DeleteEvent:
	//r.crdt.del(e)
	case PositionEvent:
		r.crdt.add(e)
	}

	r.listItemCache[e.ListItemKey] = item

	return item, err
}

func updateItemFromEvent(item *ListItem, e EventLog, email string) error {
	// TODO migration no longer splits updates for Line/Note, HANDLE BACKWARDS COMPATIBILITY
	item.rawLine = e.Line
	item.Note = e.Note
	item.IsHidden = e.IsHidden

	// item.friends.emails is a map, which we only ever want to OR with to aggregate
	mergedEmailMap := make(map[string]struct{})
	for _, e := range item.friends.Emails {
		mergedEmailMap[e] = struct{}{}
	}
	for _, e := range e.Friends.Emails {
		mergedEmailMap[e] = struct{}{}
	}
	item.friends.IsProcessed = e.Friends.IsProcessed
	item.friends.Offset = e.Friends.Offset

	emails := []string{}
	for e := range mergedEmailMap {
		if e != email {
			emails = append(emails, e)
		}
	}
	sort.Strings(emails)
	item.friends.Emails = emails

	return nil
}

func (r *DBListRepo) getLatestVectorClock(item *ListItem) map[uuid]int64 {
	// if the item is being newly added, the add event comes before the position event, so default to the add event if position is not
	// yet set
	latestVectorClockEvent, exists := r.positionEventSet[item.key]
	if !exists {
		latestVectorClockEvent = r.addEventSet[item.key]
	}
	return latestVectorClockEvent.VectorClock
}

// Replay updates listItems based on the current state of the local WAL logs. It generates or updates the linked list
// which is attached to DBListRepo.Root
func (r *DBListRepo) Replay(partialWal []EventLog) error {
	// No point merging with an empty partialWal
	if len(partialWal) == 0 {
		return nil
	}

	for _, e := range partialWal {
		r.processEventLog(e)
	}

	return nil
}

func getNextEventLogFromWalFile(r io.Reader, schemaVersionID uint16) (*EventLog, error) {
	el := EventLog{}

	switch schemaVersionID {
	case 3:
		wi := walItemSchema2{}
		err := binary.Read(r, binary.LittleEndian, &wi)
		if err != nil {
			return nil, err
		}

		el.UUID = wi.UUID
		el.EventType = wi.EventType
		el.ListItemKey = strconv.Itoa(int(wi.UUID)) + ":" + strconv.Itoa(int(wi.ListItemCreationTime))
		el.TargetListItemKey = strconv.Itoa(int(wi.TargetUUID)) + ":" + strconv.Itoa(int(wi.TargetListItemCreationTime))
		el.setLocalVectorDT(wi.EventTime)

		line := make([]byte, wi.LineLength)
		err = binary.Read(r, binary.LittleEndian, &line)
		if err != nil {
			return nil, err
		}
		el.Line = string(line)

		if wi.NoteExists {
			el.Note = []byte{}
		}
		if wi.NoteLength > 0 {
			note := make([]byte, wi.NoteLength)
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

func getOldEventLogKeys(i interface{}) (string, string) {
	var key, targetKey string
	switch e := i.(type) {
	case EventLogSchema4:
		key = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.ListItemCreationTime))
		targetKey = strconv.Itoa(int(e.TargetUUID)) + ":" + strconv.Itoa(int(e.TargetListItemCreationTime))
	case EventLogSchema5:
		key = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.ListItemCreationTime))
		targetKey = strconv.Itoa(int(e.TargetUUID)) + ":" + strconv.Itoa(int(e.TargetListItemCreationTime))
	}
	return key, targetKey
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
		switch walSchemaVersionID {
		case 1, 2:
			if _, err := io.Copy(pw, raw); err != nil {
				errChan <- err
			}
		default:
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

	switch walSchemaVersionID {
	case 1, 2, 3:
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
	case 4:
		var oel []EventLogSchema4
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&oel); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}
		for _, oe := range oel {
			key, targetKey := getOldEventLogKeys(oe)
			e := EventLog{
				UUID:              oe.UUID,
				ListItemKey:       key,
				TargetListItemKey: targetKey,
				EventType:         oe.EventType,
				Line:              oe.Line,
				Note:              oe.Note,
				Friends: LineFriends{
					IsProcessed: oe.Friends.IsProcessed,
					Offset:      oe.Friends.Offset,
					emailsMap:   oe.Friends.Emails,
				},
			}
			e.setLocalVectorDT(oe.UnixNanoTime)
			for f := range oe.Friends.Emails {
				e.Friends.Emails = append(e.Friends.Emails, f)
			}
			sort.Strings(e.Friends.Emails)
			el = append(el, e)
		}
	case 5:
		var oel []EventLogSchema5
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&oel); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}
		for _, oe := range oel {
			key, targetKey := getOldEventLogKeys(oe)
			e := EventLog{
				UUID:              oe.UUID,
				ListItemKey:       key,
				TargetListItemKey: targetKey,
				EventType:         oe.EventType,
				Line:              oe.Line,
				Note:              oe.Note,
				Friends:           oe.Friends,
			}
			e.setLocalVectorDT(oe.UnixNanoTime)
			el = append(el, e)
		}
	case 6:
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
	if event1.getLocalVectorDT() < event2.getLocalVectorDT() ||
		event1.getLocalVectorDT() == event2.getLocalVectorDT() && event1.UUID < event2.UUID {
		return leftEventOlder
	} else if event2.getLocalVectorDT() < event1.getLocalVectorDT() ||
		event2.getLocalVectorDT() == event1.getLocalVectorDT() && event2.UUID < event1.UUID {
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
		if _, err := f.Write([]byte(i.rawLine + "\n")); err != nil {
			return err
		}
	}
	return nil
}

// This function is currently unused
//func (r *DBListRepo) generatePartialView(ctx context.Context, matchItems []ListItem) error {
//    wal := []EventLog{}
//    //now := time.Now().AddDate(-1, 0, 0).UnixNano()
//    now := int64(1) // TODO remove this - it's to ensure consistency to enable file diffs

//    // Iterate from oldest to youngest
//    for i := len(matchItems) - 1; i >= 0; i-- {
//        item := matchItems[i]
//        el := EventLog{
//            UUID:                       item.originUUID,
//            TargetUUID:                 0,
//            ListItemCreationTime:       item.creationTime,
//            TargetListItemCreationTime: 0,
//            UnixNanoTime:               now,
//            EventType:                  AddEvent,
//            Line:                       item.rawLine,
//            Note:                       item.Note,
//        }
//        wal = append(wal, el)
//        now++

//        if item.IsHidden {
//            el.EventType = HideEvent
//            el.UnixNanoTime = now
//            wal = append(wal, el)
//            now++
//        }
//    }

//    b, _ := buildByteWal(wal)
//    viewName := fmt.Sprintf(viewFilePattern, time.Now().UnixNano())
//    r.LocalWalFile.Flush(ctx, b, viewName)
//    log.Fatalf("N list generated events: %d", len(wal))
//    return nil
//}

func (r *DBListRepo) setProcessedWalChecksum(checksum string) {
	r.processedWalChecksumLock.Lock()
	defer r.processedWalChecksumLock.Unlock()
	r.processedWalChecksums[checksum] = struct{}{}
}

func (r *DBListRepo) isWalChecksumProcessed(checksum string) bool {
	r.processedWalChecksumLock.Lock()
	defer r.processedWalChecksumLock.Unlock()
	_, exists := r.processedWalChecksums[checksum]
	return exists
}

func (r *DBListRepo) pull(ctx context.Context, walFiles []WalFile, c chan []EventLog) (map[WalFile]map[string]struct{}, error) {
	// Concurrently gather all new wal UUIDs for all walFiles, tracking each in a map against a walFile key
	newWalMutex := sync.Mutex{}
	newWalfileMap := make(map[WalFile]map[string]struct{})
	var wg sync.WaitGroup
	for _, wf := range walFiles {
		wg.Add(1)
		go func(wf WalFile) {
			defer wg.Done()
			filePathPattern := path.Join(wf.GetRoot(), "wal_*.db")
			newWals, err := wf.GetMatchingWals(ctx, filePathPattern)
			if err != nil {
				//log.Fatal(err)
				return
			}
			newWalMutex.Lock()
			defer newWalMutex.Unlock()
			newWalMap := make(map[string]struct{})
			for _, f := range newWals {
				newWalMap[f] = struct{}{}
			}
			newWalfileMap[wf] = newWalMap
		}(wf)
	}
	wg.Wait()

	// We then run CPU bound ops in a single thread to mitigate excessive CPU usage (particularly as an N WalFiles
	// is unbounded)
	// We merge all wals before publishing to the walchan as each time the main app event loop consumes from walchan,
	// it blocks user input and creates a poor user experience.
	//nameC := make(chan string)
	//go func() {
	for wf, newWals := range newWalfileMap {
		for newWal := range newWals {
			//if !r.isWalChecksumProcessed(newWal) {
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

			if len(newWfWal) > 0 {
				go func() {
					c <- newWfWal
					// Add to the processed cache after we've successfully pulled it
					// TODO strictly we should only set processed once it's successfully merged and displayed to client
					//r.setProcessedWalChecksum(newWal)
				}()
			}
			//nameC <- newWal
			//}
			//r.setProcessedWalChecksum(newWal)
		}
	}
	//close(nameC)
	//}()

	//names := []string{}
	//for n := range nameC {
	//    names = append(names, n)
	//}

	//go func() {
	//    time.Sleep(time.Second * 3)
	//    log.Print(names)
	//}()

	return newWalfileMap, nil
}

//func (r *DBListRepo) gather(ctx context.Context, runCompaction bool) ([]EventLog, error) {
//    //log.Print("Gathering...")
//    // Create a new list so we don't have to keep the lock on the mutex for too long
//    r.syncWalFileMut.RLock()
//    syncWalFiles := []WalFile{}
//    for _, wf := range r.syncWalFiles {
//        syncWalFiles = append(syncWalFiles, wf)
//    }
//    r.syncWalFileMut.RUnlock()

//    r.allWalFileMut.RLock()
//    allWalFiles := []WalFile{}
//    for _, wf := range r.allWalFiles {
//        allWalFiles = append(allWalFiles, wf)
//    }
//    r.allWalFileMut.RUnlock()

//    fullWal, syncWalfileWalNameMap, err := r.pull(ctx, syncWalFiles)
//    if err != nil {
//        return []EventLog{}, err
//    }

//    fullByteWal, err := buildByteWal(fullWal)
//    if err != nil {
//        return []EventLog{}, err
//    }

//    var wg sync.WaitGroup
//    for _, wf := range allWalFiles {
//        wg.Add(1)
//        go func(wf WalFile) {
//            defer wg.Done()

//            var byteWal *bytes.Buffer

//            walNameMap, isOwned := syncWalfileWalNameMap[wf]

//            if isOwned {
//                byteWal = fullByteWal
//            }

//            // Push to ALL walFiles
//            checksum, err := r.push(ctx, wf, fullWal, byteWal)
//            if err != nil {
//                return
//            }

//            // Only delete processed files from syncWalfiles - e.g. walfiles we own
//            if isOwned {
//                delete(walNameMap, checksum)

//                filesToDelete := []string{}
//                for f := range walNameMap {
//                    filesToDelete = append(filesToDelete, f)
//                }
//                // Schedule a delete on the files
//                if len(filesToDelete) > 0 {
//                    wf.RemoveWals(ctx, filesToDelete)
//                }
//            }
//        }(wf)
//    }
//    wg.Wait()

//    return fullWal, nil
//}

func buildByteWal(el []EventLog) (*bytes.Buffer, error) {
	var outputBuf bytes.Buffer

	// Write the schema ID
	if err := binary.Write(&outputBuf, binary.LittleEndian, latestWalSchemaID); err != nil {
		return nil, err
	}

	// We need to encode the eventLog separately in order to generate a checksum
	var elBuf bytes.Buffer
	enc := gob.NewEncoder(&elBuf)
	if err := enc.Encode(el); err != nil {
		return nil, err
	}

	// Then write in the compressed bytes
	zw := gzip.NewWriter(&outputBuf)
	if _, err := zw.Write(elBuf.Bytes()); err != nil {
		return nil, err
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return &outputBuf, nil
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

func (r *DBListRepo) push(ctx context.Context, wf WalFile, el []EventLog, byteWal *bytes.Buffer) (string, error) {
	if byteWal == nil {
		// Apply any filtering based on Push match configuration
		el = r.getMatchedWal(el, wf)

		// Return for empty wals
		if len(el) == 0 {
			return "", nil
		}

		var err error
		if byteWal, err = buildByteWal(el); err != nil {
			return "", err
		}

	}
	checksum := fmt.Sprintf("%x", md5.Sum(byteWal.Bytes()))

	// Add it straight to the cache to avoid processing it in the future
	// This needs to be done PRIOR to flushing to avoid race conditions
	// (as pull is done in a separate thread of control, and therefore we might try
	// and pull our own pushed wal)
	r.setProcessedWalChecksum(checksum)
	if err := wf.Flush(ctx, byteWal, checksum); err != nil {
		return checksum, err
	}

	return checksum, nil
}

func (r *DBListRepo) flushPartialWals(ctx context.Context, wal []EventLog, waitForCompletion bool) {
	if len(wal) > 0 {
		fullByteWal, err := buildByteWal(wal)
		if err != nil {
			return
		}
		var wg sync.WaitGroup
		r.allWalFileMut.RLock()
		defer r.allWalFileMut.RUnlock()
		for _, wf := range r.allWalFiles {
			if waitForCompletion {
				wg.Add(1)
			}
			var byteWal *bytes.Buffer
			if _, isOwned := r.syncWalFiles[wf.GetUUID()]; isOwned {
				byteWal = fullByteWal
			}
			go func(wf WalFile) {
				if waitForCompletion {
					defer wg.Done()
				}
				r.push(ctx, wf, wal, byteWal)
			}(wf)
		}
		if waitForCompletion {
			wg.Wait()
		}
	}
}

func (r *DBListRepo) updateActiveFriendsMap(activeFriends, pendingFriends []string, updateChan chan interface{}) {
	activeFriendsMap := make(map[string]struct{})
	pendingFriendsMap := make(map[string]struct{})
	for _, f := range activeFriends {
		activeFriendsMap[f] = struct{}{}
	}
	for _, f := range pendingFriends {
		pendingFriendsMap[f] = struct{}{}
	}
	r.activeFriendsMapLock.Lock()
	defer r.activeFriendsMapLock.Unlock()
	r.activeFriends = activeFriendsMap
	r.pendingFriends = pendingFriendsMap
	go func() {
		updateChan <- struct{}{}
	}()
}

func (r *DBListRepo) emitRemoteUpdate(updateChan chan interface{}) {
	if r.web.isActive {
		// We need to wrap the friendsMostRecentChangeDT comparison check, as the friend map update
		// and subsequent friendsMostRecentChangeDT update needs to be an atomic operation
		r.friendsUpdateLock.RLock()
		defer r.friendsUpdateLock.RUnlock()
		if r.friendsLastPushDT == 0 || r.friendsLastPushDT < r.friendsMostRecentChangeDT {
			u, _ := url.Parse(apiURL)
			u.Path = path.Join(u.Path, "remote")

			remote := WebRemote{
				Emails:       r.friendsOrdered,
				DTLastChange: r.friendsMostRecentChangeDT,
			}
			go func() {
				if resp, err := r.web.postRemote(&remote, u); err == nil {
					r.updateActiveFriendsMap(resp.ActiveFriends, resp.PendingFriends, updateChan)
				}
			}()
			r.friendsLastPushDT = r.friendsMostRecentChangeDT
		}
	}
}

func (r *DBListRepo) startSync(ctx context.Context, replayChan chan []EventLog, inputEvtsChan chan interface{}) error {
	// syncTriggerChan is buffered, as the producer is called in the same thread as the consumer (to ensure a minimum of the
	// specified wait duration)
	syncTriggerChan := make(chan struct{}, 1)

	//gatherTriggerTimer := time.NewTimer(gatherWaitDuration)
	// Drain the initial push timer, we want to wait for initial user input
	// We do however schedule an initial iteration of a gather to ensure all local state (including any files manually
	// dropped in to the root directory, etc) are flushed
	<-r.pushTriggerTimer.C

	webPingTicker := time.NewTicker(webPingInterval)
	webRefreshTicker := time.NewTicker(webRefreshInterval)
	// We set the interval to 0 because we want the initial connection establish attempt to occur ASAP
	webRefreshTicker.Reset(time.Millisecond * 1)

	websocketPushEvents := make(chan websocketMessage)

	scheduleSync := func() {
		select {
		case syncTriggerChan <- struct{}{}:
		default:
		}
	}
	schedulePush := func() {
		r.pushTriggerTimer.Reset(pushWaitDuration)
		//gatherTriggerTimer.Reset(gatherWaitDuration)
	}

	// Run an initial load from the local walfile
	if _, err := r.pull(ctx, []WalFile{r.LocalWalFile}, replayChan); err != nil {
		return err
	}

	// Main sync event loop
	go func() {
		//reporderInc := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				//var c chan []EventLog
				//var el []EventLog
				var err error
				select {
				case <-syncTriggerChan:
					syncWalFiles := []WalFile{}
					func() {
						r.syncWalFileMut.RLock()
						defer r.syncWalFileMut.RUnlock()
						for _, wf := range r.syncWalFiles {
							syncWalFiles = append(syncWalFiles, wf)
						}
					}()
					if _, err = r.pull(ctx, syncWalFiles, replayChan); err != nil {
						log.Fatal(err)
					}
					r.hasSyncedRemotes = true
					//c = replayChan
					//case <-gatherTriggerTimer.C:
					//    // Runs compaction every Nth gather
					//    runCompaction := reporderInc > 0 && reporderInc%compactionGatherMultiple == 0
					//    if el, err = r.gather(ctx, runCompaction); err != nil {
					//        log.Fatal(err)
					//    }
					//    reporderInc++
					//    c = reorderAndReplayChan
				}

				// Rather than relying on a ticker (which will trigger the next cycle if processing time is >= the interval)
				// we set a wait interval from the end of processing. This prevents a vicious circle which could leave the
				// program with it's CPU constantly tied up, which leads to performance degradation.
				// Instead, at the end of the processing cycle, we schedule a wait period after which the next event is put
				// onto the syncTriggerChan
				//go func() {
				//    // we block on replayChan publish so it only schedules again after a Replay begins
				//    if len(el) > 0 {
				//        c <- el
				//    }
				//}()
				time.Sleep(time.Second * time.Duration(pullIntervalSeconds))
				scheduleSync()
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
					if pong, err := r.web.ping(); err != nil {
						r.web.isActive = false
						webRefreshTicker.Reset(time.Millisecond * 1)
					} else {
						r.updateActiveFriendsMap(pong.ActiveFriends, pong.PendingFriends, inputEvtsChan)
					}
				}
			case m := <-websocketPushEvents:
				if r.web.isActive {
					r.web.pushWebsocket(m)
				}
			case <-webRefreshTicker.C:
				r.hasSyncedRemotes = false
				if webCancel != nil {
					webCancel()
				}
				// Close off old websocket connection
				// Nil check because initial instantiation also occurs async in this loop (previous it was sync on startup)
				if r.web.wsConn != nil {
					r.web.wsConn.Close(websocket.StatusNormalClosure, "")
				}
				// Send a state update here to ensure "offline" state is displayed if relevant
				inputEvtsChan <- SyncEvent{}
				// Start new one
				err := r.registerWeb()
				if err != nil {
					r.web.isActive = false
					switch err.(type) {
					case authFailureError:
						return // authFailureError signifies incorrect login details, disable web and run local only mode
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

					// Send immediate `ping` to populate ActiveFriends prior to initial sync
					pong, _ := r.web.ping()
					r.updateActiveFriendsMap(pong.ActiveFriends, pong.PendingFriends, inputEvtsChan)
				}
				// Trigger web walfile sync (mostly relevant on initial start)
				scheduleSync()

				if r.web.isActive {
					webCtx, webCancel = context.WithCancel(ctx)
					wsConsAgg := []EventLog{}
					wsConsChan := make(chan []EventLog)
					go func() {
						for {
							wsEv, err := r.consumeWebsocket(webCtx)
							if err != nil {
								// webCancel() triggers error which we need to handle and return
								// to avoid haywire goroutines with infinite loops and CPU destruction
								return
							}
							wsConsChan <- wsEv
						}
					}()
					wsFlushTicker := time.NewTicker(websocketConsumeDuration)
					go func() {
						for {
							select {
							case wsEv := <-wsConsChan:
								wsConsAgg = merge(wsConsAgg, wsEv)
							case <-wsFlushTicker.C:
								if len(wsConsAgg) > 0 {
									replayChan <- wsConsAgg
									wsConsAgg = []EventLog{}
								}
							case <-webCtx.Done():
								return
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
	var flushAgg, wsPubAgg []EventLog
	if !r.isTest {
		go func() {
			for {
				select {
				case e := <-r.eventsChan:
					wsPubAgg = merge(wsPubAgg, []EventLog{e})
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wsPublishTicker := time.NewTicker(websocketPublishDuration)
	go func() {
		for {
			// The events chan contains single events. We want to aggregate them between intervals
			// and then emit them in batches, for great efficiency gains.
			select {
			case <-wsPublishTicker.C:
				if len(wsPubAgg) == 0 {
					continue
				}
				r.hasUnflushedEvents = true
				// Write in real time to the websocket, if present
				if r.web.isActive {
					func() {
						r.webWalFileMut.RLock()
						defer r.webWalFileMut.RUnlock()
						for _, wf := range r.webWalFiles {
							if matchedEventLog := r.getMatchedWal(wsPubAgg, wf); len(matchedEventLog) > 0 {
								// There are only single events, so get the zero index
								b, _ := buildByteWal(matchedEventLog)
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
					r.emitRemoteUpdate(inputEvtsChan)
				}
				// Add to an ephemeral log
				flushAgg = merge(flushAgg, wsPubAgg)
				wsPubAgg = []EventLog{}
				// Trigger an aggregated push
				schedulePush()
				inputEvtsChan <- SyncEvent{}
			case <-r.pushTriggerTimer.C:
				// On ticks, Flush what we've aggregated to all walfiles, and then reset the
				// ephemeral log. If empty, skip.
				r.flushPartialWals(ctx, flushAgg, false)
				flushAgg = []EventLog{}
				r.hasUnflushedEvents = false
				inputEvtsChan <- SyncEvent{}
			case <-ctx.Done():
				r.flushPartialWals(context.Background(), flushAgg, true)
				go func() {
					r.finalFlushChan <- struct{}{}
				}()
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
	if !purge {
		//ctx := context.Background()
		// TODO push full representation
		//checksum, err := r.push(ctx, r.LocalWalFile, []EventLog{}, nil)
		//if err != nil {
		//    return err
		//}
		//localFiles, _ := r.LocalWalFile.GetMatchingWals(ctx, fmt.Sprintf(path.Join(r.LocalWalFile.GetRoot(), walFilePattern), "*"))
		//if len(localFiles) > 0 {
		//    filesToDelete := make([]string, len(localFiles)-1)
		//    for _, f := range localFiles {
		//        if f != checksum {
		//            filesToDelete = append(filesToDelete, f)
		//        }
		//    }
		//    r.LocalWalFile.RemoveWals(ctx, filesToDelete)
		//}
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
	var lamportTimestamp int64
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		key := strconv.Itoa(int(uuid)) + ":" + strconv.Itoa(int(lamportTimestamp))
		e := EventLog{
			UUID:        uuid,
			EventType:   AddEvent,
			ListItemKey: key,
			Line:        line,
		}
		e.setLocalVectorDT(lamportTimestamp)
		el = append(el, e)
		lamportTimestamp++

		e = EventLog{
			UUID:        uuid,
			EventType:   HideEvent,
			ListItemKey: key,
		}
		if isHidden {
			el = append(el, e)
			lamportTimestamp++
		}
	}

	b, _ := buildByteWal(el)
	wf.Flush(ctx, b, fmt.Sprintf("%d", uuid))

	return nil
}

func (r *DBListRepo) TestPullLocal(c chan []EventLog) error {
	//c := make(chan []EventLog)
	if _, err := r.pull(context.Background(), []WalFile{r.LocalWalFile}, c); err != nil {
		return err
	}
	//go func() {
	//for {
	//    wal := <-c
	//    //s := fmt.Sprintf("%v", wal)
	//    runtime.Breakpoint()
	//    //fmt.Printf(s)
	//    r.Replay(wal)
	//}
	//}()
	//runtime.Breakpoint()
	return nil
}
