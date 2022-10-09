package service

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path"
	"path/filepath"
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

// IMPORTANT: bump cloud version
const LatestWalSchemaID uint16 = 7

// sync intervals
const (
	pullInterval             = time.Second * 5
	pushInterval             = time.Second * 5
	gatherInterval           = time.Second * 10
	websocketConsumeInterval = time.Millisecond * 600
	websocketPublishInterval = time.Millisecond * 600
)

func generateUUID() uuid {
	return uuid(rand.Uint32())
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

type LineFriends struct {
	IsProcessed bool
	Offset      int
	Emails      []string
	emailsMap   map[string]struct{}
}

type EventLog struct {
	UUID                           uuid // origin UUID
	LamportTimestamp               int64
	EventType                      EventType
	ListItemKey, TargetListItemKey string
	Line                           string
	Note                           []byte
	IsHidden                       bool
	Friends                        LineFriends
	cachedKey                      string
}

func (r *DBListRepo) newEventLog(t EventType) EventLog {
	return EventLog{
		UUID:             r.uuid,
		LamportTimestamp: r.currentLamportTimestamp,
		EventType:        t,
	}
}

func (r *DBListRepo) newEventLogFromListItem(t EventType, item *ListItem) EventLog {
	e := r.newEventLog(t)
	e.ListItemKey = item.key
	if item.matchChild != nil {
		e.TargetListItemKey = item.matchChild.key
	}
	e.Line = item.rawLine
	e.Note = item.Note
	e.IsHidden = item.IsHidden
	return e
}

func (e *EventLog) key() string {
	if e.cachedKey == "" {
		e.cachedKey = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.LamportTimestamp))
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
		// - Item `fzn_cfg:friend foo@bar.com` is deleted
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

			if dtLastChange, exists := friendItems[e.ListItemKey]; !exists || e.LamportTimestamp > dtLastChange {
				r.friends[friendToAdd][e.ListItemKey] = e.LamportTimestamp
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
				if dtLastChange, exists := friendItems[e.ListItemKey]; exists && e.LamportTimestamp > dtLastChange {
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
		if (friendToAdd != "" || friendToRemove != "") && r.friendsMostRecentChangeDT < e.LamportTimestamp {
			r.friendsMostRecentChangeDT = e.LamportTimestamp
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

func (a *EventLog) before(b EventLog) bool {
	return leftEventOlder == checkEquality(*a, b)
}

func (r *DBListRepo) processEventLog(e EventLog) (*ListItem, error) {
	item := r.getOrCreateListItem(e.ListItemKey)

	var eventCache map[string]EventLog
	switch e.EventType {
	case UpdateEvent:
		eventCache = r.crdt.addEventSet
	case DeleteEvent:
		eventCache = r.crdt.deleteEventSet
	case PositionEvent:
		eventCache = r.crdt.positionEventSet
	}

	// Check the event cache and skip if the event is older than the most-recently processed
	if ce, exists := eventCache[e.ListItemKey]; exists {
		if e.before(ce) {
			return item, nil
		}
	}

	// Add the event to the cache after the pre-existence checks above
	eventCache[e.ListItemKey] = e

	// Local lamport timestamp is ONLY incremented here
	if r.currentLamportTimestamp <= e.LamportTimestamp {
		r.currentLamportTimestamp = e.LamportTimestamp + 1
	}

	r.generateFriendChangeEvents(e, item)

	var err error
	switch e.EventType {
	case UpdateEvent:
		err = updateItemFromEvent(item, e, r.email)
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

// BuildFromFileTreeSchema parses wals with schema v7 or higher
func BuildFromFileTreeSchema(walSchemaVersionID uint16, raw io.Reader) ([]EventLog, uint16, error) {
	// passing walSchemaVersionID as 0 tells this function that the ID has yet to be retrieved from the Reader,
	// and therefore should be here.
	if walSchemaVersionID == 0 {
		if err := binary.Read(raw, binary.LittleEndian, &walSchemaVersionID); err != nil {
			if err == io.EOF {
				return []EventLog{}, walSchemaVersionID, nil
			}
			return []EventLog{}, walSchemaVersionID, err
		}
	}

	var el []EventLog
	pr, pw := io.Pipe()
	errChan := make(chan error, 1)
	go func() {
		defer pw.Close()
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
		errChan <- nil
	}()

	switch walSchemaVersionID {
	case 7:
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&el); err != nil {
			return el, walSchemaVersionID, err
		}
		if err := <-errChan; err != nil {
			return el, walSchemaVersionID, err
		}
	}

	return el, walSchemaVersionID, nil
}

func (r *DBListRepo) buildFromFile(raw io.Reader) ([]EventLog, error) {
	var walSchemaVersionID uint16
	if err := binary.Read(raw, binary.LittleEndian, &walSchemaVersionID); err != nil {
		if err == io.EOF {
			return []EventLog{}, nil
		}
		return []EventLog{}, err
	}

	if walSchemaVersionID <= 6 {
		return r.legacyBuildFromFile(walSchemaVersionID, raw)
	}

	el, _, err := BuildFromFileTreeSchema(walSchemaVersionID, raw)
	return el, err
}

const (
	eventsEqual int = iota
	leftEventOlder
	rightEventOlder
)

func checkEquality(event1 EventLog, event2 EventLog) int {
	if event1.LamportTimestamp < event2.LamportTimestamp ||
		event1.LamportTimestamp == event2.LamportTimestamp && event1.UUID < event2.UUID {
		return leftEventOlder
	} else if event2.LamportTimestamp < event1.LamportTimestamp ||
		event2.LamportTimestamp == event1.LamportTimestamp && event2.UUID < event1.UUID {
		return rightEventOlder
	}
	return eventsEqual
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

//    b, _ := BuildByteWal(wal)
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

func (r *DBListRepo) pull(ctx context.Context, walFiles []WalFile, replayChan chan namedWal) error {
	type wfWalPair struct {
		wf      WalFile
		newWals []string
	}

	nameChan := make(chan wfWalPair)
	go func() {
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
				nameChan <- wfWalPair{wf, newWals}
			}(wf)
		}
		wg.Wait()
		close(nameChan)
	}()

	var wg sync.WaitGroup
	for n := range nameChan {
		wf := n.wf
		newWals := n.newWals
		for _, newWal := range newWals {
			if !r.isWalChecksumProcessed(newWal) {
				pr, pw := io.Pipe()
				go func() {
					defer pw.Close()
					if err := wf.GetWalBytes(ctx, pw, newWal); err != nil {
						// TODO handle
						//log.Fatal(err)
					}
				}()

				// Build new wals
				newWfWal, err := r.buildFromFile(pr)
				if err != nil {
					// Ignore incompatible files
					continue
				}

				if len(newWfWal) > 0 {
					wg.Add(1)
					go func(n string, wal []EventLog) {
						defer wg.Done()
						replayChan <- namedWal{
							name: n,
							wal:  wal,
						}
					}(newWal, newWfWal)
				}
			}
		}
	}
	wg.Wait()

	return nil
}

func (r *DBListRepo) gather(ctx context.Context) error {
	//log.Print("Gathering...")
	// Create a new list so we don't have to keep the lock on the mutex for too long
	r.syncWalFileMut.RLock()
	r.allWalFileMut.RLock()
	ownedWalFiles := []WalFile{}
	nonOwnedWalFiles := []WalFile{}
	for _, wf := range r.syncWalFiles {
		ownedWalFiles = append(ownedWalFiles, wf)
	}
	for k, wf := range r.allWalFiles {
		if _, exists := r.syncWalFiles[k]; !exists {
			nonOwnedWalFiles = append(nonOwnedWalFiles, wf)
		}
	}
	r.syncWalFileMut.RUnlock()
	r.allWalFileMut.RUnlock()

	// DANGER: running a pull here can cause map contention (concurrent iteration and write) in the
	// crdt caches, namely `positionEventSet` in `generateEvents`
	//if err := r.pull(ctx, ownedWalFiles, replayChan); err != nil {
	//return err
	//}

	fullWal := r.crdt.generateEvents()

	fullByteWal, err := BuildByteWal(fullWal)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, wf := range ownedWalFiles {
		wg.Add(1)
		go func(wf WalFile) {
			defer wg.Done()

			name := fmt.Sprintf("%v%v", r.uuid, generateUUID())
			err := r.push(ctx, wf, []EventLog{}, fullByteWal, name)
			if err != nil {
				return
			}

			// TODO dedup from `pull` call above
			// retrieve list of files to delete
			filesToDelete := []string{}
			allFiles, err := wf.GetMatchingWals(ctx, path.Join(wf.GetRoot(), "wal_*.db"))
			if err != nil {
				return
			}

			for _, f := range allFiles {
				if f != name && r.isWalChecksumProcessed(f) {
					filesToDelete = append(filesToDelete, f)
				}
			}

			// Schedule a delete on the files
			if len(filesToDelete) > 0 {
				wf.RemoveWals(ctx, filesToDelete)
			}
		}(wf)
	}
	wg.Wait()

	for _, wf := range nonOwnedWalFiles {
		wg.Add(1)
		go func(wf WalFile) {
			defer wg.Done()

			var byteWal *bytes.Buffer

			// Push to ALL walFiles
			// we don't set a common name, as filtering could generate different wals to each walfile
			if err := r.push(ctx, wf, fullWal, byteWal, ""); err != nil {
				return
			}
		}(wf)
	}
	wg.Wait()

	return nil
}

func BuildByteWal(el []EventLog) (*bytes.Buffer, error) {
	var outputBuf bytes.Buffer

	// Write the schema ID
	if err := binary.Write(&outputBuf, binary.LittleEndian, LatestWalSchemaID); err != nil {
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
		// TODO 2022-10-07: I've temporarily disabled per line collab until I can figure how to cleanly share all required PositionEvents as well as UpdateEvents
		//if e.emailHasAccess(walFileOwnerEmail) {
		//filteredWal = append(filteredWal, e)
		//}
	}
	return filteredWal
}

func (r *DBListRepo) push(ctx context.Context, wf WalFile, el []EventLog, byteWal *bytes.Buffer, name string) error {
	if byteWal == nil {
		// Apply any filtering based on Push match configuration
		el = r.getMatchedWal(el, wf)

		// Return for empty wals
		if len(el) == 0 {
			return nil
		}

		var err error
		if byteWal, err = BuildByteWal(el); err != nil {
			return err
		}

	}

	if name == "" {
		name = fmt.Sprintf("%v%v", r.uuid, generateUUID())
	}

	// Add it straight to the cache to avoid processing it in the future
	// This needs to be done PRIOR to flushing to avoid race conditions
	// (as pull is done in a separate thread of control, and therefore we might try
	// and pull our own pushed wal)
	// There is a chance that Flush would fail, but given the names are randomly generated, the impact of caching
	// the broken name is small.
	r.setProcessedWalChecksum(name)
	if err := wf.Flush(ctx, byteWal, name); err != nil {
		return err
	}

	return nil
}

func (r *DBListRepo) flushPartialWals(ctx context.Context, wal []EventLog, waitForCompletion bool) {
	if len(wal) > 0 {
		fullByteWal, err := BuildByteWal(wal)
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
				// we don't set a common name, as filtering could generate different wals to each walfile
				r.push(ctx, wf, wal, byteWal, "")
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

func (r *DBListRepo) startSync(ctx context.Context, replayChan chan namedWal, inputEvtsChan chan interface{}) error {
	// syncTriggerChan is buffered, as the producer is called in the same thread as the consumer (to ensure a minimum of the
	// specified wait duration)
	syncTriggerChan := make(chan struct{}, 1)

	gatherTriggerTimer := time.NewTimer(gatherInterval)
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
		r.pushTriggerTimer.Reset(pushInterval)
		gatherTriggerTimer.Reset(gatherInterval)
	}

	// Run an initial load from the local walfile
	if err := r.pull(ctx, []WalFile{r.LocalWalFile}, replayChan); err != nil {
		return err
	}

	// Main sync event loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
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
					if err = r.pull(ctx, syncWalFiles, replayChan); err != nil {
						log.Fatal(err)
					}
					r.hasSyncedRemotes = true
				case <-gatherTriggerTimer.C:
					if err = r.gather(ctx); err != nil {
						log.Fatal(err)
					}
				}
				time.Sleep(pullInterval)
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
						scheduleSync() // still trigger pull cycle for local only sync
						return         // authFailureError signifies incorrect login details, disable web and run local only mode
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
					wsFlushTicker := time.NewTicker(websocketConsumeInterval)
					go func() {
						for {
							select {
							case wsEv := <-wsConsChan:
								wsConsAgg = append(wsConsAgg, wsEv...)
							case <-wsFlushTicker.C:
								if len(wsConsAgg) > 0 {
									replayChan <- namedWal{
										name: "",
										wal:  wsConsAgg,
									}
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
					wsPubAgg = append(wsPubAgg, e)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	wsPublishTicker := time.NewTicker(websocketPublishInterval)
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
								b, _ := BuildByteWal(matchedEventLog)
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
				flushAgg = append(flushAgg, wsPubAgg...)
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
	id := generateUUID()
	prevKey := ""
	var lamportTimestamp int64
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		key := strconv.Itoa(int(id)) + ":" + strconv.Itoa(int(lamportTimestamp))
		e := EventLog{
			UUID:             id,
			EventType:        UpdateEvent,
			ListItemKey:      key,
			Line:             line,
			LamportTimestamp: lamportTimestamp,
		}
		el = append(el, e)
		lamportTimestamp++

		e = EventLog{
			UUID:              id,
			EventType:         PositionEvent,
			ListItemKey:       key,
			TargetListItemKey: prevKey,
			LamportTimestamp:  lamportTimestamp,
		}
		el = append(el, e)
		lamportTimestamp++

		if isHidden {
			e = EventLog{
				UUID:        id,
				EventType:   HideEvent,
				ListItemKey: key,
			}
			el = append(el, e)
			lamportTimestamp++
		}

		prevKey = key
	}

	b, _ := BuildByteWal(el)
	wf.Flush(ctx, b, fmt.Sprintf("%d", id))

	return nil
}

func (r *DBListRepo) TestPullLocal(c chan namedWal) error {
	//c := make(chan []EventLog)
	if err := r.pull(context.Background(), []WalFile{r.LocalWalFile}, c); err != nil {
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
