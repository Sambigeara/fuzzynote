package service

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"
)

type (
	uuid         uint32
	fileSchemaID uint16
)

const (
	walFilePattern    = "wal_%v.db"
	viewFilePattern   = "view_%v"
	exportFilePattern = "export_%v.txt"

	crdtPositionTreeDegree = 6
)

type bits uint32

const (
	hidden bits = 1 << iota
)

func set(b, flag bits) bits    { return b | flag }
func clear(b, flag bits) bits  { return b &^ flag }
func toggle(b, flag bits) bits { return b ^ flag }
func has(b, flag bits) bool    { return b&flag != 0 }

type Client interface {
	HandleEvent(interface{}) error
	AwaitEvent() interface{}
}

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	eventLogger    *DbEventLogger
	matchListItems map[string]*ListItem

	//currentLamportTimestamp int64
	vectorClock   map[uuid]int64
	listItemCache map[string]*ListItem
	//processedEventLogCache             map[string]struct{}
	//listItemProcessedEventLogTypeCache        map[EventType]map[string]EventLog
	addEventSet, deleteEventSet, positionEventSet map[string]EventLog

	crdtPositionTree    *btree.BTree
	crdtTargetItemCache map[string]*ListItem
	crdt                *crdtTree

	// Wal stuff
	uuid              uuid
	eventsChan        chan EventLog
	web               *Web
	latestWalSchemaID uint16

	remoteCursorMoveChan chan cursorMoveEvent
	localCursorMoveChan  chan cursorMoveEvent
	collabPositions      map[string]cursorMoveEvent
	collabMapLock        *sync.Mutex
	previousListItemKey  string

	email string
	//cfgFriendRegex            *regexp.Regexp
	friends                       map[string]map[string]int64
	friendsUpdateLock             *sync.RWMutex
	friendsOrdered                []string            // operating sort of like a queue, with earliest friends at the head
	activeFriends, pendingFriends map[string]struct{} // returned from the cloud
	activeFriendsMapLock          *sync.RWMutex
	friendsMostRecentChangeDT     int64
	friendsLastPushDT             int64

	// TODO better naming convention
	LocalWalFile   LocalWalFile
	webWalFiles    map[string]WalFile
	allWalFiles    map[string]WalFile
	syncWalFiles   map[string]WalFile
	webWalFileMut  *sync.RWMutex
	allWalFileMut  *sync.RWMutex
	syncWalFileMut *sync.RWMutex

	processedWalChecksums    map[string]struct{}
	processedWalChecksumLock *sync.Mutex

	pushTriggerTimer   *time.Timer
	hasUnflushedEvents bool
	finalFlushChan     chan struct{}

	hasSyncedRemotes bool
}

// NewDBListRepo returns a pointer to a new instance of DBListRepo
func NewDBListRepo(localWalFile LocalWalFile, webTokenStore WebTokenStore) *DBListRepo {
	listRepo := &DBListRepo{
		// TODO rename this cos it's solely for UNDO/REDO
		eventLogger: NewDbEventLogger(),

		// Wal stuff
		uuid:              generateUUID(),
		latestWalSchemaID: latestWalSchemaID,
		vectorClock:       make(map[uuid]int64),
		listItemCache:     make(map[string]*ListItem),
		//processedEventLogCache:             make(map[string]struct{}),
		//listItemProcessedEventLogTypeCache: make(map[EventType]map[string]EventLog),
		addEventSet:      make(map[string]EventLog),
		deleteEventSet:   make(map[string]EventLog),
		positionEventSet: make(map[string]EventLog),

		crdtPositionTree:    btree.New(crdtPositionTreeDegree),
		crdtTargetItemCache: make(map[string]*ListItem),
		crdt:                newTree(),

		LocalWalFile: localWalFile,
		eventsChan:   make(chan EventLog),

		collabMapLock: &sync.Mutex{},

		webWalFiles:    make(map[string]WalFile),
		allWalFiles:    make(map[string]WalFile),
		syncWalFiles:   make(map[string]WalFile),
		webWalFileMut:  &sync.RWMutex{},
		allWalFileMut:  &sync.RWMutex{},
		syncWalFileMut: &sync.RWMutex{},

		processedWalChecksums:    make(map[string]struct{}),
		processedWalChecksumLock: &sync.Mutex{},

		friends:              make(map[string]map[string]int64),
		friendsUpdateLock:    &sync.RWMutex{},
		activeFriendsMapLock: &sync.RWMutex{},

		pushTriggerTimer: time.NewTimer(time.Second * 0),
		finalFlushChan:   make(chan struct{}),
	}

	// The localWalFile gets attached to the Wal independently (there are certain operations
	// that require us to only target the local walfile rather than all). We still need to register
	// it as we call all walfiles in the next line.
	listRepo.AddWalFile(localWalFile, true)

	if webTokenStore.Email() != "" {
		listRepo.setEmail(webTokenStore.Email())
		//listRepo.cfgFriendRegex = regexp.MustCompile(fmt.Sprintf("^fzn_cfg:friend +(%s) +@%s$", EmailRegex, regexp.QuoteMeta(listRepo.email)))
		//listRepo.cfgFriendRegex = regexp.MustCompile(fmt.Sprintf("^fzn_cfg:friend (%s) @%s$", EmailRegex, regexp.QuoteMeta(listRepo.email)))
	}

	// Tokens are generated on `login`
	// Keeping the web assignment outside of registerWeb, as we use registerWeb to reinstantiate
	// the web walfiles and connections periodically during runtime, and this makes it easier... (for now)
	listRepo.web = NewWeb(webTokenStore)

	// Establish the chan used to track and display collaborator cursor positions
	listRepo.remoteCursorMoveChan = make(chan cursorMoveEvent)  // incoming events
	listRepo.localCursorMoveChan = make(chan cursorMoveEvent)   // outgoing events
	listRepo.collabPositions = make(map[string]cursorMoveEvent) // map[collaboratorEmail]currentKey

	return listRepo
}

func (r *DBListRepo) setEmail(email string) {
	r.email = strings.ToLower(email)
	r.friends[email] = make(map[string]int64)
}

func (r *DBListRepo) getVectorDT(id uuid) (int64, bool) {
	dt, exists := r.vectorClock[id]
	return dt, exists
}

func (r *DBListRepo) setVectorDT(id uuid, dt int64) {
	r.vectorClock[id] = dt
}

func (r *DBListRepo) getLocalVectorDT() int64 {
	dt, _ := r.getVectorDT(r.uuid)
	return dt
}

func (r *DBListRepo) setLocalVectorDT(dt int64) {
	r.setVectorDT(r.uuid, dt)
}

func (r *DBListRepo) incLocalVectorDT() {
	r.vectorClock[r.uuid]++
}

// ForceTriggerFlush will zero the flush timer, and block til completion. E.g. this is a synchronous flush
func (r *DBListRepo) ForceTriggerFlush() {
	go func() {
		r.pushTriggerTimer.Reset(time.Millisecond * 1)
	}()
}

// IsSynced is a boolean value defining whether or now there are currently events held in memory that are yet to be
// flushed to local storage
func (r *DBListRepo) IsSynced() bool {
	return !r.hasUnflushedEvents
}

// ListItem is a mergeable data structure which represents a single item in the main list. It maintains record of the
// last update lamport times
type ListItem struct {
	rawLine  string
	Note     []byte // TODO make private
	IsHidden bool

	child       *ListItem
	parent      *ListItem
	matchChild  *ListItem
	matchParent *ListItem

	friends LineFriends

	localEmail string // set at creation time and used to exclude from Friends() method
	key        string
}

// Line returns a post-processed rawLine, with any matched collaborators omitted
func (i *ListItem) Line() string {
	if i.friends.IsProcessed {
		return i.rawLine[:i.friends.Offset]
	}
	return i.rawLine
}

func (i *ListItem) Friends() []string {
	// The emails are stored as a map of strings. We need to generate a sorted slice to return
	// to the client
	// TODO cache for optimisation?? need to cover updates
	sortedEmails := []string{}
	for _, e := range i.friends.Emails {
		if e != i.localEmail {
			sortedEmails = append(sortedEmails, e)
		}
	}
	sort.Strings(sortedEmails)
	return sortedEmails
}

// TODO make attribute public directly??
func (i *ListItem) Key() string {
	return i.key
}

func (r *DBListRepo) addEventLog(el EventLog) (*ListItem, error) {
	var err error
	var item *ListItem

	// At this point we inspect the Line in the event log for `@friends`, and if they're present and currently
	// enabled (e.g. in the friends cache), cut them from any central location in the line and append to the end.
	// This is required for later public client APIs (e.g. we only return a slice of the rawLine to clients via
	// the Line() API).
	el = r.repositionActiveFriends(el)

	item, err = r.processEventLog(el)
	r.eventsChan <- el
	return item, err
}

// Add adds a new LineItem with string, note and a position to insert the item into the matched list
// It returns a string representing the unique key of the newly created item
func (r *DBListRepo) Add(line string, note []byte, childItem *ListItem) (string, error) {
	e := r.newEventLog(UpdateEvent, true)
	listItemKey := strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.getLocalVectorDT()))
	e.ListItemKey = listItemKey
	if childItem != nil {
		e.TargetListItemKey = childItem.key
	}
	e.Line = line
	e.Note = note

	newItem, _ := r.addEventLog(e)

	ue := r.newEventLog(DeleteEvent, false)
	ue.ListItemKey = listItemKey
	ue.Line = line // needed for Friends generation in repositionActiveFriends

	r.addUndoLog(ue, e)

	return newItem.key, nil
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) Update(line string, item *ListItem) error {
	if item == nil {
		return nil
	}

	e := r.newEventLogFromListItem(UpdateEvent, item, true)
	e.Line = line

	// Undo event created from pre event processing state
	ue := r.newEventLogFromListItem(UpdateEvent, item, false)

	if c := item.matchChild; c != nil {
		e.TargetListItemKey = c.key
		ue.TargetListItemKey = c.key
	}

	r.addEventLog(e)
	r.addUndoLog(ue, e)
	return nil
}

// TODO rethink this interface
func (r *DBListRepo) UpdateNote(note []byte, item *ListItem) error {
	if item == nil {
		return nil
	}

	e := r.newEventLogFromListItem(UpdateEvent, item, true)
	e.Note = note

	// Undo event created from pre event processing state
	ue := r.newEventLogFromListItem(UpdateEvent, item, false)

	r.addEventLog(e)
	r.addUndoLog(ue, e)
	return nil
}

// Delete will remove an existing ListItem
func (r *DBListRepo) Delete(item *ListItem) (string, error) {

	e := r.newEventLogFromListItem(DeleteEvent, item, true)
	ue := r.newEventLogFromListItem(UpdateEvent, item, false)

	r.addEventLog(e)
	r.addUndoLog(ue, e)

	// the client is responsible for repositioning the old parent, if present
	if p := item.matchParent; p != nil {
		posEvent := r.newEventLogFromListItem(PositionEvent, p, true)
		if item.matchChild != nil {
			posEvent.TargetListItemKey = item.matchChild.key
		}
		r.addEventLog(posEvent)
	}

	// We use matchChild to set the next "current key", otherwise, if we delete the final matched item, which happens
	// to have a child in the full (un-matched) set, it will default to that on the return (confusing because it will
	// not match the current specified search groups)
	if item.matchChild != nil {
		return item.matchChild.key, nil
	}
	return "", nil
}

// MoveUp will swop a ListItem with the ListItem directly above it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveUp(item *ListItem) error {
	// We need to target the child of the child (as when we apply move events, we specify the target that we want to be
	// the new child. Only relevant for non-startup case
	if item.matchChild != nil {
		e := r.newEventLogFromListItem(PositionEvent, item, true)
		e.TargetListItemKey = ""
		if item.matchChild.matchChild != nil {
			e.TargetListItemKey = item.matchChild.matchChild.key
		}
		r.addEventLog(e)

		// client is responsible for repointing the matchChild and matchParent
		childEvent := r.newEventLogFromListItem(PositionEvent, item.matchChild, true)
		childEvent.TargetListItemKey = item.key
		r.addEventLog(childEvent)
		if item.matchParent != nil {
			parentEvent := r.newEventLogFromListItem(PositionEvent, item.matchParent, true)
			parentEvent.TargetListItemKey = item.matchChild.key
			r.addEventLog(parentEvent)
		}

		ue := r.newEventLogFromListItem(PositionEvent, item, false)
		ue.TargetListItemKey = item.matchChild.key
		r.addUndoLog(ue, e)
		// TODO extra undo events too (due to repointing)
	}
	return nil
}

// MoveDown will swop a ListItem with the ListItem directly below it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveDown(item *ListItem) error {
	if item.matchParent != nil {
		e := r.newEventLogFromListItem(PositionEvent, item, true)
		e.TargetListItemKey = item.matchParent.key
		r.addEventLog(e)

		// client is responsible for repointing the matchParent and matchParent.matchParent
		upEvent := r.newEventLogFromListItem(PositionEvent, item.matchParent, true)
		upEvent.TargetListItemKey = ""
		if item.matchChild != nil {
			upEvent.TargetListItemKey = item.matchChild.key
		}
		r.addEventLog(upEvent)
		if item.matchParent.matchParent != nil {
			upEvent := r.newEventLogFromListItem(PositionEvent, item.matchParent.matchParent, true)
			upEvent.TargetListItemKey = item.key
			r.addEventLog(upEvent)
		}

		ue := r.newEventLogFromListItem(PositionEvent, item, false)
		ue.TargetListItemKey = ""
		if item.matchChild != nil {
			ue.TargetListItemKey = item.matchChild.key
		}
		r.addUndoLog(ue, e)
		// TODO extra undo events too (due to repointing)
	}
	return nil
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(item *ListItem) (string, error) {
	//var evType, oppEvType EventType
	var newIsHidden bool
	var focusedItemKey string
	if item.IsHidden {
		newIsHidden = false
		//evType = ShowEvent
		//oppEvType = HideEvent
		// Cursor should remain on newly visible key
		focusedItemKey = item.key
	} else {
		//evType = HideEvent
		//oppEvType = ShowEvent
		newIsHidden = true
		// Set focusedItemKey to parent if available, else child (e.g. bottom of list)
		if item.matchParent != nil {
			focusedItemKey = item.matchParent.key
		} else if item.matchChild != nil {
			focusedItemKey = item.matchChild.key
		}
	}
	e := r.newEventLogFromListItem(UpdateEvent, item, true)
	e.IsHidden = newIsHidden
	r.addEventLog(e)

	ue := r.newEventLogFromListItem(UpdateEvent, item, false)
	ue.IsHidden = !newIsHidden
	r.addUndoLog(ue, e)

	return focusedItemKey, nil
}

func (r *DBListRepo) Undo() (string, error) {
	if r.eventLogger.curIdx > 0 {
		ue := r.eventLogger.log[r.eventLogger.curIdx]
		e := ue.oppEvent

		// TODO centralise
		// If the oppEvent event type == AddEvent, the event ListItemKey will become inconsistent with the
		// UUID and LamportTimestamp of the new event. However, we need to maintain the old key in order to map
		// to the old ListItem in the caches
		r.incLocalVectorDT()
		e.VectorClock = r.getLocalVectorClockCopy()

		item, err := r.addEventLog(e)
		r.eventLogger.curIdx--
		return item.key, err
	}
	return "", nil
}

func (r *DBListRepo) Redo() (string, error) {
	// Redo needs to look forward +1 index when actioning events
	if r.eventLogger.curIdx < len(r.eventLogger.log)-1 {
		ue := r.eventLogger.log[r.eventLogger.curIdx+1]
		e := ue.event

		// TODO centralise
		// If the oppEvent event type == AddEvent, the event ListItemKey will become inconsistent with the
		// UUID and LamportTimestamp of the new event. However, we need to maintain the old key in order to map
		// to the old ListItem in the caches
		r.incLocalVectorDT()
		e.VectorClock = r.getLocalVectorClockCopy()

		item, err := r.addEventLog(e)
		r.eventLogger.curIdx++
		var key string
		if item != nil {
			if c := item.matchChild; e.EventType == DeleteEvent && c != nil {
				key = c.key
			} else {
				key = item.key
			}
		}
		return key, err
	}
	return "", nil
}

// Match takes a set of search groups and applies each to all ListItems, returning those that
// fulfil all rules. `showHidden` dictates whether or not hidden items are returned. `curKey` is used to identify
// the currently selected item. `offset` and `limit` can be passed to paginate over the match-set, if `limit==0`, all matches
// from `offset` will be returned (e.g. no limit will be applied).
func (r *DBListRepo) Match(keys [][]rune, showHidden bool, curKey string, offset int, limit int) ([]ListItem, int, error) {
	res := []ListItem{}
	if offset < 0 {
		return res, 0, errors.New("offset must be >= 0")
	} else if limit < 0 {
		return res, 0, errors.New("limit must be >= 0")
	}

	var lastCur *ListItem

	r.matchListItems = make(map[string]*ListItem)

	idx := 0
	listItemMatchIdx := make(map[string]int)
	//for cur := range r.getListItems() {
	for curKey := range r.crdt.traverse() {
		cur := r.listItemCache[curKey]
		//for {
		// Nullify match pointers
		// TODO centralise this logic, it's too closely coupled with the moveItem logic (if match pointers
		// aren't cleaned up between ANY ops, it can lead to weird behaviour as things operate based on
		// the existence and setting of them)
		cur.matchChild, cur.matchParent = nil, nil

		if showHidden || !cur.IsHidden {
			matched := true
			for _, group := range keys {
				// Match the currently selected item.
				// Also, match any items with empty Lines (this accounts for lines added when search is active)
				if cur.key == curKey || len(cur.rawLine) == 0 {
					break
				}
				// TODO unfortunate reuse of vars - refactor to tidy
				pattern, nChars := GetMatchPattern(group)

				// Rather than use rawLine (which houses the client local email too, which we _dont_ want to
				// match on), we generate a new line for the match func
				var sb strings.Builder
				sb.WriteString(cur.Line())
				for _, f := range cur.Friends() {
					sb.WriteString(" @")
					sb.WriteString(f)
				}
				if !isMatch(group[nChars:], sb.String(), pattern) {
					matched = false
					break
				}
			}
			if matched {
				// Pagination: only add to results set if we've surpassed the min boundary of the page,
				// otherwise only increment `idx`.
				if idx >= offset {
					r.matchListItems[cur.key] = cur

					// ListItems stored in the `res` slice are copies, and therefore will not reflect the
					// matchChild/matchParent setting below. This doesn't reflect normal function as we only
					// return `res` to the client for displaying lines (any mutations to backend state are done
					// via index and act on the matchListItems slice which stores the original items by ptr)
					// TODO centralise this
					res = append(res, *cur)

					if lastCur != nil {
						lastCur.matchParent = cur
					}
					cur.matchChild = lastCur
					lastCur = cur

					// Set the new idx for the next iteration
					listItemMatchIdx[cur.key] = idx
				}
				idx++
			}
		}
		// Terminate if we reach the root, or for when pagination is active and we reach the max boundary
		if limit > 0 && idx == offset+limit {
			break
		}
	}
	newPos := -1
	if p, exists := listItemMatchIdx[curKey]; exists {
		newPos = p
	}
	return res, newPos, nil
}

func (r *DBListRepo) GetFriendFromConfig(item ListItem) (string, bool) {
	if fields, isConfig := r.checkIfConfigLine(item.rawLine); isConfig {
		return string(fields[1]), true
	}
	return "", false
}

type FriendState int

const (
	FriendNull FriendState = iota
	FriendPending
	FriendActive
)

func (r *DBListRepo) GetFriendState(f string) FriendState {
	r.activeFriendsMapLock.RLock()
	defer r.activeFriendsMapLock.RUnlock()
	f = strings.ToLower(f)
	if _, exists := r.activeFriends[f]; exists {
		return FriendActive
	} else if _, exists := r.pendingFriends[f]; exists {
		return FriendPending
	}
	return FriendNull
}

type SyncState int

const (
	SyncOffline SyncState = iota
	SyncSyncing
	SyncSynced
)

type SyncEvent struct{}

func (r *DBListRepo) GetSyncState() SyncState {
	if !r.web.isActive {
		return SyncOffline
	}

	if !r.hasSyncedRemotes || r.hasUnflushedEvents {
		return SyncSyncing
	}

	return SyncSynced
}

func (r *DBListRepo) GetListItem(key string) (ListItem, bool) {
	itemPtr, exists := r.listItemCache[key]
	if exists {
		return *itemPtr, true
	}
	return ListItem{}, false
}

func (r *DBListRepo) EmitCursorMoveEvent(key string) {
	// We need to _only_ emit an event if the curKey has changed since the previous Match call.
	// This prevents an endless loop that arises when more than one client is active and communicating on the same wal.
	// If we emitted every time, the following would happen:
	// 1. receive cursor move websocket event
	// 2. process it, trigger a client refresh
	// 3. which calls this function, which then emits an event
	// 4. trigger stage 1 on remote...
	if r.web.isActive && r.web.wsConn != nil {
		if key != r.previousListItemKey {
			//log.Println("cursor move: ", key)
			go func() {
				r.localCursorMoveChan <- cursorMoveEvent{
					listItemKey:  key,
					unixNanoTime: time.Now().UnixNano(),
				}
			}()
			r.previousListItemKey = key
		}
	}
}

func getChangedListItemKeysFromWal(wal []EventLog) (map[string]struct{}, bool) {
	var allowOverride bool
	allowOverride = true
	keys := make(map[string]struct{})
	for _, el := range wal {
		keys[el.ListItemKey] = struct{}{}
		switch el.EventType {
		// TODO event updates
		case MoveUpEvent, MoveDownEvent, AddEvent, DeleteEvent:
			allowOverride = false
		}
	}
	return keys, allowOverride
}

func (r *DBListRepo) GetListItemNote(key string) []byte {
	var note []byte
	if item, exists := r.matchListItems[key]; exists {
		note = item.Note
	}
	return note
}

func (r *DBListRepo) SaveListItemNote(key string, note []byte) {
	if item, exists := r.matchListItems[key]; exists {
		r.UpdateNote(note, item)
	}
}

// GetCollabPositions returns a map of listItemKeys against all collaborators currently on that listItem
func (r *DBListRepo) GetCollabPositions() map[string][]string {
	r.collabMapLock.Lock()
	defer r.collabMapLock.Unlock()

	pos := make(map[string][]string)
	for email, ev := range r.collabPositions {
		key := ev.listItemKey
		_, exists := pos[key]
		if !exists {
			pos[key] = []string{}
		}
		pos[key] = append(pos[key], email)
	}
	return pos
}

func (r *DBListRepo) SetCollabPosition(ev cursorMoveEvent) bool {
	r.collabMapLock.Lock()
	defer r.collabMapLock.Unlock()

	// Only update if the event occurred more recently
	old, exists := r.collabPositions[ev.email]
	if !exists || old.unixNanoTime < ev.unixNanoTime {
		r.collabPositions[ev.email] = ev
		return true
	}
	return false
}

func (r *DBListRepo) ExportToPlainText(matchKeys [][]rune, showHidden bool) error {
	matchedItems, _, _ := r.Match(matchKeys, showHidden, "", 0, 0)
	return generatePlainTextFile(matchedItems)
}
