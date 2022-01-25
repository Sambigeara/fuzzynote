package service

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	uuid         uint32
	fileSchemaID uint16
)

const (
	walFilePattern    = "wal_%v.db"
	viewFilePattern   = "view_%v"
	exportFilePattern = "export_%v.txt"
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
	HandleEvent(interface{}) (bool, bool, error)
	AwaitEvent() interface{}
}

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	Root           *ListItem
	eventLogger    *DbEventLogger
	matchListItems map[string]*ListItem

	currentLamportTimestamp            int64
	listItemTracker                    map[string]*ListItem
	processedEventLogCache             map[string]struct{}
	listItemProcessedEventLogTypeCache map[EventType]map[string]EventLog

	// Wal stuff
	uuid              uuid
	log               []EventLog
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
	friends                   map[string]map[string]int64
	friendsUpdateLock         *sync.RWMutex
	friendsMostRecentChangeDT int64
	friendsLastPushDT         int64

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

	stopChan chan struct{}
}

// NewDBListRepo returns a pointer to a new instance of DBListRepo
func NewDBListRepo(localWalFile LocalWalFile, webTokenStore WebTokenStore, syncFrequency uint32, gatherFrequency uint32) *DBListRepo {
	listRepo := &DBListRepo{
		// TODO rename this cos it's solely for UNDO/REDO
		eventLogger: NewDbEventLogger(),

		stopChan: make(chan struct{}),

		// Wal stuff
		uuid:                               generateUUID(),
		log:                                []EventLog{},
		latestWalSchemaID:                  latestWalSchemaID,
		listItemTracker:                    make(map[string]*ListItem),
		processedEventLogCache:             make(map[string]struct{}),
		listItemProcessedEventLogTypeCache: make(map[EventType]map[string]EventLog),

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

		friends:           make(map[string]map[string]int64),
		friendsUpdateLock: &sync.RWMutex{},
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

// ListItem represents a single item in the returned list, based on the Match() input
type ListItem struct {
	rawLine string
	Note    []byte // TODO make private

	IsHidden bool

	child       *ListItem
	parent      *ListItem
	matchChild  *ListItem
	matchParent *ListItem

	friends LineFriends

	localEmail string // set at creation time and used to exclude from Friends() method
	key        string

	isDeleted bool
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

func (r *DBListRepo) newEventLog(t EventType) EventLog {
	r.currentLamportTimestamp++
	return EventLog{
		UUID:             r.uuid,
		LamportTimestamp: r.currentLamportTimestamp,
		EventType:        t,
	}
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
	r.log = append(r.log, el)
	return item, err
}

// Add adds a new LineItem with string, note and a position to insert the item into the matched list
// It returns a string representing the unique key of the newly created item
func (r *DBListRepo) Add(line string, note []byte, childItem *ListItem) (string, error) {
	e := r.newEventLog(AddEvent)
	listItemKey := strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.LamportTimestamp))
	e.ListItemKey = listItemKey
	if childItem != nil {
		e.TargetListItemKey = childItem.key
	}
	e.Line = line
	e.Note = note

	newItem, _ := r.addEventLog(e)

	ue := r.newEventLog(DeleteEvent)
	ue.ListItemKey = listItemKey
	ue.Line = line // needed for Friends generation in repositionActiveFriends

	r.addUndoLog(ue, e)

	return newItem.key, nil
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) Update(line string, note []byte, item *ListItem) error {
	if item == nil {
		return nil
	}

	var childKey string
	if c := item.child; c != nil {
		childKey = c.key
	}

	e := r.newEventLog(UpdateEvent)
	e.ListItemKey = item.key
	e.TargetListItemKey = childKey
	e.Line = line
	e.Note = note

	// Undo event created from pre event processing state
	ue := r.newEventLog(UpdateEvent)
	ue.ListItemKey = item.key
	ue.TargetListItemKey = childKey
	ue.Line = item.rawLine
	ue.Note = item.Note

	r.addEventLog(e)
	r.addUndoLog(ue, e)
	return nil
}

// Delete will remove an existing ListItem
func (r *DBListRepo) Delete(item *ListItem) (string, error) {
	var childKey, matchChildKey string
	if c := item.child; c != nil {
		childKey = c.key
	}

	e := r.newEventLog(DeleteEvent)
	e.ListItemKey = item.key
	e.Line = item.rawLine // needed for Friends generation in repositionActiveFriends

	r.addEventLog(e)

	ue := r.newEventLog(AddEvent)
	ue.ListItemKey = item.key
	ue.TargetListItemKey = childKey
	ue.Line = item.rawLine
	ue.Note = item.Note

	r.addUndoLog(ue, e)

	// We use matchChild to set the next "current key", otherwise, if we delete the final matched item, which happens
	// to have a child in the full (un-matched) set, it will default to that on the return (confusing because it will
	// not match the current specified search groups)
	if item.matchChild != nil {
		matchChildKey = item.matchChild.key
	}
	return matchChildKey, nil
}

// MoveUp will swop a ListItem with the ListItem directly above it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveUp(item *ListItem) error {
	// We need to target the child of the child (as when we apply move events, we specify the target that we want to be
	// the new child. Only relevant for non-startup case
	if item.matchChild != nil {
		e := r.newEventLog(MoveUpEvent)
		e.ListItemKey = item.key
		if item.matchChild.child != nil {
			e.TargetListItemKey = item.matchChild.child.key
		}
		e.Line = item.rawLine // needed for Friends generation in repositionActiveFriends

		r.addEventLog(e)

		ue := r.newEventLog(MoveDownEvent)
		ue.ListItemKey = item.key
		if item.matchParent != nil {
			ue.TargetListItemKey = item.matchChild.key
		}
		ue.Line = item.rawLine // needed for Friends generation in repositionActiveFriends

		r.addUndoLog(ue, e)
	}
	return nil
}

// MoveDown will swop a ListItem with the ListItem directly below it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveDown(item *ListItem) error {
	//if item.matchParent != nil && item.matchParent.lamportTimestamp != 0 {
	if item.matchParent != nil {
		e := r.newEventLog(MoveDownEvent)
		e.ListItemKey = item.key
		e.TargetListItemKey = item.matchParent.key
		e.Line = item.rawLine // needed for Friends generation in repositionActiveFriends

		r.addEventLog(e)

		ue := r.newEventLog(MoveUpEvent)
		ue.ListItemKey = item.key
		if item.matchChild != nil {
			ue.TargetListItemKey = item.matchChild.key
		}
		ue.Line = item.rawLine // needed for Friends generation in repositionActiveFriends

		r.addUndoLog(ue, e)
	}
	return nil
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(item *ListItem) (string, error) {
	var evType, oppEvType EventType
	var focusedItemKey string
	if item.IsHidden {
		evType = ShowEvent
		oppEvType = HideEvent
		// Cursor should remain on newly visible key
		focusedItemKey = item.key
	} else {
		evType = HideEvent
		oppEvType = ShowEvent
		// Set focusedItemKey to parent if available, else child (e.g. bottom of list)
		if item.matchParent != nil {
			focusedItemKey = item.matchParent.key
		} else if item.matchChild != nil {
			focusedItemKey = item.matchChild.key
		}
	}
	e := r.newEventLog(evType)
	e.ListItemKey = item.key
	e.Line = item.rawLine // needed for Friends generation in repositionActiveFriends

	r.addEventLog(e)

	ue := r.newEventLog(oppEvType)
	ue.ListItemKey = item.key
	ue.Line = item.rawLine // needed for Friends generation in repositionActiveFriends

	r.addUndoLog(ue, e)

	return focusedItemKey, nil
}

func (r *DBListRepo) Undo() (string, error) {
	if r.eventLogger.curIdx > 0 {
		ue := r.eventLogger.log[r.eventLogger.curIdx]
		e := ue.oppEvent

		// TODO centralise
		r.currentLamportTimestamp++
		e.LamportTimestamp = r.currentLamportTimestamp

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
		r.currentLamportTimestamp++
		e.LamportTimestamp = r.currentLamportTimestamp

		item, err := r.addEventLog(e)
		r.eventLogger.curIdx++
		var key string
		if c := item.matchChild; e.EventType == DeleteEvent && c != nil {
			key = c.key
		} else {
			key = item.key
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

	cur := r.Root
	var lastCur *ListItem

	r.matchListItems = make(map[string]*ListItem)

	newPos := -1
	if cur == nil {
		return res, newPos, nil
	}

	// If web connection is enabled, broadcast a cursor move event
	// We need to _only_ emit an event if the curKey has changed since the previous Match call.
	// This prevents an endless loop that arises when more than one client is active and communicating on the same wal.
	// If we emitted every time, the following would happen:
	// 1. receive cursor move websocket event
	// 2. process it, trigger a client refresh
	// 3. which calls this function, which then emits an event
	// 4. trigger stage 1 on remote...
	if curKey != r.previousListItemKey && r.web.isActive && r.web.wsConn != nil {
		r.EmitCursorMoveEvent(curKey)
	}

	r.previousListItemKey = curKey

	idx := 0
	listItemMatchIdx := make(map[string]int)
	for {
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
		if cur.parent == nil || (limit > 0 && idx == offset+limit) {
			if p, exists := listItemMatchIdx[curKey]; exists {
				newPos = p
			}
			return res, newPos, nil
		}
		cur = cur.parent
	}
}

func (r *DBListRepo) GetListItem(key string) (ListItem, bool) {
	itemPtr, exists := r.listItemTracker[key]
	if exists {
		return *itemPtr, true
	}
	return ListItem{}, false
}

func (r *DBListRepo) EmitCursorMoveEvent(key string) {
	r.localCursorMoveChan <- cursorMoveEvent{
		listItemKey:  key,
		unixNanoTime: time.Now().UnixNano(),
	}
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
		r.Update("", note, item)
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
