package service

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
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

// TODO can this be made useful?
// ListRepo represents the main interface to the in-mem ListItem store
//type ListRepo interface {
//    Add(line string, note *[]byte, idx int) (string, error)
//    Update(line string, note *[]byte, idx int) error
//    Delete(idx int) (string, error)
//    MoveUp(idx int) error
//    MoveDown(idx int) error
//    ToggleVisibility(idx int) (string, error)
//    Undo() (string, error)
//    Redo() (string, error)
//    Match(keys [][]rune, showHidden bool, curKey string, offset int, limit int) ([]ListItem, int, error)
//    //SetCollabPosition(cursorMoveEvent) bool
//    //GetCollabPositions() map[string][]string
//}

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	Root           *ListItem
	eventLogger    *DbEventLogger
	matchListItems []*ListItem

	// Wal stuff
	uuid              uuid
	log               []EventLog // log represents a fresh set of events (unique from the historical log below)
	latestWalSchemaID uint16
	listItemTracker   map[string]*ListItem
	eventsChan        chan EventLog
	web               *Web

	remoteCursorMoveChan chan cursorMoveEvent
	localCursorMoveChan  chan cursorMoveEvent
	collabPositions      map[string]cursorMoveEvent
	collabMapLock        *sync.Mutex
	previousListItemKey  string

	email                     string
	cfgFriendRegex            *regexp.Regexp
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

	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
}

// NewDBListRepo returns a pointer to a new instance of DBListRepo
func NewDBListRepo(localWalFile LocalWalFile, webTokenStore WebTokenStore, syncFrequency uint32, gatherFrequency uint32) *DBListRepo {
	listRepo := &DBListRepo{
		// TODO rename this cos it's solely for UNDO/REDO
		eventLogger: NewDbEventLogger(),

		// Wal stuff
		uuid:              generateUUID(),
		log:               []EventLog{},
		latestWalSchemaID: latestWalSchemaID,
		listItemTracker:   make(map[string]*ListItem),
		LocalWalFile:      localWalFile,
		eventsChan:        make(chan EventLog),

		collabMapLock: &sync.Mutex{},

		webWalFiles:    make(map[string]WalFile),
		allWalFiles:    make(map[string]WalFile),
		syncWalFiles:   make(map[string]WalFile),
		webWalFileMut:  &sync.RWMutex{},
		allWalFileMut:  &sync.RWMutex{},
		syncWalFileMut: &sync.RWMutex{},

		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},

		friends:           make(map[string]map[string]int64),
		friendsUpdateLock: &sync.RWMutex{},
	}

	// The localWalFile gets attached to the Wal independently (there are certain operations
	// that require us to only target the local walfile rather than all). We still need to register
	// it as we call all walfiles in the next line.
	listRepo.AddWalFile(localWalFile, true)

	if webTokenStore.Email() != "" {
		listRepo.setEmail(webTokenStore.Email())
		listRepo.cfgFriendRegex = regexp.MustCompile(fmt.Sprintf("^fzn_cfg:friend +(%s) +@%s$", EmailRegex, regexp.QuoteMeta(listRepo.email)))
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
	r.email = email
	r.friends[email] = make(map[string]int64)
}

// ListItem represents a single item in the returned list, based on the Match() input
type ListItem struct {
	// TODO these can all be private now
	//Line         string
	rawLine      string
	Note         *[]byte
	IsHidden     bool
	originUUID   uuid
	creationTime int64
	child        *ListItem
	parent       *ListItem
	matchChild   *ListItem
	matchParent  *ListItem
	friends      lineFriends
}

// Line returns a post-processed rawLine, with any matched collaborators omitted
func (i *ListItem) Line() string {
	if i.friends.isProcessed {
		return i.rawLine[:i.friends.offset]
	}
	return i.rawLine
}

func (i *ListItem) Friends() []string {
	// The emails are stored as a map of strings. We need to generate an sorted slice to return
	// to the client
	// TODO cache for optimisation??
	sortedEmails := []string{}
	for e := range i.friends.emails {
		sortedEmails = append(sortedEmails, e)
	}
	sort.Strings(sortedEmails)
	return sortedEmails
}

func (i *ListItem) Key() string {
	return fmt.Sprintf("%d:%d", i.originUUID, i.creationTime)
}

func (r *DBListRepo) addEventLog(el EventLog) (*ListItem, error) {
	// If an event is an Update which is setting a previously set note to an empty note (e.g. a deletion),
	// we mutate the empty note by adding a null byte. This occurs in the thread which consumes from
	// eventsChan. Because `el.Note` is a ptr to a note, when we update it in that thread, it's also
	// updated on the original event which we pass to processEventLog. This is still the case
	// even if we copy the struct type (as we pass the ptr address in the copy). Therefore, we need to
	// do this rather nasty copy operation to copy the note and and set the new ptr address. We use this
	// copy for the websocket event.
	elCopy := el
	if el.Note != nil {
		newNote := *el.Note
		elCopy.Note = &newNote
	}

	var err error
	var item *ListItem
	r.Root, item, err = r.processEventLog(r.Root, &el)

	// We pass a pointer to the new event log to the processing function, and publish to the eventsChan and
	// r.log after, as there is a chance of post-processing being applied within the processEventLog step.
	// We want these mutations to be applied prior to flushing to local state or to remotes.
	r.eventsChan <- elCopy
	r.log = append(r.log, el)

	return item, err
}

// Add adds a new LineItem with string, note and a position to insert the item into the matched list
// It returns a string representing the unique key of the newly created item
func (r *DBListRepo) Add(line string, note *[]byte, idx int) (string, error) {
	// TODO put idx check and retrieval into single helper function
	if idx < 0 || idx > len(r.matchListItems) {
		return "", fmt.Errorf("ListItem idx out of bounds: %v", idx)
	}

	childCreationTime := int64(0)
	// In order to be able to resolve child node from the tracker mapping, we need UUIDs to be consistent
	// Therefore, whenever we reference a child, we need to set the originUUID to be consistent
	childUUID := uuid(0)
	if idx > 0 {
		childItem := r.matchListItems[idx-1]
		childCreationTime = childItem.creationTime
		childUUID = childItem.originUUID
	}
	// TODO ideally we'd use the same unixtime for log creation and the listItem creation time for Add()
	// We can't for now because other invocations of addEventLog rely on the passed in (pre-existing)
	// listItem.creationTime
	now := time.Now().UnixNano()
	el := EventLog{
		EventType:                  AddEvent,
		UUID:                       r.uuid,
		TargetUUID:                 childUUID,
		UnixNanoTime:               time.Now().UnixNano(),
		ListItemCreationTime:       now,
		TargetListItemCreationTime: childCreationTime,
		Line:                       line,
		Note:                       note,
	}
	newItem, _ := r.addEventLog(el)
	undoEl := EventLog{
		EventType:            DeleteEvent,
		UUID:                 r.uuid,
		ListItemCreationTime: now,
	}
	r.addUndoLog(undoEl, el)
	return newItem.Key(), nil
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) Update(line string, note *[]byte, idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return fmt.Errorf("ListItem idx out of bounds: %v", idx)
	}

	listItem := r.matchListItems[idx]
	childCreationTime := int64(0)
	childUUID := uuid(0)
	if listItem.child != nil {
		childCreationTime = listItem.child.creationTime
		childUUID = listItem.child.originUUID
	}

	el := EventLog{
		EventType:                  UpdateEvent,
		UUID:                       listItem.originUUID,
		TargetUUID:                 childUUID,
		UnixNanoTime:               time.Now().UnixNano(),
		ListItemCreationTime:       listItem.creationTime,
		TargetListItemCreationTime: childCreationTime,
		Line:                       line,
		Note:                       note,
	}

	// Undo event created from pre event processing state
	undoEl := EventLog{
		EventType:                  UpdateEvent,
		UUID:                       listItem.originUUID,
		TargetUUID:                 childUUID,
		ListItemCreationTime:       listItem.creationTime,
		TargetListItemCreationTime: childCreationTime,
		Line:                       listItem.rawLine,
		Note:                       listItem.Note,
	}

	r.addEventLog(el)
	r.addUndoLog(undoEl, el)
	return nil
}

// Delete will remove an existing ListItem
func (r *DBListRepo) Delete(idx int) (string, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return "", errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetCreationTime int64
	var targetUUID uuid
	if listItem.child != nil {
		targetCreationTime = listItem.child.creationTime
		targetUUID = listItem.child.originUUID
	}
	el := EventLog{
		EventType:            DeleteEvent,
		UUID:                 listItem.originUUID,
		UnixNanoTime:         time.Now().UnixNano(),
		ListItemCreationTime: listItem.creationTime,
	}

	r.addEventLog(el)
	undoEl := EventLog{
		EventType:                  AddEvent,
		UUID:                       r.uuid,
		TargetUUID:                 targetUUID,
		ListItemCreationTime:       listItem.creationTime,
		TargetListItemCreationTime: targetCreationTime,
		Line:                       listItem.rawLine,
		Note:                       listItem.Note,
	}
	r.addUndoLog(undoEl, el)

	key := ""
	// We use matchChild to set the next "current key", otherwise, if we delete the final matched item, which happens
	// to have a child in the full (un-matched) set, it will default to that on the return (confusing because it will
	// not match the current specified search groups)
	if listItem.matchChild != nil {
		key = listItem.matchChild.Key()
	}
	return key, nil
}

// MoveUp will swop a ListItem with the ListItem directly above it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveUp(idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetCreationTime int64
	var targetUUID uuid
	if listItem.matchChild != nil {
		// We need to target the child of the child (as when we apply move events, we specify the target that we want to be
		// the new child. Only relevant for non-startup case
		if listItem.matchChild.child != nil {
			targetCreationTime = listItem.matchChild.child.creationTime
			targetUUID = listItem.matchChild.child.originUUID
		}
	}

	if listItem.matchChild != nil && listItem.matchChild.creationTime != 0 {
		el := EventLog{
			EventType:                  MoveUpEvent,
			UUID:                       listItem.originUUID,
			TargetUUID:                 targetUUID,
			UnixNanoTime:               time.Now().UnixNano(),
			ListItemCreationTime:       listItem.creationTime,
			TargetListItemCreationTime: targetCreationTime,
		}
		r.addEventLog(el)

		undoEl := EventLog{
			EventType:                  MoveDownEvent,
			UUID:                       listItem.originUUID,
			TargetUUID:                 listItem.matchChild.originUUID,
			ListItemCreationTime:       listItem.creationTime,
			TargetListItemCreationTime: listItem.matchChild.creationTime,
		}
		r.addUndoLog(undoEl, el)
	}
	return nil
}

// MoveDown will swop a ListItem with the ListItem directly below it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveDown(idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetCreationTime int64
	var targetUUID uuid
	if listItem.matchParent != nil {
		targetCreationTime = listItem.matchParent.creationTime
		targetUUID = listItem.matchParent.originUUID
	}

	if listItem.matchParent != nil && listItem.matchParent.creationTime != 0 {
		el := EventLog{
			EventType:                  MoveDownEvent,
			UUID:                       listItem.originUUID,
			TargetUUID:                 targetUUID,
			UnixNanoTime:               time.Now().UnixNano(),
			ListItemCreationTime:       listItem.creationTime,
			TargetListItemCreationTime: targetCreationTime,
		}
		r.addEventLog(el)

		var targetUUID uuid
		var targetCreationTime int64
		if listItem.matchChild != nil {
			targetUUID = listItem.matchChild.originUUID
			targetCreationTime = listItem.matchChild.creationTime
		}
		undoEl := EventLog{
			EventType:                  MoveUpEvent,
			UUID:                       listItem.originUUID,
			TargetUUID:                 targetUUID,
			ListItemCreationTime:       listItem.creationTime,
			TargetListItemCreationTime: targetCreationTime,
		}
		r.addUndoLog(undoEl, el)
	}
	return nil
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(idx int) (string, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return "", errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var evType, oppEvType EventType
	var itemKey string
	if listItem.IsHidden {
		evType = ShowEvent
		oppEvType = HideEvent
		// Cursor should remain on newly visible key
		itemKey = listItem.Key()
	} else {
		evType = HideEvent
		oppEvType = ShowEvent
		// Set itemKey to parent if available, else child (e.g. bottom of list)
		if listItem.matchParent != nil {
			itemKey = listItem.matchParent.Key()
		} else if listItem.matchChild != nil {
			itemKey = listItem.matchChild.Key()
		}
	}
	el := EventLog{
		EventType:            evType,
		UUID:                 listItem.originUUID,
		UnixNanoTime:         time.Now().UnixNano(),
		ListItemCreationTime: listItem.creationTime,
	}
	r.addEventLog(el)

	undoEl := EventLog{
		EventType:            oppEvType,
		UUID:                 listItem.originUUID,
		ListItemCreationTime: listItem.creationTime,
	}
	r.addUndoLog(undoEl, el)

	return itemKey, nil
}

func (r *DBListRepo) Undo() (string, error) {
	if r.eventLogger.curIdx > 0 {
		uel := r.eventLogger.log[r.eventLogger.curIdx]
		el := uel.oppEvent
		el.UnixNanoTime = time.Now().UnixNano()
		listItem, err := r.addEventLog(el)
		r.eventLogger.curIdx--
		return listItem.Key(), err
	}
	return "", nil
}

func (r *DBListRepo) Redo() (string, error) {
	// Redo needs to look forward +1 index when actioning events
	if r.eventLogger.curIdx < len(r.eventLogger.log)-1 {
		uel := r.eventLogger.log[r.eventLogger.curIdx+1]
		el := uel.event
		el.UnixNanoTime = time.Now().UnixNano()
		listItem, err := r.addEventLog(el)
		r.eventLogger.curIdx++
		return listItem.Key(), err
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

	r.matchListItems = []*ListItem{}

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
		r.localCursorMoveChan <- cursorMoveEvent{
			listItemKey:  curKey,
			unixNanoTime: time.Now().UnixNano(),
		}
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
				if cur.Key() == curKey || len(cur.rawLine) == 0 {
					break
				}
				// TODO unfortunate reuse of vars - refactor to tidy
				pattern, nChars := GetMatchPattern(group)
				if !isMatch(group[nChars:], cur.rawLine, pattern) {
					matched = false
					break
				}
			}
			if matched {
				// Pagination: only add to results set if we've surpassed the min boundary of the page,
				// otherwise only increment `idx`.
				if idx >= offset {
					r.matchListItems = append(r.matchListItems, cur)

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
					listItemMatchIdx[cur.Key()] = idx
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
