package service

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type (
	uuid         uint32
	fileSchemaID uint16
)

const (
	rootFileName      = "primary.db"
	walFilePattern    = "wal_%v.db"
	viewFilePattern   = "view_%v"
	exportFilePattern = "export_%v.txt"

	latestFileSchemaID = fileSchemaID(3)

	//DefaultSyncFrequency   = uint32(10000) // 10 seconds
	//DefaultGatherFrequency = uint32(30000) // 30 seconds
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
	HandleEvent(interface{}) (bool, error)
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
	log               *[]EventLog // log represents a fresh set of events (unique from the historical log below)
	latestWalSchemaID uint16
	listItemTracker   map[string]*ListItem
	LocalWalFile      LocalWalFile
	walFiles          []WalFile
	webWalFiles       []WalFile
	s3WalFiles        []WalFile
	eventsChan        chan EventLog
	stop              chan struct{}
	web               *Web

	remoteCursorMoveChan chan cursorMoveEvent
	localCursorMoveChan  chan cursorMoveEvent
	collabPositions      map[string]cursorMoveEvent
	collabMapLock        *sync.Mutex
	previousListItemKey  string

	//webSyncTicker  *time.Ticker
	//fileSyncTicker *time.Ticker
	//syncTicker *time.Ticker
	pushTicker *time.Ticker
	//gatherTicker *time.Ticker

	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
}

// NewDBListRepo returns a pointer to a new instance of DBListRepo
func NewDBListRepo(localWalFile LocalWalFile, webTokenStore WebTokenStore, syncFrequency uint32, gatherFrequency uint32) *DBListRepo {
	fakeCtx := ""
	baseUUID, err := localWalFile.Load(fakeCtx)
	if err != nil {
		log.Fatal(err)
	}

	listRepo := &DBListRepo{
		// TODO rename this cos it's solely for UNDO/REDO
		eventLogger: NewDbEventLogger(),

		// Wal stuff
		uuid:              uuid(baseUUID),
		log:               &[]EventLog{},
		latestWalSchemaID: latestWalSchemaID,
		listItemTracker:   make(map[string]*ListItem),
		LocalWalFile:      localWalFile,
		eventsChan:        make(chan EventLog),
		stop:              make(chan struct{}, 1),

		collabMapLock: &sync.Mutex{},

		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
	}

	// The localWalFile gets attached to the Wal independently (there are certain operations
	// that require us to only target the local walfile rather than all). We still need to register
	// it as we call all walfiles in the next line.
	listRepo.RegisterWalFile(localWalFile)

	// Tokens are generated on `login`
	// Theoretically only need refresh token to have a go at authentication
	if webTokenStore.RefreshToken() != "" {
		web := NewWeb(webTokenStore)
		web.uuid = listRepo.uuid // TODO does web need to store uuid??

		// Default the other ticker intervals
		//fileSyncFrequency = DefaultSyncFrequency
		//gatherFrequency = DefaultGatherFrequency

		// registerWeb also deals with the retrieval and instantiation of the web remotes
		// Keeping the web assignment outside of registerWeb, as we use registerWeb to reinstantiate
		// the web walfiles and connections periodically during runtime, and this makes it easier... (for now)
		listRepo.web = web

		// Establish the chan used to track and display collaborator cursor positions
		listRepo.remoteCursorMoveChan = make(chan cursorMoveEvent)  // incoming events
		listRepo.localCursorMoveChan = make(chan cursorMoveEvent)   // outgoing events
		listRepo.collabPositions = make(map[string]cursorMoveEvent) // map[collaboratorEmail]currentKey
	}

	// Start the web sync ticker. Strictly this isn't required if web isn't enabled, but things break if it's
	// disabled at the mo so leave in (it's inexpensive)
	//listRepo.webSyncTicker = time.NewTicker(time.Millisecond * time.Duration(fileSyncFrequency))
	// If the `web` integration isn't enabled (websockets et al), we allow the user to pass intervals
	// for local/S3 sync/push/gather. If web IS enabled, we override (above) as all syncing is done in
	// real time via websockets, and therefore short intervals aren't required.
	//listRepo.fileSyncTicker = time.NewTicker(time.Millisecond * time.Duration(fileSyncFrequency))
	//listRepo.syncTicker = time.NewTicker(time.Millisecond * time.Duration(syncFrequency))
	listRepo.pushTicker = time.NewTicker(time.Millisecond * time.Duration(syncFrequency))
	//listRepo.gatherTicker = time.NewTicker(time.Millisecond * time.Duration(gatherFrequency))

	return listRepo
}

// ListItem represents a single item in the returned list, based on the Match() input
type ListItem struct {
	// TODO these can all be private now
	Line         string
	Note         *[]byte
	IsHidden     bool
	originUUID   uuid
	creationTime int64
	child        *ListItem
	parent       *ListItem
	matchChild   *ListItem
	matchParent  *ListItem
}

func (i *ListItem) Key() string {
	return fmt.Sprintf("%d:%d", i.originUUID, i.creationTime)
}

// IsWebConnected returns whether or not the DBListRepo successfully connected to the web remote
func (r *DBListRepo) IsWebConnected() bool {
	if r.web != nil {
		return true
	}
	return false
}

func (r *DBListRepo) processEventLog(e EventType, creationTime int64, targetCreationTime int64, newLine string, newNote *[]byte, originUUID uuid, targetUUID uuid) (*ListItem, error) {
	el := EventLog{
		EventType:                  e,
		UUID:                       originUUID,
		TargetUUID:                 targetUUID,
		UnixNanoTime:               time.Now().UnixNano(),
		ListItemCreationTime:       creationTime,
		TargetListItemCreationTime: targetCreationTime,
		Line:                       newLine,
		Note:                       newNote,
	}
	r.eventsChan <- el
	*r.log = append(*r.log, el)
	var err error
	var item *ListItem
	r.Root, item, err = r.CallFunctionForEventLog(r.Root, el)
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
	// We can't for now because other invocations of processEventLog rely on the passed in (pre-existing)
	// listItem.creationTime
	now := time.Now().UnixNano()
	newItem, _ := r.processEventLog(AddEvent, now, childCreationTime, line, note, r.uuid, childUUID)
	r.addUndoLog(AddEvent, now, childCreationTime, r.uuid, childUUID, line, note, line, note)
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

	// Add the UndoLog here to allow us to access existing Line/Note state
	r.addUndoLog(UpdateEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, listItem.Line, listItem.Note, line, note)
	r.processEventLog(UpdateEvent, listItem.creationTime, childCreationTime, line, note, listItem.originUUID, childUUID)
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
	r.processEventLog(DeleteEvent, listItem.creationTime, 0, "", nil, listItem.originUUID, uuid(0))
	r.addUndoLog(DeleteEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, listItem.Line, listItem.Note, listItem.Line, listItem.Note)
	key := ""
	if listItem.child != nil {
		key = listItem.child.Key()
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
		//} else if listItem.child != nil {
		//    // Cover nil child case (e.g. attempting to move top of list up)
		//    // matchChild will only be null in this context on initial startup with loading
		//    // from the WAL
		//    targetCreationTime = listItem.child.creationTime
		//    targetUUID = listItem.child.originUUID
	}

	r.processEventLog(MoveUpEvent, listItem.creationTime, targetCreationTime, "", nil, listItem.originUUID, targetUUID)
	// There's no point in moving if there's nothing to move to
	if listItem.matchChild != nil && listItem.matchChild.creationTime != 0 {
		r.addUndoLog(MoveUpEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, "", nil, "", nil)
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
		//} else if listItem.parent != nil {
		//    targetCreationTime = listItem.parent.creationTime
		//    targetUUID = listItem.parent.originUUID
	}

	r.processEventLog(MoveDownEvent, listItem.creationTime, targetCreationTime, "", nil, listItem.originUUID, targetUUID)
	// There's no point in moving if there's nothing to move to
	if listItem.matchParent != nil && listItem.matchParent.creationTime != 0 {
		r.addUndoLog(MoveDownEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, "", nil, "", nil)
	}
	return nil
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(idx int) (string, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return "", errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var evType EventType
	var itemKey string
	if listItem.IsHidden {
		evType = ShowEvent
		r.addUndoLog(ShowEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, "", nil, "", nil)
		// Cursor should remain on newly visible key
		itemKey = listItem.Key()
	} else {
		evType = HideEvent
		r.addUndoLog(HideEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, "", nil, "", nil)
		// Set itemKey to parent if available, else child (e.g. bottom of list)
		if listItem.matchParent != nil {
			itemKey = listItem.matchParent.Key()
		} else if listItem.matchChild != nil {
			itemKey = listItem.matchChild.Key()
		}
	}
	r.processEventLog(evType, listItem.creationTime, 0, "", nil, listItem.originUUID, uuid(0))
	return itemKey, nil
}

func (r *DBListRepo) Undo() (string, error) {
	if r.eventLogger.curIdx > 0 {
		// undo event log
		uel := r.eventLogger.log[r.eventLogger.curIdx]

		listItem, err := r.processEventLog(oppositeEvent[uel.eventType], uel.listItemCreationTime, uel.targetListItemCreationTime, uel.undoLine, uel.undoNote, uel.uuid, uel.targetUUID)
		r.eventLogger.curIdx--
		return listItem.Key(), err
	}
	return "", nil
}

func (r *DBListRepo) Redo() (string, error) {
	// Redo needs to look forward +1 index when actioning events
	if r.eventLogger.curIdx < len(r.eventLogger.log)-1 {
		uel := r.eventLogger.log[r.eventLogger.curIdx+1]

		listItem, err := r.processEventLog(uel.eventType, uel.listItemCreationTime, uel.targetListItemCreationTime, uel.redoLine, uel.redoNote, uel.uuid, uel.targetUUID)
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
	if curKey != r.previousListItemKey && r.web != nil && r.web.wsConn != nil {
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
				if cur.Key() == curKey || len(cur.Line) == 0 {
					break
				}
				// TODO unfortunate reuse of vars - refactor to tidy
				pattern, nChars := GetMatchPattern(group)
				if !isMatch(group[nChars:], cur.Line, pattern) {
					matched = false
					break
				}
			}
			if matched {
				// Pagination: only add to results set if we've surpassed the min boundary of the page,
				// otherwise only increment `idx`.
				if idx >= offset {
					r.matchListItems = append(r.matchListItems, cur)
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
	return r.generatePlainTextFile(matchedItems)
}
