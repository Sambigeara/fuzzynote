package service

import (
	"errors"
	"fmt"
	"math/rand"
	"path"
	"time"
)

type (
	uuid         uint32
	fileSchemaID uint16
)

const (
	// This is THE date that Golang needs to determine custom formatting
	dateFormat     = "Mon, Jan 2, 2006"
	rootFileName   = "primary.db"
	walFilePattern = "wal_%v.db"
	syncFile       = "_sync_lock.db"
)

// ListRepo represents the main interface to the in-mem ListItem store
type ListRepo interface {
	Add(line string, note *[]byte, idx int, callback func()) error
	Update(line string, note *[]byte, idx int, callback func()) error
	Delete(idx int, callback func()) error
	MoveUp(idx int, callback func()) error
	MoveDown(idx int, callback func()) error
	ToggleVisibility(idx int, callback func()) error
	Undo(callback func()) error
	Redo(callback func()) error
	Match(keys [][]rune, showHidden bool) ([]MatchItem, error)
	GetMatchPattern(sub []rune) (matchPattern, int)
	ScheduleCursorMove(callback func()) error
}

// Wal manages the state of the WAL, via all update functions and replay functionality
type Wal struct {
	uuid uuid
	// log represents a fresh set of events (unique from the historical log below)
	log *[]eventLog
	// fullLog is a historical log of events
	fullLog           *[]eventLog
	listItemTracker   map[string]*ListItem
	walPathPattern    string
	latestWalSchemaID uint16
	syncFilePath      string
}

func generateUUID() uuid {
	return uuid(rand.Uint32())
}

func NewWal(rootDir string) *Wal {
	return &Wal{
		uuid:              generateUUID(),
		walPathPattern:    path.Join(rootDir, walFilePattern),
		latestWalSchemaID: latestWalSchemaID,
		log:               &[]eventLog{},
		fullLog:           &[]eventLog{},
		listItemTracker:   make(map[string]*ListItem),
		syncFilePath:      path.Join(rootDir, syncFile),
	}
}

type listItemKey string

const listItemKeyPattern = "%v_%v"

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	Root               *ListItem
	NextID             uint64
	rootPath           string
	eventLogger        *DbEventLogger
	wal                *Wal
	matchListItems     []*ListItem
	latestFileSchemaID fileSchemaID
	EventQueue         chan *eventLog
}

// ListItem represents a single item in the returned list, based on the Match() input
type ListItem struct {
	// TODO these can all be private now
	Line        string
	Note        *[]byte
	IsHidden    bool
	originUUID  uuid
	child       *ListItem
	parent      *ListItem
	id          uint64
	matchChild  *ListItem
	matchParent *ListItem
}

type bits uint32

const (
	hidden bits = 1 << iota
)

func set(b, flag bits) bits    { return b | flag }
func clear(b, flag bits) bits  { return b &^ flag }
func toggle(b, flag bits) bits { return b ^ flag }
func has(b, flag bits) bool    { return b&flag != 0 }

// NewDBListRepo returns a pointer to a new instance of DBListRepo
func NewDBListRepo(rootDir string) *DBListRepo {
	rootPath := path.Join(rootDir, rootFileName)
	return &DBListRepo{
		rootPath:           rootPath,
		eventLogger:        NewDbEventLogger(),
		wal:                NewWal(rootDir),
		NextID:             1,
		latestFileSchemaID: fileSchemaID(3),
		EventQueue:         make(chan *eventLog),
	}
}

func (r *DBListRepo) getLog(e eventType, id uint64, targetID uint64, newLine string, newNote *[]byte, originUUID uuid, callback func()) eventLog {
	return eventLog{
		uuid:             originUUID,
		unixNanoTime:     time.Now().UnixNano(),
		eventType:        e,
		listItemID:       id,
		targetListItemID: targetID,
		line:             newLine,
		note:             newNote,
		callback:         callback,
		//curRoot:          r.Root,
	}
}

func (r *DBListRepo) AddLog(el eventLog) {
	// Append to log
	*r.wal.log = append(*r.wal.log, el)
}

// Add adds a new LineItem with string, note and a position to insert the item into the matched list
func (r *DBListRepo) Add(line string, note *[]byte, idx int, callback func()) error {
	// TODO put idx check and retrieval into single helper function
	if idx < 0 || idx > len(r.matchListItems) {
		return fmt.Errorf("ListItem idx out of bounds: %v", idx)
	}

	var childID uint64
	// In order to be able to resolve child node from the tracker mapping, we need UUIDs to be consistent
	// Therefore, whenever we reference a child, we need to set the originUUID to be consistent
	// This is (conveniently) in line with intended behaviour for now. If we merge a remote, it makes sense
	// to attribute new items to that WAL when adding below focused items.
	// TODO actually, this should RESOLVE with the original UUID, but set with the local!!!
	newUUID := r.wal.uuid
	if idx > 0 {
		childItem := r.matchListItems[idx-1]
		childID = childItem.id
		newUUID = childItem.originUUID
	}
	el := r.getLog(addEvent, r.NextID, childID, line, note, newUUID, callback)
	r.EventQueue <- &el
	return nil
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) Update(line string, note *[]byte, idx int, callback func()) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return fmt.Errorf("ListItem idx out of bounds: %v", idx)
	}

	listItem := r.matchListItems[idx]

	// Need to add to Undo/Redo log before mutating, because the getLog
	// function requires previous state
	// TODO unbreak this
	r.addUndoLog(updateEvent, listItem, line, note)

	el := r.getLog(updateEvent, listItem.id, 0, line, note, listItem.originUUID, callback)
	var err error
	r.EventQueue <- &el
	return err
}

// Delete will remove an existing ListItem
func (r *DBListRepo) Delete(idx int, callback func()) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	el := r.getLog(deleteEvent, listItem.id, 0, "", nil, listItem.originUUID, callback)
	var err error
	r.EventQueue <- &el
	return err
}

// MoveUp will swop a ListItem with the ListItem directly above it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveUp(idx int, callback func()) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetItemID uint64
	if listItem.matchChild != nil {
		targetItemID = listItem.matchChild.id
	} else if listItem.child != nil {
		// Cover nil child case (e.g. attempting to move top of list up)

		// matchChild will only be null in this context on initial startup with loading
		// from the WAL
		targetItemID = listItem.child.id
	}

	// There's no point in moving if there's nothing to move to
	el := r.getLog(moveUpEvent, listItem.id, targetItemID, "", nil, listItem.originUUID, callback)
	r.EventQueue <- &el
	return nil
}

// MoveDown will swop a ListItem with the ListItem directly below it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveDown(idx int, callback func()) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetItemID uint64
	if listItem.matchParent != nil {
		targetItemID = listItem.matchParent.id
	} else if listItem.parent != nil {
		targetItemID = listItem.parent.id
	}

	el := r.getLog(moveDownEvent, listItem.id, targetItemID, "", nil, listItem.originUUID, callback)
	r.EventQueue <- &el
	return nil
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(idx int, callback func()) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var evType eventType
	if listItem.IsHidden {
		evType = showEvent
	} else {
		evType = hideEvent
	}
	el := r.getLog(evType, listItem.id, 0, "", nil, listItem.originUUID, callback)
	r.EventQueue <- &el
	return nil
}

func (r *DBListRepo) Undo(callback func()) error {
	if r.eventLogger.curIdx > 0 {
		// undo event log
		uel := r.eventLogger.log[r.eventLogger.curIdx]

		targetListItemID := uint64(0)
		if uel.ptr.child != nil {
			targetListItemID = uel.ptr.child.id
		}

		el := r.getLog(oppositeEvent[uel.eventType], uel.ptr.id, targetListItemID, uel.undoLine, uel.undoNote, uel.uuid, callback)
		var err error
		r.EventQueue <- &el
		r.eventLogger.curIdx--
		return err
	}
	return nil
}

func (r *DBListRepo) Redo(callback func()) error {
	// Redo needs to look forward +1 index when actioning events
	if r.eventLogger.curIdx < len(r.eventLogger.log)-1 {
		uel := r.eventLogger.log[r.eventLogger.curIdx+1]

		targetListItemID := uint64(0)
		if uel.ptr.child != nil {
			targetListItemID = uel.ptr.child.id
		}

		el := r.getLog(uel.eventType, uel.ptr.id, targetListItemID, uel.redoLine, uel.redoNote, uel.uuid, callback)
		var err error
		r.EventQueue <- &el
		r.eventLogger.curIdx++
		return err
	}
	return nil
}

// MatchItem holds all data required by the client
type MatchItem struct {
	Line     string
	Note     *[]byte
	IsHidden bool
}

// Match takes a set of search groups and applies each to all ListItems, returning those that
// fulfil all rules.
func (r *DBListRepo) Match(keys [][]rune, showHidden bool) ([]MatchItem, error) {
	// For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups

	// We need to pre-process the keys to parse any operators. We can't do this in the same loop as when
	// we have no matching lines, the parsing logic will not be reached, and things get messy
	for i, group := range keys {
		group = []rune(parseOperatorGroups(string(group)))
		// TODO Confirm: The slices within the slice appear to be the same mem locations as those
		// passed in so they mutate as needed
		keys[i] = group
	}

	cur := r.Root
	var lastCur *ListItem

	r.matchListItems = []*ListItem{}
	res := []MatchItem{}

	if cur == nil {
		return res, nil
	}

	for {
		// Nullify match pointers
		// TODO centralise this logic, it's too closely coupled with the moveItem logic (if match pointers
		// aren't cleaned up between ANY ops, it can lead to weird behaviour as things operate based on
		// the existence and setting of them)
		cur.matchChild, cur.matchParent = nil, nil

		if showHidden || !cur.IsHidden {
			matched := true
			for _, group := range keys {
				// Match any items with empty Lines (this accounts for lines added when search is active)
				// "active" listItems pass automatically to allow mid-search item editing
				if len(cur.Line) == 0 {
					break
				}
				// TODO unfortunate reuse of vars - refactor to tidy
				pattern, nChars := r.GetMatchPattern(group)
				if !isMatch(group[nChars:], cur.Line, pattern) {
					matched = false
					break
				}
			}
			if matched {
				r.matchListItems = append(r.matchListItems, cur)
				res = append(res, MatchItem{
					cur.Line,
					cur.Note,
					cur.IsHidden,
				})

				if lastCur != nil {
					lastCur.matchParent = cur
				}
				cur.matchChild = lastCur
				lastCur = cur
			}
		}

		if cur.parent == nil {
			return res, nil
		}

		cur = cur.parent
	}
}

func (r *DBListRepo) ScheduleCursorMove(callback func()) error {
	el := eventLog{
		eventType: cursorMoveEvent,
		callback:  callback,
	}
	r.EventQueue <- &el
	return nil
}
