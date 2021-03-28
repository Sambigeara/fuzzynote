package service

import (
	"errors"
	"fmt"
	"os"
	"path"
	"time"
)

type (
	uuid         uint32
	fileSchemaID uint16
)

const (
	// This is THE date that Golang needs to determine custom formatting
	rootFileName    = "primary.db"
	walFilePattern  = "wal_%v.db"
	viewFilePattern = "view_%v.db"
	syncFile        = "_sync_lock.db"
)

// ListRepo represents the main interface to the in-mem ListItem store
type ListRepo interface {
	Add(line string, note *[]byte, idx int) error
	Update(line string, note *[]byte, idx int) error
	Delete(idx int) error
	MoveUp(idx int) (bool, error)
	MoveDown(idx int) (bool, error)
	ToggleVisibility(idx int) error
	Undo() error
	Redo() error
	Match(keys [][]rune, showHidden bool) ([]ListItem, error)
	GetMatchPattern(sub []rune) (matchPattern, int)
	GenerateView(matchKeys [][]rune, showHidden bool) error
}

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	Root               *ListItem
	NextID             uint64
	rootPath           string
	eventLogger        *DbEventLogger
	wal                *Wal
	matchListItems     []*ListItem
	latestFileSchemaID fileSchemaID
	listItemMatchIdx   map[string]int
}

// ListItem represents a single item in the returned list, based on the Match() input
type ListItem struct {
	// TODO these can all be private now
	Line         string
	Note         *[]byte
	IsHidden     bool
	Offset       int
	originUUID   uuid
	creationTime int64
	child        *ListItem
	parent       *ListItem
	matchChild   *ListItem
	matchParent  *ListItem
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
func NewDBListRepo(rootDir string, localWalFile *localWalFile, pushFrequency uint16) *DBListRepo {
	// Make sure the root directory exists
	os.Mkdir(rootDir, os.ModePerm)

	rootPath := path.Join(rootDir, rootFileName)
	return &DBListRepo{
		rootPath:           rootPath,
		eventLogger:        NewDbEventLogger(),
		wal:                NewWal(localWalFile, pushFrequency),
		NextID:             1,
		latestFileSchemaID: fileSchemaID(3),
		listItemMatchIdx:   make(map[string]int),
	}
}

func (r *DBListRepo) processEventLog(e eventType, creationTime int64, targetCreationTime int64, newLine string, newNote *[]byte, originUUID uuid, targetUUID uuid) error {
	el := EventLog{
		eventType:                  e,
		uuid:                       originUUID,
		targetUUID:                 targetUUID,
		unixNanoTime:               time.Now().UnixNano(),
		listItemCreationTime:       creationTime,
		targetListItemCreationTime: targetCreationTime,
		line:                       newLine,
		note:                       newNote,
	}
	r.wal.eventsChan <- el
	*r.wal.log = append(*r.wal.log, el)
	var err error
	r.Root, err = r.CallFunctionForEventLog(r.Root, el)
	return err
}

// Add adds a new LineItem with string, note and a position to insert the item into the matched list
func (r *DBListRepo) Add(line string, note *[]byte, idx int) error {
	// TODO put idx check and retrieval into single helper function
	if idx < 0 || idx > len(r.matchListItems) {
		return fmt.Errorf("ListItem idx out of bounds: %v", idx)
	}

	var childCreationTime int64
	// In order to be able to resolve child node from the tracker mapping, we need UUIDs to be consistent
	// Therefore, whenever we reference a child, we need to set the originUUID to be consistent
	// This is (conveniently) in line with intended behaviour for now. If we merge a remote, it makes sense
	// to attribute new items to that WAL when adding below focused items.
	// TODO actually, this should RESOLVE with the original UUID, but set with the local!!!
	newUUID := r.wal.uuid
	if idx > 0 {
		childItem := r.matchListItems[idx-1]
		childCreationTime = childItem.creationTime
		newUUID = childItem.originUUID
	}
	// TODO ideally we'd use the same unixtime for log creation and the listItem creation time for Add()
	now := time.Now().UnixNano()
	r.processEventLog(addEvent, now, childCreationTime, line, note, newUUID, newUUID)
	r.addUndoLog(addEvent, now, childCreationTime, newUUID, newUUID, line, note, line, note)
	return nil
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) Update(line string, note *[]byte, idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return fmt.Errorf("ListItem idx out of bounds: %v", idx)
	}

	listItem := r.matchListItems[idx]

	// Add the UndoLog here to allow us to access existing Line/Note state
	r.addUndoLog(updateEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, listItem.Line, listItem.Note, line, note)
	r.processEventLog(updateEvent, listItem.creationTime, 0, line, note, listItem.originUUID, uuid(0))
	return nil
}

// Delete will remove an existing ListItem
func (r *DBListRepo) Delete(idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetCreationTime int64
	var targetUUID uuid
	if listItem.child != nil {
		targetCreationTime = listItem.child.creationTime
		targetUUID = listItem.child.originUUID
	}
	r.processEventLog(deleteEvent, listItem.creationTime, 0, "", nil, listItem.originUUID, uuid(0))
	r.addUndoLog(deleteEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, listItem.Line, listItem.Note, listItem.Line, listItem.Note)
	return nil
}

// MoveUp will swop a ListItem with the ListItem directly above it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveUp(idx int) (bool, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return false, errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetCreationTime int64
	var targetUUID uuid
	if listItem.matchChild != nil {
		targetCreationTime = listItem.matchChild.creationTime
		targetUUID = listItem.matchChild.originUUID
	} else if listItem.child != nil {
		// Cover nil child case (e.g. attempting to move top of list up)

		// matchChild will only be null in this context on initial startup with loading
		// from the WAL
		targetCreationTime = listItem.child.creationTime
		targetUUID = listItem.child.originUUID
	}

	r.processEventLog(moveUpEvent, listItem.creationTime, targetCreationTime, "", nil, listItem.originUUID, targetUUID)
	// There's no point in moving if there's nothing to move to
	if targetCreationTime != 0 {
		r.addUndoLog(moveUpEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, "", nil, "", nil)
		return true, nil
	}
	return false, nil
}

// MoveDown will swop a ListItem with the ListItem directly below it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveDown(idx int) (bool, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return false, errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var targetCreationTime int64
	var targetUUID uuid
	if listItem.matchParent != nil {
		targetCreationTime = listItem.matchParent.creationTime
		targetUUID = listItem.matchParent.originUUID
	} else if listItem.parent != nil {
		targetCreationTime = listItem.parent.creationTime
		targetUUID = listItem.parent.originUUID
	}

	r.processEventLog(moveDownEvent, listItem.creationTime, targetCreationTime, "", nil, listItem.originUUID, targetUUID)
	// There's no point in moving if there's nothing to move to
	if targetCreationTime != 0 {
		r.addUndoLog(moveDownEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, "", nil, "", nil)
		return true, nil
	}
	return false, nil
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var evType eventType
	if listItem.IsHidden {
		evType = showEvent
		r.addUndoLog(showEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, "", nil, "", nil)
	} else {
		evType = hideEvent
		r.addUndoLog(hideEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, "", nil, "", nil)
	}
	r.processEventLog(evType, listItem.creationTime, 0, "", nil, listItem.originUUID, uuid(0))
	return nil
}

func (r *DBListRepo) Undo() error {
	if r.eventLogger.curIdx > 0 {
		// undo event log
		uel := r.eventLogger.log[r.eventLogger.curIdx]

		r.processEventLog(oppositeEvent[uel.eventType], uel.listItemCreationTime, uel.targetListItemCreationTime, uel.undoLine, uel.undoNote, uel.uuid, uel.targetUUID)
		var err error
		r.eventLogger.curIdx--
		return err
	}
	return nil
}

func (r *DBListRepo) Redo() error {
	// Redo needs to look forward +1 index when actioning events
	if r.eventLogger.curIdx < len(r.eventLogger.log)-1 {
		uel := r.eventLogger.log[r.eventLogger.curIdx+1]

		r.processEventLog(uel.eventType, uel.listItemCreationTime, uel.targetListItemCreationTime, uel.redoLine, uel.redoNote, uel.uuid, uel.targetUUID)
		var err error
		r.eventLogger.curIdx++
		return err
	}
	return nil
}

// Match takes a set of search groups and applies each to all ListItems, returning those that
// fulfil all rules.
func (r *DBListRepo) Match(keys [][]rune, showHidden bool) ([]ListItem, error) {
	// For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups

	cur := r.Root
	var lastCur *ListItem

	r.matchListItems = []*ListItem{}
	res := []ListItem{}

	if cur == nil {
		return res, nil
	}

	//var curIdx, curOffset int
	var curIdx int
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
				// If it exists, retrieve the previous position, compare it to the new position,
				// and return the offset with the ListItem. Otherwise, keep at the default 0.
				//
				// We set the default to 1 because it will only use the default value when we're adding new items
				// and therefore they won't exist in the map (but also want to bump down all items below).
				offset := 1
				if oldIdx, exists := r.listItemMatchIdx[fmt.Sprintf("%d:%d", cur.originUUID, cur.creationTime)]; exists {
					offset = curIdx - oldIdx
				}
				cur.Offset = offset
				res = append(res, *cur)

				if lastCur != nil {
					lastCur.matchParent = cur
				}
				cur.matchChild = lastCur
				lastCur = cur

				// Set the new idx for the next iteration
				// TODO figure out a clean way to remove old items. Maybe create a new map and override at the end.
				r.listItemMatchIdx[fmt.Sprintf("%d:%d", cur.originUUID, cur.creationTime)] = curIdx
				curIdx++
			}
		}

		if cur.parent == nil {
			return res, nil
		}

		cur = cur.parent
	}
}

func (r *DBListRepo) GenerateView(matchKeys [][]rune, showHidden bool) error {
	matchedItems, _ := r.Match(matchKeys, showHidden)

	// Now we have our list of matchedItems, we need to iterate over them to retrieve all originUUID:creationTime keys
	// which map to the wal entries
	logKeys := make(map[string]struct{})
	for _, i := range matchedItems {
		logKeys[fmt.Sprintf("%d:%d", i.originUUID, i.creationTime)] = struct{}{}
	}

	// Now we have our keys, we can iterate over the entire wal, and generate a partial wal containing only eventLogs
	// for ListItems retrieved above.
	// IMPORTANT: we also need to include ALL deleteEvent logs, as otherwise we may end up with orphaned items in the
	// target view.
	partialWal := []EventLog{}
	for _, e := range *r.wal.log {
		if _, exists := logKeys[fmt.Sprintf("%d:%d", e.uuid, e.listItemCreationTime)]; exists || e.eventType == deleteEvent {
			partialWal = append(partialWal, e)
		}
	}

	// We now need to generate a temp name (NOT matching the standard wal pattern) and push to it. We can then manually
	// retrieve and handle the wal (for now)
	// Use the current time to generate the name
	b := buildByteWal(&partialWal)
	viewName := fmt.Sprintf(path.Join(r.wal.localWalFile.getRootDir(), viewFilePattern), time.Now().UnixNano())
	r.wal.localWalFile.flush(b, viewName)
	return nil
}
