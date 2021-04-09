package service

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"
)

type (
	uuid         uint32
	fileSchemaID uint16
)

const (
	rootFileName    = "primary.db"
	walFilePattern  = "wal_%v.db"
	viewFilePattern = "view_%v.db"
)

type bits uint32

const (
	hidden bits = 1 << iota
)

func set(b, flag bits) bits    { return b | flag }
func clear(b, flag bits) bits  { return b &^ flag }
func toggle(b, flag bits) bits { return b ^ flag }
func has(b, flag bits) bool    { return b&flag != 0 }

// ListRepo represents the main interface to the in-mem ListItem store
type ListRepo interface {
	Add(line string, note *[]byte, idx int) (string, error)
	Update(line string, note *[]byte, idx int) error
	Delete(idx int) (string, error)
	MoveUp(idx int) error
	MoveDown(idx int) error
	ToggleVisibility(idx int) (string, error)
	Undo() (string, error)
	Redo() (string, error)
	Match(keys [][]rune, showHidden bool, curKey string) ([]ListItem, int, error)
	GetMatchPattern(sub []rune) (matchPattern, int)
	GenerateView(matchKeys [][]rune, showHidden bool) error
	GetNewLinePrefix(search [][]rune) string
}

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	Root               *ListItem
	rootPath           string
	eventLogger        *DbEventLogger
	wal                *Wal
	matchListItems     []*ListItem
	latestFileSchemaID fileSchemaID
}

// NewDBListRepo returns a pointer to a new instance of DBListRepo
func NewDBListRepo(rootDir string, localWalFile WalFile, pushFrequency uint16) *DBListRepo {
	// Make sure the root directory exists
	os.Mkdir(rootDir, os.ModePerm)

	rootPath := path.Join(rootDir, rootFileName)
	return &DBListRepo{
		rootPath:           rootPath,
		eventLogger:        NewDbEventLogger(),
		wal:                NewWal(localWalFile, pushFrequency),
		latestFileSchemaID: fileSchemaID(3),
	}
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

func (i *ListItem) Key() string {
	return fmt.Sprintf("%d:%d", i.originUUID, i.creationTime)
}

func (r *DBListRepo) processEventLog(e eventType, creationTime int64, targetCreationTime int64, newLine string, newNote *[]byte, originUUID uuid, targetUUID uuid) (*ListItem, error) {
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
	newItem, _ := r.processEventLog(addEvent, now, childCreationTime, line, note, r.wal.uuid, childUUID)
	r.addUndoLog(addEvent, now, childCreationTime, r.wal.uuid, childUUID, line, note, line, note)
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
	r.addUndoLog(updateEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, listItem.Line, listItem.Note, line, note)
	r.processEventLog(updateEvent, listItem.creationTime, childCreationTime, line, note, listItem.originUUID, childUUID)
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
	r.processEventLog(deleteEvent, listItem.creationTime, 0, "", nil, listItem.originUUID, uuid(0))
	r.addUndoLog(deleteEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, listItem.Line, listItem.Note, listItem.Line, listItem.Note)
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
	if listItem.matchChild != nil && listItem.matchChild.creationTime != 0 {
		r.addUndoLog(moveUpEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, "", nil, "", nil)
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
	} else if listItem.parent != nil {
		targetCreationTime = listItem.parent.creationTime
		targetUUID = listItem.parent.originUUID
	}

	r.processEventLog(moveDownEvent, listItem.creationTime, targetCreationTime, "", nil, listItem.originUUID, targetUUID)
	// There's no point in moving if there's nothing to move to
	if listItem.matchParent != nil && listItem.matchParent.creationTime != 0 {
		r.addUndoLog(moveDownEvent, listItem.creationTime, targetCreationTime, listItem.originUUID, targetUUID, "", nil, "", nil)
	}
	return nil
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(idx int) (string, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return "", errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	var evType eventType
	var itemKey string
	if listItem.IsHidden {
		evType = showEvent
		r.addUndoLog(showEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, "", nil, "", nil)
		// Cursor should remain on newly visible key
		itemKey = listItem.Key()
	} else {
		evType = hideEvent
		r.addUndoLog(hideEvent, listItem.creationTime, 0, listItem.originUUID, listItem.originUUID, "", nil, "", nil)
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
// fulfil all rules.
func (r *DBListRepo) Match(keys [][]rune, showHidden bool, curKey string) ([]ListItem, int, error) {
	// For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups

	cur := r.Root
	var lastCur *ListItem

	r.matchListItems = []*ListItem{}
	res := []ListItem{}

	newPos := -1
	if cur == nil {
		return res, newPos, nil
	}

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
				pattern, nChars := r.GetMatchPattern(group)
				if !isMatch(group[nChars:], cur.Line, pattern) {
					matched = false
					break
				}
			}
			if matched {
				r.matchListItems = append(r.matchListItems, cur)
				res = append(res, *cur)

				if lastCur != nil {
					lastCur.matchParent = cur
				}
				cur.matchChild = lastCur
				lastCur = cur

				// Set the new idx for the next iteration
				listItemMatchIdx[cur.Key()] = idx
				idx++
			}
		}
		if cur.parent == nil {
			if p, exists := listItemMatchIdx[curKey]; exists {
				newPos = p
			}
			return res, newPos, nil
		}
		cur = cur.parent
	}
}

func (r *DBListRepo) GenerateView(matchKeys [][]rune, showHidden bool) error {
	matchedItems, _, _ := r.Match(matchKeys, showHidden, "")
	return r.wal.generatePartialView(matchedItems)
}

func (db *DBListRepo) GetNewLinePrefix(search [][]rune) string {
	var searchStrings []string
	for _, group := range search {
		pattern, nChars := db.GetMatchPattern(group)
		if pattern != InverseMatchPattern && len(group) > 0 {
			searchStrings = append(searchStrings, string(group[nChars:]))
		}
	}
	newString := ""
	if len(searchStrings) > 0 {
		newString = fmt.Sprintf("%s ", strings.Join(searchStrings, " "))
	}
	return newString
}
