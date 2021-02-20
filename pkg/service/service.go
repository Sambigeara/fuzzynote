package service

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"
	"unicode"
)

// This is THE date that Golang needs to determine custom formatting
const dateFormat string = "Mon, Jan 2, 2006"

// ListRepo represents the main interface to the in-mem ListItem store
type ListRepo interface {
	Add(line string, note *[]byte, idx int) error
	Update(line string, note *[]byte, idx int) (string, error)
	Delete(idx int) error
	MoveUp(idx int) (bool, error)
	MoveDown(idx int) (bool, error)
	ToggleVisibility(idx int) error
	Undo() error
	Redo() error
	Match(keys [][]rune, showHidden bool) ([]MatchItem, error)
	GetMatchPattern(sub []rune) (matchPattern, int)
}

// Wal manages the state of the WAL, via all update functions and replay functionality
type Wal struct {
	uuid            uuid
	log             *[]eventLog
	listItemTracker map[string]*ListItem

	// Legacy
	walPathPattern    string
	latestWalSchemaID uint16
}

func generateUUID() uuid {
	return uuid(rand.Uint32())
}

func NewWal(walPathPattern string) *Wal {
	return &Wal{
		uuid:              generateUUID(),
		walPathPattern:    walPathPattern,
		latestWalSchemaID: latestWalSchemaID,
		log:               &[]eventLog{},
		listItemTracker:   make(map[string]*ListItem),
	}
}

type listItemKey string

const listItemKeyPattern = "%v_%v"

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	Root             *ListItem
	NextID           uint64
	PendingDeletions []*ListItem
	rootPath         string
	eventLogger      *DbEventLogger
	wal              *Wal
	// TODO remove
	listItemMap    map[listItemKey]*ListItem
	matchListItems []*ListItem
}

// ListItem represents a single item in the returned list, based on the Match() input
type ListItem struct {
	Line        string
	Note        *[]byte
	IsHidden    bool
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
func NewDBListRepo(rootPath string, eventLogger *DbEventLogger, wal *Wal) *DBListRepo {
	return &DBListRepo{
		rootPath:    rootPath,
		eventLogger: eventLogger,
		wal:         wal,
		NextID:      1,
		listItemMap: make(map[listItemKey]*ListItem),
	}
}

func (w *Wal) addLog(e eventType, id uint64, childID uint64, newLine string, newNote *[]byte) (eventLog, error) {
	// Base next id off of previous. It's a bit brute-force, but will enforce uniqueness in a single WAL
	// which is our end goal here
	nextID := uint64(1)
	if len(*w.log) > 0 {
		nextID = (*w.log)[len(*w.log)-1].logID + 1
	}

	el := eventLog{
		logID:           nextID,
		uuid:            w.uuid,
		unixTime:        time.Now().Unix(),
		eventType:       e,
		listItemID:      id,
		childListItemID: childID,
		line:            newLine,
		note:            newNote,
	}

	// Append to log
	*w.log = append(*w.log, el)

	return el, nil
}

func (r *DBListRepo) callFunctionForEventLog(e eventLog) (*ListItem, error) {
	item := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.listItemID)]
	child := r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, e.childListItemID)]

	var err error
	switch e.eventType {
	case addEvent:
		item, err = add(r, e.line, e.note, child)
		item.id = e.listItemID
		r.wal.listItemTracker[fmt.Sprintf("%d:%d", e.uuid, item.id)] = item
	case deleteEvent:
		err = del(r, item)
	case updateEvent:
		item, err = update(r, e.line, e.note, item)
	case moveUpEvent:
		_, err = moveUp(r, item)
	case moveDownEvent:
		_, err = moveDown(r, item)
	case visibilityEvent:
		err = toggleVisibility(r, item)
	}
	return item, err
}

// Add adds a new LineItem with string, note and a position to insert the item into the matched list
func (r *DBListRepo) Add(line string, note *[]byte, idx int) error {
	// TODO put idx check and retrieval into single helper function
	if idx < 0 || idx > len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	var childID uint64
	if idx > 0 {
		childID = r.matchListItems[idx-1].id
	}
	el, err := r.wal.addLog(addEvent, r.NextID, childID, line, note)
	listItem, _ := r.callFunctionForEventLog(el)
	r.addUndoLog(addEvent, listItem, line, note)
	return err
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) Update(line string, note *[]byte, idx int) (string, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return "", errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	// Need to add to Undo/Redo log before mutating, because the addLog
	// function requires previous state
	r.addUndoLog(updateEvent, listItem, line, note)

	el, err := r.wal.addLog(updateEvent, listItem.id, 0, line, note)
	listItem, err = r.callFunctionForEventLog(el)
	return listItem.Line, err
}

// Delete will remove an existing ListItem
func (r *DBListRepo) Delete(idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	el, err := r.wal.addLog(deleteEvent, listItem.id, 0, "", nil)
	r.callFunctionForEventLog(el)
	r.addUndoLog(deleteEvent, listItem, "", nil)
	return err
}

// MoveUp will swop a ListItem with the ListItem directly above it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveUp(idx int) (bool, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return false, errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]
	oldChild := listItem.child

	el, err := r.wal.addLog(moveUpEvent, listItem.id, 0, "", nil)
	listItem, err = r.callFunctionForEventLog(el)
	moved := listItem.child != oldChild
	if moved {
		r.addUndoLog(moveUpEvent, listItem, "", nil)
	}
	return moved, err
}

// MoveDown will swop a ListItem with the ListItem directly below it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveDown(idx int) (bool, error) {
	if idx < 0 || idx >= len(r.matchListItems) {
		return false, errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]
	oldParent := listItem.parent

	el, err := r.wal.addLog(moveDownEvent, listItem.id, 0, "", nil)
	listItem, err = r.callFunctionForEventLog(el)
	moved := listItem.parent != oldParent
	if moved {
		r.addUndoLog(moveDownEvent, listItem, "", nil)
	}
	return moved, err
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(idx int) error {
	if idx < 0 || idx >= len(r.matchListItems) {
		return errors.New("ListItem idx out of bounds")
	}

	listItem := r.matchListItems[idx]

	r.addUndoLog(visibilityEvent, listItem, "", nil)
	el, err := r.wal.addLog(visibilityEvent, listItem.id, 0, "", nil)
	r.callFunctionForEventLog(el)
	return err
}

func (r *DBListRepo) Undo() error {
	if r.eventLogger.curIdx > 0 {
		// undo event log
		uel := r.eventLogger.log[r.eventLogger.curIdx]

		childListItemID := uint64(0)
		if uel.ptr.child != nil {
			childListItemID = uel.ptr.child.id
		}

		el, err := r.wal.addLog(oppositeEvent[uel.eventType], uel.ptr.id, childListItemID, uel.undoLine, uel.undoNote)
		_, err = r.callFunctionForEventLog(el)
		r.eventLogger.curIdx--
		return err
	}
	return nil
}

func (r *DBListRepo) Redo() error {
	// Redo needs to look forward +1 index when actioning events
	if r.eventLogger.curIdx < len(r.eventLogger.log)-1 {
		uel := r.eventLogger.log[r.eventLogger.curIdx+1]

		childListItemID := uint64(0)
		if uel.ptr.child != nil {
			childListItemID = uel.ptr.child.id
		}

		el, err := r.wal.addLog(uel.eventType, uel.ptr.id, childListItemID, uel.redoLine, uel.redoNote)
		_, err = r.callFunctionForEventLog(el)
		r.eventLogger.curIdx++
		return err
	}
	return nil
}

// Search functionality

func isSubString(sub string, full string) bool {
	if strings.Contains(strings.ToLower(full), strings.ToLower(sub)) {
		return true
	}
	return false
}

// Iterate through the full string, when you match the "head" of the sub rune slice,
// pop it and continue through. If you clear sub, return true. Searches in O(n)
func isFuzzyMatch(sub []rune, full string) bool {
	for _, c := range full {
		if unicode.ToLower(c) == unicode.ToLower(sub[0]) {
			_, sub = sub[0], sub[1:]
		}
		if len(sub) == 0 {
			return true
		}
	}
	return false
}

const (
	openOp  rune = '{'
	closeOp rune = '}'
)

type matchPattern int

const (
	fullMatchPattern matchPattern = iota
	inverseMatchPattern
	fuzzyMatchPattern
	noMatchPattern
)

// matchChars represents the number of characters at the start of the string
// which are attributed to the match pattern.
// This is used elsewhere to strip the characters where appropriate
var matchChars = map[matchPattern]int{
	fullMatchPattern:    1,
	inverseMatchPattern: 2,
	fuzzyMatchPattern:   0,
	noMatchPattern:      0,
}

// GetMatchPattern will return the matchPattern of a given string, if any, plus the number
// of chars that can be omitted to leave only the relevant text
func (r *DBListRepo) GetMatchPattern(sub []rune) (matchPattern, int) {
	if len(sub) == 0 {
		return noMatchPattern, 0
	}
	pattern := fuzzyMatchPattern
	if sub[0] == '#' {
		pattern = fullMatchPattern
		if len(sub) > 1 {
			// Inverse string match if a search group begins with `#!`
			if sub[1] == '!' {
				pattern = inverseMatchPattern
			}
		}
	}
	nChars, _ := matchChars[pattern]
	return pattern, nChars
}

func (r *DBListRepo) parseOperatorGroups(sub string) string {
	// Match the op against any known operator (e.g. date) and parse if applicable.
	// TODO for now, just match `d` or `D` for date, we'll expand in the future.
	now := time.Now()
	dateString := now.Format(dateFormat)
	sub = strings.ReplaceAll(sub, "{d}", dateString)
	return sub
}

// If a matching group starts with `#` do a substring match, otherwise do a fuzzy search
func isMatch(sub []rune, full string, pattern matchPattern) bool {
	if len(sub) == 0 {
		return true
	}
	switch pattern {
	case fullMatchPattern:
		return isSubString(string(sub), full)
	case inverseMatchPattern:
		return !isSubString(string(sub), full)
	case fuzzyMatchPattern:
		return isFuzzyMatch(sub, full)
	default:
		// Shouldn't reach here
		return false
	}
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
		group = []rune(r.parseOperatorGroups(string(group)))
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
