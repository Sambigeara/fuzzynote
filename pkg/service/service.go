package service

import (
	"strings"
	"time"
	"unicode"
)

// This is THE date that Golang needs to determine custom formatting
const dateFormat string = "Mon, Jan 2, 2006"

// ListRepo represents the main interface to the in-mem ListItem store
type ListRepo interface {
	Add(line string, note *[]byte, childItem *ListItem, newItem *ListItem) error
	Update(line string, note *[]byte, listItem *ListItem) error
	Delete(listItem *ListItem) error
	MoveUp(listItem *ListItem) (bool, error)
	MoveDown(listItem *ListItem) (bool, error)
	ToggleVisibility(listItem *ListItem) error
	Undo() error
	Redo() error
	Match(keys [][]rune, active *ListItem, showHidden bool) ([]*ListItem, error)
	GetMatchPattern(sub []rune) (matchPattern, int)
}

// DBListRepo is an implementation of the ListRepo interface
type DBListRepo struct {
	Root             *ListItem
	NextID           uint64
	PendingDeletions []*ListItem
	eventLogger      *DbEventLogger
	walLogger        *WalEventLogger
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
func NewDBListRepo(root *ListItem, nextID uint64, eventLogger *DbEventLogger, walLogger *WalEventLogger) *DBListRepo {
	return &DBListRepo{
		Root:        root,
		eventLogger: eventLogger,
		walLogger:   walLogger,
		NextID:      nextID,
	}
}

func (r *DBListRepo) add(line string, note *[]byte, childItem *ListItem, newItem *ListItem) (*ListItem, error) {
	if note == nil {
		note = &[]byte{}
	}
	if newItem == nil {
		newItem = &ListItem{
			Line:  line,
			id:    r.NextID,
			child: childItem,
			Note:  note,
		}
	}
	r.NextID++

	// If `child` is nil, it's the first item in the list so set as root and return
	if childItem == nil {
		oldRoot := r.Root
		r.Root = newItem
		if oldRoot != nil {
			newItem.parent = oldRoot
			oldRoot.child = newItem
		}
		return newItem, nil
	}

	if childItem.parent != nil {
		childItem.parent.child = newItem
		newItem.parent = childItem.parent
	}
	childItem.parent = newItem

	return newItem, nil
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) update(line string, note *[]byte, listItem *ListItem) error {
	line = r.parseOperatorGroups(line)
	listItem.Line = line
	listItem.Note = note
	return nil
}

func (r *DBListRepo) del(item *ListItem) error {
	if item.child != nil {
		item.child.parent = item.parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		r.Root = item.parent
	}

	if item.parent != nil {
		item.parent.child = item.child
	}

	r.PendingDeletions = append(r.PendingDeletions, item)

	return nil
}

func (r *DBListRepo) moveItem(item *ListItem, newChild *ListItem, newParent *ListItem) error {
	// Close off gap from source location (for whole dataset)
	if item.child != nil {
		item.child.parent = item.parent
	}
	if item.parent != nil {
		item.parent.child = item.child
	}

	// Insert item into new position based on Matched pointers
	item.child = newChild
	item.parent = newParent

	// Update pointers at target location
	if newParent != nil {
		newParent.child = item
	}
	if newChild != nil {
		newChild.parent = item
	}

	// Update root if required
	for r.Root.child != nil {
		r.Root = r.Root.child
	}
	return nil
}

func (r *DBListRepo) moveUp(item *ListItem) (bool, error) {
	targetItem := item.matchChild
	if targetItem == nil {
		return false, nil
	}

	newChild := targetItem.child
	newParent := targetItem
	err := r.moveItem(item, newChild, newParent)
	return true, err
}

func (r *DBListRepo) moveDown(item *ListItem) (bool, error) {
	targetItem := item.matchParent
	if targetItem == nil {
		return false, nil
	}

	newChild := targetItem
	newParent := targetItem.parent
	err := r.moveItem(item, newChild, newParent)
	return true, err
}

func (r *DBListRepo) toggleVisibility(item *ListItem) error {
	item.IsHidden = !item.IsHidden
	return nil
}

// Add adds a new LineItem with string, note and a pointer to the child LineItem for positioning
func (r *DBListRepo) Add(line string, note *[]byte, childItem *ListItem, newItem *ListItem) error {
	newItem, err := r.add(line, note, childItem, newItem)
	r.eventLogger.addLog(addEvent, newItem, "", nil)
	r.walLogger.addLog(addEvent, newItem, "", nil)
	return err
}

// Update will update the line or note of an existing ListItem
func (r *DBListRepo) Update(line string, note *[]byte, listItem *ListItem) error {
	r.eventLogger.addLog(updateEvent, listItem, line, note)
	r.walLogger.addLog(updateEvent, listItem, line, note)
	return r.update(line, note, listItem)
}

// Delete will remove an existing ListItem
func (r *DBListRepo) Delete(item *ListItem) error {
	r.eventLogger.addLog(deleteEvent, item, "", nil)
	r.walLogger.addLog(deleteEvent, item, "", nil)
	return r.del(item)
}

// MoveUp will swop a ListItem with the ListItem directly above it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveUp(item *ListItem) (bool, error) {
	moved, err := r.moveUp(item)
	if moved {
		r.eventLogger.addLog(moveUpEvent, item, "", nil)
		r.walLogger.addLog(moveUpEvent, item, "", nil)
	}
	return moved, err
}

// MoveDown will swop a ListItem with the ListItem directly below it, taking visibility and
// current matches into account.
func (r *DBListRepo) MoveDown(item *ListItem) (bool, error) {
	moved, err := r.moveDown(item)
	if moved {
		r.eventLogger.addLog(moveDownEvent, item, "", nil)
		r.walLogger.addLog(moveDownEvent, item, "", nil)
	}
	return moved, err
}

// ToggleVisibility will toggle an item to be visible or invisible
func (r *DBListRepo) ToggleVisibility(item *ListItem) error {
	r.eventLogger.addLog(visibilityEvent, item, "", nil)
	r.walLogger.addLog(visibilityEvent, item, "", nil)
	return r.toggleVisibility(item)
}

func (r *DBListRepo) Undo() error {
	if r.eventLogger.curIdx > 0 {
		eventLog := r.eventLogger.log[r.eventLogger.curIdx]
		// callFunctionForEventLog needs to call the appropriate function with the
		// necessary parameters to reverse the operation
		opEv := oppositeEvent[eventLog.eventType]
		err := r.callFunctionForEventLog(opEv, eventLog.ptr, eventLog.undoLine, eventLog.undoNote)
		r.eventLogger.curIdx--
		return err
	}
	return nil
}

func (r *DBListRepo) Redo() error {
	// Redo needs to look forward +1 index when actioning events
	if r.eventLogger.curIdx < len(r.eventLogger.log)-1 {
		eventLog := r.eventLogger.log[r.eventLogger.curIdx+1]
		err := r.callFunctionForEventLog(eventLog.eventType, eventLog.ptr, eventLog.redoLine, eventLog.redoNote)
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

// Match takes a set of search groups and applies each to all ListItems, returning those that
// fulfil all rules.
func (r *DBListRepo) Match(keys [][]rune, active *ListItem, showHidden bool) ([]*ListItem, error) {
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

	res := []*ListItem{}

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
				if len(cur.Line) == 0 || cur == active {
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
				res = append(res, cur)

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
