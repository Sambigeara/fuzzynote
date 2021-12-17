package service

import (
	"fmt"
	"log"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"mvdan.cc/xurls/v2"
)

const (
	dateFormat            = "Mon, Jan 02, 2006"
	emptySearchLinePrompt = "Search here..."
	searchGroupPrompt     = "TAB: Create new search group"
	newLinePrompt         = "Enter: Create new line"
	collabEmailPattern    = " @%s"
)

// ClientBase ...
type ClientBase struct {
	db                                    *DBListRepo
	Search                                [][]rune
	matches                               []ListItem
	CurItem                               *ListItem // The currently selected item
	Editor                                string
	W, H                                  int
	ReservedTopLines, ReservedBottomLines int
	CurX, CurY                            int // Cur "screen" index, not related to matched item lists
	VertOffset                            int // The index of the first displayed item in the match set
	HorizOffset                           int // The index of the first displayed char in the curItem
	ShowHidden                            bool
	SelectedItems                         map[int]ListItem
	copiedItem                            *ListItem
	HiddenMatchPrefix                     string // The common string that we want to truncate from each line
	//previousKey       InteractionEventType // Keep track of the previous keypress
	//footerMessage string // Because we refresh on an ongoing basis, this needs to be emitted each time we paint
}

// NewClientBase ...
func NewClientBase(db *DBListRepo, maxWidth int, maxHeight int) *ClientBase {
	return &ClientBase{
		db:                  db,
		W:                   maxWidth,
		H:                   maxHeight,
		SelectedItems:       make(map[int]ListItem),
		ReservedTopLines:    1,
		ReservedBottomLines: 2,
	}
}

// InteractionEventType ...
type InteractionEventType uint32

// InteractionEvent ...
type InteractionEvent struct {
	T InteractionEventType
	R []rune
}

const (
	KeyNull InteractionEventType = iota
	KeyEscape
	KeyEnter
	KeyDeleteItem
	KeyOpenNote
	KeyGotoStart
	KeyGotoEnd
	KeyVisibility
	KeyUndo
	KeyRedo
	KeyCopy
	KeyPaste
	KeyOpenURL
	KeyExport
	KeySelect
	KeyAddSearchGroup
	KeyBackspace
	KeyDelete
	KeyMoveItemUp
	KeyMoveItemDown
	KeyCursorDown
	KeyCursorUp
	KeyCursorRight
	KeyCursorLeft
	KeyRune

	SetText
)

// TODO duplicated in getHiddenLinePrefix function, figure out how to unify
func getLenHiddenMatchPrefix(line string, hiddenMatchPrefix string) int {
	l := 0
	if strings.HasPrefix(strings.ToLower(line), hiddenMatchPrefix) {
		l += len([]byte(hiddenMatchPrefix))
	}
	return l
}

func getMapIntersection(a map[string]struct{}, b map[string]struct{}) map[string]struct{} {
	intersect := make(map[string]struct{})
	for i := range a {
		if _, exists := b[i]; exists {
			intersect[i] = struct{}{}
		}
	}
	return intersect
}

func getCommonSearchPrefixAndFriends(selectedItems map[int]ListItem) [][]rune {
	searchGroups := [][]rune{}
	if len(selectedItems) == 0 {
		return searchGroups
	}

	var lines []string

	// We need to do a set intersection on the friends, as we only want to display
	// commonly shared emails
	// Therefore, we set the friends map to that of the first line, and then iterate
	// over the subsequent lines, taking the intersect of the resultant map as we go
	friends := make(map[string]struct{})
	for _, item := range selectedItems {
		for _, f := range item.Friends() {
			friends[f] = struct{}{}
		}
		break // We only need one item from the iterable map to instantiate the set
	}
	for _, item := range selectedItems {
		lines = append(lines, strings.TrimSpace(item.Line()))
		lineFriends := make(map[string]struct{})
		for _, f := range item.Friends() {
			lineFriends[f] = struct{}{}
		}
		friends = getMapIntersection(friends, lineFriends)
	}

	var prefix string
	// Only take longest common prefix on line if there's more than one line
	if len(lines) > 1 {
		prefix = strings.TrimSpace(longestCommonPrefix(lines))
	}

	if len(prefix) > 0 {
		searchGroups = append(searchGroups, []rune(fmt.Sprintf("=%s", prefix)))
	}

	for f := range friends {
		searchGroups = append(searchGroups, []rune(fmt.Sprintf("=%s", f)))
	}

	return searchGroups
}

func longestCommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}
	prefix := strs[0]
	for i := 1; i < len(strs); i++ {
		for !strings.HasPrefix(strs[i], prefix) {
			prefix = prefix[0 : len(prefix)-1]
			if len(prefix) == 0 {
				return ""
			}
		}
	}
	return prefix
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (t *ClientBase) getLenSearchBox() int {
	// Add up max potential position based on number of runes in groups, and separators between
	lenSearchBox := max(0, len(t.Search)-1) // Account for spaces between search groups
	for _, g := range t.Search {
		lenSearchBox += len(g)
	}
	return lenSearchBox
}

func MatchFirstURL(line string) string {
	// Attempt to match any urls in the line.
	// Try "Strict" match first. This only matches if scheme is included.
	rxStrict := xurls.Strict()
	var match string
	if match = rxStrict.FindString(line); match == "" {
		// Otherwise, if we succeed on a "Relaxed" match, we can infer
		// that the scheme wasn't present. The darwin/macOS `open` command
		// expects full URLs with scheme, so do a brute force prepend of
		// `http://` to see if that works.
		rxRelaxed := xurls.Relaxed()
		if match = rxRelaxed.FindString(line); match != "" {
			match = fmt.Sprintf("http://%s", match)
		}
	}
	return match
}

// open opens the specified URL in the default browser of the user.
// https://stackoverflow.com/a/39324149
func openURL(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...).Start()
}

// GetSearchGroupIdxAndOffset returns the group index and offset within that group, respectively.
// This might have unpredictable results if called on non-search lines (e.g. when CurY != 0)
func (t *ClientBase) GetSearchGroupIdxAndOffset() (int, int) {
	// Get search group to operate on, and the char within that
	grpIdx, start := 0, 0
	// This is a public API, so it's worth covering false cases when this is called on zero/empty search
	// groups
	if len(t.Search) == 0 {
		return 0, 0
	}
	end := len(t.Search[grpIdx])
	for end < t.CurX {
		grpIdx++
		start = end + 1 // `1` accounts for the visual separator between groups
		end = start
		if grpIdx < len(t.Search) {
			end += len(t.Search[grpIdx])
		}
	}
	charOffset := t.CurX - start
	return grpIdx, charOffset
}

func insertCharInPlace(line []rune, offset int, newChars []rune) []rune {
	// TODO this is an ugly catch to prevent an edge condition that sometimes occurs.
	// Better to just ignore the op rather than crashing the program...
	// (until I find the solution I mean)
	if len(line) < offset {
		return line
	}
	lenNewChars := len(newChars)
	line = append(line, newChars...)
	copy(line[offset+lenNewChars:], line[offset:])
	copy(line[offset:], newChars)
	return line
}

func parseOperatorGroups(sub string) string {
	// Match the op against any known operator (e.g. date) and parse if applicable.
	// TODO for now, just match `d` or `D` for date, we'll expand in the future.
	now := time.Now()
	dateString := now.Format(dateFormat)
	sub = strings.ReplaceAll(sub, "{d}", dateString)
	return sub
}

func getHiddenLinePrefix(keys [][]rune) string {
	// Only apply the trunaction on "closed" search groups (e.g. when the user has tabbed to
	// the next one).
	if len(keys) == 0 || (len(keys) == 1 && len(keys[0]) == 0) {
		return ""
	}

	// Only operate on the first key
	key := keys[0]
	_, nChars := GetMatchPattern(key) // TODO can be private function now in same service
	trimmedKey := string(key[nChars:])

	shortenedPrefix := fmt.Sprintf("%s ", strings.TrimSpace(strings.ToLower(trimmedKey)))

	return shortenedPrefix
}

// TrimPrefix ...
func (t *ClientBase) TrimPrefix(line string) string {
	// Truncate the full search string from any lines matching the entire thing,
	// ignoring search operators
	// Op needs to be case-insensitive, but must not mutate underlying line

	if t.HiddenMatchPrefix == "" {
		return line
	}

	// We explicitly avoid using `strings.TrimPrefix(...)` here because the lines are case-sensitive
	if strings.HasPrefix(strings.ToLower(line), t.HiddenMatchPrefix) {
		line = string([]rune(line)[len([]byte(t.HiddenMatchPrefix)):])
		// If we strip the match prefix, and there is a space remaining, trim that too
		return strings.TrimPrefix(line, " ")
	}
	return line
}

func (t *ClientBase) GetUnsearchedFriends(friends []string) []string {
	removedSearchFriends := []string{}
	for _, f := range friends {
		isInSearch := false
		for _, s := range t.Search {
			if pattern, nChars := GetMatchPattern(s); pattern == FullMatchPattern {
				s = s[nChars:]
			} else if pattern == InverseMatchPattern {
				break
			}
			if f == string(s) {
				isInSearch = true
				break
			}
		}
		if !isInSearch {
			removedSearchFriends = append(removedSearchFriends, f)
		}
	}
	return removedSearchFriends
}

// TODO rename "t"
func (t *ClientBase) HandleInteraction(ev InteractionEvent, limit int) ([]ListItem, bool, error) {
	posDiff := []int{0, 0} // x and y mutations to apply after db data mutations

	// itemKey represents the unique identifying key for the ListItem. We set it explicitly only
	// when creating new ListItems via the Add interface, so we can tell the backend what our
	// current item is when asking for Matches (and adjusting for cursor offsets due to live collab)
	// If itemKey is empty by the time we reach the Match call, we offset any movement against our
	// existing match set (e.g. for Move*) and then default to matchedItem.Key()
	itemKey := ""

	// relativeY accounts for any hidden lines at the top, which is required for match indexing
	relativeY := t.CurY + t.VertOffset

	// offsetX represents the position in the underying curItem.Line
	// Only apply the prefix offset if the line starts with the prefix, other lines will
	// match but not have the prefix truncated
	offsetX := t.HorizOffset + t.CurX
	lenHiddenMatchPrefix := 0
	if t.CurItem != nil {
		lenHiddenMatchPrefix = getLenHiddenMatchPrefix(t.CurItem.Line(), t.HiddenMatchPrefix)
		offsetX += lenHiddenMatchPrefix
	}
	var err error
	switch ev.T {
	case KeyEscape:
		if len(t.SelectedItems) > 0 {
			t.SelectedItems = make(map[int]ListItem)
		} else {
			t.VertOffset = 0
			relativeY = 0
		}
	case KeyEnter:
		if len(t.SelectedItems) > 0 {
			// Add common search prefix to search groups
			if newSearch := getCommonSearchPrefixAndFriends(t.SelectedItems); len(newSearch) > 0 {
				t.Search = newSearch
			}
			t.SelectedItems = make(map[int]ListItem)
			t.CurY = 0
			if len(t.Search) > 0 {
				posDiff[0] += len(t.Search[0])
			}
		} else {
			// Add a new item below current cursor position
			// This will insert the contents of the current search string (omitting search args like `=`)
			var err error
			if relativeY == t.ReservedTopLines-1 {
				if len(t.Search) > 0 {
					posDiff[0] -= len([]byte(strings.TrimSpace(string(t.Search[0])))) + 1
				}
			}
			newString := GetNewLinePrefix(t.Search)
			// If the user is logged in, we automatically prepend the client email to each line (this is used
			// for collaboration purposes), UNLESS the email is already present in the new line prefix
			// TODO do the email-in check better
			if t.db.email != "" {
				ownerPattern := fmt.Sprintf(collabEmailPattern, t.db.email)
				if !strings.Contains(newString, ownerPattern) {
					newString = fmt.Sprintf("%s%s", newString, ownerPattern)
					//posDiff[0] += len([]byte(ownerPattern))
				}
			}
			itemKey, err = t.db.Add(newString, nil, relativeY)
			if err != nil {
				log.Fatal(err)
			}
			posDiff[1]++
		}
	case KeyDeleteItem:
		if relativeY == t.ReservedTopLines-1 {
			// Only remove the current search group
			grpIdx, _ := t.GetSearchGroupIdxAndOffset()
			if len(t.Search) == 1 {
				t.Search = [][]rune{[]rune{}}
			} else if grpIdx == 0 {
				t.Search = t.Search[1:]
			} else {
				t.Search = append(t.Search[:grpIdx], t.Search[grpIdx+1:]...)
			}
		} else {
			// Copy into buffer in case we're moving it elsewhere
			t.copiedItem = t.CurItem
			if relativeY-1 != len(t.matches)-1 {
				// TODO make `==` and reorder
				// Default behaviour on delete is to return and set position to the child item.
				// We don't want to do that here, so ignore the itemKey return from Delete, and
				// increment the Y position.
				// Because we increment, the key passed to Match below will exist and therefore
				// will resolve properly.
				_, err = t.db.Delete(relativeY - 1)
				if err != nil {
					log.Fatal(err)
				}
				posDiff[1]++
			} else {
				// APART from when calling CtrlD on the bottom item, in which case use the default
				// behaviour and target the child for new positioning
				itemKey, err = t.db.Delete(relativeY - 1)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
		t.HorizOffset = 0
	//case KeyOpenNote:
	//    if relativeY != 0 {
	//        if err := t.S.Suspend(); err == nil {
	//            err = t.openEditorSession()
	//            if err != nil {
	//                log.Fatal(err)
	//            }
	//            if err := t.S.Resume(); err != nil {
	//                panic("failed to resume: " + err.Error())
	//            }
	//        }
	//    }
	case KeyGotoStart:
		// Go to beginning of line
		// TODO decouple cursor mutation from key handling
		t.CurX = 0
		t.HorizOffset = 0
	case KeyGotoEnd:
		// Go to end of line
		if relativeY == t.ReservedTopLines-1 {
			t.CurX = t.getLenSearchBox()
		} else {
			// TODO
			t.CurX = len([]rune(t.CurItem.Line()))
		}
		t.HorizOffset = t.CurX - t.W
	case KeyVisibility:
		// Toggle hidden item visibility
		if relativeY == t.ReservedTopLines-1 {
			t.ShowHidden = !t.ShowHidden
		} else {
			// Default returned itemKey behaviour on ToggleVisibility is one of the following:
			// - on "show", it will return itself
			// - on "hide", it will look for matchParent first, matchChild second
			// This is expected behaviour on the default "hide hidden" view, but when we're showing
			// and operating on all items (including hidden), we don't need to change the itemKey
			newItemKey, err := t.db.ToggleVisibility(relativeY - 1)
			if err != nil {
				log.Fatal(err)
			}
			if !t.ShowHidden {
				itemKey = newItemKey
			}
		}
	case KeyUndo:
		itemKey, err = t.db.Undo()
		if err != nil {
			log.Fatal(err)
		}
	case KeyRedo:
		itemKey, err = t.db.Redo()
		if err != nil {
			log.Fatal(err)
		}
	case KeyCopy:
		// Copy functionality
		if relativeY != t.ReservedTopLines-1 {
			t.copiedItem = t.CurItem
		}
	case KeyOpenURL:
		if relativeY != t.ReservedTopLines-1 {
			if url := MatchFirstURL(t.CurItem.Line()); url != "" {
				openURL(url)
			}
		}
	case KeyExport:
		t.db.ExportToPlainText(t.Search, t.ShowHidden)
	case KeyPaste:
		// Paste functionality
		if t.copiedItem != nil {
			itemKey, err = t.db.Add(t.copiedItem.rawLine, nil, relativeY)
			if err != nil {
				log.Fatal(err)
			}
			posDiff[1]++
		}
	case KeySelect:
		if relativeY != t.ReservedTopLines-1 {
			// If exists, clear, otherwise set
			if _, ok := t.SelectedItems[relativeY-t.ReservedTopLines]; ok {
				delete(t.SelectedItems, relativeY-t.ReservedTopLines)
			} else {
				t.SelectedItems[relativeY-t.ReservedTopLines] = t.matches[relativeY-t.ReservedTopLines]
			}
		}
	case KeyAddSearchGroup:
		if relativeY == t.ReservedTopLines-1 {
			// If no search groups exist, rely on separate new char insertion elsewhere
			if len(t.Search) > 0 {
				// The location of the cursor will determine where the search group is added
				// If `Tabbing` in the middle of the search group, we need to split the group into two
				// The character immediately after the current position will represent the first
				// character in the new (right most) search group
				grpIdx, charOffset := t.GetSearchGroupIdxAndOffset()
				currentGroup := t.Search[grpIdx]
				newLeft, newRight := currentGroup[:charOffset], currentGroup[charOffset:]
				t.Search = append(t.Search, []rune{})
				copy(t.Search[grpIdx+1:], t.Search[grpIdx:])
				t.Search[grpIdx] = newLeft
				t.Search[grpIdx+1] = newRight
			}
			posDiff[0]++
		}
	case KeyBackspace:
		if relativeY == t.ReservedTopLines-1 {
			if len(t.Search) > 0 {
				grpIdx, charOffset := t.GetSearchGroupIdxAndOffset()
				newGroup := []rune(t.Search[grpIdx])

				// If charOffset == 0 we are acting on the previous separator
				if charOffset == 0 {
					// If we are operating on a middle (or initial) separator, we need to merge
					// previous and next search groups before cleaning up the search group
					if grpIdx > 0 {
						newGroup = append(t.Search[grpIdx-1], t.Search[grpIdx]...)
						t.Search[grpIdx-1] = newGroup
						t.Search = append(t.Search[:grpIdx], t.Search[grpIdx+1:]...)
					}
				} else {
					newGroup = append(newGroup[:charOffset-1], newGroup[charOffset:]...)
					t.Search[grpIdx] = newGroup
				}
				posDiff[0]--
			}
		} else {
			// If cursor in 0 position and current line is empty, delete current line and go
			// to end of previous line (if present)
			newLine := []rune(t.CurItem.rawLine)
			if t.HorizOffset+t.CurX > 0 && len(t.CurItem.Line()) > 0 {
				newLine = append(newLine[:offsetX-1], newLine[offsetX:]...)
				err = t.db.Update(string(newLine), nil, relativeY-t.ReservedTopLines)
				if err != nil {
					log.Fatal(err)
				}
				posDiff[0]--
			} else if (offsetX-lenHiddenMatchPrefix) == 0 && (len(t.CurItem.Line())-lenHiddenMatchPrefix) == 0 {
				itemKey, err = t.db.Delete(relativeY - 1)
				if err != nil {
					log.Fatal(err)
				}
				// Move up a cursor position
				posDiff[1]--
				if relativeY > t.ReservedTopLines {
					// TODO setting to the max width isn't completely robust as other
					// decrements will affect, but it's good enough for now as the cursor
					// repositioning logic will take care of over-increments
					posDiff[0] += t.W
				}
			}
		}
	case KeyDelete:
		// TODO this is very similar to the Backspace logic above, refactor to avoid duplication
		if relativeY == t.ReservedTopLines-1 {
			if len(t.Search) > 0 {
				grpIdx, charOffset := t.GetSearchGroupIdxAndOffset()
				newGroup := []rune(t.Search[grpIdx])

				// If charOffset == len(t.Search[grpIdx]) we need to merge with the next group (if present)
				if charOffset == len(t.Search[grpIdx]) {
					if grpIdx < len(t.Search)-1 {
						newGroup = append(t.Search[grpIdx], t.Search[grpIdx+1]...)
						t.Search[grpIdx] = newGroup
						t.Search = append(t.Search[:grpIdx+1], t.Search[grpIdx+2:]...)
					}
				} else {
					newGroup = append(newGroup[:charOffset], newGroup[charOffset+1:]...)
					t.Search[grpIdx] = newGroup
				}
			}
		} else {
			// If cursor in 0 position and current line is empty, delete current line and go
			// to end of previous line (if present)
			newLine := []rune(t.CurItem.rawLine)
			if len(t.CurItem.Line()) > 0 && t.HorizOffset+t.CurX+lenHiddenMatchPrefix < len(t.CurItem.Line()) {
				newLine = append(newLine[:offsetX], newLine[offsetX+1:]...)
				err = t.db.Update(string(newLine), nil, relativeY-t.ReservedTopLines)
				if err != nil {
					log.Fatal(err)
				}
			} else if (offsetX-lenHiddenMatchPrefix) == 0 && (len(t.CurItem.Line())-lenHiddenMatchPrefix) == 0 {
				itemKey, err = t.db.Delete(relativeY - 1)
				if err != nil {
					log.Fatal(err)
				}
				// Move up a cursor position
				posDiff[1]--
				if relativeY > t.ReservedTopLines {
					// TODO setting to the max width isn't completely robust as other
					// decrements will affect, but it's good enough for now as the cursor
					// repositioning logic will take care of over-increments
					posDiff[0] += t.W
				}
			}
		}
	case KeyMoveItemUp:
		if relativeY > t.ReservedTopLines-1 {
			// Move the current item up and follow with cursor
			if err = t.db.MoveUp(relativeY - 1); err != nil {
				log.Fatal(err)
			}
			// Set itemKey to current to ensure the cursor follows it
			itemKey = t.CurItem.Key()
		}
	case KeyMoveItemDown:
		if relativeY > t.ReservedTopLines-1 {
			// Move the current item down and follow with cursor
			if err = t.db.MoveDown(relativeY - 1); err != nil {
				log.Fatal(err)
			}
			// Set itemKey to current to ensure the cursor follows it
			itemKey = t.CurItem.Key()
		}
	case KeyCursorDown:
		posDiff[1]++
	case KeyCursorUp:
		posDiff[1]--
	case KeyCursorRight:
		posDiff[0]++
	case KeyCursorLeft:
		posDiff[0]--
	case KeyRune:
		if relativeY == t.ReservedTopLines-1 {
			if len(t.Search) > 0 {
				grpIdx, charOffset := t.GetSearchGroupIdxAndOffset()
				newGroup := make([]rune, len(t.Search[grpIdx]))
				copy(newGroup, t.Search[grpIdx])

				// We want to insert a char into the current search group then update in place
				newGroup = insertCharInPlace(newGroup, charOffset, ev.R)
				oldLen := len(string(newGroup))
				parsedGroup := parseOperatorGroups(string(newGroup))
				t.Search[grpIdx] = []rune(parsedGroup)
				posDiff[0] += len(parsedGroup) - oldLen
			} else {
				var newTerm []rune
				newTerm = append(newTerm, ev.R...)
				t.Search = append(t.Search, newTerm)
			}
		} else {
			newLine := []rune(t.CurItem.rawLine)
			// Insert characters at position
			if len(newLine) == 0 {
				newLine = append(newLine, ev.R...)
			} else {
				newLine = insertCharInPlace(newLine, offsetX, ev.R)
			}
			oldLen := len(newLine)
			parsedNewLine := parseOperatorGroups(string(newLine))
			err = t.db.Update(parsedNewLine, nil, relativeY-t.ReservedTopLines)
			if err != nil {
				log.Fatal(err)
			}
			posDiff[0] += len([]rune(parsedNewLine)) - oldLen
		}
		posDiff[0] += len(ev.R)
		//t.footerMessage = ""
	case SetText:
		if relativeY == t.ReservedTopLines-1 {
			if len(t.Search) > 0 {
				grpIdx, _ := t.GetSearchGroupIdxAndOffset()
				oldLen := len(string(t.Search[grpIdx]))
				parsedGroup := parseOperatorGroups(string(ev.R))
				t.Search[grpIdx] = []rune(parsedGroup)
				posDiff[0] += len(parsedGroup) - oldLen
			} else {
				t.Search = append(t.Search, ev.R)
			}
		} else {
			oldLen := len([]rune(t.CurItem.rawLine))
			// Add in the hidden match prefix
			parsedNewLine := parseOperatorGroups(fmt.Sprintf("%s%s", t.HiddenMatchPrefix, string(ev.R)))
			err = t.db.Update(parsedNewLine, nil, relativeY-t.ReservedTopLines)
			if err != nil {
				log.Fatal(err)
			}
			posDiff[0] += len([]rune(parsedNewLine)) - oldLen
		}
	}
	//t.previousKey = ev.T

	t.HiddenMatchPrefix = getHiddenLinePrefix(t.Search)

	matchIdx := relativeY - t.ReservedTopLines
	// Adjust with any explicit moves
	matchIdx = Min(matchIdx+posDiff[1], len(t.matches)-1)
	// Set itemKey to the client's current curItem
	if itemKey == "" && matchIdx >= 0 && matchIdx < len(t.matches) {
		itemKey = t.matches[matchIdx].Key()
	}

	// Handle any offsets that occurred due to other collaborators interacting with the same list
	// at the same time
	t.matches, matchIdx, err = t.db.Match(t.Search, t.ShowHidden, itemKey, 0, limit)

	windowSize := t.H - t.ReservedTopLines - t.ReservedBottomLines

	t.CurY = 0
	if matchIdx >= 0 {
		if matchIdx < t.VertOffset {
			t.VertOffset = matchIdx
			t.CurY = t.ReservedTopLines
		} else if matchIdx >= t.VertOffset+windowSize {
			t.VertOffset = matchIdx - windowSize
			t.CurY = t.ReservedTopLines + windowSize
		} else {
			t.CurY = matchIdx - t.VertOffset + t.ReservedTopLines
		}
	}

	isSearchLine := t.CurY <= t.ReservedTopLines-1 // `- 1` for 0 idx

	// Set curItem before establishing max X position based on the len of the curItem line (to avoid
	// nonexistent array indexes). If on search line, just set to nil
	if isSearchLine || len(t.matches) == 0 {
		t.CurItem = nil
	} else {
		t.CurItem = &t.matches[matchIdx]
	}

	// Then refresh the X position based on vertical position and curItem

	// If we've moved up or down, clear the horizontal offset
	if posDiff[1] > 0 || posDiff[1] < 0 {
		t.HorizOffset = 0
	}

	// Some logic above does forceful operations to ensure that the cursor is moved to MINIMUM beginning of line
	// Therefore ensure we do not go < 0
	t.HorizOffset = max(0, t.HorizOffset)

	newXIdx := t.CurX + posDiff[0]
	if isSearchLine {
		newXIdx = max(0, newXIdx) // Prevent index < 0
		t.CurX = Min(newXIdx, t.getLenSearchBox())
	} else {
		// Deal with horizontal offset if applicable
		if newXIdx < 0 {
			if t.HorizOffset > 0 {
				t.HorizOffset--
			}
			t.CurX = 0 // Prevent index < 0
		} else {
			if newXIdx > t.W-1 && t.HorizOffset+t.W-1 < len(t.CurItem.Line()) {
				t.HorizOffset++
			}
			// We need to recalc lenHiddenMatchPrefix here to cover the case when we arrow down from
			// the search line to the top line, when there's a hidden prefix (otherwise there is a delay
			// before the cursor sets to the end of the line and we're at risk of an index error).
			lenHiddenMatchPrefix = getLenHiddenMatchPrefix(t.CurItem.Line(), t.HiddenMatchPrefix)
			newXIdx = Min(newXIdx, len([]rune(t.CurItem.Line()))-t.HorizOffset-lenHiddenMatchPrefix) // Prevent going out of range of the line
			t.CurX = Min(newXIdx, t.W-1)                                                             // Prevent going out of range of the page
		}
	}

	return t.matches, true, nil
}
