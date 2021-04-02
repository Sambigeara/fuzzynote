package client

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/atotto/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/gdamore/tcell/v2/encoding"
	"github.com/mattn/go-runewidth"
	"mvdan.cc/xurls/v2"

	"fuzzy-note/pkg/service"
)

const (
	dateFormat                            = "Mon, Jan 02, 2006"
	reservedTopLines, reservedBottomLines = 1, 1
	reservedEndChars                      = 1
	emptySearchLinePrompt                 = "Search here..."
	searchGroupPrompt                     = "TAB: Create new search group"
	newLinePrompt                         = "Enter: Create new line"
)

type Terminal struct {
	db                service.ListRepo
	search            [][]rune
	matches           []service.ListItem
	curItem           *service.ListItem // The currently selected item
	S                 tcell.Screen
	style             tcell.Style
	promptStyle       tcell.Style
	Editor            string
	w, h              int
	curX, curY        int // Cur "screen" index, not related to matched item lists
	vertOffset        int // The index of the first displayed item in the match set
	horizOffset       int // The index of the first displayed char in the curItem
	showHidden        bool
	selectedItems     map[int]string // struct{} is more space efficient than bool
	copiedItem        *service.ListItem
	hiddenMatchPrefix string    // The common string that we want to truncate from each line
	previousKey       tcell.Key // Keep track of the previous keypress
	footerMessage     string    // Because we refresh on an ongoing basis, this needs to be emitted each time we paint
}

func NewTerm(db service.ListRepo, colour string, editor string) *Terminal {
	encoding.Register()

	var defStyle, promptStyle tcell.Style
	if colour == "light" {
		defStyle = tcell.StyleDefault.
			Background(tcell.ColorWhite).
			Foreground(tcell.ColorBlack)
		promptStyle = tcell.StyleDefault.
			Background(tcell.ColorWhite).
			Foreground(tcell.ColorGray)
	} else {
		defStyle = tcell.StyleDefault.
			Background(tcell.ColorBlack).
			Foreground(tcell.ColorWhite)
		promptStyle = tcell.StyleDefault.
			Background(tcell.ColorBlack).
			Foreground(tcell.ColorGray)
	}

	s := newInstantiatedScreen(defStyle)

	showHidden := false

	matches, err := db.Match([][]rune{}, showHidden, "")
	if err != nil {
		log.Fatal(err)
	}

	w, h := s.Size()
	t := Terminal{
		db:            db,
		S:             s,
		style:         defStyle,
		promptStyle:   promptStyle,
		Editor:        editor,
		w:             w,
		h:             h,
		showHidden:    showHidden,
		selectedItems: make(map[int]string),
		matches:       matches,
	}
	t.paint(t.matches, false)
	return &t
}

func min(a, b int) int {
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

func parseOperatorGroups(sub string) string {
	// Match the op against any known operator (e.g. date) and parse if applicable.
	// TODO for now, just match `d` or `D` for date, we'll expand in the future.
	now := time.Now()
	dateString := now.Format(dateFormat)
	sub = strings.ReplaceAll(sub, "{d}", dateString)
	return sub
}

func emitStr(s tcell.Screen, x, y int, style tcell.Style, str string) {
	for _, c := range str {
		var comb []rune
		w := runewidth.RuneWidth(c)
		if w == 0 {
			comb = []rune{c}
			c = ' '
			w = 1
		}
		s.SetContent(x, y, c, comb, style)
		x += w
	}
}

func newInstantiatedScreen(style tcell.Style) tcell.Screen {
	s, e := tcell.NewScreen()
	if e != nil {
		fmt.Fprintf(os.Stderr, "%v\n", e)
		os.Exit(1)
	}
	if e := s.Init(); e != nil {
		fmt.Fprintf(os.Stderr, "%v\n", e)
		os.Exit(1)
	}
	s.SetStyle(style)
	//s.EnableMouse()
	s.Clear()
	return s
}

func (t *Terminal) openEditorSession() error {
	// Write text to temp file
	tmpfile, err := ioutil.TempFile("", "fzn_buffer")
	if err != nil {
		log.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	var note []byte
	if t.curItem.Note != nil {
		note = *t.curItem.Note
	}
	if _, err := tmpfile.Write(note); err != nil {
		log.Fatal(err)
		return err
	}

	//https://stackoverflow.com/questions/21513321/how-to-start-vim-from-go
	cmd := exec.Command(t.Editor, tmpfile.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	if err != nil {
		// For now, show a warning and return
		// TODO make more robust
		t.footerMessage = fmt.Sprintf("Unable to open Note using editor setting : \"%s\"", t.Editor)
	}

	// Read back from the temp file, and return to the write function
	newDat, err := ioutil.ReadFile(tmpfile.Name())
	if err != nil {
		log.Fatal(err)
		return nil
	}

	err = t.db.Update("", &newDat, t.curY-reservedTopLines)
	if err != nil {
		log.Fatal(err)
	}

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (t *Terminal) buildSearchBox(s tcell.Screen) {
	// If no search items at all, display emptySearchLinePrompt and return
	if len(t.search) == 0 || len(t.search) == 1 && len(t.search[0]) == 0 {
		emitStr(s, 0, 0, t.promptStyle, emptySearchLinePrompt)
	}

	searchStyle := tcell.StyleDefault.
		Foreground(tcell.ColorWhite).Background(tcell.ColorGrey)

	var pos, l int
	for _, key := range t.search {
		emitStr(s, pos, 0, searchStyle, string(key))
		l = len(key)
		pos = pos + l + 1 // Add a separator between groups with `+ 1`
	}

	// Display `TAB` prompt after final search group if only one search group
	// +1 just to give some breathing room
	if len(t.search) == 1 && len(t.search[0]) > 0 && t.curY == 0 {
		emitStr(s, pos+1, 0, t.promptStyle, searchGroupPrompt)
	}

	// Display whether all items or just non-hidden items are currently displayed
	if t.showHidden {
		indicator := "VIS"
		emitStr(s, t.w-len([]byte(indicator))+reservedEndChars, 0, searchStyle, indicator)
	} else {
		indicator := "HID"
		emitStr(s, t.w-len([]byte(indicator))+reservedEndChars, 0, searchStyle, indicator)
	}
}

func (t *Terminal) buildFooter(s tcell.Screen, text string) {
	footer := tcell.StyleDefault.
		Foreground(tcell.ColorBlack).Background(tcell.ColorYellow)

	// Pad out remaining line with spaces to ensure whole bar is filled
	lenStr := len([]rune(text))
	text += string(make([]rune, t.w-lenStr))
	// reservedBottomLines is subtracted from t.h globally, and we want to print on the bottom line
	// so add it back in here
	emitStr(s, 0, t.h-1+reservedBottomLines, footer, text)
}

func (t *Terminal) resizeScreen() {
	w, h := t.S.Size()
	t.w = w - reservedEndChars
	t.h = h - reservedBottomLines
}

func (t *Terminal) paint(matches []service.ListItem, saveWarning bool) error {
	// Build top search box
	t.buildSearchBox(t.S)

	// Style for highlighting currnetly selected items
	selectedStyle := tcell.StyleDefault.
		Background(tcell.ColorGrey).
		Foreground(tcell.ColorWhite)

	// Style for highlighting notes
	noteStyle := tcell.StyleDefault.
		Background(tcell.ColorMaroon).
		Foreground(tcell.ColorWhite)

	// Fill lineItems
	var offset int
	var style tcell.Style
	for i, r := range matches[t.vertOffset:min(len(matches), t.vertOffset+t.h-reservedTopLines)] {
		offset = i + reservedTopLines
		// If item is highlighted, indicate accordingly
		if _, ok := t.selectedItems[i]; ok {
			style = selectedStyle
		} else if r.Note != nil && len(*(r.Note)) > 0 {
			// If note is present, indicate with a different style
			style = noteStyle
		} else {
			style = t.style
		}

		line := r.Line

		// Truncate the full search string from any lines matching the entire thing,
		// ignoring search operators
		// Op needs to be case-insensitive, but must not mutate underlying line
		if strings.HasPrefix(strings.ToLower(line), t.hiddenMatchPrefix) {
			line = string([]rune(line)[len([]byte(t.hiddenMatchPrefix)):])
		}
		// If we strip the match prefix, and there is a space remaining, trim that too
		line = strings.TrimPrefix(line, " ")

		// Account for horizontal offset if on curItem
		if i == t.curY-reservedTopLines {
			line = line[t.horizOffset:]
		}

		// Emit line
		emitStr(t.S, 0, offset, style, line)
		// Emit `hidden` indicator
		if r.IsHidden {
			emitStr(t.S, t.w, offset, style, "*")
		}
		if offset == t.h {
			break
		}
	}

	// If no matches, display help prompt on first line
	// TODO ordering
	if len(matches) == 0 {
		if len(t.search) > 0 && len(t.search[0]) > 0 {
			newLinePrefixPrompt := fmt.Sprintf("Enter: Create new line with search prefix: \"%s\"", t.getNewLinePrefix())
			emitStr(t.S, 0, reservedTopLines, t.promptStyle, newLinePrefixPrompt)
		} else {
			emitStr(t.S, 0, reservedTopLines, t.promptStyle, newLinePrompt)
		}
	}

	if t.footerMessage != "" {
		t.buildFooter(t.S, t.footerMessage)
	}

	//if saveWarning {
	//    // Fill reserved footer/info line at the bottom of the screen if present
	//    t.buildFooter(t.S, saveWarningMsg)
	//}

	t.S.ShowCursor(t.curX, t.curY)
	return nil
}

func (t *Terminal) getHiddenLinePrefix(keys [][]rune) string {
	// Only apply the trunaction on "closed" search groups (e.g. when the user has tabbed to
	// the next one).

	if len(keys) == 0 || (len(keys) == 1 && len(keys[0]) == 0) {
		return ""
	}

	// Only operate on the first key
	key := keys[0]
	_, nChars := t.db.GetMatchPattern(key)
	trimmedKey := string(key[nChars:])

	shortenedPrefix := fmt.Sprintf("%s ", strings.TrimSpace(strings.ToLower(trimmedKey)))

	return shortenedPrefix
}

func (t *Terminal) getSearchGroupIdxAndOffset() (int, int) {
	// Get search group to operate on, and the char within that
	grpIdx, start := 0, 0
	end := len(t.search[grpIdx])
	for end < t.curX {
		grpIdx++
		start = end + 1 // `1` accounts for the visual separator between groups
		end = start + len(t.search[grpIdx])
	}
	charOffset := t.curX - start
	return grpIdx, charOffset
}

func (t *Terminal) insertCharInPlace(line []rune, offset int, newChar rune) []rune {
	line = append(line, 0)
	copy(line[offset+1:], line[offset:])
	line[offset] = newChar
	return line
}

func (t *Terminal) getLenSearchBox() int {
	// Add up max potential position based on number of runes in groups, and separators between
	lenSearchBox := max(0, len(t.search)-1) // Account for spaces between search groups
	for _, g := range t.search {
		lenSearchBox += len(g)
	}
	return lenSearchBox
}

func longestCommonPrefix(strs []string) string {
	if len(strs) == 0 {
		return ""
	}
	// Assume prefix
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

func getCommonSearchPrefix(selectedItems map[int]string) [][]rune {
	var lines []string
	for _, line := range selectedItems {
		lines = append(lines, strings.TrimSpace(line))
	}
	prefix := strings.TrimSpace(longestCommonPrefix(lines))
	if len(prefix) == 0 {
		return [][]rune{}
	}
	return [][]rune{[]rune(fmt.Sprintf("=%s", prefix))}
}

type RefreshKey struct {
	T time.Time
}

// Implementing When() to fulfil the tcell.Event interface
func (ev *RefreshKey) When() time.Time {
	return ev.T
}

func (t *Terminal) getNewLinePrefix() string {
	var searchStrings []string
	for _, group := range t.search {
		pattern, nChars := t.db.GetMatchPattern(group)
		if pattern != service.InverseMatchPattern && len(group) > 0 {
			searchStrings = append(searchStrings, string(group[nChars:]))
		}
	}
	newString := ""
	if len(searchStrings) > 0 {
		newString = fmt.Sprintf("%s ", strings.Join(searchStrings, " "))
	}
	return newString
}

func matchFirstURL(line string) string {
	// Attempt to match any urls in the line.
	// If present, copy the first to the system clipboard.
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

func getLenHiddenMatchPrefix(line string, hiddenMatchPrefix string) int {
	l := 0
	if strings.HasPrefix(strings.ToLower(line), hiddenMatchPrefix) {
		l = len([]byte(hiddenMatchPrefix))
	}
	return l
}

func (t *Terminal) HandleKeyEvent(ev tcell.Event) (bool, error) {
	posDiff := []int{0, 0} // x and y mutations to apply after db data mutations

	// itemKey represents the unique identifying key for the ListItem. We set it explicitly only
	// when creating new ListItems via the Add interface, so we can tell the backend what our
	// current item is when asking for Matches (and adjusting for cursor offsets due to live collab)
	// If itemKey is empty by the time we reach the Match call, we offset any movement against our
	// existing match set (e.g. for Move*) and then default to matchedItem.Key()
	itemKey := ""

	// matchIdx accounts for any offset and reserved lines at the top - it represents the true index
	// of the item in the match set
	matchIdx := t.curY + t.vertOffset - reservedTopLines
	isSearchLine := matchIdx == -1

	// offsetX represents the position in the underying curItem.Line
	// Only apply the prefix offset if the line starts with the prefix, other lines will
	// match but not have the prefix truncated
	offsetX := t.horizOffset + t.curX
	lenHiddenMatchPrefix := 0
	if t.curItem != nil {
		lenHiddenMatchPrefix = getLenHiddenMatchPrefix(t.curItem.Line, t.hiddenMatchPrefix)
	}
	offsetX += lenHiddenMatchPrefix
	var err error
	switch ev := ev.(type) {
	case *tcell.EventKey:
		switch ev.Key() {
		case tcell.KeyEscape:
			if t.previousKey == tcell.KeyEscape {
				t.S.Fini()
				return false, nil
			}
			if len(t.selectedItems) > 0 {
				t.selectedItems = make(map[int]string)
			} else {
				t.curY = 0 // TODO
			}
		case tcell.KeyEnter:
			if len(t.selectedItems) > 0 {
				// Add common search prefix to search groups
				t.search = getCommonSearchPrefix(t.selectedItems)
				t.selectedItems = make(map[int]string)
				t.curY = 0
				if len(t.search) > 0 {
					posDiff[0] += len(t.search[0])
				}
			} else {
				// Add a new item below current cursor position
				// This will insert the contents of the current search string (omitting search args like `=`)
				var err error
				if isSearchLine {
					if len(t.search) > 0 {
						posDiff[0] -= len([]byte(strings.TrimSpace(string(t.search[0])))) + 1
					}
				}
				newString := t.getNewLinePrefix()
				itemKey, err = t.db.Add(newString, nil, matchIdx+1)
				if err != nil {
					log.Fatal(err)
				}
				posDiff[1]++
			}
		case tcell.KeyCtrlD:
			if isSearchLine {
				t.search = [][]rune{}
			} else {
				// Copy into buffer in case we're moving it elsewhere
				t.copiedItem = t.curItem
				err := t.db.Delete(matchIdx)
				if err != nil {
					log.Fatal(err)
				}
			}
			t.horizOffset = 0
		case tcell.KeyCtrlO:
			if isSearchLine {
				if err := t.S.Suspend(); err == nil {
					err = t.openEditorSession()
					if err != nil {
						log.Fatal(err)
					}
					if err := t.S.Resume(); err != nil {
						panic("failed to resume: " + err.Error())
					}
				}
			}
		case tcell.KeyCtrlA:
			// Go to beginning of line
			// TODO decouple cursor mutation from key handling
			t.curX = 0
			t.horizOffset = 0
		case tcell.KeyCtrlE:
			// Go to end of line
			if isSearchLine {
				t.curX = t.getLenSearchBox()
			} else {
				t.curX = len([]rune(t.curItem.Line))
			}
			t.horizOffset = t.curX - t.w
		case tcell.KeyCtrlV:
			// Toggle hidden item visibility
			if isSearchLine {
				t.showHidden = !t.showHidden
			} else {
				err = t.db.ToggleVisibility(matchIdx)
				if err != nil {
					log.Fatal(err)
				}
			}
		case tcell.KeyCtrlU:
			err := t.db.Undo()
			if err != nil {
				log.Fatal(err)
			}
		case tcell.KeyCtrlR:
			err := t.db.Redo()
			if err != nil {
				log.Fatal(err)
			}
		case tcell.KeyCtrlC:
			// Copy functionality
			if !isSearchLine {
				t.copiedItem = t.curItem
				if url := matchFirstURL(t.curItem.Line); url != "" {
					clipboard.WriteAll(url)
				}
			}
		case tcell.KeyCtrlUnderscore:
			if !isSearchLine {
				if url := matchFirstURL(t.curItem.Line); url != "" {
					openURL(url)
				}
			}
		case tcell.KeyCtrlCarat:
			// This is experimental functionality atm, hence the weird keypress
			t.db.GenerateView(t.search, t.showHidden)
		case tcell.KeyCtrlP:
			// Paste functionality
			if t.copiedItem != nil {
				itemKey, err = t.db.Add(t.copiedItem.Line, nil, matchIdx+1)
				if err != nil {
					log.Fatal(err)
				}
				posDiff[1]++
			}
		case tcell.KeyCtrlS:
			if !isSearchLine {
				// If exists, clear, otherwise set
				if _, ok := t.selectedItems[matchIdx]; ok {
					delete(t.selectedItems, matchIdx)
				} else {
					t.selectedItems[matchIdx] = t.matches[matchIdx].Line
				}
			}
		case tcell.KeyTab:
			if isSearchLine {
				// If no search groups exist, rely on separate new char insertion elsewhere
				if len(t.search) > 0 {
					// The location of the cursor will determine where the search group is added
					// If `Tabbing` in the middle of the search group, we need to split the group into two
					// The character immediately after the current position will represent the first
					// character in the new (right most) search group
					grpIdx, charOffset := t.getSearchGroupIdxAndOffset()
					currentGroup := t.search[grpIdx]
					newLeft, newRight := currentGroup[:charOffset], currentGroup[charOffset:]
					t.search = append(t.search, []rune{})
					copy(t.search[grpIdx+1:], t.search[grpIdx:])
					t.search[grpIdx] = newLeft
					t.search[grpIdx+1] = newRight
				}
				posDiff[0]++
			}
		case tcell.KeyBackspace:
		case tcell.KeyBackspace2:
			if isSearchLine {
				if len(t.search) > 0 {
					grpIdx, charOffset := t.getSearchGroupIdxAndOffset()
					newGroup := []rune(t.search[grpIdx])

					// If charOffset == 0 we are acting on the previous separator
					if charOffset == 0 {
						// If we are operating on a middle (or initial) separator, we need to merge
						// previous and next search groups before cleaning up the search group
						if grpIdx > 0 {
							newGroup = append(t.search[grpIdx-1], t.search[grpIdx]...)
							t.search[grpIdx-1] = newGroup
							t.search = append(t.search[:grpIdx], t.search[grpIdx+1:]...)
						}
					} else {
						newGroup = append(newGroup[:charOffset-1], newGroup[charOffset:]...)
						t.search[grpIdx] = newGroup
					}
					posDiff[0]--
				}
			} else {
				// If cursor in 0 position and current line is empty, delete current line and go
				// to end of previous line (if present)
				newLine := []rune(t.curItem.Line)
				if t.horizOffset+t.curX > 0 && len(newLine) > 0 {
					newLine = append(newLine[:offsetX-1], newLine[offsetX:]...)
					err = t.db.Update(string(newLine), nil, matchIdx)
					if err != nil {
						log.Fatal(err)
					}
					posDiff[0]--
				} else if (offsetX-lenHiddenMatchPrefix) == 0 && (len(newLine)-lenHiddenMatchPrefix) == 0 {
					err := t.db.Delete(matchIdx)
					if err != nil {
						log.Fatal(err)
					}
					// Move up a cursor position
					posDiff[1]--
					if matchIdx >= 0 {
						// TODO setting to the max width isn't completely robust as other
						// decrements will affect, but it's good enough for now as the cursor
						// repositioning logic will take care of over-increments
						posDiff[0] += t.w
					}
				}
			}
		case tcell.KeyDelete:
			// TODO this is very similar to the Backspace logic above, refactor to avoid duplication
			if matchIdx == 0 {
				if len(t.search) > 0 {
					grpIdx, charOffset := t.getSearchGroupIdxAndOffset()
					newGroup := []rune(t.search[grpIdx])

					// If charOffset == len(t.search[grpIdx]) we need to merge with the next group (if present)
					if charOffset == len(t.search[grpIdx]) {
						if grpIdx < len(t.search)-1 {
							newGroup = append(t.search[grpIdx], t.search[grpIdx+1]...)
							t.search[grpIdx] = newGroup
							t.search = append(t.search[:grpIdx+1], t.search[grpIdx+2:]...)
						}
					} else {
						newGroup = append(newGroup[:charOffset], newGroup[charOffset+1:]...)
						t.search[grpIdx] = newGroup
					}
				}
			} else {
				newLine := []rune(t.curItem.Line)
				if len(newLine) > 0 && t.horizOffset+t.curX+lenHiddenMatchPrefix < len(newLine) {
					newLine = append(newLine[:offsetX], newLine[offsetX+1:]...)
					err = t.db.Update(string(newLine), nil, matchIdx)
					if err != nil {
						log.Fatal(err)
					}
				} else if (offsetX-lenHiddenMatchPrefix) == 0 && (len(newLine)-lenHiddenMatchPrefix) == 0 {
					// If cursor in 0 position and current line is empty, delete current line and go
					// to end of previous line (if present)
					err := t.db.Delete(matchIdx)
					if err != nil {
						log.Fatal(err)
					}
					// Move up a cursor position
					posDiff[1]--
					if matchIdx >= 0 {
						// TODO setting to the max width isn't completely robust as other
						// decrements will affect, but it's good enough for now as the cursor
						// repositioning logic will take care of over-increments
						posDiff[0] += t.w
					}
				}
			}
		case tcell.KeyPgUp:
			if matchIdx > 0 {
				// Move the current item up and follow with cursor
				moved, err := t.db.MoveUp(matchIdx)
				if err != nil {
					log.Fatal(err)
				}
				if moved {
					posDiff[1]--
				}
			}
		case tcell.KeyPgDn:
			if matchIdx > 0 {
				// Move the current item down and follow with cursor
				moved, err := t.db.MoveDown(matchIdx)
				if err != nil {
					log.Fatal(err)
				}
				if moved {
					posDiff[1]++
				}
			}
		case tcell.KeyDown:
			posDiff[1]++
		case tcell.KeyUp:
			posDiff[1]--
		case tcell.KeyRight:
			posDiff[0]++
		case tcell.KeyLeft:
			posDiff[0]--
		default:
			if isSearchLine {
				if len(t.search) > 0 {
					grpIdx, charOffset := t.getSearchGroupIdxAndOffset()
					newGroup := make([]rune, len(t.search[grpIdx]))
					copy(newGroup, t.search[grpIdx])

					// We want to insert a char into the current search group then update in place
					newGroup = t.insertCharInPlace(newGroup, charOffset, ev.Rune())
					oldLen := len(string(newGroup))
					parsedGroup := parseOperatorGroups(string(newGroup))
					t.search[grpIdx] = []rune(parsedGroup)
					posDiff[0] += len(parsedGroup) - oldLen
				} else {
					var newTerm []rune
					newTerm = append(newTerm, ev.Rune())
					t.search = append(t.search, newTerm)
				}
				posDiff[0]++
			} else {
				newLine := []rune(t.curItem.Line)
				// Insert characters at position
				if len(newLine) == 0 || len(newLine) == offsetX {
					newLine = append(newLine, ev.Rune())
				} else {
					newLine = t.insertCharInPlace(newLine, offsetX, ev.Rune())
				}
				oldLen := len(newLine)
				parsedNewLine := parseOperatorGroups(string(newLine))
				err = t.db.Update(parsedNewLine, nil, matchIdx)
				if err != nil {
					log.Fatal(err)
				}
				posDiff[0] += len([]rune(parsedNewLine)) - oldLen + 1
			}
			t.footerMessage = ""
		}
		t.previousKey = ev.Key()
	}
	t.S.Clear()

	t.hiddenMatchPrefix = t.getHiddenLinePrefix(t.search)

	// Account for any explicit moves
	matchIdx += posDiff[1]

	if itemKey == "" && matchIdx >= 0 && matchIdx < len(t.matches) {
		itemKey = t.matches[matchIdx].Key()
	}

	// Handle any offsets that occurred due to other collaborators interacting with the same list
	// at the same time
	t.matches, err = t.db.Match(t.search, t.showHidden, itemKey)

	if matchIdx >= 0 && matchIdx < len(t.matches) {
		currentOffset := t.matches[matchIdx].Offset
		oneAbove := matchIdx - 1
		oneBelow := matchIdx + 1
		// If the current matchItem is the top/root, and the offset < 0, set it to 0
		if matchIdx == 0 && currentOffset < 0 {
			currentOffset = 0
		} else if currentOffset != 0 && oneAbove >= reservedTopLines {
			childMatchItem := t.matches[oneAbove-reservedTopLines]
			if childMatchItem.Offset-1 == currentOffset {
				// When the previous current line has been deleted. The item that will then be under the cursor will
				// have moved up, and hence the Offset will reflect that, but in this particular case, we want the
				// cursor to remain in the same position.
				// Therefore, check the offset of the child matchItem - if it's 1 less than the current (we need to
				// account for _other_ changes that could have occurred too), then subtract one from the actionable
				// offset. HOWEVER, we only do this for deletions, not additions, so assert on negative currentOffset
				// too.
				currentOffset++
			} else if childMatchItem.Offset+2 == currentOffset {
				// On MoveUp actions, we move the item up and attempt to follow it, but the item we swop with has a
				// +1 diff applied to it, so it cancels it out. In this case, if the offset of the current is 2 more than
				// the offset of the child matchItem, subtract another decrement from the currentOffset.
				currentOffset--
			}
		} else if oneBelow-reservedTopLines < len(t.matches) {
			// Likewise on MoveDown, we need to account for the opposite
			// NOTE: this is only the case when on the top line (e.g. root item)
			parentMatchItem := t.matches[oneBelow-reservedTopLines]
			if parentMatchItem.Offset-2 == currentOffset {
				currentOffset++
			}
		}
		posDiff[1] += currentOffset
	}

	// N available item slots
	windowSize := t.h - reservedTopLines - reservedBottomLines

	// Change vertical cursor position based on a number of constraints.
	// This needs to happen after matches have been refreshed, but before setting a new curItem
	newYIdx := t.curY + posDiff[1] // Apply diff from ops

	// If there are hidden items above the top displayed item, shift all down 1
	if newYIdx < reservedTopLines-1 {
		if t.vertOffset > 0 {
			t.vertOffset--
		}
		newYIdx = reservedTopLines - 1
	}

	// Cater for hidden items below
	if newYIdx > t.h-1 {
		// N items still available below the top offset (aka any hidden at the top of the list)
		nItemsBelowOffset := len(t.matches) - t.vertOffset

		// N items remaining BELOW the item slots (aka visible portion of the list)
		nItemsBelowInvisible := nItemsBelowOffset - windowSize

		if nItemsBelowInvisible > 0 {
			t.vertOffset++
		}
	}
	// Prevent going out of range of screen
	newYIdx = min(newYIdx, t.h-1)

	// Prevent going out of range of returned matches
	// This needs knowledge of the vertOffset as we will never show empty lines at the bottom of the
	// screen if there is an offset available at the top
	// TODO change this, follow the cursor...
	t.curY = min(newYIdx, reservedTopLines+len(t.matches)-1)
	// Reset matchIdx
	matchIdx = t.curY + t.vertOffset - reservedTopLines

	// Reset
	isSearchLine = matchIdx < 0

	// Set curItem before establishing max X position based on the len of the curItem line (to avoid
	// nonexistent array indexes). If on search line, just set to nil
	if isSearchLine {
		t.curItem = nil
	} else {
		t.curItem = &t.matches[matchIdx]
	}

	// Then refresh the X position based on vertical position and curItem

	// If we've moved up or down, clear the horizontal offset
	if posDiff[1] > 0 || posDiff[1] < 0 {
		t.horizOffset = 0
	}

	// Some logic above does forceful operations to ensure that the cursor is moved to MINIMUM beginning of line
	// Therefore ensure we do not go < 0
	t.horizOffset = max(0, t.horizOffset)

	newXIdx := t.curX + posDiff[0]
	if isSearchLine {
		newXIdx = max(0, newXIdx) // Prevent index < 0
		t.curX = min(newXIdx, t.getLenSearchBox())
	} else {
		// Deal with horizontal offset if applicable
		if newXIdx < 0 {
			if t.horizOffset > 0 {
				t.horizOffset--
			}
			t.curX = 0 // Prevent index < 0
		} else {
			if newXIdx > t.w-1 && t.horizOffset+t.w-1 < len(t.curItem.Line) {
				t.horizOffset++
			}
			// We need to recalc lenHiddenMatchPrefix here to cover the case when we arrow down from
			// the search line to the top line, when there's a hidden prefix (otherwise there is a delay
			// before the cursor sets to the end of the line and we're at risk of an index error).
			lenHiddenMatchPrefix = getLenHiddenMatchPrefix(t.curItem.Line, t.hiddenMatchPrefix)
			newXIdx = min(newXIdx, len([]rune(t.curItem.Line))-t.horizOffset-lenHiddenMatchPrefix) // Prevent going out of range of the line
			t.curX = min(newXIdx, t.w-1)                                                           // Prevent going out of range of the page
		}
	}

	t.resizeScreen()
	t.paint(t.matches, false)
	t.S.Show()

	return true, nil
}
