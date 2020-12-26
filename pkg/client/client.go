package client

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"

	"fuzzy-note/pkg/service"
	//"github.com/Sambigeara/fuzzy-note/pkg/service"

	"github.com/gdamore/tcell"
	"github.com/gdamore/tcell/encoding"
	"github.com/jpillora/longestcommon"
	"github.com/mattn/go-runewidth"
	"github.com/micmonay/keybd_event"
)

const (
	reservedTopLines int    = 1
	reservedEndChars int    = 1
	saveWarningMsg   string = "UNSAVED CHANGES: save with `Ctrl-s`, or ignore changes and exit with `Ctrl-_`"
)

type Terminal struct {
	db                    service.ListRepo
	search                [][]rune
	curItem               *service.ListItem // The currently selected item
	s                     tcell.Screen
	style                 tcell.Style
	w                     int
	h                     int
	curX                  int // Cur "screen" index, not related to matched item lists
	curY                  int // Cur "screen" index, not related to matched item lists
	vertOffset            int // The index of the first displayed item in the match set
	horizOffset           int // The index of the first displayed char in the curItem
	showHidden            bool
	selectedItems         map[service.ListItem]struct{} // struct{} is more space efficient than bool
	hiddenMatchPrefix     string                        // The common string that we want to truncate from each line
	hiddenFullMatchPrefix string                        // The common string that we want to truncate from each line
}

func NewTerm(db service.ListRepo, colour string) *Terminal {
	encoding.Register()

	var defStyle tcell.Style
	if colour == "light" {
		defStyle = tcell.StyleDefault.
			Background(tcell.ColorWhite).
			Foreground(tcell.ColorBlack)
	} else {
		defStyle = tcell.StyleDefault.
			Background(tcell.ColorBlack).
			Foreground(tcell.ColorWhite)
	}

	s := newInstantiatedScreen(defStyle)

	w, h := s.Size()
	return &Terminal{
		db:         db,
		s:          s,
		style:      defStyle,
		w:          w,
		h:          h,
		showHidden: false,
	}
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

// This method is a horrible workaround for this bug: https://github.com/gdamore/tcell/issues/194
// When closing a screen session (to give full control to the external program, e.g. vim), a subsequent keypress is dropped.
// This aditional EOL event is sent to ensure consequent events are correctly handled
func sendExtraEventFix() {
	kb, err := keybd_event.NewKeyBonding()
	if err != nil {
		panic(err)
	}
	kb.SetKeys(keybd_event.VK_ENTER)
	err = kb.Launching()
	if err != nil {
		panic(err)
	}
}

func (t *Terminal) openEditorSession() error {
	// TODO https://github.com/gdamore/tcell/issues/194
	sendExtraEventFix()

	// Write text to temp file
	tmpfile, err := ioutil.TempFile("", "fzn_buffer")
	if err != nil {
		log.Fatal(err)
		return err
	}

	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write(*t.curItem.Note); err != nil {
		log.Fatal(err)
		return err
	}

	//https://stackoverflow.com/questions/21513321/how-to-start-vim-from-go
	cmd := exec.Command("vim", tmpfile.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	if err != nil {
		log.Printf("Command finished with error: %v", err)
		return err
	}

	// Read back from the temp file, and return to the write function
	newDat, err := ioutil.ReadFile(tmpfile.Name())
	if err != nil {
		log.Fatal(err)
		return nil
	}

	err = t.db.Update(t.curItem.Line, &newDat, t.curItem)
	if err != nil {
		log.Fatal(err)
	}

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (t *Terminal) buildSearchBox(s tcell.Screen) {
	searchStyle := tcell.StyleDefault.
		Foreground(tcell.ColorWhite).Background(tcell.ColorGrey)

	var pos, l int
	for _, key := range t.search {
		emitStr(s, pos, 0, searchStyle, string(key))
		l = len(key)
		pos = pos + l + 1 // Add a separator between groups with `+ 1`
	}

	// Display whether all items or just non-hidden items are currently displayed
	if t.showHidden {
		emitStr(s, t.w-3+reservedEndChars, 0, searchStyle, "VIS")
	} else {
		emitStr(s, t.w-3+reservedEndChars, 0, searchStyle, "HID")
	}
}

func (t *Terminal) buildFooter(s tcell.Screen, text string) {
	footer := tcell.StyleDefault.
		Foreground(tcell.ColorBlue).Background(tcell.ColorYellow)

	// Pad out remaining line with spaces to ensure whole bar is filled
	lenStr := len([]rune(text))
	text += string(make([]rune, t.w-lenStr))
	emitStr(s, 0, t.h-1, footer, text)
}

func (t *Terminal) resizeScreen() {
	w, h := t.s.Size()
	t.w = w - reservedEndChars
	t.h = h
}

func (t *Terminal) paint(matches []*service.ListItem, saveWarning bool) error {
	// Build top search box
	t.buildSearchBox(t.s)

	// Style for highlighting notes
	noteStyle := tcell.StyleDefault.
		Background(tcell.ColorGrey).
		Foreground(tcell.ColorWhite)

	selectedStyle := tcell.StyleDefault.
		Background(tcell.ColorMaroon).
		Foreground(tcell.ColorWhite)

	// Fill lineItems
	var offset int
	var style tcell.Style
	for i, r := range matches[t.vertOffset:] {
		offset = i + reservedTopLines
		// If item is highlighted, indicate accordingly
		if _, ok := t.selectedItems[*r]; ok {
			style = selectedStyle
		} else if len(*(r.Note)) > 0 {
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
			line = string([]rune(line)[len(t.hiddenMatchPrefix):])
		}
		//line = strings.TrimPrefix(line, t.hiddenMatchPrefix)
		// If we strip the match prefix, and there is a space remaining, trim that too
		line = strings.TrimPrefix(line, " ")

		// Account for horizontal offset if on curItem
		if r == t.curItem {
			line = line[t.horizOffset:]
		}

		// Emit line
		emitStr(t.s, 0, offset, style, line)
		// Emit `hidden` indicator
		if r.IsHidden {
			emitStr(t.s, t.w, offset, style, "*")
		}
		if offset == t.h {
			break
		}
	}

	//if saveWarning {
	//    // Fill reserved footer/info line at the bottom of the screen if present
	//    t.buildFooter(t.s, saveWarningMsg)
	//}

	t.s.ShowCursor(t.curX, t.curY)
	return nil
}

func (t *Terminal) getHiddenLinePrefix(keys [][]rune) (string, string) {
	// Only apply the trunaction on "closed" search groups (e.g. when the user has tabbed to
	// the next one).

	if len(keys) == 0 || (len(keys) == 1 && len(keys[0]) == 0) {
		return "", ""
	}

	//keyArray := []string{}
	//// We keep an array with the modifiers as well to allow us to return true lengths
	//fullKeyArray := []string{}
	//for _, key := range keys {
	//    // Strip all trailing and leading spaces
	//    // Ignore empty keys
	//    key = []rune(strings.TrimSpace(string(key)))
	//    if len(key) > 0 {
	//        _, nChars := t.db.GetMatchPattern(key)
	//        keyArray = append(keyArray, string(key[nChars:]))
	//        fullKeyArray = append(fullKeyArray, string(key))
	//    }
	//}

	//shortenedPrefix := fmt.Sprintf("%s ", strings.Join(keyArray, " "))
	//fullPrefix := fmt.Sprintf("%s ", strings.Join(fullKeyArray, " "))

	// The above joins all keys in the search group, but this causes numerous strange behaviours.
	// For now, only operate on the first key.
	key := keys[0]
	_, nChars := t.db.GetMatchPattern(key)
	trimmedKey := string(key[nChars:])
	fullKey := string(key)

	shortenedPrefix := fmt.Sprintf("%s ", strings.TrimSpace(strings.ToLower(trimmedKey)))
	fullPrefix := fmt.Sprintf("%s ", strings.TrimSpace(strings.ToLower(fullKey)))

	return shortenedPrefix, fullPrefix
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

func (t *Terminal) handleTextLengthChange(startLen int, endLenFunc func() int, posDiff *[]int, updateFunc func() error) {
	// Certain search character combo's are interpreted as special operators by the backend
	// This means that the length of the search string might unpredictably change when calling
	// `Match` leaving the cursor in an undesired location. To combat this, we need to take
	// note of the length of the search string prior to a key press and after, and apply the diff.
	// We wrap this around the `Match` call
	err := updateFunc()
	if err != nil {
		log.Fatal(err)
	}
	(*posDiff)[0] += endLenFunc() - startLen
}

func getCommonSearchPrefix(selectedItems map[service.ListItem]struct{}) [][]rune {
	var lines []string
	for item := range selectedItems {
		lines = append(lines, item.Line)
	}
	prefix := strings.TrimSpace(longestcommon.Prefix(lines))
	if len(prefix) == 0 {
		return [][]rune{}
	}
	return [][]rune{[]rune(fmt.Sprintf("#%s", prefix))}
}

// RunClient reads key presses on a loop
func (t *Terminal) RunClient() error {

	matches, err := t.db.Match([][]rune{}, nil, t.showHidden)
	if err != nil {
		log.Fatal(err)
	}

	// Pre-instantiate the selectedItems map
	t.selectedItems = make(map[service.ListItem]struct{})

	t.paint(matches, false)

	for {
		posDiff := []int{0, 0} // x and y mutations to apply after db data mutations
		t.s.Show()
		ev := t.s.PollEvent()

		// offsetX represents the position in the underying curItem.Line
		lenHiddenMatchPrefix := len([]byte(t.hiddenMatchPrefix))
		offsetX := t.horizOffset + t.curX + lenHiddenMatchPrefix

		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Key() {
			case tcell.KeyCtrlUnderscore:
				t.s.Fini()
				return nil
			case tcell.KeyEnter:
				if len(t.selectedItems) > 0 {
					// Add common search prefix to search groups
					t.search = getCommonSearchPrefix(t.selectedItems)
					t.selectedItems = make(map[service.ListItem]struct{})
					t.curY = 0
				} else {
					// Add a new item below current cursor position
					// This will insert the contents of the current search string (omitting search args like `#`)
					var searchStrings []string
					// We can't use t.hiddenMatchPrefix as it only includes the first group at present
					for _, group := range t.search {
						_, nChars := t.db.GetMatchPattern(group)
						if len(group) > 0 {
							searchStrings = append(searchStrings, string(group[nChars:]))
						}
					}
					newString := fmt.Sprintf("%s ", strings.Join(searchStrings, " "))

					var err error
					if t.curY == reservedTopLines-1 {
						err = t.db.Add(newString, nil, nil, nil)
						posDiff[0] -= len([]byte(t.hiddenFullMatchPrefix))
					} else {
						err = t.db.Add(newString, nil, t.curItem, nil)
					}
					if err != nil {
						log.Fatal(err)
					}
					posDiff[1]++
				}
			case tcell.KeyCtrlD:
				if t.curY == reservedTopLines-1 {
					t.search = [][]rune{}
				} else {
					err := t.db.Delete(t.curItem)
					if err != nil {
						log.Fatal(err)
					}
				}
			case tcell.KeyCtrlO:
				if t.curY != 0 {
					t.s.Fini()
					err = t.openEditorSession()
					if err != nil {
						log.Fatal(err)
					}
					t.s = newInstantiatedScreen(t.style)
				}
			case tcell.KeyCtrlA:
				// Go to beginning of line
				// TODO decouple cursor mutation from key handling
				t.curX = 0
				t.horizOffset = 0
			case tcell.KeyCtrlE:
				// Go to end of line
				if t.curY == reservedTopLines-1 {
					t.curX = t.getLenSearchBox()
				} else {
					// TODO
					t.curX = len([]rune(t.curItem.Line))
				}
				t.horizOffset = t.curX - t.w
			case tcell.KeyCtrlV:
				// Toggle hidden item visibility
				if t.curY == reservedTopLines-1 {
					t.showHidden = !t.showHidden
				} else {
					err = t.db.ToggleVisibility(t.curItem)
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
			case tcell.KeyCtrlS:
				// If exists, clear, otherwise set
				if _, ok := t.selectedItems[*t.curItem]; ok {
					delete(t.selectedItems, *t.curItem)
				} else {
					t.selectedItems[*t.curItem] = struct{}{}
				}
			case tcell.KeyEscape:
				if len(t.selectedItems) > 0 {
					t.selectedItems = make(map[service.ListItem]struct{})
				} else {
					t.curY = 0 // TODO
				}
			case tcell.KeyTab:
				if t.curY == reservedTopLines-1 {
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
				if t.curY == reservedTopLines-1 {
					if len(t.search) > 0 {
						grpIdx, charOffset := t.getSearchGroupIdxAndOffset()
						newGroup := []rune(t.search[grpIdx])

						// If charOffset == 0 we are acting on the previous separator
						if charOffset <= 0 {
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
						err := t.db.Update(string(newLine), t.curItem.Note, t.curItem)
						if err != nil {
							log.Fatal(err)
						}
						posDiff[0]--
					} else if (offsetX-lenHiddenMatchPrefix) == 0 && (len(newLine)-lenHiddenMatchPrefix) == 0 {
						err := t.db.Delete(t.curItem)
						if err != nil {
							log.Fatal(err)
						}
						// Move up a cursor position
						posDiff[1]--
						if t.curY > reservedTopLines {
							// TODO setting to the max width isn't completely robust as other
							// decrements will affect, but it's good enough for now as the cursor
							// repositioning logic will take care of over-increments
							posDiff[0] += t.w
						}
					}
				}
			case tcell.KeyDown:
				// TODO remove duplication once key combos are sorted (also handled on `Alt-[`)
				if ev.Modifiers()&tcell.ModAlt != 0 && t.curY > reservedTopLines-1 {
					// Move the current item down and follow with cursor
					moved, err := t.db.MoveDown(t.curItem)
					if err != nil {
						log.Fatal(err)
					}
					if moved {
						posDiff[1]++
					}
				} else {
					posDiff[1]++
				}
			case tcell.KeyUp:
				// TODO remove duplication once key combos are sorted (also handled on `Alt-]`)
				if ev.Modifiers()&tcell.ModAlt != 0 && t.curY > reservedTopLines-1 {
					// Move the current item up and follow with cursor
					moved, err := t.db.MoveUp(t.curItem)
					if err != nil {
						log.Fatal(err)
					}
					if moved {
						posDiff[1]--
					}
				} else {
					posDiff[1]--
				}
			case tcell.KeyRight:
				posDiff[0]++
			case tcell.KeyLeft:
				posDiff[0]--
			default:
				if t.curY == reservedTopLines-1 {
					if len(t.search) > 0 {
						grpIdx, charOffset := t.getSearchGroupIdxAndOffset()
						newGroup := make([]rune, len(t.search[grpIdx]))
						copy(newGroup, t.search[grpIdx])

						// We want to insert a char into the current search group then update in place
						newGroup = t.insertCharInPlace(newGroup, charOffset, ev.Rune())
						t.search[grpIdx] = newGroup
					} else {
						var newTerm []rune
						newTerm = append(newTerm, ev.Rune())
						t.search = append(t.search, newTerm)
					}
					posDiff[0]++
				} else {
					if ev.Rune() == '[' && ev.Modifiers()&tcell.ModAlt != 0 {
						// Move the current item down and follow with cursor
						moved, err := t.db.MoveDown(t.curItem)
						if err != nil {
							log.Fatal(err)
						}
						if moved {
							posDiff[1]++
						}
					} else if ev.Rune() == ']' && ev.Modifiers()&tcell.ModAlt != 0 {
						// Move the current item up and follow with cursor
						moved, err := t.db.MoveUp(t.curItem)
						if err != nil {
							log.Fatal(err)
						}
						if moved {
							posDiff[1]--
						}
					} else {
						newLine := []rune(t.curItem.Line)
						// Insert characters at position
						if len(newLine) == 0 || len(newLine) == offsetX {
							newLine = append(newLine, ev.Rune())
						} else {
							newLine = t.insertCharInPlace(newLine, offsetX, ev.Rune())
						}

						lenFunc := func() int {
							return len([]rune(t.curItem.Line))
						}
						updateFunc := func() error {
							return t.db.Update(string(newLine), t.curItem.Note, t.curItem)
						}
						// len([]rune) rather than len(string) as some string chars are >1 bytes
						t.handleTextLengthChange(len([]rune(newLine)), lenFunc, &posDiff, updateFunc)

						posDiff[0]++
					}
				}
			}
		}
		t.s.Clear()

		t.hiddenMatchPrefix, t.hiddenFullMatchPrefix = t.getHiddenLinePrefix(t.search)

		var cur *service.ListItem
		if t.curItem != nil {
			cur = t.curItem
		}

		matches := []*service.ListItem{}
		updateFunc := func() error {
			matches, err = t.db.Match(t.search, cur, t.showHidden)
			return err
		}
		t.handleTextLengthChange(t.getLenSearchBox(), t.getLenSearchBox, &posDiff, updateFunc)

		// N available item slots
		nItemSlots := t.h - reservedTopLines

		// Before doing anything with vertical positioning, we need to automatically reduce available
		// offset if there are unused visible lines
		linesToClear := nItemSlots - len(matches) + t.vertOffset
		if linesToClear > 0 {
			t.vertOffset = max(0, t.vertOffset-linesToClear)
		}

		// Change vertical cursor position based on a number of constraints.
		// This needs to happen after matches have been refreshed, but before setting a new curItem
		newYIdx := t.curY + posDiff[1] // Apply diff from ops

		// If there are hidden items above the top displayed item, shift all down 1
		if newYIdx < reservedTopLines-1 {
			if t.vertOffset > 0 {
				t.vertOffset--
			}
		}

		// Prevent index < 0
		newYIdx = max(newYIdx, reservedTopLines-1)

		// Cater for hidden items below
		if newYIdx > t.h-1 {
			// N items still available below the top offset (aka any hidden at the top of the list)
			nItemsBelowOffset := len(matches) - t.vertOffset

			// N items remaining BELOW the item slots (aka visible portion of the list)
			nItemsBelowInvisible := nItemsBelowOffset - nItemSlots

			if nItemsBelowInvisible > 0 {
				t.vertOffset++
			}
		}
		// Prevent going out of range of screen
		newYIdx = min(newYIdx, t.h-1)

		// Prevent going out of range of returned matches
		// This needs to knowledge of the vertOffset as we will never show empty lines at the bottom of the screen if there is an
		// offset available at the top
		t.curY = min(newYIdx, reservedTopLines+len(matches)-1)

		isSearchLine := t.curY <= reservedTopLines-1 // `- 1` for 0 idx

		// Set curItem before establishing max X position based on the len of the curItem line (to avoid nonexistent array indexes)
		// If on search line, just set to nil
		if isSearchLine {
			t.curItem = nil
		} else {
			t.curItem = matches[t.curY-reservedTopLines+t.vertOffset]
		}

		// Then refresh the X position based on vertical position and curItem

		// If we've moved up for down, clear the horizontal offset
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
				// len([]rune) rather than len(string) as some string chars are >1 bytes
				newXIdx = min(newXIdx, len([]rune(t.curItem.Line))-t.horizOffset-lenHiddenMatchPrefix) // Prevent going out of range of the line
				t.curX = min(newXIdx, t.w-1)                                                           // Prevent going out of range of the page
			}
		}

		t.resizeScreen()
		t.paint(matches, false)
	}
}
