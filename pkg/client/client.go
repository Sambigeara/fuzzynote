package client

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"

	"fuzzy-note/pkg/service"
	//"github.com/Sambigeara/fuzzy-note/pkg/service"

	"github.com/gdamore/tcell"
	"github.com/gdamore/tcell/encoding"
	"github.com/mattn/go-runewidth"
	"github.com/micmonay/keybd_event"
)

const (
	reservedTopLines int    = 1
	saveWarningMsg   string = "UNSAVED CHANGES: save with `Ctrl-s`, or ignore changes and exit with `Ctrl-_`"
)

type Terminal struct {
	db          service.ListRepo
	search      [][]rune
	curItem     *service.ListItem // The currently selected item
	s           tcell.Screen
	style       tcell.Style
	w           int
	h           int
	curX        int // Cur "screen" index, not related to matched item lists
	curY        int // Cur "screen" index, not related to matched item lists
	vertOffset  int // The index of the first displayed item in the match set
	horizOffset int // The index of the first displayed char in the curItem
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
		db:    db,
		s:     s,
		style: defStyle,
		w:     w,
		h:     h,
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
	tempFile := "/tmp/fzn_buffer"
	f, err := os.Create(tempFile)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	_, err = f.Write(*t.curItem.Note)
	if err != nil {
		log.Fatal(err)
		return err
	}

	//https://stackoverflow.com/questions/21513321/how-to-start-vim-from-go
	cmd := exec.Command("vim", tempFile)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	if err != nil {
		log.Printf("Command finished with error: %v", err)
	}

	// Read back from the temp file, and return to the write function
	newDat, err := ioutil.ReadFile(tempFile)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	err = t.db.Update(t.curItem.Line, &newDat, t.curItem)
	if err != nil {
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
}

func (t *Terminal) buildFooter(s tcell.Screen, text string) {
	footer := tcell.StyleDefault.
		Foreground(tcell.ColorBlue).Background(tcell.ColorYellow)

	// Pad out remaining line with spaces to ensure whole bar is filled
	lenStr := len(text)
	text += string(make([]rune, t.w-lenStr))
	emitStr(s, 0, t.h-1, footer, text)
}

func (t *Terminal) resizeScreen() {
	w, h := t.s.Size()
	t.w = w
	t.h = h
}

func (t *Terminal) paint(matches []*service.ListItem, saveWarning bool) error {
	// Build top search box
	t.buildSearchBox(t.s)

	// Style for highlighting notes
	noteStyle := tcell.StyleDefault.
		Background(tcell.ColorGrey).
		Foreground(tcell.ColorWhite)

	// Fill lineItems
	var offset int
	var style tcell.Style
	for i, r := range matches[t.vertOffset:] {
		offset = i + reservedTopLines
		// If note is present, indicate with a different style
		if len(*(r.Note)) > 0 {
			style = noteStyle
		} else {
			style = t.style
		}

		line := r.Line
		// Account for horizontal offset if on curItem
		if r == t.curItem {
			line = line[t.horizOffset:]
		}

		// Emit line
		emitStr(t.s, 0, offset, style, line)
		if offset == t.h {
			break
		}
	}

	if saveWarning {
		// Fill reserved footer/info line at the bottom of the screen if present
		t.buildFooter(t.s, saveWarningMsg)
	}

	t.s.ShowCursor(t.curX, t.curY)
	return nil
}

// RunClient Read key presses on a loop
func (t *Terminal) RunClient() error {

	// List instantiation
	err := t.db.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

	matches, err := t.db.Match([][]rune{}, nil)
	if err != nil {
		log.Fatal(err)
	}

	triggerSaveWarning := false
	t.paint(matches, triggerSaveWarning)

	for {
		posDiff := []int{0, 0} // x and y mutations to apply after db data mutations
		t.s.Show()
		ev := t.s.PollEvent()

		triggerSaveWarning = false

		offsetX := t.horizOffset + t.curX

		// https://github.com/gdamore/tcell/blob/master/_demos/mouse.go
		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Key() {
			case tcell.KeyCtrlS:
				err := t.db.Save()
				if err != nil {
					log.Fatal(err)
				}
			case tcell.KeyCtrlX:
				if !t.db.HasPendingChanges() {
					t.s.Fini()
					os.Exit(0)
				} else {
					triggerSaveWarning = true
				}
			case tcell.KeyCtrlUnderscore:
				t.s.Fini()
				os.Exit(0)
			case tcell.KeyEnter:
				// Add a new item below current cursor position
				var err error
				if t.curY == reservedTopLines-1 {
					err = t.db.Add("", nil, nil)
				} else {
					err = t.db.Add("", nil, t.curItem)
				}
				if err != nil {
					log.Fatal(err)
				}
				posDiff[1]++
			case tcell.KeyCtrlD:
				if t.curY == reservedTopLines-1 {
					t.search = [][]rune{}
				} else {
					err := t.db.Delete(t.curItem)
					if err != nil {
						log.Fatal(err)
					}
				}
			case tcell.KeyTab:
				if t.curY == reservedTopLines-1 {
					// If current search group has runes, close off and create new one
					if len(t.search) > 0 {
						lastTerm := t.search[len(t.search)-1]
						if len(lastTerm) > 0 {
							t.search = append(t.search, []rune{})
						}
					}
					posDiff[0]++
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
				if t.curY != 0 {
					// TODO decouple cursor mutation from key handling
					t.curX = 0
					t.horizOffset = 0
				}
			case tcell.KeyCtrlE:
				// Go to end of line
				if t.curY != 0 {
					// TODO
					t.curX = len(t.curItem.Line)
					t.horizOffset = len(t.curItem.Line) - t.w
				}
			case tcell.KeyEscape:
				t.curY = 0 // TODO
			case tcell.KeyBackspace:
			case tcell.KeyBackspace2:
				if t.curY == reservedTopLines-1 {
					// Delete removes last item from last rune slice. If final slice is empty, remove that instead
					if len(t.search) > 0 {
						lastTerm := t.search[len(t.search)-1]
						if len(lastTerm) > 0 {
							lastTerm = lastTerm[:len(lastTerm)-1]
							t.search[len(t.search)-1] = lastTerm
						} else {
							t.search = t.search[:len(t.search)-1]
						}
					}
				} else if offsetX > 0 {
					// Only delete character if not in the first position
					newLine := []rune(t.curItem.Line)
					if len(newLine) > 0 {
						newLine = append(newLine[:offsetX-1], newLine[offsetX:]...)
						err := t.db.Update(string(newLine), t.curItem.Note, t.curItem)
						if err != nil {
							log.Fatal(err)
						}
						posDiff[0]--
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
				if t.curY == reservedTopLines-1 {
					if len(t.search) > 0 {
						lastTerm := t.search[len(t.search)-1]
						lastTerm = append(lastTerm, ev.Rune())
						t.search[len(t.search)-1] = lastTerm
					} else {
						var newTerm []rune
						newTerm = append(newTerm, ev.Rune())
						t.search = append(t.search, newTerm)
					}
				} else {
					newLine := []rune(t.curItem.Line)
					// Insert characters at position
					if len(newLine) == 0 || len(newLine) == offsetX {
						newLine = append(newLine, ev.Rune())
					} else {
						newLine = append(newLine, 0)
						copy(newLine[offsetX+1:], newLine[offsetX:])
						newLine[offsetX] = ev.Rune()
					}
					err := t.db.Update(string(newLine), t.curItem.Note, t.curItem)
					if err != nil {
						log.Fatal(err)
					}
				}
				posDiff[0]++
			}
		}
		t.s.Clear()

		// NOTE this is useful for debugging curItem setting
		//if t.curItem != nil {
		//    strID := fmt.Sprint(t.curItem.ID)
		//    emitStr(t.s, 0, t.h-1, t.style, strID)
		//}

		var cur *service.ListItem
		if t.curItem != nil {
			cur = t.curItem
		}
		matches, err = t.db.Match(t.search, cur)
		if err != nil {
			log.Println("stdin:", err)
			break
		}

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

		newXIdx := t.curX + posDiff[0]
		if isSearchLine {
			newXIdx = max(0, newXIdx) // Prevent index < 0
			// Add up max potential position based on number of runes in groups, and separators between
			lenSearchBox := max(0, len(t.search)-1) // Account for spaces between search groups
			for _, g := range t.search {
				lenSearchBox += len(g)
			}
			t.curX = min(newXIdx, lenSearchBox)
		} else {
			// Deal with horizontal offset if applicable
			if newXIdx < 0 {
				if t.horizOffset > 0 {
					t.horizOffset--
				}
				t.curX = max(0, newXIdx) // Prevent index < 0
			} else {
				if newXIdx > t.w-1 && t.horizOffset+t.w-1 < len(t.curItem.Line) {
					t.horizOffset++
				}
				newXIdx = min(newXIdx, len(t.curItem.Line)) // Prevent going out of range of the line
				t.curX = min(newXIdx, t.w-1)                // Prevent going out of range of the page
			}
		}

		t.paint(matches, triggerSaveWarning)
	}

	return nil
}
