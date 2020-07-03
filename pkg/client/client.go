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

const firstListLineIdx = 1

type Terminal struct {
	db      service.ListRepo
	search  [][]rune
	curItem *service.ListItem // The currently selected item
	s       tcell.Screen
	style   tcell.Style
	w       int
	h       int
	curX    int
	curY    int
}

func NewTerm(db service.ListRepo) *Terminal {
	encoding.Register()

	defStyle = tcell.StyleDefault.
		Background(tcell.ColorWhite).
		Foreground(tcell.ColorBlack)

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

var defStyle tcell.Style

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

	dat, writeFn, err := t.db.EditPage(t.curItem.ID)

	// Write text to temp file
	tempFile := "/tmp/fzn_buffer"
	f, err := os.Create(tempFile)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	_, err = f.Write(*dat)
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

	return writeFn(&newDat)
}

func (t *Terminal) setMaxCurPos() {
	// Ignore if on search string
	if t.curY >= firstListLineIdx {
		t.curX = min(t.curX, len(t.curItem.Line))
	}
}

func (t *Terminal) realignPos() {
	// Update cursor position if typing in search box
	newCurPos := len(t.search) - 1 // Account for spaces between search groups
	for _, g := range t.search {
		newCurPos += len(g)
	}
	t.curX = newCurPos
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

func (t *Terminal) resizeScreen() {
	w, h := t.s.Size()
	t.w = w
	t.h = h
}

func (t *Terminal) paint(matches []*service.ListItem) error {
	t.buildSearchBox(t.s)

	for i, r := range matches {
		emitStr(t.s, 0, i+1, defStyle, r.Line)
	}

	t.s.ShowCursor(t.curX, t.curY)
	return nil
}

// RunClient Read key presses on a loop
func (t *Terminal) RunClient() error {

	// List instantiation
	var err error
	err = t.db.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

	matches, err := t.db.Match([][]rune{}, nil)
	if err != nil {
		log.Fatal(err)
	}

	t.paint(matches)

	for {
		posDiff := []int{0, 0} // x and y mutations to apply after db data mutations
		t.s.Show()
		ev := t.s.PollEvent()

		// https://github.com/gdamore/tcell/blob/master/_demos/mouse.go
		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Key() {
			case tcell.KeyCtrlX:
				t.s.Fini()
				err := t.db.Save()
				if err != nil {
					log.Fatal(err)
				}
				os.Exit(0)
			case tcell.KeyEnter:
				// Add a new item below current cursor position
				var err error
				if t.curY == 0 {
					if len(matches) == 0 {
						_, err = t.db.Add("", nil, true)
					} else {
						_, err = t.db.Add("", matches[0], true)
					}
				} else {
					_, err = t.db.Add("", t.curItem, false)
				}
				if err != nil {
					log.Fatal(err)
				}
				posDiff[1]++
				t.curX = 0
			case tcell.KeyCtrlD:
				if t.curY == 0 {
					t.search = [][]rune{}
				} else {
					// Retrieve the new curItem prior to deleting the existing curItem so we can set it afterwards
					err := t.db.Delete(t.curItem)
					if err != nil {
						log.Fatal(err)
					}
				}
			case tcell.KeyTab:
				if t.curY == 0 {
					// If current search group has runes, close off and create new one
					if len(t.search) > 0 {
						lastTerm := t.search[len(t.search)-1]
						if len(lastTerm) > 0 {
							t.search = append(t.search, []rune{})
						}
					}
				}
			case tcell.KeyCtrlO:
				if t.curY != 0 {
					t.s.Fini()
					err = t.openEditorSession()
					if err != nil {
						log.Fatal(err)
					}
					t.s = newInstantiatedScreen(defStyle)
				}
			case tcell.KeyEscape:
				t.curX = 0 // TODO
				t.curY = 0 // TODO
			case tcell.KeyBackspace:
			case tcell.KeyBackspace2:
				if t.curY == 0 {
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
				} else if t.curX > 0 {
					newLine := []rune(t.curItem.Line)
					if len(newLine) > 0 {
						newLine = append(newLine[:t.curX-1], newLine[t.curX:]...)
						err := t.db.Update(string(newLine), t.curItem)
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
				if t.curY == 0 {
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
					if len(newLine) == 0 || len(newLine) == t.curX {
						newLine = append(newLine, ev.Rune())
					} else {
						newLine = append(newLine, 0)
						copy(newLine[t.curX+1:], newLine[t.curX:])
						newLine[t.curX] = ev.Rune()
					}
					err := t.db.Update(string(newLine), t.curItem)
					if err != nil {
						log.Fatal(err)
					}
					posDiff[0]++
				}
			}
		}
		t.s.Clear()

		// NOTE this is useful for debugging curItem setting
		//if t.curItem != nil {
		//    strID := fmt.Sprint(t.curItem.ID)
		//    emitStr(t.s, 0, t.h-1, t.style, strID)
		//}

		matches, err = t.db.Match(t.search, t.curItem)
		if err != nil {
			log.Println("stdin:", err)
			break
		}

		// Change cursor position based on aggregated position increments
		// Neither can be less than 0. The upper bounds are limited by the screen size.
		t.curX = min(max(posDiff[0], 0), t.w-1)
		t.curY = min(max(posDiff[1], 0), t.h-1)

		emitStr(t.s, 0, t.h-1, t.style, fmt.Sprintf("HELLO %v", len(matches)))

		if t.curY == 0 {
			t.curItem = nil
		} else {
			maxIdx := len(matches) - 1
			if maxIdx >= 0 {
				t.curItem = matches[maxIdx]
			}
		}
		//if len(matches) > 0 && t.curY <= len(matches) {
		//    t.curItem = matches[t.curY-1]
		//} else {
		//    t.curItem = nil
		//    t.realignPos()
		//}

		t.setMaxCurPos()

		t.paint(matches)
		//fmt.Printf("HELLOOOO %v\n", t.curY)
	}

	return nil
}
