package client

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	//"time"

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
	root    *service.ListItem // The top/youngest listItem
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
		Background(tcell.ColorBlack).
		Foreground(tcell.ColorWhite)

	s := newInstantiatedScreen(defStyle)

	w, h := s.Size()
	return &Terminal{
		db:    db,
		s:     s,
		style: tcell.StyleDefault.Background(tcell.ColorBlack).Foreground(tcell.ColorWhite),
		w:     w,
		h:     h,
	}
}

var defStyle tcell.Style

func min(a, b int) (int, bool) {
	// Returns `true` if first argument was smaller
	if a < b {
		return a, true
	}
	return b, false
}

func max(a, b int) (int, bool) {
	// Returns `true` if first argument was larger
	if a > b {
		return a, true
	}
	return b, false
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

func openEditorSession() {
	// TODO https://github.com/gdamore/tcell/issues/194
	sendExtraEventFix()
	//https://stackoverflow.com/questions/21513321/how-to-start-vim-from-go
	cmd := exec.Command("vim")
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err != nil {
		log.Printf("Command finished with error: %v", err)
	}
}

type cursor struct {
	X    int
	Y    int
	XMax int
	YMax int
}

func (t *Terminal) setMaxCurPos() {
	// Ignore if on search string
	if t.curY >= firstListLineIdx {
		t.curX, _ = min(t.curX, len(t.curItem.Line))
	}
}

func (t *Terminal) goDown() {
	newY, isChanged := min(t.curY+1, t.h-1)
	t.curY = newY
	if isChanged {
		if newY == firstListLineIdx {
			t.curItem = t.root
		} else {
			t.curItem = t.curItem.Parent
		}
	}
	t.setMaxCurPos()
}

func (t *Terminal) goUp() {
	newY, isChanged := max(t.curY-1, 0)
	t.curY = newY
	if isChanged {
		t.curItem = t.curItem.Child
	}
	t.setMaxCurPos()
}

func (t *Terminal) goRight() {
	t.curX, _ = min(t.curX+1, t.w-1)
	t.setMaxCurPos()
}

func (t *Terminal) goLeft() {
	t.curX, _ = max(t.curX-1, 0)
	t.setMaxCurPos()
}

func (t *Terminal) realignPos() {
	// Update cursor position if typing in search box
	newCurPos := len(t.search) - 1 // Account for spaces between search groups
	for _, g := range t.search {
		newCurPos += len(g)
	}
	t.curX = newCurPos
	t.curY = 0
}

func (t *Terminal) buildSearchBox(s tcell.Screen, style tcell.Style) {
	var pos, l int
	for _, key := range t.search {
		emitStr(s, pos, 0, style, string(key))
		l = len(key)
		pos = pos + l + 1 // Add a separator between groups with `+ 1`
	}
}

func (t *Terminal) resizeScreen() {
	w, h := t.s.Size()
	t.w = w
	t.h = h
}

func (t *Terminal) paint(cur *service.ListItem) error {
	t.buildSearchBox(t.s, t.style)

	if cur == nil {
		return nil
	}

	idx := firstListLineIdx
	for {
		emitStr(t.s, 0, idx, t.style, cur.Line)
		if cur.Parent == nil {
			break
		}
		cur = cur.Parent
		idx++
	}

	t.s.ShowCursor(t.curX, t.curY)
	return nil
}

// RunClient Read key presses on a loop
func (t *Terminal) RunClient() error {

	// TODO instantiate these elsewhere and put in a colour map
	//white := tcell.StyleDefault.
	//    Foreground(tcell.ColorWhite).Background(tcell.ColorGrey)

	// List instantiation
	root, err := t.db.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}
	// Initially set root to the absolute root
	t.root = root

	t.paint(t.root)

	for {
		t.s.Show()
		ev := t.s.PollEvent()

		// https://github.com/gdamore/tcell/blob/master/_demos/mouse.go
		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Key() {
			case tcell.KeyCtrlC:
				t.s.Fini()
				err := t.db.Save(t.root)
				if err != nil {
					log.Fatal(err)
				}
				os.Exit(0)
			case tcell.KeyEnter:
				// Add a new item below current cursor position
				var err error
				if t.curY == 0 {
					t.root, err = t.db.Add("", nil, t.root)
				} else {
					_, err = t.db.Add("", t.curItem, nil)
				}
				if err != nil {
					log.Fatal(err)
				}
				t.curX = 0
				t.goDown()
			case tcell.KeyCtrlD:
				if t.curY != 0 {
					var err error
					t.curItem, err = t.db.Delete(t.curItem)
					if err != nil {
						log.Fatal(err)
					}
					t.s.Clear()
					if t.curY == firstListLineIdx {
						// If deleting root, reset root
						t.root = t.curItem
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
					t.realignPos()
				}
			case tcell.KeyCtrlO:
				if t.curY != 0 {
					t.s.Fini()
					openEditorSession()
					t.s = newInstantiatedScreen(defStyle)
				}
			case tcell.KeyEscape:
				if t.curY == 0 {
					t.search = [][]rune{}
				}
				t.curX = 0
				t.curY = 0
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
					t.realignPos()
				} else if t.curX > 0 {
					newLine := []rune(t.curItem.Line)
					if len(newLine) > 0 {
						newLine = append(newLine[:t.curX-1], newLine[t.curX:]...)
						err := t.db.Update(string(newLine), t.curItem)
						if err != nil {
							log.Fatal(err)
						}
						t.goLeft()
					}
				}
			case tcell.KeyDown:
				t.goDown()
			case tcell.KeyUp:
				t.goUp()
			case tcell.KeyRight:
				t.goRight()
			case tcell.KeyLeft:
				t.goLeft()
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
					t.realignPos()
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
					t.goRight()
				}
			}
		}
		t.s.Clear()

		matchRoot, err := t.db.Match(t.search, t.root)
		if err != nil {
			log.Println("stdin:", err)
			break
		}

		t.paint(matchRoot)
	}

	return nil
}
