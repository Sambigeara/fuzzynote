package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/gdamore/tcell"
	"github.com/gdamore/tcell/encoding"
	"github.com/mattn/go-runewidth"
	"github.com/micmonay/keybd_event"
)

var defStyle tcell.Style

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
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

func (p *List) buildSearchBox(s tcell.Screen, searchGroups [][]rune, style tcell.Style) {
	var pos, l int
	for _, key := range searchGroups {
		emitStr(s, pos, 0, style, string(key))
		l = len(key)
		pos = pos + l + 1 // Add a separator between groups with `+ 1`
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

func (c *cursor) goDown() {
	c.Y = min(c.Y+1, c.YMax)
}

func (c *cursor) goUp() {
	c.Y = max(c.Y-1, 0)
}

func (c *cursor) goRight() {
	c.X = min(c.X+1, c.XMax)
}

func (c *cursor) goLeft() {
	c.X = max(c.X-1, 0)
}

func (c *cursor) realignPos(keys [][]rune) {
	// Update cursor position if typing in search box
	newCurPos := len(keys) - 1 // Account for spaces between search groups
	for _, g := range keys {
		newCurPos += len(g)
	}
	c.X = newCurPos
	c.Y = 0
}

// HandleKeyPresses Read key presses on a loop
func (p *List) HandleKeyPresses() {

	encoding.Register()

	defStyle = tcell.StyleDefault.
		Background(tcell.ColorBlack).
		Foreground(tcell.ColorWhite)

	s := newInstantiatedScreen(defStyle)

	white := tcell.StyleDefault.
		Foreground(tcell.ColorWhite).Background(tcell.ColorGrey)

	w, h := s.Size()
	ecnt := 0
	ecntDt := time.Now()
	curs := cursor{
		XMax: w,
		YMax: h,
	}

	for {
		s.Show()
		ev := s.PollEvent()
		w, h = s.Size()
		curs.XMax = w
		curs.YMax = h
		search := &p.Search

		// https://github.com/gdamore/tcell/blob/master/_demos/mouse.go
		switch ev := ev.(type) {
		case *tcell.EventKey:
			if ev.Key() != tcell.KeyCtrlD || ecntDt.Add(time.Second*1).Before(time.Now()) {
				ecnt = 0
			}
			switch ev.Key() {
			case tcell.KeyCtrlC:
				s.Fini()
				p.StoreList()
				os.Exit(0)
			case tcell.KeyEnter:
				// Add a new item below current cursor position
				l := ListItem{}
				if curs.Y == 0 {
					p.ListItems = PrependListArray(p.ListItems, l)
				} else {
					newItemIdx := curs.Y - 1
					copy(p.ListItems[newItemIdx+2:], p.ListItems[newItemIdx+1:])
					p.ListItems[newItemIdx+1] = l
				}
				curs.goDown()
			case tcell.KeyCtrlD:
				if curs.Y != 0 {
					ecnt++
					ecntDt = time.Now()
					if ecnt > 1 {
						itemIdx := curs.Y - 1
						p.ListItems = append(p.ListItems[:itemIdx], p.ListItems[itemIdx+1:]...)
						ecnt = 0
					}
				}
			case tcell.KeyTab:
				if curs.Y == 0 {
					// If current search.Keys group has runes, close off and create new one
					if len(search.Keys) > 0 {
						lastTerm := search.Keys[len(search.Keys)-1]
						if len(lastTerm) > 0 {
							search.Keys = append(search.Keys, []rune{})
						}
					}
					curs.realignPos(search.Keys)
				}
			case tcell.KeyCtrlO:
				if curs.Y != 0 {
					s.Fini()
					openEditorSession()
					s = newInstantiatedScreen(defStyle)
				}
			case tcell.KeyEscape:
				if curs.Y == 0 {
					search.Keys = [][]rune{}
				}
				curs.X = 0
				curs.Y = 0
			case tcell.KeyBackspace:
			case tcell.KeyBackspace2:
				// Delete removes last item from last rune slice. If final slice is empty, remove that instead
				if len(search.Keys) > 0 {
					lastTerm := search.Keys[len(search.Keys)-1]
					if len(lastTerm) > 0 {
						lastTerm = lastTerm[:len(lastTerm)-1]
						search.Keys[len(search.Keys)-1] = lastTerm
					} else {
						search.Keys = search.Keys[:len(search.Keys)-1]
					}
				}
				//curs.realignPos(search.Keys)
			case tcell.KeyDown:
				curs.goDown()
			case tcell.KeyUp:
				curs.goUp()
			case tcell.KeyRight:
				curs.goRight()
			case tcell.KeyLeft:
				curs.goLeft()
			default:
				if curs.Y == 0 {
					if len(search.Keys) > 0 {
						lastTerm := search.Keys[len(search.Keys)-1]
						lastTerm = append(lastTerm, ev.Rune())
						search.Keys[len(search.Keys)-1] = lastTerm
					} else {
						var newTerm []rune
						newTerm = append(newTerm, ev.Rune())
						search.Keys = append(search.Keys, newTerm)
					}
				} else {
					// Retrieve item to update
					listItemIdx := curs.Y - 1
					updatedListItem := p.ListItems[listItemIdx]
					newLine := []rune(updatedListItem.Line)
					// Insert characters at position
					newCharIdx := curs.X
					copy(newLine[newCharIdx+1:], newLine[newCharIdx:])
					newLine[newCharIdx] = ev.Rune()
					p.ListItems[listItemIdx].Line = string(newLine)
					curs.goRight()
				}
			}
		}
		s.Clear()

		matches, err := p.FetchMatches()
		if err != nil {
			log.Println("stdin:", err)
			break
		}

		p.buildSearchBox(s, search.Keys, white)
		for i, r := range matches {
			emitStr(s, 0, i+1, defStyle, r.Line)
		}
		s.ShowCursor(curs.X, curs.Y)
	}
}
