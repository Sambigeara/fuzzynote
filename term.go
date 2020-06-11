package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"

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

func (curs *cursor) realignPos(keys [][]rune) {
	// Update cursor position if typing in search box
	newCurPos := len(keys) - 1 // Account for spaces between search groups
	for _, g := range keys {
		newCurPos += len(g)
	}
	curs.X = newCurPos
	curs.Y = 0
}

func (p *List) HandleKeyPresses() {

	encoding.Register()

	defStyle = tcell.StyleDefault.
		Background(tcell.ColorBlack).
		Foreground(tcell.ColorWhite)

	s := newInstantiatedScreen(defStyle)

	white := tcell.StyleDefault.
		Foreground(tcell.ColorWhite).Background(tcell.ColorGrey)

	w, h := s.Size()
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
			s.SetContent(curs.XMax-1, curs.YMax-1, ev.Rune(), nil, defStyle)
			switch ev.Key() {
			case tcell.KeyCtrlC:
				s.Fini()
				os.Exit(0)
			case tcell.KeyEnter:
				if curs.Y == 0 {
					// If current search.Keys group has runes, close off and create new one
					if len(search.Keys) > 0 {
						lastTerm := search.Keys[len(search.Keys)-1]
						if len(lastTerm) > 0 {
							search.Keys = append(search.Keys, []rune{})
						}
					}
					curs.realignPos(search.Keys)
				} else {
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
				curs.realignPos(search.Keys)
			case tcell.KeyDown:
				curs.Y = min(curs.Y+1, curs.YMax)
			case tcell.KeyUp:
				curs.Y = max(curs.Y-1, 0)
			case tcell.KeyRight:
				curs.X = min(curs.X+1, curs.XMax)
			case tcell.KeyLeft:
				curs.X = max(curs.X-1, 0)
			default:
				if len(search.Keys) > 0 {
					lastTerm := search.Keys[len(search.Keys)-1]
					lastTerm = append(lastTerm, ev.Rune())
					search.Keys[len(search.Keys)-1] = lastTerm
				} else {
					var newTerm []rune
					newTerm = append(newTerm, ev.Rune())
					search.Keys = append(search.Keys, newTerm)
				}
				if curs.Y == 0 {
					curs.realignPos(search.Keys)
				}
			}
		}
		s.Clear()

		matches, err := p.FetchMatches(search.Keys)
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
