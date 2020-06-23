package client

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"fuzzy-note/pkg/service"
	//"github.com/Sambigeara/fuzzy-note/pkg/service"

	"github.com/gdamore/tcell"
	"github.com/gdamore/tcell/encoding"
	"github.com/mattn/go-runewidth"
	"github.com/micmonay/keybd_event"
)

type Terminal struct {
	db service.ListRepo
}

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

func NewTerm(db service.ListRepo) *Terminal {
	return &Terminal{db}
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

func buildSearchBox(s tcell.Screen, searchGroups [][]rune, style tcell.Style) {
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

// RunClient Read key presses on a loop
func (term *Terminal) RunClient() error {

	listItems, err := term.db.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

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
	var search [][]rune

	for {
		s.Show()
		ev := s.PollEvent()
		w, h = s.Size()
		curs.XMax = w
		curs.YMax = h

		// https://github.com/gdamore/tcell/blob/master/_demos/mouse.go
		switch ev := ev.(type) {
		case *tcell.EventKey:
			if ev.Key() != tcell.KeyCtrlD || ecntDt.Add(time.Second*1).Before(time.Now()) {
				ecnt = 0
			}
			switch ev.Key() {
			case tcell.KeyCtrlC:
				s.Fini()
				err := term.db.Save(listItems)
				if err != nil {
					log.Fatal(err)
				}
				os.Exit(0)
			case tcell.KeyEnter:
				// Add a new item below current cursor position
				var err error
				listItems, err = term.db.Add("", curs.Y, &listItems)
				if err != nil {
					log.Fatal(err)
				}
				curs.X = 0
				curs.goDown()
			case tcell.KeyCtrlD:
				if curs.Y != 0 {
					ecnt++
					ecntDt = time.Now()
					if ecnt > 1 {
						var err error
						listItems, err = term.db.Delete(curs.Y-1, &listItems)
						s.Clear()
						if err != nil {
							log.Fatal(err)
						}
						ecnt = 0
					}
				}
			case tcell.KeyTab:
				if curs.Y == 0 {
					// If current search group has runes, close off and create new one
					if len(search) > 0 {
						lastTerm := search[len(search)-1]
						if len(lastTerm) > 0 {
							search = append(search, []rune{})
						}
					}
					curs.realignPos(search)
				}
			case tcell.KeyCtrlO:
				if curs.Y != 0 {
					s.Fini()
					openEditorSession()
					s = newInstantiatedScreen(defStyle)
				}
			case tcell.KeyEscape:
				if curs.Y == 0 {
					search = [][]rune{}
				}
				curs.X = 0
				curs.Y = 0
			case tcell.KeyBackspace:
			case tcell.KeyBackspace2:
				if curs.Y == 0 {
					// Delete removes last item from last rune slice. If final slice is empty, remove that instead
					if len(search) > 0 {
						lastTerm := search[len(search)-1]
						if len(lastTerm) > 0 {
							lastTerm = lastTerm[:len(lastTerm)-1]
							search[len(search)-1] = lastTerm
						} else {
							search = search[:len(search)-1]
						}
					}
					curs.realignPos(search)
				} else if curs.X > 0 {
					// Retrieve item to update
					listItemIdx := curs.Y - 1
					updatedListItem := listItems[listItemIdx]
					newLine := []rune(updatedListItem.Line)
					if len(newLine) > 0 {
						newLine = append(newLine[:curs.X-1], newLine[curs.X:]...)
						listItems[listItemIdx].Line = string(newLine)
						err := term.db.Update(string(newLine), listItemIdx, &listItems)
						if err != nil {
							log.Fatal(err)
						}
						curs.goLeft()
					}
				}
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
					if len(search) > 0 {
						lastTerm := search[len(search)-1]
						lastTerm = append(lastTerm, ev.Rune())
						search[len(search)-1] = lastTerm
					} else {
						var newTerm []rune
						newTerm = append(newTerm, ev.Rune())
						search = append(search, newTerm)
					}
					curs.realignPos(search)
				} else {
					// Retrieve item to update
					listItemIdx := curs.Y - 1
					newLine := []rune(listItems[listItemIdx].Line)
					// Insert characters at position
					if len(newLine) == 0 || len(newLine) == curs.X {
						newLine = append(newLine, ev.Rune())
					} else {
						newLine = append(newLine, 0)
						copy(newLine[curs.X+1:], newLine[curs.X:])
						newLine[curs.X] = ev.Rune()
					}
					err := term.db.Update(string(newLine), listItemIdx, &listItems)
					if err != nil {
						log.Fatal(err)
					}
					//listItems[listItemIdx].Line = string(newLine)
					curs.goRight()
				}
			}
		}
		s.Clear()

		matches, err := term.db.Match(search, &listItems)
		if err != nil {
			log.Println("stdin:", err)
			break
		}

		buildSearchBox(s, search, white)
		for i, r := range matches {
			emitStr(s, 0, i+1, defStyle, r.Line)
		}
		s.ShowCursor(curs.X, curs.Y)
	}

	return nil
}
