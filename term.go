package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gdamore/tcell"
	"github.com/gdamore/tcell/encoding"
	"github.com/mattn/go-runewidth"
)

var defStyle tcell.Style

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

func (p *List) HandleKeyPresses() {

	encoding.Register()

	s, e := tcell.NewScreen()
	if e != nil {
		fmt.Fprintf(os.Stderr, "%v\n", e)
		os.Exit(1)
	}
	if e := s.Init(); e != nil {
		fmt.Fprintf(os.Stderr, "%v\n", e)
		os.Exit(1)
	}
	defStyle = tcell.StyleDefault.
		Background(tcell.ColorBlack).
		Foreground(tcell.ColorWhite)
	s.SetStyle(defStyle)
	s.EnableMouse()
	s.Clear()

	white := tcell.StyleDefault.
		Foreground(tcell.ColorBlack).Background(tcell.ColorWhite)

	w, h := s.Size()

	for {
		s.Show()
		ev := s.PollEvent()
		w, h = s.Size()
		search := &p.Search

		// https://github.com/gdamore/tcell/blob/master/_demos/mouse.go
		switch ev := ev.(type) {
		case *tcell.EventKey:
			s.SetContent(w-1, h-1, ev.Rune(), nil, defStyle)
			switch ev.Key() {
			case tcell.KeyCtrlC:
				s.Fini()
				os.Exit(0)
			case tcell.KeyEnter:
				// If current search.Keys group has runes, close off and create new one
				if len(search.Keys) > 0 {
					lastTerm := search.Keys[len(search.Keys)-1]
					if len(lastTerm) > 0 {
						search.Keys = append(search.Keys, []rune{})
					}
				}
			case tcell.KeyEscape:
				search.Keys = [][]rune{}
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
	}
}
