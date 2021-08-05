package term

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"

	"github.com/atotto/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/gdamore/tcell/v2/encoding"
	"github.com/mattn/go-runewidth"

	"github.com/sambigeara/fuzzynote/pkg/service"
)

const (
	reservedTopLines, reservedBottomLines = 1, 1
	reservedEndChars                      = 1
	emptySearchLinePrompt                 = "Search here..."
	searchGroupPrompt                     = "TAB: Create new search group"
	newLinePrompt                         = "Enter: Create new line"
)

type Terminal struct {
	db *service.DBListRepo
	c  *service.ClientBase

	S      tcell.Screen
	style  tcell.Style
	colour string
	Editor string

	previousKey tcell.Key // Keep track of the previous keypress

	//footerMessage     string    // Because we refresh on an ongoing basis, this needs to be emitted each time we paint
}

func NewTerm(db *service.DBListRepo, colour string, editor string) *Terminal {
	encoding.Register()

	defStyle := tcell.StyleDefault.
		Background(tcell.ColorWhite).
		Foreground(tcell.ColorBlack)

	if colour == "dark" {
		defStyle = defStyle.Reverse(true)
	}

	s := newInstantiatedScreen(defStyle)

	w, h := s.Size()
	t := Terminal{
		db:     db,
		c:      service.NewClientBase(db, w, h),
		S:      s,
		style:  defStyle,
		colour: colour,
		Editor: editor,
	}
	return &t
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

func (t *Terminal) buildSearchBox(s tcell.Screen) {
	// If no search items at all, display emptySearchLinePrompt and return
	if len(t.c.Search) == 0 || len(t.c.Search) == 1 && len(t.c.Search[0]) == 0 {
		emitStr(s, 0, 0, t.style.Dim(true), emptySearchLinePrompt)
	}

	searchStyle := tcell.StyleDefault.
		Foreground(tcell.ColorWhite).Background(tcell.ColorGrey)

	var pos, l int
	for _, key := range t.c.Search {
		emitStr(s, pos, 0, searchStyle, string(key))
		l = len(key)
		pos = pos + l + 1 // Add a separator between groups with `+ 1`
	}

	// Display `TAB` prompt after final search group if only one search group
	// +1 just to give some breathing room
	if len(t.c.Search) == 1 && len(t.c.Search[0]) > 0 && t.c.CurY == 0 {
		emitStr(s, pos+1, 0, t.style.Dim(true), searchGroupPrompt)
	}

	// Display whether all items or just non-hidden items are currently displayed
	if t.c.ShowHidden {
		indicator := "VIS"
		emitStr(s, t.c.W-len([]byte(indicator))+reservedEndChars, 0, searchStyle, indicator)
	} else {
		indicator := "HID"
		emitStr(s, t.c.W-len([]byte(indicator))+reservedEndChars, 0, searchStyle, indicator)
	}
}

func (t *Terminal) buildFooter(s tcell.Screen, text string) {
	footer := tcell.StyleDefault.
		Foreground(tcell.ColorBlack).Background(tcell.ColorYellow)

	// Pad out remaining line with spaces to ensure whole bar is filled
	lenStr := len([]rune(text))
	text += string(make([]rune, t.c.W-lenStr))
	// reservedBottomLines is subtracted from t.c.H globally, and we want to print on the bottom line
	// so add it back in here
	emitStr(s, 0, t.c.H-1+reservedBottomLines, footer, text)
}

func (t *Terminal) buildCollabDisplay(s tcell.Screen, collaborators map[tcell.Style]string) {
	x := 0
	for style, collabStr := range collaborators {
		emitStr(s, x, t.c.H-1+reservedBottomLines, style, collabStr)
		x += len(collabStr)
	}
}

func (t *Terminal) resizeScreen() {
	w, h := t.S.Size()
	t.c.W = w - reservedEndChars
	t.c.H = h - reservedBottomLines
}

// A selection of colour combos to apply to collaborators
var (
	collabStyleCombos []tcell.Style = []tcell.Style{
		tcell.StyleDefault.
			Background(tcell.Color25).
			Foreground(tcell.ColorWhite),
		tcell.StyleDefault.
			Background(tcell.Color90).
			Foreground(tcell.ColorWhite),
		tcell.StyleDefault.
			Background(tcell.Color124).
			Foreground(tcell.ColorWhite),
		tcell.StyleDefault.
			Background(tcell.Color126).
			Foreground(tcell.ColorWhite),
		tcell.StyleDefault.
			Background(tcell.Color136).
			Foreground(tcell.ColorWhite),
		tcell.StyleDefault.
			Background(tcell.Color166).
			Foreground(tcell.ColorWhite),
	}
	collabStyleIncStart = rand.Intn(len(collabStyleCombos))
)

func (t *Terminal) paint(matches []service.ListItem, saveWarning bool) error {
	t.S.Clear()
	t.resizeScreen()

	// Get collaborator map
	collabMap := t.db.GetCollabPositions()

	// Build top search box
	t.buildSearchBox(t.S)

	// Store comma separated strings of collaborator emails against the style
	collaborators := make(map[tcell.Style]string)

	// Randomise the starting colour index for bants
	collabStyleInc := collabStyleIncStart
	offset := 0
	for i, r := range matches[t.c.VertOffset:service.Min(len(matches), t.c.VertOffset+t.c.H-reservedTopLines)] {
		style := t.style
		offset = i + reservedTopLines

		// Get current collaborators on item, if any
		lineCollabers := collabMap[r.Key()]

		// Mutually exclusive style triggers
		if _, ok := t.c.SelectedItems[i]; ok {
			// Currently selected with Ctrl-S
			// By default, we reverse the colourscheme for "dark" settings, so undo the
			// reversal, to reverse again...
			if t.colour == "light" {
				style = style.Reverse(true)
			} else if t.colour == "dark" {
				style = style.Reverse(false)
			}
		} else if len(lineCollabers) > 0 {
			// If collaborators are on line
			style = collabStyleCombos[collabStyleInc%len(collabStyleCombos)]
			collaborators[style] = strings.Join(lineCollabers, ",")
			collabStyleInc++
		}

		if r.Note != nil && len(*(r.Note)) > 0 {
			style = style.Underline(true).Bold(true)
		}

		if r.IsHidden {
			style = style.Dim(true)
		}

		line := t.c.TrimPrefix(r.Line)

		// Account for horizontal offset if on curItem
		if i == t.c.CurY-reservedTopLines {
			if len(line) > 0 {
				line = line[t.c.HorizOffset:]
			}
		}

		// Emit line
		emitStr(t.S, 0, offset, style, line)

		if offset == t.c.H {
			break
		}
	}

	// If no matches, display help prompt on first line
	// TODO ordering
	if len(matches) == 0 {
		if len(t.c.Search) > 0 && len(t.c.Search[0]) > 0 {
			newLinePrefixPrompt := fmt.Sprintf("Enter: Create new line with search prefix: \"%s\"", service.GetNewLinePrefix(t.c.Search))
			emitStr(t.S, 0, reservedTopLines, t.style.Dim(true), newLinePrefixPrompt)
		} else {
			emitStr(t.S, 0, reservedTopLines, t.style.Dim(true), newLinePrompt)
		}
	}

	// Show active collaborators
	if len(collaborators) > 0 {
		t.buildCollabDisplay(t.S, collaborators)
	}

	//if t.footerMessage != "" {
	//    t.buildFooter(t.S, t.footerMessage)
	//}

	t.S.ShowCursor(t.c.CurX, t.c.CurY)
	t.S.Show()

	return nil
}

func (t *Terminal) openEditorSession() error {
	// Write text to temp file
	tmpfile, err := ioutil.TempFile("", "fzn_buffer")
	if err != nil {
		log.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	var note []byte
	if t.c.CurItem.Note != nil {
		note = *t.c.CurItem.Note
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
		//t.footerMessage = fmt.Sprintf("Unable to open Note using editor setting : \"%s\"", t.Editor)
	}

	// Read back from the temp file, and return to the write function
	newDat, err := ioutil.ReadFile(tmpfile.Name())
	if err != nil {
		log.Fatal(err)
		return nil
	}

	err = t.db.Update("", &newDat, t.c.CurY-reservedTopLines)
	if err != nil {
		log.Fatal(err)
	}

	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (t *Terminal) AwaitEvent() interface{} {
	return t.S.PollEvent()
}

func (t *Terminal) HandleEvent(ev interface{}) (bool, error) {
	interactionEvent := service.InteractionEvent{}
	switch ev := ev.(type) {
	case *tcell.EventKey:
		switch ev.Key() {
		case tcell.KeyEscape:
			interactionEvent.T = service.KeyEscape
			if t.previousKey == tcell.KeyEscape {
				t.S.Fini()
				return false, nil
			}
		case tcell.KeyEnter:
			interactionEvent.T = service.KeyEnter
		case tcell.KeyCtrlD:
			interactionEvent.T = service.KeyDeleteItem
		case tcell.KeyCtrlO:
			//interactionEvent.T = service.KeyOpenNote
			if t.c.CurY+t.c.VertOffset != 0 {
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
			interactionEvent.T = service.KeyGotoStart
		case tcell.KeyCtrlE:
			interactionEvent.T = service.KeyGotoEnd
		case tcell.KeyCtrlV:
			interactionEvent.T = service.KeyVisibility
		case tcell.KeyCtrlU:
			interactionEvent.T = service.KeyUndo
		case tcell.KeyCtrlR:
			interactionEvent.T = service.KeyRedo
		case tcell.KeyCtrlC:
			if url := service.MatchFirstURL(t.c.CurItem.Line); url != "" {
				clipboard.WriteAll(url)
			}
			interactionEvent.T = service.KeyCopy
		case tcell.KeyCtrlUnderscore:
			interactionEvent.T = service.KeyOpenURL
		case tcell.KeyCtrlCarat:
			interactionEvent.T = service.KeyExport
		case tcell.KeyCtrlP:
			interactionEvent.T = service.KeyPaste
		case tcell.KeyCtrlS:
			interactionEvent.T = service.KeySelect
		case tcell.KeyTab:
			interactionEvent.T = service.KeyAddSearchGroup
		case tcell.KeyBackspace:
			fallthrough
		case tcell.KeyBackspace2:
			interactionEvent.T = service.KeyBackspace
		case tcell.KeyDelete:
			interactionEvent.T = service.KeyDelete
		case tcell.KeyPgUp:
			interactionEvent.T = service.KeyMoveItemUp
		case tcell.KeyPgDn:
			interactionEvent.T = service.KeyMoveItemDown
		case tcell.KeyDown:
			interactionEvent.T = service.KeyCursorDown
		case tcell.KeyUp:
			interactionEvent.T = service.KeyCursorUp
		case tcell.KeyRight:
			interactionEvent.T = service.KeyCursorRight
		case tcell.KeyLeft:
			interactionEvent.T = service.KeyCursorLeft
		default:
			interactionEvent.T = service.KeyRune
			interactionEvent.R = ev.Rune()
			//t.footerMessage = ""
		}
		t.previousKey = ev.Key()
	}

	matches, cont, err := t.c.HandleInteraction(interactionEvent)
	if err != nil {
		return cont, err
	}
	if !cont {
		return false, nil
	}

	t.paint(matches, false)

	return true, nil
}
