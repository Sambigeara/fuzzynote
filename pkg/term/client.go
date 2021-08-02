package term

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/gdamore/tcell/v2/encoding"
	"github.com/mattn/go-runewidth"

	"github.com/sambigeara/fuzzynote/pkg/service"
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
	db *service.DBListRepo

	//search            [][]rune
	//matches           []service.ListItem
	//curItem           *service.ListItem // The currently selected item

	S      tcell.Screen
	style  tcell.Style
	colour string
	Editor string

	//w, h              int
	//curX, curY        int // Cur "screen" index, not related to matched item lists
	//vertOffset        int // The index of the first displayed item in the match set
	//horizOffset       int // The index of the first displayed char in the curItem
	//showHidden        bool
	//selectedItems     map[int]string // struct{} is more space efficient than bool
	//copiedItem        *service.ListItem
	//hiddenMatchPrefix string    // The common string that we want to truncate from each line

	previousKey tcell.Key // Keep track of the previous keypress

	//footerMessage     string    // Because we refresh on an ongoing basis, this needs to be emitted each time we paint

	C *service.ClientBase
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

	showHidden := false

	matches, _, err := db.Match([][]rune{}, showHidden, "", 0, 0)
	if err != nil {
		log.Fatal(err)
	}

	w, h := s.Size()
	t := Terminal{
		db:     db,
		C:      service.NewClientBase(db, matches, w, h),
		S:      s,
		style:  defStyle,
		colour: colour,
		Editor: editor,
		//w:             w,
		//h:             h,
		//showHidden:    showHidden,
		//selectedItems: make(map[int]string),
		//matches:       matches,
	}
	t.paint(matches, false)
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
	if len(t.C.Search) == 0 || len(t.C.Search) == 1 && len(t.C.Search[0]) == 0 {
		emitStr(s, 0, 0, t.style.Dim(true), emptySearchLinePrompt)
	}

	searchStyle := tcell.StyleDefault.
		Foreground(tcell.ColorWhite).Background(tcell.ColorGrey)

	var pos, l int
	for _, key := range t.C.Search {
		emitStr(s, pos, 0, searchStyle, string(key))
		l = len(key)
		pos = pos + l + 1 // Add a separator between groups with `+ 1`
	}

	// Display `TAB` prompt after final search group if only one search group
	// +1 just to give some breathing room
	if len(t.C.Search) == 1 && len(t.C.Search[0]) > 0 && t.C.CurY == 0 {
		emitStr(s, pos+1, 0, t.style.Dim(true), searchGroupPrompt)
	}

	// Display whether all items or just non-hidden items are currently displayed
	if t.C.ShowHidden {
		indicator := "VIS"
		emitStr(s, t.C.W-len([]byte(indicator))+reservedEndChars, 0, searchStyle, indicator)
	} else {
		indicator := "HID"
		emitStr(s, t.C.W-len([]byte(indicator))+reservedEndChars, 0, searchStyle, indicator)
	}
}

func (t *Terminal) buildFooter(s tcell.Screen, text string) {
	footer := tcell.StyleDefault.
		Foreground(tcell.ColorBlack).Background(tcell.ColorYellow)

	// Pad out remaining line with spaces to ensure whole bar is filled
	lenStr := len([]rune(text))
	text += string(make([]rune, t.C.W-lenStr))
	// reservedBottomLines is subtracted from t.C.H globally, and we want to print on the bottom line
	// so add it back in here
	emitStr(s, 0, t.C.H-1+reservedBottomLines, footer, text)
}

func (t *Terminal) buildCollabDisplay(s tcell.Screen, collaborators map[tcell.Style]string) {
	x := 0
	for style, collabStr := range collaborators {
		emitStr(s, x, t.C.H-1+reservedBottomLines, style, collabStr)
		x += len(collabStr)
	}
}

func (t *Terminal) resizeScreen() {
	w, h := t.S.Size()
	t.C.W = w - reservedEndChars
	t.C.H = h - reservedBottomLines
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
	// Get collaborator map
	collabMap := t.db.GetCollabPositions()

	// Build top search box
	t.buildSearchBox(t.S)

	// Store comma separated strings of collaborator emails against the style
	collaborators := make(map[tcell.Style]string)

	// Randomise the starting colour index for bants
	collabStyleInc := collabStyleIncStart
	offset := 0
	for i, r := range matches[t.C.VertOffset:service.Min(len(matches), t.C.VertOffset+t.C.H-reservedTopLines)] {
		style := t.style
		offset = i + reservedTopLines

		// Get current collaborators on item, if any
		lineCollabers := collabMap[r.Key()]

		// Mutually exclusive style triggers
		if _, ok := t.C.SelectedItems[i]; ok {
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

		line := r.Line

		// Truncate the full search string from any lines matching the entire thing,
		// ignoring search operators
		// Op needs to be case-insensitive, but must not mutate underlying line
		if strings.HasPrefix(strings.ToLower(line), t.C.HiddenMatchPrefix) {
			line = string([]rune(line)[len([]byte(t.C.HiddenMatchPrefix)):])
		}
		// If we strip the match prefix, and there is a space remaining, trim that too
		line = strings.TrimPrefix(line, " ")

		// Account for horizontal offset if on curItem
		if i == t.C.CurY-reservedTopLines {
			if len(line) > 0 {
				line = line[t.C.HorizOffset:]
			}
		}

		// Emit line
		emitStr(t.S, 0, offset, style, line)

		if offset == t.C.H {
			break
		}
	}

	// If no matches, display help prompt on first line
	// TODO ordering
	if len(matches) == 0 {
		if len(t.C.Search) > 0 && len(t.C.Search[0]) > 0 {
			newLinePrefixPrompt := fmt.Sprintf("Enter: Create new line with search prefix: \"%s\"", service.GetNewLinePrefix(t.C.Search))
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

	t.S.ShowCursor(t.C.CurX, t.C.CurY)
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
			interactionEvent.T = service.KeyOpenNote
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
	t.S.Clear()

	matches, cont, err := t.C.HandleInteraction(interactionEvent)
	if err != nil {
		return cont, err
	}
	if !cont {
		return false, nil
	}

	t.resizeScreen()
	t.paint(matches, false)
	t.S.Show()

	return true, nil
}
