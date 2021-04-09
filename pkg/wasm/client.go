package web

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	//"unicode"

	"github.com/maxence-charriere/go-app/v8/pkg/app"

	"fuzzynote/pkg/service"
)

const (
	searchGroupKey = "search:%d"
	mainKey        = "main:%d"
)

var (
	tempBlock chan struct{}
)

type browserWalFile struct {
	//RefreshTicker            *time.Ticker
	//GatherTicker             *time.Ticker
	//processedPartialWals     map[string]struct{}
	//processedPartialWalsLock *sync.Mutex
	mode               service.Mode
	pushMatchTerm      []rune
	processedEventLock *sync.Mutex
	processedEventMap  map[string]struct{}
}

func newBrowserWalFile() *browserWalFile {
	// TODO Retrieve config from localStorage
	// TODO if not present, retrieve from lambda
	return &browserWalFile{
		mode:               service.Sync,
		pushMatchTerm:      []rune{},
		processedEventLock: &sync.Mutex{},
		processedEventMap:  make(map[string]struct{}),
	}
}

// TODO these are mostly just stub functions for now to satisfy the interface
func (wf *browserWalFile) GetRootDir() string { return "" }
func (wf *browserWalFile) GetFileNamesMatchingPattern(pattern string) ([]string, error) {
	return []string{}, nil
}
func (wf *browserWalFile) GenerateLogFromFile(fileName string) ([]service.EventLog, error) {
	return []service.EventLog{}, nil
}
func (wf *browserWalFile) RemoveFile(fileName string) error             { return nil }
func (wf *browserWalFile) Flush(b *bytes.Buffer, fileName string) error { return nil }
func (wf *browserWalFile) SetProcessedPartialWals(partialWal string)    {}
func (wf *browserWalFile) IsPartialWalProcessed(partialWal string) bool { return false }
func (wf *browserWalFile) AwaitPull()                                   { <-tempBlock }
func (wf *browserWalFile) AwaitGather()                                 { <-tempBlock }
func (wf *browserWalFile) StopTickers()                                 {}
func (wf *browserWalFile) GetMode() service.Mode                        { return wf.mode }
func (wf *browserWalFile) GetPushMatchTerm() []rune                     { return wf.pushMatchTerm }
func (wf *browserWalFile) SetProcessedEvent(key string)                 {}
func (wf *browserWalFile) IsEventProcessed(key string) bool             { return true }

// Page is the main component encompassing the whole app
type Page struct {
	app.Compo

	db *service.DBListRepo

	curIdx            int
	curSearchGroupIdx int
	curIdxKey         string
	SearchGroups      []string
	ListItems         []service.ListItem // Public attribute to allow for updates https://go-app.dev/components#exported-fields
	walChan           chan *[]service.EventLog
}

func (p *Page) Render() app.UI {
	return app.Div().
		Class("container").
		Body(
			app.Div().
				Class("matchgroup").
				ID(fmt.Sprintf(mainKey, -1)).
				// OnKeyDown is here so we get the correct key on "Enter" within `handleNav`
				OnKeyDown(p.handleNav).
				Body(
					app.Range(p.SearchGroups).Slice(func(i int) app.UI {
						return app.Input().
							ID(fmt.Sprintf(searchGroupKey, i)).
							Value(p.SearchGroups[i]).
							Class("matchitem").
							Placeholder("Search here...").
							OnInput(p.handleMatchChange).
							OnClick(p.focus)
					})),
			app.Div().
				Class("listgroup").
				Body(
					app.Range(p.ListItems).Slice(func(i int) app.UI {
						return app.Input().
							ID(fmt.Sprintf(mainKey, i)).
							Value((p.ListItems)[i].Line).
							Class("listitem").
							Placeholder("Type something...").
							OnInput(p.handleListItemChange).
							OnKeyDown(p.handleNav).
							OnClick(p.focus)
						//return app.Span().
						//    ContentEditable(true).
						//    ID(strconv.Itoa(i)).
						//    Text((p.ListItems)[i].Line).
						//    OnInput(p.handleListItemChange).
						//    OnKeyDown(p.handleNav).
						//    OnClick(p.focus)
					})),
		)
}

func (p *Page) loadDB() {
	// TODO from config
	localRefreshFrequency := uint16(1000)

	wf := newBrowserWalFile()
	p.db = service.NewDBListRepo("", wf, localRefreshFrequency)
	p.db.RegisterWalFile(wf)

	remotes := service.GetRemotesConfig("")
	ws := service.NewWebsocketTarget(remotes.Websocket)
	p.db.RegisterWebsocket(ws)
}

func (p *Page) getRuneSearchGroups() [][]rune {
	runeSearchGroups := [][]rune{}
	for _, s := range p.SearchGroups {
		runeSearchGroups = append(runeSearchGroups, []rune(s))
	}
	return runeSearchGroups
}

func (p *Page) OnMount(ctx app.Context) {
	p.loadDB()

	p.curIdx = -1 // Default to search line

	// Pre-instatiate a single empty search group
	p.SearchGroups = append(p.SearchGroups, "")

	// TODO remove this, just blocking pull/push on the browserWalFile for now
	if tempBlock != nil {
		tempBlock = make(chan struct{})
	}

	p.walChan = make(chan *[]service.EventLog)
	if err := p.db.StartWeb(p.walChan); err != nil {
		log.Fatal(err)
	}

	ctx.Async(func() {
		for {
			partialWal := <-p.walChan
			if err := p.db.Replay(partialWal); err != nil {
				log.Fatal(err)
			}

			runeSearchGroups := p.getRuneSearchGroups()
			p.ListItems, p.curIdx, _ = p.db.Match(runeSearchGroups, false, p.curIdxKey)

			key := ""
			if p.curIdx == -1 {
				key = fmt.Sprintf(searchGroupKey, p.curSearchGroupIdx)
			} else {
				key = fmt.Sprintf(mainKey, p.curIdx)
			}
			if elem := app.Window().GetElementByID(key); !elem.IsNull() {
				elem.Call("focus")
			}

			//if p.curIdx > -1 {
			//    if elem := app.Window().GetElementByID(fmt.Sprintf(mainKey, p.curIdx)); !elem.IsNull() {
			//        elem.Call("focus")
			//    }
			//}

			// TODO Temp "hack" to bring html state back in line with JS (probably specifically
			// for input text)
			// At the moment, adding a line onto the bottom will not move the cursor down (another
			// side affect of this issue, perhaps)
			for i, item := range p.ListItems {
				if elem := app.Window().GetElementByID(fmt.Sprintf(mainKey, i)); !elem.IsNull() {
					elem.Set("value", item.Line)
				}
			}
			p.Update()
		}
	})
}

func (p *Page) OnDismount(ctx app.Context) {
	err := p.db.Stop()
	if err != nil {
		log.Fatal(err)
	}
}

func (p *Page) getKey(idx int) string {
	key := ""
	if idx >= 0 && idx < len(p.ListItems) {
		key = p.ListItems[idx].Key()
	}
	return key
}

func (p *Page) focus(ctx app.Context, e app.Event) {
	id := ctx.JSSrc.Get("id").String()
	var idx int
	// TODO dedup
	if p.curIdx == -1 {
		fmt.Sscanf(id, searchGroupKey, &idx)
		p.curSearchGroupIdx = idx
	} else {
		fmt.Sscanf(id, mainKey, &idx)
	}

	p.curIdxKey = p.getKey(idx)
}

func (p *Page) handleMatchChange(ctx app.Context, e app.Event) {
	s := ctx.JSSrc.Get("value").String()

	// Retrieve idx from key
	id := ctx.JSSrc.Get("id").String()
	var idx int
	fmt.Sscanf(id, searchGroupKey, &idx)
	p.SearchGroups[idx] = s // Name field is modified

	p.walChan <- &[]service.EventLog{}
}

func (p *Page) handleListItemChange(ctx app.Context, e app.Event) {
	s := ctx.JSSrc.Get("value").String()

	// Retrieve idx from key
	id := ctx.JSSrc.Get("id").String()
	var idx int
	fmt.Sscanf(id, mainKey, &idx)
	p.db.Update(s, nil, idx)
}

// TODO figure out more appropriate function name
func (p *Page) handleNav(ctx app.Context, e app.Event) {
	key := e.Get("key").String()

	// TODO helper method to dedup
	// Retrieve idx from key
	id := ctx.JSSrc.Get("id").String()
	var idx int
	fmt.Sscanf(id, mainKey, &idx)

	// TODO windows/firefox compat https://developer.mozilla.org/en-US/docs/Web/API/KeyboardEvent/metaKey
	// Handle undo/redo on meta-z, meta-shift-z
	if e.Get("metaKey").Bool() && key == "z" {
		if !e.Get("shiftKey").Bool() {
			p.curIdxKey, _ = p.db.Undo()
		} else {
			p.curIdxKey, _ = p.db.Redo()
		}
	} else if e.Get("ctrlKey").Bool() {
		switch key {
		case "d":
			e.PreventDefault()
			if p.curIdx == -1 {
				if len(p.SearchGroups) > 1 {
					// If there's more than one search group, delete it
					p.SearchGroups = append(p.SearchGroups[:p.curSearchGroupIdx], p.SearchGroups[p.curSearchGroupIdx+1:]...)
					if p.curSearchGroupIdx > 0 {
						p.curSearchGroupIdx--
					}
				}
			} else if p.curIdx == len(p.ListItems)-1 {
				// If on lowest line, we want to move to the pre-delete child, which the `Delete`
				// function returns to us in `curIdxKey`
				p.curIdxKey, _ = p.db.Delete(idx)
			} else if p.curIdx >= 0 {
				p.db.Delete(idx)
				p.curIdxKey = p.getKey(p.curIdx + 1)
			}
		default:
			return
		}
	} else {
		switch key {
		case "Enter":
			newString := p.db.GetNewLinePrefix(p.getRuneSearchGroups())
			// TODO not currently going to end of line
			p.curIdxKey, _ = p.db.Add(newString, nil, idx+1)
		case "Tab":
			// We rely on default browser behaviour for now to move between search groups with
			// Tab. We want to preventDefault on a Shift-Tab press so it avoids creating a new
			// search group before going back one group
			e.PreventDefault()
			if e.Get("shiftKey").Bool() && p.curSearchGroupIdx > 0 {
				p.curSearchGroupIdx--
			} else if !e.Get("shiftKey").Bool() && p.curIdx == -1 {
				if p.curSearchGroupIdx == len(p.SearchGroups)-1 {
					p.SearchGroups = append(p.SearchGroups, "")
					//p.Update()
				}
				p.curSearchGroupIdx++
			}
		case "Backspace":
			if p.curIdx == -1 {
				if len(p.SearchGroups) > 1 && len(p.SearchGroups[p.curSearchGroupIdx]) == 0 && p.curSearchGroupIdx > 0 {
					p.SearchGroups = append(p.SearchGroups[:p.curSearchGroupIdx], p.SearchGroups[p.curSearchGroupIdx+1:]...)
					// We already check that it's above 0 above
					p.curSearchGroupIdx--
				}
			} else if p.curIdx < len(p.ListItems) && len(p.ListItems[p.curIdx].Line) == 0 {
				// If the current line is empty, backspace will delete it
				p.curIdxKey, _ = p.db.Delete(idx)
			}
		case "ArrowUp":
			if p.curIdx >= 0 {
				p.curIdxKey = p.getKey(p.curIdx - 1)
			}
		case "ArrowDown":
			if p.curIdx < len(p.ListItems)-1 {
				p.curIdxKey = p.getKey(p.curIdx + 1)
			}
		default:
			return
		}
	}
	// We only want to add an event on keys handled in this function
	// We also need to explicitly update state changes here as these are required prior to updates in the main loop
	p.Update()
	p.walChan <- &[]service.EventLog{}
}

//func (p *Page) setCursorPos(elem app.Value) {
//    r := app.Window().Get("document").Call("createRange")
//    s := app.Window().Call("getSelection")

//    r.Call("setStart", elem.Get("firstChild"), "0")
//    r.Call("collapse", true)

//    s.Call("removeAllRanges")
//    s.Call("addRange", r)
//}
