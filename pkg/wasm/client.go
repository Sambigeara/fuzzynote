package web

import (
	"bytes"
	//"fmt"
	"log"
	"strconv"
	"sync"
	//"unicode"

	"github.com/maxence-charriere/go-app/v8/pkg/app"

	"fuzzynote/pkg/service"
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

	curIdx    int
	curKey    string
	match     string
	ListItems []service.ListItem // Public attribute to allow for updates https://go-app.dev/components#exported-fields
	walChan   chan *[]service.EventLog
}

type changeEvent struct {
	ctx app.Context
	e   app.Event
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

func (p *Page) OnMount(ctx app.Context) {
	p.loadDB()

	p.curIdx = -1 // Default to search line

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

			p.ListItems, p.curIdx, _ = p.db.Match([][]rune{[]rune(p.match)}, false, p.curKey)

			if elem := app.Window().GetElementByID(strconv.Itoa(p.curIdx)); !elem.IsNull() {
				elem.Call("focus")
			}

			// TODO Temp "hack" to bring html state back in line with JS (probably specifically
			// for input text)
			// At the moment, adding a line onto the bottom will not move the cursor down (another
			// side affect of this issue, perhaps)
			for i, item := range p.ListItems {
				if elem := app.Window().GetElementByID(strconv.Itoa(i)); !elem.IsNull() {
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

func (p *Page) Render() app.UI {
	return app.Div().Body(
		app.Input().
			ID(strconv.Itoa(-1)).
			Value(p.match).
			OnInput(p.handleMatchChange).
			OnKeyDown(p.handleNav).
			OnClick(p.focus),
		app.Stack().
			//Center().
			Vertical().
			Content(
				app.Range(p.ListItems).Slice(func(i int) app.UI {
					return app.Input().
						ID(strconv.Itoa(i)).
						Value((p.ListItems)[i].Line).
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

func (p *Page) getKey(idx int) string {
	key := ""
	if idx >= 0 && idx < len(p.ListItems) {
		key = p.ListItems[idx].Key()
	}
	return key
}

func (p *Page) focus(ctx app.Context, e app.Event) {
	idx, _ := strconv.Atoi(ctx.JSSrc.Get("id").String())
	p.curKey = p.getKey(idx)
}

func (p *Page) handleMatchChange(ctx app.Context, e app.Event) {
	s := ctx.JSSrc.Get("value").String()
	p.match = s // Name field is modified
	p.walChan <- &[]service.EventLog{}
}

func (p *Page) handleListItemChange(ctx app.Context, e app.Event) {
	s := ctx.JSSrc.Get("value").String()
	//s := ctx.JSSrc.Get("innerText").String()
	id, _ := strconv.Atoi(ctx.JSSrc.Get("id").String())
	p.db.Update(s, nil, id)
}

// TODO figure out more appropriate function name
func (p *Page) handleNav(ctx app.Context, e app.Event) {
	//e.PreventDefault()
	key := e.Get("key").String()
	id, _ := strconv.Atoi(ctx.JSSrc.Get("id").String())
	if e.Get("ctrlKey").Bool() {
		switch key {
		case "d":
			if p.curIdx == len(p.ListItems)-1 {
				p.curKey, _ = p.db.Delete(id)
			} else {
				p.db.Delete(id)
				p.curKey = p.getKey(p.curIdx + 1)
			}
		default:
			return
		}
	} else {
		switch key {
		case "Enter":
			p.curKey, _ = p.db.Add("", nil, id+1)
		case "Backspace":
			// If the current line is empty, backspace will delete it
			if p.curIdx >= 0 && p.curIdx < len(p.ListItems) && len(p.ListItems[p.curIdx].Line) == 0 {
				p.curKey, _ = p.db.Delete(id)
				e.PreventDefault()
			}
		case "ArrowUp":
			if p.curIdx >= 0 {
				p.curKey = p.getKey(p.curIdx - 1)
			}
		case "ArrowDown":
			if p.curIdx < len(p.ListItems)-1 {
				p.curKey = p.getKey(p.curIdx + 1)
			}
		default:
			return
		}
	}
	// We only want to add an event on keys handled in this function
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
