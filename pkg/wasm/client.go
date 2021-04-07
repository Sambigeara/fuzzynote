package web

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/maxence-charriere/go-app/v8/pkg/app"

	"fuzzynote/pkg/service"
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
func (wf *browserWalFile) AwaitPull()                                   { time.Sleep(10) }
func (wf *browserWalFile) AwaitGather()                                 { time.Sleep(10) }
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
	match     string
	listItems []service.ListItem
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

	walChan := make(chan *[]service.EventLog)
	if err := p.db.StartWeb(walChan); err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			partialWal := <-walChan
			if err := p.db.Replay(partialWal); err != nil {
				log.Fatal(err)
			}
			p.listItems, _, _ = p.db.Match([][]rune{[]rune(p.match)}, false, "")
			p.Update()
		}
	}()
}

func (p *Page) OnDismount(ctx app.Context) {
	err := p.db.Stop()
	if err != nil {
		log.Fatal(err)
	}
}

func (p *Page) Render() app.UI {
	return app.Ul().Body(
		app.Input().
			ID(strconv.Itoa(-1)).
			Value(p.match).
			OnKeyup(p.onKey).
			OnInput(p.OnMatchChange),
		app.Range(p.listItems).Slice(func(i int) app.UI {
			//return app.Li().Text((p.listItems)[i].Line)
			return app.Input().
				ID(strconv.Itoa(i)).
				Value((p.listItems)[i].Line).
				//Class(func() string {
				//    ret := ""
				//    if i == p.curIdx {
				//        ret = "focus"
				//    }
				//    return ret
				//}()).
				OnKeyup(p.onKey).
				OnInput(p.OnListItemChange)
		}),
	).OnClick(p.focus)
}

func (p *Page) curKey() string {
	key := ""
	if p.curIdx >= 0 && p.curIdx < len(p.listItems) {
		key = p.listItems[p.curIdx].Key()
	}
	return key
}

func (p *Page) focus(ctx app.Context, e app.Event) {
	id, _ := strconv.Atoi(ctx.JSSrc.Get("id").String())
	p.curIdx = id
}

func (p *Page) onKey(ctx app.Context, e app.Event) {
	//foo := ctx.JSSrc.Get("keyCode").String() // Name field is modified
	key := e.Get("key").String()
	log.Printf("Key is: %v", key)
	//e.PreventDefault()
	switch key {
	case "Enter":
		id, _ := strconv.Atoi(ctx.JSSrc.Get("id").String())
		p.db.Add("", nil, id+1)
		p.curIdx++
	case "ArrowUp":
		if p.curIdx > -1 {
			p.curIdx--
		}
	case "ArrowDown":
		if p.curIdx < len(p.listItems)-1 {
			p.curIdx++
		}
	}
	if elem := app.Window().GetElementByID(strconv.Itoa(p.curIdx)); !elem.IsNull() {
		elem.Call("focus")
	}
}

func (p *Page) OnMatchChange(ctx app.Context, e app.Event) {
	p.match = ctx.JSSrc.Get("value").String() // Name field is modified
	p.listItems, _, _ = p.db.Match([][]rune{[]rune(p.match)}, false, p.curKey())
	p.Update()
}

func (p *Page) OnListItemChange(ctx app.Context, e app.Event) {
	newLine := ctx.JSSrc.Get("value").String()
	id, _ := strconv.Atoi(ctx.JSSrc.Get("id").String())
	p.db.Update(newLine, nil, id)
	//p.listItems[id].Line = newLine
	//p.Update()
}

//func (h *Hello) Render() app.UI {
//    return app.Div().Body(
//        app.H1().Body(
//            app.Text("Hello "),
//            app.Text(h.name), // The name field used in the title
//        ),

//        // The input HTML element that get the username.
//        app.Input().
//            Value(h.name).             // The name field used as current input value
//            OnInput(h.OnInputChange), // The event handler that will store the username
//    )
//}

//func (h *Hello) OnInputChange(ctx app.Context, e app.Event) {
//    h.name = ctx.JSSrc.Get("value").String() // Name field is modified
//    h.Update()                               // Update the component UI
//}
