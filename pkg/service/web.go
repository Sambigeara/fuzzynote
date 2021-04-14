package service

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

const (
	webRefreshFrequencyMs        = 1000
	websocketAuthorizationHeader = "Auth"
	walSyncAuthorizationHeader   = "Authorization"
	fakeWalName                  = "wal_0.db"
)

var (
	tempBlock = make(chan struct{})
)

type WebWalFile struct {
	wsConn      *websocket.Conn
	walURL      *url.URL
	accessToken string

	mode                     Mode
	pushMatchTerm            []rune
	processedEventLock       *sync.Mutex
	processedEventMap        map[string]struct{}
	RefreshTicker            *time.Ticker
	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
}

func NewWebWalFile(cfg webRemote, accessToken string) *WebWalFile {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	websocketURL := "wss://4hlf98q6mh.execute-api.eu-west-1.amazonaws.com/prod"
	walSyncURL := "https://9hpxwerc9l.execute-api.eu-west-1.amazonaws.com/prod"

	// TODO move this out of NewWebWalFile func
	wsURI, err := url.Parse(websocketURL)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}
	headers := make(http.Header)
	headers.Add(websocketAuthorizationHeader, accessToken)
	wsConn, _, err := websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		log.Fatal("dial:", err)
	}

	walURL, err := url.Parse(walSyncURL)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}

	return &WebWalFile{
		wsConn:      wsConn,
		walURL:      walURL,
		accessToken: accessToken,

		mode:                     Sync,
		pushMatchTerm:            []rune{},
		processedEventLock:       &sync.Mutex{},
		processedEventMap:        make(map[string]struct{}),
		RefreshTicker:            time.NewTicker(time.Millisecond * time.Duration(webRefreshFrequencyMs)),
		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
	}
}

func (wf *WebWalFile) GetRoot() string { return "" }
func (wf *WebWalFile) GetMatchingWals(pattern string) ([]string, error) {
	return []string{fakeWalName}, nil
}
func (wf *WebWalFile) GetWal(fileName string) ([]EventLog, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", wf.walURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add(walSyncAuthorizationHeader, wf.accessToken)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("wal sync: ", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Fatal("wal sync: ", resp)
	}

	body, err := ioutil.ReadAll(resp.Body)
	strWal, _ := b64.StdEncoding.DecodeString(string(body))
	buf := bytes.NewBuffer([]byte(strWal))

	var el []EventLog
	if el, err = BuildFromFile(buf); err != nil {
		log.Fatal("wal sync: ", err)
	}
	return el, nil
}
func (wf *WebWalFile) RemoveWal(fileName string) error              { return nil }
func (wf *WebWalFile) Flush(b *bytes.Buffer, fileName string) error { return nil }
func (wf *WebWalFile) SetProcessedPartialWals(partialWal string) {
	wf.processedPartialWalsLock.Lock()
	defer wf.processedPartialWalsLock.Unlock()
	wf.processedPartialWals[partialWal] = struct{}{}
}
func (wf *WebWalFile) IsPartialWalProcessed(partialWal string) bool {
	wf.processedPartialWalsLock.Lock()
	defer wf.processedPartialWalsLock.Unlock()
	_, exists := wf.processedPartialWals[partialWal]
	return exists
}
func (wf *WebWalFile) AwaitPull()                       { <-wf.RefreshTicker.C }
func (wf *WebWalFile) AwaitGather()                     { <-tempBlock }
func (wf *WebWalFile) StopTickers()                     { wf.RefreshTicker.Stop() }
func (wf *WebWalFile) GetMode() Mode                    { return wf.mode }
func (wf *WebWalFile) GetPushMatchTerm() []rune         { return wf.pushMatchTerm }
func (wf *WebWalFile) SetProcessedEvent(key string)     {}
func (wf *WebWalFile) IsEventProcessed(key string) bool { return true }

func (ws *WebWalFile) consumeWebsocket(walChan chan *[]EventLog) error {
	var err error
	// Might take a while for an event to come in, so timeouts aren't super useful here
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, message, err := ws.wsConn.Read(ctx)
	if err != nil {
		log.Println("read:", err)
	}
	strWal, _ := b64.StdEncoding.DecodeString(string(message))
	buf := bytes.NewBuffer([]byte(strWal))
	if el, err := BuildFromFile(buf); err == nil {
		walChan <- &el
	}
	return err
}

func (ws *WebWalFile) pushWebsocket(el EventLog) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	b := buildByteWal(&[]EventLog{el})
	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())
	err := ws.wsConn.Write(ctx, websocket.MessageText, []byte(b64Wal))
	if err != nil {
		log.Println("write:", err)
		return
	}
}
