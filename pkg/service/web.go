package service

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
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

	// TODO
	websocketURL      = "wss://4hlf98q6mh.execute-api.eu-west-1.amazonaws.com/prod"
	walSyncURL        = "https://9hpxwerc9l.execute-api.eu-west-1.amazonaws.com/prod"
	authenticationURL = "https://tt2lmb4xla.execute-api.eu-west-1.amazonaws.com/prod"
)

type WebWalFile struct {
	wsConn *websocket.Conn
	walURL *url.URL
	tokens *WebTokens

	mode                     Mode
	pushMatchTerm            []rune
	processedEventLock       *sync.Mutex
	processedEventMap        map[string]struct{}
	RefreshTicker            *time.Ticker
	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
}

func NewWebWalFile(cfg webRemote, webTokens *WebTokens) *WebWalFile {
	walURL, err := url.Parse(walSyncURL)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}

	return &WebWalFile{
		walURL: walURL,
		tokens: webTokens,

		mode:                     Sync,
		pushMatchTerm:            []rune{},
		processedEventLock:       &sync.Mutex{},
		processedEventMap:        make(map[string]struct{}),
		RefreshTicker:            time.NewTicker(time.Millisecond * time.Duration(webRefreshFrequencyMs)),
		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
	}
}

func (ws *WebWalFile) establishConnection() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// TODO move this out of NewWebWalFile func
	wsURI, err := url.Parse(websocketURL)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}
	header := make(http.Header)
	header.Add(websocketAuthorizationHeader, ws.tokens.Access)
	var resp *http.Response
	ws.wsConn, resp, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		//log.Fatal("dial:", err)
	}
	// TODO re-authentication explicitly handled here as wss handshake only occurs once (doesn't require
	// retries).
	// TODO can definite dedup at least a little
	//if resp.StatusCode == http.StatusUnauthorized {
	if resp.StatusCode != http.StatusOK {
		body := map[string]string{
			"refreshToken": ws.tokens.Refresh,
		}
		marshalBody, err := json.Marshal(body)
		if err != nil {
			log.Fatal(err)
		}
		err = ws.tokens.Authenticate(marshalBody)
		if err != nil {
			log.Fatal(err)
		}
		header.Set(websocketAuthorizationHeader, ws.tokens.Access)
		ws.wsConn, _, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (wf *WebWalFile) GetRoot() string { return "" }
func (wf *WebWalFile) GetMatchingWals(pattern string) ([]string, error) {
	return []string{fakeWalName}, nil
}
func (wf *WebWalFile) GetWal(fileName string) ([]EventLog, error) {
	req, err := http.NewRequest("GET", wf.walURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add(walSyncAuthorizationHeader, wf.tokens.Access)
	resp, err := wf.tokens.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		log.Fatal("wal sync: ", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
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
