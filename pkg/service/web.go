package service

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

const (
	webRefreshFrequencyMs        = 60000 // 1 minute
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

type Web struct {
	wsConn *websocket.Conn
	walURL *url.URL
	tokens WebTokenStore
}

func NewWeb(webTokens WebTokenStore) *Web {
	walURL, err := url.Parse(walSyncURL)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}

	return &Web{
		walURL: walURL,
		tokens: webTokens,
	}
}

type WebWalFile struct {
	web                      *Web
	mode                     Mode
	pushMatchTerm            []rune
	processedEventLock       *sync.Mutex
	processedEventMap        map[string]struct{}
	RefreshTicker            *time.Ticker
	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
}

func NewWebWalFile(cfg webRemote, web *Web) *WebWalFile {
	return &WebWalFile{
		web:                      web,
		mode:                     Sync,
		pushMatchTerm:            []rune{},
		processedEventLock:       &sync.Mutex{},
		processedEventMap:        make(map[string]struct{}),
		RefreshTicker:            time.NewTicker(time.Millisecond * time.Duration(webRefreshFrequencyMs)),
		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
	}
}

func (wf *WebWalFile) getPresignedURLForWal(originUUID string, UUID string, method string) (string, error) {
	// Create copy of url
	u := wf.web.walURL

	// Add required querystring params
	q, _ := url.ParseQuery(u.RawQuery)
	q.Add("method", method)
	q.Add("origin-uuid", originUUID)
	q.Add("uuid", UUID)
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Fatal("Error retrieving presigned URL: ", err)
	}
	defer resp.Body.Close()

	// The response body will contain the presigned URL we will use to actually retrieve the wal
	presignedURL, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error parsing presigned url from response: ", err)
	}

	return string(presignedURL), nil
}

func (wf *WebWalFile) GetRoot() string { return "" }
func (wf *WebWalFile) GetMatchingWals(pattern string) ([]string, error) {
	return []string{fakeWalName}, nil
}
func (wf *WebWalFile) GetWal(fileName string) ([]EventLog, error) {
	// In order to retrieve a WalFile, we need to retrieve a presigned s3 url, and then use that to GET the file.

	presignedURL, err := wf.getPresignedURLForWal(fileName, fileName, "get")
	if err != nil {
		return nil, err
	}

	s3Resp, err := http.Get(presignedURL)
	if err != nil {
		log.Fatal("Error retrieving file using presigned S3 URL: ", err)
	}
	defer s3Resp.Body.Close()

	wal, err := ioutil.ReadAll(s3Resp.Body)
	if err != nil {
		log.Fatal("Error parsing wal from S3 response body: ", err)
	}
	buf := bytes.NewBuffer(wal)

	var el []EventLog
	if el, err = BuildFromFile(buf); err != nil {
		log.Fatal("Error building wal from S3: ", err)
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
func (wf *WebWalFile) AwaitPull()                       { <-tempBlock }
func (wf *WebWalFile) AwaitGather()                     { <-tempBlock }
func (wf *WebWalFile) StopTickers()                     { wf.RefreshTicker.Stop() }
func (wf *WebWalFile) GetMode() Mode                    { return wf.mode }
func (wf *WebWalFile) GetPushMatchTerm() []rune         { return wf.pushMatchTerm }
func (wf *WebWalFile) SetProcessedEvent(key string)     {}
func (wf *WebWalFile) IsEventProcessed(key string) bool { return true }

func (w *Web) establishWebSocketConnection(uuid uuid) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	wsURI, err := url.Parse(websocketURL)
	if err != nil {
		// TODO fail silently - do not use websocket
		log.Fatal("broken url:", err)
	}
	header := make(http.Header)
	header.Add(websocketAuthorizationHeader, w.tokens.AccessToken())
	header.Add("Origin-Uuid", fmt.Sprintf("%d", uuid))
	//var resp *http.Response
	//w.wsConn, resp, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
	w.wsConn, _, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
	// TODO re-authentication explicitly handled here as wss handshake only occurs once (doesn't require
	// retries).
	// TODO can definite dedup at least a little
	if err != nil {
		//    log.Fatal("dial:", err)
		//}
		//if resp.StatusCode == http.StatusUnauthorized {
		body := map[string]string{
			"refreshToken": w.tokens.RefreshToken(),
		}
		marshalBody, err := json.Marshal(body)
		if err != nil {
			log.Fatal(err)
		}
		err = Authenticate(w.tokens, marshalBody)
		if err != nil {
			log.Fatal(err)
		}
		header.Set(websocketAuthorizationHeader, w.tokens.AccessToken())
		w.wsConn, _, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (w *Web) pushWebsocket(el EventLog, uuid uuid) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	b := BuildByteWal(&[]EventLog{el})
	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())
	data := map[string]string{
		"uuid": fmt.Sprintf("%d", uuid),
		"wal":  b64Wal,
	}
	marshalData, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}
	err = w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
	if err != nil {
		log.Println("write:", err)
		return
	}
}

func (w *Web) consumeWebsocket(walChan chan *[]EventLog) error {
	var err error
	// Might take a while for an event to come in, so timeouts aren't super useful here
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, message, err := w.wsConn.Read(ctx)
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
