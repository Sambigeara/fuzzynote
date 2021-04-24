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
	"path"
	"strings"
	"sync"
	"time"

	"nhooyr.io/websocket"
)

const (
	webRefreshFrequencyMs        = 10000 // 10 seconds
	webGatherFrequencyMs         = 60000 // 1 minute
	websocketAuthorizationHeader = "Auth"
	walSyncAuthorizationHeader   = "Authorization"
)

var (
	// TODO
	websocketURL      = "wss://4hlf98q6mh.execute-api.eu-west-1.amazonaws.com/prod"
	walSyncURL        = "https://jf7i5gi0f4.execute-api.eu-west-1.amazonaws.com/prod"
	authenticationURL = "https://tt2lmb4xla.execute-api.eu-west-1.amazonaws.com/prod"
)

type Web struct {
	wsConn *websocket.Conn
	tokens WebTokenStore
}

func NewWeb(webTokens WebTokenStore) *Web {
	return &Web{
		tokens: webTokens,
	}
}

type WebWalFile struct {
	uuid                     string
	web                      *Web
	mode                     Mode
	pushMatchTerm            []rune
	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
	processedEventLock       *sync.Mutex
	processedEventMap        map[string]struct{}
	RefreshTicker            *time.Ticker
	GatherTicker             *time.Ticker
}

func NewWebWalFile(cfg webRemote, web *Web) *WebWalFile {
	return &WebWalFile{
		uuid:                     cfg.UUID,
		web:                      web,
		mode:                     cfg.Mode,
		pushMatchTerm:            []rune(cfg.Match),
		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
		processedEventLock:       &sync.Mutex{},
		processedEventMap:        make(map[string]struct{}),
		RefreshTicker:            time.NewTicker(time.Millisecond * time.Duration(webRefreshFrequencyMs)),
		GatherTicker:             time.NewTicker(time.Millisecond * time.Duration(webGatherFrequencyMs)),
	}
}

func (wf *WebWalFile) getPresignedURLForWal(originUUID string, uuid string, method string) (string, error) {
	u, _ := url.Parse(walSyncURL)
	q, _ := url.ParseQuery(u.RawQuery)
	q.Add("method", method)
	u.RawQuery = q.Encode()

	u.Path = path.Join(u.Path, "presigned", originUUID, uuid)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Unable to generate presigned `put` url: %s", body)
	}

	// The response body will contain the presigned URL we will use to actually retrieve the wal
	if err != nil {
		return "", fmt.Errorf("Error parsing presigned url from response: %s", err)
	}

	return string(body), nil
}

func (wf *WebWalFile) GetUUID() string {
	return wf.uuid
}

func (wf *WebWalFile) GetRoot() string { return "" }

func (wf *WebWalFile) GetMatchingWals(pattern string) ([]string, error) {
	u, _ := url.Parse(walSyncURL)
	u.Path = path.Join(u.Path, "wal", "list", wf.uuid)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Printf("Error retrieving presigned URL: %s", err)
		return nil, nil
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Error retrieving presigned URL: %s", err)
		return nil, nil
	}

	strUUIDs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error parsing wals from S3 response body: %s", err)
		return nil, nil
	}

	var uuids []string
	err = json.Unmarshal([]byte(strUUIDs), &uuids)
	if err != nil {
		return nil, err
	}

	return uuids, nil
}

func (wf *WebWalFile) GetWal(fileName string) ([]EventLog, error) {
	presignedURL, err := wf.getPresignedURLForWal(wf.uuid, fileName, "get")
	if err != nil {
		log.Printf("Error retrieving wal %s: %s", fileName, err)
		return nil, nil
	}

	s3Resp, err := http.Get(presignedURL)
	if err != nil {
		log.Printf("Error retrieving file using presigned S3 URL: %s", err)
		return nil, nil
	}
	defer s3Resp.Body.Close()

	b64Wal, err := ioutil.ReadAll(s3Resp.Body)
	if err != nil {
		log.Printf("Error parsing wal from S3 response body: %s", err)
		return nil, nil
	}

	// Wals are transmitted over the wire in binary format, so decode
	wal, err := b64.StdEncoding.DecodeString(string(b64Wal))
	if err != nil {
		log.Printf("Error decoding wal: %s", err)
		return nil, nil
	}

	buf := bytes.NewBuffer(wal)

	var el []EventLog
	if el, err = BuildFromFile(buf); err != nil {
		log.Printf("Error building wal from S3: %s", err)
		return nil, nil
	}
	return el, nil
}

func (wf *WebWalFile) RemoveWals(fileNames []string) error {
	var uuids []string
	for _, f := range fileNames {
		uuids = append(uuids, f)
	}

	u, _ := url.Parse(walSyncURL)
	u.Path = path.Join(u.Path, "wal", "delete", wf.uuid)

	marshalNames, err := json.Marshal(fileNames)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(string(marshalNames)))
	if err != nil {
		return err
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil || resp.StatusCode != http.StatusOK {
		errBody, _ := ioutil.ReadAll(resp.Body)
		log.Fatalf("resp body %s", errBody)
	}
	resp.Body.Close()
	return nil
}

func (wf *WebWalFile) Flush(b *bytes.Buffer, fileName string) error {
	// TODO refactor to pass only UUID, rather than full path (currently blocked by all WalFile != WebWalFile
	partialWal := strings.Split(strings.Split(fileName, "_")[1], ".")[0]

	presignedURL, err := wf.getPresignedURLForWal(wf.uuid, partialWal, "put")
	if err != nil || presignedURL == "" {
		return nil
	}

	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())

	req, err := http.NewRequest("PUT", presignedURL, strings.NewReader(b64Wal))
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	resp.Body.Close()
	return nil
}

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

func (wf *WebWalFile) AwaitPull() {
	<-wf.RefreshTicker.C
}

func (wf *WebWalFile) AwaitGather() {
	<-wf.GatherTicker.C
}

func (wf *WebWalFile) StopTickers() {
	wf.RefreshTicker.Stop()
	wf.GatherTicker.Stop()
}

func (wf *WebWalFile) GetMode() Mode            { return wf.mode }
func (wf *WebWalFile) GetPushMatchTerm() []rune { return wf.pushMatchTerm }

func (wf *WebWalFile) SetProcessedEvent(key string) {
	wf.processedEventLock.Lock()
	defer wf.processedEventLock.Unlock()
	wf.processedEventMap[key] = struct{}{}
}

func (wf *WebWalFile) IsEventProcessed(key string) bool {
	wf.processedEventLock.Lock()
	defer wf.processedEventLock.Unlock()
	_, exists := wf.processedEventMap[key]
	return exists
}

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
	var resp *http.Response
	w.wsConn, resp, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
	// TODO re-authentication explicitly handled here as wss handshake only occurs once (doesn't require
	// retries).
	// TODO can definite dedup at least a little
	if resp.StatusCode == http.StatusUnauthorized {
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
		w.wsConn, resp, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
		if err != nil {
			b, _ := ioutil.ReadAll(resp.Body)
			log.Fatalf("Error establishing websocket connection: %s", b)
		}
	}
}

func (w *Web) pushWebsocket(el EventLog, uuid string) {
	// TODO this is a hack to work around the GetUUID stubs I have in place atm:
	if uuid == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	b := BuildByteWal(&[]EventLog{el})
	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())
	data := map[string]string{
		"uuid": uuid,
		"wal":  b64Wal,
	}
	marshalData, err := json.Marshal(data)
	if err != nil {
		// Fail silently for now, rely on future syncs sorting out discrepencies
		log.Println("push websocket:", err)
		return
	}
	err = w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
	if err != nil {
		log.Println("push websocket:", err)
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
