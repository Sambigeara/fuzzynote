package service

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	//"errors"
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
	//walURL *url.URL
	tokens WebTokenStore
}

func NewWeb(webTokens WebTokenStore) *Web {
	//walURL, err := url.Parse(walSyncURL)
	//if err != nil {
	//    // TODO fail silently - do not use websocket
	//    log.Fatal("broken url:", err)
	//}

	return &Web{
		//walURL: walURL,
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
	// Create copy of url
	u, _ := url.Parse(walSyncURL)

	// Add required querystring params
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
	if resp.StatusCode != http.StatusOK {
		//errBody, _ := ioutil.ReadAll(resp.Body)
		//log.Fatalf("url %s, resp body %s", u.String(), errBody)
		//log.Fatalf("Error retrieving presigned URL for origin %s and uuid %s with method %s", originUUID, uuid, method)
		//return "", errors.New("Unable to generate presigned `put` url")
		return "", nil
	}

	// The response body will contain the presigned URL we will use to actually retrieve the wal
	presignedURL, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error parsing presigned url from response: ", err)
	}
	//log.Fatalf("%v", presignedURL)

	return string(presignedURL), nil
}

func (wf *WebWalFile) GetUUID() string {
	return wf.uuid
}

func (wf *WebWalFile) GetRoot() string { return "" }

func (wf *WebWalFile) GetMatchingWals(pattern string) ([]string, error) {
	// TODO dedup
	// Create copy of url
	u, _ := url.Parse(walSyncURL)

	// Add required querystring params
	//q, _ := url.ParseQuery(u.RawQuery)
	//q.Add("method", method)
	//q.Add("origin-uuid", originUUID)
	//q.Add("uuid", uuid)
	//u.RawQuery = q.Encode()

	u.Path = path.Join(u.Path, "wal", "list", wf.uuid)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return []string{}, err
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return []string{}, nil // TODO
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return []string{}, nil // TODO
		//log.Fatal("Error retrieving presigned URL: ", err)
	}

	strUUIDs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error parsing wals from S3 response body: ", err)
	}

	var uuids []string
	err = json.Unmarshal([]byte(strUUIDs), &uuids)
	if err != nil {
		return []string{}, err
	}

	return uuids, nil
}

func (wf *WebWalFile) GetWal(fileName string) ([]EventLog, error) {
	// TODO lol
	//log.Fatal(fileName)
	//log.Fatalf("POW %s", fileName)
	//partialWal := strings.Split(strings.Split(fileName, "_")[1], ".")[0]
	presignedURL, err := wf.getPresignedURLForWal(wf.uuid, fileName, "get")
	if err != nil || presignedURL == "" {
		//return nil, err
		return []EventLog{}, nil
	}

	s3Resp, err := http.Get(presignedURL)
	if err != nil {
		log.Fatal("Error retrieving file using presigned S3 URL: ", err)
	}
	defer s3Resp.Body.Close()

	b64Wal, err := ioutil.ReadAll(s3Resp.Body)
	if err != nil {
		log.Fatal("Error parsing wal from S3 response body: ", err)
	}

	// Wals are transmitted over the wire in binary format, so decode
	wal, _ := b64.StdEncoding.DecodeString(string(b64Wal))

	buf := bytes.NewBuffer(wal)

	var el []EventLog
	if el, err = BuildFromFile(buf); err != nil {
		log.Fatal("Error building wal from S3: ", err)
	}
	return el, nil
}

func (wf *WebWalFile) RemoveWals(fileNames []string) error {
	// TODO lol
	var uuids []string
	for _, f := range fileNames {
		//partialWal := strings.Split(strings.Split(f, "_")[1], ".")[0]
		uuids = append(uuids, f)
	}
	//log.Fatalf("wals %v", uuids)

	// TODO dedup
	// Create copy of url
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
		//log.Fatalf("POW %v", resp)
		//log.Fatal("Error deleting wals")
	}
	resp.Body.Close()
	return nil
}

func (wf *WebWalFile) Flush(b *bytes.Buffer, fileName string) error {
	// TODO lol
	partialWal := strings.Split(strings.Split(fileName, "_")[1], ".")[0]

	presignedURL, err := wf.getPresignedURLForWal(wf.uuid, partialWal, "put")
	if err != nil || presignedURL == "" {
		//log.Fatalf("Unable to retrieve presigned url for origin %s uuid %s method `put`", wf.uuid, partialWal)
		//return err
		return nil
	}

	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())

	req, err := http.NewRequest("PUT", presignedURL, strings.NewReader(b64Wal))
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
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
