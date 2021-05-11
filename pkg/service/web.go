package service

import (
	"bytes"
	"context"
	"encoding/base64"
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
	// TODO envvars
	websocketURL = "wss://ws.fuzzynote.co.uk/v1"
	apiURL       = "https://api.fuzzynote.co.uk/v1"

	walSyncAuthorizationHeader = "Authorization"
	iDTokenHeader              = "Id-Token"

	refreshFrequency = 30000 // 30 seconds
)

type WebWalFile struct {
	uuid                     string
	web                      *Web
	mode                     string
	pushMatchTerm            []rune
	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
	processedEventLock       *sync.Mutex
	processedEventMap        map[string]struct{}
	RefreshTicker            *time.Ticker
}

func NewWebWalFile(cfg WebRemote, web *Web) *WebWalFile {
	return &WebWalFile{
		uuid:                     cfg.UUID,
		web:                      web,
		mode:                     cfg.Mode,
		pushMatchTerm:            []rune(cfg.Match),
		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
		processedEventLock:       &sync.Mutex{},
		processedEventMap:        make(map[string]struct{}),
		RefreshTicker:            time.NewTicker(time.Millisecond * time.Duration(refreshFrequency)),
	}
}

func (w *Web) establishWebSocketConnection() {
	//if w.wsConn != nil {
	//    ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	//    defer cancel()
	//    // Return on successful response
	//    if err := w.wsConn.Ping(ctx); err == nil {
	//        return
	//    }
	//}

	dialFunc := func(accessToken string) (*websocket.Conn, *http.Response) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		u, _ := url.Parse(websocketURL)
		q, _ := url.ParseQuery(u.RawQuery)
		q.Add("auth", accessToken)
		q.Add("uuid", fmt.Sprintf("%d", w.uuid))
		u.RawQuery = q.Encode()

		wsConn, resp, err := websocket.Dial(ctx, u.String(), &websocket.DialOptions{})
		if err != nil {
			// If StatusUnauthorized, let it return as normal so we can attempt reauth
			if resp != nil && resp.StatusCode == http.StatusUnauthorized {
				return nil, resp
			}
			log.Fatalf("Error establishing websocket connection: %s", err)
		}
		//if resp.StatusCode == http.StatusUnauthorized {
		//    errStr := ""
		//    if resp.Body != nil {
		//        b, _ := ioutil.ReadAll(resp.Body)
		//        errStr = string(b)
		//    }
		//    log.Fatalf("Error establishing websocket connection: %s", errStr)
		//}
		return wsConn, resp
	}

	var resp *http.Response
	w.wsConn, resp = dialFunc(w.tokens.AccessToken())
	// TODO re-authentication explicitly handled here as wss handshake only occurs once (doesn't require
	// retries) - can probably dedup at least a little
	if resp.StatusCode == http.StatusUnauthorized {
		w.wsConn = nil
		body := map[string]string{
			"refreshToken": w.tokens.RefreshToken(),
		}
		marshalBody, err := json.Marshal(body)
		if err != nil {
			return
		}
		err = Authenticate(w.tokens, marshalBody, nil)
		if err != nil {
			return
		}
		w.wsConn, resp = dialFunc(w.tokens.AccessToken())
	}
}

type message struct {
	UUID   string `json:"uuid"`
	Wal    string `json:"wal"`
	Action string `json:"action"`
}

func (w *Web) pushWebsocket(el EventLog, uuid string) {
	// TODO this is a hack to work around the GetUUID stubs I have in place atm:
	if uuid == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	b := BuildByteWal(&[]EventLog{el})
	b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())
	m := message{
		UUID:   uuid,
		Wal:    b64Wal,
		Action: "wal",
	}
	marshalData, err := json.Marshal(m)
	if err != nil {
		log.Fatal("Json marshal: malformed WAL data on websocket push")
	}
	err = w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
	if err != nil {
		log.Fatal("Hello")
		// Re-establish websocket connection on error
		// TODO currently attempting to re-establish connection on ANY error - do better at
		// identifying websocket connect issues
		w.establishWebSocketConnection()

		// if re-establish successful (e.g. wsConn != nil) reattempt write
		if w.wsConn != nil {
			err = w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
			if err != nil {
				w.wsConn = nil
			}
		}
	}
}

func (w *Web) consumeWebsocket(walChan chan *[]EventLog) error {
	var err error
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, body, err := w.wsConn.Read(ctx)
	if err != nil {
		// Re-establish websocket connection on error
		// TODO currently attempting to re-establish connection on ANY error - do better at
		// identifying websocket connect issues
		w.establishWebSocketConnection()

		// if re-establish successful (e.g. wsConn != nil) reattempt write
		if w.wsConn != nil {
			_, body, err = w.wsConn.Read(ctx)
			if err != nil {
				return nil
			}
		}
	}

	var m message
	err = json.Unmarshal(body, &m)
	if err != nil {
		return nil
	}
	strWal, _ := base64.StdEncoding.DecodeString(string(m.Wal))

	buf := bytes.NewBuffer([]byte(strWal))
	el, err := BuildFromFile(buf)
	if err == nil && len(el) > 0 {
		walChan <- &el

		// Acknowledge the event in the WalFile event cache, so we know to emit further events even
		// if the match term doesn't match
		wf, exists := w.walFileMap[m.UUID]

		// TODO Add the walfile if it doesn't exist in the map already.
		// This can occur when running two independent instances for the same user.
		if exists {
			key, _ := el[0].getKeys()
			(*wf).SetProcessedEvent(key)
		}
	}

	return err
}

func (wf *WebWalFile) getPresignedURLForWal(originUUID string, uuid string, method string) (string, error) {
	u, _ := url.Parse(apiURL)
	q, _ := url.ParseQuery(u.RawQuery)
	q.Add("method", method)
	q.Add("origin-uuid", originUUID)
	q.Add("uuid", uuid)
	u.RawQuery = q.Encode()

	u.Path = path.Join(u.Path, "wal", "presigned")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	req.Header.Add(iDTokenHeader, wf.web.tokens.IDToken())
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
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "wal", "list", wf.uuid)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Fatalf("Error creating wal list request: %v", err)
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	req.Header.Add(iDTokenHeader, wf.web.tokens.IDToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Error retrieving wal list: %v", err)
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
	wal, err := base64.StdEncoding.DecodeString(string(b64Wal))
	if err != nil {
		//log.Printf("Error decoding wal: %s", err)
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

	u, _ := url.Parse(apiURL)
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
	req.Header.Add(iDTokenHeader, wf.web.tokens.IDToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (wf *WebWalFile) Flush(b *bytes.Buffer, partialWal string) error {
	// TODO refactor to pass only UUID, rather than full path (currently blocked by all WalFile != WebWalFile
	//partialWal := strings.Split(strings.Split(fileName, "_")[1], ".")[0]

	presignedURL, err := wf.getPresignedURLForWal(wf.uuid, partialWal, "put")
	if err != nil || presignedURL == "" {
		return nil
	}

	b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())

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

func (wf *WebWalFile) StopTickers() {
	wf.RefreshTicker.Stop()
}

func (wf *WebWalFile) GetMode() string          { return wf.mode }
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
