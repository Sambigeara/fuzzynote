package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
	webRefreshInterval         = time.Minute * 10
)

type WebWalFile struct {
	uuid               string
	web                *Web
	mode               string
	pushMatchTerm      []rune
	processedEventLock *sync.Mutex
	processedEventMap  map[string]struct{}
}

func NewWebWalFile(cfg WebRemote, web *Web) *WebWalFile {
	return &WebWalFile{
		uuid:               cfg.UUID,
		web:                web,
		mode:               cfg.Mode,
		pushMatchTerm:      []rune(cfg.Match),
		processedEventLock: &sync.Mutex{},
		processedEventMap:  make(map[string]struct{}),
	}
}

func (w *Web) establishWebSocketConnection() error {
	// TODO close off previous connection gracefully if present??

	dialFunc := func(accessToken string) (*websocket.Conn, *http.Response, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		u, _ := url.Parse(websocketURL)
		q, _ := url.ParseQuery(u.RawQuery)
		q.Add("auth", accessToken)
		q.Add("uuid", fmt.Sprintf("%d", w.uuid))
		u.RawQuery = q.Encode()

		return websocket.Dial(ctx, u.String(), &websocket.DialOptions{})
	}

	var resp *http.Response
	var err error
	w.wsConn, resp, err = dialFunc(w.tokens.IDToken())
	if err != nil && resp == nil {
		return fmt.Errorf("Failed to establish websocket connection: %s", err)
	}
	// TODO re-authentication explicitly handled here as wss handshake only occurs once (doesn't require
	// retries) - can probably dedup at least a little
	if resp == nil || resp.StatusCode != http.StatusSwitchingProtocols {
		w.wsConn = nil
		body := map[string]string{
			"refreshToken": w.tokens.RefreshToken(),
		}
		marshalBody, err := json.Marshal(body)
		if err != nil {
			return err
		}
		err = Authenticate(w.tokens, marshalBody, nil)
		if err != nil {
			return err
		}
		w.wsConn, resp, err = dialFunc(w.tokens.IDToken())
		// need to return within this nested block otherwise the outside err still holds
		// data from previous calls
		return err
	}
	return err
}

type websocketMessage struct {
	Action string `json:"action"`

	// `wal` events
	UUID string `json:"uuid"`
	Wal  string `json:"wal"`

	// `position` events (collaborator cursor positions)
	Email        string `json:"email"`
	Key          string `json:"key"`
	UnixNanoTime int64  `json:"dt"`
}

type cursorMoveEvent struct {
	email        string
	listItemKey  string
	unixNanoTime int64
}

func (w *Web) pushWebsocket(m websocketMessage) {
	marshalData, err := json.Marshal(m)
	if err != nil {
		//log.Fatal("Json marshal: malformed WAL data on websocket push")
		// TODO proper handling
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	err = w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
	//if err != nil {
	//    // Re-establish websocket connection on error
	//    // TODO currently attempting to re-establish connection on ANY error - do better at
	//    // identifying websocket connect issues
	//    w.establishWebSocketConnection()

	//    // if re-establish successful (e.g. wsConn != nil) reattempt write
	//    if w.wsConn != nil {
	//        err = w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
	//        if err != nil {
	//            w.wsConn = nil
	//        }
	//    }
	//}
}

func (w *Web) consumeWebsocket(ctx context.Context, walChan chan *[]EventLog, remoteCursorMoveChan chan cursorMoveEvent) error {
	_, body, err := w.wsConn.Read(ctx)
	if err != nil {
		return err
	}

	var m websocketMessage
	err = json.Unmarshal(body, &m)
	if err != nil {
		return nil
	}

	switch m.Action {
	case "wal":
		strWal, err := base64.StdEncoding.DecodeString(m.Wal)
		if err != nil {
			// TODO proper handling
			return nil
		}

		buf := bytes.NewBuffer([]byte(strWal))
		el, err := buildFromFile(buf)
		if err == nil && len(*el) > 0 {
			walChan <- el

			// Acknowledge the event in the WalFile event cache, so we know to emit further events even
			// if the match term doesn't match
			wf, exists := w.walFileMap[m.UUID]
			// This can occur when running two independent instances for the same user.
			if exists {
				key, _ := (*el)[0].getKeys()
				(*wf).SetProcessedEvent(key)
			}
		}
	case "position":
		remoteCursorMoveChan <- cursorMoveEvent{
			email:        m.Email,
			listItemKey:  m.Key,
			unixNanoTime: m.UnixNanoTime,
		}
	}

	// TODO proper handling
	return nil
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

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.IDToken())
	resp, err := wf.web.CallWithReAuth(req)
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

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.IDToken())
	resp, err := wf.web.CallWithReAuth(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		//log.Printf("Error retrieving wal list: %v", err)
		return nil, nil
	}

	strUUIDs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//log.Printf("Error parsing wals from S3 response body: %s", err)
		return nil, nil
	}

	var uuids []string
	err = json.Unmarshal([]byte(strUUIDs), &uuids)
	if err != nil {
		return nil, err
	}

	return uuids, nil
}

func (wf *WebWalFile) GetWalBytes(w io.Writer, fileName string) error {
	presignedURL, err := wf.getPresignedURLForWal(wf.uuid, fileName, "get")
	if err != nil {
		//log.Printf("Error retrieving wal %s: %s", fileName, err)
		return err
	}

	s3Resp, err := http.Get(presignedURL)
	if err != nil {
		//log.Printf("Error retrieving file using presigned S3 URL: %s", err)
		return err
	}
	defer s3Resp.Body.Close()

	dec := base64.NewDecoder(base64.StdEncoding, s3Resp.Body)

	io.Copy(w, dec)

	return nil
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

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.IDToken())
	resp, err := wf.web.CallWithReAuth(req)
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
