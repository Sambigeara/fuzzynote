package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"nhooyr.io/websocket"
)

const (
	websocketURL = "wss://ws.fuzzynote.co.uk/v1"
	apiURL       = "https://api.fuzzynote.co.uk/v1"

	walSyncAuthorizationHeader = "Authorization"

	webPingInterval    = time.Second * 30       // lower ping intervals (5s has been tested) cause performance degradation in the wasm app
	webRefreshInterval = time.Second * (2 << 6) // 128 seconds ~= 2 minutes, because we use exponential backoffs
)

type WebWalFile struct {
	// TODO rename uuid to email
	uuid string
	web  *Web
	mode string
}

func (w *Web) establishWebSocketConnection() error {
	// TODO close off previous connection gracefully if present??

	dialFunc := func(token string) (*websocket.Conn, *http.Response, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		u, _ := url.Parse(websocketURL)
		q, _ := url.ParseQuery(u.RawQuery)
		q.Add("auth", token)
		u.RawQuery = q.Encode()

		return websocket.Dial(ctx, u.String(), &websocket.DialOptions{})
	}

	var resp *http.Response
	var err error
	idToken := w.tokens.IDToken()
	if idToken != "" {
		w.wsConn, resp, err = dialFunc(w.tokens.IDToken())
	}
	// TODO re-authentication explicitly handled here as wss handshake only occurs once (doesn't require
	// retries) - can probably dedup at least a little
	if idToken == "" || err != nil || resp == nil || resp.StatusCode != http.StatusSwitchingProtocols {
		defer w.tokens.Flush()
		w.tokens.SetIDToken("")
		w.wsConn = nil
		body := map[string]string{
			"refreshToken": w.tokens.RefreshToken(),
		}
		err = Authenticate(w.tokens, body)
		if err != nil {
			if _, ok := err.(authFailureError); ok {
				w.tokens.SetRefreshToken("")
				w.tokens.Flush()
			}
			return err
		}
		w.wsConn, resp, err = dialFunc(w.tokens.IDToken())
		// need to return within this nested block otherwise the outside err still holds
		// data from previous calls
		return err
	}
	return err
}

type pong struct {
	Response                      string
	User                          string
	ActiveFriends, PendingFriends []string
}

func (w *Web) ping() (pong, error) {
	p := pong{}
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "ping")
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return p, err
	}

	resp, err := w.CallWithReAuth(req)
	if err != nil {
		return p, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return p, errors.New("ping did not return 201")
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return p, err
	}
	if err := json.Unmarshal(respBytes, &p); err != nil {
		return p, err
	}
	if p.Response != "pong" {
		return p, errors.New("ping did not return a pong")
	}
	return p, nil
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

func (w *Web) pushWebsocket(m websocketMessage) error {
	marshalData, err := json.Marshal(m)
	if err != nil {
		//log.Fatal("Json marshal: malformed WAL data on websocket push")
		// TODO proper handling
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	return w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
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

func (r *DBListRepo) consumeWebsocket(ctx context.Context) ([]EventLog, error) {
	var el []EventLog
	_, body, err := r.web.wsConn.Read(ctx)
	if err != nil {
		return el, err
	}

	var m websocketMessage
	err = json.Unmarshal(body, &m)
	if err != nil {
		return el, nil
	}

	switch m.Action {
	case "wal":
		strWal, err := base64.StdEncoding.DecodeString(m.Wal)
		if err != nil {
			// TODO proper handling
			return el, nil
		}

		buf := bytes.NewBuffer([]byte(strWal))
		if el, err = r.buildFromFile(buf); err != nil {
			return el, err
		}
	case "position":
		r.remoteCursorMoveChan <- cursorMoveEvent{
			email:        m.Email,
			listItemKey:  m.Key,
			unixNanoTime: m.UnixNanoTime,
		}
	}

	// TODO proper handling
	return el, nil
}

func (wf *WebWalFile) getPresignedURLForWal(ctx context.Context, owner string, checksum string, method string) (string, error) {
	u, _ := url.Parse(apiURL)
	q, _ := url.ParseQuery(u.RawQuery)
	q.Add("method", method)
	q.Add("owner", owner)
	q.Add("checksum", checksum)
	u.RawQuery = q.Encode()

	u.Path = path.Join(u.Path, "wal", "presigned")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return "", err
	}

	req = req.WithContext(ctx)

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

func (wf *WebWalFile) GetMatchingWals(ctx context.Context, pattern string) ([]string, error) {
	u, _ := url.Parse(apiURL)
	// we now pass emails as `wf.uuid`, so escape it here
	escapedEmail := url.PathEscape(wf.uuid)
	u.Path = path.Join(u.Path, "wal", "list", escapedEmail)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Fatalf("Error creating wal list request: %v", err)
	}

	req = req.WithContext(ctx)

	resp, err := wf.web.CallWithReAuth(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		//log.Printf("Error retrieving wal list: %v", err)
		return nil, nil
	}

	byteUUIDs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil
	}

	var uuids []string
	err = json.Unmarshal(byteUUIDs, &uuids)
	if err != nil {
		return nil, err
	}

	return uuids, nil
}

func (wf *WebWalFile) GetWalBytes(ctx context.Context, w io.Writer, fileName string) error {
	presignedURL, err := wf.getPresignedURLForWal(ctx, wf.GetUUID(), fileName, "get")
	if err != nil {
		//log.Printf("Error retrieving wal %s: %s", fileName, err)
		return err
	}

	req, err := http.NewRequest("GET", presignedURL, nil)
	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	dec := base64.NewDecoder(base64.StdEncoding, resp.Body)

	io.Copy(w, dec)

	return nil
}

func (wf *WebWalFile) RemoveWals(ctx context.Context, fileNames []string) error {
	var uuids []string
	for _, f := range fileNames {
		uuids = append(uuids, f)
	}

	u, _ := url.Parse(apiURL)
	escapedEmail := url.PathEscape(wf.uuid)
	u.Path = path.Join(u.Path, "wal", "delete", escapedEmail)

	marshalNames, err := json.Marshal(fileNames)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(string(marshalNames)))
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)

	resp, err := wf.web.CallWithReAuth(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (wf *WebWalFile) Flush(ctx context.Context, b *bytes.Buffer, checksum string) error {
	// TODO refactor to pass only UUID, rather than full path (currently blocked by all WalFile != WebWalFile
	//tempUUID := strings.Split(strings.Split(fileName, "_")[1], ".")[0]

	presignedURL, err := wf.getPresignedURLForWal(ctx, wf.GetUUID(), checksum, "put")
	if err != nil || presignedURL == "" {
		return nil
	}

	b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())

	req, err := http.NewRequest("PUT", presignedURL, strings.NewReader(b64Wal))
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
