package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"

	"nhooyr.io/websocket"
)

const (
	websocketURL               = "wss://ws.fuzzynote.co.uk/v1"
	apiURL                     = "https://api.fuzzynote.co.uk/v1"
	walSyncAuthorizationHeader = "Authorization"
	webPingInterval            = time.Second * 30
	websocketPageSize          = 500
	websocketMaxPageSizeBytes  = 32768
)

const ()

type Web struct {
	client   *http.Client
	wsConn   *websocket.Conn
	tokens   WebTokenStore
	isActive bool
}

func NewWeb(webTokens WebTokenStore) *Web {
	return &Web{
		client: &http.Client{
			Timeout: 3 * time.Second,
		},
		tokens: webTokens,
	}
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

type websocketAction string

const (
	websocketActionWal      websocketAction = "wal"
	websocketActionPosition websocketAction = "position"
	websocketActionSync     websocketAction = "sync"
)

type websocketMessage struct {
	Action websocketAction `json:"action"`

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

type messageTooBigError struct {
}

func (e messageTooBigError) Error() string { return "fail" }

//type messageTooBigError error

func (w *Web) publishWebsocket(ctx context.Context, m websocketMessage) error {
	marshalData, err := json.Marshal(m)
	if err != nil {
		//log.Fatal("Json marshal: malformed WAL data on websocket push")
		// TODO proper handling
		return err
	}

	//log.Println("LEN: ", len(marshalData))
	if len(marshalData) >= websocketMaxPageSizeBytes {
		return messageTooBigError{}
	}

	return w.wsConn.Write(ctx, websocket.MessageText, marshalData)
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
	case websocketActionWal:
		strWal, err := base64.StdEncoding.DecodeString(m.Wal)
		if err != nil {
			// TODO proper handling
			return el, nil
		}
		buf := bytes.NewBuffer([]byte(strWal))
		if el, err = r.buildFromFile(buf); err != nil {
			return el, err
		}
	case websocketActionPosition:
		r.remoteCursorMoveChan <- cursorMoveEvent{
			email:        m.Email,
			listItemKey:  m.Key,
			unixNanoTime: m.UnixNanoTime,
		}
	case websocketActionSync:
		strWal, err := base64.StdEncoding.DecodeString(m.Wal)
		if err != nil {
			// TODO proper handling
			return el, nil
		}
		buf := bytes.NewBuffer([]byte(strWal))
		if el, err = r.buildFromFile(buf); err != nil {
			return el, err
		}
		e := el[0]
		syncLog, eventKnown := r.crdt.sync(e)
		if len(syncLog) > 0 {
			go func() {
				r.eventsChan <- syncLog
			}()
			// if event is not recognised, requests a sync in the reverse direction
		}
		if !eventKnown {
			r.requestSync()
		}
		return []EventLog{}, nil
	}

	// TODO proper handling
	return el, nil
}

func (r *DBListRepo) publishEventLog(ctx context.Context, el []EventLog, email string) error {
	if r.web.isActive {
		for left := 0; left < len(el); left += websocketPageSize {
			right := left + websocketPageSize
			if right > len(el) {
				right = len(el)
			}
			page := el[left:right]
			b, _ := BuildByteWal(page)
			b64Wal := base64.StdEncoding.EncodeToString(b.Bytes())

			m := websocketMessage{
				Action: websocketActionWal,
				UUID:   email,
				Wal:    b64Wal,
			}
			if err := r.web.publishWebsocket(ctx, m); err != nil {
				if _, ok := err.(messageTooBigError); ok {
					mid := len(page) / 2
					if mid == 0 {
						return err
					}
					go func() {
						r.eventsChan <- page[:mid]
						r.eventsChan <- page[mid:]
					}()
					continue
				}
				//log.Fatalln(err, "CODE: ", websocket.CloseStatus(err))
				//websocket.CloseStatus(err) == websocket.StatusMessageTooBig
				return err
			}
		}
	}
	return nil
}

func (w *Web) publishPosition(ctx context.Context, e cursorMoveEvent, email string) error {
	if w.isActive {
		m := websocketMessage{
			Action:       websocketActionPosition,
			UUID:         email,
			Key:          e.listItemKey,
			UnixNanoTime: e.unixNanoTime,
		}
		if err := w.publishWebsocket(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

func (w *Web) publishSync(ctx context.Context, b []byte, email string) error {
	m := websocketMessage{
		Action: websocketActionSync,
		UUID:   email,
		Wal:    base64.StdEncoding.EncodeToString(b),
	}
	if err := w.publishWebsocket(ctx, m); err != nil {
		return err
	}
	return nil
}

func (r *DBListRepo) startWeb(ctx context.Context, replayChan chan []EventLog, inputEvtsChan chan interface{}) {
	webPingTicker := time.NewTicker(webPingInterval)

	connectChan := make(chan struct{})
	go func() {
		connectChan <- struct{}{}
	}()

	var wsCtx context.Context
	var wsCancel context.CancelFunc

	// Prioritise async web start-up to minimise wait time before websocket instantiation
	// Create a loop responsible for periodic refreshing of web connections and web walfiles.
	go func() {
		for {
			select {
			case <-webPingTicker.C:
				// is !isActive, we've already entered the exponential retry backoff below
				if r.web.isActive {
					if pong, err := r.web.ping(); err != nil {
					} else {
						r.updateActiveFriendsMap(pong.ActiveFriends, pong.PendingFriends, inputEvtsChan)
					}
				}
			case <-connectChan:
				if wsCancel != nil {
					wsCancel()
				}
				wsCtx, wsCancel = context.WithCancel(ctx)
				err := r.registerWeb(inputEvtsChan)
				if err != nil {
					r.web.isActive = false
					switch err.(type) {
					case authFailureError:
						return // authFailureError signifies incorrect login details, disable web and run local only mode
					default:
						go func() {
							time.Sleep(webPingInterval)
							connectChan <- struct{}{}
						}()
						continue
					}
				}

				r.web.isActive = true

				go func() {
					for {
						wsEv, err := r.consumeWebsocket(wsCtx)
						if err != nil {
							//if websocket.CloseStatus(err) == websocket.StatusMessageTooBig {
							////r.web.connectChan <- struct{}{}
							//}
							return
						}
						replayChan <- wsEv
					}
				}()

				// Send initial sync event to websocket
				r.requestSync()
			case <-ctx.Done():
				if r.web.wsConn != nil {
					r.web.wsConn.Close(websocket.StatusNormalClosure, "")
				}
				return
			}
		}
	}()
}

func (r *DBListRepo) registerWeb(inputEvtsChan chan interface{}) error {
	if err := r.web.establishWebSocketConnection(); err != nil {
		return err
	}

	if r.email == "" {
		if pong, err := r.web.ping(); err == nil {
			r.setEmail(pong.User)
			r.web.tokens.SetEmail(pong.User)
			r.web.tokens.Flush()
			r.updateActiveFriendsMap(pong.ActiveFriends, pong.PendingFriends, inputEvtsChan)
		}
	}

	return nil
}
