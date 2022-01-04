package service

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"nhooyr.io/websocket"
)

type Web struct {
	wsConn   *websocket.Conn
	tokens   WebTokenStore
	isActive bool
}

func NewWeb(webTokens WebTokenStore) *Web {
	return &Web{
		tokens: webTokens,
	}
}

type WebRemote struct {
	Emails       []string
	DTLastChange int64
}

// PostRemote is responsible for both additions and deletions
func (w *Web) PostRemote(remote *WebRemote, u *url.URL) error {
	body, err := json.Marshal(remote)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.Header.Add(walSyncAuthorizationHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Error creating new remote: %s", body)
	}
	return nil
}
