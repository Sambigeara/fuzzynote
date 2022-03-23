package service

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"nhooyr.io/websocket"
)

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

type WebRemote struct {
	Emails       []string
	DTLastChange int64
}

type postRemoteResponse struct {
	ActiveFriends []string
}

// postRemote is responsible for both additions and deletions
func (w *Web) postRemote(remote *WebRemote, u *url.URL) ([]string, error) {
	body, err := json.Marshal(remote)
	if err != nil {
		return []string{}, err
	}

	req, err := http.NewRequest("POST", u.String(), strings.NewReader(string(body)))
	if err != nil {
		return []string{}, err
	}

	req.Header.Add(walSyncAuthorizationHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req)
	if err != nil {
		return []string{}, err
	}
	defer resp.Body.Close()

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{}, err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return []string{}, fmt.Errorf("Error creating new remote: %s", respBytes)
	}

	remoteResp := postRemoteResponse{}
	if err := json.Unmarshal(respBytes, &remoteResp); err != nil {
		return []string{}, err
	}

	return remoteResp.ActiveFriends, nil
}
