package service

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v2"
	"nhooyr.io/websocket"
)

const (
	configFileName = "config.yml"
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

type S3Remote struct {
	Key    string
	Secret string
	Bucket string
	Prefix string
}

type WebRemote struct {
	Emails       []string
	DTLastChange int64
}

// Remotes represent a single remote Wal target (rather than a type), and the config lists
// all within a single configuration (listed by category)
type Remotes struct {
	S3 []S3Remote
}

func GetS3Config(root string) []S3Remote {
	cfgFile := path.Join(root, configFileName)
	f, err := os.Open(cfgFile)

	r := Remotes{}
	if err == nil {
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(&r)
		if err != nil {
			//log.Fatalf("main : Parsing File Config : %v", err)
			// TODO handle with appropriate error message
			return r.S3
		}
		defer f.Close()
	}
	return r.S3
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
