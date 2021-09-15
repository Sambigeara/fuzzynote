package service

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
	"nhooyr.io/websocket"
)

const (
	configFileName = "config.yml"
)

type Web struct {
	wsConn     *websocket.Conn
	tokens     WebTokenStore
	walFileMap map[string]*WalFile
	uuid       string
}

func NewWeb(webTokens WebTokenStore) *Web {
	return &Web{
		tokens:     webTokens,
		walFileMap: make(map[string]*WalFile),
	}
}

type S3Remote struct {
	//remote
	//Mode   string
	Match  string
	Key    string
	Secret string
	Bucket string
	Prefix string
}

type WebRemote struct {
	//remote
	Email    string `json:"Email"`
	Name     string `json:"WalName"`
	UUID     string `json:"WalUUID"`
	Mode     string `json:"Mode"`
	Match    string `json:"Match"`
	IsOwner  bool   `json:"IsOwner"`
	IsActive bool   `json:"IsActive"`
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

// OverrideBaseUUID will attempt to retrieve an existing UUID to set and store within the primary.db file
// If we don't do this, we randomly generate a UUID and add a remote each time we log in.
// Only use an existing UUID if there is a remote with empty match term, otherwise
// maintain the randomly generated UUID.
func (w *Web) OverrideBaseUUID(localWalFile LocalWalFile) error {
	remotes, err := w.GetRemotes("", nil)
	if err != nil {
		return err
	}
	if len(remotes) > 0 {
		var baseUUID64 uint64
		for _, r := range remotes {
			if r.Match == "" {
				var err error
				baseUUID64, err = strconv.ParseUint(r.UUID, 10, 32)
				if err != nil {
					return err
				}
				break
			}
		}
		if baseUUID64 != 0 {
			localWalFile.SetBaseUUID(uint32(baseUUID64))
		}
	}
	return nil
}

// TODO move this somewhere better - it's separate from normal business logic
func (w *Web) GetRemotes(uuid string, u *url.URL) ([]WebRemote, error) {
	// This just allows callers to override the URL
	if u == nil {
		u, _ = url.Parse(apiURL)

		p := path.Join(u.Path, "remote")
		if uuid != "" {
			p = path.Join(p, uuid)
		}
		u.Path = p
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	req.Header.Add(walSyncAuthorizationHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Not authorized, please try logging in")
	}
	remotes := []WebRemote{}
	err = json.Unmarshal(body, &remotes)
	if err != nil {
		return nil, err
	}
	return remotes, nil
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

func (w *Web) UpdateRemote(remote WebRemote) error {
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "remote", remote.UUID)

	body, err := json.Marshal(remote)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", u.String(), strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.Header.Add(walSyncAuthorizationHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Failed to update key for item %v", remote)
	}
	return nil
}

func (w *Web) DeleteRemote(uuid string) error {
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "remote", uuid, "delete")
	remote := WebRemote{
		UUID: uuid,
	}

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

	if resp.StatusCode != http.StatusOK {
		respBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Error deleting remote: %s", respBody)
	}
	return nil
}

func (w *Web) GetUsersForRemote(uuid string) ([]string, error) {
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "remote", uuid, "users")

	remotes, err := w.GetRemotes(uuid, u)
	if err != nil {
		return nil, err
	}
	emails := []string{}
	for _, r := range remotes {
		// Don't return owner, as owner can't delete self
		if !r.IsOwner {
			emails = append(emails, r.Email)
		}
	}
	return emails, nil
}

func (w *Web) AddUserToRemote(uuid string, email string) error {
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "remote", uuid, "users")
	remote := WebRemote{
		Email: email,
	}
	return w.PostRemote(&remote, u)
}

func (w *Web) DeleteUserFromRemote(uuid string, email string) error {
	// We never actually delete a remote completely, but this function is removing certain non-owner users
	// from a given remote
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "remote", uuid, "users", "delete")
	remote := WebRemote{
		Email: email,
	}
	return w.PostRemote(&remote, u)
}

func (r *WebRemote) Key() string {
	key := fmt.Sprintf("(%s)", r.UUID)
	if r.Name != "" {
		key = fmt.Sprintf("%s (%s)", r.Name, r.UUID)
	}
	key = fmt.Sprintf("Remote: %s", key)
	return key
}
