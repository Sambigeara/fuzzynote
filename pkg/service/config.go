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

const (
	ModePush = "push"
	ModePull = "pull"
	ModeSync = "sync"
)

const (
	yesKey          = "Yes"
	noKey           = "No"
	newRemoteKey    = "Add new remote..."
	manageCollabKey = "Manage collaborators..."
	archiveKey      = "Archive? WARNING: cannot be undone"
	addCollabKey    = "Add new collaborator..."
	exitKey         = "Exit"
	selectSize      = 20
)

type Web struct {
	wsConn     *websocket.Conn
	tokens     WebTokenStore
	walFileMap map[string]*WalFile
	uuid       uuid
}

func NewWeb(webTokens WebTokenStore) *Web {
	return &Web{
		tokens:     webTokens,
		walFileMap: make(map[string]*WalFile),
	}
}

//type remote struct {
//    //UUID  string
//    //Name  string
//    //Mode  Mode
//    Mode     Mode
//    Match    string
//    MatchAll bool
//}

type S3Remote struct {
	//remote
	Mode          string
	Match         string
	MatchAll      bool
	Key           string
	Secret        string
	Bucket        string
	Prefix        string
	RefreshFreqMs uint16
}

type WebRemote struct {
	//remote
	Email      string `json:"Email"`
	Name       string `json:"WalName"`
	UUID       string `json:"WalUUID"`
	Mode       string `json:"Mode"`
	Match      string `json:"Match"`
	MatchAll   bool   `json:"MatchAll"`
	IsOwner    bool   `json:"IsOwner"`
	IsActive   bool   `json:"IsActive"`
	IsArchived bool
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
	req.Header.Add(walSyncAuthorizationHeader, w.tokens.AccessToken())
	req.Header.Add(iDTokenHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Error retrieving remotes: %s", body)
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

	req.Header.Add(walSyncAuthorizationHeader, w.tokens.AccessToken())
	req.Header.Add(iDTokenHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req, walSyncAuthorizationHeader)
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

	req.Header.Add(walSyncAuthorizationHeader, w.tokens.AccessToken())
	req.Header.Add(iDTokenHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Failed to update key for item %v", remote)
	}
	return nil
}

func (w *Web) ArchiveRemote(uuid string) error {
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "remote", uuid, "archive")
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

	req.Header.Add(walSyncAuthorizationHeader, w.tokens.AccessToken())
	req.Header.Add(iDTokenHeader, w.tokens.IDToken())
	resp, err := w.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Error creating new remote: %s", respBody)
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
