package service

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/manifoldco/promptui"
	"nhooyr.io/websocket"
)

const (
	websocketAuthorizationHeader = "Auth"
	walSyncAuthorizationHeader   = "Authorization"
	iDTokenHeader                = "Id-Token"
)

var (
	// TODO
	websocketURL      = "wss://4hlf98q6mh.execute-api.eu-west-1.amazonaws.com/prod"
	walSyncURL        = "https://jf7i5gi0f4.execute-api.eu-west-1.amazonaws.com/prod"
	authenticationURL = "https://tt2lmb4xla.execute-api.eu-west-1.amazonaws.com/prod"
	remoteURL         = "https://czcon196gf.execute-api.eu-west-1.amazonaws.com/prod"
)

type Web struct {
	wsConn     *websocket.Conn
	tokens     WebTokenStore
	walFileMap map[string]*WalFile
}

func NewWeb(webTokens WebTokenStore) *Web {
	return &Web{
		tokens:     webTokens,
		walFileMap: make(map[string]*WalFile),
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

func NewWebWalFile(cfg WebRemote, refreshFrequency uint16, gatherFrequency uint16, web *Web) *WebWalFile {
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
		GatherTicker:             time.NewTicker(time.Millisecond * time.Duration(gatherFrequency)),
	}
}

func (wf *WebWalFile) getPresignedURLForWal(originUUID string, uuid string, method string) (string, error) {
	u, _ := url.Parse(walSyncURL)
	q, _ := url.ParseQuery(u.RawQuery)
	q.Add("method", method)
	u.RawQuery = q.Encode()

	u.Path = path.Join(u.Path, "presigned", originUUID, uuid)

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
	u, _ := url.Parse(walSyncURL)
	u.Path = path.Join(u.Path, "wal", "list", wf.uuid)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		log.Printf("Error retrieving presigned URL: %s", err)
		return nil, nil
	}

	req.Header.Add(walSyncAuthorizationHeader, wf.web.tokens.AccessToken())
	req.Header.Add(iDTokenHeader, wf.web.tokens.IDToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Error retrieving presigned URL: %s", err)
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
	wal, err := b64.StdEncoding.DecodeString(string(b64Wal))
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
	req.Header.Add(iDTokenHeader, wf.web.tokens.IDToken())
	resp, err := wf.web.CallWithReAuth(req, walSyncAuthorizationHeader)
	if err != nil || resp.StatusCode != http.StatusOK {
		errBody, _ := ioutil.ReadAll(resp.Body)
		log.Fatalf("resp body %s", errBody)
	}
	resp.Body.Close()
	return nil
}

func (wf *WebWalFile) Flush(b *bytes.Buffer, fileName string) error {
	// TODO refactor to pass only UUID, rather than full path (currently blocked by all WalFile != WebWalFile
	partialWal := strings.Split(strings.Split(fileName, "_")[1], ".")[0]

	presignedURL, err := wf.getPresignedURLForWal(wf.uuid, partialWal, "put")
	if err != nil || presignedURL == "" {
		return nil
	}

	b64Wal := b64.StdEncoding.EncodeToString(b.Bytes())

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
	var resp *http.Response
	w.wsConn, resp, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
	if err != nil && resp.StatusCode != http.StatusUnauthorized {
		b, _ := ioutil.ReadAll(resp.Body)
		log.Fatalf("Error establishing websocket connection: %s", b)
	}
	// TODO re-authentication explicitly handled here as wss handshake only occurs once (doesn't require
	// retries).
	// TODO can definite dedup at least a little
	if resp.StatusCode == http.StatusUnauthorized {
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
		w.wsConn, resp, err = websocket.Dial(ctx, wsURI.String(), &websocket.DialOptions{HTTPHeader: header})
		if err != nil {
			b, _ := ioutil.ReadAll(resp.Body)
			log.Fatalf("Error establishing websocket connection: %s", b)
		}
	}
}

type message struct {
	UUID string `json:"uuid"`
	Wal  string `json:"wal"`
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
	//data := map[string]string{
	//    "uuid": uuid,
	//    "wal":  b64Wal,
	//}
	m := message{
		UUID: uuid,
		Wal:  b64Wal,
	}
	marshalData, err := json.Marshal(m)
	if err != nil {
		// Fail silently for now, rely on future syncs sorting out discrepencies
		log.Println("push websocket:", err)
		return
	}
	err = w.wsConn.Write(ctx, websocket.MessageText, []byte(marshalData))
	if err != nil {
		log.Println("push websocket:", err)
		return
	}
}

func (w *Web) consumeWebsocket(walChan chan *[]EventLog) error {
	var err error
	// Might take a while for an event to come in, so timeouts aren't super useful here
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, body, err := w.wsConn.Read(ctx)
	if err != nil {
		// TODO this generally errors on close, so commenting out the log line for now
		//log.Println("read:", err)
	}
	var m message
	err = json.Unmarshal(body, &m)
	if err != nil {
		return nil
	}
	strWal, _ := b64.StdEncoding.DecodeString(string(m.Wal))

	buf := bytes.NewBuffer([]byte(strWal))
	el, err := BuildFromFile(buf)
	if err == nil {
		walChan <- &el

		// Acknowledge the event in the WalFile event cache, so we know to emit further events even
		// if the match term doesn't match
		wf := *w.walFileMap[m.UUID]
		key, _ := el[0].getKeys()
		wf.SetProcessedEvent(key)
	}

	return err
}

// TODO move this somewhere better - it's separate from normal business logic
func (w *Web) GetRemotes(uuid string, u *url.URL) ([]WebRemote, error) {
	// This just allows callers to override the URL
	if u == nil {
		u, _ = url.Parse(remoteURL)

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

// postRemote is responsible for both additions and deletions
func (w *Web) postRemote(remote *WebRemote, u *url.URL) error {
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

func (w *Web) updateRemote(remote WebRemote) error {
	u, _ := url.Parse(remoteURL)
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

func (w *Web) getUsersForRemote(uuid string) ([]string, error) {
	u, _ := url.Parse(remoteURL)
	u.Path = path.Join(u.Path, "remote", uuid, "user", "list")

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

func (w *Web) addUserToRemote(uuid string, email string) error {
	u, _ := url.Parse(remoteURL)
	u.Path = path.Join(u.Path, "remote", uuid, "user", "add")
	remote := WebRemote{
		Email: email,
	}
	return w.postRemote(&remote, u)
}

func (w *Web) deleteUserFromRemote(uuid string, email string) error {
	// We never actually delete a remote completely, but this function is removing certain non-owner users
	// from a given remote
	u, _ := url.Parse(remoteURL)
	u.Path = path.Join(u.Path, "remote", uuid, "user", "delete")
	remote := WebRemote{
		Email: email,
	}
	return w.postRemote(&remote, u)
}

// TODO rename
func (w *Web) getRemoteFields(r WebRemote) ([]string, map[string]func(string) error, map[string]func(string) error) {
	nameKey := fmt.Sprintf("Name: %s", r.Name)
	modeKey := fmt.Sprintf("Mode: %s", r.Mode)
	matchKey := fmt.Sprintf("Match: %s", r.Match)
	matchAllKey := fmt.Sprintf("MatchAll: %v", r.MatchAll)
	isActiveKey := fmt.Sprintf("IsActive: %v", r.IsActive)

	fields := []string{
		nameKey,
		modeKey,
		matchKey,
		matchAllKey,
		isActiveKey,
	}

	// TODO make key -> json key mapping more robust
	updateFuncMap := map[string]func(string) error{
		nameKey: func(v string) error {
			r.Name = v
			return w.updateRemote(r)
		},
		modeKey: func(v string) error {
			r.Mode = Mode(v)
			return w.updateRemote(r)
		},
		matchKey: func(v string) error {
			r.Match = v
			return w.updateRemote(r)
		},
		matchAllKey: func(v string) error {
			r.MatchAll = false
			if v == "true" {
				r.MatchAll = true
			}
			return w.updateRemote(r)
		},
		isActiveKey: func(v string) error {
			r.IsActive = false
			if v == "true" {
				r.IsActive = true
			}
			return w.updateRemote(r)
		},
	}

	defaultTrue := func(v string) error { return nil }
	boolValidationFn := func(v string) error {
		if v != "true" && v != "false" {
			return errors.New("")
		}
		return nil
	}
	validationFuncMap := map[string]func(string) error{
		nameKey:     defaultTrue,
		modeKey:     defaultTrue,
		matchKey:    defaultTrue,
		matchAllKey: boolValidationFn,
		isActiveKey: boolValidationFn,
	}

	return fields, updateFuncMap, validationFuncMap
}

// LaunchRemotesCLI launches the interactive Remote management CLI tool
func (w *Web) LaunchRemotesCLI() {
	defer os.Exit(0)

	const (
		yesKey          = "Yes"
		noKey           = "No"
		newRemoteKey    = "Add new remote..."
		manageCollabKey = "Manage collaborators..."
		addCollabKey    = "Add new collaborator..."
		exitKey         = "Exit..."
		selectSize      = 20
	)

	// Generate a map of remotes
	for {
		remotes, err := w.GetRemotes("", nil)
		if err != nil {
			log.Fatalf("%s", err)
		}

		remotesSelectOptions := []string{}
		remoteMap := make(map[string]WebRemote)
		for _, r := range remotes {
			key := fmt.Sprintf("(%s)", r.UUID)
			if r.Name != "" {
				key = fmt.Sprintf("%s (%s)", r.Name, r.UUID)
			}
			key = fmt.Sprintf("Remote: %s", key)
			remotesSelectOptions = append(remotesSelectOptions, key)
			remoteMap[key] = r
		}
		remotesSelectOptions = append(remotesSelectOptions, newRemoteKey, exitKey)

		sel := promptui.Select{
			Label: "Select action",
			Items: remotesSelectOptions,
			Size:  selectSize,
		}

		_, result, err := sel.Run()
		if err != nil {
			return
		}

		if result == exitKey {
			fmt.Print("Goodbye!")
			os.Exit(0)
		} else if result == newRemoteKey {
			// Add a new named remote then cycle back round
			prompt := promptui.Prompt{
				Label: "Specify name for new remote",
			}
			newName, err := prompt.Run()
			if err != nil {
				fmt.Printf("Prompt failed %v\n", err)
				os.Exit(1)
			}

			u, _ := url.Parse(remoteURL)
			u.Path = path.Join(u.Path, "remote")
			remote := WebRemote{
				Name: newName,
				UUID: fmt.Sprintf("%d", generateUUID()),
			}
			err = w.postRemote(&remote, u)
			if err != nil {
				fmt.Printf("%v", err)
				os.Exit(1)
			}
			continue
		}

		remote := remoteMap[result]
		fields, updateFuncMap, validationFuncMap := w.getRemoteFields(remote)
		fields = append([]string{manageCollabKey}, fields...)
		fields = append(fields, exitKey)
		for {
			sel = promptui.Select{
				Label: result,
				Items: fields,
				Size:  selectSize,
			}

			// This result will be the key to update
			idx, result, err := sel.Run()
			if err != nil {
				return
			}

			if result == exitKey {
				break
			} else if result == manageCollabKey {
				userFields, err := w.getUsersForRemote(remote.UUID)
				if err != nil {
					return
				}

				userFields = append(userFields, addCollabKey, exitKey)
				sel = promptui.Select{
					Label: "Manage collaborators",
					Items: userFields,
					Size:  selectSize,
				}

				// This result will be the key to update
				_, emailResult, err := sel.Run()
				if err != nil {
					return
				}

				if emailResult == exitKey {
					continue
				} else if emailResult == addCollabKey {
					prompt := promptui.Prompt{
						Label:    "Enter email address",
						Validate: isEmailValid,
					}
					newEmail, err := prompt.Run()
					if err != nil {
						fmt.Printf("Prompt failed %v\n", err)
						os.Exit(1)
					}
					if err = w.addUserToRemote(remote.UUID, newEmail); err != nil {
						fmt.Printf("Failed to add new collaborator: %s", err)
						os.Exit(1)
					}
				} else {
					// Bring up Delete yes/no option
					sel = promptui.Select{
						Label: "Delete?",
						Items: []string{yesKey, noKey},
						Size:  selectSize,
					}

					_, deleteResult, err := sel.Run()
					if err != nil {
						return
					}

					if deleteResult == yesKey {
						if err = w.deleteUserFromRemote(remote.UUID, emailResult); err != nil {
							fmt.Printf("Failed to delete collaborator: %s", err)
							os.Exit(1)
						}
					}
				}
				continue
			}

			// For `Mode` or boolean selection, use a nested Select prompt with the appropriate fields
			field := strings.Split(result, ":")[0]
			newVal := ""
			if field == "Mode" {
				sel = promptui.Select{
					Label: "Select Mode",
					Items: []string{string(ModeSync), string(ModePush), string(ModePull), exitKey},
					Size:  selectSize,
				}
				_, newVal, err = sel.Run()
				if err != nil {
					return
				}
				if result == exitKey {
					break
				}
			} else if field == "IsActive" || field == "MatchAll" {
				sel = promptui.Select{
					Label: "Select",
					Items: []string{"true", "false", exitKey},
					Size:  selectSize,
				}
				_, newVal, err = sel.Run()
				if err != nil {
					return
				}
				if newVal == exitKey {
					break
				}
			} else {
				// Trigger a prompt to the user to enter a new value
				prompt := promptui.Prompt{
					Label:    "Enter new value",
					Validate: validationFuncMap[result],
				}
				newVal, err = prompt.Run()
				if err != nil {
					fmt.Printf("Prompt failed %v\n", err)
					os.Exit(1)
				}
			}

			// Retrieve the update function from the updateFuncMap
			f := updateFuncMap[result]

			// Update the field name in the UI
			// TODO get rid of this shameful hack
			parts := strings.Split(fields[idx], ": ")
			fields[idx] = fmt.Sprintf("%s: %s", parts[0], newVal)

			err = f(newVal)
			if err != nil {
				fmt.Printf("Update failed %v\n", err)
				os.Exit(1)
			}
		}
	}
}
