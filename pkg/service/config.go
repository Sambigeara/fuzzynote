package service

import (
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

	"github.com/manifoldco/promptui"
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

//type remote struct {
//    //UUID  string
//    //Name  string
//    //Mode  Mode
//    Mode     Mode
//    Match    string
//    MatchAll bool
//}

type s3Remote struct {
	//remote
	Mode          string
	Match         string
	MatchAll      bool
	Key           string
	Secret        string
	Bucket        string
	Prefix        string
	RefreshFreqMs uint16
	GatherFreqMs  uint16
}

type WebRemote struct {
	//remote
	Email    string `json:"Email"`
	Name     string `json:"WalName"`
	UUID     string `json:"WalUUID"`
	Mode     string `json:"Mode"`
	Match    string `json:"Match"`
	MatchAll bool   `json:"MatchAll"`
	IsOwner  bool   `json:"IsOwner"`
	IsActive bool   `json:"IsActive"`
}

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

// Remotes represent a single remote Wal target (rather than a type), and the config lists
// all within a single configuration (listed by category)
type Remotes struct {
	S3 []s3Remote
}

func GetRemotesConfig(root string) Remotes {
	cfgFile := path.Join(root, configFileName)
	f, err := os.Open(cfgFile)

	r := Remotes{}
	if err == nil {
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(&r)
		if err != nil {
			//log.Fatalf("main : Parsing File Config : %v", err)
			// TODO handle with appropriate error message
			return r
		}
		defer f.Close()
	}
	return r
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
			r.Mode = v
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
