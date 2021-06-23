package prompt

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/manifoldco/promptui"

	"github.com/sambigeara/fuzzynote/pkg/service"
)

const (
	yesKey          = "Yes"
	noKey           = "No"
	newRemoteKey    = "Add new remote..."
	manageCollabKey = "Manage collaborators..."
	deleteBaseKey   = "Delete?"
	addCollabKey    = "Add new collaborator..."
	exitKey         = "Exit"
	selectSize      = 20

	// TODO dedup (currently in service package too)
	websocketURL = "wss://ws.fuzzynote.co.uk/v1"
	apiURL       = "https://api.fuzzynote.co.uk/v1"

	nAllowedActiveRemotes = 5
)

var emailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

// isEmailValid checks if the email provided passes the required structure and length.
func isEmailValid(e string) error {
	if len(e) < 3 && len(e) > 254 || !emailRegex.MatchString(e) {
		return errors.New("Invalid email address")
	}
	return nil
}

// TODO rename
func getRemoteFields(w *service.Web, r service.WebRemote) ([]string, map[string]func(string) error, map[string]func(string) error) {
	nameKey := fmt.Sprintf("Name: %s", r.Name)
	//modeKey := fmt.Sprintf("Mode: %s", r.Mode)
	matchKey := fmt.Sprintf("Match: %s", r.Match)
	isActiveKey := fmt.Sprintf("IsActive: %v", r.IsActive)

	fields := []string{
		nameKey,
		//modeKey,
		matchKey,
		isActiveKey,
	}

	// TODO make key -> json key mapping more robust
	updateFuncMap := map[string]func(string) error{
		nameKey: func(v string) error {
			r.Name = v
			return w.UpdateRemote(r)
		},
		//modeKey: func(v string) error {
		//    r.Mode = v
		//    return w.UpdateRemote(r)
		//},
		matchKey: func(v string) error {
			r.Match = v
			return w.UpdateRemote(r)
		},
		isActiveKey: func(v string) error {
			r.IsActive = false
			if v == "true" {
				r.IsActive = true
			}
			return w.UpdateRemote(r)
		},
	}

	alwaysValid := func(v string) error { return nil }
	boolValidationFn := func(v string) error {
		if v != "true" && v != "false" {
			return errors.New("")
		}
		return nil
	}
	validationFuncMap := map[string]func(string) error{
		nameKey: alwaysValid,
		//modeKey:     alwaysValid,
		matchKey:    alwaysValid,
		isActiveKey: boolValidationFn,
	}

	return fields, updateFuncMap, validationFuncMap
}

// Login starts an interacting CLI flow to accept user credentials, and uses them to try and authenticate.
// If successful, the access and refresh tokens will be stored in memory, and persisted locally (dependent on the
// client).
func Login(root string) {
	defer os.Exit(0)

	prompt := promptui.Prompt{
		Label:    "Enter email",
		Validate: isEmailValid,
	}
	email, err := prompt.Run()
	if err != nil {
		fmt.Printf("Prompt failed %v\n", err)
		os.Exit(1)
	}

	prompt = promptui.Prompt{
		Label: "Enter password",
		Validate: func(input string) error {
			if len(input) < 6 {
				return errors.New("Password must have more than 6 characters")
			}
			return nil
		},
		Mask: '*',
	}
	password, err := prompt.Run()
	if err != nil {
		fmt.Printf("Prompt failed %v\n", err)
		os.Exit(1)
	}

	// Attempt to authenticate
	body := map[string]string{
		"user":     email,
		"password": password,
	}
	marshalBody, err := json.Marshal(body)

	wt := service.NewFileWebTokenStore(root)
	err = service.Authenticate(wt, marshalBody, nil)
	if err != nil {
		fmt.Print("Login unsuccessful :(\n")
		os.Exit(0)
	}

	w := service.NewWeb(wt)
	err = w.OverrideBaseUUID(service.NewLocalFileWalFile(root), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("Login successful!")
}

// LaunchRemotesCLI launches the interactive Remote management CLI tool
func LaunchRemotesCLI(w *service.Web, wf *service.LocalFileWalFile) {
	defer os.Exit(0)

	// Load the local walfile to ensure that a base UUID is already set (or set one if it isn't)
	fakeCtx := ""
	baseUUID, err := wf.Load(fakeCtx)
	if err != nil {
		log.Fatal(err)
	}

	// Generate a map of remotes
	for {
		remotes, err := w.GetRemotes("", nil)
		if err != nil {
			log.Fatalf("%s", err)
		}

		// If none of the remotes share the same UUID as the current baseUUID, set the local to match, with
		// precedence towards a remote with no match term set
		uuidMatches := false
		var matchAllUUID uint32
		for i, r := range remotes {
			if r.UUID == fmt.Sprintf("%d", baseUUID) {
				uuidMatches = true
				break
			}
			// If a remote has no match term set, we can use it to override the local baseUUID if necessary.
			// If there is no matchAll remote, set it to an arbitrary one (the last one will do)
			if matchAllUUID == 0 && (r.Match == "" || i == len(remotes)-1) {
				uuid64, err := strconv.ParseUint(r.UUID, 10, 32)
				if err != nil {
					log.Fatal(err)
				}
				matchAllUUID = uint32(uuid64)
			}
		}
		if !uuidMatches {
			wf.SetBaseUUID(matchAllUUID, fakeCtx)
		}

		remotesSelectOptions := []string{}
		remoteMap := make(map[string]service.WebRemote)
		for _, r := range remotes {
			remotesSelectOptions = append(remotesSelectOptions, r.Key())
			remoteMap[r.Key()] = r
		}

		nActiveRemotes := 0
		for _, r := range remotes {
			if r.IsActive {
				nActiveRemotes++
			}
		}
		if nActiveRemotes < nAllowedActiveRemotes {
			remotesSelectOptions = append(remotesSelectOptions, newRemoteKey)
		}
		remotesSelectOptions = append(remotesSelectOptions, exitKey)

		sel := promptui.Select{
			Label: "Select action",
			Items: remotesSelectOptions,
			Size:  selectSize,
		}

		_, remoteResult, err := sel.Run()
		if err != nil {
			return
		}

		if remoteResult == exitKey {
			fmt.Print("Goodbye!")
			os.Exit(0)
		} else if remoteResult == newRemoteKey {
			// Add a new named remote then cycle back round
			prompt := promptui.Prompt{
				Label: "Specify name for new remote",
			}
			newName, err := prompt.Run()
			if err != nil {
				fmt.Printf("Prompt failed %v\n", err)
				os.Exit(1)
			}

			u, _ := url.Parse(apiURL)
			u.Path = path.Join(u.Path, "remote")
			remote := service.WebRemote{
				Name: newName,
				UUID: fmt.Sprintf("%d", rand.Uint32()),
			}
			err = w.PostRemote(&remote, u)
			if err != nil {
				fmt.Printf("%v", err)
				os.Exit(1)
			}
			continue
		}

		for {
			remote := remoteMap[remoteResult]
			fields, updateFuncMap, validationFuncMap := getRemoteFields(w, remote)
			deleteKey := deleteBaseKey
			if remote.IsOwner {
				fields = append([]string{manageCollabKey}, fields...)
				deleteKey += " (for all collaborators)"
			}
			fields = append(fields, deleteKey)
			fields = append(fields, exitKey)

			sel = promptui.Select{
				Label: remoteResult,
				Items: fields,
				Size:  selectSize,
			}

			// This result will be the key to update
			var resultField string
			//idx, resultField, err = sel.Run()
			_, resultField, err = sel.Run()
			if err != nil {
				return
			}

			if resultField == exitKey {
				break
			} else if resultField == manageCollabKey {
				userFields, err := w.GetUsersForRemote(remote.UUID)
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
					if err = w.AddUserToRemote(remote.UUID, newEmail); err != nil {
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
						if err = w.DeleteUserFromRemote(remote.UUID, emailResult); err != nil {
							fmt.Printf("Failed to delete collaborator: %s", err)
							os.Exit(1)
						}
					}
				}
				continue
			} else if resultField == deleteKey {
				// TODO dedup
				// Bring up Delete yes/no option
				sel = promptui.Select{
					Label: "Are you sure?",
					Items: []string{yesKey, noKey},
					Size:  selectSize,
				}

				_, deleteResult, err := sel.Run()
				if err != nil {
					return
				}

				if deleteResult == yesKey {
					if err = w.DeleteRemote(remote.UUID); err != nil {
						fmt.Printf("Failed to delete remote: %s", err)
						os.Exit(1)
					}
				}
				break
			}

			// For `Mode` or boolean selection, use a nested Select prompt with the appropriate fields
			field := strings.Split(resultField, ":")[0]
			newVal := ""
			if field == "Mode" {
				sel = promptui.Select{
					Label: "Select Mode",
					// ModePush temporarily disabled until infra changes are done
					//Items: []string{string(ModeSync), string(ModePush), string(ModePull), exitKey},
					Items: []string{string(service.ModeSync), string(service.ModePull), exitKey},
					Size:  selectSize,
				}
				_, newVal, err = sel.Run()
				if err != nil {
					return
				}
				if newVal == exitKey {
					break
				}
			} else if field == "IsActive" {
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
					Validate: validationFuncMap[resultField],
				}
				newVal, err = prompt.Run()
				if err != nil {
					fmt.Printf("Prompt failed %v\n", err)
					os.Exit(1)
				}
			}

			// Retrieve the update function from the updateFuncMap
			f := updateFuncMap[resultField]

			err = f(newVal)
			if err != nil {
				fmt.Printf("Update failed %v\n", err)
				os.Exit(1)
			}

			// TODO this can be done without needing to call out to the API for updates but for now this will do.
			// Refresh the remote in the map
			newRemote, err := w.GetRemotes(remote.UUID, nil)
			if err != nil {
				fmt.Printf("Update failed %v\n", err)
				os.Exit(1)
			}
			// Will only return single remote, call idx 0
			remoteMap[remoteResult] = newRemote[0]
		}
	}
}
