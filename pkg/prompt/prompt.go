package prompt

import (
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/manifoldco/promptui"

	"github.com/sambigeara/fuzzynote/pkg/service"
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
	modeKey := fmt.Sprintf("Mode: %s", r.Mode)
	matchKey := fmt.Sprintf("Match: %s", r.Match)
	isActiveKey := fmt.Sprintf("IsActive: %v", r.IsActive)

	fields := []string{
		nameKey,
		modeKey,
		matchKey,
		isActiveKey,
	}

	// TODO make key -> json key mapping more robust
	updateFuncMap := map[string]func(string) error{
		nameKey: func(v string) error {
			r.Name = v
			return w.UpdateRemote(r)
		},
		modeKey: func(v string) error {
			r.Mode = v
			return w.UpdateRemote(r)
		},
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
		nameKey:     alwaysValid,
		modeKey:     alwaysValid,
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

	wt := service.NewFileWebTokenStore(root)
	err = service.Authenticate(wt, body)
	if err != nil {
		fmt.Print("Login unsuccessful :(\n")
		os.Exit(0)
	}

	w := service.NewWeb(wt)
	err = w.OverrideBaseUUID(service.NewLocalFileWalFile(root))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("Login successful!")
}
