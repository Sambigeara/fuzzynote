package prompt

import (
	"errors"
	"fmt"
	"os"

	"github.com/manifoldco/promptui"

	"github.com/sambigeara/fuzzynote/pkg/service"
)

// isEmailValid checks if the email provided passes the required structure and length.
func isEmailValid(e string) error {
	if len(e) < 3 && len(e) > 254 || !service.EmailRegex.MatchString(e) {
		return errors.New("Invalid email address")
	}
	return nil
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

	fmt.Print("Login successful!")
}
