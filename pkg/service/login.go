package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"

	cognito "github.com/aws/aws-sdk-go/service/cognitoidentityprovider"
	"github.com/manifoldco/promptui"
	"gopkg.in/yaml.v2"
)

var emailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

// isEmailValid checks if the email provided passes the required structure and length.
func isEmailValid(e string) bool {
	if len(e) < 3 && len(e) > 254 {
		return false
	}
	return emailRegex.MatchString(e)
}

func FlushWebTokens(wt WebTokens, root string) {
	b, err := yaml.Marshal(&wt)
	if err != nil {
		log.Fatal(err)
	}

	tokenFile := path.Join(root, webTokensFileName)
	f, err := os.Create(tokenFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	f.Write(b)
}

// Login starts an interacting CLI flow to accept user credentials, and uses them to try and authenticate.
// If successful, the access and refresh tokens will be stored in memory, and persisted locally (dependent on the
// client).
func Login(root string) {
	defer os.Exit(0)

	prompt := promptui.Prompt{
		Label: "Enter email",
		Validate: func(input string) error {
			if !isEmailValid(input) {
				return errors.New("Invalid email address")
			}
			return nil
		},
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

	resp, err := http.Post(authenticationURL, "application/json", bytes.NewBuffer(marshalBody))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	if resp.StatusCode != 200 {
		//bodyString := string(bodyBytes)
		//fmt.Printf("Login failed: %s\n", bodyString)
		fmt.Print("Login unsuccessful")
		os.Exit(1)
	}

	var authResult cognito.AuthenticationResultType
	err = json.Unmarshal(bodyBytes, &authResult)
	if err != nil {
		log.Fatal(err)
	}

	// If successful, override the token file
	wt := WebTokens{
		Access:  *authResult.AccessToken,
		Refresh: *authResult.RefreshToken,
	}

	FlushWebTokens(wt, root)

	fmt.Print("Login successful!")
}
