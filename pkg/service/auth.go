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

type WebTokens struct {
	root    string
	Access  string `yaml:"accessToken"`
	Refresh string `yaml:"refreshToken"`
}

func GetWebTokens(root string) *WebTokens {
	// Attempt to read from file
	tokenFile := path.Join(root, webTokensFileName)
	f, err := os.Open(tokenFile)

	wt := &WebTokens{root: root}
	if err == nil {
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(wt)
		if err != nil {
			log.Fatalf("main : Parsing Token File : %v", err)
			// TODO handle with appropriate error message
			return wt
		}
		defer f.Close()
	}
	return wt
}

func (wt *WebTokens) Flush() {
	b, err := yaml.Marshal(&wt)
	if err != nil {
		log.Fatal(err)
	}

	tokenFile := path.Join(wt.root, webTokensFileName)
	f, err := os.Create(tokenFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	f.Write(b)
}

// CallWithReAuth accepts a pre-built request, attempts to call it, and if it fails authorisation due to an
// expired AccessToken, will reauth, and then retry the original function.
func (wt *WebTokens) CallWithReAuth(req *http.Request, header string) (*http.Response, error) {
	f := func(req *http.Request) (*http.Response, error) {
		client := &http.Client{}
		return client.Do(req)
	}
	resp, err := f(req)
	if err != nil {
		return nil, err
	}
	//if resp.StatusCode == http.StatusUnauthorized {
	if resp.StatusCode != http.StatusOK {
		body := map[string]string{
			"refreshToken": wt.Refresh,
		}
		marshalBody, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		err = wt.Authenticate(marshalBody)
		if err != nil {
			return nil, err
		}
		req.Header.Set(header, wt.Access)
		// TODO do we need to copy the request?
		resp, err = f(req)
	}
	return resp, err
}

var emailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

// isEmailValid checks if the email provided passes the required structure and length.
func isEmailValid(e string) bool {
	if len(e) < 3 && len(e) > 254 {
		return false
	}
	return emailRegex.MatchString(e)
}

func (wt *WebTokens) Authenticate(body []byte) error {
	resp, err := http.Post(authenticationURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		//log.Fatal(err)
		return err
	}

	if resp.StatusCode != http.StatusOK {
		//bodyString := string(bodyBytes)
		//fmt.Printf("Login failed: %s\n", bodyString)
		return err
	}

	var authResult cognito.AuthenticationResultType
	err = json.Unmarshal(bodyBytes, &authResult)
	if err != nil {
		log.Fatal(err)
	}
	if authResult.AccessToken != nil {
		wt.Access = *authResult.AccessToken
	}
	if authResult.RefreshToken != nil {
		wt.Refresh = *authResult.RefreshToken
	}
	wt.Flush()
	return nil
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

	// TODO refactor so WebTokens don't need to know about root
	wt := WebTokens{root: root}
	err = wt.Authenticate(marshalBody)
	if err != nil {
		fmt.Print("Login unsuccessful")
		os.Exit(1)
	}
	fmt.Print("Login successful!")
}
