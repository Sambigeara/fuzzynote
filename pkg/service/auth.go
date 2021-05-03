package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"

	cognito "github.com/aws/aws-sdk-go/service/cognitoidentityprovider"
	"github.com/manifoldco/promptui"
	"gopkg.in/yaml.v2"
)

const (
	webTokensFileName = ".tokens.yml"
)

var emailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

// isEmailValid checks if the email provided passes the required structure and length.
func isEmailValid(e string) error {
	if len(e) < 3 && len(e) > 254 || !emailRegex.MatchString(e) {
		return errors.New("Invalid email address")
	}
	return nil
}

type WebTokenStore interface {
	SetAccessToken(string)
	SetRefreshToken(string)
	SetIDToken(string)
	AccessToken() string
	RefreshToken() string
	IDToken() string
	Flush()
}

type FileWebTokenStore struct {
	root    string
	Access  string `yaml:"accessToken"`
	Refresh string `yaml:"refreshToken"`
	ID      string `yaml:"idToken"`
}

func NewFileWebTokenStore(root string) *FileWebTokenStore {
	// Attempt to read from file
	tokenFile := path.Join(root, webTokensFileName)
	f, err := os.Open(tokenFile)

	wt := &FileWebTokenStore{root: root}
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

func (wt *FileWebTokenStore) SetAccessToken(s string)  { wt.Access = s }
func (wt *FileWebTokenStore) SetRefreshToken(s string) { wt.Refresh = s }
func (wt *FileWebTokenStore) SetIDToken(s string)      { wt.ID = s }
func (wt *FileWebTokenStore) AccessToken() string      { return wt.Access }
func (wt *FileWebTokenStore) RefreshToken() string     { return wt.Refresh }
func (wt *FileWebTokenStore) IDToken() string          { return wt.ID }
func (wt *FileWebTokenStore) Flush() {
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

func Authenticate(wt WebTokenStore, body []byte) error {
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "auth")
	resp, err := http.Post(u.String(), "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("Authentication unsuccessful, please try logging in")
	}

	var authResult cognito.AuthenticationResultType
	err = json.Unmarshal(bodyBytes, &authResult)
	if err != nil {
		return nil
	}
	if authResult.AccessToken != nil {
		wt.SetAccessToken(*authResult.AccessToken)
	}
	if authResult.RefreshToken != nil {
		wt.SetRefreshToken(*authResult.RefreshToken)
	}
	if authResult.IdToken != nil {
		wt.SetIDToken(*authResult.IdToken)
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

	wt := FileWebTokenStore{root: root}
	err = Authenticate(&wt, marshalBody)
	if err != nil {
		fmt.Print("Login unsuccessful :(\n")
		os.Exit(0)
	}
	fmt.Print("Login successful!")
}

// CallWithReAuth accepts a pre-built request, attempts to call it, and if it fails authorisation due to an
// expired AccessToken, will reauth, and then retry the original function.
func (w *Web) CallWithReAuth(req *http.Request, header string) (*http.Response, error) {
	f := func(req *http.Request) (*http.Response, error) {
		return http.DefaultClient.Do(req)
	}
	resp, err := f(req)
	if err != nil && resp.StatusCode != http.StatusUnauthorized {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		body := map[string]string{
			"refreshToken": w.tokens.RefreshToken(),
		}
		marshalBody, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		err = Authenticate(w.tokens, marshalBody)
		if err != nil {
			return nil, err
		}
		req.Header.Set(header, w.tokens.AccessToken())
		resp, err = f(req)
	}
	return resp, err
}
