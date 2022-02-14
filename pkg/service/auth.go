package service

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"

	"gopkg.in/yaml.v2"
)

const (
	webTokensFileName = ".tokens.yml"
)

type WebTokenStore interface {
	SetEmail(string)
	SetRefreshToken(string)
	SetIDToken(string)
	Email() string
	RefreshToken() string
	IDToken() string
	Flush()
}

type FileWebTokenStore struct {
	root    string
	User    string `yaml:"user"`
	Refresh string `yaml:"refreshToken"`
	ID      string `yaml:"idToken"`
}

func NewFileWebTokenStore(root string) *FileWebTokenStore {
	// Attempt to read from file
	tokenFile := path.Join(root, webTokensFileName)
	f, err := os.Open(tokenFile)
	defer f.Close()

	wt := &FileWebTokenStore{root: root}
	if err == nil {
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(wt)
		if err != nil {
			log.Fatalf("main : Parsing Token File : %v", err)
			// TODO handle with appropriate error message
			return wt
		}
	}
	return wt
}

type authFailureError struct{}

func (e authFailureError) Error() string {
	return "Authentication unsuccessful, please try logging in"
}

// TODO reconsider this interface...
func (wt *FileWebTokenStore) SetEmail(s string)        { wt.User = s }
func (wt *FileWebTokenStore) SetRefreshToken(s string) { wt.Refresh = s }
func (wt *FileWebTokenStore) SetIDToken(s string)      { wt.ID = s }
func (wt *FileWebTokenStore) Email() string            { return wt.User }
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

type authenticationResultType struct {
	IdToken      *string `type:"string" sensitive:"true"`
	RefreshToken *string `type:"string" sensitive:"true"`
}

func Authenticate(wt WebTokenStore, args map[string]string) error {
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}

	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "auth")
	resp, err := http.Post(u.String(), "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return authFailureError{}
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var authResult authenticationResultType
	if err := json.Unmarshal(bodyBytes, &authResult); err != nil {
		return err
	}

	if email, ok := args["user"]; ok {
		wt.SetEmail(email)
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

// CallWithReAuth accepts a pre-built request, attempts to call it, and if it fails authorisation due to an
// expired IDToken, will reauth, and then retry the original function.
func (w *Web) CallWithReAuth(req *http.Request) (*http.Response, error) {
	resp, err := w.client.Do(req)
	if err != nil && (resp == nil || resp.StatusCode != http.StatusUnauthorized) {
		return nil, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		defer w.tokens.Flush()
		w.tokens.SetIDToken("")
		body := map[string]string{
			"refreshToken": w.tokens.RefreshToken(),
		}
		err = Authenticate(w.tokens, body)
		if err != nil {
			if _, ok := err.(authFailureError); ok {
				w.tokens.SetRefreshToken("")
				w.tokens.Flush()
			}
			return nil, err
		}
		req.Header.Set(walSyncAuthorizationHeader, w.tokens.IDToken())
		resp, err = w.client.Do(req)
	}
	return resp, err
}
