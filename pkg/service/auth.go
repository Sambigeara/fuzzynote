package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"

	cognito "github.com/aws/aws-sdk-go/service/cognitoidentityprovider"
	"gopkg.in/yaml.v2"
)

const (
	webTokensFileName = ".tokens.yml"
)

type WebTokenStore interface {
	SetAccessToken(string)
	SetRefreshToken(string)
	SetIDToken(string)
	AccessToken() string
	RefreshToken() string
	IDToken() string
	Flush(ctx interface{})
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
func (wt *FileWebTokenStore) Flush(ctx interface{}) {
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

func Authenticate(wt WebTokenStore, body []byte, ctx interface{}) error {
	u, _ := url.Parse(apiURL)
	u.Path = path.Join(u.Path, "auth")
	resp, err := http.Post(u.String(), "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New("Authentication unsuccessful, please try logging in")
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
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
	wt.Flush(ctx)
	return nil
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
		err = Authenticate(w.tokens, marshalBody, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set(header, w.tokens.AccessToken())
		resp, err = f(req)
	}
	return resp, err
}
