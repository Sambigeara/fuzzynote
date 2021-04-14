package service

import (
	"log"
	"os"
	"path"

	"gopkg.in/yaml.v2"
)

const (
	configFileName    = "config.yml"
	webTokensFileName = ".tokens.yml"
)

type Mode string

const (
	Push Mode = "push"
	Pull Mode = "pull"
	Sync Mode = "sync"
)

type remote struct {
	//UUID  string
	//Name  string
	//Mode  Mode
	Mode     string
	Match    string
	MatchAll bool
}

type s3Remote struct {
	// TODO use struct composition
	Key           string
	Secret        string
	Bucket        string
	Prefix        string
	RefreshFreqMs uint16
	GatherFreqMs  uint16
	Mode          Mode
	Match         string
	MatchAll      bool
	//remote
}

type webRemote struct {
	// TODO use struct composition
	//WebsocketURL string
	Mode     Mode
	Match    string
	MatchAll bool
	//URL          string
}

type Remotes struct {
	S3  []s3Remote
	Web webRemote `yaml:",omitempty"`
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

type WebTokens struct {
	Access  string
	Refresh string
}

func GetWebTokens(root string) WebTokens {
	// Attempt to read from file
	tokenFile := path.Join(root, webTokensFileName)
	f, err := os.Open(tokenFile)

	wt := WebTokens{}
	if err == nil {
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(&wt)
		if err != nil {
			log.Fatalf("main : Parsing Token File : %v", err)
			// TODO handle with appropriate error message
			return wt
		}
		defer f.Close()
	}
	return wt
}
