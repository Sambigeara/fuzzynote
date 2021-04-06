package service

import (
	"os"
	"path"

	"gopkg.in/yaml.v2"
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

type websocketRemote struct {
	// TODO use struct composition
	URLString string
	Mode      Mode
	Match     string
	MatchAll  bool
}

type remotes struct {
	S3        []s3Remote
	Websocket websocketRemote `yaml:",omitempty"`
}

func GetRemotesConfig(root string) remotes {
	cfgFile := path.Join(root, ".config.yml")
	f, err := os.Open(cfgFile)

	r := remotes{}

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
