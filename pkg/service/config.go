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
	Mode  string
	Match string
}

type s3Remote struct {
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

type remotes struct {
	S3 []s3Remote
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
