package service

import (
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
	Mode     Mode
	Match    string
	MatchAll bool
}

type s3Remote struct {
	//remote
	Mode          Mode
	Match         string
	MatchAll      bool
	Key           string
	Secret        string
	Bucket        string
	Prefix        string
	RefreshFreqMs uint16
	GatherFreqMs  uint16
}

type webRemote struct {
	//remote
	Mode     Mode
	Match    string
	MatchAll bool
}

// Remotes represent a single remote Wal target (rather than a type), and the config lists
// all within a single configuration (listed by category)
type Remotes struct {
	S3  []s3Remote
	Web []webRemote
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
