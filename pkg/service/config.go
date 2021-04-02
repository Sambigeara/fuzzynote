package service

import (
	"log"
	"os"
	"path"

	"gopkg.in/yaml.v2"
)

type remote struct {
	UUID  string
	Name  string
	Mode  string
	Match []string
}

type s3Remote struct {
	Key           string
	Secret        string
	Bucket        string
	Prefix        string
	RefreshFreqMs uint16
	GatherFreqMs  uint16
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
			log.Fatalf("main : Parsing File Config : %v", err)
		}
		defer f.Close()
	}
	return r
}
