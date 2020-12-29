package service

import (
	"fmt"
	"log"
	"os"
)

// Wal represents the interface between the in-mem and persisted WAL
type Wal interface {
	Load() error
	Save() error
}

// WalFile is a file representation of the Wal interface
type WalFile struct {
	rootPath          string
	walPathPattern    string
	latestWalSchemaID uint16
}

const latestWalSchemaID uint16 = 1

// NewWalFile instantiates and returns a new WalFile instance
func NewWalFile(rootPath string, walPathPattern string) *WalFile {
	return &WalFile{
		rootPath:          rootPath,
		walPathPattern:    walPathPattern,
		latestWalSchemaID: latestWalSchemaID,
	}
}

type walHeader struct {
	schemaID fileSchemaID
}

type walItemSchema1 struct {
	dbUUID      uint32
	logID       uint64
	listItemID  uint32
	epochLength uint64
	epochDt     uint32
	eventType   eventType
	lineLength  uint64
	noteLength  uint64
}

func (w *WalFile) Load(uuid uuid) error {
	walFilePath := fmt.Sprintf(w.walPathPattern, uuid)

	f, err := os.OpenFile(walFilePath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	return nil
}

func (w *WalFile) Save(uuid uuid) error {
	if uuid == 0 {
		return nil
	}

	walFilePath := fmt.Sprintf(w.walPathPattern, uuid)

	f, err := os.Create(walFilePath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	return nil
}
