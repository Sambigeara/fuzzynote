package service

import (
	"encoding/binary"
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
	logger            *WalEventLogger
}

const latestWalSchemaID uint16 = 1

// NewWalFile instantiates and returns a new WalFile instance
func NewWalFile(rootPath string, walPathPattern string, logger *WalEventLogger) *WalFile {
	return &WalFile{
		rootPath:          rootPath,
		walPathPattern:    walPathPattern,
		latestWalSchemaID: latestWalSchemaID,
		logger:            logger,
	}
}

type walHeader struct {
	schemaID fileSchemaID
}

type walItemSchema1 struct {
	dbUUID     uuid
	listItemID uint64
	epochDt    uint32
	eventType  eventType
	lineLength uint64
	noteLength uint64
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

	for _, item := range w.logger.log {
		lenLine := uint64(len([]byte(item.redoLine)))
		var lenNote uint64
		if item.redoNote != nil {
			lenNote = uint64(len(*(item.redoNote)))
		}
		data := []interface{}{
			walItemSchema1{
				dbUUID:     item.uuid,
				listItemID: item.ID,
				epochDt:    item.unixTime,
				eventType:  item.eventType,
				lineLength: lenLine,
				noteLength: lenNote,
			},
			[]byte(item.redoLine),
		}
		if item.redoNote != nil {
			data = append(data, item.redoNote)
		}
		for _, v := range data {
			err = binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				fmt.Printf("binary.Write failed when writing field for WAL log item %v: %s\n", v, err)
				log.Fatal(err)
				return err
			}
		}
	}

	return nil
}
