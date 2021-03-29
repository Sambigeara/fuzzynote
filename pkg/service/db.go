package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

type fileHeader struct {
	SchemaID fileSchemaID
	UUID     uuid
	// TODO introduce migration to remove this legacy ID
	NextListItemID uint64
}

type listItemSchema1 struct {
	PageID     uint32
	Metadata   bits
	LineLength uint64
	NoteLength uint64
}

// Start instantiates the app and begins push/pull for all WalFiles
func (r *DBListRepo) Start(walChan chan *[]EventLog) error {
	f, err := os.OpenFile(r.rootPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	fileHeader := fileHeader{}
	err = binary.Read(f, binary.LittleEndian, &fileHeader)
	if err != nil {
		// For initial load cases (first time an app is run) to beat an edge case race condition
		// (loading two apps in a fresh root without saves) we need to flush state to the primary.db
		// file. This prevents initial apps getting confused and generating different WAL UUIDs (thus
		// ultimately leading to data loss)
		if err == io.EOF {
			r.flushPrimary(f)
		} else {
			log.Fatal(err)
			return err
		}
	}

	// We can now override uuid as it's been read from the file
	// TODO UUID should always be set now, so this `if UUID != 0` check can go, but currently breaks
	// tests if I do remove.
	if fileHeader.UUID != 0 {
		r.wal.uuid = fileHeader.UUID
	}

	return r.wal.startSync(walChan)
}

func (r *DBListRepo) flushPrimary(f *os.File) error {
	// Truncate and move to start of file just in case
	f.Truncate(0)
	f.Seek(0, io.SeekStart)

	// Write the file header to the start of the file
	fileHeader := fileHeader{
		SchemaID: r.latestFileSchemaID,
		UUID:     r.wal.uuid,
	}
	err := binary.Write(f, binary.LittleEndian, &fileHeader)
	if err != nil {
		fmt.Println("binary.Write failed when writing fileHeader:", err)
		log.Fatal(err)
		return err
	}
	return nil
}

// Stop is called on app shutdown. It flushes all state changes in memory to disk
func (r *DBListRepo) Stop() error {
	f, err := os.Create(r.rootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	err = r.flushPrimary(f)
	if err != nil {
		return err
	}

	r.wal.finish()

	return nil
}

func (r *DBListRepo) RegisterWalFile(wf WalFile) {
	r.wal.walFiles = append(r.wal.walFiles, wf)
}
