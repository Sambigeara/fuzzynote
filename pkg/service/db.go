package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"
	//"runtime"
)

type fileHeader struct {
	SchemaID       fileSchemaID
	UUID           uuid
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
		SchemaID:       r.latestFileSchemaID,
		UUID:           r.wal.uuid,
		NextListItemID: r.NextID,
	}
	err := binary.Write(f, binary.LittleEndian, &fileHeader)
	if err != nil {
		fmt.Println("binary.Write failed when writing fileHeader:", err)
		log.Fatal(err)
		return err
	}
	return nil
}

// WARNING use with caution
// This is an emergency function which will generate a new Wal based off of the ListItems in the app.
// The output will be a wal consisting of addEvents and hideEvents
func (r *DBListRepo) forceOverrideWal() {
	overrideWal := []EventLog{}
	// We need to implement a nasty and non-guaranteed hack here, but it's only super
	// temporary. In order to ensure uniqueness, each record needs to have it's own UUID.
	// We don't know how many records there will be without traversing, and because I'm
	// lazy, it's easier just to preset `now` to 1 year ago and increment a nanosecond
	// at a time
	now := time.Now().AddDate(-1, 0, 0).UnixNano()

	// Go to the oldest item
	item := r.Root
	if item == nil {
		return
	}
	for item.parent != nil {
		item = item.parent
	}

	for item != nil {
		el := EventLog{
			uuid:                       item.originUUID,
			targetUUID:                 0,
			listItemCreationTime:       item.creationTime,
			targetListItemCreationTime: 0,
			unixNanoTime:               now,
			eventType:                  addEvent,
			line:                       item.Line,
			note:                       item.Note,
		}
		overrideWal = append(overrideWal, el)
		now++

		if item.IsHidden {
			el.eventType = hideEvent
			el.unixNanoTime = now
			overrideWal = append(overrideWal, el)
			now++
		}
		item = item.child
	}

	b := buildByteWal(&overrideWal)

	viewName := fmt.Sprintf(path.Join(r.wal.localWalFile.getRootDir(), "override_%v.db"), time.Now().UnixNano())
	r.wal.localWalFile.flush(b, viewName)
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

	//r.forceOverrideWal()

	return nil
}

func (r *DBListRepo) RegisterWalFile(wf WalFile) {
	r.wal.walFiles = append(r.wal.walFiles, wf)
}
