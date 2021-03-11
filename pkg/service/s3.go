package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3FileWal struct {
	svc                  *s3.S3
	downloader           *s3manager.Downloader
	uploader             *s3manager.Uploader
	bucket               string
	processedPartialWals map[string]struct{}
	syncFilePath         string
}

func NewS3FileWal(rootDir string) *s3FileWal {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1"),
	})
	if err != nil {
		log.Fatal(err)
	}

	return &s3FileWal{
		svc:                  s3.New(sess),
		downloader:           s3manager.NewDownloader(sess),
		uploader:             s3manager.NewUploader(sess),
		bucket:               "fuzzynote-pub",
		processedPartialWals: make(map[string]struct{}),
		syncFilePath:         path.Join(rootDir, syncFile),
	}
}

// TODO get rid of these
func (wf *s3FileWal) lock() error {
	return nil
}

func (wf *s3FileWal) unlock() error {
	return nil
}

func (wf *s3FileWal) getFileNamesMatchingPattern(matchPattern string) ([]string, error) {
	resp, err := wf.svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(wf.bucket),
	})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", wf.bucket, err)
	}

	fileNames := []string{}
	for _, item := range resp.Contents {
		//fmt.Println("Name:         ", *item.Key)
		//fmt.Println("Last modified:", *item.LastModified)
		//fmt.Println("Size:         ", *item.Size)
		//fmt.Println("Storage class:", *item.StorageClass)
		//fmt.Println("")
		fileNames = append(fileNames, *item.Key)
	}
	return fileNames, nil
}

func (wf *s3FileWal) generateLogFromFile(fileName string) ([]eventLog, error) {
	f, err := os.Create(fileName)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", fileName, err)
	}
	defer f.Close()

	//numBytes, err := wf.downloader.Download(f,
	_, err = wf.downloader.Download(f,
		&s3.GetObjectInput{
			Bucket: aws.String(wf.bucket),
			Key:    aws.String(fileName),
		})
	if err != nil {
		// If the file has been removed, skip, as it means another process has already merged
		// and deleted this one
		if _, ok := err.(awserr.Error); !ok {
			// process SDK error
			exitErrorf("Unable to download item %q, %v", fileName, err)
		}
	}

	//fmt.Println("Downloaded", f.Name(), numBytes, "bytes")

	wal, err := buildFromFile(f)
	if err != nil {
		return wal, err
	}
	return wal, nil
}

func (wf *s3FileWal) removeFile(fileName string) error {
	// Delete the item
	_, err := wf.svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(wf.bucket), Key: aws.String(fileName)})
	if err != nil {
		exitErrorf("Unable to delete object %q from bucket %q, %v", fileName, wf.bucket, err)
	}

	err = wf.svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(wf.bucket),
		Key:    aws.String(fileName),
	})
	if err != nil {
		exitErrorf("Error occurred while waiting for object %q to be deleted, %v", fileName, err)
	}

	return os.Remove(fileName)
}

func (wf *s3FileWal) flush(fileName string, walLog *[]eventLog) error {
	// TODO make generic function with walFile specific functions retrieving specific io.Writer
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// TODO this might not be needed anymore now we use fresh files each time
	// Seek to beginning of file just in case
	f.Truncate(0)
	f.Seek(0, io.SeekStart)

	// Write the schema ID
	err = binary.Write(f, binary.LittleEndian, latestWalSchemaID)
	if err != nil {
		return err
	}

	for _, item := range *walLog {
		lenLine := uint64(len([]byte(item.line)))
		var lenNote uint64
		if item.note != nil {
			lenNote = uint64(len(*(item.note)))
		}
		i := walItemSchema1{
			UUID:             item.uuid,
			TargetUUID:       item.targetUUID,
			ListItemID:       item.listItemID,
			TargetListItemID: item.targetListItemID,
			UnixTime:         item.unixNanoTime,
			EventType:        item.eventType,
			LineLength:       lenLine,
			NoteLength:       lenNote,
		}
		data := []interface{}{
			i,
			[]byte(item.line),
		}
		if item.note != nil {
			data = append(data, item.note)
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

	f.Seek(0, io.SeekStart)
	_, err = wf.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(wf.bucket),
		Key:    aws.String(fileName),
		Body:   f,
	})
	if err != nil {
		exitErrorf("Unable to upload %q to %q, %v", fileName, wf.bucket, err)
	}
	return nil
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
