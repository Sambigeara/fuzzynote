package service

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3FileWal struct {
	RefreshTicker            *time.Ticker
	GatherTicker             *time.Ticker
	svc                      *s3.S3
	downloader               *s3manager.Downloader
	uploader                 *s3manager.Uploader
	processedPartialWals     map[string]struct{}
	processedPartialWalsLock *sync.Mutex
	localRootDir             string
	key                      string
	secret                   string
	bucket                   string
	prefix                   string
	mode                     Mode
	pushMatchTerm            []rune
	processedEventMap        map[string]struct{}
	processedEventLock       *sync.Mutex
}

func NewS3FileWal(cfg s3Remote, root string) *s3FileWal {
	// Handle defaults if not set
	if cfg.RefreshFreqMs == 0 {
		cfg.RefreshFreqMs = 2000
	}
	if cfg.GatherFreqMs == 0 {
		cfg.GatherFreqMs = 10000
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("eu-west-1"),
		Credentials: credentials.NewStaticCredentials(cfg.Key, cfg.Secret, ""),
	})
	if err != nil {
		log.Fatal(err)
	}

	return &s3FileWal{
		RefreshTicker:            time.NewTicker(time.Millisecond * time.Duration(cfg.RefreshFreqMs)),
		GatherTicker:             time.NewTicker(time.Millisecond * time.Duration(cfg.GatherFreqMs)),
		svc:                      s3.New(sess),
		downloader:               s3manager.NewDownloader(sess),
		uploader:                 s3manager.NewUploader(sess),
		processedPartialWals:     make(map[string]struct{}),
		processedPartialWalsLock: &sync.Mutex{},
		localRootDir:             root,
		key:                      cfg.Key,
		secret:                   cfg.Secret,
		bucket:                   cfg.Bucket,
		prefix:                   cfg.Prefix,
		mode:                     cfg.Mode,
		pushMatchTerm:            []rune(cfg.Match),
		processedEventMap:        make(map[string]struct{}),
		processedEventLock:       &sync.Mutex{},
	}
}

func (wf *s3FileWal) getRootDir() string {
	return wf.prefix
}

func (wf *s3FileWal) getFileNamesMatchingPattern(matchPattern string) ([]string, error) {
	// TODO matchPattern isn't actually doing anything atm
	resp, err := wf.svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(wf.bucket),
		Prefix: aws.String(wf.getRootDir()),
	})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", wf.bucket, err)
	}

	fileNames := []string{}
	for _, item := range resp.Contents {
		fileNames = append(fileNames, *item.Key)
	}
	return fileNames, nil
}

func (wf *s3FileWal) generateLogFromFile(fileName string) ([]EventLog, error) {
	// Read into bytes rather than file
	b := aws.NewWriteAtBuffer([]byte{})

	// Default concurrency = 5
	_, err := wf.downloader.Download(b,
		&s3.GetObjectInput{
			Bucket: aws.String(wf.bucket),
			Key:    aws.String(fileName),
		})
	if err != nil {
		// If the file has been removed, skip, as it means another process has already merged
		// and deleted this one

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			//case "NoSuchKey": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
			//    return []EventLog{}, nil
			case s3.ErrCodeNoSuchKey:
				return []EventLog{}, nil
			default:
				exitErrorf("Unable to download item %q, %v", fileName, err)
			}
		}
	}

	buf := bytes.NewBuffer(b.Bytes())

	wal, err := buildFromFile(buf)
	if err != nil {
		return wal, err
	}
	return wal, nil
}

func (wf *s3FileWal) removeFile(fileName string) error {
	// Delete the item
	_, err := wf.svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(wf.bucket), Key: aws.String(fileName)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			//case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
			//    return nil
			case s3.ErrCodeNoSuchKey:
				return nil
			default:
				exitErrorf("Unable to delete object %q from bucket %q, %v", fileName, wf.bucket, err)
			}
		}

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

func (wf *s3FileWal) flush(b *bytes.Buffer, fileName string) error {
	_, err := wf.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(wf.bucket),
		Key:    aws.String(fileName),
		Body:   b,
	})
	if err != nil {
		exitErrorf("Unable to upload %q to %q, %v", fileName, wf.bucket, err)
	}
	return nil
}

func (wf *s3FileWal) setProcessedPartialWals(fileName string) {
	wf.processedPartialWalsLock.Lock()
	defer wf.processedPartialWalsLock.Unlock()
	wf.processedPartialWals[fileName] = struct{}{}
}

func (wf *s3FileWal) isPartialWalProcessed(fileName string) bool {
	wf.processedPartialWalsLock.Lock()
	defer wf.processedPartialWalsLock.Unlock()
	_, exists := wf.processedPartialWals[fileName]
	return exists
}

func (wf *s3FileWal) awaitPull() {
	<-wf.RefreshTicker.C
}

func (wf *s3FileWal) awaitGather() {
	<-wf.GatherTicker.C
}

func (wf *s3FileWal) stopTickers() {
	wf.RefreshTicker.Stop()
	wf.GatherTicker.Stop()
}

func (wf *s3FileWal) getMode() Mode {
	return wf.mode
}

func (wf *s3FileWal) getPushMatchTerm() []rune {
	return wf.pushMatchTerm
}

func (wf *s3FileWal) setProcessedEvent(fileName string) {
	wf.processedEventLock.Lock()
	defer wf.processedEventLock.Unlock()
	wf.processedEventMap[fileName] = struct{}{}
}

func (wf *s3FileWal) isEventProcessed(fileName string) bool {
	wf.processedEventLock.Lock()
	defer wf.processedEventLock.Unlock()
	_, exists := wf.processedEventMap[fileName]
	return exists
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
