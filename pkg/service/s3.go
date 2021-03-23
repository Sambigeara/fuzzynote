package service

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3FileWal struct {
	RefreshTicker        *time.Ticker
	svc                  *s3.S3
	downloader           *s3manager.Downloader
	uploader             *s3manager.Uploader
	processedPartialWals map[string]struct{}
	localRootDir         string
	key                  string
	secret               string
	bucket               string
	prefix               string
}

func NewS3FileWal(refreshFrequency uint16, key string, secret string, bucket string, prefix string, localRootDir string) *s3FileWal {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("eu-west-1"),
		Credentials: credentials.NewStaticCredentials(key, secret, ""),
	})
	if err != nil {
		log.Fatal(err)
	}

	return &s3FileWal{
		RefreshTicker:        time.NewTicker(time.Millisecond * time.Duration(refreshFrequency)),
		svc:                  s3.New(sess),
		downloader:           s3manager.NewDownloader(sess),
		uploader:             s3manager.NewUploader(sess),
		processedPartialWals: make(map[string]struct{}),
		localRootDir:         localRootDir,
		key:                  key,
		secret:               secret,
		bucket:               bucket,
		prefix:               prefix,
	}
}

func (wf *s3FileWal) getRootDir() string {
	return wf.prefix
}

func (wf *s3FileWal) getLocalRootDir() string {
	return wf.localRootDir
}

// TODO get rid of these
func (wf *s3FileWal) lock() error {
	return nil
}

func (wf *s3FileWal) unlock() error {
	return nil
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
		// TODO there's a chance a file will have been deleted by the cleanup task. If this is the
		// case we should just fail silently and return.
		if _, ok := err.(awserr.Error); !ok {
			// process SDK error
			exitErrorf("Unable to download item %q, %v", fileName, err)
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

func (wf *s3FileWal) isPartialWalProcessed(fileName string) bool {
	_, exists := wf.processedPartialWals[fileName]
	return exists
}

func (wf *s3FileWal) setProcessedPartialWals(fileName string) {
	wf.processedPartialWals[fileName] = struct{}{}
}

func (wf *s3FileWal) getTicker() {
	<-wf.RefreshTicker.C
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
