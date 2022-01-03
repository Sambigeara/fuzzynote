package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type s3WalFile struct {
	svc          *s3.S3
	downloader   *s3manager.Downloader
	uploader     *s3manager.Uploader
	localRootDir string
	key          string
	secret       string
	bucket       string
	prefix       string
}

func NewS3WalFile(cfg S3Remote, root string) *s3WalFile {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("eu-west-1"),
		Credentials: credentials.NewStaticCredentials(cfg.Key, cfg.Secret, ""),
	})
	if err != nil {
		log.Fatal(err)
	}

	return &s3WalFile{
		svc:          s3.New(sess),
		downloader:   s3manager.NewDownloader(sess),
		uploader:     s3manager.NewUploader(sess),
		localRootDir: root,
		key:          cfg.Key,
		secret:       cfg.Secret,
		bucket:       cfg.Bucket,
		prefix:       cfg.Prefix,
	}
}

func (wf *s3WalFile) GetUUID() string {
	return fmt.Sprintf("%s:%s", wf.bucket, wf.GetRoot())
}

func (wf *s3WalFile) GetRoot() string {
	return wf.prefix
}

func (wf *s3WalFile) GetMatchingWals(ctx context.Context, matchPattern string) ([]string, error) {
	fileNames := []string{}
	// TODO matchPattern isn't actually doing anything atm
	resp, err := wf.svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(wf.bucket),
		Prefix: aws.String(wf.GetRoot()),
	})
	if err != nil {
		//exitErrorf("Unable to list items in bucket %q, %v", wf.bucket, err)
		return fileNames, err
	}

	for _, item := range resp.Contents {
		fileNames = append(fileNames, strings.Split(strings.Split(*item.Key, "_")[1], ".")[0])
	}
	return fileNames, nil
}

func (wf *s3WalFile) GetWalBytes(ctx context.Context, w io.Writer, fileName string) error {
	// TODO implement streaming

	// Read into bytes rather than file
	b := aws.NewWriteAtBuffer([]byte{})

	// Default concurrency = 5
	_, err := wf.downloader.Download(b,
		&s3.GetObjectInput{
			Bucket: aws.String(wf.bucket),
			Key:    aws.String(fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), fileName)),
		})
	if err != nil {
		// If the file has been removed, skip, as it means another process has already merged
		// and deleted this one

		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			//case "NoSuchKey": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
			//    return []EventLog{}, nil
			case s3.ErrCodeNoSuchKey:
				return nil
			default:
				//exitErrorf("Unable to download item %q, %v", fileName, err)
				// For now, continue silently rather than exiting
				return err
			}
		}
	}

	w.Write(b.Bytes())
	return nil
}

func (wf *s3WalFile) RemoveWals(ctx context.Context, fileNames []string) error {
	// Delete the item
	objects := []*s3.ObjectIdentifier{}
	for _, f := range fileNames {
		objects = append(objects, &s3.ObjectIdentifier{
			Key: aws.String(fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), f)),
		})
	}
	//del := []s3.Delete{}
	_, err := wf.svc.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(wf.bucket),
		Delete: &s3.Delete{
			Objects: objects,
		},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			//case "NotFound": // s3.ErrCodeNoSuchKey does not work, aws is missing this error code so we hardwire a string
			//    return nil
			case s3.ErrCodeNoSuchKey:
				return nil
			default:
				//exitErrorf("Unable to delete objects %q from bucket %q, %v", fileNames, wf.bucket, err)
				// For now, continue silently rather than exiting
				return err
			}
		}

	}

	//err = wf.svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
	//    Bucket: aws.String(wf.bucket),
	//    Key:    aws.String(fileName),
	//})
	//if err != nil {
	//    exitErrorf("Error occurred while waiting for object %q to be deleted, %v", fileName, err)
	//}

	// TODO why was I also removing local??
	//return os.Remove(fileName)
	return nil
}

func (wf *s3WalFile) Flush(ctx context.Context, b *bytes.Buffer, randomUUID string) error {
	fileName := fmt.Sprintf(path.Join(wf.GetRoot(), walFilePattern), randomUUID)
	// IMPORTANT we need to take a copy here, because passing the bytes.Buffer as the Body io.Reader in
	// UploadInput means that the buffer is emptied on read.
	// In `gather`, we pass a single Buffer pointer to numerous `push` calls for efficiency - if this
	// s3 method is called before other walFile Flush calls, the buffer will be empty
	// TODO think about a less risky implementation of this...
	bCopy := *b
	_, err := wf.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(wf.bucket),
		Key:    aws.String(fileName),
		//Body:   b,
		Body: &bCopy,
	})
	if err != nil {
		//exitErrorf("Unable to upload %q to %q, %v", fileName, wf.bucket, err)
		// For now, continue silently rather than exiting
		return err
	}
	return nil
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
