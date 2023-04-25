package cmd

import (
	"context"
	"ebsnapshot/gconc"
	"ebsnapshot/tools"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/cobra"
)

type SourceType int

const (
	SourceTypeStdIn SourceType = iota
	SourceTypeS3
	SourceTypeFile
)

type DestinationType int

const (
	DestinationTypeStdOut DestinationType = iota
	DestinationTypeS3
	DestinationTypeFile
)

func validateSource(cmd *cobra.Command, sourceFlag string, isRaw bool, rawSize int64) (SourceType, *tools.S3Url, error) {

	var u tools.S3Url
	var err error
	var sourceType SourceType
	if sourceFlag == "-" {
		sourceType = SourceTypeStdIn
	} else if u, err = tools.ParseS3Url(sourceFlag); err == nil {
		sourceType = SourceTypeS3
	} else {
		sourceType = SourceTypeFile
	}

	switch sourceType {
	case SourceTypeS3:
		if isRaw {
			if !cmd.Flag("size").Changed {
				return sourceType, nil, errors.New("volume size must be specified")
			}
			if rawSize < 1 {
				return sourceType, nil, errors.New("invalid volume size")
			}
		}
	case SourceTypeFile:
		if isRaw {
			if cmd.Flag("size").Changed {
				if rawSize < 1 {
					return sourceType, nil, errors.New("invalid volume size")
				}
			}
		}
	case SourceTypeStdIn:
		if isRaw {
			if !cmd.Flag("size").Changed {
				return sourceType, nil, errors.New("volume size must be specified")
			}
			if rawSize < 1 {
				return sourceType, nil, errors.New("invalid volume size")
			}
		}
	default:
		panic("invalid source value")
	}

	return sourceType, &u, nil
}

func validateDestination(cmd *cobra.Command, destinationFlag string, isRaw bool) (DestinationType, *tools.S3Url, error) {

	var u tools.S3Url
	var err error
	var destinationType DestinationType
	if destinationFlag == "-" {
		destinationType = DestinationTypeStdOut
	} else if u, err = tools.ParseS3Url(destinationFlag); err == nil {
		destinationType = DestinationTypeS3
	} else {
		destinationType = DestinationTypeFile
	}

	switch destinationType {
	case DestinationTypeS3:
		// storageClass
		validClasses := s3Types.ObjectStorageClass.Values("")
		if !gconc.Contains(validClasses, s3Types.ObjectStorageClass(flagConvertS3StorageClassName)) {
			c := gconc.CopyStringSlice(validClasses)
			return destinationType, nil, errors.New("invalid storage class, valid: " + strings.Join(c, ", "))
		}
	case DestinationTypeFile:
	case DestinationTypeStdOut:
	default:
		panic("invalid destination value")
	}

	return destinationType, &u, nil
}

func getFileWriterCloser(outputFilePath string) (io.WriteCloser, error) {
	f, err := os.Create(outputFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open outputFilePath: %w", err)
	}
	return f, nil
}

func getS3WriteCloser(bucket, key string, flagS3StorageClassName string, results chan struct {
	output *manager.UploadOutput
	err    error
}) (io.WriteCloser, error) {
	var err error
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	pr, pw := io.Pipe()

	go func() {
		uploader := manager.NewUploader(s3Client, func(u *manager.Uploader) { u.MaxUploadParts = 10000; u.PartSize = 512 * 1024 * 1024 })
		putObjectOutput, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket:       aws.String(bucket),
			Key:          aws.String(key),
			StorageClass: s3Types.StorageClass(flagS3StorageClassName),
			Body:         pr,
		})
		if err != nil {
			log.Printf("getS3WriteCloser finished: %v err: %v ", putObjectOutput, err)
			pr.Close()
		}
		results <- struct {
			output *manager.UploadOutput
			err    error
		}{output: putObjectOutput, err: err}
		log.Printf("getS3WriteCloser goroutine finished")
	}()

	return pw, nil
}

func getFileReadCloser(inputFilePath string) (io.ReadCloser, error) {
	f, err := os.Open(inputFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open inputFilePath: %w", err)
	}
	return f, nil
}

func getS3ReadCloser(bucket, key string) (io.ReadCloser, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg)
	goResult, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open S3 object: %w", err)
	}
	return goResult.Body, nil
}

func getStdInReadCloser() io.ReadCloser {
	f := os.Stdin
	return f
}

func getStdOutWriteCloser() io.WriteCloser {
	f := os.Stdout
	return f
}
