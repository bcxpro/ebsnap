package cmd

import (
	"ebsnapshot/tools"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/spf13/cobra"
)

func convert(cmd *cobra.Command) error {
	s3UploadResults := make(chan struct {
		output *manager.UploadOutput
		err    error
	}, 1)

	var rc io.ReadCloser

	switch convertSourceType {
	case SourceTypeFile:
		var err error
		rc, err = getFileReadCloser(flagConvertSource)
		if err != nil {
			return fmt.Errorf("convert failure: %w", err)
		}
	case SourceTypeS3:
		var err error
		rc, err = getS3ReadCloser(convertSrcS3Url.Bucket, convertSrcS3Url.Key)
		if err != nil {
			return fmt.Errorf("convert failure: %w", err)
		}
	case SourceTypeStdIn:
		rc = getStdInReadCloser()
	default:
		panic("invalid source type")
	}

	var wc io.WriteCloser

	switch convertDestinationType {
	case DestinationTypeFile:
		var err error
		wc, err = getFileWriterCloser(flagConvertDest)
		if err != nil {
			return fmt.Errorf("convert failure: %w", err)
		}
	case DestinationTypeS3:
		var err error
		wc, err = getS3WriteCloser(convertDstS3Url.Bucket, convertDstS3Url.Key, flagConvertS3StorageClassName, s3UploadResults)
		if err != nil {
			return fmt.Errorf("convert failure: %w", err)
		}
	case DestinationTypeStdOut:
		var err error
		wc = getStdOutWriteCloser()
		if err != nil {
			return fmt.Errorf("convert failure: %w", err)
		}
	default:
		panic("invalid destination type")
	}

	_, err := tools.Convert(cmd.Context(), rc, convertRawSource, convertRawSize, wc, convertRawDest)
	if err != nil {
		return fmt.Errorf("convert failure: %w", err)
	}

	// If it was an S3 destination wait for finish
	if convertDestinationType == DestinationTypeS3 {
		r := <-s3UploadResults
		if r.err != nil {
			return fmt.Errorf("convert failure: %w", r.err)
		}
	}

	return nil
}

var (
	convertSourceType             SourceType
	convertDestinationType        DestinationType
	convertSrcS3Url               *tools.S3Url
	convertDstS3Url               *tools.S3Url
	flagConvertSource             string
	flagConvertDest               string
	flagConvertS3StorageClassName string
	convertRawSource              bool
	convertRawDest                bool
	convertRawSize                int64 = -1
)

func NewConvertCommand() *cobra.Command {

	var cmd = &cobra.Command{
		Use:   "convert",
		Short: "Convert the format of a snapshot stored in a file or S3 object",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {

			var err error
			convertSourceType, convertSrcS3Url, err = validateSource(cmd, flagConvertSource, convertRawSource, convertRawSize)
			if err != nil {
				return err
			}

			convertDestinationType, convertDstS3Url, err = validateDestination(cmd, flagConvertDest, convertRawDest)
			if err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return convert(cmd)
		},
	}

	cmd.Flags().StringVarP(&flagConvertSource, "source", "", "", "Path to a file or S3 object URL that contains the source file")
	cmd.MarkFlagRequired("source")

	cmd.Flags().BoolVarP(&convertRawSource, "rawsrc", "", false, "Read from a raw disk image file")

	cmd.Flags().Int64VarP(&convertRawSize, "size", "s", -1, "Size of the source snapshot in GB (for raw sources)")

	cmd.Flags().StringVarP(&flagConvertDest, "dest", "d", "", "Path to a file or S3 object URL that will contain the converted snapshot")
	cmd.MarkFlagRequired("dest")

	cmd.Flags().StringVarP(&flagConvertS3StorageClassName, "storage-class", "c", "STANDARD", "S3 storage class")

	cmd.Flags().BoolVarP(&convertRawDest, "rawdst", "", false, "Write a raw disk image file")

	return cmd
}
