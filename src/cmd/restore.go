package cmd

import (
	"ebsnapshot/tools"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

func restoreFromS3(cmd *cobra.Command) error {
	var f io.ReadCloser
	f, err := getS3ReadCloser(restoreS3Url.Bucket, restoreS3Url.Key)
	if err != nil {
		return fmt.Errorf("restore failure: %w", err)
	}

	tools.Restore(cmd.Context(), f, restoreRaw, backupRawSize)
	return nil
}

func restoreFromFile(cmd *cobra.Command) error {
	var f io.ReadCloser
	f, err := getFileReadCloser(flagBackupSource)
	if err != nil {
		return fmt.Errorf("restore failure: %w", err)
	}

	_, err = tools.Restore(cmd.Context(), f, restoreRaw, backupRawSize)
	if err != nil {
		return fmt.Errorf("restore failure: %w", err)
	}
	return nil
}

func restoreFromStdIn(cmd *cobra.Command) error {
	var f io.ReadCloser = getStdInReadCloser()

	_, err := tools.Restore(cmd.Context(), f, restoreRaw, backupRawSize)
	if err != nil {
		return fmt.Errorf("restore failure: %w", err)
	}
	return nil
}

var (
	restoreSourceType SourceType
	restoreS3Url      *tools.S3Url
	flagBackupSource  string
	restoreRaw        bool
	backupRawSize     int64 = -1
)

func NewRestoreCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "restore",
		Short: "Restore an EBS snapshot",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {

			var err error
			restoreSourceType, restoreS3Url, err = validateSource(cmd, flagBackupSource, restoreRaw, backupRawSize)
			if err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			switch restoreSourceType {
			case SourceTypeStdIn:
				return restoreFromStdIn(cmd)
			case SourceTypeS3:
				return restoreFromS3(cmd)
			case SourceTypeFile:
				return restoreFromFile(cmd)
			default:
				panic("invalid restore source value")
			}
		},
	}

	cmd.Flags().StringVarP(&flagBackupSource, "source", "", "", "Path to a file or S3 object URL that contains the backup")
	cmd.MarkFlagRequired("source")

	cmd.Flags().BoolVarP(&restoreRaw, "rawsrc", "r", false, "Read from a raw disk image file")

	cmd.Flags().Int64VarP(&backupRawSize, "size", "s", -1, "size of the snapshot in GB (for raw sources)")

	return cmd
}
