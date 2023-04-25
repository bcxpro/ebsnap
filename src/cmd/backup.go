package cmd

import (
	"ebsnapshot/gconc"
	"ebsnapshot/tools"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/cobra"
)

func backupToS3(cmd *cobra.Command) error {
	var f io.WriteCloser
	var rc chan struct {
		output *manager.UploadOutput
		err    error
	}
	var err error

	rc = make(chan struct {
		output *manager.UploadOutput
		err    error
	}, 1)
	f, err = getS3WriteCloser(s3Url.Bucket, s3Url.Key, flagS3StorageClassName, rc)
	if err != nil {
		return fmt.Errorf("backup failure: %w", err)
	}

	backupOutput, err := tools.Backup(cmd.Context(), flagSnapshotId, f, flagRaw)
	if err != nil {
		return fmt.Errorf("backup failure: %w", err)
	}

	r := <-rc
	if r.err != nil {
		return fmt.Errorf("backup failure: %w", r.err)
	}
	cmd.Printf("Finished backing up the snapshot, %v bytes written.\n", backupOutput.BytesWritten)
	return nil
}

func backupToFile(cmd *cobra.Command) error {
	var f io.WriteCloser
	var err error

	f, _ = getFileWriterCloser(flagBackupDest)

	backupOutput, err := tools.Backup(cmd.Context(), flagSnapshotId, f, flagRaw)
	if err != nil {
		return fmt.Errorf("backup failure: %w", err)
	}

	cmd.Printf("Finished backing up the snapshot, %v bytes written.\n", backupOutput.BytesWritten)
	return nil
}

var (
	flagSnapshotId         string
	s3Url                  *tools.S3Url
	flagBackupDest         string
	flagS3StorageClassName string
	flagRaw                bool
)

func NewBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup an EBS snapshot",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {

			u, err := tools.ParseS3Url(flagBackupDest)
			if err == nil {
				s3Url = &u
			}

			// storageClass
			validClasses := s3Types.ObjectStorageClass.Values("")
			if !gconc.Contains(validClasses, s3Types.ObjectStorageClass(flagS3StorageClassName)) {
				c := gconc.CopyStringSlice(validClasses)
				return errors.New("invalid storage class, valid: " + strings.Join(c, ", "))
			}
			return nil
		},

		RunE: func(cmd *cobra.Command, args []string) error {
			if s3Url == nil {
				return backupToFile(cmd)
			} else {
				return backupToS3(cmd)
			}
		},
	}

	cmd.Flags().StringVarP(&flagBackupDest, "dest", "d", "", "Path to a file or S3 object URL that will contain the backup")
	cmd.MarkFlagRequired("dest")

	cmd.Flags().StringVarP(&flagSnapshotId, "snapshot-id", "s", "", "Snapshot Id to backup")
	cmd.MarkFlagRequired("snapshot-id")

	cmd.Flags().StringVarP(&flagS3StorageClassName, "storage-class", "c", "STANDARD", "S3 storage class")

	cmd.Flags().BoolVarP(&flagRaw, "rawdst", "r", false, "Write a raw disk image file")

	return cmd

}
