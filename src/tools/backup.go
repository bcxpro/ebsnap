package tools

import (
	"context"
	"fmt"
	"io"
	"log"

	"ebsnapshot/snapshot"
)

type BackupOutput struct {
	BytesWritten int64
}

func Backup(ctx context.Context, snapshotId string, f io.WriteCloser, raw bool) (*BackupOutput, error) {
	defer f.Close()

	retriever := snapshot.NewEBSSnapshotRetriever(snapshotId)

	var creator snapshot.Creator
	if raw {
		creator = snapshot.NewRawCreator(f)
	} else {
		creator = snapshot.NewBinaryCreator(f)
	}

	progressPrint := func(p snapshot.CopyProgress) {
		log.Printf("Copied %v of %v, state %v\n", p.LastBlockCopied, p.TotalBlocks, p.State)
	}

	err := snapshot.Copy(ctx, creator, retriever, progressPrint)
	if err != nil {
		return nil, fmt.Errorf("failed to perform snapshot backup: %w", err)
	}

	return &BackupOutput{}, nil
}
