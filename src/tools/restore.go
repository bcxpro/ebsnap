package tools

import (
	// "context"

	"context"
	"ebsnapshot/snapshot"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
)

type RestoreOutput struct {
}

func Restore(ctx context.Context, rc io.ReadCloser, raw bool, volumeSize int64) (*RestoreOutput, error) {

	var err error

	defer rc.Close()

	if raw && volumeSize <= 0 {
		volumeSize, err = calcVolumeSize(rc)
		if err != nil {
			return nil, err
		}
	}

	_, err = config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed loading default aws config: %w", err)
	}

	var retriever snapshot.Retriever
	if raw {
		info := snapshot.Info{
			BlockSize:  512 * 1024,
			VolumeSize: volumeSize,
		}
		retriever = snapshot.NewRawRetriever(rc, info)
	} else {
		retriever = snapshot.NewBinaryRetriever(rc)
	}
	creator := snapshot.NewEBSCreator()

	progressPrint := func(p snapshot.CopyProgress) {
		log.Printf("Copied %v of %v, state %v\n", p.LastBlockCopied, p.TotalBlocks, p.State)
	}

	err = snapshot.Copy(ctx, creator, retriever, progressPrint)
	if err != nil {
		return nil, fmt.Errorf("failed to perform snapshot backup: %w", err)
	}

	return &RestoreOutput{}, nil
}
