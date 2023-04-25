package tools

import (
	"context"
	"fmt"
	"io"
	"log"

	"ebsnapshot/snapshot"
)

type ConvertOutput struct {
	BytesWritten int64
}

func Convert(ctx context.Context, rc io.ReadCloser, rawSrc bool, volumeSize int64, wc io.WriteCloser, rawDst bool) (*ConvertOutput, error) {
	defer wc.Close()

	var retriever snapshot.Retriever
	if rawSrc {
		if volumeSize <= 0 {
			var err error
			volumeSize, err = calcVolumeSize(rc)
			if err != nil {
				return nil, err
			}
		}
		info := snapshot.Info{
			BlockSize:  512 * 1024,
			VolumeSize: volumeSize,
		}
		retriever = snapshot.NewRawRetriever(rc, info)
	} else {
		retriever = snapshot.NewBinaryRetriever(rc)
	}

	var creator snapshot.Creator
	if rawDst {
		creator = snapshot.NewRawCreator(wc)
	} else {
		creator = snapshot.NewBinaryCreator(wc)
	}

	progressPrint := func(p snapshot.CopyProgress) {
		log.Printf("Converted %v of %v, state %v\n", p.LastBlockCopied, p.TotalBlocks, p.State)
	}

	err := snapshot.Copy(ctx, creator, retriever, progressPrint)
	if err != nil {
		return nil, fmt.Errorf("failed to perform snapshot conversion: %w", err)
	}

	return &ConvertOutput{}, nil
}
