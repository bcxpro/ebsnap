package snapshot

import (
	"bytes"
	"context"
	"ebsnapshot/gconc"
	"fmt"
	"io"
)

type RawRetriever struct {
	r           io.Reader
	initialized bool
	eof         bool
	errorState  bool

	blocksGen      *gconc.Generator[[]byte]
	blockList      BlockList
	sInfo          Info
	nextBlockIndex int32

	zeroes []byte
}

func NewRawRetriever(r io.Reader, sInfo Info) *RawRetriever {
	rt := RawRetriever{r: r, initialized: false}
	rt.sInfo = sInfo
	rt.zeroes = make([]byte, sInfo.BlockSize)
	return &rt
}

func (rt *RawRetriever) Initialize(ctx context.Context) error {
	if rt.initialized {
		return ErrAlreadyInitialized
	}

	if rt.sInfo.VolumeSize <= 0 {
		return fmt.Errorf("invalid volume size %v", rt.sInfo.VolumeSize)
	}

	if rt.sInfo.BlockSize <= 0 {
		return fmt.Errorf("invalid block size %v", rt.sInfo.BlockSize)
	}

	rt.blocksGen = gconc.NewGenerator(context.TODO(), readRawBlocksFunc(rt))

	bc := int32(blockCount(rt.sInfo.VolumeSize, rt.sInfo.BlockSize))
	rt.blockList = make(BlockList, bc)
	for i := int32(0); i < bc; i++ {
		rt.blockList[i] = i
	}

	rt.initialized = true
	return nil
}

func (rt *RawRetriever) SnapshotInfo(ctx context.Context) (*Info, error) {
	if !rt.initialized {
		return nil, ErrNotInitialized
	}
	out := rt.sInfo
	return &out, nil
}

func (rt *RawRetriever) NextBlock(ctx context.Context) (*BlockContent, error) {
	if rt.errorState {
		return nil, ErrErrorStatus
	}
	if !rt.initialized {
		return nil, ErrNotInitialized
	}
	if rt.eof {
		return nil, io.EOF
	}

	var (
		ok         bool
		recordRead []byte
	)

	// Loop over blocks until finding one that is non zero
	for {
		
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context done: %w", ctx.Err())
		case recordRead, ok = <-rt.blocksGen.Out():
		}
		if !ok {
			<-rt.blocksGen.Done()
			genErr := rt.blocksGen.Err()
			if genErr != nil {
				rt.errorState = true
				return nil, fmt.Errorf("error reading: %w", genErr)
			}
			rt.eof = true
			return nil, io.EOF
		}

		if bytes.Equal(recordRead, rt.zeroes) {
			rt.nextBlockIndex++
			continue
		}
		
		content := newBlockContent(rt.nextBlockIndex, recordRead)

		rt.nextBlockIndex++
		return content, nil
	}
}

func readRawBlocksFunc(rt *RawRetriever) func(ctx context.Context, out chan<- []byte) error {

	blockSize := 512 * 1024
	totalBlocks := blockCount(rt.sInfo.VolumeSize, rt.sInfo.BlockSize)

	f := func(ctx context.Context, out chan<- []byte) error {
		var eof bool
		var blocksGenerated int32

		for {
			if eof {
				if blocksGenerated < int32(totalBlocks) {
					zeroes := make([]byte, blockSize)
					select {
					case out <- zeroes:
						blocksGenerated++
					case <-ctx.Done():
						return fmt.Errorf("context done: %w", ctx.Err())
					}
					continue
				}
				return nil
			}

			data := make([]byte, blockSize)
			n, err := io.ReadAtLeast(rt.r, data, blockSize)
			if err == io.EOF {
				eof = true
				continue
			}
			if err == io.ErrUnexpectedEOF {
				// There was an unexpected EOF, the buffer has to be padded with zeroes
				padding := make([]byte, blockSize-n)
				copy(data[n:], padding)
				eof = true
				err = nil
			}
			if err != nil {
				return fmt.Errorf("failed to read block: %w", err)
			}
			select {
			case out <- data:
				blocksGenerated++
			case <-ctx.Done():
				return fmt.Errorf("context done: %w", ctx.Err())
			}
		}
	}
	return f
}
