package snapshot

import (
	"context"
	"fmt"
	"io"
	"sync"
)

type RawCreator struct {
	sInfo     Info

	initialized bool
	closed      bool
	errorState  bool

	lastPutBlockIndex int32
	putBlocksCount    int32
	writer            io.Writer
	maxBlockCount     int32

	incomingBlocks chan *BlockContent

	writeContentResult     chan error // Return value of the writeContent goroutine
	putBlockEmergencyBreak chan bool  // when closed signals the blocked PutBlock call to stop and return an error
	opMutex                sync.Mutex
}

func NewRawCreator(w io.Writer) *RawCreator {
	return &RawCreator{
		initialized:            false,
		writer:                 w,
		writeContentResult:     make(chan error),
		putBlockEmergencyBreak: make(chan bool),
	}
}

func (cr *RawCreator) Initialize(ctx context.Context, sInfo Info) error {
	if cr.errorState {
		return ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if cr.initialized {
		return ErrAlreadyInitialized
	}

	cr.lastPutBlockIndex = -1
	cr.sInfo = sInfo
	cr.incomingBlocks = make(chan *BlockContent)

	cr.maxBlockCount = int32(blockCount(cr.sInfo.VolumeSize, cr.sInfo.BlockSize))

	// launches the writing goroutine
	go cr.writeContent(cr.writeContentResult)

	cr.initialized = true
	return nil
}

func (cr *RawCreator) writeContent(result chan<- error) {

	emptyBlock := make([]byte, cr.sInfo.BlockSize)

	fillBlocks := func(after int32, before int32) error {
		for i := after + 1; i < before; i++ {
			_, err := cr.writer.Write(emptyBlock)
			if err != nil {
				return err
			}
		}
		return nil
	}

	var lastBlockWritten int32 = -1

	for  {

		bc, ok := <-cr.incomingBlocks
		if !ok {
			break
		}

		err := fillBlocks(lastBlockWritten, bc.BlockIndex)
		if err != nil {
			result <- fmt.Errorf("error filling blocks: %w", err)
			return
		}

		_, err = cr.writer.Write(bc.Data)
		if err != nil {
			result <- fmt.Errorf("write error on block %v: %w", bc.BlockIndex, err)
			return
		}
		lastBlockWritten = bc.BlockIndex
	}

	err := fillBlocks(lastBlockWritten, cr.maxBlockCount)
	if err != nil {
		result <- fmt.Errorf("error filling last blocks: %w", err)
		return
	}

	result <- nil
}

func (cr *RawCreator) PutBlock(ctx context.Context, b *BlockContent) error {
	if cr.errorState {
		return ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if !cr.initialized {
		return ErrNotInitialized
	}
	if cr.closed {
		return ErrErrorClosed
	}
	if b.BlockIndex >= int32(blockCount(cr.sInfo.VolumeSize, cr.sInfo.BlockSize)) || b.BlockIndex < 0 {
		return fmt.Errorf("block index out of range")
	}
	if b.BlockIndex <= cr.lastPutBlockIndex {
		return fmt.Errorf("put block not ordered, put block %v after %v", b.BlockIndex, cr.lastPutBlockIndex)
	}

	if !b.verifyIntegrity() {
		return fmt.Errorf("block content failed integrity verification")
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	default:
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	case cr.incomingBlocks <- b:
		cr.putBlocksCount++
		return nil
	case err := <-cr.writeContentResult:
		cr.errorState = true
		cr.closed = true
		return fmt.Errorf("writing error: %w", err)
	}

}

func (cr *RawCreator) Complete(ctx context.Context) (*Info, error) {
	if cr.errorState {
		return nil, ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if !cr.initialized {
		return nil, ErrNotInitialized
	}
	if cr.closed {
		return nil, ErrErrorClosed
	}
	cr.closed = true

	// close the put channel and because its goroutine finishes when the channel is closed
	// read the results
	close(cr.incomingBlocks)
	err := <-cr.writeContentResult
	if err != nil {
		cr.errorState = true
		return nil, fmt.Errorf("cannot complete due to error on writer goroutine: %w", err)
	}
	return &Info{
		SnapshotId:       "",
		ParentSnapshotId: "",
		VolumeId:         "",
		VolumeSize:       cr.sInfo.VolumeSize,
		BlockSize:        cr.sInfo.BlockSize,
		Description:      "",
		Tags:             nil,
	}, nil
}
