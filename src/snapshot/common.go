package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/ebs"
	ebsTypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

var (
	ErrNotInitialized     = errors.New("not initialized")
	ErrAlreadyInitialized = errors.New("already initialized")
	ErrErrorStatus        = errors.New("error state")
	ErrErrorClosed        = errors.New("already closed")
)

type EBSSnapshotWriteAPI interface {
	StartSnapshot(ctx context.Context, params *ebs.StartSnapshotInput, optFns ...func(*ebs.Options)) (*ebs.StartSnapshotOutput, error)
	PutSnapshotBlock(ctx context.Context, params *ebs.PutSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.PutSnapshotBlockOutput, error)
	CompleteSnapshot(ctx context.Context, params *ebs.CompleteSnapshotInput, optFns ...func(*ebs.Options)) (*ebs.CompleteSnapshotOutput, error)
}

type EBSSnapshotReadAPI interface {
	ListSnapshotBlocks(ctx context.Context, params *ebs.ListSnapshotBlocksInput, optFns ...func(*ebs.Options)) (*ebs.ListSnapshotBlocksOutput, error)
	GetSnapshotBlock(ctx context.Context, params *ebs.GetSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error)
}

type EBSSnapshotAPI interface {
	EBSSnapshotReadAPI
	EBSSnapshotWriteAPI
}

type EC2SnapshotAPI interface {
	DescribeSnapshots(ctx context.Context, params *ec2.DescribeSnapshotsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error)
}

// Represents the contents of an EBS Snapshot block
type BlockContent struct {
	BlockIndex        int32                      // block index number
	Checksum          []byte                     // checksum for the data using the algorithm specified in ChecksumAlgorithm
	Data              []byte                     // block data
	DataLength        int32                      // length of the data stored in the Data field
	ChecksumAlgorithm ebsTypes.ChecksumAlgorithm // Checksum used (currently only SHA256 is available)
}

func (bc *BlockContent) verifyChecksum() bool {
	switch bc.ChecksumAlgorithm {
	case ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256:
		calculatedChecksum := sha256.Sum256(bc.Data)
		return bytes.Equal(bc.Checksum, calculatedChecksum[:])
	default:
		return false
	}
}

func (bc *BlockContent) verifyIntegrity() bool {
	return len(bc.Data) == int(bc.DataLength) && bc.verifyChecksum()
}

type BlockList []int32

func (bl BlockList) Len() int {
	return len(bl)
}

func (bl BlockList) Less(i, j int) bool {
	return bl[i] < bl[j]
}

func (bl BlockList) Swap(i, j int) {
	bl[i], bl[j] = bl[j], bl[i]
}

func (bl BlockList) Sort() {
	sort.Sort(bl)
}

func (bl BlockList) Contains(index int32) bool {
	// binary search to see if the block is in the list
	p := sort.Search(len(bl), func(i int) bool { return bl[i] >= index })
	return p != len(bl)
}

type Retriever interface {
	Initialize(ctx context.Context) error
	NextBlock(ctx context.Context) (*BlockContent, error)
	SnapshotInfo(ctx context.Context) (*Info, error)
}

type Creator interface {
	Initialize(ctx context.Context, info Info) error
	PutBlock(ctx context.Context, b *BlockContent) error
	Complete(ctx context.Context) (*Info, error)
}

type Info struct {
	SnapshotId       string
	ParentSnapshotId string
	VolumeId         string
	VolumeSize       int64
	BlockSize        int32
	Description      string
	Tags             map[string]string
}

type CopyProgress struct {
	State           CopyState
	TotalBlocks     int32
	LastBlockCopied int32
}

type CopyState int

const (
	Initializing CopyState = iota
	Copying
	Completing
	Completed
)

func (e CopyState) String() string {
	l := [...]string{"Initializing", "Copying", "Completing", "Completed"}
	if int(e) < len(l) || int(e) < 0 {
		return l[e]
	} else {
		return fmt.Sprintf("%d", int(e))
	}
}

func Copy(ctx context.Context, cr Creator, rt Retriever, progress func(CopyProgress)) error {

	sendProgressReport := func(c chan CopyProgress, state CopyState, total, copied int32, skippable bool) {
		if skippable {
			select {
			case c <- CopyProgress{
				State:           state,
				TotalBlocks:     total,
				LastBlockCopied: copied,
			}:
			default:
			}
		} else {
			c <- CopyProgress{
				State:           state,
				TotalBlocks:     total,
				LastBlockCopied: copied,
			}
		}
	}

	var progressWg sync.WaitGroup
	progressChannel := make(chan CopyProgress)
	progressWg.Add(1)
	defer progressWg.Wait()
	defer close(progressChannel)
	go func() {
		defer progressWg.Done()
		for p := range progressChannel {
			if progress != nil {
				progress(p)
			}
		}
	}()

	sendProgressReport(progressChannel, Initializing, 0, 0, false)

	err := rt.Initialize(ctx)
	if err != nil {
		return fmt.Errorf("could not initialize retriever: %w", err)
	}

	sInfo, err := rt.SnapshotInfo(ctx)
	if err != nil {
		return fmt.Errorf("could not retrieve SnapshotInfo: %w", err)
	}

	err = cr.Initialize(ctx, *sInfo)
	if err != nil {
		return fmt.Errorf("could not initialize creator: %w", err)
	}

	var blocksToUpload int32 = int32(blockCount(sInfo.VolumeSize, sInfo.BlockSize))
	var lastBlockCopiedIndex int32 = 0
	var blocksSinceLastReport int32 = 0
	sendProgressReport(progressChannel, Copying, blocksToUpload, lastBlockCopiedIndex, false)
	for {

		select {
		case <-ctx.Done():
			return fmt.Errorf("context done: %w", ctx.Err())
		default:
		}

		bl, err := rt.NextBlock(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to get next block: %w", err)
		}
		err = cr.PutBlock(ctx, bl)
		if err != nil {
			return fmt.Errorf("failed to put block: %w", err)
		}

		lastBlockCopiedIndex = bl.BlockIndex
		// progress notification
		blocksSinceLastReport++
		if blocksSinceLastReport > 64 {
			sendProgressReport(progressChannel, Copying, blocksToUpload, lastBlockCopiedIndex, true)
			blocksSinceLastReport = 0
		}
	}
	_, err = cr.Complete(ctx)
	if err != nil {
		return fmt.Errorf("failed to complete snapshot: %w", err)
	}
	sendProgressReport(progressChannel, Completed, blocksToUpload, blocksToUpload, false)
	return nil
}

func blockCount(volumeSize int64, blockSize int32) int {
	return int(volumeSize) * int(1024*1024*1024/blockSize)
}

func newBlockContent(blockIndex int32, data []byte) *BlockContent {

	checksum := sha256.Sum256(data)

	bc := BlockContent{
		BlockIndex:        blockIndex,
		Checksum:          checksum[:],
		Data:              data,
		DataLength:        int32(len(data)),
		ChecksumAlgorithm: ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256,
	}

	return &bc
}
