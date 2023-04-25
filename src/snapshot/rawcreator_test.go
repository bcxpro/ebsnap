package snapshot

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestRawCreator(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1

	var destinationFile *fakeRawSnapshotWriter

	prepare := func(t *testing.T, fakeBlockCreator func(blockIndex int32) *BlockContent) (Creator, Info, BlockList, func(blockIndex int32) *BlockContent) {

		maxBlockCount := blockCount(volumeSize, blockSize)

		blockCheck := func(blockIndex int32, data []byte) error {

			if len(data) != int(blockSize) {
				return fmt.Errorf("block index %v invalid data length: %v, expected: %v", blockIndex, len(data), blockSize)
			}
			if blockIndex >= int32(maxBlockCount) || blockIndex < 0 {
				return fmt.Errorf("block index %v out of the allowed range 0-%v", blockIndex, maxBlockCount-1)
			}

			bc := fakeBlockCreator(blockIndex)
			if !bytes.Equal(data, bc.Data) {
				return fmt.Errorf("block index %v data mismatch", blockIndex)
			}
			return nil
		}

		destinationFile = newFakeRawSnapshotWriter(blockSize, volumeSize, blockCheck)
		cr := NewRawCreator(destinationFile)

		sInfo := Info{
			SnapshotId:       "s-abcde",
			ParentSnapshotId: "",
			VolumeId:         "vol-fff",
			VolumeSize:       volumeSize,
			BlockSize:        blockSize,
			Description:      "test raw snapshot",
		}

		blockList := make(BlockList, maxBlockCount)

		for i := range blockList {
			blockList[i] = int32(i)
		}

		return cr, sInfo, blockList, createFakeBlockContent
	}

	postFullCycle := func(t *testing.T, completeInfo Info, completeErr error) {
		assert.NoError(t, completeErr)
		expectedInfo := Info{
			SnapshotId:       "",
			ParentSnapshotId: "",
			VolumeId:         "",
			VolumeSize:       volumeSize,
			BlockSize:        blockSize,
			Description:      "",
		}
		assert.Equal(t, expectedInfo, completeInfo)

		err := destinationFile.Close()
		assert.NoError(t, err)
		assert.NoError(t, destinationFile.ErrorStatus)
	}

	s := creatorTestSuite{
		Suite:         suite.Suite{},
		prepare:       prepare,
		postFullCycle: postFullCycle,
	}
	suite.Run(t, &s)
}
