package snapshot

import (
	"bytes"
	"crypto/sha256"
	"ebsnapshot/protos"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
)

func TestBinaryCreator(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	var fakeWr *fakeBinarySnapshotWriter

	prepare := func(t *testing.T, fakeBlockCreator func(blockIndex int32) *BlockContent) (Creator, Info, BlockList, func(blockIndex int32) *BlockContent) {

		maxBlockCount := blockCount(volumeSize, blockSize)

		sInfo := Info{
			SnapshotId:       "s-abcdef",
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

		postHeaderCheck := func(h protos.SnapshotRecord_Header) error {
			if h.Header.VolumeSize != int64(volumeSize) {
				return fmt.Errorf("volume size mismatch, expected %v, actual %v", volumeSize, h.Header.VolumeSize)
			}
			if h.Header.BlockSize != int32(blockSize) {
				return fmt.Errorf("block size mismatch, expected %v, actual %v", blockSize, h.Header.BlockSize)
			}
			if h.Header.Description != sInfo.Description {
				return fmt.Errorf("description mismatch, expected \"%v\", actual \"%v\"", sInfo.Description, h.Header.Description)
			}
			if h.Header.ParentSnapshotId != sInfo.ParentSnapshotId {
				return fmt.Errorf("parent snapshot id mismatch, expected \"%v\", actual \"%v\"", sInfo.ParentSnapshotId, h.Header.ParentSnapshotId)
			}
			if h.Header.SnapshotId != sInfo.SnapshotId {
				return fmt.Errorf("snapshot id mismatch, expected \"%v\", actual \"%v\"", sInfo.SnapshotId, h.Header.SnapshotId)
			}
			// TODO: check tags
			return nil
		}

		postBlockListCheck := func(bl protos.SnapshotRecord_BlockList) error {
			var lastBlockIndex int32 = -1
			if len(bl.BlockList.Blocks) > maxBlockCount {
				return fmt.Errorf("block list length larger than its maximum size, maximum %v, actual %v", maxBlockCount, len(bl.BlockList.Blocks))
			}
			for _, bIndex := range bl.BlockList.Blocks {
				if bIndex >= int32(maxBlockCount) || bIndex < 0 {
					return fmt.Errorf("block index %v out of the allowed range 0-%v", bIndex, maxBlockCount-1)
				}
				if bIndex <= lastBlockIndex {
					return fmt.Errorf("block index %v not expected after block %v", bIndex, lastBlockIndex)
				}
				lastBlockIndex = bIndex
			}
			return nil
		}

		postBlockCheck := func(b protos.SnapshotRecord_Block) error {
			if b.Block.BlockIndex >= int32(maxBlockCount) || b.Block.BlockIndex < 0 {
				return fmt.Errorf("block index %v out of the allowed range 0-%v", b.Block.BlockIndex, maxBlockCount-1)
			}
			bc, err := protoBlockToBlockContent(&b)
			if err != nil {
				return fmt.Errorf("failed to convert proto block to block content: %w", err)
			}
			if len(bc.Data) != int(blockSize) {
				return fmt.Errorf("block size mismatch, expected %v, actual %v", blockSize, len(b.Block.Data))
			}
			if !bc.verifyIntegrity() {
				return fmt.Errorf("checksum validation error on block %v", bc.BlockIndex)
			}
			testBlockContent := createFakeBlockContent(bc.BlockIndex)
			if !bytes.Equal(testBlockContent.Data, bc.Data) {
				return fmt.Errorf("block index %v data mismatch", bc.BlockIndex)
			}
			return nil
		}

		fakeWr = newFakeBinarySnapshotWriter(postHeaderCheck, postBlockListCheck, postBlockCheck)

		cr := NewBinaryCreator(fakeWr)

		return cr, sInfo, blockList, createFakeBlockContent
	}

	postFullCycle := func(t *testing.T, completeInfo Info, completeErr error) {
		assert.NoError(t, completeErr)
		expectedInfo := Info{
			SnapshotId:       "s-abcdef",
			ParentSnapshotId: "",
			VolumeId:         "vol-fff",
			VolumeSize:       volumeSize,
			BlockSize:        blockSize,
			Description:      "test raw snapshot",
		}
		assert.Equal(t, expectedInfo, completeInfo)
		err := fakeWr.Close()
		assert.NoError(t, err)
		assert.NoError(t, fakeWr.ErrorStatus)
	}

	s := creatorTestSuite{
		Suite:         suite.Suite{},
		prepare:       prepare,
		postFullCycle: postFullCycle,
	}
	suite.Run(t, &s)
}

func TestEncoder(t *testing.T) {

	header := protos.SnapshotHeader{
		Version:          1,
		SnapshotId:       "snap-123",
		ParentSnapshotId: "",
		VolumeSize:       10,
		BlockSize:        512 * 1024,
	}
	record := protos.SnapshotRecord{
		Record: &protos.SnapshotRecord_Header{Header: &header},
	}
	encoded, err := binaryEncodeRecord(&record)
	if err != nil {
		t.Fatalf("could not encode block: %v", err)
	}

	marshaled, _ := proto.Marshal(&record)
	marshaledLen := byte(len(marshaled))

	assert.Equal(t, encodedRecordLengthFieldLength+int(marshaledLen)+encodedRecordChecksumLength, len(encoded), "encoded record lenght check is wrong")

	assert.Equal(t, []byte{0, 0, 0, marshaledLen}, encoded[:encodedRecordLengthFieldLength])

	assert.Equal(t, marshaled, encoded[encodedRecordLengthFieldLength:encodedRecordLengthFieldLength+len(marshaled)])

	expectedHash := sha256.Sum256(encoded[:len(encoded)-encodedRecordChecksumLength])
	assert.Equal(t, expectedHash[:], encoded[len(encoded)-encodedRecordChecksumLength:])
}
