package snapshot

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestRawRetriever(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	var pr *fakeRawSnapshotReader

	prepare := func(t *testing.T) (Retriever, Info, func(blockIndex int32) *BlockContent, error) {

		pr = newFakeRawSnapshotReader(blockSize, volumeSize, createFakeBlockContent)

		info := Info{
			SnapshotId:       "s-abcde",
			ParentSnapshotId: "",
			VolumeId:         "vol-123",
			VolumeSize:       volumeSize,
			BlockSize:        blockSize,
			Description:      "raw snapshot test sample",
			Tags:             map[string]string{"FakeSnapId": "s-abcde", "FakeSnapType": "fakeRaw"},
		}
		rt := NewRawRetriever(pr, info)

		return rt, info, createFakeBlockContent, nil
	}

	postFullCycle := func(t *testing.T) {
		err := pr.Close()
		assert.NoError(t, err)
		assert.NoError(t, pr.ErrorStatus)
	}

	s := retrieverTestSuite{
		prepare:       prepare,
		postFullCycle: postFullCycle,
	}
	suite.Run(t, &s)
	suite.Run(t, &s)
}

// Tests a raw file that es smaller than the expected size of the snapshot
// If it ends in the middle of a block, the block is padded with zeroes
// The remaining blocks will be full-zero blocks
func TestTruncatedRawSource(t *testing.T) {
	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1

	info := Info{
		SnapshotId:       "s-abcde",
		ParentSnapshotId: "",
		VolumeId:         "vol-123",
		VolumeSize:       volumeSize,
		BlockSize:        blockSize,
		Description:      "raw snapshot test sample",
		Tags:             map[string]string{"FakeSnapId": "s-abcde", "FakeSnapType": "fakeRaw"},
	}

	// We will use a simulated file that is stored in memory
	// Because []byte implements io.Reader this will be the file
	// It is a short file, its first block has content but is not complete 
	content := make([]byte, blockSize/2)
	for i := 1; i < len(content); i++ {
		content[i] = 255
	}
	contentBuf := bytes.NewBuffer(content)
	rt := NewRawRetriever(contentBuf, info)

	err := rt.Initialize(context.TODO())
	if !assert.NoError(t, err) {
		return
	}

	// Read the first block, the only block that has something
	firstBlock, err := rt.NextBlock(context.TODO())
	if !assert.NoError(t, err) {
		return
	}

	expectedBlock := make([]byte, blockSize)
	copy(expectedBlock, content)
	if !assert.EqualValues(t, expectedBlock, firstBlock.Data) {
		return
	}

	// There are no more blocks after this because the contents are zero (the file was truncated in the middle of the first block)
	_, err = rt.NextBlock(context.TODO())
	if !assert.ErrorIs(t, err, io.EOF) {
		return
	}

}
