package snapshot

import (
	"bytes"
	"ebsnapshot/protos"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestBinaryRetriever(t *testing.T) {
	s := retrieverTestSuite{
		prepare: testBinaryRetrieverWithFunction,
	}
	suite.Run(t, &s)
}

func testBinaryRetrieverWithFunction(t *testing.T) (Retriever, Info, func(blockIndex int32) *BlockContent, error) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1

	fakeRd := newFakeBinarySnapshotReader(blockSize, volumeSize)

	rt := NewBinaryRetriever(fakeRd)

	expectedInfo := Info{
		BlockSize:        blockSize,
		VolumeSize:       volumeSize,
		SnapshotId:       "snap-ffff0000000000001",
		Description:      "Synthetic binary snapshot 1",
		ParentSnapshotId: "",
		Tags: map[string]string{
			"FakeSnapId":   "snap-ffff0000000000001",
			"FakeSnapType": "synthetic",
		},
	}

	return rt, expectedInfo, createFakeBlockContent, nil
}

func TestDecoder(t *testing.T) {

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
	encoded, _ := binaryEncodeRecord(&record)

	reader := bytes.NewReader(encoded)
	decoded, err := binaryDecodeRecord(reader)
	if err != nil {
		t.FailNow()
	}

	if h, ok := decoded.Record.(*protos.SnapshotRecord_Header); ok {
		assert.Equal(t, header.Version, h.Header.Version)
		assert.Equal(t, header.SnapshotId, h.Header.SnapshotId)
		assert.Equal(t, header.ParentSnapshotId, h.Header.ParentSnapshotId)
		assert.Equal(t, header.VolumeSize, h.Header.VolumeSize)
		assert.Equal(t, header.BlockSize, h.Header.BlockSize)
	} else {
		assert.Fail(t, "decoded record is not a header record")
	}

}
