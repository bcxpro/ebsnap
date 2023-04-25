package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestEBSCreator(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1

	prepare := func(t *testing.T, fakeBlockCreator func(blockIndex int32) *BlockContent) (Creator, Info, BlockList, func(blockIndex int32) *BlockContent) {

		ebsClient, ec2Client := getClients(false)

		maxBlockCount := blockCount(volumeSize, blockSize)

		cr := NewEBSCreator(WithEBSCreatorEBSClient(ebsClient), WithEBSCreatorEC2Client(ec2Client))

		sInfo := Info{
			SnapshotId:       "s-abc123",
			ParentSnapshotId: "",
			VolumeId:         "vol-fff",
			VolumeSize:       volumeSize,
			BlockSize:        blockSize,
			Description:      "test raw snapshot",
			Tags: map[string]string{"UserTag": "Uservalue",
				"dlm:something": "dlmValue",
				"aws:something": "awsValue",
				"type":          "xxxx",
				"typewriter":    "thisisvalid"},
		}

		blockList := make(BlockList, maxBlockCount)

		for i := range blockList {
			blockList[i] = int32(i)
		}

		return cr, sInfo, blockList, createFakeBlockContent
	}

	postFullCycle := func(t *testing.T, completeInfo Info, completeErr error) {
		assert.NoError(t, completeErr)
		assert.NotEqual(t, "", completeInfo.SnapshotId)
		assert.NotEqual(t, "s-abc123", completeInfo.SnapshotId)
		assert.Equal(t, "", completeInfo.ParentSnapshotId)
		assert.Equal(t, "", completeInfo.VolumeId)
		assert.Equal(t, blockSize, completeInfo.BlockSize)
		assert.Equal(t, volumeSize, completeInfo.VolumeSize)
		assert.Equal(t, "test raw snapshot", completeInfo.Description)
		assert.Equal(t, map[string]string{"UserTag": "Uservalue", "typewriter": "thisisvalid"}, completeInfo.Tags)
	}

	s := creatorTestSuite{
		prepare:       prepare,
		postFullCycle: postFullCycle,
	}
	suite.Run(t, &s)
}
