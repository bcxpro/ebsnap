package snapshot

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/ebs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestEBSRetriever(t *testing.T) {
	s := retrieverTestSuite{
		prepare: testEBSRetrieverWithFunction,
	}
	suite.Run(t, &s)
}

func testEBSRetrieverWithFunction(t *testing.T) (Retriever, Info, func(blockIndex int32) *BlockContent, error) {

	snapshotId := "snap-ffff0000000000001"
	ebsClient, ec2Client := getClients(false) // fake clients

	// the retriever uses a fake snapshot
	rt := NewEBSSnapshotRetriever(snapshotId,
		WithRetrieverEBSClient(ebsClient), WithRetrieverEC2Client(ec2Client))

	// retrieve block size and volume size
	blocksList, err := ebsClient.ListSnapshotBlocks(context.TODO(), &ebs.ListSnapshotBlocksInput{SnapshotId: &snapshotId})
	if !assert.NoError(t, err) {
		return nil, Info{}, nil, err
	}

	expectedInfo := Info{
		BlockSize:        *blocksList.BlockSize,
		VolumeSize:       *blocksList.VolumeSize,
		SnapshotId:       snapshotId,
		Description:      "Synthetic snapshot 1",
		ParentSnapshotId: "",
		Tags:             map[string]string{"FakeSnapType": "synthetic", "FakeSnapId": "snap-ffff0000000000001"},
	}

	return rt, expectedInfo, createFakeBlockContent, nil
}

func TestEBSRetriever_NextBlock_Failure(t *testing.T) {
	snapshotId := "snap-ffff0000000000002"
	ebsClient, ec2Client := getClients(false) // fake clients

	// the retriever uses a fake snapshot
	rt := NewEBSSnapshotRetriever(snapshotId, WithRetrieverEBSClient(ebsClient), WithRetrieverEC2Client(ec2Client))

	err := rt.Initialize(context.TODO())
	require.NoError(t, err)

	_, err = rt.NextBlock(context.TODO())
	if assert.Error(t, err) {
		return
	}
}

func TestEBSRetriever_NextBlock_CorruptData(t *testing.T) {
	snapshotId := "snap-ffff0000000000003"
	ebsClient, ec2Client := getClients(false) // fake clients

	// the retriever uses a fake snapshot
	rt := NewEBSSnapshotRetriever(snapshotId, WithRetrieverEBSClient(ebsClient), WithRetrieverEC2Client(ec2Client))

	err := rt.Initialize(context.TODO())
	require.NoError(t, err)

	_, err = rt.NextBlock(context.TODO())
	if assert.Error(t, err) {
		return
	}
}
