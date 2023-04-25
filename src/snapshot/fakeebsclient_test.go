package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	ebsTypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

func startSnapshot(c EBSSnapshotWriteAPI) (*ebs.StartSnapshotOutput, error) {

	params := ebs.StartSnapshotInput{
		VolumeSize:  aws.Int64(1),
		Description: aws.String("Test snapshot"),
		Encrypted:   aws.Bool(false),
		Tags:        []ebsTypes.Tag{{Key: aws.String("FakeSnapType"), Value: aws.String("new")}},
	}
	output, err := c.StartSnapshot(context.Background(), &params)
	return output, err
}

func TestStartSnapshot(t *testing.T) {

	c, _ := getClients(false)

	output, err := startSnapshot(c)

	assert.Nil(t, err)
	assert.Equal(t, int32(512*1024), *output.BlockSize)
	assert.Equal(t, int64(1), *output.VolumeSize)
	assert.Equal(t, "Test snapshot", *output.Description)
	assert.Equal(t, ebsTypes.StatusPending, output.Status)
	assert.NotNil(t, output.StartTime)
	assert.Equal(t, []ebsTypes.Tag{{Key: aws.String("FakeSnapType"), Value: aws.String("new")}}, output.Tags)
	assert.Equal(t, 1, 1)
}

func putSnapshotBlock(c EBSSnapshotWriteAPI, snapshotId string, bc *BlockContent) (*ebs.PutSnapshotBlockOutput, error) {

	b64Checksum := base64.StdEncoding.EncodeToString(bc.Checksum)
	input := ebs.PutSnapshotBlockInput{
		BlockData:         bytes.NewReader(bc.Data),
		BlockIndex:        &bc.BlockIndex,
		Checksum:          &b64Checksum,
		ChecksumAlgorithm: bc.ChecksumAlgorithm,
		DataLength:        &bc.DataLength,
		SnapshotId:        &snapshotId,
	}
	return c.PutSnapshotBlock(context.Background(), &input)
}

func TestPutSnapshotBlock(t *testing.T) {

	c, _ := getClients(false)

	bc := createFakeBlockContent(257)
	assert.EqualValues(t, 512*1024, bc.DataLength)

	startOutput, err := startSnapshot(c)
	assert.Nil(t, err)

	putOutput, err := putSnapshotBlock(c, *startOutput.SnapshotId, bc)
	assert.Nil(t, err)

	assert.Equal(t, base64.StdEncoding.EncodeToString(bc.Checksum), *putOutput.Checksum)
	assert.Equal(t, ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256, putOutput.ChecksumAlgorithm)
}

type fakeSnapshotCompletionOptions struct {
	corruptBase64Checksum    bool
	invalidAggregationMethod bool
	invalidChecksumAlgorithm bool
}

func fakeSnapshotCompletion(c EBSSnapshotWriteAPI, snapshotId string, completeChecksum []byte,
	changedBlockCount int32, optFns ...func(*fakeSnapshotCompletionOptions)) (*ebs.CompleteSnapshotOutput, error) {

	b64CompleteChecksum := base64.StdEncoding.EncodeToString(completeChecksum)
	completeInput := ebs.CompleteSnapshotInput{
		ChangedBlocksCount:        &changedBlockCount,
		SnapshotId:                &snapshotId,
		Checksum:                  &b64CompleteChecksum,
		ChecksumAggregationMethod: ebsTypes.ChecksumAggregationMethodChecksumAggregationLinear,
		ChecksumAlgorithm:         ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256,
	}

	options := fakeSnapshotCompletionOptions{}
	for _, f := range optFns {
		f(&options)
	}

	if options.corruptBase64Checksum {
		completeInput.Checksum = aws.String("Abracadabra!")
	}

	if options.invalidAggregationMethod {
		completeInput.ChecksumAggregationMethod = "RIDICULOUS"
	}

	if options.invalidChecksumAlgorithm {
		completeInput.ChecksumAlgorithm = "SHA666"
	}

	completeOutput, err := c.CompleteSnapshot(context.TODO(), &completeInput)
	return completeOutput, err
}

func TestPutSnapshotBlockAndCompleSnapshot(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// Put a single block
	bc := createFakeBlockContent(257)
	assert.EqualValues(t, 512*1024, bc.DataLength)
	_, err = putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc)
	assert.Nil(t, err)

	// Complete the snapshot with correct parameters
	completeChecksum := sha256.Sum256(bc.Checksum)
	completeOutput, err := fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum[:], 1)
	assert.Nil(t, err)
	assert.Contains(t, []ebsTypes.Status{ebsTypes.StatusPending, ebsTypes.StatusCompleted}, completeOutput.Status)

	// Eventually the status of the snapshot will be completed
	state, err := waitForSnapshotTerminalState(ec2Client, *startOutput.SnapshotId, 3*time.Minute, nil)
	assert.NoError(t, err)
	assert.Equal(t, ec2Types.SnapshotStateCompleted, state)
}

func TestDescribeSnapshot(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// Put a single block
	bc := createFakeBlockContent(257)
	assert.EqualValues(t, 512*1024, bc.DataLength)
	_, err = putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc)
	assert.Nil(t, err)

	// Complete the snapshot with correct parameters
	completeChecksum := sha256.Sum256(bc.Checksum)
	_, err = fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum[:], 1)
	assert.Nil(t, err)

	// Eventually the status of the snapshot will be completed
	state, err := waitForSnapshotTerminalState(ec2Client, *startOutput.SnapshotId, 3*time.Minute,
		func(o *ec2.DescribeSnapshotsOutput, err error) {
			// Check that describe snapshot returns correct parameters when invoked by the waiter function
			assert.NoError(t, err)
			assert.EqualValues(t, 1, len(o.Snapshots))
			s := o.Snapshots[0]
			assert.Equal(t, "Test snapshot", *s.Description)
			assert.Equal(t, *startOutput.SnapshotId, *s.SnapshotId)
			assert.NotNil(t, s.StartTime)
		})
	assert.NoError(t, err)
	assert.Equal(t, ec2Types.SnapshotStateCompleted, state)
}

func TestDescribeSnapshotsTags(t *testing.T) {

	containsTag := func(tags []ec2Types.Tag, key, value string) bool {
		for _, t := range tags {
			if key == *t.Key && value == *t.Value {
				return true
			}
		}
		return false
	}

	_, ec2Client := getClients(false)
	snapshotId := "snap-ffff0000000000001"
	describeInput := ec2.DescribeSnapshotsInput{
		SnapshotIds: []string{snapshotId},
	}
	describeOut, err := ec2Client.DescribeSnapshots(context.Background(), &describeInput)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	if !assert.Len(t, describeOut.Snapshots, 1) {
		return
	}
	s := describeOut.Snapshots[0]

	if !assert.Len(t, s.Tags, 2) {
		return
	}

	assert.True(t, containsTag(s.Tags, "FakeSnapType", "synthetic"))
	assert.True(t, containsTag(s.Tags, "FakeSnapId", "snap-ffff0000000000001"))
}

func TestStartSnapshotTags(t *testing.T) {

	containsTag := func(tags []ec2Types.Tag, key, value string) bool {
		for _, t := range tags {
			if key == *t.Key && value == *t.Value {
				return true
			}
		}
		return false
	}

	ebsClient, ec2Client := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	if !assert.Nil(t, err) {
		return
	}

	describeInput := ec2.DescribeSnapshotsInput{
		SnapshotIds: []string{*startOutput.SnapshotId},
	}
	describeOut, err := ec2Client.DescribeSnapshots(context.Background(), &describeInput)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, describeOut.Snapshots, 1) {
		return
	}
	s := describeOut.Snapshots[0]

	if !assert.Len(t, s.Tags, 1) {
		return
	}

	assert.True(t, containsTag(s.Tags, "FakeSnapType", "new"))
}

func TestPutBlockWrongIndex(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// put a block with an index out of the bounds of the snapshot
	var wrongBlockIndex int32 = 450000
	bc1 := createFakeBlockContent(wrongBlockIndex)
	_, err = putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc1)
	assert.Error(t, err)

	assertEBSValidationException(t, err, "PutSnapshotBlock",
		aws.String(fmt.Sprintf("Block index %v is beyond the volume size extent", wrongBlockIndex)),
		ebsTypes.ValidationExceptionReasonInvalidBlock, "")
}

func TestPutBlockWrongChecksum(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// create a block, tweak its checksum and try to put it
	bc1 := createFakeBlockContent(1)
	bc1.Checksum = make([]byte, 32)
	_, err = putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc1)
	assert.Error(t, err)

	var ae *smithy.GenericAPIError
	if assert.ErrorAs(t, err, &ae) {
		assert.Equal(t, "InvalidSignatureException", ae.Code)
		assert.Regexp(t, `^The value passed in as x-amz-Checksum does not match the computed checksum\. Computed checksum: ([A-Za-z0-9+\/]+={0,2}) expected checksum: ([A-Za-z0-9+\/]+={0,2})$`,
			ae.Message)
	}

	var oe *smithy.OperationError
	if assert.ErrorAs(t, err, &oe) {
		assert.Equal(t, "EBS", oe.ServiceID)
		assert.Equal(t, "PutSnapshotBlock", oe.OperationName)
	}
}

func TestPutBlockWrongSnapshotId(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// break the snapshot id
	*startOutput.SnapshotId = "snap-12340000000000000"

	// Create a block and put it with the wrong snapshot id
	bc1 := createFakeBlockContent(1)
	_, err = putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc1)
	assert.Error(t, err)

	assertEBSValidationException(t, err, "PutSnapshotBlock",
		aws.String("Value (snap-12340000000000000) for parameter snapshotId is invalid. Expected: 'snap-...'."),
		ebsTypes.ValidationExceptionReasonInvalidSnapshotId, "")
}

func TestCompleteSnapshotWrongSnapshotId(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// Put a single block with the correct checksum and calculate the full checksum of the snapshot
	h := sha256.New()
	putBlockCount := 1
	bc1 := createFakeBlockContent(1)
	_, err = putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc1)
	assert.Nil(t, err)
	h.Write(bc1.Checksum)
	completeChecksum := h.Sum(nil)

	// tweak the snapshot id to be wrong
	*startOutput.SnapshotId = "snap-12340000000000000"

	// complete the snapshot
	_, err = fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum,
		int32(putBlockCount))
	assert.Error(t, err)

	assertEBSValidationException(t, err, "CompleteSnapshot",
		aws.String("Value (snap-12340000000000000) for parameter snapshotId is invalid. Expected: 'snap-...'."),
		ebsTypes.ValidationExceptionReasonInvalidSnapshotId, "")
}
func TestCompleteSnapshotBlockCount(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// puts several blocks not in order and calculates the checksum in order
	putBlockCount, completeChecksum := putSynthBlocks(startOutput, ebsClient, t)

	// Completes the snapshot witha a wrong block count
	wrongPutBlockCount := int32(putBlockCount - 1)
	completeOutput, err := fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum,
		wrongPutBlockCount)
	assert.NoError(t, err)
	assert.NotNil(t, completeOutput)
	// The complete operation succeeds with the pending status despite the checksum being wrong
	assert.Equal(t, ebsTypes.StatusPending, completeOutput.Status)

	// Eventually the status of the snapshot will be error
	state, err := waitForSnapshotTerminalState(ec2Client, *startOutput.SnapshotId, 3*time.Minute, nil)
	assert.NoError(t, err)
	assert.Equal(t, ec2Types.SnapshotStateError, state)
}

func putSynthBlocks(startOutput *ebs.StartSnapshotOutput, ebsClient EBSSnapshotWriteAPI, t *testing.T) (int, []byte) {

	h := sha256.New()
	putBlockCount := 0
	fullSizeInBlocks := blockCount(*startOutput.VolumeSize, *startOutput.BlockSize)
	for i := 0; i < fullSizeInBlocks; i += 512 {
		bc1 := createFakeBlockContent(int32(i + 511))
		_, err := putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc1)
		assert.Nil(t, err)
		bc0 := createFakeBlockContent(int32(i))
		_, err = putSnapshotBlock(ebsClient, *startOutput.SnapshotId, bc0)
		assert.Nil(t, err)
		h.Write(bc0.Checksum)
		h.Write(bc1.Checksum)
		putBlockCount += 2
	}
	completeChecksum := h.Sum(nil)
	return putBlockCount, completeChecksum
}

func TestCompleteSnapshotWrongChecksum(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// puts several blocks not in order and calculates the checksum in order
	putBlockCount, _ := putSynthBlocks(startOutput, ebsClient, t)

	// Completes the snpashot with a wrong checksum
	wrongChecksum := make([]byte, 32)
	completeOutput, err := fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, wrongChecksum,
		int32(putBlockCount))
	assert.NoError(t, err)
	assert.NotNil(t, completeOutput)

	// The complete operation succeeds with the pending status despite the checksum being wrong
	assert.Equal(t, ebsTypes.StatusPending, completeOutput.Status)

	// Eventually the status of the snapshot will be error
	state, err := waitForSnapshotTerminalState(ec2Client, *startOutput.SnapshotId, 3*time.Minute, nil)
	assert.NoError(t, err)
	assert.Equal(t, ec2Types.SnapshotStateError, state)
}

func TestCompleteSnapshotWrongChecksumAlgorithm(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// puts several blocks not in order and calculates the checksum in order
	putBlockCount, completeChecksum := putSynthBlocks(startOutput, ebsClient, t)

	// Completes the snapshot with a wrong checksum algorithm
	_, err = fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum,
		int32(putBlockCount), func(o *fakeSnapshotCompletionOptions) { o.invalidChecksumAlgorithm = true })
	assert.Error(t, err)

	assertEBSValidationException(t, err, "CompleteSnapshot", nil, ebsTypes.ValidationExceptionReason(""), "")
}

func TestCompleteSnapshotWrongBase64Checksum(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// puts several blocks not in order and calculates the checksum in order
	putBlockCount, completeChecksum := putSynthBlocks(startOutput, ebsClient, t)

	// Completes the snapshot with a wrong checksum algorithm
	_, err = fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum,
		int32(putBlockCount), func(o *fakeSnapshotCompletionOptions) {
			o.corruptBase64Checksum = true
		})
	assert.Error(t, err)

	assertEBSValidationException(t, err, "CompleteSnapshot", nil, ebsTypes.ValidationExceptionReason(""), "")

}

func TestCompleteSnapshotWrongAggregationMethod(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// puts several blocks not in order and calculates the checksum in order
	putBlockCount, completeChecksum := putSynthBlocks(startOutput, ebsClient, t)

	// Completes the snapshot with a wrong checksum algorithm
	_, err = fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum,
		int32(putBlockCount), func(o *fakeSnapshotCompletionOptions) {
			o.invalidAggregationMethod = true
		})
	assert.Error(t, err)
	assertEBSValidationException(t, err, "CompleteSnapshot", nil, ebsTypes.ValidationExceptionReason(""), "")

}

func TestFullSuccessful(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Start a snapshot
	startOutput, err := startSnapshot(ebsClient)
	assert.Nil(t, err)

	// puts several blocks not in order and calculates the checksum in order
	putBlockCount, completeChecksum := putSynthBlocks(startOutput, ebsClient, t)

	// Completes the snapshot
	_, err = fakeSnapshotCompletion(ebsClient, *startOutput.SnapshotId, completeChecksum,
		int32(putBlockCount))
	assert.Nil(t, err)

	// Eventually the status of the snapshot will be completed
	state, err := waitForSnapshotTerminalState(ec2Client, *startOutput.SnapshotId, 3*time.Minute,
		func(o *ec2.DescribeSnapshotsOutput, err error) {
			// Check that describe snapshot returns correct parameters when invoked by the waiter function
			assert.NoError(t, err)
			assert.EqualValues(t, 1, len(o.Snapshots))
			s := o.Snapshots[0]
			assert.Equal(t, "Test snapshot", *s.Description)
			assert.Equal(t, *startOutput.SnapshotId, *s.SnapshotId)
			assert.NotNil(t, s.StartTime)
		})
	assert.NoError(t, err)
	assert.Equal(t, ec2Types.SnapshotStateCompleted, state)
}

func assertEBSValidationException(t *testing.T, err error, operation string, message *string, reason ebsTypes.ValidationExceptionReason, msg string) {
	var ve *ebsTypes.ValidationException
	if assert.ErrorAs(t, err, &ve, msg) {
		if message == nil {
			assert.Nil(t, ve.Message, msg)
		} else {
			assert.NotNil(t, ve.Message, msg)
			if ve.Message != nil {
				assert.Equal(t, *message, *ve.Message, msg)
			}
		}
		assert.Equal(t, reason, ve.Reason, msg)
	}

	var oe *smithy.OperationError
	if assert.ErrorAs(t, err, &oe, msg) {
		assert.Equal(t, "EBS", oe.ServiceID, msg)
		assert.Equal(t, operation, oe.OperationName, msg)
	}
}

func assertEBSResourceNotFoundException(t *testing.T, err error, operation string, message *string, reason ebsTypes.ResourceNotFoundExceptionReason, msg string) {
	var ve *ebsTypes.ResourceNotFoundException
	if assert.ErrorAs(t, err, &ve, msg) {
		if message == nil {
			assert.Nil(t, ve.Message, msg)
		} else {
			assert.NotNil(t, ve.Message, msg)
			if ve.Message != nil {
				assert.Equal(t, *message, *ve.Message, msg)
			}
		}
		assert.Equal(t, reason, ve.Reason, msg)
	}

	var oe *smithy.OperationError
	if assert.ErrorAs(t, err, &oe, msg) {
		assert.Equal(t, "EBS", oe.ServiceID, msg)
		assert.Equal(t, operation, oe.OperationName, msg)
	}
}

func TestListSnapshotBlocksWrongSnapshotId(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Real API results:
	// snap-THING --> ValidationException empty reason
	// snap-00000000000000001 --> ResourceNotFoundExceptionReasonSnapshotNotFound "ResourceNotFoundException: The snapshot 'snap-00000000000000001' does not exist."
	// snap-f0000000000000001 --> ValidationExceptionReason"INVALID_SNAPSHOT_ID"
	//   (this cannot be tested because we do not know how to detect it, it looks well formatted)
	// This tests a snapshot ID that is syntatically invalid (does not match regexp ^snap-[0-9a-f]+$)
	input := ebs.ListSnapshotBlocksInput{
		SnapshotId: aws.String("snap-THING"),
	}
	_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
	assert.Error(t, err)
	assertEBSValidationException(t, err, "ListSnapshotBlocks", nil, ebsTypes.ValidationExceptionReason(""), "")

	// snap-00000000000000001
	input = ebs.ListSnapshotBlocksInput{
		SnapshotId: aws.String("snap-00000000000000001"),
	}
	_, err = ebsClient.ListSnapshotBlocks(context.Background(), &input)
	assert.Error(t, err)
	assertEBSResourceNotFoundException(t, err, "ListSnapshotBlocks", aws.String("The snapshot 'snap-00000000000000001' does not exist."),
		ebsTypes.ResourceNotFoundExceptionReasonSnapshotNotFound,
		"expecting an snapshot not found error")

}

func TestListSnapshotBlocksSnapshotIdNotExist(t *testing.T) {

	ebsClient, _ := getClients(false)

	input := ebs.ListSnapshotBlocksInput{
		SnapshotId: aws.String("snap-00000000000000001"),
	}
	_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
	assert.Error(t, err)

	assertEBSResourceNotFoundException(t, err, "ListSnapshotBlocks",
		aws.String("The snapshot 'snap-00000000000000001' does not exist."),
		ebsTypes.ResourceNotFoundExceptionReasonSnapshotNotFound, "check non-existant snapshot")
}

func TestListSnapshotBlocksWrongMaxResults(t *testing.T) {

	ebsClient, _ := getClients(false)

	testBlock := func(index int32) {
		msg := fmt.Sprintf("testing MaxResults=%v", index)
		input := ebs.ListSnapshotBlocksInput{
			SnapshotId: aws.String("snap-08c71954ee482248f"),
			MaxResults: aws.Int32(index),
		}
		_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
		assert.Error(t, err)
		assertEBSValidationException(t, err, "ListSnapshotBlocks", nil, ebsTypes.ValidationExceptionReason(""), msg)
	}

	testBlock(99)
	testBlock(10001)
}

func TestListSnapshotBlocksWrongStartingBlockIndex(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Checks that the snapshot exists and is completed
	// snapshotId := "snap-061fd926896e47e17" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status

	_, exists := assertExistingSnapshot(snapshotId, ec2Client, t)
	if !exists {
		return
	}

	// Actual test knowing that the snapshot exists
	testStartingBlockIndex := func(index int32) {
		msg := fmt.Sprintf("testing StartingBlockIndex=%v", index)
		input := ebs.ListSnapshotBlocksInput{
			SnapshotId:         aws.String(snapshotId), // Real snapshot that exist
			StartingBlockIndex: aws.Int32(index),
		}
		_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
		assert.Error(t, err)
		assertEBSValidationException(t, err, "ListSnapshotBlocks", nil, ebsTypes.ValidationExceptionReason(""), msg)
	}

	testStartingBlockIndex(-1)
}

func TestListSnapshotBlocksInvalidNextToken(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Checks that the snapshot exists and is completed
	// snapshotId := "snap-061fd926896e47e17" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status

	_, exists := assertExistingSnapshot(snapshotId, ec2Client, t)
	if !exists {
		return
	}

	// Actual test knowing that the snapshot exists
	testStartingNextToken := func(token []byte) {
		msg := fmt.Sprintf("testing NextToken=%v", token)

		encodedToken := base64.StdEncoding.EncodeToString(token)
		input := ebs.ListSnapshotBlocksInput{
			SnapshotId: aws.String(snapshotId), // Real snapshot that exist
			NextToken:  aws.String(encodedToken),
		}
		_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
		assert.Error(t, err)
		assertEBSValidationException(t, err, "ListSnapshotBlocks",
			aws.String(fmt.Sprintf("The pagination token %v is invalid.", encodedToken)),
			ebsTypes.ValidationExceptionReasonInvalidPageToken, msg)
	}

	testStartingNextToken([]byte{1, 2, 3})
	testStartingNextToken(make([]byte, 20))
}

func TestListSnapshotIterateNextToken(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Checks that the snapshot exists and is completed
	// snapshotId := "snap-0dbb927cd40d73e30" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status
	// snapshotId := "snap-061fd926896e47e17" // This snapshot must exist and be in completed status
	snapshot, exists := assertExistingSnapshot(snapshotId, ec2Client, t)
	if !exists {
		return
	}

	// Iterate over the block list using NextToken

	var nextToken *string
	var maxBlocks int = 100
	for {
		input := ebs.ListSnapshotBlocksInput{
			SnapshotId: aws.String(snapshotId), // Real snapshot that exist
			NextToken:  nextToken,
			MaxResults: aws.Int32(int32(maxBlocks)),
		}
		out, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
		assert.NoError(t, err)
		if err != nil {
			return
		}

		assert.Greater(t, len(out.Blocks), 0)
		assert.LessOrEqual(t, len(out.Blocks), maxBlocks)
		assert.Equal(t, *out.VolumeSize, int64(*snapshot.VolumeSize))
		assert.Equal(t, int32(512*1024), *out.BlockSize)
		assert.NotNil(t, out.ExpiryTime)
		nextToken = out.NextToken
		if nextToken == nil {
			assert.Less(t, len(out.Blocks), maxBlocks)
			break
		}
		assert.Equal(t, len(out.Blocks), maxBlocks)
		assert.Regexp(t, "^[A-Za-z0-9+/=]+$", *out.NextToken)
	}

}

func TestListSnapshotWrongMaxResults(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Checks that the snapshot exists and is completed
	// snapshotId := "snap-0dbb927cd40d73e30" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status
	// snapshotId := "snap-061fd926896e47e17" // This snapshot must exist and be in completed status
	_, exists := assertExistingSnapshot(snapshotId, ec2Client, t)
	if !exists {
		return
	}

	test := func(maxBlocks int) {
		input := ebs.ListSnapshotBlocksInput{
			SnapshotId: aws.String(snapshotId), // Real snapshot that exist
			MaxResults: aws.Int32(int32(maxBlocks)),
		}
		_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
		assert.Error(t, err)
		assertEBSValidationException(t, err, "ListSnapshotBlocks",
			nil, ebsTypes.ValidationExceptionReason(""), "MaxResults out of allowed 100 - 10000 range")
	}

	test(99)
	test(10001)
}

func TestListSnapshotSnapshotNotExists(t *testing.T) {

	ebsClient, _ := getClients(false)

	// Checks that the snapshot does not exists and is completed
	snapshotId := "snap-0ae3dda5966b9b089" // This is a valid snapshot Id that does not exist

	input := ebs.ListSnapshotBlocksInput{
		SnapshotId: aws.String(snapshotId), // Real snapshot that exist
	}
	_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
	assert.Error(t, err)
	assertEBSResourceNotFoundException(t, err, "ListSnapshotBlocks",
		aws.String(fmt.Sprintf("The snapshot '%v' does not exist.", snapshotId)),
		ebsTypes.ResourceNotFoundExceptionReasonSnapshotNotFound, "It is expected that the snapshot does not exist")
}

func TestListSnapshotInvalidPageToken(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// Checks that the snapshot exists and is completed
	// snapshotId := "snap-0dbb927cd40d73e30" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status
	_, exists := assertExistingSnapshot(snapshotId, ec2Client, t)
	if !exists {
		return
	}

	test := func(token string) {
		input := ebs.ListSnapshotBlocksInput{
			SnapshotId: aws.String(snapshotId), // Real snapshot that exist
			NextToken:  &token,
		}
		_, err := ebsClient.ListSnapshotBlocks(context.Background(), &input)
		assert.Error(t, err)
		assertEBSValidationException(t, err, "ListSnapshotBlocks",
			aws.String(fmt.Sprintf("The pagination token %s is invalid.", token)),
			ebsTypes.ValidationExceptionReasonInvalidPageToken, "The NextToken provided must be invalid")
	}

	test("KK")
}

func assertExistingSnapshot(snapshotId string, ec2Client EC2SnapshotAPI, t *testing.T) (*ec2Types.Snapshot, bool) {

	describeInput := ec2.DescribeSnapshotsInput{
		SnapshotIds: []string{snapshotId},
	}
	describeOut, err := ec2Client.DescribeSnapshots(context.Background(), &describeInput)
	assert.NoError(t, err)
	if err != nil {
		return nil, false
	}
	assert.Equal(t, 1, len(describeOut.Snapshots))
	s := describeOut.Snapshots[0]
	state := s.State
	assert.Equal(t, ec2Types.SnapshotStateCompleted, state)
	return &s, ec2Types.SnapshotStateCompleted == state
}

func TestGetSnapshotBlock(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// snapshotId := "snap-061fd926896e47e17" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status

	snapshot, ok := assertExistingSnapshot(snapshotId, ec2Client, t)
	assert.NotNil(t, snapshot)
	if !ok {
		assert.Fail(t, fmt.Sprintf("failed to verify that the snapshot %v exists and is completed", snapshotId))
		return
	}

	// Obtain the first block of the snapshot
	// Real snapshot that exist
	list, err := assertListBlocksFirstPage(snapshotId, ebsClient, t)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	firstBlock := list.Blocks[0]

	// retrieve the block
	getInput := ebs.GetSnapshotBlockInput{
		SnapshotId: aws.String(snapshotId),
		BlockIndex: firstBlock.BlockIndex,
		BlockToken: firstBlock.BlockToken,
	}
	out, err := ebsClient.GetSnapshotBlock(context.Background(), &getInput)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	dataBuf := bytes.Buffer{}
	dataBuf.ReadFrom(out.BlockData)
	dataChecksum := sha256.Sum256(dataBuf.Bytes())
	assert.Equal(t, list.BlockSize, out.DataLength)
	assert.Equal(t, *out.DataLength, int32(dataBuf.Len()))
	assert.Equal(t, ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256, out.ChecksumAlgorithm)
	assert.Equal(t, *out.Checksum, base64.StdEncoding.EncodeToString(dataChecksum[:]))

}

func TestGetSnapshotBlockWrongToken(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// snapshotId := "snap-061fd926896e47e17" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status

	snapshot, ok := assertExistingSnapshot(snapshotId, ec2Client, t)
	assert.NotNil(t, snapshot)
	if !ok {
		assert.Fail(t, fmt.Sprintf("failed to verify that the snapshot %v exists and is completed", snapshotId))
		return
	}

	// Obtain the first block of the snapshot
	// Real snapshot that exist
	list, err := assertListBlocksFirstPage(snapshotId, ebsClient, t)
	assert.NoError(t, err)
	if err != nil {
		return
	}

	firstBlock := list.Blocks[0]

	// retrieve the block
	invalidToken := "INVALIDTOKEN"
	getInput := ebs.GetSnapshotBlockInput{
		SnapshotId: aws.String(snapshotId),
		BlockIndex: firstBlock.BlockIndex,
		BlockToken: aws.String(invalidToken),
	}
	_, err = ebsClient.GetSnapshotBlock(context.Background(), &getInput)
	assert.Error(t, err)
	assertEBSValidationException(t, err, "GetSnapshotBlock",
		aws.String(fmt.Sprintf("The block token %s is invalid.", invalidToken)),
		ebsTypes.ValidationExceptionReasonInvalidBlockToken, "The BlockToken provided must be invalid for the test to succeed")

}

func TestGetSnapshotBlockWrongBlockIndex(t *testing.T) {

	ebsClient, ec2Client := getClients(false)

	// snapshotId := "snap-061fd926896e47e17" // This snapshot must exist and be in completed status
	snapshotId := "snap-ffff0000000000001" // This snapshot must exist and be in completed status

	snapshot, ok := assertExistingSnapshot(snapshotId, ec2Client, t)
	assert.NotNil(t, snapshot)
	if !ok {
		assert.Fail(t, fmt.Sprintf("failed to verify that the snapshot %v exists and is completed", snapshotId))
		return
	}

	// Obtain the first block of the snapshot
	// Real snapshot that exist
	list, err := assertListBlocksFirstPage(snapshotId, ebsClient, t)
	assert.NoError(t, err)
	if err != nil {
		return
	}

	firstBlock := list.Blocks[0]

	// retrieve the block
	wrongBlockIndex := int32(math.MaxInt32) // Hopefully such a high snapshot block does not exit
	getInput := ebs.GetSnapshotBlockInput{
		SnapshotId: aws.String(snapshotId),
		BlockIndex: &wrongBlockIndex,
		BlockToken: firstBlock.BlockToken,
	}
	_, err = ebsClient.GetSnapshotBlock(context.Background(), &getInput)
	assert.Error(t, err)
	assertEBSValidationException(t, err, "GetSnapshotBlock",
		aws.String(fmt.Sprintf("The block token %s is invalid.", *firstBlock.BlockToken)),
		ebsTypes.ValidationExceptionReasonInvalidBlockToken, "The BlockToken provided must be invalid for the test to succeed")

}

func assertListBlocksFirstPage(snapshotId string, ebsClient EBSSnapshotAPI, t *testing.T) (*ebs.ListSnapshotBlocksOutput, error) {
	listInput := ebs.ListSnapshotBlocksInput{
		SnapshotId: aws.String(snapshotId),
	}
	list, err := ebsClient.ListSnapshotBlocks(context.Background(), &listInput)
	assert.NoError(t, err)
	if err != nil {
		return nil, err
	}
	assert.Greater(t, len(list.Blocks), 0)
	return list, nil
}

func newTestRealEBSClient() (*ebs.Client, *ec2.Client) {
	cfg, _ := config.LoadDefaultConfig(context.Background(), config.WithSharedConfigProfile("odisidev"), config.WithRegion("us-east-1"))
	ebsC := ebs.NewFromConfig(cfg)
	ec2C := ec2.NewFromConfig(cfg)
	return ebsC, ec2C
}

func getClients(real bool) (EBSSnapshotAPI, ec2.DescribeSnapshotsAPIClient) {
	if real {
		return newTestRealEBSClient()
	} else {
		fc := NewFakeEBSClient()
		return fc, fc
	}
}

func waitForSnapshotTerminalState(client ec2.DescribeSnapshotsAPIClient, snapshotId string,
	maxWaitDur time.Duration, postDescribe func(*ec2.DescribeSnapshotsOutput, error)) (ec2Types.SnapshotState, error) {

	if maxWaitDur <= 0 {
		return "", fmt.Errorf("maximum wait time for must be greater than zero")
	}

	start := time.Now()
	describeInput := &ec2.DescribeSnapshotsInput{
		SnapshotIds: []string{snapshotId},
	}
	var currentState ec2Types.SnapshotState
	for {
		out, err := client.DescribeSnapshots(context.Background(), describeInput)
		if postDescribe != nil {
			postDescribe(out, err)
		}
		if err != nil {
			return currentState, fmt.Errorf("error describing snapshot: %w", err)
		}
		if len(out.Snapshots) < 1 {
			return currentState, fmt.Errorf("no snapshots returned: %w", err)
		}
		s := out.Snapshots[0]
		currentState = s.State
		if currentState != ec2Types.SnapshotStatePending {
			break
		}
		if time.Now().After(start.Add(maxWaitDur)) {
			break
		}
		time.Sleep(10 * time.Second)
	}

	if currentState != ec2Types.SnapshotStateCompleted && currentState != ec2Types.SnapshotStateError {
		return currentState, fmt.Errorf("no terminal status reached before timeout")
	}
	return currentState, nil
}
