package snapshot

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	ebsTypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type fakeEBSClient struct {
	snapshots   map[string]*fakeEBSSnapshot
	snapshotsMx sync.RWMutex
}

type fakeEBSSnapshot struct {
	snapshotId                 string
	description                string
	blockChecksums             map[int32][]byte
	checksumsMx                sync.RWMutex
	volumeSize                 int64
	blockSize                  int32
	blockCount                 int32
	blockList                  BlockList
	volumeId                   string
	status                     ebsTypes.Status
	startTime                  time.Time
	tags                       map[string]string
	completeTime               time.Time
	completeChecksumError      bool
	completeBlockCountMismatch bool
	synthetic                  bool
	getSnapshotBlockIntercept  func(index int32, output *ebs.GetSnapshotBlockOutput) (*ebs.GetSnapshotBlockOutput, error)
}

func newFakeEBSSnapsot(size int64, description string, volumeId string, tags []ebsTypes.Tag) *fakeEBSSnapshot {

	var blockSize int32 = 512 * 1024
	tagsMap := make(map[string]string)
	for _, t := range tags {
		tagsMap[*t.Key] = *t.Value
	}
	s := fakeEBSSnapshot{
		snapshotId:                 genFakeEbsSnapshotId(),
		description:                description,
		blockChecksums:             map[int32][]byte{},
		volumeSize:                 size,
		blockSize:                  blockSize,
		blockCount:                 int32(blockCount(size, blockSize)),
		volumeId:                   volumeId,
		status:                     ebsTypes.StatusPending,
		startTime:                  time.Now(),
		tags:                       tagsMap,
		completeChecksumError:      false,
		completeBlockCountMismatch: false,
		synthetic:                  false,
	}
	return &s
}

func (s *fakeEBSSnapshot) ec2Tags() []ec2Types.Tag {
	output := make([]ec2Types.Tag, len(s.tags))
	i := 0
	for k, v := range s.tags {
		output[i] = ec2Types.Tag{Key: aws.String(k), Value: aws.String(v)}
		i++
	}
	return output
}

func genFakeEbsSnapshotId() string {
	n := 7
	var digits = []rune("0123456789abcdef")
	s := make([]rune, n)
	for i := range s {
		s[i] = digits[rand.Intn(len(digits))]
	}
	return "snap-0000000000" + string(s)
}

func newFakeSyntheticEBSSnapshot(snapshotId string, size int64,
	description string, volumeId string, tags []ebsTypes.Tag) *fakeEBSSnapshot {

	s := newFakeEBSSnapsot(size, description, volumeId, tags)
	s.status = ebsTypes.StatusCompleted
	s.synthetic = true
	s.snapshotId = snapshotId
	s.blockList = make(BlockList, 0)
	for i := 0; i < int(s.blockCount); i++ {
		if i%10 == 0 {
			s.blockList = append(s.blockList, int32(i))
		}
	}
	return s
}

func NewFakeEBSClient() *fakeEBSClient {
	c := fakeEBSClient{
		snapshots: make(map[string]*fakeEBSSnapshot),
	}
	c.snapshotsMx.Lock()
	defer c.snapshotsMx.Unlock()
	c.snapshots["snap-ffff0000000000001"] = newFakeSyntheticEBSSnapshot(
		"snap-ffff0000000000001",
		8,
		"Synthetic snapshot 1",
		"vol-abcdefg",
		[]ebsTypes.Tag{
			{Key: aws.String("FakeSnapType"), Value: aws.String("synthetic")},
			{Key: aws.String("FakeSnapId"), Value: aws.String("snap-ffff0000000000001")},
		})

	c.snapshots["snap-ffff0000000000002"] = newFakeSyntheticEBSSnapshot(
		"snap-ffff0000000000002",
		8,
		"Synthetic snapshot 2",
		"vol-hijklm",
		[]ebsTypes.Tag{
			{Key: aws.String("FakeSnapType"), Value: aws.String("synthetic")},
			{Key: aws.String("FakeSnapId"), Value: aws.String("snap-ffff0000000000002")},
		})
	c.snapshots["snap-ffff0000000000002"].getSnapshotBlockIntercept = func(index int32, output *ebs.GetSnapshotBlockOutput) (*ebs.GetSnapshotBlockOutput, error) {
		return output, errors.New("fake error")
	}
	c.snapshots["snap-ffff0000000000003"] = newFakeSyntheticEBSSnapshot(
		"snap-ffff0000000000003",
		8,
		"Synthetic snapshot 3",
		"vol-hijklm",
		[]ebsTypes.Tag{
			{Key: aws.String("FakeSnapType"), Value: aws.String("synthetic")},
			{Key: aws.String("FakeSnapId"), Value: aws.String("snap-ffff0000000000003")},
		})
	c.snapshots["snap-ffff0000000000003"].getSnapshotBlockIntercept = func(index int32, output *ebs.GetSnapshotBlockOutput) (*ebs.GetSnapshotBlockOutput, error) {
		c := "./&453"
		output.Checksum = &c
		return output, nil
	}
	return &c
}

func (c *fakeEBSClient) StartSnapshot(ctx context.Context, params *ebs.StartSnapshotInput, optFns ...func(*ebs.Options)) (*ebs.StartSnapshotOutput, error) {
	s := newFakeEBSSnapsot(*params.VolumeSize, *params.Description, "vol-ffff", params.Tags)
	out := ebs.StartSnapshotOutput{
		BlockSize:   &s.blockSize,
		SnapshotId:  &s.snapshotId,
		Status:      s.status,
		VolumeSize:  &s.volumeSize,
		Description: &s.description,
		StartTime:   &s.startTime,
		Tags:        params.Tags,
	}
	c.snapshotsMx.Lock()
	c.snapshots[s.snapshotId] = s
	c.snapshotsMx.Unlock()
	return &out, nil
}

func (c *fakeEBSClient) PutSnapshotBlock(ctx context.Context, params *ebs.PutSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.PutSnapshotBlockOutput, error) {

	apiOperation := "PutSnapshotBlock"

	// retrieve the snapshot struct
	c.snapshotsMx.RLock()
	s, ok := c.snapshots[*params.SnapshotId]
	if !ok {
		c.snapshotsMx.RUnlock()
		return nil, ebsValidationExceptionInvalidSnapshotIdError(apiOperation, *params.SnapshotId)
	}
	c.snapshotsMx.RUnlock()

	// verify SHA256 checksum is provided
	if params.ChecksumAlgorithm != ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256 {
		return nil, fmt.Errorf("invalid checksum algorithm %v", params.ChecksumAlgorithm)
	}

	if *params.BlockIndex >= s.blockCount || *params.BlockIndex < 0 {
		return nil, ebsValidationExceptionError(apiOperation,
			aws.String(fmt.Sprintf("Block index %v is beyond the volume size extent", *params.BlockIndex)),
			ebsTypes.ValidationExceptionReasonInvalidBlock)
	}

	if *params.DataLength != s.blockSize {
		return nil, fmt.Errorf("invalid data length %v, valid %v ", params.DataLength, s.blockSize)
	}

	if *params.SnapshotId != s.snapshotId {
		return nil, fmt.Errorf("invalid data length %v, valid %v ", params.DataLength, s.blockSize)
	}

	b := bytes.Buffer{}
	// wr, err := io.Copy(&b, params.BlockData)
	b.ReadFrom(params.BlockData)
	computedChecksum := sha256.Sum256(b.Bytes())
	computedChecksumB64 := base64.StdEncoding.EncodeToString(computedChecksum[:])
	if *params.Checksum != computedChecksumB64 {
		ae := smithy.GenericAPIError{
			Code: "InvalidSignatureException",
			Message: fmt.Sprintf("The value passed in as x-amz-Checksum does not match the computed checksum. Computed checksum: %s expected checksum: %s",
				computedChecksumB64, *params.Checksum),
		}
		oe := smithy.OperationError{ServiceID: "EBS", OperationName: apiOperation, Err: &ae}
		return nil, &oe
	}

	// Add the checksum to the map of checksums that will be used by CompleteSnapshot to calculate the
	// full snapshot checksum
	s.checksumsMx.Lock()
	_, ok = s.blockChecksums[*params.BlockIndex]
	if ok {
		s.checksumsMx.Unlock()
		return nil, fmt.Errorf("block %v was already put", params.BlockIndex)
	}
	s.blockChecksums[*params.BlockIndex] = computedChecksum[:]
	s.checksumsMx.Unlock()

	out := ebs.PutSnapshotBlockOutput{
		Checksum:          aws.String(*params.Checksum),
		ChecksumAlgorithm: params.ChecksumAlgorithm,
	}

	time.Sleep(time.Duration(2+rand.Intn(8)) * time.Millisecond)
	return &out, nil
}

func (c *fakeEBSClient) CompleteSnapshot(ctx context.Context, params *ebs.CompleteSnapshotInput, optFns ...func(*ebs.Options)) (*ebs.CompleteSnapshotOutput, error) {

	apiOperation := "CompleteSnapshot"

	// retrieve the snapshot struct
	c.snapshotsMx.RLock()
	defer c.snapshotsMx.RUnlock()

	s, ok := c.snapshots[*params.SnapshotId]
	if !ok {
		return nil, ebsValidationExceptionInvalidSnapshotIdError(apiOperation, *params.SnapshotId)
	}

	// verify changed block counts
	if len(s.blockChecksums) != int(*params.ChangedBlocksCount) {
		s.completeBlockCountMismatch = true
	}

	// verify checksum aggregation method
	if params.ChecksumAggregationMethod != ebsTypes.ChecksumAggregationMethodChecksumAggregationLinear {
		return nil, ebsValidationExceptionError(apiOperation, nil, "")
	}

	// verify checksum algorithm
	if params.ChecksumAlgorithm != ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256 {
		return nil, ebsValidationExceptionError(apiOperation, nil, "")
	}

	// verify checksum
	paramChecksum, err := base64.StdEncoding.DecodeString(*params.Checksum)
	if err != nil {
		return nil, ebsValidationExceptionError(apiOperation, nil, "")
	}

	s.checksumsMx.RLock()
	// calculate the block list from the hashes map and sort it
	blockList := make(BlockList, 0, len(s.blockChecksums))
	for k := range s.blockChecksums {
		blockList = append(blockList, k)
	}
	blockList.Sort()

	// travel the sorted block list and get each checksum, add each one to the complete checksum
	completeChecksum := sha256.New()
	for _, index := range blockList {
		completeChecksum.Write(s.blockChecksums[index])
	}
	calculatedChecksum := completeChecksum.Sum(nil)
	s.checksumsMx.RUnlock()

	if !bytes.Equal(paramChecksum, calculatedChecksum) {
		s.completeChecksumError = true
	}

	time.Sleep(time.Duration(5+rand.Intn(10)) * time.Millisecond)

	s.completeTime = time.Now()
	s.status = ebsTypes.StatusPending

	out := ebs.CompleteSnapshotOutput{
		Status: ebsTypes.StatusPending,
	}

	return &out, nil
}

func (c *fakeEBSClient) ListSnapshotBlocks(ctx context.Context, params *ebs.ListSnapshotBlocksInput,
	optFns ...func(*ebs.Options)) (*ebs.ListSnapshotBlocksOutput, error) {

	apiOperation := "ListSnapshotBlocks"

	generateList := func(s *fakeEBSSnapshot, start int32, maxResults int32, expires time.Time) ([]ebsTypes.Block, bool) {

		bLen := int32(len(s.blockList))
		end := start + maxResults
		if end > bLen { // Trim the end if it exceeds the end limits
			end = bLen
		}

		output := make([]ebsTypes.Block, 0)
		for _, blockIndex := range s.blockList[start:end] {
			blockToken := createToken(blockIndex, s.snapshotId, expires)
			output = append(output, ebsTypes.Block{BlockIndex: aws.Int32(blockIndex), BlockToken: &blockToken})
		}

		containsLastBlock := end == bLen
		return output, containsLastBlock
	}

	// Validate snapshot Id syntax
	match, _ := regexp.MatchString("^snap-[0-9a-f]+$", *params.SnapshotId)
	if !match {
		return nil, ebsValidationExceptionError(apiOperation, nil, ebsTypes.ValidationExceptionReason(""))
	}

	// Validate block index
	if params.StartingBlockIndex != nil && *params.StartingBlockIndex < 0 {
		return nil, ebsValidationExceptionError(apiOperation, nil, ebsTypes.ValidationExceptionReason(""))
	}

	// Validation tests of MaxResults go first according to the empirical test of the real AWS API
	if params.MaxResults != nil && (*params.MaxResults < 100 || *params.MaxResults > 10_000) {
		return nil, ebsValidationExceptionError(apiOperation, nil, "")
	}

	// retrieve the snapshot struct
	c.snapshotsMx.RLock()
	s, ok := c.snapshots[*params.SnapshotId]
	if !ok {
		c.snapshotsMx.RUnlock()
		return nil, ebsResourceNotFoundExceptionSnapshotNotFoundError(apiOperation, *params.SnapshotId)
	}
	c.snapshotsMx.RUnlock()

	if !s.synthetic {
		return nil, fmt.Errorf("not a synthetic snapshot")
	}

	var startIndex int32
	var maxResults int32
	if params.NextToken != nil {
		i, err := validateToken(*params.NextToken, s.snapshotId)
		if err != nil {
			return nil, ebsValidationExceptionInvalidPageTokenError(apiOperation, *params.NextToken)
		}
		startIndex = i
	}
	if params.StartingBlockIndex != nil {
		startIndex = *params.StartingBlockIndex
	}
	if params.MaxResults == nil {
		maxResults = 10000
	} else {
		maxResults = *params.MaxResults
	}

	expiryTime := time.Now().Add(60 * time.Minute)
	blocks, containsLastBlock := generateList(s, startIndex, maxResults, expiryTime)

	listOutput := ebs.ListSnapshotBlocksOutput{
		BlockSize:  aws.Int32(s.blockSize),
		Blocks:     blocks,
		ExpiryTime: aws.Time(time.Now().Add(1 * time.Hour)),
		VolumeSize: aws.Int64(s.volumeSize),
	}
	if !containsLastBlock {
		listOutput.NextToken = aws.String(createToken(startIndex+int32(len(blocks)), s.snapshotId, expiryTime))
	}
	return &listOutput, nil
}

func (c *fakeEBSClient) GetSnapshotBlock(ctx context.Context, params *ebs.GetSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error) {

	apiOperation := "GetSnapshotBlock"

	// retrieve the snapshot struct
	c.snapshotsMx.RLock()
	s, ok := c.snapshots[*params.SnapshotId]
	if !ok {
		c.snapshotsMx.RUnlock()
		return nil, ebsResourceNotFoundExceptionSnapshotNotFoundError(apiOperation, *params.SnapshotId)
	}
	c.snapshotsMx.RUnlock()

	if !s.synthetic {
		return nil, fmt.Errorf("not a synthetic snapshot")
	}

	blockIndex, err := validateToken(*params.BlockToken, *params.SnapshotId)
	if err != nil || blockIndex != *params.BlockIndex {
		return nil, ebsValidationExceptionInvalidBlockTokenError(apiOperation, *params.BlockToken)
	}

	if !s.blockList.Contains(blockIndex) {
		return nil, fmt.Errorf("BLOCK NOT IN THE SNAPSHOT")
	}

	bc := createFakeBlockContent(blockIndex)
	bChecksum := base64.StdEncoding.EncodeToString(bc.Checksum)

	output := ebs.GetSnapshotBlockOutput{
		BlockData:         io.NopCloser(bytes.NewReader(bc.Data)),
		Checksum:          &bChecksum,
		ChecksumAlgorithm: bc.ChecksumAlgorithm,
		DataLength:        &bc.DataLength,
	}

	time.Sleep(time.Duration(4+rand.Intn(6)) * time.Millisecond)

	// This is a tap point to modify the output (to introduce mocked errors)
	if s.getSnapshotBlockIntercept != nil {
		return s.getSnapshotBlockIntercept(blockIndex, &output)
	}
	return &output, nil
}

func (c *fakeEBSClient) DescribeSnapshots(ctx context.Context, params *ec2.DescribeSnapshotsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeSnapshotsOutput, error) {
	if params == nil {
		params = &ec2.DescribeSnapshotsInput{}
	}

	if params.SnapshotIds == nil || len(params.SnapshotIds) < 1 {
		return nil, fmt.Errorf("this fake method requires at least one snapshot id")
	}

	outSnapshots := make([]ec2Types.Snapshot, 0)
	for _, sId := range params.SnapshotIds {
		s, err := c.describeSnapshot(sId)
		if err != nil {
			return nil, err
		}
		outSnapshots = append(outSnapshots, *s)
	}

	out := ec2.DescribeSnapshotsOutput{
		Snapshots: outSnapshots,
	}

	time.Sleep(time.Duration(2+rand.Intn(3)) * time.Millisecond)
	return &out, nil
}

func (c *fakeEBSClient) describeSnapshot(snapshotId string) (*ec2Types.Snapshot, error) {

	// retrieve the snapshot struct
	c.snapshotsMx.RLock()
	s, ok := c.snapshots[snapshotId]
	if !ok {
		c.snapshotsMx.RUnlock()
		return nil, fmt.Errorf("invalid snapshot id %v", snapshotId)
	}
	c.snapshotsMx.RUnlock()

	// competed non-synthetic snapshots: after 10 seconds of completed they move from pending to
	// completed or error
	// synthetic snapshots are completed by definition
	if !s.synthetic && s.status == ebsTypes.StatusPending && !s.completeTime.IsZero() {
		if s.completeTime.Add(10 * time.Second).Before(time.Now()) {
			if s.completeChecksumError || s.completeBlockCountMismatch {
				s.status = ebsTypes.StatusError
			} else {
				s.status = ebsTypes.StatusCompleted
			}
		}
	}

	ec2Snapshot := ec2Types.Snapshot{
		DataEncryptionKeyId: new(string),
		Description:         aws.String(s.description),
		Encrypted:           aws.Bool(false),
		SnapshotId:          aws.String(s.snapshotId),
		VolumeId:            aws.String(s.volumeId),
		StartTime:           aws.Time(s.startTime),
		VolumeSize:          aws.Int32(int32(s.volumeSize)),
		State:               ebsToEc2SnapshotState(s.status),
		Tags:                s.ec2Tags(),
	}
	return &ec2Snapshot, nil
}

func ebsToEc2SnapshotState(state ebsTypes.Status) ec2Types.SnapshotState {
	switch state {
	case ebsTypes.StatusCompleted:
		return ec2Types.SnapshotStateCompleted
	case ebsTypes.StatusPending:
		return ec2Types.SnapshotStatePending
	case ebsTypes.StatusError:
		return ec2Types.SnapshotStateError
	}
	return ""
}

func createToken(index int32, snapshotId string, expires time.Time) string {

	key := []byte{1, 2, 3}

	// Token: 4-byte big endian int32 block index
	//        8-byte expiration timestamp in nilliseconds since Unix epoch (int64)
	//        n-byte snapshot id (string utf-8 encoded)
	//        32-byte sha256hmac
	// The token is encoded in base64
	buff := bytes.Buffer{}
	binary.Write(&buff, binary.BigEndian, index)
	binary.Write(&buff, binary.BigEndian, expires.UnixMilli())
	buff.Write([]byte(snapshotId))

	mac := hmac.New(sha256.New, key)
	mac.Write(buff.Bytes())
	buff.Write(mac.Sum(nil))

	return base64.StdEncoding.EncodeToString(buff.Bytes())
}

func validateToken(token string, snapshotId string) (int32, error) {

	key := []byte{1, 2, 3}

	t, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("invalid token, failed to decode string")
	}
	if len(t) < 4+1+32 {
		return 0, fmt.Errorf("invalid token, too short")
	}

	readHmac := t[len(t)-32:]
	readIndex := int32(binary.BigEndian.Uint32(t[:4]))
	readExpMillis := int64(binary.BigEndian.Uint64(t[4:12]))
	readSnapshotId := string(t[12 : len(t)-32])

	// validate hmac
	mac := hmac.New(sha256.New, key)
	mac.Write(t[:len(t)-32])
	expectedMAC := mac.Sum(nil)
	if !hmac.Equal(readHmac, expectedMAC) {
		return 0, fmt.Errorf("invalid token, failed to match hmac signature")
	}

	if snapshotId != readSnapshotId {
		return 0, fmt.Errorf("invalid token, snapshot id mismatch")
	}

	if time.Now().UnixMilli() > readExpMillis {
		return 0, fmt.Errorf("invalid token, expired")
	}

	return readIndex, nil
}

func createFakeBlockContent(blockIndex int32) *BlockContent {

	dataLength := int32(512 * 1024)

	data := make([]byte, dataLength)
	for i := range data {
		data[i] = byte(blockIndex % 256)
	}

	checksum := sha256.Sum256(data)

	bc := BlockContent{
		BlockIndex:        blockIndex,
		Checksum:          checksum[:],
		Data:              data,
		DataLength:        dataLength,
		ChecksumAlgorithm: ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256,
	}

	return &bc
}

func createFakeZeroBlockContent(blockIndex int32) *BlockContent {

	dataLength := int32(512 * 1024)

	data := make([]byte, dataLength)

	checksum := sha256.Sum256(data)

	bc := BlockContent{
		BlockIndex:        blockIndex,
		Checksum:          checksum[:],
		Data:              data,
		DataLength:        dataLength,
		ChecksumAlgorithm: ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256,
	}

	return &bc
}

func ebsValidationExceptionError(operation string, message *string, reason ebsTypes.ValidationExceptionReason) error {
	ve := ebsTypes.ValidationException{Message: message, Reason: reason}
	oe := smithy.OperationError{ServiceID: "EBS", OperationName: operation, Err: &ve}
	return &oe
}

func ebsValidationExceptionInvalidSnapshotIdError(operation string, snapshotId string) error {
	return ebsValidationExceptionError(operation,
		aws.String(fmt.Sprintf("Value (%s) for parameter snapshotId is invalid. Expected: 'snap-...'.", snapshotId)),
		ebsTypes.ValidationExceptionReasonInvalidSnapshotId)
}

func ebsValidationExceptionInvalidBlockTokenError(operation string, token string) error {
	return ebsValidationExceptionError(operation,
		aws.String(fmt.Sprintf("The block token %s is invalid.", token)),
		ebsTypes.ValidationExceptionReasonInvalidBlockToken)
}

func ebsValidationExceptionInvalidPageTokenError(operation string, token string) error {
	return ebsValidationExceptionError(operation,
		aws.String(fmt.Sprintf("The pagination token %s is invalid.", token)),
		ebsTypes.ValidationExceptionReasonInvalidPageToken)
}

func ebsResourceNotFoundError(operation string, message *string, reason ebsTypes.ResourceNotFoundExceptionReason) error {
	ve := ebsTypes.ResourceNotFoundException{Message: message, Reason: reason}
	oe := smithy.OperationError{ServiceID: "EBS", OperationName: operation, Err: &ve}
	return &oe
}

func ebsResourceNotFoundExceptionSnapshotNotFoundError(operation string, snapshotId string) error {
	return ebsResourceNotFoundError(operation,
		aws.String(fmt.Sprintf("The snapshot '%v' does not exist.", snapshotId)),
		ebsTypes.ResourceNotFoundExceptionReasonSnapshotNotFound)
}
