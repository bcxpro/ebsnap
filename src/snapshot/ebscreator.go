package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"ebsnapshot/gconc"
	"encoding/base64"
	"fmt"
	"regexp"
	"time"

	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	ebstypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type EBSCreator struct {
	sInfo         Info
	blockList     BlockList
	newSnapshotId string
	newTags       map[string]string

	initialized bool
	closed      bool
	errorState  bool

	ebsClient EBSSnapshotWriteAPI
	ec2Client EC2SnapshotAPI

	succesfullyWrittenBlocks int32
	skippedBlockCount        int32
	blocksWrittenChecksums   map[int32][]byte

	uploader    *gconc.ParallelOrdered[*BlockContent, uploadBlockWorkerResult]
	collectorWg sync.WaitGroup

	opMutex    sync.Mutex
	closeMutex sync.RWMutex

	zeroedBlock []byte
}

func NewEBSCreator(optFns ...func(*EBSCreatorOptions) error) *EBSCreator {

	var options EBSCreatorOptions
	for _, optFn := range optFns {
		optFn(&options)
	}

	cr := EBSCreator{initialized: false,
		blocksWrittenChecksums: make(map[int32][]byte),
	}
	var awsConfig aws.Config
	if cr.ebsClient == nil || cr.ec2Client == nil {
		if options.AwsConfig != nil {
			awsConfig = *options.AwsConfig
		} else {
			awsConfig, _ = config.LoadDefaultConfig(context.Background())
		}
	}

	if options.EbsClient != nil {
		cr.ebsClient = options.EbsClient
	} else {
		cr.ebsClient = ebs.NewFromConfig(awsConfig)
	}

	if options.Ec2Client != nil {
		cr.ec2Client = options.Ec2Client
	} else {
		cr.ec2Client = ec2.NewFromConfig(awsConfig)
	}

	return &cr
}

type EBSCreatorOptions struct {
	AwsConfig        *aws.Config
	EbsClient        EBSSnapshotAPI
	Ec2Client        EC2SnapshotAPI
	OrderedRetrieval bool
}

func WithEBSCreatorEBSClient(v EBSSnapshotAPI) func(*EBSCreatorOptions) error {
	return func(o *EBSCreatorOptions) error {
		o.EbsClient = v
		return nil
	}
}

func WithEBSCreatorEC2Client(v EC2SnapshotAPI) func(*EBSCreatorOptions) error {
	return func(o *EBSCreatorOptions) error {
		o.Ec2Client = v
		return nil
	}
}

func (cr *EBSCreator) Initialize(ctx context.Context, sInfo Info) error {
	if cr.errorState {
		return ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if cr.initialized {
		return ErrAlreadyInitialized
	}

	cr.sInfo = sInfo
	cr.blockList = make([]int32, 0)

	// Create the snapshot
	ssi := ebs.StartSnapshotInput{
		VolumeSize: &cr.sInfo.VolumeSize,
		Tags:       make([]ebstypes.Tag, 0, len(cr.sInfo.Tags)),
	}
	if cr.sInfo.Description != "" {
		ssi.Description = &cr.sInfo.Description
	}

	// Remove some tags
	re := regexp.MustCompile("^(aws:|dlm:|type$)")
	for k, v := range cr.sInfo.Tags {
		if !re.MatchString(k) {
			t := ebstypes.Tag{Key: aws.String(k), Value: aws.String(v)}
			ssi.Tags = append(ssi.Tags, t)
		}
	}
	startResult, err := cr.ebsClient.StartSnapshot(ctx, &ssi)
	if err != nil {
		cr.errorState = true
		return fmt.Errorf("failed to start new snapshot: %w", err)
	}
	if cr.sInfo.BlockSize != *startResult.BlockSize {
		cr.errorState = true
		return fmt.Errorf("snapshot blocksize mismatch, source block size %v, destination block size %v",
			cr.sInfo.BlockSize, startResult.BlockSize)
	}
	cr.newSnapshotId = *startResult.SnapshotId
	cr.newTags = map[string]string{}
	for _, v := range startResult.Tags {
		cr.newTags[*v.Key] = *v.Value
	}

	cr.zeroedBlock = make([]byte, cr.sInfo.BlockSize)

	// Launch parallel
	cr.uploader = gconc.NewParallelOrdered(context.TODO(), getUploadBlockTransform(cr), numUploadBlocksWorkers, 2, 1)

	// Launch collector
	cr.collectorWg.Add(1)
	go cr.collector()

	cr.initialized = true
	return nil
}

func (cr *EBSCreator) PutBlock(ctx context.Context, b *BlockContent) error {
	if cr.errorState {
		return ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if !cr.initialized {
		return ErrNotInitialized
	}
	cr.closeMutex.RLock()
	defer cr.closeMutex.RUnlock()
	if cr.closed {
		return ErrErrorClosed
	}

	if b.BlockIndex >= int32(blockCount(cr.sInfo.VolumeSize, cr.sInfo.BlockSize)) || b.BlockIndex < 0 {
		return fmt.Errorf("block index out of range")
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
	case cr.uploader.In() <- b:
		cr.blockList = append(cr.blockList, b.BlockIndex)
		return nil
	}
}

func (cr *EBSCreator) closeInput() {
	cr.closeMutex.Lock()
	defer cr.closeMutex.Unlock()
	if !cr.closed {
		close(cr.uploader.In())
		cr.closed = true
	}
}

func (cr *EBSCreator) Complete(ctx context.Context) (*Info, error) {
	if cr.errorState {
		return nil, ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if cr.closed {
		return nil, ErrErrorClosed
	}
	if !cr.initialized {
		return nil, ErrNotInitialized
	}
	cr.closeInput()

	select {
	case <-cr.uploader.Done():
	case <-ctx.Done():
		return nil, fmt.Errorf("context done: %w", ctx.Err())
	}
	err := cr.uploader.Err()
	if err != nil {
		cr.errorState = true
		if e, ok := err.(*gconc.ProcessingError[*BlockContent]); ok {
			return nil, fmt.Errorf("cannot complete because block %v had an error: %w", e.WorkItem.BlockIndex, err)
		}
		return nil, fmt.Errorf("cannot complete because the snapshot upload phase had an unknown error: %w", err)
	}

	cr.collectorWg.Wait()

	fullChecksum, err := cr.calculateCompleteChecksum()
	if err != nil {
		cr.errorState = true
		return nil, fmt.Errorf("cannot calculate complete checksum: %w", err)
	}

	b64Checksum := base64.StdEncoding.EncodeToString(fullChecksum)
	_, err = cr.ebsClient.CompleteSnapshot(ctx, &ebs.CompleteSnapshotInput{
		ChangedBlocksCount:        aws.Int32(int32(len(cr.blocksWrittenChecksums))),
		SnapshotId:                &cr.newSnapshotId,
		Checksum:                  &b64Checksum,
		ChecksumAggregationMethod: ebstypes.ChecksumAggregationMethodChecksumAggregationLinear,
		ChecksumAlgorithm:         ebstypes.ChecksumAlgorithmChecksumAlgorithmSha256,
	})
	if err != nil {
		return nil, fmt.Errorf("could not complete snapshot: %w", err)
	}

	state, err := waitForCompletedSnapshot(ctx, cr.newSnapshotId, cr.ec2Client)
	if err != nil {
		return nil, fmt.Errorf("snapshot %v did not complete successfully: %w", cr.newSnapshotId, err)
	}

	if state != ec2types.SnapshotStateCompleted {
		return nil, fmt.Errorf("snapshot %v did not reach the completed state, state: %v", cr.newSnapshotId, state)
	}

	return &Info{
		SnapshotId:       cr.newSnapshotId,
		ParentSnapshotId: "",
		VolumeId:         "",
		VolumeSize:       cr.sInfo.VolumeSize,
		BlockSize:        cr.sInfo.BlockSize,
		Description:      cr.sInfo.Description,
		Tags:             cr.newTags,
	}, nil
}

func (cr *EBSCreator) uploadBlock(ctx context.Context, b *BlockContent) error {
	b64Checksum := base64.StdEncoding.EncodeToString(b.Checksum)
	dataLen := int32(len(b.Data))
	putResult, err := cr.ebsClient.PutSnapshotBlock(ctx, &ebs.PutSnapshotBlockInput{
		BlockData:         bytes.NewReader(b.Data),
		BlockIndex:        &b.BlockIndex,
		Checksum:          &b64Checksum,
		ChecksumAlgorithm: b.ChecksumAlgorithm,
		DataLength:        &dataLen,
		SnapshotId:        &cr.newSnapshotId,
	})
	if err != nil {
		return fmt.Errorf("failed to put block %v: %w", b.BlockIndex, err)
	}
	if *putResult.Checksum != b64Checksum {
		return fmt.Errorf("put block checksum mismatch for block %v", b.BlockIndex)
	}
	return nil
}

type uploadBlockWorkerResult struct {
	bl      *BlockContent
	skipped bool
}

func getUploadBlockTransform(cr *EBSCreator) gconc.Transform[*BlockContent, uploadBlockWorkerResult] {
	return func(ctx context.Context, b *BlockContent) (uploadBlockWorkerResult, error) {
		// Zero block detection, skip uploading them
		if !bytes.Equal(cr.zeroedBlock, b.Data) {
			err := cr.uploadBlock(context.TODO(), b)
			return uploadBlockWorkerResult{b, false}, err
		} else {
			return uploadBlockWorkerResult{b, true}, nil
		}
	}
}

const numUploadBlocksWorkers = 30

func (cr *EBSCreator) collector() {

	defer cr.collectorWg.Done()

collectorLoop:
	for {
		select {
		case r, ok := <-cr.uploader.Out():
			if !ok {
				break collectorLoop
			}
			cr.succesfullyWrittenBlocks++
			// empty blocks that were not uploaded to EBS are not taken into account in the checksum calculation
			if !r.skipped {
				cr.blocksWrittenChecksums[r.bl.BlockIndex] = r.bl.Checksum
			} else {
				cr.skippedBlockCount++
			}
		case <-cr.uploader.Done():
			break collectorLoop
		}
	}

	cr.closeInput()
}

func (cr *EBSCreator) calculateCompleteChecksum() ([]byte, error) {
	completeChecksum := sha256.New()
	if len(cr.blockList) != len(cr.blocksWrittenChecksums)+int(cr.skippedBlockCount) {
		return nil, fmt.Errorf("block list has %v blocks but blocks written (%v) plus skipped (%v) do not match", len(cr.blockList), len(cr.blocksWrittenChecksums), cr.skippedBlockCount)
	}
	for _, blockIndex := range cr.blockList {
		var checksum []byte
		var ok bool
		if checksum, ok = cr.blocksWrittenChecksums[int32(blockIndex)]; ok {
			completeChecksum.Write(checksum)
		}
	}
	return completeChecksum.Sum(nil), nil
}

func waitForCompletedSnapshot(ctx context.Context, snapshotId string, c EC2SnapshotAPI) (ec2types.SnapshotState, error) {

	waitTime := 10 * time.Second
	timer := time.NewTimer(waitTime)

	for {
		snapshotIds := []string{snapshotId}
		output, err := c.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
			SnapshotIds: snapshotIds,
		})
		if err != nil {
			return "", fmt.Errorf("describe snapshots call failed for snapshot id %v: %w", snapshotId, err)
		}
		snapshotsIncluded := len(output.Snapshots)
		if snapshotsIncluded != 1 {
			return "", fmt.Errorf("expected snapshot results for one snapshot, received %v", snapshotsIncluded)
		}
		state := output.Snapshots[0].State

		if state != ec2types.SnapshotStatePending {
			return state, nil
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(waitTime)
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("context done: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
