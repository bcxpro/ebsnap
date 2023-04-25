package snapshot

import (
	"context"
	"ebsnapshot/gconc"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	ebsTypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// An EBSRetriever is used to retrieve the contents of an EBS snapshot from AWS using the EBS direct API.
type EBSRetriever struct {
	sInfo Info

	initialized bool
	errorState  bool
	eof         bool

	ebsClient EBSSnapshotReadAPI
	ec2Client EC2SnapshotAPI

	blockGenerator *gconc.Generator[*BlockContent]

	listSnapshotBlocksGen *gconc.Generator[*ebsTypes.Block]
	cancelListSnapshotBlocksGen context.CancelFunc
}

type EBSSnapshotRetrieverOptions struct {
	AwsConfig *aws.Config
	EbsClient EBSSnapshotAPI
	Ec2Client EC2SnapshotAPI
}

func WithRetrieverEBSClient(v EBSSnapshotAPI) func(*EBSSnapshotRetrieverOptions) error {
	return func(o *EBSSnapshotRetrieverOptions) error {
		o.EbsClient = v
		return nil
	}
}

func WithRetrieverEC2Client(v EC2SnapshotAPI) func(*EBSSnapshotRetrieverOptions) error {
	return func(o *EBSSnapshotRetrieverOptions) error {
		o.Ec2Client = v
		return nil
	}
}

// NewEBSSnapshotRetriever return an EBSSnapshotRetriever for the specified EBS snapshot id.
//
// It does not perform any active action. To start getting information about the snapshot
// it must be initialized using the Initialize method.
func NewEBSSnapshotRetriever(snapshotId string, optFns ...func(*EBSSnapshotRetrieverOptions) error) *EBSRetriever {
	var options EBSSnapshotRetrieverOptions
	for _, optFn := range optFns {
		optFn(&options)
	}

	rt := EBSRetriever{
		sInfo:       Info{SnapshotId: snapshotId, Tags: make(map[string]string)},
		initialized: false,
	}

	var awsConfig aws.Config
	if rt.ebsClient == nil || rt.ec2Client == nil {
		if options.AwsConfig != nil {
			awsConfig = *options.AwsConfig
		} else {
			awsConfig, _ = config.LoadDefaultConfig(context.Background())
		}
	}

	if options.EbsClient != nil {
		rt.ebsClient = options.EbsClient
	} else {
		rt.ebsClient = ebs.NewFromConfig(awsConfig)
	}

	if options.Ec2Client != nil {
		rt.ec2Client = options.Ec2Client
	} else {
		rt.ec2Client = ec2.NewFromConfig(awsConfig)
	}

	return &rt
}

// Initializes the EBSSnapshotRetriever, reading the EBS snapshot metadata and block list form AWS
func (rt *EBSRetriever) Initialize(ctx context.Context) error {
	if rt.errorState {
		return ErrErrorStatus
	}
	if rt.initialized {
		return ErrAlreadyInitialized
	}

	err := fillInfoFromSnapshot(ctx, &rt.sInfo, rt.ec2Client)
	if err != nil {
		return fmt.Errorf("failed to retrieve snapshot info: %w", err)
	}

	resp, err := rt.ebsClient.ListSnapshotBlocks(ctx, &ebs.ListSnapshotBlocksInput{SnapshotId: aws.String(rt.sInfo.SnapshotId), MaxResults: aws.Int32(100)})
	if err != nil {
		return fmt.Errorf("failed to retrieve snapshot info: %w", err)
	}

	rt.sInfo.BlockSize = *resp.BlockSize
	rt.sInfo.VolumeSize = *resp.VolumeSize


	listBlocksCtx, listBlocksGen := context.WithCancel(ctx)
	rt.cancelListSnapshotBlocksGen = listBlocksGen

	rt.listSnapshotBlocksGen = gconc.NewGenerator(listBlocksCtx, func(ctx context.Context, out chan<- *ebsTypes.Block) error {
		paginator := ebs.NewListSnapshotBlocksPaginator(rt.ebsClient,
			&ebs.ListSnapshotBlocksInput{SnapshotId: aws.String(rt.sInfo.SnapshotId)},
			func(opts *ebs.ListSnapshotBlocksPaginatorOptions) {},
		)

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return err
			}
			for _, block := range page.Blocks {
				v := block
				select {
				case out <- &v:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return nil
	})

	rt.initialized = true
	return nil
}

func (rt *EBSRetriever) SnapshotInfo(ctx context.Context) (*Info, error) {
	if rt.errorState {
		return nil, ErrErrorStatus
	}
	if !rt.initialized {
		return nil, ErrNotInitialized
	}
	out := rt.sInfo
	return &out, nil
}

func (rt *EBSRetriever) NextBlock(ctx context.Context) (*BlockContent, error) {
	if rt.errorState {
		return nil, ErrErrorStatus
	}
	if !rt.initialized {
		return nil, ErrNotInitialized
	}
	if rt.eof {
		return nil, io.EOF
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context done: %w", ctx.Err())
	default:
	}

	if rt.blockGenerator == nil {
		rt.blockGenerator = gconc.NewGenerator(context.TODO(), rt.getBlockGeneratorFunction())
	}

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context done: %w", ctx.Err())
	// case <- rt.blockGenerator.Done():
	// 	return nil, fmt.Errorf("could not retrieve next block: %w",  rt.blockGenerator.Err())
	case block, ok := <-rt.blockGenerator.Out():
		if !ok {
			<-rt.blockGenerator.Done()
			err := rt.blockGenerator.Err()
			if err != nil {
				rt.errorState = true
				return nil, fmt.Errorf("could not retrieve next block: %w", err)
			}
			rt.eof = true
			return nil, io.EOF
		}
		return block, nil
	}
}

func getBlockRetrieverTransform(snapshotId string, ebsClient EBSSnapshotReadAPI) func(ctx context.Context, block *ebsTypes.Block) (*BlockContent, error) {

	t := func(ctx context.Context, block *ebsTypes.Block) (*BlockContent, error) {
		retrievedBlock, err := ebsClient.GetSnapshotBlock(ctx, &ebs.GetSnapshotBlockInput{BlockIndex: block.BlockIndex, BlockToken: block.BlockToken, SnapshotId: &snapshotId})
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve snapshot block with index %v: %w", *block.BlockIndex, err)
		}

		data, err := ioutil.ReadAll(retrievedBlock.BlockData)
		if err != nil {
			return nil, fmt.Errorf("failed to read data from block with index %v: %w", block.BlockIndex, err)
		}

		checksum, err := base64.StdEncoding.DecodeString(*retrievedBlock.Checksum)
		if err != nil {
			return nil, fmt.Errorf("failed to decode checksum for block with index %v: %w", block.BlockIndex, err)
		}

		return &BlockContent{
			BlockIndex:        *block.BlockIndex,
			Checksum:          checksum,
			Data:              data,
			DataLength:        *retrievedBlock.DataLength,
			ChecksumAlgorithm: retrievedBlock.ChecksumAlgorithm,
		}, nil
	}

	return t
}

func (rt *EBSRetriever) getBlockGeneratorFunction() func(ctx context.Context, out chan<- *BlockContent) error {

	f := func(ctx context.Context, out chan<- *BlockContent) error {

		if !rt.initialized {
			return ErrNotInitialized
		}

		feederCtx, cancelFeeder := context.WithCancel(ctx)

		retrieveBlock := getBlockRetrieverTransform(rt.sInfo.SnapshotId, rt.ebsClient)
		parallel := gconc.NewParallelOrdered(feederCtx, retrieveBlock, 25, 4, 4)

		// feeder
		feederWg := sync.WaitGroup{}
		feederWg.Add(1)
		go func() {
			defer feederWg.Done()
			defer close(parallel.In())
		blockLoop:
			for block := range rt.listSnapshotBlocksGen.Out() {
				select {
				case parallel.In() <- block:
				case <-parallel.Done():
					break blockLoop
				case <-feederCtx.Done():
					break blockLoop
				}
			}
			<-rt.listSnapshotBlocksGen.Done()
		}()

		// collector
		collectorWg := sync.WaitGroup{}
		collectorWg.Add(1)
		go func() {
			defer collectorWg.Done()
			defer cancelFeeder()

		myLoop:
			for {
				select {
				case bc, ok := <-parallel.Out():
					if !ok {
						break myLoop
					}
					select {
					case out <- bc:
					case <-ctx.Done():
						cancelFeeder()
						break myLoop
					}
				case <-feederCtx.Done():
					break myLoop
				case <-ctx.Done():
					break myLoop
				}
			}
			<-parallel.Done()
		}()

		collectorWg.Wait()

		// Check if the collector failed
		parallelErr := parallel.Err()
		if parallelErr != nil {
			rt.cancelListSnapshotBlocksGen()
			return parallelErr
		}

		feederWg.Wait()
		blockListErr := rt.listSnapshotBlocksGen.Err()
		if blockListErr != nil {
			return fmt.Errorf("failed to retrieve snapshot blocks from the EBS API: %w", blockListErr)
		}

		return nil
	}

	return f
}

func fillInfoFromSnapshot(ctx context.Context, sInfo *Info, c EC2SnapshotAPI) error {
	snapshotIds := []string{sInfo.SnapshotId}
	output, err := c.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
		SnapshotIds: snapshotIds,
	})
	if err != nil {
		return fmt.Errorf("describe snapshots call failed for snapshot id %v: %w", sInfo.SnapshotId, err)
	}
	snapshotsIncluded := len(output.Snapshots)
	if snapshotsIncluded != 1 {
		return fmt.Errorf("expected snapshot results for one snapshot, received %v", snapshotsIncluded)
	}

	s := &output.Snapshots[0]

	if s.State != ec2types.SnapshotStateCompleted {
		return fmt.Errorf("the snapshot is not in complete state: %v", s.State)
	}

	sInfo.Description = *s.Description
	sInfo.VolumeId = *s.VolumeId

	// copy the tags
	for _, t := range s.Tags {
		sInfo.Tags[*t.Key] = *t.Value
	}
	return nil
}
