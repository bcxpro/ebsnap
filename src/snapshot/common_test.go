package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestCopyA(t *testing.T) {

	ebsClient, ec2Client := getClients(false) // fake clients

	// the retriever uses a fake snapshot
	retriever := NewEBSSnapshotRetriever("snap-ffff0000000000001", WithRetrieverEBSClient(ebsClient), WithRetrieverEC2Client(ec2Client))
	creator := NewBinaryCreator(io.Discard)

	err := Copy(context.TODO(), creator, retriever, nil)
	assert.NoError(t, err)
}

func TestCopyA_Cancelled(t *testing.T) {

	ebsClient, ec2Client := getClients(false) // fake clients

	// the retriever uses a fake snapshot
	retriever := NewEBSSnapshotRetriever("snap-ffff0000000000001", WithRetrieverEBSClient(ebsClient), WithRetrieverEC2Client(ec2Client))
	creator := NewBinaryCreator(io.Discard)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := Copy(ctx, creator, retriever, nil)
		if assert.Error(t, err) {
			assert.ErrorIs(t, err, context.Canceled)
		}
	}()

	cancel()
	wg.Wait()
}

func TestCopyB(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 4

	fakeRd := newFakeBinarySnapshotReader(blockSize, volumeSize)

	ebsClient, ec2Client := getClients(false) // fake clients

	retriever := NewBinaryRetriever(fakeRd)
	creator := NewEBSCreator(WithEBSCreatorEBSClient(ebsClient), WithEBSCreatorEC2Client(ec2Client))

	err := Copy(context.TODO(), creator, retriever, nil)
	assert.NoError(t, err)
	assert.True(t, true)

	err = fakeRd.Close()
	assert.NoError(t, err)
	assert.NoError(t, fakeRd.ErrorStatus)
}

func TestCopyC(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 4
	var maxBlockCount int = blockCount(volumeSize, blockSize)
	var expectedBlockIndex int32 = 0

	zeroes := make([]byte, blockSize)

	blockChecker := func(blockIndex int32, data []byte) error {
		defer func() {
			expectedBlockIndex++
		}()

		if blockIndex != expectedBlockIndex {
			return fmt.Errorf("block index %v, unexpected, expected %v", blockIndex, expectedBlockIndex)
		}
		if len(data) != int(blockSize) {
			return fmt.Errorf("block index %v invalid data length: %v, expected: %v", blockIndex, len(data), blockSize)
		}
		if blockIndex >= int32(maxBlockCount) || blockIndex < 0 {
			return fmt.Errorf("block index %v out of the allowed range 0-%v", blockIndex, maxBlockCount-1)
		}

		var compareWith []byte
		if blockIndex%10 == 0 {
			expectedBlockContent := createFakeBlockContent(blockIndex)
			compareWith = expectedBlockContent.Data
		} else {
			compareWith = zeroes
		}

		if !bytes.Equal(data, compareWith) {
			return fmt.Errorf("block index %v data mismatch", blockIndex)
		}
		return nil
	}

	sourceFile := newFakeBinarySnapshotReader(blockSize, volumeSize)
	destinationFile := newFakeRawSnapshotWriter(blockSize, volumeSize, blockChecker)

	retriever := NewBinaryRetriever(sourceFile)
	creator := NewRawCreator(destinationFile)

	err := Copy(context.TODO(), creator, retriever, nil)
	assert.NoError(t, err)
	err = destinationFile.Close()
	assert.NoError(t, err)
	assert.NoError(t, destinationFile.ErrorStatus)
	err = sourceFile.Close()
	assert.NoError(t, err)
	assert.NoError(t, sourceFile.ErrorStatus)
}

// Copy from raw retriever to raw creator
func TestCopyD(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	var maxBlockCount int = blockCount(volumeSize, blockSize)
	var expectedBlockIndex int32 = 0

	creatorBlockChecker := func(blockIndex int32, data []byte) error {
		defer func() {
			expectedBlockIndex++
		}()

		if blockIndex != expectedBlockIndex {
			return fmt.Errorf("block index %v, unexpected, expected %v", blockIndex, expectedBlockIndex)
		}
		if len(data) != int(blockSize) {
			return fmt.Errorf("block index %v invalid data length: %v, expected: %v", blockIndex, len(data), blockSize)
		}
		if blockIndex >= int32(maxBlockCount) || blockIndex < 0 {
			return fmt.Errorf("block index %v out of the allowed range 0-%v", blockIndex, maxBlockCount-1)
		}

		var compareWith []byte
		expectedBlockContent := createFakeBlockContent(blockIndex)
		compareWith = expectedBlockContent.Data

		if !bytes.Equal(data, compareWith) {
			return fmt.Errorf("block index %v data mismatch", blockIndex)
		}
		return nil
	}

	sourceFile := newFakeRawSnapshotReader(blockSize, volumeSize, createFakeBlockContent)
	destinationFile := newFakeRawSnapshotWriter(blockSize, volumeSize, creatorBlockChecker)

	retriever := NewRawRetriever(sourceFile, Info{VolumeSize: volumeSize, BlockSize: blockSize})
	creator := NewRawCreator(destinationFile)

	err := Copy(context.TODO(), creator, retriever, nil)
	assert.NoError(t, err)
	err = destinationFile.Close()
	assert.NoError(t, err)
	assert.NoError(t, destinationFile.ErrorStatus)
	err = sourceFile.Close()
	assert.NoError(t, err)
	assert.NoError(t, sourceFile.ErrorStatus)
}

type creatorTestSuite struct {
	suite.Suite
	prepare       func(t *testing.T, fakeBlockCreator func(blockIndex int32) *BlockContent) (Creator, Info, BlockList, func(blockIndex int32) *BlockContent)
	postFullCycle func(t *testing.T, completeInfo Info, completeErr error)
}

func (suite *creatorTestSuite) TestCreator_PutBlockNotInitialized() {

	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		// Put blocks
		err := cr.PutBlock(context.TODO(), blockContent(0))
		if assert.Error(t, err) {
			assert.ErrorIs(t, err, ErrNotInitialized)
		}
	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
}

func (suite *creatorTestSuite) TestCreator_CompleteNotInitialized() {

	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		// Put blocks
		_, err := cr.Complete(context.TODO())
		if assert.Error(t, err) {
			assert.ErrorIs(t, err, ErrNotInitialized)
		}
	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
}

func (suite *creatorTestSuite) TestCreator_PutBlockFailedIntegrity() {

	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := cr.Initialize(context.TODO(), sInfo)
		require.NoError(t, err)

		// Put just a block
		bc := blockContent(blockList[0])
		bc.Data[0]++ // corrupts the data
		err = cr.PutBlock(context.TODO(), bc)
		if assert.Error(t, err) {
			assert.EqualValues(t, "block content failed integrity verification", err.Error())
		}
	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
}

func (suite *creatorTestSuite) TestCreator_PutBlockOutOfRange() {

	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		maxBlockCount := int32(blockCount(sInfo.VolumeSize, sInfo.BlockSize))

		// Initialize
		err := cr.Initialize(context.TODO(), sInfo)
		require.NoError(t, err)

		// Put just a block out of the range of valid block indexes
		bc := blockContent(maxBlockCount)
		err = cr.PutBlock(context.TODO(), bc)
		if assert.Error(t, err) {
			assert.EqualValues(t, "block index out of range", err.Error())
		}

		// Put just a block with negative index
		bc = blockContent(-1)
		err = cr.PutBlock(context.TODO(), bc)
		if assert.Error(t, err) {
			assert.EqualValues(t, "block index out of range", err.Error())
		}

	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
}

func (suite *creatorTestSuite) TestCreator_PutBlock_Cancelled() {

	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := cr.Initialize(context.TODO(), sInfo)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.TODO())
		// cancel before calling to make sure it will return cancelled
		cancel()

		// Put just a block
		bc := blockContent(blockList[0])
		err = cr.PutBlock(ctx, bc)
		assert.ErrorIs(t, err, context.Canceled)
	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
}

func (suite *creatorTestSuite) TestCreator_CompleteError() {

	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := cr.Initialize(context.TODO(), sInfo)
		require.NoError(t, err)

		// Complete: it should close ok with 0 blocks
		_, err = cr.Complete(context.TODO())
		require.NoError(t, err)

		// Calling Complete again must error as closed snapshot
		_, err = cr.Complete(context.TODO())
		if assert.Error(t, err) {
			assert.ErrorIs(t, err, ErrErrorClosed)
		}

	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeZeroBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
}

func (suite *creatorTestSuite) TestCreator_FullCycle() {

	var completeErr error
	var completeInfo *Info
	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := cr.Initialize(context.TODO(), sInfo)
		require.NoError(t, err)

		// Put blocks
		for _, i := range blockList {
			err := cr.PutBlock(context.TODO(), blockContent(i))
			require.NoError(t, err)
		}

		// Complete
		completeInfo, completeErr = cr.Complete(context.TODO())
		require.NoError(t, completeErr)
	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
	if suite.postFullCycle != nil {
		suite.postFullCycle(suite.T(), *completeInfo, completeErr)
	}
}

func (suite *creatorTestSuite) TestCreator_DoubleInit() {

	thisTest := func(t *testing.T, cr Creator, sInfo Info, blockList BlockList,
		blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := cr.Initialize(context.TODO(), sInfo)
		require.NoError(t, err)

		// Initialize
		err = cr.Initialize(context.TODO(), sInfo)
		require.ErrorIs(t, err, ErrAlreadyInitialized)

	}

	cr, sInfo, blockList, blockContent := suite.prepare(suite.T(), createFakeBlockContent)
	thisTest(suite.T(), cr, sInfo, blockList, blockContent)
}

type retrieverTestSuite struct {
	suite.Suite
	prepare       func(t *testing.T) (Retriever, Info, func(blockIndex int32) *BlockContent, error)
	postFullCycle func(t *testing.T)
}

func (suite *retrieverTestSuite) TestRetriever_FullCycle() {

	thisTest := func(t *testing.T, rt Retriever, expectedInfo Info, blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := rt.Initialize(context.TODO())
		if !assert.NoError(t, err) {
			return
		}

		// Snapshot info
		sInfo, err := rt.SnapshotInfo(context.TODO())
		if assert.NoError(t, err) {
			assert.EqualValues(t, expectedInfo.BlockSize, sInfo.BlockSize)
			assert.EqualValues(t, expectedInfo.VolumeSize, sInfo.VolumeSize)
			assert.EqualValues(t, expectedInfo.SnapshotId, sInfo.SnapshotId)
			assert.EqualValues(t, expectedInfo.ParentSnapshotId, sInfo.ParentSnapshotId)
			assert.EqualValues(t, expectedInfo.Description, sInfo.Description)
			assert.EqualValues(t, expectedInfo.Tags, sInfo.Tags)
		}

		blockCount := 0
		for {
			bc, err := rt.NextBlock(context.TODO())
			if err == io.EOF {
				break
			}
			if assert.NoError(t, err) {
				assert.True(t, bc.verifyIntegrity())
				// assert.EqualValues(t, bc.BlockIndex, (*bl)[blockCount])
			}
			blockCount++
		}

		// assert.EqualValues(t, len(*bl), blockCount, "blocks expected vs blocks retrieved")
	}

	rt, expectedInfo, blockContent, prepErr := suite.prepare(suite.T())
	if prepErr == nil {
		thisTest(suite.T(), rt, expectedInfo, blockContent)
		if suite.postFullCycle != nil {
			suite.postFullCycle(suite.T())
		}
	}
}

func (suite *retrieverTestSuite) TestRetriever_DoubleInit() {

	thisTest := func(t *testing.T, rt Retriever, expectedInfo Info, blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := rt.Initialize(context.TODO())
		if !assert.NoError(t, err) {
			return
		}

		// Initialize
		err = rt.Initialize(context.TODO())
		assert.ErrorIs(t, err, ErrAlreadyInitialized)
	}

	rt, expectedInfo, blockContent, prepErr := suite.prepare(suite.T())
	if prepErr == nil {
		thisTest(suite.T(), rt, expectedInfo, blockContent)
	}
}

func (suite *retrieverTestSuite) TestRetriever_SnapshotInfoNotInitialized() {

	thisTest := func(t *testing.T, rt Retriever, expectedInfo Info, blockContent func(blockIndex int32) *BlockContent) {

		// Snapshot info
		_, err := rt.SnapshotInfo(context.TODO())
		if assert.Error(t, err) {
			assert.ErrorIs(t, err, ErrNotInitialized)
		}
	}

	rt, expectedInfo, blockContent, prepErr := suite.prepare(suite.T())
	if prepErr == nil {
		thisTest(suite.T(), rt, expectedInfo, blockContent)
	}
}

func (suite *retrieverTestSuite) TestRetriever_NextBlockNotInitialized() {

	thisTest := func(t *testing.T, rt Retriever, expectedInfo Info, blockContent func(blockIndex int32) *BlockContent) {

		// Snapshot info
		_, err := rt.NextBlock(context.TODO())
		if assert.Error(t, err) {
			assert.ErrorIs(t, err, ErrNotInitialized)
		}
	}

	rt, expectedInfo, blockContent, prepErr := suite.prepare(suite.T())
	if prepErr == nil {
		thisTest(suite.T(), rt, expectedInfo, blockContent)
	}
}

func (suite *retrieverTestSuite) TestRetriever_NextBlock_Cancelled() {

	thisTest := func(t *testing.T, rt Retriever, expectedInfo Info, blockContent func(blockIndex int32) *BlockContent) {

		// Initialize
		err := rt.Initialize(context.TODO())
		if !assert.NoError(t, err) {
			return
		}

		ctx, cancel := context.WithCancel(context.TODO())

		// cancel before calling to make sure it will return cancelled
		cancel()

		_, err = rt.NextBlock(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	}

	rt, expectedInfo, blockContent, prepErr := suite.prepare(suite.T())
	if prepErr == nil {
		thisTest(suite.T(), rt, expectedInfo, blockContent)
	}
}
