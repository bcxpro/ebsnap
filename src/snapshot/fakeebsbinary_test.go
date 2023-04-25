package snapshot

import (
	"ebsnapshot/protos"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteBinarySnapshot(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 2

	pr, pw := io.Pipe()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pw.Close()
		err := writeFakeBinarySnapshot(pw, blockSize, volumeSize)
		assert.NoError(t, err)
	}()

	// Test the fake read
	err := readFakeBinarySnapshot(pr, nil, nil, nil)
	assert.NoError(t, err)
	pr.Close()
	wg.Wait()
}

func TestReadWriteBinarySnapshot(t *testing.T) {

	snapshotId := "snap-ffff0000000000001"
	parentSnapshotId := ""
	description := "Synthetic binary snapshot 1"
	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 8
	maxBlockCount := blockCount(volumeSize, blockSize)
	pr, pw := io.Pipe()

	go func(w io.WriteCloser) {
		writeFakeBinarySnapshot(w, blockSize, volumeSize)
		w.Close()
	}(pw)

	postHeaderCheck := func(h protos.SnapshotRecord_Header) error {
		if h.Header.VolumeSize != int64(volumeSize) {
			return fmt.Errorf("volume size mismatch, expected %v, actual %v", volumeSize, h.Header.VolumeSize)
		}
		if h.Header.BlockSize != int32(blockSize) {
			return fmt.Errorf("block size mismatch, expected %v, actual %v", blockSize, h.Header.BlockSize)
		}
		if h.Header.Description != description {
			return fmt.Errorf("description mismatch, expected \"%v\", actual \"%v\"", description, h.Header.Description)
		}
		if h.Header.ParentSnapshotId != parentSnapshotId {
			return fmt.Errorf("parent snapshot id mismatch, expected \"%v\", actual \"%v\"", parentSnapshotId, h.Header.ParentSnapshotId)
		}
		if h.Header.SnapshotId != snapshotId {
			return fmt.Errorf("snapshot id mismatch, expected \"%v\", actual \"%v\"", snapshotId, h.Header.SnapshotId)
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
			if bIndex%10 != 0 {
				return fmt.Errorf("block index %v not expected in the block list", bIndex)
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
		// Todo, checksum y decompresión
		return nil
	}

	err := readFakeBinarySnapshot(pr, postHeaderCheck, postBlockListCheck, postBlockCheck)
	assert.NoError(t, err)
}

type fakeBinarySnapshotReader struct {
	ErrorStatus error

	blockSize  int32
	volumeSize int64

	pr *io.PipeReader
	pw *io.PipeWriter
	wg sync.WaitGroup
}

func newFakeBinarySnapshotReader(blockSize int32, volumeSize int64) *fakeBinarySnapshotReader {

	fr := fakeBinarySnapshotReader{
		ErrorStatus: nil,
		blockSize:   blockSize,
		volumeSize:  volumeSize,
	}

	fr.pr, fr.pw = io.Pipe()

	fr.wg.Add(1)
	go func() {
		defer fr.wg.Done()
		defer fr.pw.Close()
		err := writeFakeBinarySnapshot(fr.pw, blockSize, volumeSize)
		fr.ErrorStatus = err
	}()

	return &fr
}

func (fr *fakeBinarySnapshotReader) Read(p []byte) (n int, err error) {
	n, err = fr.pr.Read(p)
	return
}

func (fr *fakeBinarySnapshotReader) Close() error {
	err := fr.pr.Close()
	fr.wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func writeFakeBinarySnapshot(w io.Writer, blockSize int32, volumeSize int64) error {

	// Header
	tags := make(map[string]string)
	tags["FakeSnapType"] = "synthetic"
	tags["FakeSnapId"] = "snap-ffff0000000000001"

	sInfo := Info{
		SnapshotId:       "snap-ffff0000000000001",
		ParentSnapshotId: "",
		VolumeId:         "vol-abcdefg",
		VolumeSize:       volumeSize,
		BlockSize:        blockSize,
		Description:      "Synthetic binary snapshot 1",
		Tags:             tags,
	}
	h := makeSnapshotHeaderRecord(sInfo)
	encodedheader, err := binaryEncodeRecord(h)
	if err != nil {
		return fmt.Errorf("failed to encode fake header record: %w", err)
	}
	_, err = w.Write(encodedheader)
	if err != nil {
		return fmt.Errorf("failed to write header record: %w", err)
	}

	// Block list

	blockList := make([]int32, 0)
	totalBlocks := int32(blockCount(volumeSize, blockSize))
	for i := int32(0); i < totalBlocks; i++ {
		if i%10 == 0 {
			blockList = append(blockList, i)
		}
	}

	bl := makeSnapshotBlockListRecord(blockList)

	// Blocks

	for _, bi := range blockList {
		bc := createFakeBlockContent(bi)
		blockRecord, err := makeSnapshotBlockRecord(bc, protos.Block_LZ4)
		if err != nil {
			return fmt.Errorf("failed to make protobuf block record with index %v: %w", bi, err)
		}
		encodedBlock, err := binaryEncodeRecord(blockRecord)
		if err != nil {
			return fmt.Errorf("failed to encode fake block content record with index %v: %w", bi, err)
		}
		_, err = w.Write(encodedBlock)
		if err != nil {
			return fmt.Errorf("failed to write fake block content record with index %v: %w", bi, err)
		}
	}

	encodedBlockList, err := binaryEncodeRecord(bl)
	if err != nil {
		return fmt.Errorf("failed to encode fake block list record: %w", err)
	}
	_, err = w.Write(encodedBlockList)
	if err != nil {
		return fmt.Errorf("failed to write block list record: %w", err)
	}

	return nil
}

func TestFakeBinarySnapshotReader_Complete(t *testing.T) {
	//assert.FailNow(t, "not implemented")
	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1

	r := newFakeBinarySnapshotReader(blockSize, volumeSize)

	var recordCount int
	for {
		_, err := binaryDecodeRecord(r)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		recordCount++
	}

	assert.GreaterOrEqual(t, recordCount, 3)
	err := r.Close()
	assert.NoError(t, err)
	assert.NoError(t, r.ErrorStatus)
}

type fakeBinarySnapshotWriter struct {
	ErrorStatus error

	postHeaderCheck    func(protos.SnapshotRecord_Header) error
	postBlockListCheck func(protos.SnapshotRecord_BlockList) error
	postBlockCheck     func(protos.SnapshotRecord_Block) error

	pw *io.PipeWriter
	pr *io.PipeReader
	wg sync.WaitGroup
}

func newFakeBinarySnapshotWriter(
	postHeaderCheck func(protos.SnapshotRecord_Header) error,
	postBlockListCheck func(protos.SnapshotRecord_BlockList) error,
	postBlockCheck func(protos.SnapshotRecord_Block) error) *fakeBinarySnapshotWriter {

	fw := fakeBinarySnapshotWriter{
		postHeaderCheck:    postHeaderCheck,
		postBlockListCheck: postBlockListCheck,
		postBlockCheck:     postBlockCheck,
	}

	fw.pr, fw.pw = io.Pipe()

	fw.wg.Add(1)

	go func() {
		defer fw.wg.Done()
		defer fw.pr.Close()
		err := readFakeBinarySnapshot(fw.pr, fw.postHeaderCheck, fw.postBlockListCheck, fw.postBlockCheck)
		fw.ErrorStatus = err
	}()

	return &fw
}

func (fw *fakeBinarySnapshotWriter) Write(p []byte) (n int, err error) {
	n, err = fw.pw.Write(p)
	return
}

func (fw *fakeBinarySnapshotWriter) Close() error {
	err := fw.pw.Close()
	fw.wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func readFakeBinarySnapshot(r io.Reader, postHeaderCheck func(protos.SnapshotRecord_Header) error,
	postBlockListCheck func(protos.SnapshotRecord_BlockList) error,
	postBlockCheck func(protos.SnapshotRecord_Block) error) error {

	// Read header
	record, err := binaryDecodeRecord(r)
	if err != nil {
		return fmt.Errorf("could not decode header record: %w", err)
	}

	headerRecord, ok := record.Record.(*protos.SnapshotRecord_Header)
	if !ok {
		return fmt.Errorf("the first record decoded is not a header: %w", err)
	}

	if headerRecord.Header.Version != 1 {
		return fmt.Errorf("header version %v is not version 1", headerRecord.Header.Version)
	}

	if postHeaderCheck != nil {
		err = postHeaderCheck(*headerRecord)
		if err != nil {
			return fmt.Errorf("failed header post check: %w", err)
		}
	}

	// Read blocks
	for {
		// Read and decode a record, we do not know at this time if it is a block or other type
		record, err = binaryDecodeRecord(r)
		if err != nil {
			return fmt.Errorf("could not decode expected record")
		}
		// Tests if it is a block, if not exit the loop as no more blocks are expected
		blockRecord, ok := record.Record.(*protos.SnapshotRecord_Block)
		if !ok {
			break
		}
		// It is a block, so execute the post checks
		if postBlockCheck != nil {
			err = postBlockCheck(*blockRecord)
			if err != nil {
				return fmt.Errorf("failed block post check for block for block read containing index %v: %w", blockRecord.Block.BlockIndex, err)
			}
		}
	}

	blockListRecord, ok := record.Record.(*protos.SnapshotRecord_BlockList)
	if !ok {
		return fmt.Errorf("the decoded record is not a block list: %w", err)
	}

	if postBlockListCheck != nil {
		err = postBlockListCheck(*blockListRecord)
		if err != nil {
			return fmt.Errorf("failed block list post check: %w", err)
		}
	}

	_, err = binaryDecodeRecord(r)
	if err == nil {
		return fmt.Errorf("xread record beyond the end of the block list: %w", err)
	}

	return nil
}

func TestFakeBinarySnapshotWriter_Complete(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1

	fw := newFakeBinarySnapshotWriter(nil, nil, nil)

	fr := newFakeBinarySnapshotReader(blockSize, volumeSize)
	defer fr.Close()

	n, err := io.Copy(fw, fr)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, n, int64(0))

	err = fw.Close()
	assert.NoError(t, err)
	assert.NoError(t, fw.ErrorStatus)
}
