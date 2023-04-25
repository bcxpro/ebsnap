package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"ebsnapshot/gconc"
	"ebsnapshot/protos"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sync"

	ebsTypes "github.com/aws/aws-sdk-go-v2/service/ebs/types"

	"github.com/pierrec/lz4"
	"google.golang.org/protobuf/proto"
)

type BinaryCreator struct {
	sInfo     Info
	blockList BlockList

	initialized bool
	closed      bool
	errorState  bool

	lastBlockIndex int32

	writer io.Writer

	incomingBlocks chan *BlockContent // Blocks received and put in this channel to be encoded and written

	succesfullyPutBlocks int32

	writeContentResult     chan error // Return value of the writeContent goroutine
	putBlockEmergencyBreak chan bool  // when closed signals the blocked PutBlock call to stop and return an error
	opMutex                sync.Mutex
}

func NewBinaryCreator(w io.Writer) *BinaryCreator {
	return &BinaryCreator{
		initialized: false, writer: w,
		writeContentResult:     make(chan error),
		putBlockEmergencyBreak: make(chan bool),
	}
}

func (cr *BinaryCreator) Initialize(ctx context.Context, sInfo Info) error {
	if cr.errorState {
		return ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if cr.initialized {
		return ErrAlreadyInitialized
	}

	cr.lastBlockIndex = -1
	cr.sInfo = sInfo
	cr.blockList = make([]int32, 0)
	cr.incomingBlocks = make(chan *BlockContent, 10)

	// launches the writing goroutine
	go cr.writeContent(cr.writeContentResult)

	cr.initialized = true
	return nil
}

func (cr *BinaryCreator) PutBlock(ctx context.Context, b *BlockContent) error {
	if cr.errorState {
		return ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if !cr.initialized {
		return ErrNotInitialized
	}
	if cr.closed {
		return ErrErrorClosed
	}

	if b.BlockIndex >= int32(blockCount(cr.sInfo.VolumeSize, cr.sInfo.BlockSize)) || b.BlockIndex < 0 {
		return fmt.Errorf("block index out of range")
	}

	if !b.verifyIntegrity() {
		return fmt.Errorf("block content failed integrity verification")
	}

	if b.BlockIndex <= cr.lastBlockIndex {
		return fmt.Errorf("put block not ordered, put block %v after %v", b.BlockIndex, cr.lastBlockIndex)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	default:
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("context done: %w", ctx.Err())
	case cr.incomingBlocks <- b:
		cr.lastBlockIndex = b.BlockIndex
		cr.blockList = append(cr.blockList, b.BlockIndex)
		cr.succesfullyPutBlocks++
		return nil
	case <-cr.putBlockEmergencyBreak:
		cr.errorState = true
		return fmt.Errorf("put block cancelled while waiting to put block %v", b.BlockIndex)
	}
}

func (cr *BinaryCreator) Complete(ctx context.Context) (*Info, error) {
	if cr.errorState {
		return nil, ErrErrorStatus
	}
	cr.opMutex.Lock()
	defer cr.opMutex.Unlock()
	if !cr.initialized {
		return nil, ErrNotInitialized
	}
	if cr.closed {
		return nil, ErrErrorClosed
	}

	// close the put channel and because its goroutine finishes when the channel is closed
	// read the results
	close(cr.incomingBlocks)
	err := <-cr.writeContentResult
	if err != nil {
		cr.errorState = true
		return nil, fmt.Errorf("cannot complete due to error on writer goroutine: %w", err)
	}

	return &Info{
		SnapshotId:       cr.sInfo.SnapshotId,
		ParentSnapshotId: cr.sInfo.ParentSnapshotId,
		VolumeId:         cr.sInfo.VolumeId,
		VolumeSize:       cr.sInfo.VolumeSize,
		BlockSize:        cr.sInfo.BlockSize,
		Description:      cr.sInfo.Description,
		Tags:             cr.sInfo.Tags,
	}, nil
}

func (cr *BinaryCreator) writeContent(result chan<- error) {

	var wg sync.WaitGroup

	var blockWriterErr error
	records := make(chan *protos.SnapshotRecord)

	blockWriter := func() {
		defer wg.Done()
		for record := range records {

			encoded, err := binaryEncodeRecord(record)
			if err != nil {
				blockWriterErr = fmt.Errorf("binary encoding record failed: %w", err)
				return
			}

			_, err = cr.writer.Write(encoded)
			if err != nil {
				close(cr.putBlockEmergencyBreak) // signals an active put block that it has to stop
				blockWriterErr = fmt.Errorf("failed to write binary encoded record: %w", err)
				return
			}

		}

	}

	converter := func(f *gconc.ParallelOrdered[*BlockContent, *protos.SnapshotRecord], wg *sync.WaitGroup) {
		defer wg.Done()
		for block := range cr.incomingBlocks {
			f.In() <- block
		}
		close(f.In())
	}

	wg.Add(1)
	go blockWriter()

	// emit the header
	records <- makeSnapshotHeaderRecord(cr.sInfo)

	tr := func(ctx context.Context, b *BlockContent) (*protos.SnapshotRecord, error) {
		return makeSnapshotBlockRecord(b, protos.Block_LZ4)
	}

	f := gconc.NewParallelOrdered(context.TODO(), tr, runtime.NumCPU(), 1, 1)

	wg.Add(1)
	go converter(f, &wg)

Loop:
	for {
		select {
		case r, ok := <-f.Out():
			if !ok {
				break Loop
			}
			records <- r
		case <-f.Done():
			if f.Err() != nil {
				if e, ok := f.Err().(*gconc.ProcessingError[*BlockContent]); ok {
					result <- fmt.Errorf("could not create block record for index %v had an error: %w", e.WorkItem.BlockIndex, e)
				} else {
					result <- fmt.Errorf("could not create some block record: %w", f.Err())
				}
				close(records)
				wg.Wait()
				return
			}
		}
	}

	// emit the block list
	records <- makeSnapshotBlockListRecord(cr.blockList)

	close(records)
	wg.Wait()

	if blockWriterErr != nil {
		result <- blockWriterErr
		return
	}

	cr.closed = true
	result <- nil
}

const encodedRecordLengthFieldLength = 4 // uint32
const encodedRecordChecksumLength = 32   // SHA-256

// Receives a protobuf snapshot record and returns a byte slice containing the marshalled record plus
// its length and checksum
// Each encoded record has the format
// - length-4-byte big endian uint32 (lenght of the marshalled record only)
// - marshalled protobuf SnapshotRecord
// - SHA256 (32-byte) hash of the length + marshalled protobuf record
func binaryEncodeRecord(record *protos.SnapshotRecord) ([]byte, error) {

	marshalledRecord, err := proto.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("failed to marshall record: %v", err)
	}

	marshalledRecordLength := uint32(len(marshalledRecord))
	fullLength := encodedRecordLengthFieldLength + marshalledRecordLength + encodedRecordChecksumLength
	output := make([]byte, fullLength)

	// length
	binary.BigEndian.PutUint32(output[:encodedRecordLengthFieldLength], marshalledRecordLength)

	// marshalled SnapshotRecord
	copy(output[encodedRecordLengthFieldLength:encodedRecordLengthFieldLength+marshalledRecordLength],
		marshalledRecord)

	// checksum
	checksum := sha256.Sum256(output[:encodedRecordLengthFieldLength+marshalledRecordLength])
	copy(output[fullLength-encodedRecordChecksumLength:], checksum[:])

	return output, nil
}

func makeSnapshotHeaderRecord(sInfo Info) *protos.SnapshotRecord {
	h := protos.SnapshotHeader{
		Version:          1,
		SnapshotId:       sInfo.SnapshotId,
		ParentSnapshotId: sInfo.ParentSnapshotId,
		VolumeId:         sInfo.VolumeId,
		VolumeSize:       sInfo.VolumeSize,
		BlockSize:        sInfo.BlockSize,
		Description:      sInfo.Description,
		Tags:             sInfo.Tags,
	}

	r := protos.SnapshotRecord{Record: &protos.SnapshotRecord_Header{Header: &h}}
	return &r
}

func makeSnapshotBlockListRecord(blockList BlockList) *protos.SnapshotRecord {
	bl := protos.BlockList{Blocks: blockList}
	r := protos.SnapshotRecord{Record: &protos.SnapshotRecord_BlockList{BlockList: &bl}}
	return &r
}

func makeSnapshotBlockRecord(bc *BlockContent, encoding protos.Block_Encoding) (*protos.SnapshotRecord, error) {

	var checksumAlgorithm protos.Block_ChecksumAlgorithm
	switch bc.ChecksumAlgorithm {
	case ebsTypes.ChecksumAlgorithmChecksumAlgorithmSha256:
		checksumAlgorithm = protos.Block_SHA256
	default:
		return nil, fmt.Errorf("unsupported checksum algorithm %v", bc.ChecksumAlgorithm)
	}

	var encodedData []byte

	switch encoding {
	case protos.Block_RAW:
		encodedData = bc.Data
	case protos.Block_GZIP:
		buf := &bytes.Buffer{}
		gzWriter := gzip.NewWriter(buf)
		gzWriter.Write(bc.Data)
		gzWriter.Close()
		encodedData = buf.Bytes()
	case protos.Block_LZ4:
		buf := &bytes.Buffer{}
		lz4Writer := lz4.NewWriter(buf)
		lz4Writer.Write(bc.Data)
		lz4Writer.Close()
		encodedData = buf.Bytes()
	default:
		return nil, fmt.Errorf("unsupported encoding %v", encoding)
	}

	b := protos.Block{
		BlockIndex:        bc.BlockIndex,
		ChecksumAlgorithm: checksumAlgorithm,
		DataChecksum:      bc.Checksum,
		DataEncoding:      encoding,
		Data:              encodedData,
	}

	r := protos.SnapshotRecord{Record: &protos.SnapshotRecord_Block{Block: &b}}
	return &r, nil
}
