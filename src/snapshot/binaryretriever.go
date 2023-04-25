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

	"github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"github.com/pierrec/lz4"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"
)

type BinaryRetriever struct {
	r           io.Reader
	initialized bool
	eof         bool
	recordsGen  *gconc.Generator[*protos.SnapshotRecord]
	blocks      BlockList
	sInfo       Info
	version     int32
}

func NewBinaryRetriever(r io.Reader) *BinaryRetriever {
	decoder := BinaryRetriever{r: r, initialized: false}
	return &decoder
}

func (rt *BinaryRetriever) SnapshotInfo(ctx context.Context) (*Info, error) {
	if !rt.initialized {
		return nil, ErrNotInitialized
	}
	out := rt.sInfo
	return &out, nil
}

func (rt *BinaryRetriever) Initialize(ctx context.Context) error {
	if rt.initialized {
		return ErrAlreadyInitialized
	}

	rt.recordsGen = gconc.NewGenerator(context.TODO(), func(ctx context.Context, out chan<- *protos.SnapshotRecord) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			record, err := binaryDecodeRecord(rt.r)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("decode error: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- record:
			}
		}
	})

	// read the first record
	var header *protos.SnapshotHeader
	record, ok := <-rt.recordsGen.Out()
	if !ok {
		return fmt.Errorf("record channel closed before reading the first record")
	}
	switch casted := record.Record.(type) {
	case *protos.SnapshotRecord_Header:
		header = casted.Header
	default:
		return fmt.Errorf("first record is not a header record")
	}

	rt.sInfo.SnapshotId = header.SnapshotId
	rt.sInfo.BlockSize = header.BlockSize
	rt.sInfo.VolumeSize = header.VolumeSize
	rt.sInfo.Description = header.Description
	rt.sInfo.Tags = header.Tags
	rt.sInfo.VolumeId = header.VolumeId
	rt.version = header.Version

	if rt.version != 1 {
		return fmt.Errorf("unsupported version (%v) in header", rt.version)
	}

	rt.blocks = make([]int32, 0)

	rt.initialized = true
	return nil
}

func (rt *BinaryRetriever) NextBlock(ctx context.Context) (*BlockContent, error) {
	if !rt.initialized {
		return nil, ErrNotInitialized
	}
	if rt.eof {
		return nil, io.EOF
	}

	var (
		content *BlockContent
		record  *protos.SnapshotRecord
		ok      bool
	)

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context done: %w", ctx.Err())
	case record, ok = <-rt.recordsGen.Out():
		if !ok {
			<-rt.recordsGen.Done()
			if rt.recordsGen.Err() != nil {
				return nil, fmt.Errorf("records generator failed: %w", rt.recordsGen.Err())
			}
			rt.eof = true
			return nil, io.EOF
		}
	}

	switch casted := record.Record.(type) {
	case *protos.SnapshotRecord_Block:
		var err error
		content, err = protoBlockToBlockContent(casted)
		if err != nil {
			return nil, fmt.Errorf("could not convert protbuf block to BlockContent: %w", err)
		}
	case *protos.SnapshotRecord_BlockList:
		// This is the end of the snapshot, the last record, so it is time to check ¡
		// if the blocks retrieved coincide with the declared list
		if !slices.Equal(rt.blocks, casted.BlockList.Blocks) {
			return nil, fmt.Errorf("unexpected record type %T", casted)
		}
		return nil, io.EOF
	default:
		return nil, fmt.Errorf("unexpected record type %T", casted)
	}

	rt.blocks = append(rt.blocks, content.BlockIndex)
	return content, nil
}

func protoBlockToBlockContent(b *protos.SnapshotRecord_Block) (*BlockContent, error) {
	var decodedData []byte
	switch b.Block.DataEncoding {
	case protos.Block_RAW:
		decodedData = b.Block.Data
	case protos.Block_GZIP:
		buf := &bytes.Buffer{}
		bytesReader := bytes.NewReader(b.Block.Data)
		gzReader, _ := gzip.NewReader(bytesReader)
		io.Copy(buf, gzReader)
		gzReader.Close()
		decodedData = buf.Bytes()
	case protos.Block_LZ4:
		buf := &bytes.Buffer{}
		bytesReader := bytes.NewReader(b.Block.Data)
		lz4Reader := lz4.NewReader(bytesReader)
		io.Copy(buf, lz4Reader)
		decodedData = buf.Bytes()
	default:
		return nil, fmt.Errorf("unknown encoding")
	}

	checksum := sha256.Sum256(decodedData)
	content := BlockContent{
		BlockIndex:        b.Block.BlockIndex,
		Checksum:          checksum[:],
		Data:              decodedData,
		DataLength:        int32(len(decodedData)),
		ChecksumAlgorithm: types.ChecksumAlgorithmChecksumAlgorithmSha256,
	}

	return &content, nil
}

func binaryDecodeRecord(r io.Reader) (*protos.SnapshotRecord, error) {
	lengthBuf := make([]byte, encodedRecordLengthFieldLength)

	// read the marshalled record length
	_, err := io.ReadAtLeast(r, lengthBuf, encodedRecordLengthFieldLength)
	if err != nil {
		switch err {
		case io.ErrUnexpectedEOF:
			return nil, fmt.Errorf("failed to read record length while decoding record: %w", err)
		case io.EOF:
			return nil, io.EOF
		default:
			return nil, fmt.Errorf("failed to read record length while decoding record: %w", err)
		}
	}
	marshalledRecordLength := binary.BigEndian.Uint32(lengthBuf)

	checksummer := sha256.New()
	checksummer.Write(lengthBuf)

	// read the marshalled record plus the SHA256 hash
	buf := make([]byte, marshalledRecordLength+encodedRecordChecksumLength)
	_, err = io.ReadAtLeast(r, buf, int(marshalledRecordLength)+encodedRecordChecksumLength)
	if err != nil {
		return nil, fmt.Errorf("failed to read marshalled record while decoding: %w", err)
	}

	// split the record
	marshalledRecord := buf[:marshalledRecordLength]
	readChecksum := buf[marshalledRecordLength:]

	// validate checksum
	checksummer.Write(marshalledRecord)
	calculatedChecksum := checksummer.Sum(nil)
	if !bytes.Equal(readChecksum, calculatedChecksum) {
		return nil, fmt.Errorf("checksum mismatch while decoding record: %w", err)
	}

	// unmarshall the record
	record := protos.SnapshotRecord{}
	err = proto.Unmarshal(marshalledRecord, &record)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshall record: %w", err)
	}

	return &record, nil
}
