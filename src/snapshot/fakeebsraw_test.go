package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteRawSnapshot(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 2
	var maxBlockCount = blockCount(volumeSize, blockSize)
	pr, pw := io.Pipe()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pw.Close()
		err := writeFakeRawSnapshot(pw, blockSize, volumeSize, createFakeBlockContent)
		assert.NoError(t, err)
	}()

	postBlockCheck := func(blockIndex int32, data []byte) error {

		if len(data) != int(blockSize) {
			return fmt.Errorf("block index %v invalid data length: %v, expected: %v", blockIndex, len(data), blockSize)
		}
		if blockIndex >= int32(maxBlockCount) || blockIndex < 0 {
			return fmt.Errorf("block index %v out of the allowed range 0-%v", blockIndex, maxBlockCount-1)
		}
		compareData := createFakeBlockContent(blockIndex).Data
		if !bytes.Equal(data, compareData) {
			return fmt.Errorf("block index %v data mismatch", blockIndex)
		}
		return nil
	}

	// Test the fake read
	err := readFakeRawSnapshot(pr, blockSize, volumeSize, postBlockCheck)
	assert.NoError(t, err)
	pr.Close()
	wg.Wait()
}

func TestReadRawBinarySnapshot(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 2
	maxBlockCount := blockCount(int64(volumeSize), int32(blockSize))
	pr, pw := io.Pipe()

	go func(w io.WriteCloser) {
		defer w.Close()
		for i := 0; i < maxBlockCount; i++ {
			data := createFakeBlockContent(int32(i)).Data
			n, err := w.Write(data)
			if assert.NoError(t, err) {
				assert.EqualValues(t, blockSize, n)
			} else {
				return
			}
		}
	}(pw)

	postBlockCheck := func(blockIndex int32, data []byte) error {

		if len(data) != int(blockSize) {
			return fmt.Errorf("block index %v invalid data length: %v, expected: %v", blockIndex, len(data), blockSize)
		}
		if blockIndex >= int32(maxBlockCount) || blockIndex < 0 {
			return fmt.Errorf("block index %v out of the allowed range 0-%v", blockIndex, maxBlockCount-1)
		}
		compareData := createFakeBlockContent(blockIndex).Data
		if !bytes.Equal(data, compareData) {
			return fmt.Errorf("block index %v data mismatch", blockIndex)
		}
		return nil
	}

	err := readFakeRawSnapshot(pr, int32(blockSize), volumeSize, postBlockCheck)
	assert.NoError(t, err)
}

func TestWriteRawBinarySnapshot(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	maxBlockCount := blockCount(int64(volumeSize), int32(blockSize))
	pr, pw := io.Pipe()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		var index int32
		data := make([]byte, blockSize)
		for index = 0; index < int32(maxBlockCount); index++ {
			bc := createFakeBlockContent(index)
			_, err := io.ReadAtLeast(pr, data, int(blockSize))
			if assert.NoError(t, err) {
				assert.EqualValues(t, bc.Data, data)
			}
		}
		_, err := pr.Read(data)
		assert.ErrorIs(t, err, io.EOF)
	}()

	err := writeFakeRawSnapshot(pw, blockSize, volumeSize, createFakeBlockContent)
	assert.NoError(t, err)
	pw.Close()
	wg.Wait()
}

type fakeRawSnapshotWriter struct {
	ErrorStatus error

	blockSize      int32
	volumeSize     int64
	postBlockCheck func(index int32, data []byte) error
	pw             *io.PipeWriter
	pr             *io.PipeReader
	wg             sync.WaitGroup
}

func newFakeRawSnapshotWriter(blockSize int32, volumeSize int64, postBlockCheck func(index int32, data []byte) error) *fakeRawSnapshotWriter {

	fw := fakeRawSnapshotWriter{
		blockSize:      blockSize,
		volumeSize:     volumeSize,
		postBlockCheck: postBlockCheck,
		ErrorStatus:    nil,
	}

	fw.pr, fw.pw = io.Pipe()

	fw.wg.Add(1)
	go func() {
		defer fw.wg.Done()
		defer fw.pr.Close()
		err := readFakeRawSnapshot(fw.pr, blockSize, volumeSize, fw.postBlockCheck)
		fw.ErrorStatus = err
	}()

	return &fw
}

func (fw *fakeRawSnapshotWriter) Write(p []byte) (n int, err error) {
	n, err = fw.pw.Write(p)
	return
}

func (fw *fakeRawSnapshotWriter) Close() error {
	err := fw.pw.Close()
	fw.wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func readFakeRawSnapshot(r io.Reader, blockSize int32, volumeSize int64, postBlockCheck func(index int32, data []byte) error) error {

	buf := make([]byte, blockSize)
	numBlocks := blockCount(volumeSize, blockSize)

	var index int32
	for {
		_, err := io.ReadAtLeast(r, buf, int(blockSize))
		if err == io.EOF {
			if index < int32(numBlocks) {
				return fmt.Errorf("finished early")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if index >= int32(numBlocks) {
			return fmt.Errorf("received data after the expected end of the snapshot")
		}
		if postBlockCheck != nil {
			err = postBlockCheck(index, buf)
			if err != nil {
				return fmt.Errorf("post block check error: %w", err)
			}
		}
		index++
	}
}

func TestFakeRawSnapshotWriter_Complete(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	postCheck := func(index int32, data []byte) error {
		bc := createFakeBlockContent(index)
		if !bytes.Equal(bc.Data, data) {
			return fmt.Errorf("block data mismatch on block index %v", index)
		}
		return nil
	}

	f := newFakeRawSnapshotWriter(blockSize, volumeSize, postCheck)

	for i := 0; i < blockCount(volumeSize, blockSize); i++ {
		bc := createFakeBlockContent(int32(i))
		n, err := f.Write(bc.Data)
		if assert.NoError(t, err) {
			if !assert.Equal(t, len(bc.Data), n) {
				break
			}
		} else {
			break
		}
	}
	err := f.Close()
	assert.NoError(t, err)
}

func TestFakeRawSnapshotWriter_FailsBlockCheck(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	postCheck := func(index int32, data []byte) error {
		if index == 1 {
			return errors.New("failed post block 1 check on purpose for the test")
		}
		return nil
	}

	f := newFakeRawSnapshotWriter(blockSize, volumeSize, postCheck)

	defer f.Close()
	for i := 0; i < blockCount(volumeSize, blockSize); i++ {
		bc := createFakeBlockContent(int32(i))
		n, err := f.Write(bc.Data)
		if err != nil {
			assert.Equal(t, 2, i)
			break
		}
		if !assert.Equal(t, len(bc.Data), n) {
			break
		}

	}

	if assert.Error(t, f.ErrorStatus) {
		assert.Equal(t, "post block check error: failed post block 1 check on purpose for the test", f.ErrorStatus.Error())
	}

}

func TestFakeRawSnapshotWriter_Complete_FragmentedBlocks(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	postCheck := func(index int32, data []byte) error {
		bc := createFakeBlockContent(index)
		if !bytes.Equal(bc.Data, data) {
			return fmt.Errorf("block data mismatch on block index %v", index)
		}
		return nil
	}

	dataGen := func(blockSize int32, volumevolumeSize int64) <-chan []byte {
		ch := make(chan []byte)
		bCount := blockCount(volumevolumeSize, blockSize)

		go func() {
			defer close(ch)
			pendingData := make([]byte, 0)
			nextIndex := 0
			for {

				var chunkSize int

				// Produce different block sizes each iteration 1/4, 1 and 4 times the size of the block
				// This is to not always start or end on block boundaries
				switch nextIndex % 3 {
				case 0:
					chunkSize = int(blockSize / 4)
				case 1:
					chunkSize = int(blockSize)
				default:
					chunkSize = int(blockSize * 4)
				}

				if len(pendingData) == 0 && nextIndex == bCount {
					break
				}

				if len(pendingData) < chunkSize && nextIndex < bCount {
					bc := createFakeBlockContent(int32(nextIndex))
					pendingData = append(pendingData, bc.Data...)
					nextIndex++
					continue
				}

				cutPosition := chunkSize
				if len(pendingData) < cutPosition {
					cutPosition = len(pendingData)
				}
				o := make([]byte, cutPosition)
				copy(o, pendingData[:cutPosition])
				ch <- o
				pendingData = pendingData[cutPosition:]
			}
		}()

		return ch
	}

	f := newFakeRawSnapshotWriter(blockSize, volumeSize, postCheck)

	var dataCounter int
	for data := range dataGen(blockSize, volumeSize) {
		dataCounter += len(data)
		n, err := f.Write(data)
		if assert.NoError(t, err) {
			if !assert.Equal(t, len(data), n) {
				break
			}
		} else {
			break
		}
	}
	assert.EqualValues(t, volumeSize*1024*1024*1024, dataCounter)
	err := f.Close()
	assert.NoError(t, err)
	assert.NoError(t, f.ErrorStatus)
}

func TestFakeRawSnapshotWriter_OverSized(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	postCheck := func(index int32, data []byte) error {
		bc := createFakeBlockContent(index)
		if !bytes.Equal(bc.Data, data) {
			return fmt.Errorf("block data mismatch on block index %v", index)
		}
		return nil
	}

	f := newFakeRawSnapshotWriter(blockSize, volumeSize, postCheck)

	// It sends an extra block on purpose
	for i := 0; i <= blockCount(volumeSize, blockSize); i++ {
		bc := createFakeBlockContent(int32(i))
		n, err := f.Write(bc.Data)
		if assert.NoError(t, err) {
			if !assert.Equal(t, len(bc.Data), n) {
				break
			}
		} else {
			break
		}
	}
	err := f.Close()
	assert.NoError(t, err)
	if assert.Error(t, f.ErrorStatus) {
		assert.EqualValues(t, "received data after the expected end of the snapshot", f.ErrorStatus.Error())
	}
}

func TestFakeRawSnapshotWriter_UnderSized(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	postCheck := func(index int32, data []byte) error {
		bc := createFakeBlockContent(index)
		if !bytes.Equal(bc.Data, data) {
			return fmt.Errorf("block data mismatch on block index %v", index)
		}
		return nil
	}

	f := newFakeRawSnapshotWriter(blockSize, volumeSize, postCheck)

	// It sends one less block than is needed
	for i := 0; i < blockCount(volumeSize, blockSize)-1; i++ {
		bc := createFakeBlockContent(int32(i))
		n, err := f.Write(bc.Data)
		if assert.NoError(t, err) {
			if !assert.Equal(t, len(bc.Data), n) {
				break
			}
		} else {
			break
		}
	}
	err := f.Close()
	assert.NoError(t, err)
	if assert.Error(t, f.ErrorStatus) {
		assert.EqualValues(t, "finished early", f.ErrorStatus.Error())
	}
}

type fakeRawSnapshotReader struct {
	ErrorStatus error

	blockSize    int32
	volumeSize   int64
	blockContent func(blockIndex int32) *BlockContent
	pw           *io.PipeWriter
	pr           *io.PipeReader
	wg           sync.WaitGroup
}

func newFakeRawSnapshotReader(blockSize int32, volumeSize int64, blockContent func(blockIndex int32) *BlockContent) *fakeRawSnapshotReader {

	fr := fakeRawSnapshotReader{
		blockSize:    blockSize,
		volumeSize:   volumeSize,
		blockContent: blockContent,
		ErrorStatus:  nil,
	}

	fr.pr, fr.pw = io.Pipe()

	fr.wg.Add(1)
	go func() {
		defer fr.wg.Done()
		defer fr.pw.Close()
		err := writeFakeRawSnapshot(fr.pw, blockSize, volumeSize, blockContent)
		fr.ErrorStatus = err
	}()

	return &fr
}

func (fr *fakeRawSnapshotReader) Read(p []byte) (n int, err error) {
	n, err = fr.pr.Read(p)
	return
}

func (fr *fakeRawSnapshotReader) Close() error {
	err := fr.pr.Close()
	fr.wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func writeFakeRawSnapshot(w io.Writer, blockSize int32, volumeSize int64, blockContent func(blockIndex int32) *BlockContent) error {

	maxBlockCount := blockCount(volumeSize, blockSize)

	for i := 0; i < maxBlockCount; i++ {
		bc := blockContent(int32(i))
		_, err := w.Write(bc.Data)
		if err != nil {
			return fmt.Errorf("write block error: %w", err)
		}
	}

	return nil
}

func TestFakeRawSnapshotReader_Complete(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	maxBlockCount := blockCount(int64(volumeSize), int32(blockSize))

	r := newFakeRawSnapshotReader(blockSize, volumeSize, createFakeBlockContent)
	data := make([]byte, blockSize)

	var index int32
	for index = 0; index < int32(maxBlockCount); index++ {
		bc := createFakeBlockContent(index)
		_, err := io.ReadAtLeast(r, data, int(blockSize))
		if assert.NoError(t, err) {
			assert.EqualValues(t, bc.Data, data)
		}
	}
	_, err := r.Read(data)
	assert.ErrorIs(t, err, io.EOF)
	err = r.Close()
	assert.NoError(t, err)
	assert.NoError(t, r.ErrorStatus)
}

func TestFakeRawSnapshotReader_PrematureClose(t *testing.T) {

	var blockSize int32 = 512 * 1024
	var volumeSize int64 = 1
	maxBlockCount := blockCount(int64(volumeSize), int32(blockSize))

	r := newFakeRawSnapshotReader(blockSize, volumeSize, createFakeBlockContent)
	data := make([]byte, blockSize)

	var index int32
	for index = 0; index < int32(maxBlockCount)-1; index++ {
		bc := createFakeBlockContent(index)
		_, err := io.ReadAtLeast(r, data, int(blockSize))
		if assert.NoError(t, err) {
			assert.EqualValues(t, bc.Data, data)
		}
	}
	err := r.Close()
	assert.NoError(t, err)
	assert.Error(t, r.ErrorStatus)
}
