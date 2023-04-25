package tools

import (
	"errors"
	"fmt"
	"io"
)

func calcVolumeSize(rc io.ReadCloser) (int64, error) {

	const _1GiB = 1024 * 1024 * 1024
	s, ok := rc.(io.Seeker)
	if !ok {
		return 0, errors.New("cannot determine the size of snapshot")
	}

	currrentPosition, err := s.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("cannot determine the size of the snapshot: %w", err)
	}

	byteSize, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("cannot determine the size of the snapshot: %w", err)
	}

	_, err = s.Seek(currrentPosition, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("cannot determine the size of the snapshot: %w", err)
	}

	volumeSize, remainder := byteSize/_1GiB, byteSize%_1GiB
	if remainder > 0 {
		volumeSize++
	}

	return volumeSize, nil
}
