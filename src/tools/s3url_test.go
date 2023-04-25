package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse_OK(t *testing.T) {

	u, err := ParseS3Url("s3://mybucket/prefix1/prefix2/object.txt")

	if assert.NoError(t, err) {
		assert.EqualValues(t, S3Url{Bucket: "mybucket", Key: "prefix1/prefix2/object.txt"}, u)
	}
}

func TestParse_NotS3(t *testing.T) {

	_, err := ParseS3Url("http://wikipedia.org")

	assert.Error(t, err)
}


func TestParse_NotURL(t *testing.T) {

	_, err := ParseS3Url("something")

	assert.Error(t, err)
}
