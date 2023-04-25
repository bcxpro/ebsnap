package tools

import (
	"fmt"
	"net/url"
)

type S3Url struct {
	Bucket string
	Key    string
}

func ParseS3Url(u string) (S3Url, error) {

	parsed, err := url.Parse(u)

	if err != nil {
		return S3Url{}, fmt.Errorf("cannot parse url: %w", err)
	}

	if parsed.Scheme != "s3" && parsed.Scheme != "S3" {
		return S3Url{}, fmt.Errorf("not an S3 url")
	}

	// Remove the / character from the path
	key := parsed.Path[1:]

	return S3Url{Bucket: parsed.Host, Key: key}, nil
}
