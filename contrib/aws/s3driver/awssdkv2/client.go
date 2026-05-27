// Package awssdkv2 provides an s3driver.Client implementation backed by the
// AWS SDK for Go v2. Import this package alongside s3driver when using the
// official AWS SDK v2 client; use a different adapter package for other
// SDK versions.
//
// NOTE: Experimental
package awssdkv2

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/contrib/aws/s3driver"
)

type s3Client struct {
	client *s3.Client
}

// NewClient creates an s3driver.Client backed by an AWS SDK v2 S3 client.
//
// NOTE: Experimental
func NewClient(client *s3.Client) s3driver.Client {
	_ = "STUB: not implemented"
	return *new(s3driver.Client)
}

func (c *s3Client) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *s3Client) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	_ = "STUB: not implemented"
	return false, nil
}

// HeadObject returns a smithy APIError with code "NotFound" for
// missing objects; some SDK versions also surface *types.NotFound.

func (c *s3Client) Describe() map[string]string { _ = "STUB: not implemented"; return nil }

func (c *s3Client) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
