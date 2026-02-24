// Package s3 provides an Amazon S3 source for cas.
package s3

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Option configures a [Source]. The interface is sealed to prevent
// external implementations; use the provided With* functions.
type Option interface {
	apply(*Source)
}

type optionFunc func(*Source)

func (f optionFunc) apply(s *Source) { f(s) }

// Source downloads files from Amazon S3 using the download manager.
type Source struct {
	downloader *manager.Downloader
}

// NewSource creates a new S3 source from a download manager.
func NewSource(d *manager.Downloader, opts ...Option) *Source {
	s := &Source{downloader: d}
	for _, o := range opts {
		o.apply(s)
	}
	return s
}

// Scheme returns "s3".
func (s *Source) Scheme() string { return "s3" }

// Download writes the contents of the S3 object at uri to dst.
func (s *Source) Download(ctx context.Context, dst *os.File, u *url.URL) error {
	bucket := u.Host
	if bucket == "" {
		return fmt.Errorf("missing bucket in URI: %q", u)
	}

	key := strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return fmt.Errorf("missing key in URI: %q", u)
	}

	_, err := s.downloader.Download(ctx, dst, &awss3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("downloading S3 object: %w", err)
	}

	return nil
}
