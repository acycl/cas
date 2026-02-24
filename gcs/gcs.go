// Package gcs provides a Google Cloud Storage source for cas.
package gcs

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/storage/transfermanager"
)

// Option configures a [Source]. The interface is sealed to prevent
// external implementations; use the provided With* functions.
type Option interface {
	apply(*Source)
}

type optionFunc func(*Source)

func (f optionFunc) apply(s *Source) { f(s) }

// Source downloads files from Google Cloud Storage using the transfer manager.
type Source struct {
	downloader *transfermanager.Downloader
}

// NewSource creates a new GCS source from a transfer manager downloader.
func NewSource(d *transfermanager.Downloader, opts ...Option) *Source {
	s := &Source{downloader: d}
	for _, o := range opts {
		o.apply(s)
	}
	return s
}

// Scheme returns "gs".
func (s *Source) Scheme() string { return "gs" }

// Download writes the contents of the GCS object at uri to dst.
func (s *Source) Download(ctx context.Context, dst *os.File, u *url.URL) error {
	bucket := u.Host
	if bucket == "" {
		return fmt.Errorf("missing bucket in URI: %q", u)
	}

	object := strings.TrimPrefix(u.Path, "/")
	if object == "" {
		return fmt.Errorf("missing object path in URI: %q", u)
	}

	var (
		wg  sync.WaitGroup
		err error
	)
	wg.Add(1)

	s.downloader.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
		Bucket:      bucket,
		Object:      object,
		Destination: dst,
		Callback: func(output *transfermanager.DownloadOutput) {
			err = output.Err
			wg.Done()
		},
	})

	wg.Wait()
	if err != nil {
		return fmt.Errorf("downloading GCS object: %w", err)
	}

	return nil
}
