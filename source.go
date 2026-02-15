package cas

import (
	"context"
	"errors"
	"net/url"
	"os"
)

// ErrUnsupportedScheme is returned when a URI's scheme has no registered source.
var ErrUnsupportedScheme = errors.New("unsupported URI scheme")

// Source downloads files from a remote location.
type Source interface {
	// Download writes the contents of the remote file at u to dst.
	Download(ctx context.Context, dst *os.File, u *url.URL) error
}
