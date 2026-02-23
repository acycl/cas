// Package https provides an HTTPS source for cas.
package https

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
)

// Option configures a [Source].
type Option func(*Source)

// WithClient sets the HTTP client used by the [Source].
// If not provided, [NewSource] uses a default client that enforces
// HTTPS-only redirects and a redirect limit of 10.
func WithClient(c *http.Client) Option {
	return func(s *Source) { s.client = c }
}

// Source downloads files over HTTPS.
type Source struct {
	client *http.Client
}

// NewSource creates a new HTTPS source.
// By default the source uses an internal client that enforces HTTPS-only
// redirects and a redirect limit of 10. Use [WithClient] to override.
func NewSource(opts ...Option) *Source {
	s := &Source{client: newClient()}
	for _, fn := range opts {
		fn(s)
	}
	return s
}

// Scheme returns "https".
func (s *Source) Scheme() string { return "https" }

// Download writes the contents of the HTTPS resource at u to dst.
func (s *Source) Download(ctx context.Context, dst *os.File, u *url.URL) error {
	if u.Scheme != "https" {
		return fmt.Errorf("unsupported scheme %q: only https is allowed", u.Scheme)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("creating request for %s: %w", u.Redacted(), err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching %s: %w", u.Redacted(), err)
	}
	defer resp.Body.Close()

	if resp.Request.URL.Scheme != "https" {
		return fmt.Errorf("redirected to non-https URL: %s", resp.Request.URL.Redacted())
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("fetching %s: status %d", u.Redacted(), resp.StatusCode)
	}

	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("writing response from %s: %w", u.Redacted(), err)
	}

	return nil
}

// maxRedirects is the redirect limit used by [newClient], matching the
// standard library's default policy.
const maxRedirects = 10

// newClient returns an [http.Client] configured for use with [Source].
//
// The client enforces HTTPS-only redirect following and a redirect limit
// of 10. A custom CheckRedirect replaces the standard library's default
// policy, so both checks must be explicit. Go's default transport
// negotiates HTTP/2 via ALPN over TLS, so no extra dependencies are
// needed.
func newClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if req.URL.Scheme != "https" {
				return fmt.Errorf("refusing redirect to non-https URL: %s", req.URL.Redacted())
			}
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			return nil
		},
	}
}
