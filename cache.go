// Package cas provides a content-addressable cache for remote files.
//
// Files are downloaded from remote sources, stored locally by their content hash,
// and verified during download. The cache ensures data integrity by validating
// SHA-256 checksums before committing files to the cache.
//
// Example usage:
//
//	client, _ := storage.NewClient(ctx)
//	d, _ := transfermanager.NewDownloader(client)
//	src := gcs.NewSource(d)
//	cache := cas.New("/var/cache/files",
//	    cas.WithSource("gs", src),
//	)
//	m, _ := cas.NewManifest(
//	    cas.File("file.txt", "gs://bucket/file.txt", "sha256hash..."),
//	)
//	f, _ := cache.Open(ctx, m, "file.txt")
//	defer f.Close()
package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"

	"github.com/acycl/weakmap"
)

// maxRetries is the maximum number of times to retry creating the cache
// subdirectory if it is removed concurrently.
const maxRetries = 3

// errDirRemoved indicates the cache subdirectory was removed during download.
var errDirRemoved = errors.New("cache directory removed concurrently")

// Cache manages a local cache of remote files, indexed by content hash.
// Use [Cache.Open] to retrieve files by manifest name, and [Cache.Validate]
// to check that all manifest URIs have registered sources. It is safe for
// concurrent use. Concurrent requests for the same checksum share a single
// download, and callers can cancel their wait via context without affecting
// the in-progress download.
type Cache struct {
	dir     string
	sources map[string]Source
	sems    weakmap.Map[[sha256.Size]byte, chan struct{}]
}

// entry represents a remote file with its URI and expected content hash.
type entry struct {
	uri string
	sum [sha256.Size]byte
}

// parseEntry creates an entry from a URI and hex-encoded SHA-256 checksum.
func parseEntry(uri, sum string) (entry, error) {
	b, err := hex.DecodeString(sum)
	if err != nil {
		return entry{}, fmt.Errorf("invalid checksum: %w", err)
	}
	if len(b) != sha256.Size {
		return entry{}, fmt.Errorf("invalid checksum length: got %d bytes, want %d", len(b), sha256.Size)
	}
	return entry{uri: uri, sum: [sha256.Size]byte(b)}, nil
}

// hexSum returns the hex-encoded SHA-256 checksum.
func (e entry) hexSum() string {
	return hex.EncodeToString(e.sum[:])
}

// Entry describes a remote file by its name, URI, and expected content hash.
// The checksum is validated when the entry is added to a [Manifest].
type Entry struct {
	name string
	uri  string
	sum  string
}

// File creates an [Entry] from a name, URI, and hex-encoded SHA-256 checksum.
func File(name, uri, sum string) Entry {
	return Entry{name: name, uri: uri, sum: sum}
}

// Manifest maps logical file names to remote entries. It is a pure data type
// with no dependency on a [Cache], so it can be defined at package scope,
// loaded from configuration, or shared across cache instances.
type Manifest struct {
	entries map[string]entry
}

// NewManifest creates a [Manifest] from the given file entries. It returns
// an error if any entry has an invalid checksum.
func NewManifest(entries ...Entry) (*Manifest, error) {
	m := &Manifest{
		entries: make(map[string]entry, len(entries)),
	}
	for _, fe := range entries {
		e, err := parseEntry(fe.uri, fe.sum)
		if err != nil {
			return nil, fmt.Errorf("file %q: %w", fe.name, err)
		}
		m.entries[fe.name] = e
	}
	return m, nil
}

// Open returns a file handle for the cached file identified by name in the
// given manifest. If the file is not in the cache, it is downloaded from the
// entry's URI.
func (c *Cache) Open(ctx context.Context, m *Manifest, name string) (*os.File, error) {
	e, ok := m.entries[name]
	if !ok {
		return nil, fmt.Errorf("manifest entry not found: %q", name)
	}
	return c.open(ctx, e)
}

// Validate checks that every URI in the manifest has a registered source.
// It returns the first error found, wrapped with the file name. No downloads
// are performed — this is intended as a startup-time configuration check.
func (c *Cache) Validate(m *Manifest) error {
	for name, e := range m.entries {
		u, err := url.Parse(e.uri)
		if err != nil {
			return fmt.Errorf("file %q: parsing URI: %w", name, err)
		}
		if u.Scheme == "" {
			return fmt.Errorf("file %q: missing scheme in URI: %q", name, e.uri)
		}
		if _, ok := c.sources[u.Scheme]; !ok {
			return fmt.Errorf("file %q: %w: %q", name, ErrUnsupportedScheme, u.Scheme)
		}
	}
	return nil
}

// New creates a new Cache that stores files in the specified directory.
// Cache subdirectories are created as needed when files are downloaded.
//
// Options can be provided to configure the cache:
//   - WithSource: Register a source for a URI scheme (e.g., "gs" for GCS)
func New(dir string, opts ...Option) *Cache {
	c := &Cache{
		dir:     dir,
		sources: make(map[string]Source),
		sems: weakmap.Map[[sha256.Size]byte, chan struct{}]{
			New: func([sha256.Size]byte) *chan struct{} {
				sem := make(chan struct{}, 1)
				return &sem
			},
		},
	}

	for _, opt := range opts {
		opt.apply(c)
	}

	return c
}

// open returns a file handle for the cached file, downloading it if necessary.
func (c *Cache) open(ctx context.Context, e entry) (*os.File, error) {
	hexSum := e.hexSum()

	// Fast path: return cached file without acquiring the semaphore.
	// Cache hits are fully concurrent since no mutation occurs.
	name := filepath.Join(c.dir, hexSum[:2], hexSum)
	if file, err := os.Open(name); err == nil {
		return file, nil
	}

	s, u, err := c.source(e.uri)
	if err != nil {
		return nil, err
	}

	semPtr := c.sems.Load(e.sum)

	// Acquire the inflight semaphore to ensure only one download per checksum.
	// Use *semPtr (not a dereferenced copy) so the pointer stays reachable and
	// the weak map entry is not collected while the semaphore is held.
	select {
	case *semPtr <- struct{}{}:
		defer func() { <-*semPtr }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return c.download(ctx, s, u, e)
}

// source returns the registered source and parsed URL for the URI's scheme.
func (c *Cache) source(uri string) (Source, *url.URL, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing URI: %w", err)
	}
	if u.Scheme == "" {
		return nil, nil, fmt.Errorf("missing scheme in URI: %q", uri)
	}
	s, ok := c.sources[u.Scheme]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %q", ErrUnsupportedScheme, u.Scheme)
	}
	return s, u, nil
}

// download returns the cached file, downloading it from the source if it
// does not already exist. It retries if the cache subdirectory is removed
// concurrently.
func (c *Cache) download(ctx context.Context, s Source, u *url.URL, e entry) (*os.File, error) {
	// Use first two hex chars as subdirectory to distribute files.
	hexSum := e.hexSum()
	dir := filepath.Join(c.dir, hexSum[:2])
	name := filepath.Join(dir, hexSum)

	for range maxRetries {
		// Check cache again under the semaphore, in case another goroutine
		// completed the download while we were waiting.
		file, err := os.Open(name)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}

		// Download the file.
		if err = c.downloadToCache(ctx, s, u, e, name, dir); err != nil {
			if errors.Is(err, errDirRemoved) {
				continue
			}
			return nil, err
		}

		return os.Open(name)
	}

	return nil, fmt.Errorf("failed to open or download file after %d attempts", maxRetries)
}

// downloadToCache creates the cache subdirectory, downloads the file from
// the source, verifies the checksum, and atomically moves it into place.
func (c *Cache) downloadToCache(ctx context.Context, s Source, u *url.URL, e entry, name, dir string) (err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating cache subdirectory: %w", err)
	}

	hexSum := e.hexSum()
	tmpFile, err := os.CreateTemp(dir, hexSum+".tmp-*")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errDirRemoved
		}
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpName)
		}
	}()

	if err = s.Download(ctx, tmpFile, u); err != nil {
		return fmt.Errorf("downloading from source: %w", err)
	}

	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("syncing temp file: %w", err)
	}

	if err = verifyChecksum(tmpFile, e.sum[:]); err != nil {
		return err
	}

	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	if err = os.Rename(tmpName, name); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return errDirRemoved
		}
		return fmt.Errorf("renaming temp file to final cache path: %w", err)
	}

	return nil
}

// verifyChecksum reads f from the beginning and checks that its SHA-256
// matches expectedSum.
func verifyChecksum(f *os.File, expectedSum []byte) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seeking temp file: %w", err)
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("computing checksum: %w", err)
	}

	if actualSum := h.Sum(nil); !bytes.Equal(actualSum, expectedSum) {
		return &ErrInvalidChecksum{
			Expected: hex.EncodeToString(expectedSum),
			Actual:   hex.EncodeToString(actualSum),
		}
	}

	return nil
}

// ErrInvalidChecksum is returned when a file's content does not match
// its expected checksum.
type ErrInvalidChecksum struct {
	Expected, Actual string
}

func (e *ErrInvalidChecksum) Error() string {
	return fmt.Sprintf("invalid checksum: expected %q, got %q", e.Expected, e.Actual)
}
