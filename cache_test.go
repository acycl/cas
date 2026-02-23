package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockSource implements Source for testing. It serves data by URI and counts
// downloads atomically.
type mockSource struct {
	scheme    string
	data      map[string][]byte
	downloads atomic.Int64
}

func newMockSource(scheme string, files map[string][]byte) *mockSource {
	return &mockSource{scheme: scheme, data: files}
}

func (m *mockSource) Scheme() string { return m.scheme }

func (m *mockSource) Download(_ context.Context, dst *os.File, u *url.URL) error {
	m.downloads.Add(1)
	data, ok := m.data[u.String()]
	if !ok {
		return errors.New("not found: " + u.String())
	}
	_, err := dst.WriteAt(data, 0)
	return err
}

// slowSource blocks until signaled, then writes data. Use started to
// synchronize with the test and release to unblock the download.
type slowSource struct {
	scheme  string
	data    []byte
	started chan struct{}
	release chan struct{}
}

func (s *slowSource) Scheme() string { return s.scheme }

func (s *slowSource) Download(_ context.Context, dst *os.File, _ *url.URL) error {
	close(s.started)
	<-s.release
	_, err := dst.WriteAt(s.data, 0)
	return err
}

// barrierSource blocks each download until all expected downloads have
// started. This proves downloads run concurrently: if they were serialized,
// the barrier would never be satisfied and the test would deadlock.
type barrierSource struct {
	scheme  string
	data    map[string][]byte
	barrier *sync.WaitGroup
}

func (s *barrierSource) Scheme() string { return s.scheme }

func (s *barrierSource) Download(_ context.Context, dst *os.File, u *url.URL) error {
	s.barrier.Done()
	s.barrier.Wait()
	data, ok := s.data[u.String()]
	if !ok {
		return errors.New("not found: " + u.String())
	}
	_, err := dst.WriteAt(data, 0)
	return err
}

// dirRemovingSource removes all cache subdirectories after writing file data.
// When persistent is false, the removal happens only on the first call,
// allowing the retry to succeed. When true, every call removes the directory,
// exhausting all retries.
type dirRemovingSource struct {
	scheme     string
	data       []byte
	cacheDir   string
	persistent bool
	once       sync.Once
}

func (s *dirRemovingSource) Scheme() string { return s.scheme }

func (s *dirRemovingSource) Download(_ context.Context, dst *os.File, _ *url.URL) error {
	if _, err := dst.WriteAt(s.data, 0); err != nil {
		return err
	}
	remove := func() {
		entries, _ := os.ReadDir(s.cacheDir)
		for _, e := range entries {
			os.RemoveAll(filepath.Join(s.cacheDir, e.Name()))
		}
	}
	if s.persistent {
		remove()
	} else {
		s.once.Do(remove)
	}
	return nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestCacheOpen(t *testing.T) {
	t.Parallel()

	t.Run("download and cache hit", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("hello, world")
		sum := sha256Hex(content)

		src := newMockSource("test", map[string][]byte{
			"test://file.txt": content,
		})
		cache := New(dir, src)

		m, err := NewManifest(File("file.txt", "test://file.txt", sum))
		if err != nil {
			t.Fatal(err)
		}

		// First open downloads the file.
		f, err := cache.Open(context.Background(), m, "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()

		if !bytes.Equal(got, content) {
			t.Errorf("content = %q, want %q", got, content)
		}
		if src.downloads.Load() != 1 {
			t.Errorf("downloads = %d, want 1", src.downloads.Load())
		}

		// Second open returns the cached file without downloading.
		f2, err := cache.Open(context.Background(), m, "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		f2.Close()

		if src.downloads.Load() != 1 {
			t.Errorf("downloads after cache hit = %d, want 1", src.downloads.Load())
		}
	})

	t.Run("unsupported scheme", func(t *testing.T) {
		t.Parallel()
		cache := New(t.TempDir())

		m, err := NewManifest(File("file.txt", "unknown://file.txt", sha256Hex(nil)))
		if err != nil {
			t.Fatal(err)
		}

		_, err = cache.Open(context.Background(), m, "file.txt")
		if !errors.Is(err, ErrUnsupportedScheme) {
			t.Errorf("got %v, want ErrUnsupportedScheme", err)
		}
	})

	t.Run("missing scheme", func(t *testing.T) {
		t.Parallel()
		cache := New(t.TempDir(), newMockSource("test", nil))
		sum := sha256Hex(nil)

		cases := []struct {
			name string
			uri  string
		}{
			{"bare path", "file.txt"},
			{"empty string", ""},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				m, err := NewManifest(File("f", tt.uri, sum))
				if err != nil {
					t.Fatal(err)
				}
				_, err = cache.Open(context.Background(), m, "f")
				if err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})

	t.Run("source error", func(t *testing.T) {
		t.Parallel()
		src := newMockSource("test", map[string][]byte{})
		cache := New(t.TempDir(), src)

		m, err := NewManifest(File("missing.txt", "test://missing.txt", sha256Hex(nil)))
		if err != nil {
			t.Fatal(err)
		}

		_, err = cache.Open(context.Background(), m, "missing.txt")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("actual content")
		wrongSum := sha256Hex([]byte("different content"))

		src := newMockSource("test", map[string][]byte{
			"test://file.txt": content,
		})
		cache := New(dir, src)

		m, err := NewManifest(File("file.txt", "test://file.txt", wrongSum))
		if err != nil {
			t.Fatal(err)
		}

		_, err = cache.Open(context.Background(), m, "file.txt")

		// Verify the error type and field values.
		var checksumErr *ErrInvalidChecksum
		if !errors.As(err, &checksumErr) {
			t.Fatalf("got %T (%v), want *ErrInvalidChecksum", err, err)
		}
		if checksumErr.Expected != wrongSum {
			t.Errorf("Expected = %q, want %q", checksumErr.Expected, wrongSum)
		}
		if checksumErr.Actual != sha256Hex(content) {
			t.Errorf("Actual = %q, want %q", checksumErr.Actual, sha256Hex(content))
		}
		if msg := checksumErr.Error(); msg == "" {
			t.Error("Error() returned empty string")
		}

		// Verify the file was not cached.
		cachedPath := filepath.Join(dir, wrongSum[:2], wrongSum)
		if _, err := os.Stat(cachedPath); !os.IsNotExist(err) {
			t.Error("file should not be cached after checksum failure")
		}

		// Verify no temp files were left behind.
		matches, _ := filepath.Glob(filepath.Join(dir, "*", "*.tmp-*"))
		if len(matches) > 0 {
			t.Errorf("temp files remain after checksum failure: %v", matches)
		}
	})

	t.Run("content addressed", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("shared content")
		sum := sha256Hex(content)

		// Only register "test://a.txt" in the source.
		src := newMockSource("test", map[string][]byte{
			"test://a.txt": content,
		})
		cache := New(dir, src)

		m, err := NewManifest(
			File("a.txt", "test://a.txt", sum),
			File("b.txt", "test://b.txt", sum),
		)
		if err != nil {
			t.Fatal(err)
		}

		f, err := cache.Open(context.Background(), m, "a.txt")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()

		// Same checksum with a different URI hits the cache without
		// downloading. The source doesn't even have "test://b.txt".
		f2, err := cache.Open(context.Background(), m, "b.txt")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f2)
		if err != nil {
			t.Fatal(err)
		}
		f2.Close()

		if !bytes.Equal(got, content) {
			t.Errorf("content = %q, want %q", got, content)
		}
		if src.downloads.Load() != 1 {
			t.Errorf("downloads = %d, want 1", src.downloads.Load())
		}
	})

	t.Run("directory layout", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("layout test content")
		sum := sha256Hex(content)

		src := newMockSource("test", map[string][]byte{
			"test://file.txt": content,
		})
		cache := New(dir, src)

		m, err := NewManifest(File("file.txt", "test://file.txt", sum))
		if err != nil {
			t.Fatal(err)
		}

		f, err := cache.Open(context.Background(), m, "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()

		// Verify the two-level hex directory structure.
		path := filepath.Join(dir, sum[:2], sum)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("expected file at %s: %v", path, err)
		}
		if info.IsDir() {
			t.Error("expected regular file, got directory")
		}
	})

	t.Run("persists across instances", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("persistent content")
		sum := sha256Hex(content)

		src := newMockSource("test", map[string][]byte{
			"test://file.txt": content,
		})

		cache1 := New(dir, src)
		m, err := NewManifest(File("file.txt", "test://file.txt", sum))
		if err != nil {
			t.Fatal(err)
		}
		f, err := cache1.Open(context.Background(), m, "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		f.Close()

		// A new instance with no sources should serve from the on-disk cache.
		cache2 := New(dir)
		f2, err := cache2.Open(context.Background(), m, "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f2)
		if err != nil {
			t.Fatal(err)
		}
		f2.Close()

		if !bytes.Equal(got, content) {
			t.Errorf("content = %q, want %q", got, content)
		}
	})

	t.Run("multiple sources", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		contentA := []byte("from alpha")
		contentB := []byte("from beta")

		srcA := newMockSource("alpha", map[string][]byte{"alpha://f": contentA})
		srcB := newMockSource("beta", map[string][]byte{"beta://f": contentB})
		cache := New(dir, srcA, srcB)

		m, err := NewManifest(
			File("a", "alpha://f", sha256Hex(contentA)),
			File("b", "beta://f", sha256Hex(contentB)),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()

		fA, err := cache.Open(ctx, m, "a")
		if err != nil {
			t.Fatal(err)
		}
		gotA, err := io.ReadAll(fA)
		if err != nil {
			t.Fatal(err)
		}
		fA.Close()

		fB, err := cache.Open(ctx, m, "b")
		if err != nil {
			t.Fatal(err)
		}
		gotB, err := io.ReadAll(fB)
		if err != nil {
			t.Fatal(err)
		}
		fB.Close()

		if !bytes.Equal(gotA, contentA) {
			t.Errorf("alpha content = %q, want %q", gotA, contentA)
		}
		if !bytes.Equal(gotB, contentB) {
			t.Errorf("beta content = %q, want %q", gotB, contentB)
		}
		if srcA.downloads.Load() != 1 || srcB.downloads.Load() != 1 {
			t.Errorf("downloads: alpha=%d, beta=%d; want 1 each",
				srcA.downloads.Load(), srcB.downloads.Load())
		}
	})
}

func TestCacheOpenConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("deduplicates same checksum", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("shared content")
		sum := sha256Hex(content)

		src := newMockSource("test", map[string][]byte{
			"test://file.txt": content,
		})
		cache := New(dir, src)

		m, err := NewManifest(File("file.txt", "test://file.txt", sum))
		if err != nil {
			t.Fatal(err)
		}

		const goroutines = 10
		var wg sync.WaitGroup
		errs := make(chan error, goroutines)

		for range goroutines {
			wg.Go(func() {
				f, err := cache.Open(context.Background(), m, "file.txt")
				if err != nil {
					errs <- err
					return
				}
				f.Close()
			})
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			t.Errorf("concurrent open: %v", err)
		}
		if n := src.downloads.Load(); n != 1 {
			t.Errorf("downloads = %d, want 1", n)
		}
	})

	t.Run("parallel for different checksums", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		contentA := []byte("concurrent file A")
		contentB := []byte("concurrent file B")

		var barrier sync.WaitGroup
		barrier.Add(2)

		src := &barrierSource{
			scheme: "test",
			data: map[string][]byte{
				"test://a.txt": contentA,
				"test://b.txt": contentB,
			},
			barrier: &barrier,
		}
		cache := New(dir, src)

		m, err := NewManifest(
			File("a.txt", "test://a.txt", sha256Hex(contentA)),
			File("b.txt", "test://b.txt", sha256Hex(contentB)),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Each download blocks until both have started. Serialized
		// downloads would deadlock because the barrier never reaches zero.
		done := make(chan struct{})
		go func() {
			defer close(done)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				f, err := cache.Open(context.Background(), m, "a.txt")
				if err != nil {
					t.Errorf("file A: %v", err)
					return
				}
				f.Close()
			}()
			go func() {
				defer wg.Done()
				f, err := cache.Open(context.Background(), m, "b.txt")
				if err != nil {
					t.Errorf("file B: %v", err)
					return
				}
				f.Close()
			}()
			wg.Wait()
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("downloads appear serialized (barrier deadlock)")
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("slow content")
		sum := sha256Hex(content)

		src := &slowSource{
			scheme:  "test",
			data:    content,
			started: make(chan struct{}),
			release: make(chan struct{}),
		}
		cache := New(dir, src)

		m, err := NewManifest(File("file.txt", "test://file.txt", sum))
		if err != nil {
			t.Fatal(err)
		}

		// Start a download that blocks in the source.
		downloadDone := make(chan error, 1)
		go func() {
			f, err := cache.Open(context.Background(), m, "file.txt")
			if err == nil {
				f.Close()
			}
			downloadDone <- err
		}()
		<-src.started

		// A second caller with a cancelable context waits on the semaphore.
		ctx, cancel := context.WithCancel(context.Background())
		waiterDone := make(chan error, 1)
		go func() {
			_, err := cache.Open(ctx, m, "file.txt")
			waiterDone <- err
		}()

		// Give the waiter time to block on the semaphore, then cancel.
		time.Sleep(10 * time.Millisecond)
		cancel()

		select {
		case err := <-waiterDone:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("waiter error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiter did not return after context cancellation")
		}

		// The original download should still succeed.
		close(src.release)
		select {
		case err := <-downloadDone:
			if err != nil {
				t.Errorf("download error = %v, want nil", err)
			}
		case <-time.After(time.Second):
			t.Fatal("download did not complete")
		}

		// Verify the file was cached despite the waiter's cancellation.
		path := filepath.Join(dir, sum[:2], sum)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("file should be cached: %v", err)
		}
	})
}

func TestCacheOpenRetry(t *testing.T) {
	t.Parallel()

	t.Run("recovers from directory removal", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("retry content")
		sum := sha256Hex(content)

		src := &dirRemovingSource{
			scheme:   "test",
			data:     content,
			cacheDir: dir,
		}
		cache := New(dir, src)

		m, err := NewManifest(File("file.txt", "test://file.txt", sum))
		if err != nil {
			t.Fatal(err)
		}

		f, err := cache.Open(context.Background(), m, "file.txt")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()

		if !bytes.Equal(got, content) {
			t.Errorf("content = %q, want %q", got, content)
		}
	})

	t.Run("exhausts retries", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("doomed content")
		sum := sha256Hex(content)

		src := &dirRemovingSource{
			scheme:     "test",
			data:       content,
			cacheDir:   dir,
			persistent: true,
		}
		cache := New(dir, src)

		m, err := NewManifest(File("file.txt", "test://file.txt", sum))
		if err != nil {
			t.Fatal(err)
		}

		_, err = cache.Open(context.Background(), m, "file.txt")
		if err == nil {
			t.Fatal("expected error after retry exhaustion")
		}
	})
}

func TestNewManifest(t *testing.T) {
	t.Parallel()

	t.Run("opens files by name", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		contentA := []byte("file a content")
		contentB := []byte("file b content")

		src := newMockSource("test", map[string][]byte{
			"test://a.txt": contentA,
			"test://b.txt": contentB,
		})
		cache := New(dir, src)

		m, err := NewManifest(
			File("a.txt", "test://a.txt", sha256Hex(contentA)),
			File("b.txt", "test://b.txt", sha256Hex(contentB)),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()

		f, err := cache.Open(ctx, m, "a.txt")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		if !bytes.Equal(got, contentA) {
			t.Errorf("a.txt content = %q, want %q", got, contentA)
		}

		f2, err := cache.Open(ctx, m, "b.txt")
		if err != nil {
			t.Fatal(err)
		}
		got2, err := io.ReadAll(f2)
		if err != nil {
			t.Fatal(err)
		}
		f2.Close()
		if !bytes.Equal(got2, contentB) {
			t.Errorf("b.txt content = %q, want %q", got2, contentB)
		}

		// Re-opening should use the cache.
		f3, err := cache.Open(ctx, m, "a.txt")
		if err != nil {
			t.Fatal(err)
		}
		f3.Close()

		if src.downloads.Load() != 2 {
			t.Errorf("downloads = %d, want 2 (one per unique file)", src.downloads.Load())
		}
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		m, err := NewManifest()
		if err != nil {
			t.Fatal(err)
		}

		cache := New(t.TempDir())
		_, err = cache.Open(context.Background(), m, "missing.txt")
		if err == nil {
			t.Fatal("expected error for missing manifest entry")
		}
	})

	t.Run("invalid checksum", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name string
			sum  string
		}{
			{"bad hex", "not-valid-hex!"},
			{"wrong length", "abcd1234"},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				_, err := NewManifest(File("f", "test://f", tt.sum))
				if err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})

	t.Run("deduplicates shared checksum", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		content := []byte("shared manifest content")

		src := newMockSource("test", map[string][]byte{
			"test://file.txt": content,
		})
		cache := New(dir, src)

		sum := sha256Hex(content)
		m, err := NewManifest(
			File("name1.txt", "test://file.txt", sum),
			File("name2.txt", "test://file.txt", sum),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()
		f1, err := cache.Open(ctx, m, "name1.txt")
		if err != nil {
			t.Fatal(err)
		}
		f1.Close()

		f2, err := cache.Open(ctx, m, "name2.txt")
		if err != nil {
			t.Fatal(err)
		}
		f2.Close()

		if src.downloads.Load() != 1 {
			t.Errorf("downloads = %d, want 1", src.downloads.Load())
		}
	})
}

func TestCacheValidate(t *testing.T) {
	t.Parallel()

	t.Run("all schemes registered", func(t *testing.T) {
		t.Parallel()
		cache := New(t.TempDir(),
			newMockSource("gs", nil),
			newMockSource("s3", nil),
		)

		m, err := NewManifest(
			File("a", "gs://bucket/a", sha256Hex(nil)),
			File("b", "s3://bucket/b", sha256Hex(nil)),
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := cache.Validate(m); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("unregistered scheme", func(t *testing.T) {
		t.Parallel()
		cache := New(t.TempDir(), newMockSource("gs", nil))

		m, err := NewManifest(
			File("a", "gs://bucket/a", sha256Hex(nil)),
			File("b", "s3://bucket/b", sha256Hex(nil)),
		)
		if err != nil {
			t.Fatal(err)
		}

		err = cache.Validate(m)
		if !errors.Is(err, ErrUnsupportedScheme) {
			t.Errorf("Validate() = %v, want ErrUnsupportedScheme", err)
		}
	})

	t.Run("empty manifest", func(t *testing.T) {
		t.Parallel()
		cache := New(t.TempDir())

		m, err := NewManifest()
		if err != nil {
			t.Fatal(err)
		}

		if err := cache.Validate(m); err != nil {
			t.Errorf("Validate() = %v, want nil", err)
		}
	})

	t.Run("missing scheme in URI", func(t *testing.T) {
		t.Parallel()
		cache := New(t.TempDir(), newMockSource("test", nil))

		cases := []struct {
			name string
			uri  string
		}{
			{"bare path", "file.txt"},
			{"empty string", ""},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				m, err := NewManifest(File("f", tt.uri, sha256Hex(nil)))
				if err != nil {
					t.Fatal(err)
				}
				if err := cache.Validate(m); err == nil {
					t.Fatal("expected error")
				}
			})
		}
	})
}
