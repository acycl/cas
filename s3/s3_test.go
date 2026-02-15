package s3

import (
	"net/url"
	"strings"
	"testing"
)

// parseURI extracts bucket and key from a URI for testing.
// This mirrors the validation logic in Source.Download without needing an S3 client.
func parseURI(uri string) (bucket, key string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	return bucket, key, nil
}

func TestParseURI(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "simple key",
			uri:        "s3://my-bucket/object.txt",
			wantBucket: "my-bucket",
			wantKey:    "object.txt",
		},
		{
			name:       "nested path",
			uri:        "s3://my-bucket/path/to/object.txt",
			wantBucket: "my-bucket",
			wantKey:    "path/to/object.txt",
		},
		{
			name:       "deeply nested",
			uri:        "s3://bucket/a/b/c/d/file.gz",
			wantBucket: "bucket",
			wantKey:    "a/b/c/d/file.gz",
		},
		{
			name:       "key with special chars",
			uri:        "s3://bucket/path/file%20name.txt",
			wantBucket: "bucket",
			wantKey:    "path/file name.txt", // URL-decoded by url.Parse
		},
		{
			name:       "any scheme",
			uri:        "custom://bucket/object.txt",
			wantBucket: "bucket",
			wantKey:    "object.txt",
		},
		{
			name:    "invalid URI",
			uri:     "://invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := parseURI(tt.uri)

			if tt.wantErr {
				if err == nil {
					t.Errorf("parseURI(%q) expected error, got nil", tt.uri)
				}
				return
			}

			if err != nil {
				t.Fatalf("parseURI(%q) unexpected error: %v", tt.uri, err)
			}

			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if key != tt.wantKey {
				t.Errorf("key = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestSourceDownloadValidation(t *testing.T) {
	// Test that Source.Download validates URIs before accessing S3.
	// We use a nil downloader, so valid URIs would panic when accessing S3,
	// but invalid URIs should return errors before that point.
	tests := []struct {
		name    string
		uri     string
		wantErr string
	}{
		{
			name:    "missing bucket",
			uri:     "s3:///object",
			wantErr: "missing bucket",
		},
		{
			name:    "missing key",
			uri:     "s3://bucket/",
			wantErr: "missing key",
		},
		{
			name:    "missing key no slash",
			uri:     "s3://bucket",
			wantErr: "missing key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.uri)
			if err != nil {
				t.Fatalf("url.Parse(%q) unexpected error: %v", tt.uri, err)
			}

			s := &Source{downloader: nil}
			err = s.Download(t.Context(), nil, u)

			if err == nil {
				t.Fatalf("Download(%q) expected error containing %q, got nil", tt.uri, tt.wantErr)
			}

			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("Download(%q) error = %q, want error containing %q", tt.uri, err.Error(), tt.wantErr)
			}
		})
	}
}
