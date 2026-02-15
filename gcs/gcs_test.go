package gcs

import (
	"net/url"
	"strings"
	"testing"
)

// parseURI extracts bucket and object from a URI for testing.
// This mirrors the validation logic in Source.Download without needing a GCS client.
func parseURI(uri string) (bucket, object string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}
	bucket = u.Host
	object = strings.TrimPrefix(u.Path, "/")
	return bucket, object, nil
}

func TestParseURI(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantBucket string
		wantObject string
		wantErr    bool
	}{
		{
			name:       "simple object",
			uri:        "gs://my-bucket/object.txt",
			wantBucket: "my-bucket",
			wantObject: "object.txt",
		},
		{
			name:       "nested path",
			uri:        "gs://my-bucket/path/to/object.txt",
			wantBucket: "my-bucket",
			wantObject: "path/to/object.txt",
		},
		{
			name:       "deeply nested",
			uri:        "gs://bucket/a/b/c/d/file.gz",
			wantBucket: "bucket",
			wantObject: "a/b/c/d/file.gz",
		},
		{
			name:       "object with special chars",
			uri:        "gs://bucket/path/file%20name.txt",
			wantBucket: "bucket",
			wantObject: "path/file name.txt", // URL-decoded by url.Parse
		},
		{
			name:       "any scheme",
			uri:        "custom://bucket/object.txt",
			wantBucket: "bucket",
			wantObject: "object.txt",
		},
		{
			name:    "invalid URI",
			uri:     "://invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, object, err := parseURI(tt.uri)

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
			if object != tt.wantObject {
				t.Errorf("object = %q, want %q", object, tt.wantObject)
			}
		})
	}
}

func TestSourceDownloadValidation(t *testing.T) {
	// Test that Source.Download validates URIs before accessing GCS.
	// We use a nil downloader, so valid URIs would panic when accessing GCS,
	// but invalid URIs should return errors before that point.
	tests := []struct {
		name    string
		uri     string
		wantErr string
	}{
		{
			name:    "missing bucket",
			uri:     "gs:///object",
			wantErr: "missing bucket",
		},
		{
			name:    "missing object",
			uri:     "gs://bucket/",
			wantErr: "missing object",
		},
		{
			name:    "missing object no slash",
			uri:     "gs://bucket",
			wantErr: "missing object",
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
