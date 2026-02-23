package https

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestDownloadSuccess(t *testing.T) {
	const body = "hello, world"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	src := NewSource(WithClient(srv.Client()))
	u, err := url.Parse(srv.URL + "/file.txt")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	dst, err := os.CreateTemp(t.TempDir(), "cas-https-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer dst.Close()

	if err := src.Download(t.Context(), dst, u); err != nil {
		t.Fatalf("Download: %v", err)
	}

	got, err := os.ReadFile(dst.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != body {
		t.Errorf("got %q, want %q", got, body)
	}
}

func TestDownloadNon2xx(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	src := NewSource(WithClient(srv.Client()))
	u, err := url.Parse(srv.URL + "/missing")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	dst, err := os.CreateTemp(t.TempDir(), "cas-https-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer dst.Close()

	err = src.Download(t.Context(), dst, u)
	if err == nil {
		t.Fatal("Download: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "status 404") {
		t.Errorf("error = %q, want substring %q", err, "status 404")
	}
}

func TestDownloadRejectsNonHTTPS(t *testing.T) {
	src := NewSource()
	u := &url.URL{Scheme: "http", Host: "example.com", Path: "/file"}

	err := src.Download(t.Context(), nil, u)
	if err == nil {
		t.Fatal("Download: expected error for http scheme, got nil")
	}
	if !strings.Contains(err.Error(), "only https is allowed") {
		t.Errorf("error = %q, want substring %q", err, "only https is allowed")
	}
}

func TestDownloadRejectsNonHTTPSRedirect(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://example.com/insecure", http.StatusFound)
	}))
	defer srv.Close()

	// Use a permissive client (no CheckRedirect) so the redirect is
	// followed. The response-level scheme guard in Download must catch it.
	permissive := srv.Client()
	permissive.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return nil
	}

	src := NewSource(WithClient(permissive))
	u, err := url.Parse(srv.URL + "/redirect")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	dst, err := os.CreateTemp(t.TempDir(), "cas-https-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer dst.Close()

	err = src.Download(t.Context(), dst, u)
	if err == nil {
		t.Fatal("Download: expected error for redirect to http, got nil")
	}
	if !strings.Contains(err.Error(), "non-https") {
		t.Errorf("error = %q, want substring %q", err, "non-https")
	}
}

func TestDownloadRedirectLimit(t *testing.T) {
	var count int
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		http.Redirect(w, r, r.URL.String(), http.StatusFound)
	}))
	defer srv.Close()

	// Use the default newClient (via NewSource()) but swap in the test
	// server's transport so TLS works against the test server.
	client := newClient()
	client.Transport = srv.Client().Transport

	src := NewSource(WithClient(client))
	u, err := url.Parse(srv.URL + "/loop")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	dst, err := os.CreateTemp(t.TempDir(), "cas-https-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer dst.Close()

	err = src.Download(t.Context(), dst, u)
	if err == nil {
		t.Fatal("Download: expected error for redirect loop, got nil")
	}
	if !strings.Contains(err.Error(), "stopped after") {
		t.Errorf("error = %q, want substring %q", err, "stopped after")
	}
}

func TestDownloadContextCancellation(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	src := NewSource(WithClient(srv.Client()))
	u, err := url.Parse(srv.URL + "/slow")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}

	dst, err := os.CreateTemp(t.TempDir(), "cas-https-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer dst.Close()

	err = src.Download(ctx, dst, u)
	if err == nil {
		t.Fatal("Download: expected error for cancelled context, got nil")
	}
	if !strings.Contains(err.Error(), "context canceled") {
		t.Errorf("error = %q, want substring %q", err, "context canceled")
	}
}
