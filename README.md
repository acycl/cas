# cas

[![Go Reference](https://pkg.go.dev/badge/github.com/acycl/cas.svg)](https://pkg.go.dev/github.com/acycl/cas)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A content-addressable file cache with download verification.

Files are downloaded from pluggable remote sources, stored locally by their
SHA-256 content hash, and verified before being committed to the cache. The
cache is safe for concurrent use — simultaneous requests for the same checksum
share a single download, and callers can cancel via context without affecting
in-progress downloads.

Requires Go 1.25 or later.

## Installation

```sh
go get github.com/acycl/cas
```

Source packages are installed separately to avoid pulling in unnecessary
dependencies:

```sh
go get github.com/acycl/cas/gcs
go get github.com/acycl/cas/s3
```

## Usage

### GCS

```go
client, _ := storage.NewClient(ctx)
d, _ := transfermanager.NewDownloader(client)
src := gcs.NewSource(d)
cache := cas.New("/var/cache/files",
    cas.WithSource("gs", src),
)

m, _ := cas.NewManifest(
    cas.File("file.txt", "gs://bucket/file.txt", "ab12cd34..."),
)
f, _ := cache.Open(ctx, m, "file.txt")
defer f.Close()
data, _ := io.ReadAll(f)
```

### S3

```go
cfg, _ := config.LoadDefaultConfig(ctx)
client := awss3.NewFromConfig(cfg)
d := manager.NewDownloader(client)
src := s3.NewSource(d)
cache := cas.New("/var/cache/files",
    cas.WithSource("s3", src),
)

m, _ := cas.NewManifest(
    cas.File("file.txt", "s3://bucket/file.txt", "ab12cd34..."),
)
f, _ := cache.Open(ctx, m, "file.txt")
defer f.Close()
data, _ := io.ReadAll(f)
```

### Manifest

A `Manifest` is a standalone type that maps names to remote files. It has no
dependency on a `Cache`, so it can be defined at package scope, loaded from
configuration, or shared across cache instances:

```go
m, _ := cas.NewManifest(
    cas.File("model.bin", "gs://bucket/model.bin", "ab12cd34..."),
    cas.File("config.json", "s3://bucket/config.json", "ef56ab78..."),
)

f, _ := cache.Open(ctx, m, "model.bin")
defer f.Close()
```

Use `Validate` to check at startup that all manifest URIs have registered
sources, without downloading anything:

```go
if err := cache.Validate(m); err != nil {
    log.Fatal(err)
}
```

### Custom sources

Implement the `Source` interface to add support for any protocol:

```go
type Source interface {
    Download(ctx context.Context, dst *os.File, u *url.URL) error
}
```

Register custom sources with `WithSource`:

```go
cache := cas.New("/var/cache/files",
    cas.WithSource("myproto", mySource),
)
```
