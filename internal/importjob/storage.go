package importjob

import (
	"context"
	"io"
	"net/url"
	"time"
)

type ObjectInfo struct {
	Size     int64
	Checksum string
}

type Storage interface {
	PresignUpload(context.Context, string, time.Duration) (*url.URL, map[string]string, error)
	PresignDownload(context.Context, string, time.Duration) (*url.URL, error)
	Stat(context.Context, string) (ObjectInfo, error)
	Get(context.Context, string) (io.ReadCloser, error)
	Put(context.Context, string, io.Reader, string) error
	Delete(context.Context, string) error
}

type Upload struct {
	URL       *url.URL
	Headers   map[string]string
	ExpiresAt time.Time
}
