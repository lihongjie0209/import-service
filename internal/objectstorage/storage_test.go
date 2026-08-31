package objectstorage

import (
	"testing"
	"time"

	"github.com/lihongjie0209/import-service/internal/config"
)

func TestPresignUsesPublicEndpointWithoutChangingInternalClient(t *testing.T) {
	storage, err := New(config.Config{ObjectStorage: config.ObjectStorage{
		Enabled:         true,
		Endpoint:        "minio:9000",
		PresignEndpoint: "127.0.0.1:9000",
		AccessKey:       "access",
		SecretKey:       "secret",
		Bucket:          "imports",
		Region:          "us-east-1",
		PresignTTL:      15 * time.Minute,
	}})
	if err != nil {
		t.Fatal(err)
	}
	s3 := storage.(*S3)
	if got := s3.client.EndpointURL().Host; got != "minio:9000" {
		t.Fatalf("internal endpoint = %q", got)
	}
	value, _, err := s3.PresignUpload(t.Context(), "tenant/job/source.csv", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if value.Host != "127.0.0.1:9000" {
		t.Fatalf("presigned host = %q", value.Host)
	}
}
