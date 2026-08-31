package objectstorage

import (
	"context"
	"errors"
	"io"
	"net/url"
	"time"

	"github.com/lihongjie0209/import-service/internal/config"
	"github.com/lihongjie0209/import-service/internal/importjob"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var ErrDisabled = errors.New("object storage is disabled")

type S3 struct {
	client        *minio.Client
	presignClient *minio.Client
	cfg           config.ObjectStorage
}

func New(cfg config.Config) (importjob.Storage, error) {
	c := cfg.ObjectStorage
	if !c.Enabled {
		return &S3{cfg: c}, nil
	}
	client, err := minio.New(c.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""), Secure: c.UseSSL, Region: c.Region})
	if err != nil {
		return nil, err
	}
	presignClient := client
	if c.PresignEndpoint != "" && (c.PresignEndpoint != c.Endpoint || c.PresignUseSSL != c.UseSSL) {
		presignClient, err = minio.New(c.PresignEndpoint, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""), Secure: c.PresignUseSSL, Region: c.Region})
		if err != nil {
			return nil, err
		}
	}
	return &S3{client: client, presignClient: presignClient, cfg: c}, nil
}

func (s *S3) PresignUpload(ctx context.Context, key string, ttl time.Duration) (*url.URL, map[string]string, error) {
	if !s.enabled() {
		return nil, nil, ErrDisabled
	}
	ttl = s.presignTTL(ttl)
	value, err := s.presignClient.PresignedPutObject(ctx, s.cfg.Bucket, key, ttl)
	return value, map[string]string{"Content-Type": "application/octet-stream"}, err
}

func (s *S3) PresignDownload(ctx context.Context, key string, ttl time.Duration) (*url.URL, error) {
	if !s.enabled() {
		return nil, ErrDisabled
	}
	return s.presignClient.PresignedGetObject(ctx, s.cfg.Bucket, key, s.presignTTL(ttl), nil)
}

func (s *S3) Stat(ctx context.Context, key string) (importjob.ObjectInfo, error) {
	if !s.enabled() {
		return importjob.ObjectInfo{}, ErrDisabled
	}
	info, err := s.client.StatObject(ctx, s.cfg.Bucket, key, minio.StatObjectOptions{})
	return importjob.ObjectInfo{Size: info.Size}, err
}

func (s *S3) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if !s.enabled() {
		return nil, ErrDisabled
	}
	object, err := s.client.GetObject(ctx, s.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	if _, err := object.Stat(); err != nil {
		_ = object.Close()
		return nil, err
	}
	return object, nil
}

func (s *S3) Put(ctx context.Context, key string, source io.Reader, contentType string) error {
	if !s.enabled() {
		return ErrDisabled
	}
	_, err := s.client.PutObject(ctx, s.cfg.Bucket, key, source, -1, minio.PutObjectOptions{ContentType: contentType})
	return err
}

func (s *S3) Delete(ctx context.Context, key string) error {
	if !s.enabled() {
		return ErrDisabled
	}
	return s.client.RemoveObject(ctx, s.cfg.Bucket, key, minio.RemoveObjectOptions{})
}

func (s *S3) enabled() bool { return s != nil && s.cfg.Enabled }
func (s *S3) presignTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 || ttl > s.cfg.PresignTTL {
		return s.cfg.PresignTTL
	}
	return ttl
}
