package importjob

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

type fakeTransaction struct{}

func (fakeTransaction) Within(_ context.Context, _ *sql.TxOptions, fn func(*sqlx.Tx) error) error {
	return fn(nil)
}

type fakeRepository struct {
	job    Job
	events []OutboxEvent
}

func (r *fakeRepository) Create(_ context.Context, _ sqlx.ExtContext, value Job) (Job, bool, error) {
	if r.job.ID != "" {
		return r.job, false, nil
	}
	r.job = value
	return r.job, true, nil
}
func (r *fakeRepository) Get(_ context.Context, tenantID, id string) (Job, error) {
	if r.job.TenantID != tenantID || r.job.ID != id {
		return Job{}, ErrNotFound
	}
	return r.job, nil
}
func (*fakeRepository) List(context.Context, ListFilter) (Page, error) { return Page{}, nil }
func (r *fakeRepository) CompleteUpload(_ context.Context, _ sqlx.ExtContext, value Job, expected int64) (Job, error) {
	if r.job.Status != StatusUploading || r.job.Version != expected {
		return Job{}, ErrStaleVersion
	}
	r.job = value
	r.job.Status = StatusQueued
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) Cancel(_ context.Context, _ sqlx.ExtContext, tenantID, id string, expected int64, actor string, now time.Time) (Job, error) {
	if r.job.TenantID != tenantID || r.job.ID != id || r.job.Version != expected {
		return Job{}, ErrStaleVersion
	}
	r.job.Status, r.job.UpdatedBy, r.job.UpdatedAt = StatusCanceled, actor, now
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) Retry(_ context.Context, _ sqlx.ExtContext, value Job, expected int64) (Job, error) {
	if r.job.Version != expected {
		return Job{}, ErrStaleVersion
	}
	r.job = value
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) Confirm(_ context.Context, _ sqlx.ExtContext, value Job, expected int64) (Job, bool, error) {
	if r.job.ConfirmKey == value.ConfirmKey && value.ConfirmKey != "" {
		return r.job, false, nil
	}
	if r.job.Status != StatusReady || r.job.Version != expected {
		return Job{}, false, ErrStaleVersion
	}
	r.job.Status, r.job.ConfirmKey, r.job.UpdatedBy, r.job.UpdatedAt = StatusApplyQueued, value.ConfirmKey, value.UpdatedBy, value.UpdatedAt
	r.job.Version++
	return r.job, true, nil
}
func (r *fakeRepository) ClaimValidation(_ context.Context, tenantID, id string, now time.Time) (Job, bool, error) {
	if r.job.TenantID != tenantID || r.job.ID != id || r.job.Status != StatusQueued {
		return Job{}, false, nil
	}
	r.job.Status, r.job.StartedAt, r.job.UpdatedAt = StatusValidating, &now, now
	r.job.Version++
	return r.job, true, nil
}
func (r *fakeRepository) FinishValidation(_ context.Context, _ sqlx.ExtContext, value Job) (Job, error) {
	r.job = value
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) ClaimApply(_ context.Context, tenantID, id string, now time.Time) (Job, bool, error) {
	if r.job.TenantID != tenantID || r.job.ID != id || r.job.Status != StatusApplyQueued {
		return Job{}, false, nil
	}
	r.job.Status, r.job.StartedAt, r.job.UpdatedAt = StatusApplying, &now, now
	r.job.Version++
	return r.job, true, nil
}
func (r *fakeRepository) FinishApply(_ context.Context, _ sqlx.ExtContext, value Job) (Job, error) {
	r.job = value
	r.job.Status = StatusSucceeded
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) Fail(_ context.Context, _ sqlx.ExtContext, value Job, expected string) (Job, error) {
	if r.job.Status != expected {
		return Job{}, ErrStaleVersion
	}
	r.job = value
	r.job.Status = StatusFailed
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) ListExpired(_ context.Context, now time.Time, limit int) ([]Job, error) {
	if limit < 1 {
		return nil, nil
	}
	expires := r.job.ResultExpiresAt
	if r.job.Status == StatusUploading {
		expires = r.job.UploadExpiresAt
	}
	if expires != nil && !expires.After(now) {
		return []Job{r.job}, nil
	}
	return nil, nil
}
func (r *fakeRepository) Expire(_ context.Context, _ sqlx.ExtContext, value Job, _ time.Time) (Job, error) {
	if r.job.Status != value.Status {
		return Job{}, ErrStaleVersion
	}
	r.job = value
	r.job.Status = StatusExpired
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) ListExpiredMetadataBefore(_ context.Context, before time.Time, limit int) ([]Job, error) {
	if limit > 0 && r.job.Status == StatusExpired && r.job.UpdatedAt.Before(before) {
		return []Job{r.job}, nil
	}
	return nil, nil
}
func (r *fakeRepository) DeleteExpiredMetadata(_ context.Context, _ sqlx.ExtContext, value Job, before time.Time) (bool, error) {
	if r.job.ID != value.ID || r.job.Version != value.Version || r.job.Status != StatusExpired || !r.job.UpdatedAt.Before(before) {
		return false, nil
	}
	r.job = Job{}
	return true, nil
}
func (r *fakeRepository) AddOutbox(_ context.Context, _ sqlx.ExtContext, value OutboxEvent) error {
	r.events = append(r.events, value)
	return nil
}

type fakeStorage struct {
	info    ObjectInfo
	objects map[string][]byte
}

func (*fakeStorage) PresignUpload(context.Context, string, time.Duration) (*url.URL, map[string]string, error) {
	return &url.URL{Scheme: "https", Host: "objects.example", Path: "/upload"}, map[string]string{"Content-Type": "application/octet-stream"}, nil
}
func (*fakeStorage) PresignDownload(context.Context, string, time.Duration) (*url.URL, error) {
	return &url.URL{}, nil
}
func (s *fakeStorage) Stat(_ context.Context, key string) (ObjectInfo, error) {
	if s.info.Size != 0 {
		return s.info, nil
	}
	return ObjectInfo{Size: int64(len(s.objects[key]))}, nil
}
func (s *fakeStorage) Get(_ context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.objects[key])), nil
}
func (s *fakeStorage) Put(_ context.Context, key string, source io.Reader, _ string) error {
	value, err := io.ReadAll(source)
	if err == nil {
		if s.objects == nil {
			s.objects = map[string][]byte{}
		}
		s.objects[key] = value
	}
	return err
}
func (s *fakeStorage) Delete(_ context.Context, key string) error {
	delete(s.objects, key)
	return nil
}

func TestServiceCreateIsTenantScopedAndIdempotent(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	repository := &fakeRepository{}
	service := NewService(repository, fakeTransaction{}, &fakeStorage{}, 15*time.Minute)
	service.now = func() time.Time { return now }
	input := CreateInput{TenantID: "tenant-1", DatasetCode: "billing.invoices", ProviderService: "billing-service", Format: FormatCSV, Filename: "账单.csv", IdempotencyKey: "request-1"}
	job, upload, duplicate, err := service.Create(userContext("tenant-1"), input)
	if err != nil || duplicate || upload.URL == nil {
		t.Fatalf("job=%+v upload=%+v duplicate=%v err=%v", job, upload, duplicate, err)
	}
	if job.Status != StatusUploading || job.Filename != "账单.csv" || job.Version != 1 {
		t.Fatalf("job=%+v", job)
	}
	again, _, duplicate, err := service.Create(userContext("tenant-1"), input)
	if err != nil || !duplicate || again.ID != job.ID {
		t.Fatalf("again=%+v duplicate=%v err=%v", again, duplicate, err)
	}
	if _, _, _, err := service.Create(userContext("tenant-2"), input); err == nil {
		t.Fatal("cross-tenant create accepted")
	}
}

func TestServiceCompleteUploadVerifiesObjectAndWritesRequestedEvent(t *testing.T) {
	repository := &fakeRepository{}
	storage := &fakeStorage{info: ObjectInfo{Size: 42, Checksum: "abc"}}
	service := NewService(repository, fakeTransaction{}, storage, time.Minute)
	job, _, _, err := service.Create(userContext("tenant-1"), CreateInput{TenantID: "tenant-1", DatasetCode: "users.accounts", ProviderService: "identity-service", Format: FormatJSONL, IdempotencyKey: "request-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.CompleteUpload(userContext("tenant-1"), "tenant-1", job.ID, job.Version, 41, "abc"); err == nil {
		t.Fatal("mismatched size accepted")
	}
	updated, err := service.CompleteUpload(userContext("tenant-1"), "tenant-1", job.ID, job.Version, 42, "abc")
	if err != nil || updated.Status != StatusQueued {
		t.Fatalf("job=%+v err=%v", updated, err)
	}
	if len(repository.events) != 1 || repository.events[0].Subject != "platform.import.job.requested.v1" {
		t.Fatalf("events=%+v", repository.events)
	}
}

func TestServiceConfirmIsIdempotentAndWritesApplyEventOnce(t *testing.T) {
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", Status: StatusReady, Version: 3}}
	service := NewService(repository, fakeTransaction{}, &fakeStorage{}, time.Minute)
	job, duplicate, err := service.Confirm(userContext("tenant-1"), "tenant-1", "job-1", 3, "confirm-1")
	if err != nil || duplicate || job.Status != StatusApplyQueued {
		t.Fatalf("job=%+v duplicate=%v err=%v", job, duplicate, err)
	}
	again, duplicate, err := service.Confirm(userContext("tenant-1"), "tenant-1", "job-1", 3, "confirm-1")
	if err != nil || !duplicate || again.ID != job.ID || len(repository.events) != 1 {
		t.Fatalf("again=%+v duplicate=%v events=%d err=%v", again, duplicate, len(repository.events), err)
	}
}

func userContext(tenantID string) context.Context {
	return platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: tenantID})
}
