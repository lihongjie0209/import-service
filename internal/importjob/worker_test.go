package importjob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"
)

func TestBoundedWriterRejectsOutputPastLimit(t *testing.T) {
	t.Parallel()
	var destination bytes.Buffer
	writer := &boundedWriter{writer: &destination, limit: 4}
	if written, err := writer.Write([]byte("1234")); err != nil || written != 4 {
		t.Fatalf("first write: written=%d err=%v", written, err)
	}
	if written, err := writer.Write([]byte("5")); !errors.Is(err, ErrOutputLimitExceeded) || written != 0 {
		t.Fatalf("overflow write: written=%d err=%v", written, err)
	}
	if destination.String() != "1234" {
		t.Fatalf("destination=%q", destination.String())
	}
}

type providerStub struct {
	validate         func(ValidateBatchRequest) (ValidateBatchResult, error)
	apply            func(ApplyBatchRequest) (ApplyBatchResult, error)
	list             func(string, int32, int32) ([]DatasetSummary, int64, error)
	describe         func(string, string, string) (DatasetDescriptor, error)
	validationOpened *int
	applyOpened      *int
}

func (p providerStub) ListDatasets(_ context.Context, search string, page, size int32) ([]DatasetSummary, int64, error) {
	if p.list != nil {
		return p.list(search, page, size)
	}
	return nil, 0, nil
}
func (p providerStub) DescribeDataset(_ context.Context, tenant, service, dataset string) (DatasetDescriptor, error) {
	if p.describe != nil {
		return p.describe(tenant, service, dataset)
	}
	return DatasetDescriptor{}, nil
}

func (p providerStub) OpenValidation(context.Context, string, string, string) (ValidationSession, error) {
	if p.validationOpened != nil {
		(*p.validationOpened)++
	}
	return validationSessionStub{validate: p.validate}, nil
}
func (p providerStub) OpenApply(context.Context, string, string, string) (ApplySession, error) {
	if p.applyOpened != nil {
		(*p.applyOpened)++
	}
	return applySessionStub{apply: p.apply}, nil
}

type validationSessionStub struct {
	validate func(ValidateBatchRequest) (ValidateBatchResult, error)
}

func (s validationSessionStub) ValidateBatch(request ValidateBatchRequest) (ValidateBatchResult, error) {
	return s.validate(request)
}
func (validationSessionStub) Close() error { return nil }

type applySessionStub struct {
	apply func(ApplyBatchRequest) (ApplyBatchResult, error)
}

func (s applySessionStub) ApplyBatch(request ApplyBatchRequest) (ApplyBatchResult, error) {
	return s.apply(request)
}
func (applySessionStub) Close() error { return nil }

func TestWorkerValidatesThenAppliesInBoundedBatches(t *testing.T) {
	source := []byte("id,name\n1,A\n2,B\n")
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", DatasetCode: "users.accounts", ProviderService: "identity-service", Format: FormatCSV, Status: StatusQueued, SourceObjectKey: "source", NormalizedObjectKey: "normalized", ErrorReportObjectKey: "errors", SourceChecksum: checksum(source), Version: 2}}
	storage := &fakeStorage{objects: map[string][]byte{"source": source}}
	validationOpened, applyOpened := 0, 0
	provider := providerStub{
		validationOpened: &validationOpened,
		applyOpened:      &applyOpened,
		validate: func(request ValidateBatchRequest) (ValidateBatchResult, error) {
			if len(request.Rows) > 1 {
				t.Fatalf("validation batch size=%d", len(request.Rows))
			}
			return ValidateBatchResult{NormalizedRows: request.Rows}, nil
		},
		apply: func(request ApplyBatchRequest) (ApplyBatchResult, error) {
			if request.IdempotencyKey == "" || len(request.Rows) > 1 {
				t.Fatalf("apply request=%+v", request)
			}
			return ApplyBatchResult{AppliedRows: int64(len(request.Rows))}, nil
		},
	}
	worker := NewWorker(repository, fakeTransaction{}, storage, provider, 1, 10, 1024, time.Minute, time.Hour)
	if err := worker.Validate(context.Background(), "tenant-1", "job-1"); err != nil {
		t.Fatal(err)
	}
	if repository.job.Status != StatusReady || repository.job.ValidRows != 2 || repository.job.InvalidRows != 0 || len(storage.objects["normalized"]) == 0 {
		t.Fatalf("job=%+v objects=%v", repository.job, storage.objects)
	}
	repository.job.Status = StatusApplyQueued
	if err := worker.Apply(context.Background(), "tenant-1", "job-1"); err != nil {
		t.Fatal(err)
	}
	if repository.job.Status != StatusSucceeded || repository.job.AppliedRows != 2 {
		t.Fatalf("job=%+v", repository.job)
	}
	if validationOpened != 1 || applyOpened != 1 {
		t.Fatalf("validation streams=%d apply streams=%d", validationOpened, applyOpened)
	}
	if len(repository.events) != 2 || repository.events[0].Subject != "platform.import.job.validated.v1" || repository.events[1].Subject != "platform.import.job.succeeded.v1" {
		t.Fatalf("events=%+v", repository.events)
	}
}

func TestWorkerCreatesErrorReportAndRequiresCorrection(t *testing.T) {
	source := []byte("id,name\n1,\n")
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", DatasetCode: "users.accounts", ProviderService: "identity-service", Format: FormatCSV, Status: StatusQueued, SourceObjectKey: "source", NormalizedObjectKey: "normalized", ErrorReportObjectKey: "errors", SourceChecksum: checksum(source), Version: 1}}
	storage := &fakeStorage{objects: map[string][]byte{"source": source}}
	provider := providerStub{validate: func(request ValidateBatchRequest) (ValidateBatchResult, error) {
		return ValidateBatchResult{Issues: []RowIssue{{RowNumber: request.FirstRowNumber, ColumnKey: "name", Code: "required", Message: "name is required"}}}, nil
	}, apply: func(ApplyBatchRequest) (ApplyBatchResult, error) { return ApplyBatchResult{}, nil }}
	worker := NewWorker(repository, fakeTransaction{}, storage, provider, 10, 10, 1024, time.Minute, time.Hour)
	if err := worker.Validate(context.Background(), "tenant-1", "job-1"); err != nil {
		t.Fatal(err)
	}
	if repository.job.Status != StatusValidationFailed || repository.job.InvalidRows != 1 || len(storage.objects["errors"]) == 0 {
		t.Fatalf("job=%+v report=%q", repository.job, storage.objects["errors"])
	}
}

func TestWorkerCleansExpiredArtifactsAndEmitsEvent(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	expires := now.Add(-time.Minute)
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", Status: StatusSucceeded, SourceObjectKey: "source", NormalizedObjectKey: "normalized", ErrorReportObjectKey: "errors", ResultExpiresAt: &expires, Version: 5}}
	storage := &fakeStorage{objects: map[string][]byte{"source": {1}, "normalized": {2}, "errors": {3}}}
	provider := providerStub{validate: func(ValidateBatchRequest) (ValidateBatchResult, error) { return ValidateBatchResult{}, nil }, apply: func(ApplyBatchRequest) (ApplyBatchResult, error) { return ApplyBatchResult{}, nil }}
	worker := NewWorker(repository, fakeTransaction{}, storage, provider, 10, 10, 1024, time.Minute, time.Hour)
	worker.now = func() time.Time { return now }
	count, err := worker.CleanupExpired(context.Background(), 10)
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if repository.job.Status != StatusExpired || len(storage.objects) != 0 || len(repository.events) != 1 || repository.events[0].Subject != "platform.import.job.expired.v1" {
		t.Fatalf("job=%+v objects=%v events=%+v", repository.job, storage.objects, repository.events)
	}
}

func checksum(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
