package importjob

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	ErrInvalidProviderResponse = errors.New("import provider returned an invalid response")
	ErrOutputLimitExceeded     = errors.New("import generated output exceeds byte limit")
)

type boundedWriter struct {
	writer  io.Writer
	limit   int64
	written int64
}

func (w *boundedWriter) Write(value []byte) (int, error) {
	if int64(len(value)) > w.limit-w.written {
		return 0, ErrOutputLimitExceeded
	}
	written, err := w.writer.Write(value)
	w.written += int64(written)
	return written, err
}

type Worker struct {
	repository Repository
	transactor transactionRunner
	storage    Storage
	provider   Provider
	batchSize  int
	maxRows    int64
	maxBytes   int64
	timeout    time.Duration
	resultTTL  time.Duration
	now        func() time.Time
}

func NewWorker(repository Repository, transactor transactionRunner, storage Storage, provider Provider, batchSize int, maxRows, maxBytes int64, timeout, resultTTL time.Duration) *Worker {
	return &Worker{repository: repository, transactor: transactor, storage: storage, provider: provider, batchSize: batchSize, maxRows: maxRows, maxBytes: maxBytes, timeout: timeout, resultTTL: resultTTL, now: time.Now}
}

func (w *Worker) Validate(ctx context.Context, tenantID, id string) error {
	job, claimed, err := w.repository.ClaimValidation(ctx, tenantID, id, w.now())
	if err != nil || !claimed {
		return err
	}
	runCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	source, err := w.storage.Get(runCtx, job.SourceObjectKey)
	if err != nil {
		return w.fail(ctx, job, StatusValidating, "source_unavailable", err)
	}
	defer func() { _ = source.Close() }()
	normalized, err := os.CreateTemp("", "import-normalized-*.jsonl")
	if err != nil {
		return w.fail(ctx, job, StatusValidating, "temporary_storage_unavailable", err)
	}
	defer cleanupTemp(normalized.Name())
	report, err := os.CreateTemp("", "import-errors-*.csv")
	if err != nil {
		_ = normalized.Close()
		return w.fail(ctx, job, StatusValidating, "temporary_storage_unavailable", err)
	}
	defer cleanupTemp(report.Name())
	normalizedOutput := &boundedWriter{writer: normalized, limit: w.maxBytes}
	reportCSV := csv.NewWriter(&boundedWriter{writer: report, limit: w.maxBytes})
	if err := reportCSV.Write([]string{"row_number", "column", "code", "message"}); err != nil {
		return w.fail(ctx, job, StatusValidating, "temporary_storage_unavailable", err)
	}
	validation, err := w.provider.OpenValidation(runCtx, job.ProviderService, job.TenantID, job.DatasetCode)
	if err != nil {
		return w.fail(ctx, job, StatusValidating, "provider_unavailable", err)
	}
	defer func() { _ = validation.Close() }()
	validRows, invalidRows := int64(0), int64(0)
	parseResult, err := StreamRows(runCtx, source, job.Format, w.batchSize, w.maxRows, w.maxBytes, func(batch ParsedBatch) error {
		result, validateErr := validation.ValidateBatch(ValidateBatchRequest{TenantID: job.TenantID, DatasetCode: job.DatasetCode, JobID: job.ID, BatchNumber: batch.Number, FirstRowNumber: batch.FirstRowNumber, Rows: batch.Rows})
		if validateErr != nil {
			return validateErr
		}
		invalid := make(map[int64]struct{})
		lastRow := batch.FirstRowNumber + int64(len(batch.Rows)) - 1
		for _, issue := range result.Issues {
			if issue.RowNumber < batch.FirstRowNumber || issue.RowNumber > lastRow || strings.TrimSpace(issue.Code) == "" {
				return ErrInvalidProviderResponse
			}
			invalid[issue.RowNumber] = struct{}{}
			if err := reportCSV.Write([]string{strconv.FormatInt(issue.RowNumber, 10), issue.ColumnKey, issue.Code, issue.Message}); err != nil {
				return err
			}
		}
		batchInvalid := int64(len(invalid))
		batchValid := int64(len(batch.Rows)) - batchInvalid
		if int64(len(result.NormalizedRows)) != batchValid {
			return ErrInvalidProviderResponse
		}
		for _, row := range result.NormalizedRows {
			encoded, encodeErr := json.Marshal(row)
			if encodeErr != nil {
				return fmt.Errorf("encode normalized row: %w", encodeErr)
			}
			if _, writeErr := normalizedOutput.Write(append(encoded, '\n')); writeErr != nil {
				return writeErr
			}
		}
		validRows += batchValid
		invalidRows += batchInvalid
		return nil
	})
	reportCSV.Flush()
	if flushErr := reportCSV.Error(); err == nil && flushErr != nil {
		err = flushErr
	}
	if closeErr := source.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err == nil && !strings.EqualFold(parseResult.Checksum, job.SourceChecksum) {
		err = errors.New("source checksum mismatch")
	}
	if err != nil {
		_ = normalized.Close()
		_ = report.Close()
		return w.fail(ctx, job, StatusValidating, validationErrorCode(err), err)
	}
	if err := closeAndUpload(runCtx, normalized, w.storage, job.NormalizedObjectKey, "application/x-ndjson"); err != nil {
		_ = report.Close()
		return w.fail(ctx, job, StatusValidating, "normalized_upload_failed", err)
	}
	if invalidRows > 0 {
		if err := closeAndUpload(runCtx, report, w.storage, job.ErrorReportObjectKey, "text/csv; charset=utf-8"); err != nil {
			return w.fail(ctx, job, StatusValidating, "error_report_upload_failed", err)
		}
	} else if err := report.Close(); err != nil {
		return w.fail(ctx, job, StatusValidating, "temporary_storage_unavailable", err)
	}
	completed := w.now()
	expires := completed.Add(w.resultTTL)
	job.TotalRows, job.ValidRows, job.InvalidRows = parseResult.Rows, validRows, invalidRows
	job.Status = StatusReady
	change := "validated"
	if invalidRows > 0 {
		job.Status, change = StatusValidationFailed, "validation-failed"
	}
	job.CompletedAt, job.ResultExpiresAt, job.UpdatedAt, job.UpdatedBy = &completed, &expires, completed, "import-worker"
	return w.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		persisted, persistErr := w.repository.FinishValidation(ctx, tx, job)
		if persistErr != nil {
			return persistErr
		}
		event, eventErr := jobChangedEvent(persisted, change, persisted.UpdatedBy, completed)
		if eventErr != nil {
			return eventErr
		}
		return w.repository.AddOutbox(ctx, tx, event)
	})
}

func (w *Worker) Apply(ctx context.Context, tenantID, id string) error {
	job, claimed, err := w.repository.ClaimApply(ctx, tenantID, id, w.now())
	if err != nil || !claimed {
		return err
	}
	runCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	source, err := w.storage.Get(runCtx, job.NormalizedObjectKey)
	if err != nil {
		return w.fail(ctx, job, StatusApplying, "normalized_source_unavailable", err)
	}
	defer func() { _ = source.Close() }()
	apply, err := w.provider.OpenApply(runCtx, job.ProviderService, job.TenantID, job.DatasetCode)
	if err != nil {
		return w.fail(ctx, job, StatusApplying, "provider_unavailable", err)
	}
	defer func() { _ = apply.Close() }()
	applied := int64(0)
	_, err = StreamRows(runCtx, source, FormatJSONL, w.batchSize, w.maxRows, w.maxBytes, func(batch ParsedBatch) error {
		result, applyErr := apply.ApplyBatch(ApplyBatchRequest{TenantID: job.TenantID, DatasetCode: job.DatasetCode, JobID: job.ID, BatchNumber: batch.Number, Rows: batch.Rows, IdempotencyKey: fmt.Sprintf("%s:%d", job.ID, batch.Number)})
		if applyErr != nil {
			return applyErr
		}
		if len(result.Issues) > 0 || result.AppliedRows != int64(len(batch.Rows)) {
			return ErrInvalidProviderResponse
		}
		applied += result.AppliedRows
		return nil
	})
	if err != nil {
		return w.fail(ctx, job, StatusApplying, applyErrorCode(err), err)
	}
	completed := w.now()
	expires := completed.Add(w.resultTTL)
	job.AppliedRows, job.CompletedAt, job.ResultExpiresAt = applied, &completed, &expires
	job.UpdatedAt, job.UpdatedBy = completed, "import-worker"
	return w.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		persisted, persistErr := w.repository.FinishApply(ctx, tx, job)
		if persistErr != nil {
			return persistErr
		}
		event, eventErr := jobChangedEvent(persisted, "succeeded", persisted.UpdatedBy, completed)
		if eventErr != nil {
			return eventErr
		}
		return w.repository.AddOutbox(ctx, tx, event)
	})
}

func (w *Worker) CleanupExpired(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, errors.New("cleanup limit must be positive")
	}
	now := w.now()
	jobs, err := w.repository.ListExpired(ctx, now, limit)
	if err != nil {
		return 0, err
	}
	cleaned := 0
	for _, job := range jobs {
		for _, key := range []string{job.SourceObjectKey, job.NormalizedObjectKey, job.ErrorReportObjectKey} {
			if key != "" {
				if err := w.storage.Delete(ctx, key); err != nil {
					return cleaned, err
				}
			}
		}
		previous := job.Status
		job.Status, job.UpdatedAt, job.UpdatedBy = StatusExpired, now, "import-cleaner"
		err := w.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
			job.Status = previous
			persisted, expireErr := w.repository.Expire(ctx, tx, job, now)
			if expireErr != nil {
				return expireErr
			}
			event, eventErr := jobChangedEvent(persisted, "expired", persisted.UpdatedBy, now)
			if eventErr != nil {
				return eventErr
			}
			return w.repository.AddOutbox(ctx, tx, event)
		})
		if errors.Is(err, ErrStaleVersion) {
			continue
		}
		if err != nil {
			return cleaned, err
		}
		cleaned++
	}
	return cleaned, nil
}

func (w *Worker) fail(ctx context.Context, job Job, expectedStatus, code string, cause error) error {
	now := w.now()
	expires := now.Add(w.resultTTL)
	job.ErrorCode, job.ErrorMessage = code, truncate(cause.Error(), 2000)
	job.CompletedAt, job.ResultExpiresAt, job.UpdatedAt, job.UpdatedBy = &now, &expires, now, "import-worker"
	persistCtx := context.WithoutCancel(ctx)
	persistErr := w.transactor.Within(persistCtx, nil, func(tx *sqlx.Tx) error {
		persisted, err := w.repository.Fail(persistCtx, tx, job, expectedStatus)
		if err != nil {
			return err
		}
		event, err := jobChangedEvent(persisted, "failed", persisted.UpdatedBy, now)
		if err != nil {
			return err
		}
		return w.repository.AddOutbox(persistCtx, tx, event)
	})
	return errors.Join(cause, persistErr)
}

func closeAndUpload(ctx context.Context, file *os.File, storage Storage, key, contentType string) error {
	if err := file.Sync(); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := storage.Put(ctx, key, file, contentType); err != nil {
		return err
	}
	return file.Close()
}

func cleanupTemp(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return
	}
}

func validationErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrRowLimitExceeded):
		return "row_limit_exceeded"
	case errors.Is(err, ErrByteLimitExceeded):
		return "byte_limit_exceeded"
	case errors.Is(err, ErrOutputLimitExceeded):
		return "output_limit_exceeded"
	case errors.Is(err, ErrInvalidHeader), errors.Is(err, ErrInvalidProviderResponse):
		return "invalid_input"
	case strings.Contains(err.Error(), "checksum"):
		return "checksum_mismatch"
	default:
		return "validation_failed"
	}
}
func applyErrorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "apply_failed"
}
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
