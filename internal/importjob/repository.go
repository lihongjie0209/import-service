package importjob

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	ErrNotFound     = errors.New("import job not found")
	ErrStaleVersion = errors.New("stale import job version")
)

type Repository interface {
	Create(context.Context, sqlx.ExtContext, Job) (Job, bool, error)
	Get(context.Context, string, string) (Job, error)
	List(context.Context, ListFilter) (Page, error)
	CompleteUpload(context.Context, sqlx.ExtContext, Job, int64) (Job, error)
	Cancel(context.Context, sqlx.ExtContext, string, string, int64, string, time.Time) (Job, error)
	Retry(context.Context, sqlx.ExtContext, Job, int64) (Job, error)
	Confirm(context.Context, sqlx.ExtContext, Job, int64) (Job, bool, error)
	ClaimValidation(context.Context, string, string, time.Time) (Job, bool, error)
	FinishValidation(context.Context, sqlx.ExtContext, Job) (Job, error)
	ClaimApply(context.Context, string, string, time.Time) (Job, bool, error)
	FinishApply(context.Context, sqlx.ExtContext, Job) (Job, error)
	Fail(context.Context, sqlx.ExtContext, Job, string) (Job, error)
	ListExpired(context.Context, time.Time, int) ([]Job, error)
	Expire(context.Context, sqlx.ExtContext, Job, time.Time) (Job, error)
	AddOutbox(context.Context, sqlx.ExtContext, OutboxEvent) error
}

type SQLRepository struct{ db *sqlx.DB }

func NewRepository(db *sqlx.DB) Repository { return &SQLRepository{db: db} }

const jobColumns = "id,tenant_id,dataset_code,provider_service,format,filename,status,source_object_key,normalized_object_key,error_report_object_key,source_checksum,source_bytes,total_rows,valid_rows,invalid_rows,applied_rows,progress_percent,error_code,error_message,idempotency_key,confirm_key,upload_expires_at,started_at,completed_at,result_expires_at,version,created_at,updated_at,created_by,updated_by"

func (r *SQLRepository) Create(ctx context.Context, e sqlx.ExtContext, value Job) (Job, bool, error) {
	existing, err := r.getByIdempotency(ctx, e, value.TenantID, value.IdempotencyKey)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Job{}, false, err
	}
	args := []any{value.ID, value.TenantID, value.DatasetCode, value.ProviderService, value.Format, value.Filename, value.Status, value.SourceObjectKey, value.NormalizedObjectKey, value.ErrorReportObjectKey, value.SourceChecksum, value.SourceBytes, value.TotalRows, value.ValidRows, value.InvalidRows, value.AppliedRows, value.ProgressPercent, value.ErrorCode, value.ErrorMessage, value.IdempotencyKey, value.ConfirmKey, value.UploadExpiresAt, value.StartedAt, value.CompletedAt, value.ResultExpiresAt, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy}
	_, insertErr := e.ExecContext(ctx, r.db.Rebind("INSERT INTO import_jobs ("+jobColumns+") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"), args...)
	if insertErr == nil {
		return value, true, nil
	}
	// A concurrent request may have won the unique idempotency key race while
	// this INSERT waited. Re-read before classifying the driver-specific error.
	existing, getErr := r.getByIdempotency(ctx, e, value.TenantID, value.IdempotencyKey)
	if getErr == nil {
		return existing, false, nil
	}
	return Job{}, false, insertErr
}

func (r *SQLRepository) Get(ctx context.Context, tenantID, id string) (Job, error) {
	return r.get(ctx, r.db, tenantID, id)
}

func (r *SQLRepository) List(ctx context.Context, filter ListFilter) (Page, error) {
	where, args := "tenant_id=?", []any{filter.TenantID}
	if filter.Status != "" {
		where += " AND status=?"
		args = append(args, filter.Status)
	}
	if filter.DatasetCode != "" {
		where += " AND dataset_code=?"
		args = append(args, filter.DatasetCode)
	}
	if filter.CreatedFrom != nil {
		where += " AND created_at>=?"
		args = append(args, *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		where += " AND created_at<?"
		args = append(args, *filter.CreatedTo)
	}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind("SELECT COUNT(*) FROM import_jobs WHERE "+where), args...); err != nil {
		return Page{}, err
	}
	items := []Job{}
	pageArgs := append(append([]any{}, args...), filter.PageSize, (filter.Page-1)*filter.PageSize)
	err := r.db.SelectContext(ctx, &items, r.db.Rebind("SELECT "+jobColumns+" FROM import_jobs WHERE "+where+" ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?"), pageArgs...)
	return Page{Items: items, Total: total}, err
}

func (r *SQLRepository) CompleteUpload(ctx context.Context, e sqlx.ExtContext, value Job, expected int64) (Job, error) {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status='queued',source_checksum=?,source_bytes=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND version=? AND status='uploading'"), value.SourceChecksum, value.SourceBytes, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID, expected)
	return r.afterUpdate(ctx, e, value.TenantID, value.ID, result, err)
}

func (r *SQLRepository) Cancel(ctx context.Context, e sqlx.ExtContext, tenantID, id string, expected int64, actor string, now time.Time) (Job, error) {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status='canceled',completed_at=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND version=? AND status IN ('uploading','queued','validating','validation_failed','ready','apply_queued','applying','failed')"), now, now, actor, tenantID, id, expected)
	return r.afterUpdate(ctx, e, tenantID, id, result, err)
}

func (r *SQLRepository) Retry(ctx context.Context, e sqlx.ExtContext, value Job, expected int64) (Job, error) {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status='uploading',source_object_key=?,normalized_object_key=?,error_report_object_key=?,source_checksum='',source_bytes=0,total_rows=0,valid_rows=0,invalid_rows=0,applied_rows=0,progress_percent=0,error_code='',error_message='',idempotency_key=?,confirm_key='',upload_expires_at=?,started_at=NULL,completed_at=NULL,result_expires_at=NULL,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND version=? AND status IN ('validation_failed','failed','canceled')"), value.SourceObjectKey, value.NormalizedObjectKey, value.ErrorReportObjectKey, value.IdempotencyKey, value.UploadExpiresAt, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID, expected)
	return r.afterUpdate(ctx, e, value.TenantID, value.ID, result, err)
}

func (r *SQLRepository) Confirm(ctx context.Context, e sqlx.ExtContext, value Job, expected int64) (Job, bool, error) {
	current, err := r.get(ctx, e, value.TenantID, value.ID)
	if err != nil {
		return Job{}, false, err
	}
	if current.ConfirmKey != "" && current.ConfirmKey == value.ConfirmKey {
		return current, false, nil
	}
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status='apply_queued',confirm_key=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND version=? AND status='ready'"), value.ConfirmKey, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID, expected)
	updated, err := r.afterUpdate(ctx, e, value.TenantID, value.ID, result, err)
	return updated, err == nil, err
}

func (r *SQLRepository) ClaimValidation(ctx context.Context, tenantID, id string, now time.Time) (Job, bool, error) {
	return r.claim(ctx, tenantID, id, StatusQueued, StatusValidating, now)
}

func (r *SQLRepository) FinishValidation(ctx context.Context, e sqlx.ExtContext, value Job) (Job, error) {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status=?,total_rows=?,valid_rows=?,invalid_rows=?,progress_percent=100,normalized_object_key=?,error_report_object_key=?,completed_at=?,result_expires_at=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND status='validating'"), value.Status, value.TotalRows, value.ValidRows, value.InvalidRows, value.NormalizedObjectKey, value.ErrorReportObjectKey, value.CompletedAt, value.ResultExpiresAt, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID)
	return r.afterUpdate(ctx, e, value.TenantID, value.ID, result, err)
}

func (r *SQLRepository) ClaimApply(ctx context.Context, tenantID, id string, now time.Time) (Job, bool, error) {
	return r.claim(ctx, tenantID, id, StatusApplyQueued, StatusApplying, now)
}

func (r *SQLRepository) FinishApply(ctx context.Context, e sqlx.ExtContext, value Job) (Job, error) {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status='succeeded',applied_rows=?,progress_percent=100,completed_at=?,result_expires_at=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND status='applying'"), value.AppliedRows, value.CompletedAt, value.ResultExpiresAt, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID)
	return r.afterUpdate(ctx, e, value.TenantID, value.ID, result, err)
}

func (r *SQLRepository) Fail(ctx context.Context, e sqlx.ExtContext, value Job, expectedStatus string) (Job, error) {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status='failed',error_code=?,error_message=?,completed_at=?,result_expires_at=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND status=?"), value.ErrorCode, value.ErrorMessage, value.CompletedAt, value.ResultExpiresAt, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID, expectedStatus)
	return r.afterUpdate(ctx, e, value.TenantID, value.ID, result, err)
}

func (r *SQLRepository) claim(ctx context.Context, tenantID, id, from, to string, now time.Time) (Job, bool, error) {
	result, err := r.db.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status=?,started_at=COALESCE(started_at,?),version=version+1,updated_at=?,updated_by='import-worker' WHERE tenant_id=? AND id=? AND status=?"), to, now, now, tenantID, id, from)
	if err != nil {
		return Job{}, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows == 0 {
		return Job{}, false, err
	}
	job, err := r.Get(ctx, tenantID, id)
	return job, true, err
}

func (r *SQLRepository) ListExpired(ctx context.Context, now time.Time, limit int) ([]Job, error) {
	items := []Job{}
	query := "SELECT " + jobColumns + " FROM import_jobs WHERE (status='uploading' AND upload_expires_at IS NOT NULL AND upload_expires_at<=?) OR (status IN ('validation_failed','succeeded','failed','canceled') AND result_expires_at IS NOT NULL AND result_expires_at<=?) ORDER BY COALESCE(result_expires_at,upload_expires_at),id LIMIT ?"
	err := r.db.SelectContext(ctx, &items, r.db.Rebind(query), now, now, limit)
	return items, err
}

func (r *SQLRepository) Expire(ctx context.Context, e sqlx.ExtContext, value Job, now time.Time) (Job, error) {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE import_jobs SET status='expired',version=version+1,updated_at=?,updated_by='import-cleaner' WHERE tenant_id=? AND id=? AND status=?"), now, value.TenantID, value.ID, value.Status)
	return r.afterUpdate(ctx, e, value.TenantID, value.ID, result, err)
}

func (r *SQLRepository) AddOutbox(ctx context.Context, e sqlx.ExtContext, value OutboxEvent) error {
	_, err := e.ExecContext(ctx, r.db.Rebind("INSERT INTO import_outbox_events (id,subject,envelope,attempts,available_at,published_at,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,?,NULL,'',1,?,?,?,?)"), value.ID, value.Subject, value.Envelope, value.AvailableAt, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy)
	return err
}

func (r *SQLRepository) afterUpdate(ctx context.Context, e sqlx.ExtContext, tenantID, id string, result sql.Result, err error) (Job, error) {
	if err != nil {
		return Job{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, err
	}
	if rows == 0 {
		return Job{}, ErrStaleVersion
	}
	return r.get(ctx, e, tenantID, id)
}

func (r *SQLRepository) get(ctx context.Context, e sqlx.ExtContext, tenantID, id string) (Job, error) {
	var value Job
	err := sqlx.GetContext(ctx, e, &value, r.db.Rebind("SELECT "+jobColumns+" FROM import_jobs WHERE tenant_id=? AND id=?"), tenantID, id)
	return value, notFound(err)
}

func (r *SQLRepository) getByIdempotency(ctx context.Context, e sqlx.ExtContext, tenantID, key string) (Job, error) {
	var value Job
	err := sqlx.GetContext(ctx, e, &value, r.db.Rebind("SELECT "+jobColumns+" FROM import_jobs WHERE tenant_id=? AND idempotency_key=?"), tenantID, key)
	return value, notFound(err)
}

func notFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
