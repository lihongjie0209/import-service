package importjob

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/import-service/internal/apperror"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

var codePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{1,127}$`)

type transactionRunner interface {
	Within(context.Context, *sql.TxOptions, func(*sqlx.Tx) error) error
}

type Service struct {
	repository   Repository
	transactor   transactionRunner
	storage      Storage
	uploadTTL    time.Duration
	now          func() time.Time
	applications appaccess.Verifier
	provider     Provider
}

type allowAllApplications struct{}

func (allowAllApplications) Verify(context.Context, string, string) error { return nil }

func NewService(repository Repository, transactor transactionRunner, storage Storage, uploadTTL time.Duration) *Service {
	return &Service{repository: repository, transactor: transactor, storage: storage, uploadTTL: uploadTTL, now: time.Now, applications: allowAllApplications{}}
}

func NewRuntimeService(repository Repository, transactor transactionRunner, storage Storage, uploadTTL time.Duration, applications appaccess.Verifier, provider Provider) (*Service, error) {
	if applications == nil {
		return nil, errors.New("application verifier is required")
	}
	if provider == nil {
		return nil, errors.New("import dataset provider is required")
	}
	service := NewService(repository, transactor, storage, uploadTTL)
	service.applications = applications
	service.provider = provider
	return service, nil
}

func (s *Service) Create(ctx context.Context, input CreateInput) (Job, Upload, bool, error) {
	actor, err := s.authorizeScope(ctx, input.TenantID, input.ApplicationID)
	if err != nil {
		return Job{}, Upload{}, false, err
	}
	input.TenantID, input.ApplicationID, input.DatasetCode, input.ProviderService = clean(input.TenantID), clean(input.ApplicationID), clean(input.DatasetCode), clean(input.ProviderService)
	input.Format, input.Filename, input.IdempotencyKey = clean(input.Format), clean(input.Filename), clean(input.IdempotencyKey)
	if input.TenantID == "" || input.ApplicationID == "" || !codePattern.MatchString(input.DatasetCode) || !codePattern.MatchString(input.ProviderService) || !validFormat(input.Format) || input.IdempotencyKey == "" {
		return Job{}, Upload{}, false, apperror.Invalid("tenant_id, application_id, dataset_code, provider_service, format, and idempotency_key are required", nil)
	}
	if err := s.validateDataset(ctx, input); err != nil {
		return Job{}, Upload{}, false, err
	}
	now := s.now()
	id := uuid.NewString()
	expires := now.Add(s.uploadTTL)
	extension := "." + input.Format
	filename := strings.TrimSpace(strings.TrimSuffix(filepath.Base(input.Filename), filepath.Ext(input.Filename)))
	if filename == "" || filename == "." {
		filename = "import"
	}
	job := Job{ID: id, TenantID: input.TenantID, ApplicationID: input.ApplicationID, DatasetCode: input.DatasetCode, ProviderService: input.ProviderService, Format: input.Format, Filename: filename + extension, Status: StatusUploading, SourceObjectKey: fmt.Sprintf("%s/%s/%s/source%s", input.TenantID, input.ApplicationID, id, extension), NormalizedObjectKey: fmt.Sprintf("%s/%s/%s/normalized.jsonl", input.TenantID, input.ApplicationID, id), ErrorReportObjectKey: fmt.Sprintf("%s/%s/%s/errors.csv", input.TenantID, input.ApplicationID, id), IdempotencyKey: input.IdempotencyKey, UploadExpiresAt: &expires, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: actor, UpdatedBy: actor}
	created := false
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		var createErr error
		job, created, createErr = s.repository.Create(ctx, tx, job)
		return createErr
	})
	if err != nil {
		return Job{}, Upload{}, false, translate(err)
	}
	if !created && (job.DatasetCode != input.DatasetCode || job.ProviderService != input.ProviderService || job.Format != input.Format) {
		return Job{}, Upload{}, false, apperror.Conflict("idempotency key was used for a different import", nil)
	}
	upload, err := s.createUpload(ctx, job)
	if err != nil {
		return Job{}, Upload{}, false, err
	}
	return job, upload, !created, nil
}

func (s *Service) validateDataset(ctx context.Context, input CreateInput) error {
	if s.provider == nil {
		return nil
	}
	value, err := s.provider.DescribeDataset(ctx, input.TenantID, input.ApplicationID, input.ProviderService, input.DatasetCode)
	if err != nil {
		return apperror.Unavailable("import dataset descriptor is unavailable", err)
	}
	if value.Code != input.DatasetCode {
		return apperror.Invalid("import provider returned a different dataset", nil)
	}
	for _, format := range value.Formats {
		if format == input.Format {
			return nil
		}
	}
	return apperror.Invalid("dataset does not support the requested format", nil)
}

func (s *Service) CompleteUpload(ctx context.Context, tenantID, applicationID, id string, expected, sourceBytes int64, checksum string) (Job, error) {
	actor, err := s.authorizeScope(ctx, tenantID, applicationID)
	if err != nil {
		return Job{}, err
	}
	if expected < 1 || sourceBytes < 1 || clean(checksum) == "" {
		return Job{}, apperror.Invalid("version, source_bytes, and source_checksum are required", nil)
	}
	current, err := s.repository.Get(ctx, clean(tenantID), clean(applicationID), clean(id))
	if err != nil {
		return Job{}, translate(err)
	}
	info, err := s.storage.Stat(ctx, current.SourceObjectKey)
	if err != nil {
		return Job{}, apperror.Unavailable("uploaded object is unavailable", err)
	}
	if info.Size != sourceBytes || (info.Checksum != "" && !strings.EqualFold(info.Checksum, checksum)) {
		return Job{}, apperror.Invalid("uploaded object metadata does not match", nil)
	}
	now := s.now()
	current.SourceBytes, current.SourceChecksum, current.UpdatedAt, current.UpdatedBy = sourceBytes, clean(checksum), now, actor
	var updated Job
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		updated, err = s.repository.CompleteUpload(ctx, tx, current, expected)
		if err != nil {
			return err
		}
		return s.addEvent(ctx, tx, updated, "requested", actor, now)
	})
	return updated, translate(err)
}

func (s *Service) Get(ctx context.Context, tenantID, applicationID, id string) (Job, error) {
	if _, err := s.authorizeScope(ctx, tenantID, applicationID); err != nil {
		return Job{}, err
	}
	value, err := s.repository.Get(ctx, clean(tenantID), clean(applicationID), clean(id))
	return value, translate(err)
}

func (s *Service) List(ctx context.Context, filter ListFilter) (Page, error) {
	if _, err := s.authorizeScope(ctx, filter.TenantID, filter.ApplicationID); err != nil {
		return Page{}, err
	}
	if filter.Page < 1 {
		filter.Page = 1
	}
	if filter.PageSize < 1 {
		filter.PageSize = 20
	} else if filter.PageSize > 100 {
		filter.PageSize = 100
	}
	filter.TenantID, filter.ApplicationID = clean(filter.TenantID), clean(filter.ApplicationID)
	page, err := s.repository.List(ctx, filter)
	return page, translate(err)
}

func (s *Service) Cancel(ctx context.Context, tenantID, applicationID, id string, expected int64) (Job, error) {
	actor, err := s.authorizeScope(ctx, tenantID, applicationID)
	if err != nil {
		return Job{}, err
	}
	now := s.now()
	var updated Job
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		updated, err = s.repository.Cancel(ctx, tx, clean(tenantID), clean(applicationID), clean(id), expected, actor, now)
		if err != nil {
			return err
		}
		return s.addEvent(ctx, tx, updated, "canceled", actor, now)
	})
	return updated, translate(err)
}

func (s *Service) Retry(ctx context.Context, tenantID, applicationID, id string, expected int64, key string) (Job, Upload, bool, error) {
	actor, err := s.authorizeScope(ctx, tenantID, applicationID)
	if err != nil {
		return Job{}, Upload{}, false, err
	}
	if expected < 1 || clean(key) == "" {
		return Job{}, Upload{}, false, apperror.Invalid("version and idempotency_key are required", nil)
	}
	current, err := s.repository.Get(ctx, clean(tenantID), clean(applicationID), clean(id))
	if err != nil {
		return Job{}, Upload{}, false, translate(err)
	}
	if current.Status == StatusUploading && current.IdempotencyKey == clean(key) {
		upload, uploadErr := s.createUpload(ctx, current)
		return current, upload, true, uploadErr
	}
	now := s.now()
	expires := now.Add(s.uploadTTL)
	current.SourceObjectKey = fmt.Sprintf("%s/%s/%s/source.%s", current.TenantID, current.ApplicationID, current.ID, current.Format)
	current.NormalizedObjectKey = fmt.Sprintf("%s/%s/%s/normalized.jsonl", current.TenantID, current.ApplicationID, current.ID)
	current.ErrorReportObjectKey = fmt.Sprintf("%s/%s/%s/errors.csv", current.TenantID, current.ApplicationID, current.ID)
	current.IdempotencyKey, current.UploadExpiresAt, current.UpdatedAt, current.UpdatedBy = clean(key), &expires, now, actor
	var updated Job
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		updated, err = s.repository.Retry(ctx, tx, current, expected)
		if err != nil {
			return err
		}
		return s.addEvent(ctx, tx, updated, "retried", actor, now)
	})
	if err != nil {
		return Job{}, Upload{}, false, translate(err)
	}
	upload, err := s.createUpload(ctx, updated)
	return updated, upload, false, err
}

func (s *Service) Confirm(ctx context.Context, tenantID, applicationID, id string, expected int64, key string) (Job, bool, error) {
	actor, err := s.authorizeScope(ctx, tenantID, applicationID)
	if err != nil {
		return Job{}, false, err
	}
	if expected < 1 || clean(key) == "" {
		return Job{}, false, apperror.Invalid("version and idempotency_key are required", nil)
	}
	now := s.now()
	input := Job{TenantID: clean(tenantID), ApplicationID: clean(applicationID), ID: clean(id), ConfirmKey: clean(key), UpdatedAt: now, UpdatedBy: actor}
	var updated Job
	created := false
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		updated, created, err = s.repository.Confirm(ctx, tx, input, expected)
		if err != nil || !created {
			return err
		}
		return s.addEvent(ctx, tx, updated, "apply-requested", actor, now)
	})
	return updated, !created, translate(err)
}

func (s *Service) CreateErrorReportDownloadURL(ctx context.Context, tenantID, applicationID, id string, ttl time.Duration) (*url.URL, time.Time, Job, error) {
	job, err := s.Get(ctx, tenantID, applicationID, id)
	if err != nil {
		return nil, time.Time{}, Job{}, err
	}
	now := s.now()
	if job.InvalidRows < 1 || job.ErrorReportObjectKey == "" || job.ResultExpiresAt == nil || !job.ResultExpiresAt.After(now) {
		return nil, time.Time{}, Job{}, apperror.Conflict("import error report is not available", nil)
	}
	if ttl <= 0 || ttl > s.uploadTTL {
		ttl = s.uploadTTL
	}
	if remaining := job.ResultExpiresAt.Sub(now); ttl > remaining {
		ttl = remaining
	}
	value, err := s.storage.PresignDownload(ctx, job.ErrorReportObjectKey, ttl)
	if err != nil {
		return nil, time.Time{}, Job{}, apperror.Unavailable("object storage unavailable", err)
	}
	return value, now.Add(ttl), job, nil
}

func (s *Service) createUpload(ctx context.Context, job Job) (Upload, error) {
	if job.Status != StatusUploading || job.UploadExpiresAt == nil || !job.UploadExpiresAt.After(s.now()) {
		return Upload{}, apperror.Conflict("import upload is not available", nil)
	}
	ttl := job.UploadExpiresAt.Sub(s.now())
	value, headers, err := s.storage.PresignUpload(ctx, job.SourceObjectKey, ttl)
	if err != nil {
		return Upload{}, apperror.Unavailable("object storage unavailable", err)
	}
	return Upload{URL: value, Headers: headers, ExpiresAt: s.now().Add(ttl)}, nil
}

func (s *Service) addEvent(ctx context.Context, tx *sqlx.Tx, job Job, change, actor string, at time.Time) error {
	event, err := jobChangedEvent(job, change, actor, at)
	if err != nil {
		return err
	}
	return s.repository.AddOutbox(ctx, tx, event)
}

func authorize(ctx context.Context, tenantID string) (string, error) {
	principal, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return "", apperror.Unauthorized("authenticated actor is required")
	}
	if principal.Type != platformprincipal.TypeServiceAccount && principal.Type != platformprincipal.TypeSystem && (principal.TenantID == "" || principal.TenantID != clean(tenantID)) {
		return "", apperror.New(apperror.CodeForbidden, "tenant access denied", 403, nil)
	}
	return principal.ID, nil
}

func authorizeScope(ctx context.Context, applications appaccess.Verifier, tenantID, applicationID string) (string, error) {
	actor, err := authorize(ctx, tenantID)
	if err != nil {
		return "", err
	}
	tenantID, applicationID = clean(tenantID), clean(applicationID)
	if tenantID == "" || applicationID == "" {
		return "", apperror.Invalid("tenant_id and application_id are required", nil)
	}
	if err := applications.Verify(ctx, tenantID, applicationID); err != nil {
		if errors.Is(err, appaccess.ErrNotGranted) {
			return "", apperror.New(apperror.CodeForbidden, "application access denied", 403, err)
		}
		return "", apperror.Unavailable("application authorization unavailable", err)
	}
	return actor, nil
}

func (s *Service) authorizeScope(ctx context.Context, tenantID, applicationID string) (string, error) {
	return authorizeScope(ctx, s.applications, tenantID, applicationID)
}

func translate(err error) error {
	if err == nil {
		return nil
	}
	var appErr *apperror.Error
	if errors.As(err, &appErr) {
		return err
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return apperror.NotFound("import job not found")
	case errors.Is(err, ErrStaleVersion):
		return apperror.Conflict("import job version or state changed", err)
	default:
		return apperror.Internal(err)
	}
}

func validFormat(value string) bool {
	return value == FormatCSV || value == FormatJSONL || value == FormatXLSX
}
func clean(value string) string { return strings.TrimSpace(value) }
