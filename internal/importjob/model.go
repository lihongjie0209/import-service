package importjob

import "time"

const (
	StatusUploading        = "uploading"
	StatusQueued           = "queued"
	StatusValidating       = "validating"
	StatusValidationFailed = "validation_failed"
	StatusReady            = "ready"
	StatusApplyQueued      = "apply_queued"
	StatusApplying         = "applying"
	StatusSucceeded        = "succeeded"
	StatusFailed           = "failed"
	StatusCanceled         = "canceled"
	StatusExpired          = "expired"

	FormatCSV   = "csv"
	FormatJSONL = "jsonl"
	FormatXLSX  = "xlsx"
)

type Job struct {
	ID                   string     `db:"id" json:"id"`
	TenantID             string     `db:"tenant_id" json:"tenant_id"`
	ApplicationID        string     `db:"application_id" json:"application_id"`
	DatasetCode          string     `db:"dataset_code" json:"dataset_code"`
	ProviderService      string     `db:"provider_service" json:"provider_service"`
	Format               string     `db:"format" json:"format"`
	Filename             string     `db:"filename" json:"filename"`
	Status               string     `db:"status" json:"status"`
	SourceObjectKey      string     `db:"source_object_key" json:"source_object_key"`
	NormalizedObjectKey  string     `db:"normalized_object_key" json:"normalized_object_key"`
	ErrorReportObjectKey string     `db:"error_report_object_key" json:"error_report_object_key"`
	SourceChecksum       string     `db:"source_checksum" json:"source_checksum"`
	SourceBytes          int64      `db:"source_bytes" json:"source_bytes"`
	TotalRows            int64      `db:"total_rows" json:"total_rows"`
	ValidRows            int64      `db:"valid_rows" json:"valid_rows"`
	InvalidRows          int64      `db:"invalid_rows" json:"invalid_rows"`
	AppliedRows          int64      `db:"applied_rows" json:"applied_rows"`
	ProgressPercent      int32      `db:"progress_percent" json:"progress_percent"`
	ErrorCode            string     `db:"error_code" json:"error_code"`
	ErrorMessage         string     `db:"error_message" json:"error_message"`
	IdempotencyKey       string     `db:"idempotency_key" json:"-"`
	ConfirmKey           string     `db:"confirm_key" json:"-"`
	UploadExpiresAt      *time.Time `db:"upload_expires_at" json:"upload_expires_at,omitempty"`
	StartedAt            *time.Time `db:"started_at" json:"started_at,omitempty"`
	CompletedAt          *time.Time `db:"completed_at" json:"completed_at,omitempty"`
	ResultExpiresAt      *time.Time `db:"result_expires_at" json:"result_expires_at,omitempty"`
	Version              int64      `db:"version" json:"version"`
	CreatedAt            time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt            time.Time  `db:"updated_at" json:"updated_at"`
	CreatedBy            string     `db:"created_by" json:"created_by"`
	UpdatedBy            string     `db:"updated_by" json:"updated_by"`
}

type CreateInput struct {
	TenantID, ApplicationID, DatasetCode, ProviderService, Format, Filename, IdempotencyKey string
}

type ListFilter struct {
	TenantID, ApplicationID, Status, DatasetCode string
	CreatedFrom, CreatedTo                       *time.Time
	Page, PageSize                               int32
}

type Page struct {
	Items []Job
	Total int64
}

type OutboxEvent struct {
	ID, Subject                       string
	Envelope                          []byte
	AvailableAt, CreatedAt, UpdatedAt time.Time
	CreatedBy, UpdatedBy              string
}
