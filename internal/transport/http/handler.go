package httptransport

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lihongjie0209/import-service/internal/apperror"
	"github.com/lihongjie0209/import-service/internal/buildinfo"
	"github.com/lihongjie0209/import-service/internal/health"
	"github.com/lihongjie0209/import-service/internal/importjob"
)

type Handler struct {
	logger  *slog.Logger
	health  *health.Service
	imports *importjob.Service
	catalog *importjob.Catalog
}

func NewHandler(healthService *health.Service, importService *importjob.Service, catalog *importjob.Catalog, logger *slog.Logger) *Handler {
	return &Handler{health: healthService, imports: importService, catalog: catalog, logger: logger}
}

type ListImportDatasetsRequest struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	Search        string `json:"search"`
	Page          int32  `json:"page"`
	PageSize      int32  `json:"page_size"`
}
type DescribeImportDatasetRequest struct {
	TenantID        string `json:"tenant_id"`
	ApplicationID   string `json:"application_id"`
	ProviderService string `json:"provider_service"`
	DatasetCode     string `json:"dataset_code"`
}
type ImportDatasetSummaryBody struct {
	ProviderService  string   `json:"provider_service"`
	Code             string   `json:"code"`
	Title            string   `json:"title"`
	Formats          []string `json:"formats"`
	MaxBatchSize     int32    `json:"max_batch_size"`
	SupportsDryRun   bool     `json:"supports_dry_run"`
	HealthyInstances int32    `json:"healthy_instances"`
}
type ImportDatasetPageBody struct {
	Items    []ImportDatasetSummaryBody `json:"items"`
	Total    int64                      `json:"total"`
	Page     int32                      `json:"page"`
	PageSize int32                      `json:"page_size"`
}
type ImportColumnBody struct {
	Key         string `json:"key"`
	Title       string `json:"title"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Example     string `json:"example"`
	Required    bool   `json:"required"`
	Sensitive   bool   `json:"sensitive"`
}
type ImportDatasetDescriptorBody struct {
	Code           string             `json:"code"`
	Title          string             `json:"title"`
	Columns        []ImportColumnBody `json:"columns"`
	Formats        []string           `json:"formats"`
	MaxBatchSize   int32              `json:"max_batch_size"`
	SupportsDryRun bool               `json:"supports_dry_run"`
}

type CreateImportRequest struct {
	TenantID        string `json:"tenant_id"`
	ApplicationID   string `json:"application_id"`
	DatasetCode     string `json:"dataset_code"`
	ProviderService string `json:"provider_service"`
	Format          string `json:"format"`
	Filename        string `json:"filename"`
	IdempotencyKey  string `json:"idempotency_key"`
}
type ImportSelector struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	ID            string `json:"id"`
}
type ImportJobBody struct {
	ID              string     `json:"id"`
	TenantID        string     `json:"tenant_id"`
	ApplicationID   string     `json:"application_id"`
	DatasetCode     string     `json:"dataset_code"`
	ProviderService string     `json:"provider_service"`
	Format          string     `json:"format"`
	Filename        string     `json:"filename"`
	Status          string     `json:"status"`
	SourceChecksum  string     `json:"source_checksum"`
	SourceBytes     int64      `json:"source_bytes"`
	TotalRows       int64      `json:"total_rows"`
	ValidRows       int64      `json:"valid_rows"`
	InvalidRows     int64      `json:"invalid_rows"`
	AppliedRows     int64      `json:"applied_rows"`
	ProgressPercent int32      `json:"progress_percent"`
	ErrorCode       string     `json:"error_code"`
	ErrorMessage    string     `json:"error_message"`
	UploadExpiresAt *time.Time `json:"upload_expires_at,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	ResultExpiresAt *time.Time `json:"result_expires_at,omitempty"`
	Version         int64      `json:"version"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	CreatedBy       string     `json:"created_by"`
	UpdatedBy       string     `json:"updated_by"`
}
type ImportPageBody struct {
	Items    []ImportJobBody `json:"items"`
	Total    int64           `json:"total"`
	Page     int32           `json:"page"`
	PageSize int32           `json:"page_size"`
}
type ImportMutationBody struct {
	Job       ImportJobBody `json:"job"`
	Duplicate bool          `json:"duplicate"`
}
type ImportUploadBody struct {
	ImportMutationBody
	UploadURL          string            `json:"upload_url"`
	UploadHeaders      map[string]string `json:"upload_headers"`
	UploadURLExpiresAt time.Time         `json:"upload_url_expires_at"`
}

func importJobBody(job importjob.Job) ImportJobBody {
	return ImportJobBody{ID: job.ID, TenantID: job.TenantID, ApplicationID: job.ApplicationID, DatasetCode: job.DatasetCode, ProviderService: job.ProviderService, Format: job.Format, Filename: job.Filename, Status: job.Status, SourceChecksum: job.SourceChecksum, SourceBytes: job.SourceBytes, TotalRows: job.TotalRows, ValidRows: job.ValidRows, InvalidRows: job.InvalidRows, AppliedRows: job.AppliedRows, ProgressPercent: job.ProgressPercent, ErrorCode: job.ErrorCode, ErrorMessage: job.ErrorMessage, UploadExpiresAt: job.UploadExpiresAt, StartedAt: job.StartedAt, CompletedAt: job.CompletedAt, ResultExpiresAt: job.ResultExpiresAt, Version: job.Version, CreatedAt: job.CreatedAt, UpdatedAt: job.UpdatedAt, CreatedBy: job.CreatedBy, UpdatedBy: job.UpdatedBy}
}

func importJobBodies(jobs []importjob.Job) []ImportJobBody {
	result := make([]ImportJobBody, len(jobs))
	for i := range jobs {
		result[i] = importJobBody(jobs[i])
	}
	return result
}

type ListImportsRequest struct {
	TenantID      string     `json:"tenant_id"`
	ApplicationID string     `json:"application_id"`
	Status        string     `json:"status"`
	DatasetCode   string     `json:"dataset_code"`
	CreatedFrom   *time.Time `json:"created_from"`
	CreatedTo     *time.Time `json:"created_to"`
	Page          int32      `json:"page"`
	PageSize      int32      `json:"page_size"`
}
type VersionedImportRequest struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	ID            string `json:"id"`
	Version       int64  `json:"version"`
}
type CompleteUploadRequest struct {
	TenantID       string `json:"tenant_id"`
	ApplicationID  string `json:"application_id"`
	ID             string `json:"id"`
	Version        int64  `json:"version"`
	SourceBytes    int64  `json:"source_bytes"`
	SourceChecksum string `json:"source_checksum"`
}
type RetryImportRequest struct {
	TenantID       string `json:"tenant_id"`
	ApplicationID  string `json:"application_id"`
	ID             string `json:"id"`
	Version        int64  `json:"version"`
	IdempotencyKey string `json:"idempotency_key"`
}
type ConfirmImportRequest RetryImportRequest
type ErrorReportRequest struct {
	TenantID      string `json:"tenant_id"`
	ApplicationID string `json:"application_id"`
	ID            string `json:"id"`
	TTLSeconds    int32  `json:"ttl_seconds"`
}

// @Summary Check process liveness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Router /live [post]
func (h *Handler) Live(c *gin.Context) { OK(c, h.health.Live()) }

// @Summary Check database and Redis readiness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Failure 503 {object} Response{body=health.Status}
// @Router /ready [post]
func (h *Handler) Ready(c *gin.Context) {
	status, ready := h.health.Ready(c.Request.Context())
	if !ready {
		c.JSON(503, Response{Code: apperror.CodeDependencyUnavailable, Message: "service is not ready", Body: status, RequestID: requestID(c)})
		return
	}
	OK(c, status)
}

// @Summary Return build and runtime version information
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=buildinfo.Info}
// @Router /api/v1/version [post]
func (h *Handler) Version(c *gin.Context) { OK(c, buildinfo.Current()) }

func (h *Handler) bind(c *gin.Context, target any) bool {
	if err := c.ShouldBindJSON(target); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid JSON request", err))
		return false
	}
	return true
}

// @Summary Search available import datasets
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ListImportDatasetsRequest true "Search and pagination"
// @Success 200 {object} Response{body=ImportDatasetPageBody}
// @Router /api/v1/imports/datasets/list [post]
func (h *Handler) ListImportDatasets(c *gin.Context) {
	var r ListImportDatasetsRequest
	if !h.bind(c, &r) {
		return
	}
	items, total, page, pageSize, err := h.catalog.List(c.Request.Context(), r.TenantID, r.ApplicationID, r.Search, r.Page, r.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	body := make([]ImportDatasetSummaryBody, len(items))
	for i, item := range items {
		body[i] = ImportDatasetSummaryBody{ProviderService: item.ProviderService, Code: item.Code, Title: item.Title, Formats: item.Formats, MaxBatchSize: item.MaxBatchSize, SupportsDryRun: item.SupportsDryRun, HealthyInstances: item.HealthyInstances}
	}
	OK(c, ImportDatasetPageBody{Items: body, Total: total, Page: page, PageSize: pageSize})
}

// @Summary Describe an available import dataset
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body DescribeImportDatasetRequest true "Provider and dataset"
// @Success 200 {object} Response{body=ImportDatasetDescriptorBody}
// @Router /api/v1/imports/datasets/describe [post]
func (h *Handler) DescribeImportDataset(c *gin.Context) {
	var r DescribeImportDatasetRequest
	if !h.bind(c, &r) {
		return
	}
	value, err := h.catalog.Describe(c.Request.Context(), r.TenantID, r.ApplicationID, r.ProviderService, r.DatasetCode)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	columns := make([]ImportColumnBody, len(value.Columns))
	for i, column := range value.Columns {
		columns[i] = ImportColumnBody{Key: column.Key, Title: column.Title, Type: column.Type, Description: column.Description, Example: column.Example, Required: column.Required, Sensitive: column.Sensitive}
	}
	OK(c, ImportDatasetDescriptorBody{Code: value.Code, Title: value.Title, Columns: columns, Formats: value.Formats, MaxBatchSize: value.MaxBatchSize, SupportsDryRun: value.SupportsDryRun})
}

// @Summary Create an import job and upload URL
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body CreateImportRequest true "Import definition"
// @Success 200 {object} Response{body=ImportUploadBody}
// @Router /api/v1/imports/create [post]
func (h *Handler) CreateImport(c *gin.Context) {
	var r CreateImportRequest
	if !h.bind(c, &r) {
		return
	}
	job, upload, duplicate, err := h.imports.Create(c.Request.Context(), importjob.CreateInput{TenantID: r.TenantID, ApplicationID: r.ApplicationID, DatasetCode: r.DatasetCode, ProviderService: r.ProviderService, Format: r.Format, Filename: r.Filename, IdempotencyKey: r.IdempotencyKey})
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, ImportUploadBody{ImportMutationBody: ImportMutationBody{Job: importJobBody(job), Duplicate: duplicate}, UploadURL: upload.URL.String(), UploadHeaders: upload.Headers, UploadURLExpiresAt: upload.ExpiresAt})
}

// @Summary Verify upload and enqueue validation
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body CompleteUploadRequest true "Uploaded object metadata"
// @Success 200 {object} Response{body=ImportJobBody}
// @Router /api/v1/imports/complete-upload [post]
func (h *Handler) CompleteUpload(c *gin.Context) {
	var r CompleteUploadRequest
	if !h.bind(c, &r) {
		return
	}
	job, err := h.imports.CompleteUpload(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, r.Version, r.SourceBytes, r.SourceChecksum)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, importJobBody(job))
}

// @Summary Get an import job
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ImportSelector true "Job selector"
// @Success 200 {object} Response{body=ImportJobBody}
// @Router /api/v1/imports/get [post]
func (h *Handler) GetImport(c *gin.Context) {
	var r ImportSelector
	if !h.bind(c, &r) {
		return
	}
	job, err := h.imports.Get(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, importJobBody(job))
}

// @Summary Search import jobs
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ListImportsRequest true "Filters and pagination"
// @Success 200 {object} Response{body=ImportPageBody}
// @Router /api/v1/imports/list [post]
func (h *Handler) ListImports(c *gin.Context) {
	var r ListImportsRequest
	if !h.bind(c, &r) {
		return
	}
	page, err := h.imports.List(c.Request.Context(), importjob.ListFilter{TenantID: r.TenantID, ApplicationID: r.ApplicationID, Status: r.Status, DatasetCode: r.DatasetCode, CreatedFrom: r.CreatedFrom, CreatedTo: r.CreatedTo, Page: r.Page, PageSize: r.PageSize})
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	number, size := normalizePage(r.Page, r.PageSize)
	OK(c, ImportPageBody{Items: importJobBodies(page.Items), Total: page.Total, Page: number, PageSize: size})
}

func normalizePage(page, size int32) (int32, int32) {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	} else if size > 100 {
		size = 100
	}
	return page, size
}

// @Summary Cancel an import with optimistic locking
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body VersionedImportRequest true "Job and current version"
// @Success 200 {object} Response
// @Router /api/v1/imports/cancel [post]
func (h *Handler) CancelImport(c *gin.Context) {
	var r VersionedImportRequest
	if !h.bind(c, &r) {
		return
	}
	job, err := h.imports.Cancel(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, r.Version)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, importJobBody(job))
}

// @Summary Upload a corrected file and retry validation
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body RetryImportRequest true "Retry request"
// @Success 200 {object} Response
// @Router /api/v1/imports/retry [post]
func (h *Handler) RetryImport(c *gin.Context) {
	var r RetryImportRequest
	if !h.bind(c, &r) {
		return
	}
	job, upload, duplicate, err := h.imports.Retry(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, r.Version, r.IdempotencyKey)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, ImportUploadBody{ImportMutationBody: ImportMutationBody{Job: importJobBody(job), Duplicate: duplicate}, UploadURL: upload.URL.String(), UploadHeaders: upload.Headers, UploadURLExpiresAt: upload.ExpiresAt})
}

// @Summary Confirm a fully validated import
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ConfirmImportRequest true "Confirmation request"
// @Success 200 {object} Response
// @Router /api/v1/imports/confirm [post]
func (h *Handler) ConfirmImport(c *gin.Context) {
	var r ConfirmImportRequest
	if !h.bind(c, &r) {
		return
	}
	job, duplicate, err := h.imports.Confirm(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, r.Version, r.IdempotencyKey)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, ImportMutationBody{Job: importJobBody(job), Duplicate: duplicate})
}

// @Summary Create a validation error report URL
// @Tags imports
// @Accept json
// @Produce json
// @Security Bearer
// @Security PSK
// @Param request body ErrorReportRequest true "Download request"
// @Success 200 {object} Response
// @Router /api/v1/imports/error-report [post]
func (h *Handler) DownloadErrorReport(c *gin.Context) {
	var r ErrorReportRequest
	if !h.bind(c, &r) {
		return
	}
	value, expires, job, err := h.imports.CreateErrorReportDownloadURL(c.Request.Context(), r.TenantID, r.ApplicationID, r.ID, time.Duration(r.TTLSeconds)*time.Second)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"url": value.String(), "expires_at": expires, "filename": job.Filename + ".errors.csv", "content_type": "text/csv; charset=utf-8"})
}
