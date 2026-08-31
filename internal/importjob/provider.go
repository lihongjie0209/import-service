package importjob

import "context"

type DatasetSummary struct {
	ProviderService  string   `json:"provider_service"`
	Code             string   `json:"code"`
	Title            string   `json:"title"`
	Formats          []string `json:"formats"`
	MaxBatchSize     int32    `json:"max_batch_size"`
	SupportsDryRun   bool     `json:"supports_dry_run"`
	HealthyInstances int32    `json:"healthy_instances"`
}

type ImportColumn struct {
	Key, Title, Type, Description, Example string
	Required, Sensitive                    bool
}

type DatasetDescriptor struct {
	Code, Title    string
	Columns        []ImportColumn
	Formats        []string
	MaxBatchSize   int32
	SupportsDryRun bool
}

type RowIssue struct {
	RowNumber int64
	ColumnKey string
	Code      string
	Message   string
}

type ValidateBatchRequest struct {
	TenantID, DatasetCode, JobID string
	BatchNumber, FirstRowNumber  int64
	Rows                         []map[string]any
}
type ValidateBatchResult struct {
	NormalizedRows []map[string]any
	Issues         []RowIssue
}
type ApplyBatchRequest struct {
	TenantID, DatasetCode, JobID, IdempotencyKey string
	BatchNumber                                  int64
	Rows                                         []map[string]any
}
type ApplyBatchResult struct {
	AppliedRows int64
	Issues      []RowIssue
}

type ValidationSession interface {
	ValidateBatch(ValidateBatchRequest) (ValidateBatchResult, error)
	Close() error
}

type ApplySession interface {
	ApplyBatch(ApplyBatchRequest) (ApplyBatchResult, error)
	Close() error
}

type Provider interface {
	ListDatasets(context.Context, string, int32, int32) ([]DatasetSummary, int64, error)
	DescribeDataset(context.Context, string, string, string) (DatasetDescriptor, error)
	OpenValidation(context.Context, string, string, string) (ValidationSession, error)
	OpenApply(context.Context, string, string, string) (ApplySession, error)
}
