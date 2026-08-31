package importjob

import "context"

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

type Provider interface {
	ValidateBatch(context.Context, string, ValidateBatchRequest) (ValidateBatchResult, error)
	ApplyBatch(context.Context, string, ApplyBatchRequest) (ApplyBatchResult, error)
}
