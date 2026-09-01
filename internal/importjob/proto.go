package importjob

import (
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ToProto(value Job) *importv1.ImportJob {
	result := &importv1.ImportJob{Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, DatasetCode: value.DatasetCode, ProviderService: value.ProviderService, Format: value.Format, Filename: value.Filename, Status: value.Status, SourceObjectKey: value.SourceObjectKey, NormalizedObjectKey: value.NormalizedObjectKey, ErrorReportObjectKey: value.ErrorReportObjectKey, SourceChecksum: value.SourceChecksum, SourceBytes: value.SourceBytes, TotalRows: value.TotalRows, ValidRows: value.ValidRows, InvalidRows: value.InvalidRows, AppliedRows: value.AppliedRows, ProgressPercent: value.ProgressPercent, ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
	if value.UploadExpiresAt != nil {
		result.UploadExpiresAt = timestamppb.New(*value.UploadExpiresAt)
	}
	if value.StartedAt != nil {
		result.StartedAt = timestamppb.New(*value.StartedAt)
	}
	if value.CompletedAt != nil {
		result.CompletedAt = timestamppb.New(*value.CompletedAt)
	}
	if value.ResultExpiresAt != nil {
		result.ResultExpiresAt = timestamppb.New(*value.ResultExpiresAt)
	}
	return result
}
