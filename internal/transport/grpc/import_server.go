package grpctransport

import (
	"context"
	"errors"
	"time"

	"github.com/lihongjie0209/import-service/internal/apperror"
	"github.com/lihongjie0209/import-service/internal/importjob"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type importServer struct {
	importv1.UnimplementedImportServiceServer
	service *importjob.Service
}

func (s *importServer) CreateImportJob(ctx context.Context, r *importv1.CreateImportJobRequest) (*importv1.CreateImportJobResponse, error) {
	job, upload, duplicate, err := s.service.Create(ctx, importjob.CreateInput{TenantID: r.GetTenantId(), DatasetCode: r.GetDatasetCode(), ProviderService: r.GetProviderService(), Format: r.GetFormat(), Filename: r.GetFilename(), IdempotencyKey: r.GetIdempotencyKey()})
	response := &importv1.CreateImportJobResponse{Job: importjob.ToProto(job), UploadHeaders: upload.Headers, Duplicate: duplicate}
	if upload.URL != nil {
		response.UploadUrl, response.UploadUrlExpiresAt = upload.URL.String(), timestamppb.New(upload.ExpiresAt)
	}
	return response, importError(err)
}

func (s *importServer) CompleteUpload(ctx context.Context, r *importv1.CompleteUploadRequest) (*importv1.CompleteUploadResponse, error) {
	job, err := s.service.CompleteUpload(ctx, r.GetTenantId(), r.GetId(), r.GetVersion(), r.GetSourceBytes(), r.GetSourceChecksum())
	return &importv1.CompleteUploadResponse{Job: importjob.ToProto(job)}, importError(err)
}

func (s *importServer) GetImportJob(ctx context.Context, r *importv1.GetImportJobRequest) (*importv1.GetImportJobResponse, error) {
	job, err := s.service.Get(ctx, r.GetTenantId(), r.GetId())
	return &importv1.GetImportJobResponse{Job: importjob.ToProto(job)}, importError(err)
}

func (s *importServer) ListImportJobs(ctx context.Context, r *importv1.ListImportJobsRequest) (*importv1.ListImportJobsResponse, error) {
	page, size := int32(0), int32(0)
	if r.GetPage() != nil {
		page, size = int32(r.GetPage().GetPage()), int32(r.GetPage().GetPageSize())
	}
	filter := importjob.ListFilter{TenantID: r.GetTenantId(), Status: r.GetStatus(), DatasetCode: r.GetDatasetCode(), Page: page, PageSize: size}
	if r.GetCreatedFrom() != nil {
		value := r.GetCreatedFrom().AsTime()
		filter.CreatedFrom = &value
	}
	if r.GetCreatedTo() != nil {
		value := r.GetCreatedTo().AsTime()
		filter.CreatedTo = &value
	}
	result, err := s.service.List(ctx, filter)
	items := make([]*importv1.ImportJob, len(result.Items))
	for i := range result.Items {
		items[i] = importjob.ToProto(result.Items[i])
	}
	return &importv1.ListImportJobsResponse{Jobs: items, Page: &commonv1.PageResult{Total: uint64(result.Total), Page: uint32(max(page, 1)), PageSize: uint32(normalizedPageSize(size))}}, importError(err)
}

func (s *importServer) CancelImportJob(ctx context.Context, r *importv1.CancelImportJobRequest) (*importv1.CancelImportJobResponse, error) {
	job, err := s.service.Cancel(ctx, r.GetTenantId(), r.GetId(), r.GetVersion())
	return &importv1.CancelImportJobResponse{Job: importjob.ToProto(job)}, importError(err)
}

func (s *importServer) RetryImportJob(ctx context.Context, r *importv1.RetryImportJobRequest) (*importv1.RetryImportJobResponse, error) {
	job, upload, duplicate, err := s.service.Retry(ctx, r.GetTenantId(), r.GetId(), r.GetVersion(), r.GetIdempotencyKey())
	response := &importv1.RetryImportJobResponse{Job: importjob.ToProto(job), UploadHeaders: upload.Headers, Duplicate: duplicate}
	if upload.URL != nil {
		response.UploadUrl, response.UploadUrlExpiresAt = upload.URL.String(), timestamppb.New(upload.ExpiresAt)
	}
	return response, importError(err)
}

func (s *importServer) ConfirmImportJob(ctx context.Context, r *importv1.ConfirmImportJobRequest) (*importv1.ConfirmImportJobResponse, error) {
	job, duplicate, err := s.service.Confirm(ctx, r.GetTenantId(), r.GetId(), r.GetVersion(), r.GetIdempotencyKey())
	return &importv1.ConfirmImportJobResponse{Job: importjob.ToProto(job), Duplicate: duplicate}, importError(err)
}

func (s *importServer) CreateErrorReportDownloadURL(ctx context.Context, r *importv1.CreateErrorReportDownloadURLRequest) (*importv1.CreateErrorReportDownloadURLResponse, error) {
	value, expires, job, err := s.service.CreateErrorReportDownloadURL(ctx, r.GetTenantId(), r.GetId(), time.Duration(r.GetTtlSeconds())*time.Second)
	response := &importv1.CreateErrorReportDownloadURLResponse{}
	if err == nil {
		response.Url, response.ExpiresAt = value.String(), timestamppb.New(expires)
		response.Filename, response.ContentType = job.Filename+".errors.csv", "text/csv; charset=utf-8"
	}
	return response, importError(err)
}

func normalizedPageSize(value int32) int32 {
	if value < 1 {
		return 20
	}
	if value > 100 {
		return 100
	}
	return value
}

func importError(err error) error {
	if err == nil {
		return nil
	}
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		return status.Error(codes.Internal, "internal server error")
	}
	mapping := map[int]codes.Code{apperror.CodeInvalidArgument: codes.InvalidArgument, apperror.CodeNotFound: codes.NotFound, apperror.CodeUnauthorized: codes.Unauthenticated, apperror.CodeForbidden: codes.PermissionDenied, apperror.CodeConflict: codes.Aborted, apperror.CodeDependencyUnavailable: codes.Unavailable, apperror.CodeRequestTimeout: codes.DeadlineExceeded, apperror.CodeTooManyRequests: codes.ResourceExhausted}
	code, ok := mapping[appErr.Code]
	if !ok {
		code = codes.Internal
	}
	return status.Error(code, appErr.Message)
}
