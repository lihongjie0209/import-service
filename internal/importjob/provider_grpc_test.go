package importjob

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/lihongjie0209/import-service/internal/config"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"google.golang.org/grpc/metadata"
)

type validationStreamStub struct {
	sent      []*importv1.ValidateRowsRequest
	responses []*importv1.ValidateRowsResponse
	closed    int
}

func (s *validationStreamStub) Send(value *importv1.ValidateRowsRequest) error {
	s.sent = append(s.sent, value)
	return nil
}
func (s *validationStreamStub) Recv() (*importv1.ValidateRowsResponse, error) {
	if len(s.responses) == 0 {
		return nil, io.EOF
	}
	value := s.responses[0]
	s.responses = s.responses[1:]
	return value, nil
}
func (s *validationStreamStub) Header() (metadata.MD, error) { return nil, nil }
func (s *validationStreamStub) Trailer() metadata.MD         { return nil }
func (s *validationStreamStub) CloseSend() error             { s.closed++; return nil }
func (s *validationStreamStub) Context() context.Context     { return context.Background() }
func (s *validationStreamStub) SendMsg(any) error            { return nil }
func (s *validationStreamStub) RecvMsg(any) error            { return nil }

type applyStreamStub struct {
	sent      []*importv1.ApplyRowsRequest
	responses []*importv1.ApplyRowsResponse
	closed    int
}

func (s *applyStreamStub) Send(value *importv1.ApplyRowsRequest) error {
	s.sent = append(s.sent, value)
	return nil
}
func (s *applyStreamStub) Recv() (*importv1.ApplyRowsResponse, error) {
	if len(s.responses) == 0 {
		return nil, io.EOF
	}
	value := s.responses[0]
	s.responses = s.responses[1:]
	return value, nil
}
func (s *applyStreamStub) Header() (metadata.MD, error) { return nil, nil }
func (s *applyStreamStub) Trailer() metadata.MD         { return nil }
func (s *applyStreamStub) CloseSend() error             { s.closed++; return nil }
func (s *applyStreamStub) Context() context.Context     { return context.Background() }
func (s *applyStreamStub) SendMsg(any) error            { return nil }
func (s *applyStreamStub) RecvMsg(any) error            { return nil }

func TestValidationSessionReusesOneBidirectionalStream(t *testing.T) {
	t.Parallel()
	stream := &validationStreamStub{responses: []*importv1.ValidateRowsResponse{{BatchNumber: 1}, {BatchNumber: 2}}}
	session := &grpcValidationSession{provider: &GRPCProvider{}, tenant: "tenant-1", dataset: "identity.users", stream: stream}
	for batch := int64(1); batch <= 2; batch++ {
		if _, err := session.ValidateBatch(ValidateBatchRequest{TenantID: "tenant-1", DatasetCode: "identity.users", JobID: "job-1", BatchNumber: batch, Rows: []map[string]any{{"email": "user@example.com"}}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(stream.sent) != 2 || stream.closed != 0 {
		t.Fatalf("sent=%d closed=%d", len(stream.sent), stream.closed)
	}
	if err := session.Close(); err != nil || stream.closed != 1 {
		t.Fatalf("close count=%d err=%v", stream.closed, err)
	}
}

func TestValidationSessionReconnectsAndReplaysBatchAfterTransientFailure(t *testing.T) {
	t.Parallel()
	failed := &validationStreamStub{}
	recovered := &validationStreamStub{responses: []*importv1.ValidateRowsResponse{{BatchNumber: 7}}}
	provider := &GRPCProvider{config: config.ProviderClient{Retry: config.Retry{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}}}
	session := &grpcValidationSession{provider: provider, tenant: "tenant-1", dataset: "identity.users", ctx: t.Context(), stream: failed}
	reopens := 0
	session.open = func() error {
		reopens++
		session.stream = recovered
		return nil
	}
	result, err := session.ValidateBatch(ValidateBatchRequest{TenantID: "tenant-1", DatasetCode: "identity.users", JobID: "job-1", BatchNumber: 7, Rows: []map[string]any{{"email": "user@example.com"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Issues) != 0 || reopens != 1 || len(failed.sent) != 1 || failed.closed != 1 || len(recovered.sent) != 1 || recovered.sent[0].GetBatchNumber() != 7 {
		t.Fatalf("reopens=%d failed=%+v recovered=%+v result=%+v", reopens, failed, recovered, result)
	}
}

func TestApplySessionReconnectsWithStableIdempotencyKey(t *testing.T) {
	t.Parallel()
	failed := &applyStreamStub{}
	recovered := &applyStreamStub{responses: []*importv1.ApplyRowsResponse{{BatchNumber: 3, AppliedRows: 1}}}
	provider := &GRPCProvider{config: config.ProviderClient{Retry: config.Retry{MaxAttempts: 2, InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}}}
	session := &grpcApplySession{provider: provider, tenant: "tenant-1", dataset: "billing.plans", ctx: t.Context(), stream: failed}
	session.open = func() error {
		session.stream = recovered
		return nil
	}
	result, err := session.ApplyBatch(ApplyBatchRequest{TenantID: "tenant-1", DatasetCode: "billing.plans", JobID: "job-1", BatchNumber: 3, Rows: []map[string]any{{"code": "pro"}}, IdempotencyKey: "job-1:3"})
	if err != nil {
		t.Fatal(err)
	}
	if result.AppliedRows != 1 || len(failed.sent) != 1 || len(recovered.sent) != 1 || failed.sent[0].GetIdempotencyKey() != "job-1:3" || recovered.sent[0].GetIdempotencyKey() != "job-1:3" {
		t.Fatalf("failed=%+v recovered=%+v result=%+v", failed, recovered, result)
	}
}

func TestOpenProviderSessionRetriesDiscoveryStartupRace(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := openProviderSession(t.Context(), config.Retry{InitialBackoff: time.Nanosecond, MaxBackoff: time.Nanosecond}, 3, func() error {
		attempts++
		if attempts < 3 {
			return io.EOF
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}

func TestSupportsImportDatasetMetadata(t *testing.T) {
	t.Parallel()
	metadata := map[string]string{
		"platform.import.provider": "true",
		"platform.import.datasets": `[{"code":"identity.users","title":"Users","formats":["csv"],"max_batch_size":500,"supports_dry_run":true}]`,
	}
	if !supportsImportDataset(metadata, "identity.users") {
		t.Fatal("registered dataset was not accepted")
	}
	if supportsImportDataset(metadata, "billing.invoices") {
		t.Fatal("unregistered dataset was accepted")
	}
}

func TestSummarizeDatasetsDeduplicatesReplicasAndSearches(t *testing.T) {
	t.Parallel()
	metadata := map[string]string{
		"platform.import.provider": "true",
		"platform.import.datasets": `[{"code":"identity.users","title":"Users","formats":["csv"],"max_batch_size":500,"supports_dry_run":true}]`,
	}
	instances := []*registryv1.ServiceInstance{
		{ServiceName: "identity-service", InstanceId: "one", Metadata: metadata},
		{ServiceName: "identity-service", InstanceId: "two", Metadata: metadata},
		{ServiceName: "ignored-service", InstanceId: "invalid", Metadata: map[string]string{}},
	}
	values := summarizeDatasets(instances, "USERS")
	if len(values) != 1 || values[0].HealthyInstances != 2 || values[0].ProviderService != "identity-service" {
		t.Fatalf("values=%+v", values)
	}
	if values := summarizeDatasets(instances, "missing"); len(values) != 0 {
		t.Fatalf("unexpected search result=%+v", values)
	}
}

func TestValidateImportProviderTargetUsesDNSAllowlist(t *testing.T) {
	t.Parallel()
	if err := validateImportProviderTarget("identity-service.platform.svc.cluster.local:9090", []string{".platform.svc.cluster.local"}); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"127.0.0.1:9090", "localhost:9090", "evil.example.com:9090", "missing-port"} {
		if err := validateImportProviderTarget(target, []string{".platform.svc.cluster.local"}); err == nil {
			t.Fatalf("target %q accepted", target)
		}
	}
}
