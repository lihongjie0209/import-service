package importjob

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lihongjie0209/import-service/internal/config"
	"github.com/lihongjie0209/import-service/internal/grpcclient"
	"github.com/lihongjie0209/import-service/internal/observability"
	"github.com/lihongjie0209/import-service/internal/outbound"
	"github.com/lihongjie0209/microservice-platform-go/importprovider"
	"github.com/lihongjie0209/microservice-platform-go/serviceregistry"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func (p *GRPCProvider) ListDatasets(_ context.Context, search string, page, pageSize int32) ([]DatasetSummary, int64, error) {
	if p.discovery == nil {
		return []DatasetSummary{}, 0, nil
	}
	instances, err := p.discovery.Instances()
	if err != nil {
		return nil, 0, err
	}
	values := summarizeDatasets(instances, search)
	page, pageSize = normalizeCatalogPage(page, pageSize)
	start := int((page - 1) * pageSize)
	if start >= len(values) {
		return []DatasetSummary{}, int64(len(values)), nil
	}
	end := min(start+int(pageSize), len(values))
	return values[start:end], int64(len(values)), nil
}

func (p *GRPCProvider) DescribeDataset(ctx context.Context, tenantID, service, dataset string) (DatasetDescriptor, error) {
	client, instance, err := p.client(service, dataset)
	if err != nil {
		return DatasetDescriptor{}, err
	}
	response, err := client.DescribeImportDataset(ctx, &importv1.DescribeImportDatasetRequest{TenantId: tenantID, DatasetCode: dataset})
	if err != nil {
		p.failure(instance)
		return DatasetDescriptor{}, err
	}
	value := response.GetDataset()
	if value == nil || value.GetCode() != dataset {
		p.failure(instance)
		return DatasetDescriptor{}, ErrInvalidProviderResponse
	}
	columns := make([]ImportColumn, len(value.GetColumns()))
	for i, column := range value.GetColumns() {
		columns[i] = ImportColumn{Key: column.GetKey(), Title: column.GetTitle(), Type: column.GetType(), Required: column.GetRequired(), Description: column.GetDescription(), Example: column.GetExample(), Sensitive: column.GetSensitive()}
	}
	p.success(instance)
	return DatasetDescriptor{Code: value.GetCode(), Title: value.GetTitle(), Columns: columns, Formats: value.GetFormats(), MaxBatchSize: value.GetMaxBatchSize(), SupportsDryRun: value.GetSupportsDryRun()}, nil
}

func summarizeDatasets(instances []*registryv1.ServiceInstance, search string) []DatasetSummary {
	type key struct{ service, dataset string }
	result := map[key]DatasetSummary{}
	search = strings.ToLower(strings.TrimSpace(search))
	for _, instance := range instances {
		datasets, err := importprovider.ParseMetadata(instance.GetMetadata())
		if err != nil {
			continue
		}
		for _, dataset := range datasets {
			if search != "" && !strings.Contains(strings.ToLower(dataset.Code+" "+dataset.Title+" "+instance.GetServiceName()), search) {
				continue
			}
			currentKey := key{service: instance.GetServiceName(), dataset: dataset.Code}
			current := result[currentKey]
			if current.HealthyInstances == 0 {
				current = DatasetSummary{ProviderService: instance.GetServiceName(), Code: dataset.Code, Title: dataset.Title, Formats: dataset.Formats, MaxBatchSize: dataset.MaxBatchSize, SupportsDryRun: dataset.SupportsDryRun}
			}
			current.HealthyInstances++
			result[currentKey] = current
		}
	}
	values := make([]DatasetSummary, 0, len(result))
	for _, value := range result {
		values = append(values, value)
	}
	slices.SortFunc(values, func(a, b DatasetSummary) int {
		if compared := strings.Compare(a.ProviderService, b.ProviderService); compared != 0 {
			return compared
		}
		return strings.Compare(a.Code, b.Code)
	})
	return values
}

func normalizeCatalogPage(page, pageSize int32) (int32, int32) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	} else if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}

type providerConnection struct {
	target     string
	connection *grpc.ClientConn
}

type GRPCProvider struct {
	static             *outbound.Registry
	config             config.ProviderClient
	discovery          *serviceregistry.Discovery
	registryConnection *grpc.ClientConn
	mu                 sync.Mutex
	connections        map[string]providerConnection
	cursor             atomic.Uint64
	dial               func(grpcclient.Config) (*grpc.ClientConn, error)
	metrics            *observability.Metrics
}

func NewProvider(lifecycle fx.Lifecycle, cfg config.Config, static *outbound.Registry, metrics *observability.Metrics) (Provider, error) {
	provider := &GRPCProvider{static: static, config: cfg.ProviderClient, connections: map[string]providerConnection{}, dial: grpcclient.Dial, metrics: metrics}
	if cfg.ServiceRegistry.Enabled {
		connection, err := grpcclient.Dial(grpcclient.Config{
			Name: "service-registry-service", Target: cfg.ServiceRegistry.Target, Timeout: 3 * time.Second, PSK: cfg.ServiceRegistry.PSK,
			TLS: grpcclient.TLSConfig{Enabled: cfg.ServiceRegistry.TLS.Enabled, ServerName: cfg.ServiceRegistry.TLS.ServerName, CAFile: cfg.ServiceRegistry.TLS.CAFile, CertFile: cfg.ServiceRegistry.TLS.CertFile, KeyFile: cfg.ServiceRegistry.TLS.KeyFile, AllowInsecureToken: cfg.ServiceRegistry.AllowInsecure},
		})
		if err != nil {
			return nil, fmt.Errorf("dial service registry: %w", err)
		}
		provider.registryConnection = connection
		discovery, err := serviceregistry.NewDiscovery(
			registryv1.NewRegistryServiceClient(connection),
			serviceregistry.DiscoveryConfig{Selector: map[string]string{importprovider.ProviderMetadataKey: "true"}, MaxStale: cfg.ServiceRegistry.MaxStale, SnapshotStore: serviceregistry.FileSnapshotStore{Directory: cfg.ServiceRegistry.SnapshotDirectory}},
		)
		if err != nil {
			_ = connection.Close()
			return nil, err
		}
		provider.discovery = discovery
		var cancel context.CancelFunc
		lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				runCtx, stop := context.WithCancel(context.Background())
				cancel = stop
				go func() { _ = discovery.Run(runCtx) }()
				return nil
			},
			OnStop: func(context.Context) error {
				if cancel != nil {
					cancel()
				}
				return nil
			},
		})
	}
	lifecycle.Append(fx.StopHook(func() error { return provider.Close() }))
	return provider, nil
}

type grpcValidationSession struct {
	provider *GRPCProvider
	instance *registryv1.ServiceInstance
	tenant   string
	dataset  string
	stream   grpc.BidiStreamingClient[importv1.ValidateRowsRequest, importv1.ValidateRowsResponse]
}

func (p *GRPCProvider) OpenValidation(ctx context.Context, service, tenant, dataset string) (ValidationSession, error) {
	client, instance, err := p.client(service, dataset)
	if err != nil {
		return nil, err
	}
	stream, err := client.ValidateRows(ctx)
	if err != nil {
		p.failure(instance)
		return nil, err
	}
	return &grpcValidationSession{provider: p, instance: instance, tenant: tenant, dataset: dataset, stream: stream}, nil
}

func (s *grpcValidationSession) ValidateBatch(request ValidateBatchRequest) (ValidateBatchResult, error) {
	if request.TenantID != s.tenant || request.DatasetCode != s.dataset {
		return ValidateBatchResult{}, ErrInvalidProviderResponse
	}
	rows, err := protoRows(request.Rows)
	if err != nil {
		return ValidateBatchResult{}, err
	}
	if err := s.stream.Send(&importv1.ValidateRowsRequest{TenantId: request.TenantID, DatasetCode: request.DatasetCode, JobId: request.JobID, BatchNumber: request.BatchNumber, FirstRowNumber: request.FirstRowNumber, Rows: rows}); err != nil {
		s.provider.failure(s.instance)
		return ValidateBatchResult{}, err
	}
	response, err := s.stream.Recv()
	if err != nil {
		s.provider.failure(s.instance)
		return ValidateBatchResult{}, err
	}
	if response.GetBatchNumber() != request.BatchNumber {
		s.provider.failure(s.instance)
		return ValidateBatchResult{}, ErrInvalidProviderResponse
	}
	s.provider.success(s.instance)
	return validationResult(response), nil
}

func (s *grpcValidationSession) Close() error {
	err := s.stream.CloseSend()
	if err != nil {
		s.provider.failure(s.instance)
	}
	return err
}

type grpcApplySession struct {
	provider *GRPCProvider
	instance *registryv1.ServiceInstance
	tenant   string
	dataset  string
	stream   grpc.BidiStreamingClient[importv1.ApplyRowsRequest, importv1.ApplyRowsResponse]
}

func (p *GRPCProvider) OpenApply(ctx context.Context, service, tenant, dataset string) (ApplySession, error) {
	client, instance, err := p.client(service, dataset)
	if err != nil {
		return nil, err
	}
	stream, err := client.ApplyRows(ctx)
	if err != nil {
		p.failure(instance)
		return nil, err
	}
	return &grpcApplySession{provider: p, instance: instance, tenant: tenant, dataset: dataset, stream: stream}, nil
}

func (s *grpcApplySession) ApplyBatch(request ApplyBatchRequest) (ApplyBatchResult, error) {
	if request.TenantID != s.tenant || request.DatasetCode != s.dataset {
		return ApplyBatchResult{}, ErrInvalidProviderResponse
	}
	rows, err := protoRows(request.Rows)
	if err != nil {
		return ApplyBatchResult{}, err
	}
	if err := s.stream.Send(&importv1.ApplyRowsRequest{TenantId: request.TenantID, DatasetCode: request.DatasetCode, JobId: request.JobID, BatchNumber: request.BatchNumber, Rows: rows, IdempotencyKey: request.IdempotencyKey}); err != nil {
		s.provider.failure(s.instance)
		return ApplyBatchResult{}, err
	}
	response, err := s.stream.Recv()
	if err != nil {
		s.provider.failure(s.instance)
		return ApplyBatchResult{}, err
	}
	if response.GetBatchNumber() != request.BatchNumber {
		s.provider.failure(s.instance)
		return ApplyBatchResult{}, ErrInvalidProviderResponse
	}
	issues := make([]RowIssue, len(response.GetIssues()))
	for i, value := range response.GetIssues() {
		issues[i] = RowIssue{RowNumber: value.GetRowNumber(), ColumnKey: value.GetColumnKey(), Code: value.GetCode(), Message: value.GetMessage()}
	}
	s.provider.success(s.instance)
	return ApplyBatchResult{AppliedRows: response.GetAppliedRows(), Issues: issues}, nil
}

func (s *grpcApplySession) Close() error {
	err := s.stream.CloseSend()
	if err != nil {
		s.provider.failure(s.instance)
	}
	return err
}

func (p *GRPCProvider) client(service, dataset string) (importv1.ImportProviderServiceClient, *registryv1.ServiceInstance, error) {
	if p.discovery == nil {
		connection, ok := p.static.GRPC(service)
		if !ok {
			return nil, nil, fmt.Errorf("import provider %q is not configured", service)
		}
		return importv1.NewImportProviderServiceClient(connection), nil, nil
	}
	instances, err := p.discovery.Instances()
	if err != nil {
		return nil, nil, err
	}
	candidates := make([]*registryv1.ServiceInstance, 0)
	for _, instance := range instances {
		if instance.GetServiceName() == service && supportsImportDataset(instance.Metadata, dataset) {
			candidates = append(candidates, instance)
		}
	}
	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("import provider %q with dataset %q is not registered", service, dataset)
	}
	instance := candidates[(p.cursor.Add(1)-1)%uint64(len(candidates))]
	target := strings.TrimPrefix(strings.TrimPrefix(instance.GetEndpoint(), "grpc://"), "grpcs://")
	if err := validateImportProviderTarget(target, p.config.AllowedDNSSuffixes); err != nil {
		return nil, nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if current, ok := p.connections[instance.GetInstanceId()]; ok && current.target == target {
		return importv1.NewImportProviderServiceClient(current.connection), instance, nil
	}
	if current, ok := p.connections[instance.GetInstanceId()]; ok {
		_ = current.connection.Close()
		delete(p.connections, instance.GetInstanceId())
	}
	connection, err := p.dial(grpcclient.Config{
		Name: service, Target: target, Timeout: p.config.Timeout, PSK: p.config.PSK, Retry: p.config.Retry, Breaker: p.config.Breaker, Metrics: p.metrics,
		TLS: grpcclient.TLSConfig{Enabled: p.config.TLS.Enabled, ServerName: p.config.TLS.ServerName, CAFile: p.config.TLS.CAFile, CertFile: p.config.TLS.CertFile, KeyFile: p.config.TLS.KeyFile, AllowInsecureToken: p.config.AllowInsecure},
	})
	if err != nil {
		return nil, nil, err
	}
	p.connections[instance.GetInstanceId()] = providerConnection{target: target, connection: connection}
	return importv1.NewImportProviderServiceClient(connection), instance, nil
}

func supportsImportDataset(metadata map[string]string, dataset string) bool {
	values, err := importprovider.ParseMetadata(metadata)
	if err != nil {
		return false
	}
	for _, value := range values {
		if value.Code == dataset {
			return true
		}
	}
	return false
}

func validateImportProviderTarget(target string, suffixes []string) error {
	value := strings.TrimPrefix(target, "dns:///")
	host, port, err := net.SplitHostPort(value)
	if err != nil || host == "" || port == "" || net.ParseIP(host) != nil || strings.EqualFold(host, "localhost") {
		return errors.New("provider target must be a DNS name with port")
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, suffix := range suffixes {
		suffix = strings.ToLower(strings.TrimSpace(suffix))
		if suffix != "" && (host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix)) {
			return nil
		}
	}
	return fmt.Errorf("provider host %q is outside allowed DNS suffixes", host)
}

func (p *GRPCProvider) failure(instance *registryv1.ServiceInstance) {
	if p.discovery != nil && instance != nil {
		p.discovery.ReportFailure(instance.GetInstanceId(), p.config.FailureCooldown)
	}
}

func (p *GRPCProvider) success(instance *registryv1.ServiceInstance) {
	if p.discovery != nil && instance != nil {
		p.discovery.ReportSuccess(instance.GetInstanceId())
	}
}

func (p *GRPCProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var result error
	for id, value := range p.connections {
		if err := value.connection.Close(); err != nil && result == nil {
			result = fmt.Errorf("close provider %s: %w", id, err)
		}
	}
	p.connections = map[string]providerConnection{}
	if p.registryConnection != nil {
		if err := p.registryConnection.Close(); err != nil && result == nil {
			result = err
		}
	}
	return result
}

func protoRows(values []map[string]any) ([]*structpb.Struct, error) {
	rows := make([]*structpb.Struct, len(values))
	for i, value := range values {
		row, err := structpb.NewStruct(value)
		if err != nil {
			return nil, fmt.Errorf("encode provider row: %w", err)
		}
		rows[i] = row
	}
	return rows, nil
}

func validationResult(response *importv1.ValidateRowsResponse) ValidateBatchResult {
	rows := make([]map[string]any, len(response.GetNormalizedRows()))
	for i, value := range response.GetNormalizedRows() {
		rows[i] = value.AsMap()
	}
	issues := make([]RowIssue, len(response.GetIssues()))
	for i, value := range response.GetIssues() {
		issues[i] = RowIssue{RowNumber: value.GetRowNumber(), ColumnKey: value.GetColumnKey(), Code: value.GetCode(), Message: value.GetMessage()}
	}
	return ValidateBatchResult{NormalizedRows: rows, Issues: issues}
}
