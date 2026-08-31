//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lihongjie0209/import-service/internal/app"
	"github.com/lihongjie0209/import-service/internal/config"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAsynchronousImportThroughJetStreamAndMinIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	postgresContainer, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"), postgres.BasicWaitStrategies(), postgres.WithSQLDriver("pgx"))
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, postgresContainer)
	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	natsURL := startImportNATS(t, ctx)
	endpoint, accessKey, secretKey := startImportMinIO(t, ctx)
	admin, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.MakeBucket(ctx, "platform-imports", minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	provider := &testImportProvider{appliedKeys: map[string]struct{}{}}
	providerAddress := startImportProvider(t, provider)
	httpAddress, grpcAddress := freeAddress(t), freeAddress(t)
	migrationPath, _ := filepath.Abs(filepath.Join("..", "migrations", "postgres"))
	const psk = "integration-import-psk-0000000000000000"
	cfg := config.Config{
		Runtime: config.Runtime{ActiveProfile: "integration"}, App: config.App{Name: "import-service", Env: "integration", ShutdownTimeout: 10 * time.Second},
		HTTP: config.HTTP{Address: httpAddress, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: time.Minute, RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20},
		GRPC: config.GRPC{Enabled: true, Address: grpcAddress, MaxReceiveBytes: 4 << 20}, Log: config.Log{Level: "error", Format: "json", File: filepath.Join(t.TempDir(), "app.log"), MaxSizeMB: 1, MaxBackups: 1, MaxAgeDays: 1},
		Database:  config.Database{Enabled: true, Type: "postgres", DSN: dsn, MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, PingTimeout: 10 * time.Second},
		Migration: config.Migration{AutoUp: true, Path: migrationPath, DatabaseURL: dsn, Table: "async_import_schema_migrations"}, Health: config.Health{DatabaseTimeout: 2 * time.Second, RedisTimeout: 2 * time.Second},
		JWT: config.JWT{Issuer: "integration", Secret: psk, TTL: time.Hour}, Auth: config.Auth{PSK: config.PSK{Enabled: true, Key: psk, HTTPPaths: []string{"/api/v1/imports/*"}}, SkipHTTPPaths: []string{"/api/v1/version"}, SkipGRPCMethods: []string{"/grpc.health.v1.Health/*"}},
		Cron: config.Cron{Enabled: false, Timezone: "UTC"}, EventBus: config.EventBus{Enabled: true, URLs: []string{natsURL}, StreamName: "PLATFORM_EVENTS", Subjects: []string{"platform.>"}, Storage: "memory", MaxAge: time.Hour, DuplicateWindow: time.Minute, ConnectTimeout: 10 * time.Second, ReconnectWait: time.Second, PublishTimeout: 5 * time.Second, ConsumerAckWait: 2 * time.Minute, ConsumerMaxDeliver: 3, DispatchInterval: 20 * time.Millisecond, DispatchBatchSize: 10, DispatchLease: time.Minute, DispatchRetryDelay: 100 * time.Millisecond},
		Import: config.Import{UploadTTL: time.Minute, ResultTTL: time.Hour, JobTimeout: time.Minute, BatchSize: 1, MaxRows: 100, MaxBytes: 1 << 20}, ObjectStorage: config.ObjectStorage{Enabled: true, Endpoint: endpoint, AccessKey: accessKey, SecretKey: secretKey, Bucket: "platform-imports", PresignTTL: time.Minute},
		ProviderClient: config.ProviderClient{Retry: config.Retry{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}},
		Outbound:       config.Outbound{GRPC: map[string]config.GRPCUpstream{"test-provider": {Target: providerAddress, Timeout: 5 * time.Second, Retry: config.Retry{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}}}},
	}
	application := app.New(cfg)
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = application.Stop(stopCtx)
	})
	baseURL := "http://" + httpAddress
	body, statusCode := postJSONBody(t, baseURL+"/api/v1/imports/create", "PSK "+psk, "", `{"tenant_id":"tenant-1","dataset_code":"test.rows","provider_service":"test-provider","format":"csv","filename":"rows.csv","idempotency_key":"async-import-1"}`)
	if statusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", statusCode, body)
	}
	var created envelopeBody[struct {
		Job struct {
			ID      string `json:"id"`
			Version int64  `json:"version"`
		} `json:"job"`
		UploadURL     string            `json:"upload_url"`
		UploadHeaders map[string]string `json:"upload_headers"`
	}]
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	source := []byte("id,name\n1,Alice\n2,Bob\n")
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, created.Body.UploadURL, bytes.NewReader(source))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range created.Body.UploadHeaders {
		request.Header.Set(key, value)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("upload status=%d", response.StatusCode)
	}
	digest := sha256.Sum256(source)
	completeBody, statusCode := postJSONBody(t, baseURL+"/api/v1/imports/complete-upload", "PSK "+psk, "", `{"tenant_id":"tenant-1","id":"`+created.Body.Job.ID+`","version":1,"source_bytes":`+strconv.Itoa(len(source))+`,"source_checksum":"`+hex.EncodeToString(digest[:])+`"}`)
	if statusCode != http.StatusOK {
		t.Fatalf("complete status=%d body=%s", statusCode, completeBody)
	}
	ready := waitImportJob(t, baseURL, psk, created.Body.Job.ID, "ready")
	confirmBody, statusCode := postJSONBody(t, baseURL+"/api/v1/imports/confirm", "PSK "+psk, "", `{"tenant_id":"tenant-1","id":"`+created.Body.Job.ID+`","version":`+strconv.FormatInt(ready.Version, 10)+`,"idempotency_key":"confirm-1"}`)
	if statusCode != http.StatusOK {
		t.Fatalf("confirm status=%d body=%s", statusCode, confirmBody)
	}
	waitImportJob(t, baseURL, psk, created.Body.Job.ID, "succeeded")
	if provider.validationStreams.Load() != 2 || provider.applyStreams.Load() != 2 || provider.applied() != 2 {
		t.Fatalf("validation streams=%d apply streams=%d applied=%d", provider.validationStreams.Load(), provider.applyStreams.Load(), provider.applied())
	}
}

type envelopeBody[T any] struct {
	Code int `json:"code"`
	Body T   `json:"body"`
}

type importStatus struct {
	Status  string `json:"status"`
	Version int64  `json:"version"`
}

func waitImportJob(t *testing.T, baseURL, psk, id, expected string) importStatus {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		body, _ := postJSONBody(t, baseURL+"/api/v1/imports/get", "PSK "+psk, "", `{"tenant_id":"tenant-1","id":"`+id+`"}`)
		var result envelopeBody[importStatus]
		if json.Unmarshal(body, &result) == nil && result.Body.Status == expected {
			return result.Body
		}
		if result.Body.Status == "failed" || result.Body.Status == "validation_failed" {
			t.Fatalf("import reached %s while waiting for %s: %s", result.Body.Status, expected, body)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("import did not reach %s", expected)
	return importStatus{}
}

type testImportProvider struct {
	importv1.UnimplementedImportProviderServiceServer
	validationStreams atomic.Int32
	applyStreams      atomic.Int32
	validationFailed  atomic.Bool
	applyFailed       atomic.Bool
	mu                sync.Mutex
	appliedKeys       map[string]struct{}
}

func (*testImportProvider) DescribeImportDataset(context.Context, *importv1.DescribeImportDatasetRequest) (*importv1.DescribeImportDatasetResponse, error) {
	return &importv1.DescribeImportDatasetResponse{Dataset: &importv1.ImportDatasetDescriptor{Code: "test.rows", Title: "Rows", Formats: []string{"csv"}, MaxBatchSize: 1, SupportsDryRun: true}}, nil
}
func (p *testImportProvider) ValidateRows(stream importv1.ImportProviderService_ValidateRowsServer) error {
	p.validationStreams.Add(1)
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if p.validationFailed.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "transient validation failure")
		}
		if err := stream.Send(&importv1.ValidateRowsResponse{BatchNumber: request.GetBatchNumber(), NormalizedRows: request.GetRows()}); err != nil {
			return err
		}
	}
}
func (p *testImportProvider) ApplyRows(stream importv1.ImportProviderService_ApplyRowsServer) error {
	p.applyStreams.Add(1)
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		p.mu.Lock()
		_, duplicate := p.appliedKeys[request.GetIdempotencyKey()]
		p.appliedKeys[request.GetIdempotencyKey()] = struct{}{}
		p.mu.Unlock()
		if p.applyFailed.CompareAndSwap(false, true) {
			return status.Error(codes.Unavailable, "transient apply failure after commit")
		}
		applied := int64(len(request.GetRows()))
		if duplicate {
			applied = int64(len(request.GetRows()))
		}
		if err := stream.Send(&importv1.ApplyRowsResponse{BatchNumber: request.GetBatchNumber(), AppliedRows: applied}); err != nil {
			return err
		}
	}
}
func (p *testImportProvider) applied() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.appliedKeys)
}

func startImportProvider(t *testing.T, provider *testImportProvider) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	importv1.RegisterImportProviderServiceServer(server, provider)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String()
}

func startImportNATS(t *testing.T, ctx context.Context) string {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "nats:2.14.6-alpine", ExposedPorts: []string{"4222/tcp", "8222/tcp"}, Cmd: []string{"-js", "-m", "8222"}, WaitingFor: wait.ForHTTP("/healthz").WithPort("8222/tcp")}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4222/tcp")
	return "nats://" + host + ":" + port.Port()
}

func startImportMinIO(t *testing.T, ctx context.Context) (string, string, string) {
	t.Helper()
	const access = "integration-access"
	const secret = "integration-secret-key"
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "minio/minio:RELEASE.2025-09-07T16-13-09Z", ExposedPorts: []string{"9000/tcp"}, Env: map[string]string{"MINIO_ROOT_USER": access, "MINIO_ROOT_PASSWORD": secret}, Cmd: []string{"server", "/data"}, WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp")}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "9000/tcp")
	return host + ":" + port.Port(), access, secret
}
