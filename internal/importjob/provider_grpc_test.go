package importjob

import (
	"testing"

	registryv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/registry/v1"
)

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
