package importjob

import "testing"

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
