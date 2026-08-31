package importjob

import "testing"

func TestCatalogListsNormalizedPageWithinTenant(t *testing.T) {
	t.Parallel()
	provider := providerStub{list: func(search string, page, size int32) ([]DatasetSummary, int64, error) {
		if search != "users" || page != 1 || size != 20 {
			t.Fatalf("search=%q page=%d size=%d", search, page, size)
		}
		return []DatasetSummary{{ProviderService: "identity-service", Code: "identity.users"}}, 1, nil
	}}
	values, total, page, size, err := NewCatalog(provider).List(userContext("tenant-1"), "tenant-1", " users ", 0, 0)
	if err != nil || len(values) != 1 || total != 1 || page != 1 || size != 20 {
		t.Fatalf("values=%+v total=%d page=%d size=%d err=%v", values, total, page, size, err)
	}
	if _, _, _, _, err := NewCatalog(provider).List(userContext("tenant-2"), "tenant-1", "", 1, 20); err == nil {
		t.Fatal("cross-tenant catalog access accepted")
	}
}

func TestCatalogDescribesProviderDataset(t *testing.T) {
	t.Parallel()
	provider := providerStub{describe: func(tenant, service, dataset string) (DatasetDescriptor, error) {
		if tenant != "tenant-1" || service != "identity-service" || dataset != "identity.users" {
			t.Fatalf("tenant=%q service=%q dataset=%q", tenant, service, dataset)
		}
		return DatasetDescriptor{Code: dataset, Title: "Users"}, nil
	}}
	value, err := NewCatalog(provider).Describe(userContext("tenant-1"), "tenant-1", "identity-service", "identity.users")
	if err != nil || value.Code != "identity.users" {
		t.Fatalf("value=%+v err=%v", value, err)
	}
}
