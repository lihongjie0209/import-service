package importjob

import (
	"context"
	"strings"

	"github.com/lihongjie0209/import-service/internal/apperror"
)

type Catalog struct{ provider Provider }

func NewCatalog(provider Provider) *Catalog { return &Catalog{provider: provider} }

func (c *Catalog) List(ctx context.Context, tenantID, search string, page, pageSize int32) ([]DatasetSummary, int64, int32, int32, error) {
	if _, err := authorize(ctx, tenantID); err != nil {
		return nil, 0, 0, 0, err
	}
	page, pageSize = normalizeCatalogPage(page, pageSize)
	values, total, err := c.provider.ListDatasets(ctx, strings.TrimSpace(search), page, pageSize)
	if err != nil {
		return nil, 0, 0, 0, apperror.Unavailable("import dataset catalog is unavailable", err)
	}
	return values, total, page, pageSize, nil
}

func (c *Catalog) Describe(ctx context.Context, tenantID, service, dataset string) (DatasetDescriptor, error) {
	if _, err := authorize(ctx, tenantID); err != nil {
		return DatasetDescriptor{}, err
	}
	service, dataset = clean(service), clean(dataset)
	if !codePattern.MatchString(service) || !codePattern.MatchString(dataset) {
		return DatasetDescriptor{}, apperror.Invalid("provider_service and dataset_code are required", nil)
	}
	value, err := c.provider.DescribeDataset(ctx, clean(tenantID), service, dataset)
	if err != nil {
		return DatasetDescriptor{}, apperror.Unavailable("import dataset descriptor is unavailable", err)
	}
	return value, nil
}
