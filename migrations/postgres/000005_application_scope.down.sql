DROP INDEX import_jobs_tenant_status_created_idx;
CREATE INDEX import_jobs_tenant_status_created_idx ON import_jobs (tenant_id, status, created_at DESC);
DROP INDEX import_jobs_tenant_created_idx;
CREATE INDEX import_jobs_tenant_created_idx ON import_jobs (tenant_id, created_at DESC, id DESC);
ALTER TABLE import_jobs DROP CONSTRAINT import_jobs_idempotency_unique;
ALTER TABLE import_jobs ADD CONSTRAINT import_jobs_idempotency_unique UNIQUE (tenant_id, idempotency_key);
ALTER TABLE import_jobs DROP COLUMN application_id;
