ALTER TABLE import_jobs ADD COLUMN application_id text NOT NULL DEFAULT '';
UPDATE import_jobs
SET status = 'canceled', completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP), version = version + 1,
    updated_at = CURRENT_TIMESTAMP, updated_by = 'application-scope-migration'
WHERE status IN ('uploading','queued','validating','ready','apply_queued','applying') AND application_id = '';
ALTER TABLE import_jobs DROP CONSTRAINT import_jobs_idempotency_unique;
ALTER TABLE import_jobs ADD CONSTRAINT import_jobs_idempotency_unique UNIQUE (tenant_id, application_id, idempotency_key);
DROP INDEX import_jobs_tenant_created_idx;
CREATE INDEX import_jobs_tenant_created_idx ON import_jobs (tenant_id, application_id, created_at DESC, id DESC);
DROP INDEX import_jobs_tenant_status_created_idx;
CREATE INDEX import_jobs_tenant_status_created_idx ON import_jobs (tenant_id, application_id, status, created_at DESC);
