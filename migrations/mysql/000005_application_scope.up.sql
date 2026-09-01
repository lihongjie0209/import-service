ALTER TABLE import_jobs ADD COLUMN application_id varchar(64) NOT NULL DEFAULT '' AFTER tenant_id;
UPDATE import_jobs
SET status = 'canceled', completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP(6)), version = version + 1,
    updated_at = CURRENT_TIMESTAMP(6), updated_by = 'application-scope-migration'
WHERE status IN ('uploading','queued','validating','ready','apply_queued','applying') AND application_id = '';
ALTER TABLE import_jobs
    DROP INDEX import_jobs_idempotency_unique,
    ADD UNIQUE KEY import_jobs_idempotency_unique (tenant_id(128), application_id, idempotency_key(191)),
    DROP INDEX import_jobs_tenant_created_idx,
    ADD KEY import_jobs_tenant_created_idx (tenant_id(128), application_id, created_at DESC, id(64)),
    DROP INDEX import_jobs_tenant_status_created_idx,
    ADD KEY import_jobs_tenant_status_created_idx (tenant_id(128), application_id, status, created_at DESC);
