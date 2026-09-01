ALTER TABLE import_jobs
    DROP INDEX import_jobs_tenant_status_created_idx,
    ADD KEY import_jobs_tenant_status_created_idx (tenant_id(128), status, created_at DESC),
    DROP INDEX import_jobs_tenant_created_idx,
    ADD KEY import_jobs_tenant_created_idx (tenant_id(128), created_at DESC, id(64)),
    DROP INDEX import_jobs_idempotency_unique,
    ADD UNIQUE KEY import_jobs_idempotency_unique (tenant_id(128), idempotency_key(191)),
    DROP COLUMN application_id;
