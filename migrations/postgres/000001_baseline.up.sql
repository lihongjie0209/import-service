CREATE TABLE import_jobs (
    id text PRIMARY KEY, tenant_id text NOT NULL, dataset_code text NOT NULL, provider_service text NOT NULL,
    format text NOT NULL, filename text NOT NULL, status text NOT NULL, source_object_key text NOT NULL,
    normalized_object_key text NOT NULL, error_report_object_key text NOT NULL, source_checksum text NOT NULL DEFAULT '',
    source_bytes bigint NOT NULL DEFAULT 0, total_rows bigint NOT NULL DEFAULT 0, valid_rows bigint NOT NULL DEFAULT 0,
    invalid_rows bigint NOT NULL DEFAULT 0, applied_rows bigint NOT NULL DEFAULT 0,
    progress_percent integer NOT NULL DEFAULT 0, error_code text NOT NULL DEFAULT '', error_message text NOT NULL DEFAULT '',
    idempotency_key text NOT NULL, confirm_key text NOT NULL DEFAULT '', upload_expires_at timestamptz,
    started_at timestamptz, completed_at timestamptz, result_expires_at timestamptz,
    version bigint NOT NULL DEFAULT 1, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
    created_by text NOT NULL, updated_by text NOT NULL,
    CONSTRAINT import_jobs_status_check CHECK (status IN ('uploading','queued','validating','validation_failed','ready','apply_queued','applying','succeeded','failed','canceled','expired')),
    CONSTRAINT import_jobs_format_check CHECK (format IN ('csv','jsonl','xlsx')),
    CONSTRAINT import_jobs_progress_check CHECK (progress_percent BETWEEN 0 AND 100),
    CONSTRAINT import_jobs_idempotency_unique UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX import_jobs_tenant_created_idx ON import_jobs (tenant_id, created_at DESC, id DESC);
CREATE INDEX import_jobs_tenant_status_created_idx ON import_jobs (tenant_id, status, created_at DESC);
CREATE INDEX import_jobs_queued_idx ON import_jobs (created_at, id) WHERE status IN ('queued','apply_queued');
CREATE INDEX import_jobs_expiry_idx ON import_jobs (result_expires_at) WHERE status IN ('validation_failed','succeeded','failed','canceled');

CREATE TABLE import_outbox_events (
    id text PRIMARY KEY, subject text NOT NULL, envelope bytea NOT NULL, attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL, published_at timestamptz, last_error text NOT NULL DEFAULT '',
    version bigint NOT NULL DEFAULT 1, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
    created_by text NOT NULL, updated_by text NOT NULL
);
CREATE INDEX import_outbox_pending_idx ON import_outbox_events (available_at, created_at) WHERE published_at IS NULL;
