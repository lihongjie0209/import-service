CREATE TABLE import_jobs (
    id varchar(191) NOT NULL, tenant_id varchar(191) NOT NULL, dataset_code varchar(191) NOT NULL,
    provider_service varchar(191) NOT NULL, format varchar(16) NOT NULL, filename text NOT NULL,
    status varchar(32) NOT NULL, source_object_key text NOT NULL, normalized_object_key text NOT NULL,
    error_report_object_key text NOT NULL, source_checksum text NOT NULL, source_bytes bigint NOT NULL DEFAULT 0,
    total_rows bigint NOT NULL DEFAULT 0, valid_rows bigint NOT NULL DEFAULT 0, invalid_rows bigint NOT NULL DEFAULT 0,
    applied_rows bigint NOT NULL DEFAULT 0, progress_percent int NOT NULL DEFAULT 0, error_code text NOT NULL,
    error_message text NOT NULL, idempotency_key varchar(191) NOT NULL, confirm_key varchar(191) NOT NULL DEFAULT '',
    upload_expires_at timestamp(6) NULL, started_at timestamp(6) NULL, completed_at timestamp(6) NULL,
    result_expires_at timestamp(6) NULL, version bigint NOT NULL DEFAULT 1, created_at timestamp(6) NOT NULL,
    updated_at timestamp(6) NOT NULL, created_by text NOT NULL, updated_by text NOT NULL,
    PRIMARY KEY (id), UNIQUE KEY import_jobs_idempotency_unique (tenant_id, idempotency_key),
    KEY import_jobs_tenant_created_idx (tenant_id, created_at DESC, id),
    KEY import_jobs_tenant_status_created_idx (tenant_id, status, created_at DESC),
    KEY import_jobs_queued_idx (status, created_at, id), KEY import_jobs_expiry_idx (status, result_expires_at),
    CONSTRAINT import_jobs_status_check CHECK (status IN ('uploading','queued','validating','validation_failed','ready','apply_queued','applying','succeeded','failed','canceled','expired')),
    CONSTRAINT import_jobs_format_check CHECK (format IN ('csv','jsonl','xlsx')),
    CONSTRAINT import_jobs_progress_check CHECK (progress_percent BETWEEN 0 AND 100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE import_outbox_events (
    id varchar(191) NOT NULL, subject text NOT NULL, envelope longblob NOT NULL, attempts int NOT NULL DEFAULT 0,
    available_at timestamp(6) NOT NULL, published_at timestamp(6) NULL, last_error text NOT NULL,
    version bigint NOT NULL DEFAULT 1, created_at timestamp(6) NOT NULL, updated_at timestamp(6) NOT NULL,
    created_by text NOT NULL, updated_by text NOT NULL, PRIMARY KEY (id),
    KEY import_outbox_pending_idx (published_at, available_at, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
