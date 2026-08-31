CREATE INDEX import_outbox_retention_idx ON import_outbox_events (published_at, id);
