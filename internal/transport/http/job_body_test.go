package httptransport

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lihongjie0209/import-service/internal/importjob"
)

func TestImportJobBodyOmitsStorageAndIdempotencyInternals(t *testing.T) {
	encoded, err := json.Marshal(importJobBody(importjob.Job{
		ID:                   "job-1",
		SourceObjectKey:      "tenant/private/source.csv",
		NormalizedObjectKey:  "tenant/private/normalized.csv",
		ErrorReportObjectKey: "tenant/private/errors.csv",
		IdempotencyKey:       "create-secret",
		ConfirmKey:           "confirm-secret",
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"source_object_key", "normalized_object_key", "error_report_object_key", "idempotency_key", "confirm_key", "tenant/private"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("public job body contains internal value %q: %s", secret, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"id":"job-1"`) {
		t.Fatalf("public job body lost job identity: %s", encoded)
	}
}
