package apperror

import (
	"net/http"
	"testing"
)

func TestForbiddenUsesSharedCodeAndHTTPStatus(t *testing.T) {
	err := Forbidden("permission denied")
	if err.Code != CodeForbidden || err.HTTPStatus != http.StatusForbidden {
		t.Fatalf("Forbidden() = code %d status %d", err.Code, err.HTTPStatus)
	}
}
