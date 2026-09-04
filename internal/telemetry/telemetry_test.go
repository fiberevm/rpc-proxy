package telemetry

import (
	"errors"
	"log/slog"
	"testing"
)

func TestRedactCredentials(t *testing.T) {
	input := "dial https://alice:password@example.invalid/rpc?api_key=abc&token=def authorization=Bearer"
	got := newCredentialRedactor().redact(input)
	if got != "dial [REDACTED_URL] authorization=[REDACTED]" {
		t.Fatalf("unexpected redaction: %s", got)
	}
}

func TestRedactsCredentialsInsideErrorAttributes(t *testing.T) {
	redactor := newCredentialRedactor()
	attr := redactor.replaceAttr(nil, slog.Any("error", errors.New("dial https://rpc.example/v2/private-key: refused")))
	if attr.Value.String() != "dial [REDACTED_URL] refused" {
		t.Fatalf("credential leak: %s", attr.Value.String())
	}
}
