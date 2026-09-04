package jsonrpc

import (
	"encoding/json"
	"testing"
)

func TestParseEnvelopeBatchAndNotifications(t *testing.T) {
	body := []byte(`[
        {"jsonrpc":"2.0","id":"a","method":"eth_blockNumber"},
        {"jsonrpc":"2.0","method":"eth_blockNumber"}
    ]`)
	requests, batch, rpcErr := ParseEnvelope(body, 10)
	if rpcErr != nil || !batch || len(requests) != 2 {
		t.Fatalf("unexpected parse result: batch=%v len=%d err=%v", batch, len(requests), rpcErr)
	}
	if requests[0].IsNotification() {
		t.Fatal("request with id was classified as notification")
	}
	if !requests[1].IsNotification() {
		t.Fatal("request without id was not classified as notification")
	}
}

func TestParseEnvelopeRejectsEmptyAndOversizedBatches(t *testing.T) {
	if _, _, rpcErr := ParseEnvelope([]byte(`[]`), 10); rpcErr == nil || rpcErr.Code != CodeInvalidRequest {
		t.Fatalf("expected invalid request, got %v", rpcErr)
	}
	if _, _, rpcErr := ParseEnvelope([]byte(`[{"jsonrpc":"2.0","method":"a"},{"jsonrpc":"2.0","method":"b"}]`), 1); rpcErr == nil || rpcErr.Code != CodeInvalidRequest {
		t.Fatalf("expected batch limit error, got %v", rpcErr)
	}
}

func TestParseEnvelopeKeepsInvalidBatchItems(t *testing.T) {
	requests, batch, rpcErr := ParseEnvelope([]byte(`[1,{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`), 10)
	if rpcErr != nil || !batch || len(requests) != 2 {
		t.Fatalf("unexpected parse result: batch=%v len=%d err=%v", batch, len(requests), rpcErr)
	}
	if validation := requests[0].Validate(); validation == nil || validation.Code != CodeInvalidRequest {
		t.Fatalf("expected invalid batch member, got %v", validation)
	}
}

func TestParseEnvelopeDistinguishesInvalidRequestFromParseError(t *testing.T) {
	if _, _, rpcErr := ParseEnvelope([]byte(`1`), 10); rpcErr == nil || rpcErr.Code != CodeInvalidRequest {
		t.Fatalf("expected invalid request for valid scalar JSON, got %v", rpcErr)
	}
	if _, _, rpcErr := ParseEnvelope([]byte(`{`), 10); rpcErr == nil || rpcErr.Code != CodeParseError {
		t.Fatalf("expected parse error for malformed JSON, got %v", rpcErr)
	}
}

func TestRequestIDValidationAndErrorIdentity(t *testing.T) {
	for _, id := range []string{`true`, `false`, `[]`, `{}`, `[1]`} {
		t.Run(id, func(t *testing.T) {
			request := Request{JSONRPC: Version, Method: "eth_blockNumber", ID: json.RawMessage(id)}
			rpcErr := request.Validate()
			if rpcErr == nil || rpcErr.Code != CodeInvalidRequest {
				t.Fatalf("accepted id: %s", id)
			}
			if got := Failure(request.ID, rpcErr); string(got.ID) != "null" {
				t.Fatalf("invalid error id: %s", got.ID)
			}
		})
	}
	for _, id := range []string{`null`, `"client"`, `9007199254740993`, `-1`, `1.5`} {
		request := Request{JSONRPC: Version, Method: "eth_blockNumber", ID: json.RawMessage(id)}
		if err := request.Validate(); err != nil {
			t.Fatal(err)
		}
		if string(Success(request.ID, nil).ID) != id {
			t.Fatalf("changed valid id: %s", id)
		}
	}
}

func FuzzParseEnvelope(f *testing.F) {
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`))
	f.Add([]byte(`[]`))
	f.Fuzz(func(t *testing.T, body []byte) {
		requests, _, rpcErr := ParseEnvelope(body, 100)
		if rpcErr != nil {
			return
		}
		for _, request := range requests {
			if _, err := json.Marshal(request); err != nil {
				t.Fatal(err)
			}
		}
	})
}
