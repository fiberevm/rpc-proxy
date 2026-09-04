package jsonrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
)

const Version = "2.0"

// Request is one positional-parameter JSON-RPC 2.0 request received by the proxy.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether the request omitted an ID and therefore expects no response.
func (r Request) IsNotification() bool { return len(r.ID) == 0 }

// Validate checks the protocol fields consumed by the gateway before request transformation.
func (r Request) Validate() *Error {
	if r.JSONRPC != Version || r.Method == "" {
		return InvalidRequest("jsonrpc must be 2.0 and method must be a string")
	}
	if len(r.ID) > 0 && !validID(r.ID) {
		return InvalidRequest("id must be a string, number, or null")
	}
	return nil
}

// Response represents either a JSON-RPC result or a JSON-RPC error.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC error that can also participate in normal Go error handling.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Error formats the protocol code and message for logs and wrapped backend errors.
func (e *Error) Error() string { return fmt.Sprintf("json-rpc error %d: %s", e.Code, e.Message) }

// Success builds a response for requestID while preserving the upstream response bytes.
func Success(requestID json.RawMessage, upstreamResponse json.RawMessage) Response {
	if upstreamResponse == nil {
		upstreamResponse = json.RawMessage("null")
	}
	return Response{JSONRPC: Version, ID: idOrNull(requestID), Result: upstreamResponse}
}

// Failure builds an error response for requestID without changing the supplied protocol error.
func Failure(requestID json.RawMessage, rpcErr *Error) Response {
	return Response{JSONRPC: Version, ID: idOrNull(requestID), Error: rpcErr}
}

func idOrNull(id json.RawMessage) json.RawMessage {
	if !validID(id) {
		return json.RawMessage("null")
	}
	return id
}

func validID(id json.RawMessage) bool {
	id = bytes.TrimSpace(id)
	if !json.Valid(id) {
		return false
	}
	return id[0] == '"' || id[0] == '-' || id[0] >= '0' && id[0] <= '9' || bytes.Equal(id, []byte("null"))
}

// ParseEnvelope validates a JSON-RPC body and enforces maxBatch for array requests.
func ParseEnvelope(body []byte, maxBatch int) ([]Request, bool, *Error) {
	trimmedBody := bytes.TrimSpace(body)
	if len(trimmedBody) == 0 {
		return nil, false, ParseError("empty request body")
	}
	if trimmedBody[0] == '[' {
		var rawRequests []json.RawMessage
		if err := json.Unmarshal(trimmedBody, &rawRequests); err != nil {
			return nil, true, ParseError(err.Error())
		}
		if len(rawRequests) == 0 {
			return nil, true, InvalidRequest("batch must not be empty")
		}
		if len(rawRequests) > maxBatch {
			return nil, true, InvalidRequest("batch exceeds configured limit")
		}
		requests := make([]Request, 0, len(rawRequests))
		for _, encodedRequest := range rawRequests {
			var request Request
			if err := json.Unmarshal(encodedRequest, &request); err != nil {
				requests = append(requests, Request{})
				continue
			}
			requests = append(requests, request)
		}
		return requests, true, nil
	}
	var request Request
	if err := json.Unmarshal(trimmedBody, &request); err != nil {
		if json.Valid(trimmedBody) {
			return nil, false, InvalidRequest(err.Error())
		}
		return nil, false, ParseError(err.Error())
	}
	return []Request{request}, false, nil
}

// ParseParams decodes the positional parameter array required by the strict method registry.
func ParseParams(encodedParameters json.RawMessage) ([]json.RawMessage, *Error) {
	if len(encodedParameters) == 0 || bytes.Equal(bytes.TrimSpace(encodedParameters), []byte("null")) {
		return []json.RawMessage{}, nil
	}
	var parameters []json.RawMessage
	if err := json.Unmarshal(encodedParameters, &parameters); err != nil {
		return nil, InvalidParams("positional parameters are required")
	}
	return parameters, nil
}

// MarshalParams encodes already-validated positional parameter elements for an upstream call.
func MarshalParams(parameters []json.RawMessage) (json.RawMessage, error) {
	encodedParameters, err := json.Marshal(parameters)
	if err != nil {
		return nil, fmt.Errorf("encode JSON-RPC parameters: %w", err)
	}
	return encodedParameters, nil
}

// MarshalCallParams encodes typed positional arguments for an internally generated upstream call.
func MarshalCallParams(arguments ...any) (json.RawMessage, error) {
	encodedParameters, err := json.Marshal(arguments)
	if err != nil {
		return nil, fmt.Errorf("encode JSON-RPC call parameters: %w", err)
	}
	return encodedParameters, nil
}
