package jsonrpc

const (
	CodeParseError             = -32700
	CodeInvalidRequest         = -32600
	CodeMethodNotFound         = -32601
	CodeInvalidParams          = -32602
	CodeInternalError          = -32603
	CodeConsistencyUnavailable = -32070
	CodeUnsupportedConsistency = -32071
	CodeWritesDisabled         = -32072
	CodeChainUnavailable       = -32073
)

// ParseError reports malformed JSON text with a concise diagnostic detail.
func ParseError(detail string) *Error {
	return &Error{Code: CodeParseError, Message: "Parse error", Data: detail}
}

// InvalidRequest reports a valid JSON value that is not a valid JSON-RPC request.
func InvalidRequest(detail string) *Error {
	return &Error{Code: CodeInvalidRequest, Message: "Invalid Request", Data: detail}
}

// MethodNotFound reports that method is absent from an upstream or supported registry.
func MethodNotFound(method string) *Error {
	return &Error{Code: CodeMethodNotFound, Message: "Method not found", Data: map[string]any{"method": method}}
}

// InvalidParams reports parameters that do not satisfy a registered method's contract.
func InvalidParams(detail string) *Error {
	return &Error{Code: CodeInvalidParams, Message: "Invalid params", Data: detail}
}

// InternalError reports a proxy failure that cannot safely expose implementation details.
func InternalError(detail string) *Error {
	return &Error{Code: CodeInternalError, Message: "Internal error", Data: detail}
}

// ConsistencyUnavailable reports why no upstream can currently serve the accepted head.
func ConsistencyUnavailable(details any) *Error {
	return &Error{Code: CodeConsistencyUnavailable, Message: "Consistent head is unavailable", Data: details}
}

// UnsupportedConsistency reports a read that cannot carry this proxy's strict state guarantee.
func UnsupportedConsistency(method, detail string) *Error {
	return &Error{Code: CodeUnsupportedConsistency, Message: "Method cannot be served with the configured consistency guarantee", Data: map[string]any{"method": method, "detail": detail}}
}

// WritesDisabled rejects a transaction or node-managed signing method by policy.
func WritesDisabled(method string) *Error {
	return &Error{Code: CodeWritesDisabled, Message: "Writes are disabled", Data: map[string]any{"method": method}}
}

// ChainUnavailable reports that chain is not present in the validated runtime configuration.
func ChainUnavailable(chain string) *Error {
	return &Error{Code: CodeChainUnavailable, Message: "Chain is unavailable", Data: map[string]any{"chain": chain}}
}
