package gateway

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

func TestTransformEVMStateReadPinsBlockHash(t *testing.T) {
	runtime := testRuntime(t, "evm")
	snapshot := evmSnapshot()
	request := jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "eth_call", Params: json.RawMessage(`[{"to":"0x0000000000000000000000000000000000000000"}]`)}
	prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, snapshot, request)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if !prepared.requireEIP1898 || prepared.target == nil || prepared.target.Hash != testHash('a') {
		t.Fatalf("not pinned: %+v", prepared)
	}
	var params []json.RawMessage
	if err := json.Unmarshal(prepared.parameters, &params); err != nil {
		t.Fatal(err)
	}
	var blockRef map[string]any
	if err := json.Unmarshal(params[1], &blockRef); err != nil {
		t.Fatal(err)
	}
	if blockRef["blockHash"] != testHash('a') || blockRef["requireCanonical"] != true {
		t.Fatalf("unexpected block ref: %#v", blockRef)
	}
}

func TestTransformEveryEVMStateMethod(t *testing.T) {
	runtime := testRuntime(t, "evm")
	cases := map[string]struct {
		params     string
		blockIndex int
	}{
		"eth_getBalance":          {`["address"]`, 1},
		"eth_getStorageAt":        {`["address","0x0"]`, 2},
		"eth_getTransactionCount": {`["address"]`, 1},
		"eth_getCode":             {`["address"]`, 1},
		"eth_call":                {`[{"to":"address"},"latest",{"address":{"balance":"0x1"}}]`, 1},
		"eth_getProof":            {`["address",[]]`, 2},
	}
	for method, testCase := range cases {
		t.Run(method, func(t *testing.T) {
			prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), jsonrpc.Request{JSONRPC: "2.0", Method: method, Params: json.RawMessage(testCase.params)})
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			var params []json.RawMessage
			if err := json.Unmarshal(prepared.parameters, &params); err != nil {
				t.Fatal(err)
			}
			var blockRef struct {
				BlockHash        string `json:"blockHash"`
				RequireCanonical bool   `json:"requireCanonical"`
			}
			if err := json.Unmarshal(params[testCase.blockIndex], &blockRef); err != nil || blockRef.BlockHash != testHash('a') || !blockRef.RequireCanonical {
				t.Fatalf("invalid block pin: params=%s err=%v", prepared.parameters, err)
			}
			if method == "eth_call" && len(params) != 3 {
				t.Fatalf("eth_call state override was not preserved: %s", prepared.parameters)
			}
		})
	}
}

func TestTransformEVMBlockMethodUsesHashVariant(t *testing.T) {
	runtime := testRuntime(t, "evm")
	request := jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: "eth_getBlockByNumber", Params: json.RawMessage(`["latest",false]`)}
	prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), request)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if prepared.method != "eth_getBlockByHash" || !strings.Contains(string(prepared.parameters), testHash('a')) {
		t.Fatalf("unexpected rewrite: %+v", prepared)
	}
}

func TestTransformEveryEVMNumberMethodUsesHashVariant(t *testing.T) {
	runtime := testRuntime(t, "evm")
	testCases := map[string]string{
		"eth_getBlockByNumber":                    "eth_getBlockByHash",
		"eth_getBlockTransactionCountByNumber":    "eth_getBlockTransactionCountByHash",
		"eth_getTransactionByBlockNumberAndIndex": "eth_getTransactionByBlockHashAndIndex",
		"eth_getUncleByBlockNumberAndIndex":       "eth_getUncleByBlockHashAndIndex",
		"eth_getUncleCountByBlockNumber":          "eth_getUncleCountByBlockHash",
	}
	for method, replacement := range testCases {
		t.Run(method, func(t *testing.T) {
			params := []json.RawMessage{json.RawMessage(`"latest"`), json.RawMessage(`"0x0"`)}
			prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), jsonrpc.Request{JSONRPC: "2.0", Method: method, Params: mustMarshalParameters(t, params)})
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			if prepared.method != replacement || !strings.Contains(string(prepared.parameters), testHash('a')) {
				t.Fatalf("unexpected rewrite: %+v", prepared)
			}
		})
	}
}

func TestTransformEVMLatestAndExplicitHash(t *testing.T) {
	runtime := testRuntime(t, "evm")
	for _, testCase := range []struct{ name, selector, hash string }{
		{"latest", `"latest"`, testHash('a')},
		{"explicit", `{"blockHash":"` + testHash('d') + `","requireCanonical":false}`, testHash('d')},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			parameters := json.RawMessage(`["address",` + testCase.selector + `]`)
			prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), jsonrpc.Request{JSONRPC: "2.0", Method: "eth_getBalance", Params: parameters})
			if rpcErr != nil || prepared.target == nil || prepared.target.Hash != testCase.hash || !strings.Contains(string(prepared.parameters), `"requireCanonical":true`) {
				t.Fatalf("block reference was not pinned canonically: prepared=%+v err=%v", prepared, rpcErr)
			}
		})
	}
}

func TestTransformEVMRejectsUntrackedCommitmentsEvenWithStoredHeads(t *testing.T) {
	runtime := testRuntime(t, "evm")
	for _, tag := range []string{head.Safe, head.Finalized} {
		for _, testCase := range []struct{ name, method, parameters string }{
			{"state", "eth_getBalance", `["address","TAG"]`},
			{"call", "eth_call", `[{"to":"address"},"TAG"]`},
			{"block", "eth_getBlockByNumber", `["TAG",false]`},
			{"log start", "eth_getLogs", `[{"fromBlock":"TAG","toBlock":"latest"}]`},
			{"log end", "eth_getLogs", `[{"fromBlock":"latest","toBlock":"TAG"}]`},
			{"fees", "eth_feeHistory", `["0x1","TAG",[]]`},
		} {
			t.Run(tag+"/"+testCase.name, func(t *testing.T) {
				parameters := json.RawMessage(strings.ReplaceAll(testCase.parameters, "TAG", tag))
				prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), jsonrpc.Request{JSONRPC: "2.0", Method: testCase.method, Params: parameters})
				if rpcErr == nil || rpcErr.Code != jsonrpc.CodeUnsupportedConsistency || prepared.target != nil {
					t.Fatalf("untracked commitment was not rejected: prepared=%+v err=%v", prepared, rpcErr)
				}
			})
		}
	}
}

func TestTransformEVMRejectsPendingAndWrites(t *testing.T) {
	runtime := testRuntime(t, "evm")
	pending := jsonrpc.Request{JSONRPC: "2.0", Method: "eth_getBalance", Params: json.RawMessage(`[
        "0x0000000000000000000000000000000000000000","pending"]`)}
	if _, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), pending); rpcErr == nil || rpcErr.Code != jsonrpc.CodeUnsupportedConsistency {
		t.Fatalf("expected pending rejection, got %v", rpcErr)
	}
	write := jsonrpc.Request{JSONRPC: "2.0", Method: "eth_sendRawTransaction", Params: json.RawMessage(`["0x00"]`)}
	if _, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), write); rpcErr == nil || rpcErr.Code != jsonrpc.CodeWritesDisabled {
		t.Fatalf("expected write rejection, got %v", rpcErr)
	}
}

func TestTransformEVMLogsUsesHashForSingleHead(t *testing.T) {
	runtime := testRuntime(t, "evm")
	request := jsonrpc.Request{JSONRPC: "2.0", Method: "eth_getLogs", Params: json.RawMessage(`[{"fromBlock":"latest","toBlock":"latest","topics":[]}]`)}
	prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), request)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if !strings.Contains(string(prepared.parameters), `"blockHash"`) || strings.Contains(string(prepared.parameters), `"fromBlock"`) {
		t.Fatalf("unexpected log rewrite: %s", prepared.parameters)
	}
}

func TestTransformSolanaInjectsMaximumSlotFloor(t *testing.T) {
	runtime := testRuntime(t, "solana")
	snapshot := head.Snapshot{Chain: "test", Heads: map[string]head.Head{head.Confirmed: {Chain: "test", Family: "solana", Commitment: head.Confirmed, Number: 100}}}
	request := jsonrpc.Request{JSONRPC: "2.0", Method: "getAccountInfo", Params: json.RawMessage(`["account",{"commitment":"confirmed","minContextSlot":120}]`)}
	prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, snapshot, request)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	if prepared.target.Number != 120 || !strings.Contains(string(prepared.parameters), `"minContextSlot":120`) {
		t.Fatalf("unexpected Solana rewrite: %+v %s", prepared, prepared.parameters)
	}
}

func TestTransformEverySolanaStateMethod(t *testing.T) {
	runtime := testRuntime(t, "solana")
	snapshot := head.Snapshot{Chain: "test", Heads: map[string]head.Head{head.Finalized: {Chain: "test", Family: "solana", Commitment: head.Finalized, Number: 100}}}
	testCases := map[string]int{
		"getBlockHeight": 0, "getEpochInfo": 0, "getLatestBlockhash": 0, "getSlot": 0,
		"getSlotLeader": 0, "getStakeMinimumDelegation": 0, "getTransactionCount": 0,
		"getAccountInfo": 1, "getBalance": 1, "getFeeForMessage": 1, "getMultipleAccounts": 1,
		"getProgramAccounts": 1, "getSignaturesForAddress": 1,
		"isBlockhashValid": 1, "simulateTransaction": 1,
		"getTokenAccountsByDelegate": 2, "getTokenAccountsByOwner": 2,
	}
	for method, configIndex := range testCases {
		t.Run(method, func(t *testing.T) {
			params := make([]json.RawMessage, configIndex)
			for index := range params {
				params[index] = json.RawMessage(`"argument"`)
			}
			prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, snapshot, jsonrpc.Request{JSONRPC: "2.0", Method: method, Params: mustMarshalParameters(t, params)})
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			var rewritten []json.RawMessage
			if err := json.Unmarshal(prepared.parameters, &rewritten); err != nil {
				t.Fatal(err)
			}
			var cfg struct {
				Commitment     string `json:"commitment"`
				MinContextSlot uint64 `json:"minContextSlot"`
			}
			if err := json.Unmarshal(rewritten[configIndex], &cfg); err != nil || cfg.Commitment != head.Finalized || cfg.MinContextSlot != 100 {
				t.Fatalf("invalid Solana pin: params=%s err=%v", prepared.parameters, err)
			}
		})
	}
}

func TestVerifySolanaContextWhenPresent(t *testing.T) {
	if err := (&Gateway{}).verifySolanaContext(json.RawMessage(`{"context":{"slot":100},"value":null}`), 100); err != nil {
		t.Fatal(err)
	}
	if err := (&Gateway{}).verifySolanaContext(json.RawMessage(`{"context":{"slot":99},"value":null}`), 100); err == nil {
		t.Fatal("expected stale context rejection")
	}
	if err := (&Gateway{}).verifySolanaContext(json.RawMessage(`{"context":{},"value":null}`), 100); err == nil {
		t.Fatal("expected malformed context rejection")
	}
	if err := (&Gateway{}).verifySolanaContext(json.RawMessage(`100`), 100); err == nil {
		t.Fatal("missing context must not pass a context-bearing method")
	}
}

func TestSolanaConfigValidationAndPrecision(t *testing.T) {
	snapshot := head.Snapshot{Chain: "test", Heads: map[string]head.Head{head.Finalized: {Family: "solana", Commitment: head.Finalized, Number: 100}}}
	for _, testCase := range []struct {
		name, config string
		wantError    bool
		floor        uint64
	}{
		{"null config", ` null `, false, 100},
		{"large integer", `{"minContextSlot":9007199254740993,"dataSlice":{"offset":9007199254740993}}`, false, 9007199254740993},
		{"fraction", `{"minContextSlot":100.5}`, true, 0},
		{"negative", `{"minContextSlot":-1}`, true, 0},
		{"overflow", `{"minContextSlot":18446744073709551616}`, true, 0},
		{"string floor", `{"minContextSlot":"120"}`, true, 0},
		{"null floor", `{"minContextSlot":null}`, true, 0},
		{"wrong commitment type", `{"commitment":1}`, true, 0},
		{"null commitment", `{"commitment":null}`, true, 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			prepared, rpcErr := (&Gateway{}).transformSolanaRequest(snapshot, jsonrpc.Request{Method: "getAccountInfo", Params: json.RawMessage(`["account",` + testCase.config + `]`)})
			if testCase.wantError {
				if rpcErr == nil || rpcErr.Code != jsonrpc.CodeInvalidParams {
					t.Fatalf("expected invalid params, got %v", rpcErr)
				}
				return
			}
			if rpcErr != nil || prepared.target.Number != testCase.floor {
				t.Fatalf("prepared=%+v error=%v", prepared, rpcErr)
			}
			if testCase.floor > 100 && !strings.Contains(string(prepared.parameters), `"offset":9007199254740993`) {
				t.Fatalf("lost precision: %s", prepared.parameters)
			}
		})
	}
}

func TestSolanaRejectsMethodsWithoutSlotFloorSupport(t *testing.T) {
	for _, method := range []string{"getSupply", "getTokenAccountBalance", "getTokenLargestAccounts", "getTokenSupply"} {
		t.Run(method, func(t *testing.T) {
			_, rpcErr := (&Gateway{}).transformSolanaRequest(head.Snapshot{}, jsonrpc.Request{Method: method})
			if rpcErr == nil || rpcErr.Code != jsonrpc.CodeUnsupportedConsistency {
				t.Fatalf("unexpected error: %v", rpcErr)
			}
		})
	}
}

func TestEVMRejectsMalformedFiltersAndReferences(t *testing.T) {
	runtime := testRuntime(t, "evm")
	for _, testCase := range []struct{ method, parameters string }{
		{"eth_getLogs", `[null]`},
		{"eth_getLogs", `[{"blockHash":"` + testHash('a') + `","fromBlock":"latest"}]`},
		{"eth_getLogs", `[{"fromBlock":"latest","toBlock":null}]`},
		{"eth_getBalance", `["address",{"blockHash":"` + testHash('a') + `","blockNumber":"0x64"}]`},
		{"eth_getBalance", `["address",{"blockHash":"` + testHash('a') + `","requireCanonical":"true"}]`},
	} {
		t.Run(testCase.parameters, func(t *testing.T) {
			_, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, evmSnapshot(), jsonrpc.Request{Method: testCase.method, Params: json.RawMessage(testCase.parameters)})
			if rpcErr == nil || rpcErr.Code != jsonrpc.CodeInvalidParams {
				t.Fatalf("unexpected error: %v", rpcErr)
			}
		})
	}
}

func FuzzTransformEVM(f *testing.F) {
	f.Add("eth_getBalance", []byte(`["0x0000000000000000000000000000000000000000","latest"]`))
	f.Add("eth_call", []byte(`[{"to":"0x0000000000000000000000000000000000000000"},"pending"]`))
	f.Add("eth_sendRawTransaction", []byte(`["0x00"]`))
	runtime := testRuntime(f, "evm")
	snapshot := evmSnapshot()
	f.Fuzz(func(t *testing.T, method string, params []byte) {
		prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, snapshot, jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: method, Params: params})
		if rpcErr != nil {
			return
		}
		if len(prepared.parameters) > 0 && !json.Valid(prepared.parameters) {
			t.Fatalf("request transformer produced invalid JSON: %q", prepared.parameters)
		}
	})
}

func FuzzTransformSolana(f *testing.F) {
	f.Add("getAccountInfo", []byte(`["account",{"commitment":"confirmed","minContextSlot":1}]`))
	f.Add("sendTransaction", []byte(`["transaction"]`))
	runtime := testRuntime(f, "solana")
	snapshot := head.Snapshot{Chain: "test", Heads: map[string]head.Head{
		head.Processed: {Chain: "test", Family: "solana", Commitment: head.Processed, Number: 101},
		head.Confirmed: {Chain: "test", Family: "solana", Commitment: head.Confirmed, Number: 100},
		head.Finalized: {Chain: "test", Family: "solana", Commitment: head.Finalized, Number: 99},
	}}
	f.Fuzz(func(t *testing.T, method string, params []byte) {
		prepared, rpcErr := (&Gateway{}).transformRequest(context.Background(), runtime, snapshot, jsonrpc.Request{JSONRPC: "2.0", ID: json.RawMessage(`1`), Method: method, Params: params})
		if rpcErr != nil {
			return
		}
		if len(prepared.parameters) > 0 && !json.Valid(prepared.parameters) {
			t.Fatalf("request transformer produced invalid JSON: %q", prepared.parameters)
		}
	})
}

type testHelper interface {
	Helper()
	Fatal(...any)
}

func testRuntime(t testHelper, family string) *chain.Runtime {
	t.Helper()
	tel, err := telemetry.NewTelemetry(config.DatadogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return chain.NewRuntime(config.ChainConfig{Name: "test", Family: family, PollInterval: config.Duration(time.Second)}, tel)
}

func evmSnapshot() head.Snapshot {
	return head.Snapshot{Chain: "test", Heads: map[string]head.Head{
		head.Latest:    {Chain: "test", Family: "evm", Commitment: head.Latest, Number: 100, Hash: testHash('a')},
		head.Safe:      {Chain: "test", Family: "evm", Commitment: head.Safe, Number: 99, Hash: testHash('b')},
		head.Finalized: {Chain: "test", Family: "evm", Commitment: head.Finalized, Number: 90, Hash: testHash('c')},
	}}
}

func testHash(char byte) string { return "0x" + strings.Repeat(string(char), 64) }

func mustMarshalParameters(t *testing.T, parameters []json.RawMessage) json.RawMessage {
	t.Helper()
	encodedParameters, err := jsonrpc.MarshalParams(parameters)
	if err != nil {
		t.Fatal(err)
	}
	return encodedParameters
}
