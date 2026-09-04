package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

type preparedRequest struct {
	method            string
	parameters        json.RawMessage
	target            *head.Head
	requireEIP1898    bool
	localResponse     json.RawMessage
	verifyBoundaries  []head.Head
	verifyContextSlot bool
	unwrapContext     bool
	receiptHash       string
}

type evmBlockMethod struct {
	blockIndex  int
	replacement string
}

type evmBlockTargetRequest struct {
	runtime        *chain.Runtime
	snapshot       head.Snapshot
	blockReference json.RawMessage
}

func (g *Gateway) transformRequest(ctx context.Context, runtime *chain.Runtime, snapshot head.Snapshot, request jsonrpc.Request) (preparedRequest, *jsonrpc.Error) {
	if g.isWriteMethod(request.Method, runtime.Config.Family) {
		return preparedRequest{}, jsonrpc.WritesDisabled(request.Method)
	}
	if runtime.Config.Family == "evm" {
		return g.transformEVMRequest(ctx, runtime, snapshot, request)
	}
	return g.transformSolanaRequest(snapshot, request)
}

func (g *Gateway) transformEVMRequest(ctx context.Context, runtime *chain.Runtime, snapshot head.Snapshot, request jsonrpc.Request) (preparedRequest, *jsonrpc.Error) {
	parameters, rpcErr := jsonrpc.ParseParams(request.Params)
	if rpcErr != nil {
		return preparedRequest{}, rpcErr
	}

	if blockIndex, supported := g.getEVMStateBlockIndex(request.Method); supported {
		if len(parameters) < blockIndex {
			return preparedRequest{}, jsonrpc.InvalidParams("missing required parameters")
		}
		for len(parameters) <= blockIndex {
			parameters = append(parameters, json.RawMessage(`"latest"`))
		}

		targetRequest := evmBlockTargetRequest{runtime: runtime, snapshot: snapshot, blockReference: parameters[blockIndex]}
		target, targetErr := g.getEVMBlock(ctx, targetRequest)
		if targetErr != nil {
			return preparedRequest{}, targetErr
		}
		blockIdentifier, err := json.Marshal(map[string]any{"blockHash": target.Hash, "requireCanonical": true})
		if err != nil {
			return preparedRequest{}, jsonrpc.InternalError("encode EIP-1898 block identifier")
		}
		parameters[blockIndex] = blockIdentifier

		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		return preparedRequest{method: request.Method, parameters: encodedParameters, target: &target, requireEIP1898: true}, nil
	}

	if blockMethod, supported := g.getEVMBlockMethod(request.Method); supported {
		if len(parameters) <= blockMethod.blockIndex {
			return preparedRequest{}, jsonrpc.InvalidParams("missing block parameter")
		}

		targetRequest := evmBlockTargetRequest{runtime: runtime, snapshot: snapshot, blockReference: parameters[blockMethod.blockIndex]}
		target, targetErr := g.getEVMBlock(ctx, targetRequest)
		if targetErr != nil {
			return preparedRequest{}, targetErr
		}
		blockHash, err := json.Marshal(target.Hash)
		if err != nil {
			return preparedRequest{}, jsonrpc.InternalError("encode EVM block hash")
		}
		parameters[blockMethod.blockIndex] = blockHash

		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		return preparedRequest{method: blockMethod.replacement, parameters: encodedParameters, target: &target}, nil
	}

	if request.Method == "eth_blockNumber" {
		target, targetErr := g.getSnapshotHead(snapshot, head.Latest)
		if targetErr != nil {
			return preparedRequest{}, targetErr
		}
		blockNumber, err := json.Marshal(utils.FormatEVMQuantity(target.Number))
		if err != nil {
			return preparedRequest{}, jsonrpc.InternalError("encode EVM block number")
		}
		return preparedRequest{method: request.Method, localResponse: blockNumber, target: &target}, nil
	}

	if request.Method == "eth_getLogs" {
		return g.transformEVMLogs(ctx, runtime, snapshot, parameters)
	}

	if request.Method == "eth_feeHistory" {
		if len(parameters) < 2 {
			return preparedRequest{}, jsonrpc.InvalidParams("eth_feeHistory requires blockCount and newestBlock")
		}
		var newestBlock string
		if json.Unmarshal(parameters[1], &newestBlock) != nil || newestBlock == "" {
			return preparedRequest{}, jsonrpc.InvalidParams("newestBlock must be a block number or tag")
		}
		targetRequest := evmBlockTargetRequest{runtime: runtime, snapshot: snapshot, blockReference: parameters[1]}
		target, targetErr := g.getEVMBlock(ctx, targetRequest)
		if targetErr != nil {
			return preparedRequest{}, targetErr
		}
		blockNumber, err := json.Marshal(utils.FormatEVMQuantity(target.Number))
		if err != nil {
			return preparedRequest{}, jsonrpc.InternalError("encode EVM block number")
		}
		parameters[1] = blockNumber

		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		return preparedRequest{method: request.Method, parameters: encodedParameters, target: &target, verifyBoundaries: []head.Head{target}}, nil
	}

	if request.Method == "eth_getTransactionReceipt" {
		if len(parameters) != 1 {
			return preparedRequest{}, jsonrpc.InvalidParams("eth_getTransactionReceipt requires one transaction hash")
		}
		var transactionHash string
		if err := json.Unmarshal(parameters[0], &transactionHash); err != nil || !utils.IsEVMHash(transactionHash) {
			return preparedRequest{}, jsonrpc.InvalidParams("invalid transaction hash")
		}
		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		// Receipt lookup has no block selector and is not an EIP-1898 state read.
		return preparedRequest{method: request.Method, parameters: encodedParameters, receiptHash: transactionHash}, nil
	}

	if g.isEVMHeadNeutralMethod(request.Method) {
		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		return preparedRequest{method: request.Method, parameters: encodedParameters}, nil
	}

	if g.isEVMExplicitHashMethod(request.Method) {
		if len(parameters) == 0 {
			return preparedRequest{}, jsonrpc.InvalidParams("missing block hash")
		}
		var blockHash string
		if err := json.Unmarshal(parameters[0], &blockHash); err != nil || !utils.IsEVMHash(blockHash) {
			return preparedRequest{}, jsonrpc.InvalidParams("invalid block hash")
		}

		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		target := head.Head{Chain: runtime.Config.Name, Family: "evm", Commitment: "explicit", Hash: blockHash}
		return preparedRequest{method: request.Method, parameters: encodedParameters, target: &target}, nil
	}

	return preparedRequest{}, jsonrpc.UnsupportedConsistency(request.Method, "method is not in the strict EVM read registry")
}

func (g *Gateway) transformEVMLogs(ctx context.Context, runtime *chain.Runtime, snapshot head.Snapshot, parameters []json.RawMessage) (preparedRequest, *jsonrpc.Error) {
	if len(parameters) != 1 {
		return preparedRequest{}, jsonrpc.InvalidParams("eth_getLogs requires one filter object")
	}

	var filter map[string]json.RawMessage
	if err := json.Unmarshal(parameters[0], &filter); err != nil || filter == nil {
		return preparedRequest{}, jsonrpc.InvalidParams("invalid eth_getLogs filter")
	}
	if rawHash, exists := filter["blockHash"]; exists {
		if filter["fromBlock"] != nil || filter["toBlock"] != nil {
			return preparedRequest{}, jsonrpc.InvalidParams("blockHash cannot be combined with fromBlock or toBlock")
		}
		var blockHash string
		if err := json.Unmarshal(rawHash, &blockHash); err != nil || !utils.IsEVMHash(blockHash) {
			return preparedRequest{}, jsonrpc.InvalidParams("invalid blockHash")
		}
		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		target := head.Head{Chain: runtime.Config.Name, Family: "evm", Commitment: "explicit", Hash: blockHash}
		return preparedRequest{method: "eth_getLogs", parameters: encodedParameters, target: &target}, nil
	}

	fromBlock := filter["fromBlock"]
	if len(fromBlock) == 0 {
		fromBlock = json.RawMessage(`"latest"`)
	}
	toBlock := filter["toBlock"]
	if len(toBlock) == 0 {
		toBlock = json.RawMessage(`"latest"`)
	}
	var fromSelector, toSelector string
	if json.Unmarshal(fromBlock, &fromSelector) != nil || fromSelector == "" || json.Unmarshal(toBlock, &toSelector) != nil || toSelector == "" {
		return preparedRequest{}, jsonrpc.InvalidParams("log bounds must be block numbers or tags")
	}
	fromRequest := evmBlockTargetRequest{runtime: runtime, snapshot: snapshot, blockReference: fromBlock}
	fromTarget, fromErr := g.getEVMBlock(ctx, fromRequest)
	if fromErr != nil {
		return preparedRequest{}, fromErr
	}
	toRequest := evmBlockTargetRequest{runtime: runtime, snapshot: snapshot, blockReference: toBlock}
	toTarget, toErr := g.getEVMBlock(ctx, toRequest)
	if toErr != nil {
		return preparedRequest{}, toErr
	}
	if fromTarget.Number > toTarget.Number {
		return preparedRequest{}, jsonrpc.InvalidParams("fromBlock exceeds toBlock")
	}

	if fromTarget.Hash == toTarget.Hash {
		delete(filter, "fromBlock")
		delete(filter, "toBlock")
		blockHash, err := json.Marshal(toTarget.Hash)
		if err != nil {
			return preparedRequest{}, jsonrpc.InternalError("encode EVM log block hash")
		}
		filter["blockHash"] = blockHash

		return g.prepareEVMLogRequest(filter, toTarget, nil)
	}

	fromNumber, err := json.Marshal(utils.FormatEVMQuantity(fromTarget.Number))
	if err != nil {
		return preparedRequest{}, jsonrpc.InternalError("encode EVM log range start")
	}
	toNumber, err := json.Marshal(utils.FormatEVMQuantity(toTarget.Number))
	if err != nil {
		return preparedRequest{}, jsonrpc.InternalError("encode EVM log range end")
	}
	filter["fromBlock"] = fromNumber
	filter["toBlock"] = toNumber

	boundaries := []head.Head{fromTarget}
	if !strings.EqualFold(fromTarget.Hash, toTarget.Hash) {
		boundaries = append(boundaries, toTarget)
	}
	return g.prepareEVMLogRequest(filter, toTarget, boundaries)
}

func (g *Gateway) prepareEVMLogRequest(filter map[string]json.RawMessage, target head.Head, boundaries []head.Head) (preparedRequest, *jsonrpc.Error) {
	encodedFilter, err := json.Marshal(filter)
	if err != nil {
		return preparedRequest{}, jsonrpc.InternalError("encode EVM log filter")
	}
	encodedParameters, rpcErr := g.encodeParameters([]json.RawMessage{encodedFilter})
	if rpcErr != nil {
		return preparedRequest{}, rpcErr
	}
	return preparedRequest{method: "eth_getLogs", parameters: encodedParameters, target: &target, verifyBoundaries: boundaries}, nil
}

func (g *Gateway) transformSolanaRequest(snapshot head.Snapshot, request jsonrpc.Request) (preparedRequest, *jsonrpc.Error) {
	parameters, rpcErr := jsonrpc.ParseParams(request.Params)
	if rpcErr != nil {
		return preparedRequest{}, rpcErr
	}

	if configIndex, supported := g.getSolanaConfigIndex(request.Method); supported {
		if len(parameters) < configIndex {
			return preparedRequest{}, jsonrpc.InvalidParams("missing required parameters")
		}
		for len(parameters) <= configIndex {
			parameters = append(parameters, json.RawMessage(`{}`))
		}

		requestConfig := map[string]json.RawMessage{}
		if !bytes.Equal(bytes.TrimSpace(parameters[configIndex]), []byte("null")) {
			if err := json.Unmarshal(parameters[configIndex], &requestConfig); err != nil {
				return preparedRequest{}, jsonrpc.InvalidParams("invalid Solana configuration object")
			}
		}
		commitment := head.Finalized
		if configuredCommitment, exists := requestConfig["commitment"]; exists {
			if bytes.Equal(configuredCommitment, []byte("null")) || json.Unmarshal(configuredCommitment, &commitment) != nil {
				return preparedRequest{}, jsonrpc.InvalidParams("invalid Solana commitment")
			}
		}
		if commitment != head.Processed && commitment != head.Confirmed && commitment != head.Finalized {
			return preparedRequest{}, jsonrpc.InvalidParams("invalid Solana commitment")
		}
		target, targetErr := g.getSnapshotHead(snapshot, commitment)
		if targetErr != nil {
			return preparedRequest{}, targetErr
		}

		minimumSlot := target.Number
		if encodedSlot, exists := requestConfig["minContextSlot"]; exists {
			var callerSlot *uint64
			if json.Unmarshal(encodedSlot, &callerSlot) != nil || callerSlot == nil {
				return preparedRequest{}, jsonrpc.InvalidParams("minContextSlot must be an unsigned integer")
			}
			minimumSlot = max(minimumSlot, *callerSlot)
		}
		// Keep arbitrary configuration numbers as raw JSON; float64 loses uint64 precision.
		requestConfig["commitment"] = json.RawMessage(`"` + commitment + `"`)
		requestConfig["minContextSlot"] = json.RawMessage(fmt.Sprint(minimumSlot))
		target.Number = minimumSlot
		unwrapContext := false
		if request.Method == "getProgramAccounts" {
			var withContext bool
			if encodedFlag, exists := requestConfig["withContext"]; exists {
				if bytes.Equal(encodedFlag, []byte("null")) || json.Unmarshal(encodedFlag, &withContext) != nil {
					return preparedRequest{}, jsonrpc.InvalidParams("withContext must be a boolean")
				}
			}
			unwrapContext = !withContext
			requestConfig["withContext"] = json.RawMessage("true")
		}
		encodedConfig, err := json.Marshal(requestConfig)
		if err != nil {
			return preparedRequest{}, jsonrpc.InternalError("encode Solana configuration")
		}
		parameters[configIndex] = encodedConfig

		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		return preparedRequest{method: request.Method, parameters: encodedParameters, target: &target, verifyContextSlot: g.hasSolanaContext(request.Method), unwrapContext: unwrapContext}, nil
	}

	if g.isSolanaHeadNeutralMethod(request.Method) {
		encodedParameters, encodeErr := g.encodeParameters(parameters)
		if encodeErr != nil {
			return preparedRequest{}, encodeErr
		}
		return preparedRequest{method: request.Method, parameters: encodedParameters}, nil
	}

	return preparedRequest{}, jsonrpc.UnsupportedConsistency(request.Method, "method is not in the strict Solana read registry")
}

func (g *Gateway) getSnapshotHead(snapshot head.Snapshot, commitment string) (head.Head, *jsonrpc.Error) {
	acceptedHead, ok := snapshot.Get(commitment)
	if !ok || acceptedHead.IsZero() {
		return head.Head{}, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": snapshot.Chain, "commitment": commitment})
	}
	return acceptedHead, nil
}

func (g *Gateway) getEVMBlock(ctx context.Context, request evmBlockTargetRequest) (head.Head, *jsonrpc.Error) {
	target, err := request.runtime.GetEVMBlock(ctx, request.blockReference, request.snapshot)
	if err == nil {
		return target, nil
	}
	if errors.Is(err, chain.ErrInvalidEVMBlockReference) {
		return head.Head{}, jsonrpc.InvalidParams(err.Error())
	}
	if errors.Is(err, chain.ErrUnsupportedEVMBlockReference) {
		return head.Head{}, jsonrpc.UnsupportedConsistency("pending", err.Error())
	}
	if errors.Is(err, chain.ErrEVMBlockUnavailable) {
		return head.Head{}, jsonrpc.ConsistencyUnavailable(map[string]any{"chain": request.runtime.Config.Name, "reason": "requested block is unavailable"})
	}
	return head.Head{}, jsonrpc.InternalError("get EVM block target")
}

func (g *Gateway) isWriteMethod(method string, family string) bool {
	if family == "evm" {
		if method == "eth_accounts" {
			return true
		}
		for _, prefix := range []string{"eth_send", "eth_sign", "personal_", "wallet_", "miner_", "admin_", "engine_", "txpool_"} {
			if strings.HasPrefix(method, prefix) {
				return true
			}
		}
		return false
	}
	return method == "sendTransaction" || method == "sendRawTransaction" || method == "requestAirdrop"
}

func (g *Gateway) verifySolanaContext(response json.RawMessage, minimumSlot uint64) error {
	trimmedResponse := strings.TrimSpace(string(response))
	if trimmedResponse == "" || trimmedResponse[0] != '{' {
		return errors.New("Solana response is missing its context envelope")
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(response, &envelope); err != nil {
		return fmt.Errorf("decode Solana context: %w", err)
	}
	rawContext, exists := envelope["context"]
	if !exists || string(rawContext) == "null" {
		return errors.New("Solana response is missing its context")
	}
	var responseContext struct {
		Slot *uint64 `json:"slot"`
	}
	if err := json.Unmarshal(rawContext, &responseContext); err != nil {
		return fmt.Errorf("decode Solana response context: %w", err)
	}
	if responseContext.Slot == nil {
		return errors.New("Solana response contains an invalid context slot")
	}
	if *responseContext.Slot < minimumSlot {
		return fmt.Errorf("response context slot %d is below required slot %d", *responseContext.Slot, minimumSlot)
	}
	return nil
}

func (g *Gateway) encodeParameters(parameters []json.RawMessage) (json.RawMessage, *jsonrpc.Error) {
	encodedParameters, err := jsonrpc.MarshalParams(parameters)
	if err != nil {
		return nil, jsonrpc.InternalError("encode JSON-RPC parameters")
	}
	return encodedParameters, nil
}

func (g *Gateway) getEVMStateBlockIndex(method string) (int, bool) {
	switch method {
	case "eth_getBalance", "eth_getTransactionCount", "eth_getCode", "eth_call":
		return 1, true
	case "eth_getStorageAt", "eth_getProof":
		return 2, true
	default:
		return 0, false
	}
}

func (g *Gateway) getEVMBlockMethod(method string) (evmBlockMethod, bool) {
	switch method {
	case "eth_getBlockByNumber":
		return evmBlockMethod{blockIndex: 0, replacement: "eth_getBlockByHash"}, true
	case "eth_getBlockTransactionCountByNumber":
		return evmBlockMethod{blockIndex: 0, replacement: "eth_getBlockTransactionCountByHash"}, true
	case "eth_getTransactionByBlockNumberAndIndex":
		return evmBlockMethod{blockIndex: 0, replacement: "eth_getTransactionByBlockHashAndIndex"}, true
	case "eth_getUncleByBlockNumberAndIndex":
		return evmBlockMethod{blockIndex: 0, replacement: "eth_getUncleByBlockHashAndIndex"}, true
	case "eth_getUncleCountByBlockNumber":
		return evmBlockMethod{blockIndex: 0, replacement: "eth_getUncleCountByBlockHash"}, true
	default:
		return evmBlockMethod{}, false
	}
}

func (g *Gateway) isEVMHeadNeutralMethod(method string) bool {
	switch method {
	case "eth_chainId", "net_version", "web3_clientVersion":
		return true
	default:
		return false
	}
}

func (g *Gateway) isEVMExplicitHashMethod(method string) bool {
	switch method {
	case "eth_getBlockByHash", "eth_getBlockTransactionCountByHash", "eth_getTransactionByBlockHashAndIndex", "eth_getUncleByBlockHashAndIndex", "eth_getUncleCountByBlockHash":
		return true
	default:
		return false
	}
}

func (g *Gateway) getSolanaConfigIndex(method string) (int, bool) {
	switch method {
	case "getBlockHeight", "getEpochInfo", "getLatestBlockhash", "getSlot", "getSlotLeader", "getStakeMinimumDelegation", "getTransactionCount":
		return 0, true
	case "getAccountInfo", "getBalance", "getFeeForMessage", "getMultipleAccounts", "getProgramAccounts", "getSignaturesForAddress", "isBlockhashValid", "simulateTransaction":
		return 1, true
	case "getTokenAccountsByDelegate", "getTokenAccountsByOwner":
		return 2, true
	default:
		return 0, false
	}
}

func (g *Gateway) hasSolanaContext(method string) bool {
	switch method {
	case "getBlockHeight", "getEpochInfo", "getSlot", "getSlotLeader", "getTransactionCount", "getSignaturesForAddress":
		return false
	default:
		return true
	}
}

func (g *Gateway) isSolanaHeadNeutralMethod(method string) bool {
	switch method {
	case "getGenesisHash", "getVersion", "getBlock", "getBlockCommitment", "getBlockTime", "getHealth", "getEpochSchedule":
		return true
	default:
		return false
	}
}
