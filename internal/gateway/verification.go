package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/utils"
)

type evmResponseVerification struct {
	runtime  *chain.Runtime
	prepared preparedRequest
	payload  json.RawMessage
}

func (g *Gateway) verifyEVMResponse(verification evmResponseVerification) error {
	runtime, prepared, payload := verification.runtime, verification.prepared, verification.payload
	if prepared.requireEIP1898 {
		// Null is not state at the requested block; providers must report unavailable
		// state as an error. Do not turn a lagging backend into a successful read.
		if bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
			return errors.New("pinned state response is null")
		}
		return nil
	}
	if prepared.method == "eth_getBlockByHash" {
		block, err := runtime.ParseEVMHead(chain.EVMHeadParseRequest{Header: payload})
		if err != nil {
			return fmt.Errorf("read pinned block response: %w", err)
		}
		if !strings.EqualFold(block.Hash, prepared.target.Hash) {
			return errors.New("block response does not match requested hash")
		}
		return nil
	}
	if prepared.method == "eth_getLogs" {
		var logs []struct {
			BlockHash   string `json:"blockHash"`
			BlockNumber string `json:"blockNumber"`
			Removed     bool   `json:"removed"`
		}
		if err := json.Unmarshal(payload, &logs); err != nil {
			return fmt.Errorf("read pinned log response: %w", err)
		}
		if logs == nil {
			return errors.New("pinned logs response is null")
		}
		for _, log := range logs {
			if log.Removed {
				return errors.New("log response contains removed logs")
			}
			if prepared.logRange != nil {
				number, err := utils.ParseEVMQuantity(log.BlockNumber)
				if err != nil || number < prepared.logRange.from || number > prepared.logRange.to {
					return errors.New("log response exceeds requested block range")
				}
			} else if !strings.EqualFold(log.BlockHash, prepared.target.Hash) {
				return errors.New("log response does not match requested block")
			}
		}
	}
	if prepared.method == "eth_getBlockTransactionCountByHash" || prepared.method == "eth_getUncleCountByBlockHash" {
		var count string
		if err := json.Unmarshal(payload, &count); err != nil {
			return fmt.Errorf("read pinned block count: %w", err)
		}
		if _, err := utils.ParseEVMQuantity(count); err != nil {
			return fmt.Errorf("validate pinned block count: %w", err)
		}
	}
	if prepared.method == "eth_getTransactionByBlockHashAndIndex" && !bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
		var transaction struct {
			BlockHash string `json:"blockHash"`
		}
		if err := json.Unmarshal(payload, &transaction); err != nil {
			return fmt.Errorf("read pinned transaction: %w", err)
		}
		if !strings.EqualFold(transaction.BlockHash, prepared.target.Hash) {
			return errors.New("transaction response does not match requested block")
		}
	}
	return nil
}
