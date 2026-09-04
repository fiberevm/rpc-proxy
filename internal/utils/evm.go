// Package utils contains small, dependency-free helpers shared by service packages.
package utils

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ParseEVMQuantity validates and parses a canonical 0x-prefixed EVM quantity.
func ParseEVMQuantity(quantity string) (uint64, error) {
	if !strings.HasPrefix(quantity, "0x") || len(quantity) < 3 {
		return 0, errors.New("quantity must be 0x-prefixed")
	}
	if len(quantity) > 3 && quantity[2] == '0' {
		return 0, errors.New("quantity has leading zero")
	}
	parsedQuantity, err := strconv.ParseUint(quantity[2:], 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse EVM quantity: %w", err)
	}
	return parsedQuantity, nil
}

// FormatEVMQuantity formats an unsigned integer as a canonical EVM quantity.
func FormatEVMQuantity(number uint64) string {
	return fmt.Sprintf("0x%x", number)
}

// IsEVMHash reports whether hash is a 32-byte 0x-prefixed hexadecimal value.
func IsEVMHash(hash string) bool {
	if len(hash) != 66 || !strings.HasPrefix(hash, "0x") {
		return false
	}
	_, err := hex.DecodeString(hash[2:])
	return err == nil
}
