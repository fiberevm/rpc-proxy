package quicknode

import (
	"strings"
	"testing"
)

func TestMultichainEndpoints(t *testing.T) {
	for _, test := range []struct {
		name, source, network, httpURL, websocketURL string
	}{
		{"ethereum to base", "https://example.quiknode.pro/token/", "base-mainnet", "https://example.base-mainnet.quiknode.pro/token/", "wss://example.base-mainnet.quiknode.pro/token/"},
		{"base to ethereum", "https://example.base-mainnet.quiknode.pro/token", "mainnet", "https://example.quiknode.pro/token", "wss://example.quiknode.pro/token"},
		{"solana", "https://example.base-mainnet.quiknode.pro/token/?key=shared", "solana-mainnet", "https://example.solana-mainnet.quiknode.pro/token/?key=shared", "wss://example.solana-mainnet.quiknode.pro/token/?key=shared"},
		{"bsc", "https://example.quiknode.pro/token/", "bsc", "https://example.bsc.quiknode.pro/token/", "wss://example.bsc.quiknode.pro/token/"},
		{"avalanche", "https://example.quiknode.pro/token/", "avalanche-mainnet", "https://example.avalanche-mainnet.quiknode.pro/token/ext/bc/C/rpc", "wss://example.avalanche-mainnet.quiknode.pro/token/ext/bc/C/ws"},
		{"avalanche testnet", "https://example.quiknode.pro/token/", "avalanche-testnet", "https://example.avalanche-testnet.quiknode.pro/token/ext/bc/C/rpc", "wss://example.avalanche-testnet.quiknode.pro/token/ext/bc/C/ws"},
		{"avalanche source", "https://example.avalanche-mainnet.quiknode.pro/token/ext/bc/C/rpc", "mainnet", "https://example.quiknode.pro/token", "wss://example.quiknode.pro/token"},
		{"avalanche trailing slash", "https://example.avalanche-mainnet.quiknode.pro/token/ext/bc/C/rpc/", "base-mainnet", "https://example.base-mainnet.quiknode.pro/token", "wss://example.base-mainnet.quiknode.pro/token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewService(test.source)
			if err != nil {
				t.Fatal(err)
			}
			endpoints, err := service.GetEndpoints(test.network)
			if err != nil {
				t.Fatal(err)
			}
			if endpoints.HTTPURL != test.httpURL || endpoints.WebsocketURL != test.websocketURL {
				t.Fatalf("unexpected endpoints: %+v", endpoints)
			}
			// Deriving another network must not mutate the reusable endpoint service.
			if _, err := service.GetEndpoints("bsc"); err != nil {
				t.Fatal(err)
			}
			again, err := service.GetEndpoints(test.network)
			if err != nil || again != endpoints {
				t.Fatalf("endpoint derivation mutated the shared URL: %+v, %v", again, err)
			}
		})
	}
}

func TestRejectsInvalidEndpointsWithoutExposingCredentials(t *testing.T) {
	for _, endpoint := range []string{
		"https://example.quiknode.pro/private-token%",
		"http://example.quiknode.pro/private-token",
		"wss://example.quiknode.pro/private-token",
		"https://example.quiknode.pro.evil.invalid/private-token",
		"https://example.invalid/private-token",
		"https://example.quiknode.pro:443/private-token",
		"https://user:private-token@example.quiknode.pro/token",
		"https://example.quiknode.pro/private-token#fragment",
		"https://example.quiknode.pro/",
		"https://example.quiknode.pro/private-token/extra",
		"https://example.quiknode.pro/private-token/ext/bc/C/rpc",
		"https://example.avalanche-mainnet.quiknode.pro/private-token//",
		"https://example.quiknode.pro/private-token%2Fextra",
		"https://EXAMPLE.quiknode.pro/private-token",
	} {
		if _, err := NewService(endpoint); err == nil || strings.Contains(err.Error(), "private-token") {
			t.Fatalf("expected credential-free validation error, got %v", err)
		}
	}
}

func TestRejectsInvalidNetworks(t *testing.T) {
	service, err := NewService("https://example.quiknode.pro/token/")
	if err != nil {
		t.Fatal(err)
	}
	for _, network := range []string{"", " base-mainnet", "Base-mainnet", "base.mainnet", "../base", "-base", "base-", strings.Repeat("a", 64)} {
		if _, err := service.GetEndpoints(network); err == nil {
			t.Fatalf("accepted invalid network %q", network)
		}
	}
}
