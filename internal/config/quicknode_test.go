package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadQuickNodeMultichain(t *testing.T) {
	for _, useEnvironment := range []bool{false, true} {
		t.Run(map[bool]string{false: "inline", true: "environment"}[useEnvironment], func(t *testing.T) {
			endpointSetting := "http_url: https://example.base-mainnet.quiknode.pro/token/"
			if useEnvironment {
				t.Setenv("TEST_QUICKNODE_URL", "https://example.base-mainnet.quiknode.pro/token/")
				endpointSetting = "http_url_env: TEST_QUICKNODE_URL"
			}
			contents := `
redis:
  url: redis://localhost:6379/0
clients: [wallet, indexer]
quicknode:
  ` + endpointSetting + `
  id: shared-provider
  max_concurrency: 42
chains:
  - name: ethereum
    family: evm
    chain_id: "0x1"
    quicknode_network: mainnet
    upstreams:
      - id: fallback
        http_url: https://fallback.example.invalid/rpc
  - name: base
    family: evm
    chain_id: "0x2105"
    quicknode_network: base-mainnet
  - name: solana
    family: solana
    genesis_hash: example-genesis
    quicknode_network: solana-mainnet
  - name: custom
    family: evm
    chain_id: "0x1234"
    upstreams:
      - id: custom-provider
        http_url: https://custom.example.invalid/rpc
`
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			for i, hostname := range []string{"example.quiknode.pro", "example.base-mainnet.quiknode.pro", "example.solana-mainnet.quiknode.pro"} {
				upstreams := cfg.Chains[i].Upstreams
				upstream := upstreams[len(upstreams)-1]
				if upstream.ID != "shared-provider" || upstream.MaxConcurrency != 42 || upstream.HTTPURL != "https://"+hostname+"/token/" {
					t.Fatalf("incorrect generated upstream for %s: %+v", cfg.Chains[i].Name, upstream)
				}
				wantWebsocket := ""
				if cfg.Chains[i].Family == "evm" {
					wantWebsocket = "wss://" + hostname + "/token/"
				}
				if upstream.WebsocketURL != wantWebsocket {
					t.Fatalf("incorrect websocket URL: %s", upstream.WebsocketURL)
				}
			}
			if len(cfg.Chains[0].Upstreams) != 2 || cfg.Chains[0].Upstreams[0].ID != "fallback" || len(cfg.Chains[3].Upstreams) != 1 || cfg.Chains[3].Upstreams[0].ID != "custom-provider" {
				t.Fatal("explicit upstreams or chain opt-in were not preserved")
			}
		})
	}
}

func TestLoadQuickNodeDefaultsAndValidation(t *testing.T) {
	t.Setenv("TEST_EMPTY_QUICKNODE_URL", "")
	for _, test := range []struct{ name, quicknode, chain, wantError string }{
		{"defaults", "quicknode:\n  http_url: https://example.quiknode.pro/token/\n", "", ""},
		{"disable websocket", "quicknode:\n  http_url: https://example.quiknode.pro/token/\n  websocket_disabled: true\n", "", ""},
		{"missing provider", "", "", "requires quicknode"},
		{"empty provider", "quicknode: {}\n", "", "QuickNode http_url"},
		{"missing environment", "quicknode:\n  http_url_env: TEST_EMPTY_QUICKNODE_URL\n", "", "TEST_EMPTY_QUICKNODE_URL"},
		{"invalid network", "quicknode:\n  http_url: https://example.quiknode.pro/token/\n", "    quicknode_network: 'Base Mainnet'\n", "network must be"},
		{"duplicate id", "quicknode:\n  http_url: https://example.quiknode.pro/token/\n", "    upstreams:\n      - id: quicknode\n        http_url: https://example.invalid/rpc\n", "duplicate upstream"},
		{"invalid concurrency", "quicknode:\n  http_url: https://example.quiknode.pro/token/\n  max_concurrency: -1\n", "", "max_concurrency"},
	} {
		t.Run(test.name, func(t *testing.T) {
			networkSetting := "    quicknode_network: mainnet\n"
			if strings.Contains(test.chain, "quicknode_network") {
				networkSetting = ""
			}
			contents := "redis:\n  url: redis://localhost:6379/0\n" + test.quicknode + "chains:\n  - name: ethereum\n    family: evm\n    chain_id: '0x1'\n" + networkSetting + test.chain
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("wanted %q, got %v", test.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			upstream := cfg.Chains[0].Upstreams[0]
			if upstream.ID != "quicknode" || upstream.MaxConcurrency != 256 || (upstream.WebsocketURL == "") != cfg.QuickNode.WebsocketDisabled {
				t.Fatalf("incorrect QuickNode defaults: %+v", upstream)
			}
		})
	}
}

func TestClientLabelsValidation(t *testing.T) {
	for _, clients := range [][]string{{""}, {"wallet", "wallet"}, {"anonymous"}, {"unknown"}, {"Wallet"}, {" wallet"}, {"wallet,method:injected"}, {strings.Repeat("a", 65)}} {
		cfg, err := Load(filepath.Join("..", "..", "config.example.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Clients = clients
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "client") {
			t.Fatalf("accepted invalid client labels %v: %v", clients, err)
		}
	}
}

func TestQuickNodeExampleLoads(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config.quicknode.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Chains) != 3 || len(cfg.Clients) != 2 {
		t.Fatal("QuickNode example did not configure its chains and client labels")
	}
	for _, chain := range cfg.Chains {
		if len(chain.Upstreams) != 1 || chain.Upstreams[0].ID != "quicknode" {
			t.Fatalf("missing generated upstream for %s", chain.Name)
		}
	}
}
