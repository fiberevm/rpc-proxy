package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadImportsSecretsAndAppliesDefaults(t *testing.T) {
	t.Setenv("TEST_REDIS_URL", "redis://127.0.0.1:6379/0")
	t.Setenv("TEST_RPC_URL", "https://example.invalid/rpc")
	t.Setenv("TEST_RPC_AUTH", "Bearer secret")
	path := filepath.Join(t.TempDir(), "rpc-proxy.yaml")
	contents := []byte(`
redis:
  url_env: TEST_REDIS_URL
chains:
  - name: ethereum
    family: evm
    chain_id: "0x1"
    upstreams:
      - id: primary
        http_url_env: TEST_RPC_URL
        header_envs:
          Authorization: TEST_RPC_AUTH
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.URL != "redis://127.0.0.1:6379/0" || cfg.Server.RequestTimeout.Value() != 5*time.Second {
		t.Fatalf("defaults or Redis environment not applied: %+v", cfg)
	}
	if cfg.Cache.Disabled || cfg.Cache.MaxEntries != 4096 || cfg.Cache.MaxBytes != 64<<20 || cfg.Cache.MaxEntryBytes != 1<<20 || cfg.Cache.TTL.Value() != 5*time.Minute {
		t.Fatalf("cache defaults not applied: %+v", cfg.Cache)
	}
	upstream := cfg.Chains[0].Upstreams[0]
	if upstream.HTTPURL != "https://example.invalid/rpc" || upstream.Headers["Authorization"] != "Bearer secret" {
		t.Fatalf("upstream environment not loaded: %+v", upstream)
	}
	if cfg.Chains[0].MaxHeadAge.Value() != 24*time.Second || upstream.MaxConcurrency != 256 {
		t.Fatalf("chain defaults not applied: %+v", cfg.Chains[0])
	}
	chain := cfg.Chains[0]
	if chain.HeadRequestTimeout.Value() != 2*time.Second || chain.WebsocketIdleTimeout.Value() != 18*time.Second {
		t.Fatalf("head tracking defaults not applied: %+v", chain)
	}
}

func TestCacheAndStaticIdentityConfigurationValidation(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*Config)
		wantError string
	}{
		{"entry count", func(cfg *Config) { cfg.Cache.MaxEntries = -1 }, "cache"},
		{"byte limit", func(cfg *Config) { cfg.Cache.MaxBytes = -1 }, "cache"},
		{"entry byte limit", func(cfg *Config) { cfg.Cache.MaxEntryBytes = -1 }, "cache"},
		{"oversized entry limit", func(cfg *Config) { cfg.Cache.MaxEntryBytes = cfg.Cache.MaxBytes + 1 }, "cache"},
		{"ttl", func(cfg *Config) { cfg.Cache.TTL = -1 }, "cache"},
		{"invalid identity", func(cfg *Config) { cfg.Chains[0].ChainID = "ethereum" }, "chain_id"},
		{"noncanonical identity", func(cfg *Config) { cfg.Chains[0].ChainID = "0x01" }, "chain_id"},
		{"disabled", func(cfg *Config) { cfg.Cache.Disabled = true }, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := Load(filepath.Join("..", "..", "config.example.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			test.configure(cfg)
			err = cfg.Validate()
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("expected %s error, got %v", test.wantError, err)
			}
		})
	}
}

func TestHeadTrackingTimingsMustBePositiveAndFresh(t *testing.T) {
	for _, testCase := range []struct{ setting, duration string }{
		{"websocket_idle_timeout", "-1s"},
		{"websocket_idle_timeout", "24s"},
		{"head_request_timeout", "-1s"},
	} {
		t.Run(testCase.setting+testCase.duration, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			contents := "redis:\n  url: redis://127.0.0.1:6379/0\nchains:\n  - name: ethereum\n    family: evm\n    chain_id: '0x1'\n    " + testCase.setting + ": " + testCase.duration + "\n    upstreams:\n      - id: primary\n        http_url: https://example.invalid/rpc\n"
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), testCase.setting) {
				t.Fatalf("expected %s validation error, got %v", testCase.setting, err)
			}
		})
	}
}

func TestLoadRejectsMissingHeaderEnvironment(t *testing.T) {
	t.Setenv("TEST_REDIS_URL", "redis://127.0.0.1:6379/0")
	t.Setenv("TEST_RPC_URL", "https://example.invalid/rpc")
	path := filepath.Join(t.TempDir(), "rpc-proxy.yaml")
	contents := []byte(`
redis:
  url_env: TEST_REDIS_URL
chains:
  - name: ethereum
    family: evm
    chain_id: "0x1"
    upstreams:
      - id: primary
        http_url_env: TEST_RPC_URL
        header_envs:
          Authorization: DEFINITELY_MISSING_RPC_AUTH
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected missing header environment to fail configuration")
	}
}

func TestExampleConfigLoadsWithoutEnvironment(t *testing.T) {
	for _, name := range []string{"REDIS_URL", "ETHEREUM_PROVIDER_A_HTTP_URL", "ETHEREUM_PROVIDER_A_WS_URL", "ETHEREUM_PROVIDER_B_HTTP_URL", "ETHEREUM_PROVIDER_B_WS_URL", "SOLANA_PROVIDER_A_HTTP_URL", "SOLANA_PROVIDER_B_HTTP_URL"} {
		t.Setenv(name, "")
	}
	cfg, err := Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Chains) != 2 {
		t.Fatalf("expected two example chains, got %d", len(cfg.Chains))
	}
	if cfg.Redis.URL != "redis://127.0.0.1:6379/0" || cfg.Redis.URLEnv != "" {
		t.Fatalf("expected Redis URL directly from YAML: %+v", cfg.Redis)
	}
	expectedURLs := map[string]struct{ http, websocket string }{
		"ethereum/provider-a": {"https://ethereum-a.example.invalid/rpc", "wss://ethereum-a.example.invalid/ws"},
		"ethereum/provider-b": {"https://ethereum-b.example.invalid/rpc", "wss://ethereum-b.example.invalid/ws"},
		"solana/provider-a":   {"https://solana-a.example.invalid/rpc", ""},
		"solana/provider-b":   {"https://solana-b.example.invalid/rpc", ""},
	}
	for _, chain := range cfg.Chains {
		if len(chain.Upstreams) != 2 {
			t.Fatalf("expected two upstreams for %s", chain.Name)
		}
		for _, upstream := range chain.Upstreams {
			expected, exists := expectedURLs[chain.Name+"/"+upstream.ID]
			if !exists || upstream.HTTPURL != expected.http || upstream.WebsocketURL != expected.websocket || upstream.HTTPURLEnv != "" || upstream.WebsocketURLEnv != "" || len(upstream.HeaderEnvs) != 0 {
				t.Fatalf("expected upstream URLs directly from YAML: %+v", upstream)
			}
		}
	}
}

func TestLoadInlineAuthenticationHeaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rpc-proxy.yaml")
	contents := []byte(`
redis:
  url: "redis://127.0.0.1:6379/0"
chains:
  - name: ethereum
    family: evm
    chain_id: "0x1"
    upstreams:
      - id: primary
        http_url: "https://ethereum-a.example.invalid/rpc"
        websocket_url: "wss://ethereum-a.example.invalid/ws"
        headers:
          Authorization: "Bearer example-token"
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	upstream := cfg.Chains[0].Upstreams[0]
	if len(upstream.Headers) != 1 || upstream.Headers["Authorization"] != "Bearer example-token" || len(upstream.HeaderEnvs) != 0 {
		t.Fatalf("inline headers were not preserved: %+v", upstream)
	}
}

func TestLoadRejectsUnknownFieldsAndMultipleDocuments(t *testing.T) {
	for _, testCase := range []struct{ contents, wantError string }{
		{"server:\n  request_timout: 5s\n", "field request_timout not found"},
		{"chains:\n  - commitment_poll_interval: 12s\n", "field commitment_poll_interval not found"},
		{"chains:\n  - validation_interval: 5m\n", "field validation_interval not found"},
		{"server: {}\n---\nserver: {}\n", "exactly one YAML document"},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(testCase.contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil || !strings.Contains(err.Error(), testCase.wantError) {
			t.Fatalf("expected %q, got %v", testCase.wantError, err)
		}
	}
}
