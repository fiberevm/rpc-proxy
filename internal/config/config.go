package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/utils"
	"gopkg.in/yaml.v3"
)

type Duration time.Duration

// UnmarshalYAML parses one Go duration string from a YAML scalar into typed configuration.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// Value returns the duration in the standard-library type expected by runtime services.
func (d Duration) Value() time.Duration { return time.Duration(d) }

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Redis   RedisConfig   `yaml:"redis"`
	Datadog DatadogConfig `yaml:"datadog"`
	Cache   CacheConfig   `yaml:"cache"`
	Chains  []ChainConfig `yaml:"chains"`
}

// CacheConfig bounds the process-local cache of verified RPC results and ancestry headers.
type CacheConfig struct {
	Disabled      bool     `yaml:"disabled"`
	MaxEntries    int      `yaml:"max_entries"`
	MaxBytes      int      `yaml:"max_bytes"`
	MaxEntryBytes int      `yaml:"max_entry_bytes"`
	TTL           Duration `yaml:"ttl"`
}

type ServerConfig struct {
	ListenAddress      string   `yaml:"listen_address"`
	AdminAddress       string   `yaml:"admin_address"`
	RequestTimeout     Duration `yaml:"request_timeout"`
	MaxBodyBytes       int64    `yaml:"max_body_bytes"`
	MaxBatchSize       int      `yaml:"max_batch_size"`
	WebsocketQueueSize int      `yaml:"websocket_queue_size"`
}

type RedisConfig struct {
	URLEnv          string   `yaml:"url_env"`
	URL             string   `yaml:"url"`
	KeyPrefix       string   `yaml:"key_prefix"`
	LeaderTTL       Duration `yaml:"leader_ttl"`
	StreamMaxLength int64    `yaml:"stream_max_length"`
}

type DatadogConfig struct {
	Enabled          bool   `yaml:"enabled"`
	StatsdAddress    string `yaml:"statsd_address"`
	AgentAddress     string `yaml:"agent_address"`
	Service          string `yaml:"service"`
	Environment      string `yaml:"environment"`
	Version          string `yaml:"version"`
	ProfilingEnabled bool   `yaml:"profiling_enabled"`
}

type ChainConfig struct {
	Name                 string           `yaml:"name"`
	Family               string           `yaml:"family"`
	ChainID              string           `yaml:"chain_id"`
	GenesisHash          string           `yaml:"genesis_hash"`
	BlockTime            Duration         `yaml:"block_time"`
	PollInterval         Duration         `yaml:"poll_interval"`
	HeadRequestTimeout   Duration         `yaml:"head_request_timeout"`
	WebsocketIdleTimeout Duration         `yaml:"websocket_idle_timeout"`
	MaxHeadAge           Duration         `yaml:"max_head_age"`
	ReorgDepth           int              `yaml:"reorg_depth"`
	Upstreams            []UpstreamConfig `yaml:"upstreams"`
}

type UpstreamConfig struct {
	ID              string            `yaml:"id"`
	HTTPURL         string            `yaml:"http_url"`
	HTTPURLEnv      string            `yaml:"http_url_env"`
	WebsocketURL    string            `yaml:"websocket_url"`
	WebsocketURLEnv string            `yaml:"websocket_url_env"`
	Headers         map[string]string `yaml:"headers"`
	HeaderEnvs      map[string]string `yaml:"header_envs"`
	MaxConcurrency  int               `yaml:"max_concurrency"`
}

// Load reads one YAML file, imports declared environment values, applies defaults, and validates the complete configuration.
func Load(path string) (*Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("config must contain exactly one YAML document")
	}
	cfg.applyDefaults()
	if err := cfg.getEnvironmentValues(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (cfg *Config) applyDefaults() {
	if cfg.Cache.MaxEntries == 0 {
		cfg.Cache.MaxEntries = 4096
	}
	if cfg.Cache.MaxBytes == 0 {
		cfg.Cache.MaxBytes = 64 << 20
	}
	if cfg.Cache.MaxEntryBytes == 0 {
		cfg.Cache.MaxEntryBytes = 1 << 20
	}
	if cfg.Cache.TTL == 0 {
		cfg.Cache.TTL = Duration(5 * time.Minute)
	}
	if cfg.Server.ListenAddress == "" {
		cfg.Server.ListenAddress = ":8080"
	}
	if cfg.Server.AdminAddress == "" {
		cfg.Server.AdminAddress = "127.0.0.1:8081"
	}
	if cfg.Server.RequestTimeout == 0 {
		cfg.Server.RequestTimeout = Duration(5 * time.Second)
	}
	if cfg.Server.MaxBodyBytes == 0 {
		cfg.Server.MaxBodyBytes = 10 << 20
	}
	if cfg.Server.MaxBatchSize == 0 {
		cfg.Server.MaxBatchSize = 100
	}
	if cfg.Server.WebsocketQueueSize == 0 {
		cfg.Server.WebsocketQueueSize = 256
	}
	if cfg.Redis.KeyPrefix == "" {
		cfg.Redis.KeyPrefix = "rpc-proxy"
	}
	if cfg.Redis.LeaderTTL == 0 {
		cfg.Redis.LeaderTTL = Duration(5 * time.Second)
	}
	if cfg.Redis.StreamMaxLength == 0 {
		cfg.Redis.StreamMaxLength = 4096
	}
	if cfg.Datadog.Service == "" {
		cfg.Datadog.Service = "rpc-proxy"
	}
	if cfg.Datadog.StatsdAddress == "" {
		cfg.Datadog.StatsdAddress = "127.0.0.1:8125"
	}
	if cfg.Datadog.AgentAddress == "" {
		cfg.Datadog.AgentAddress = "127.0.0.1:8126"
	}
	for i := range cfg.Chains {
		chain := &cfg.Chains[i]
		if chain.BlockTime == 0 {
			chain.BlockTime = Duration(12 * time.Second)
		}
		if chain.PollInterval == 0 {
			chain.PollInterval = Duration(2 * time.Second)
		}
		if chain.MaxHeadAge == 0 {
			chain.MaxHeadAge = Duration(2 * chain.BlockTime.Value())
		}
		if chain.HeadRequestTimeout == 0 {
			chain.HeadRequestTimeout = Duration(2 * time.Second)
		}
		if chain.WebsocketIdleTimeout == 0 {
			chain.WebsocketIdleTimeout = chain.MaxHeadAge - chain.MaxHeadAge/4
		}
		if chain.ReorgDepth == 0 {
			chain.ReorgDepth = 64
		}
		for j := range chain.Upstreams {
			if chain.Upstreams[j].MaxConcurrency == 0 {
				chain.Upstreams[j].MaxConcurrency = 256
			}
		}
	}
}

func (cfg *Config) getEnvironmentValues() error {
	if cfg.Redis.URL == "" && cfg.Redis.URLEnv != "" {
		cfg.Redis.URL = os.Getenv(cfg.Redis.URLEnv)
		if cfg.Redis.URL == "" {
			return fmt.Errorf("environment variable %s for redis url is empty", cfg.Redis.URLEnv)
		}
	}
	for i := range cfg.Chains {
		for j := range cfg.Chains[i].Upstreams {
			up := &cfg.Chains[i].Upstreams[j]
			if up.HTTPURL == "" && up.HTTPURLEnv != "" {
				up.HTTPURL = os.Getenv(up.HTTPURLEnv)
				if up.HTTPURL == "" {
					return fmt.Errorf("chain %q upstream %q: environment variable %s for http url is empty", cfg.Chains[i].Name, up.ID, up.HTTPURLEnv)
				}
			}
			if up.WebsocketURL == "" && up.WebsocketURLEnv != "" {
				up.WebsocketURL = os.Getenv(up.WebsocketURLEnv)
				if up.WebsocketURL == "" {
					return fmt.Errorf("chain %q upstream %q: environment variable %s for websocket url is empty", cfg.Chains[i].Name, up.ID, up.WebsocketURLEnv)
				}
			}
			if up.Headers == nil {
				up.Headers = map[string]string{}
			}
			for header, envName := range up.HeaderEnvs {
				headerValue := os.Getenv(envName)
				if headerValue == "" {
					return fmt.Errorf("chain %q upstream %q: environment variable %s is empty", cfg.Chains[i].Name, up.ID, envName)
				}
				up.Headers[header] = headerValue
			}
		}
	}
	return nil
}

// Validate checks all service, chain, and upstream settings before any runtime dependency is constructed.
func (cfg *Config) Validate() error {
	var problems []error
	if cfg.Cache.MaxEntries <= 0 || cfg.Cache.MaxBytes <= 0 || cfg.Cache.MaxEntryBytes <= 0 || cfg.Cache.MaxEntryBytes > cfg.Cache.MaxBytes || cfg.Cache.TTL.Value() <= 0 {
		problems = append(problems, errors.New("cache limits and ttl must be positive; max_entry_bytes must not exceed max_bytes"))
	}
	if cfg.Server.RequestTimeout.Value() <= 0 {
		problems = append(problems, errors.New("server request_timeout must be positive"))
	}
	if cfg.Server.MaxBodyBytes <= 0 || cfg.Server.MaxBatchSize <= 0 || cfg.Server.MaxBatchSize > 100 || cfg.Server.WebsocketQueueSize <= 0 {
		problems = append(problems, errors.New("server limits must be positive and max_batch_size must not exceed 100"))
	}
	if cfg.Redis.LeaderTTL.Value() <= 0 || cfg.Redis.StreamMaxLength <= 0 {
		problems = append(problems, errors.New("redis leader_ttl and stream_max_length must be positive"))
	}
	if cfg.Redis.URL == "" {
		problems = append(problems, errors.New("redis url is required"))
	}
	if parsed, err := url.ParseRequestURI(cfg.Redis.URL); err != nil {
		problems = append(problems, fmt.Errorf("invalid redis url: %w", err))
	} else if parsed.Scheme != "redis" && parsed.Scheme != "rediss" && parsed.Scheme != "unix" {
		problems = append(problems, errors.New("redis url must use redis, rediss, or unix scheme"))
	}
	if len(cfg.Chains) == 0 {
		problems = append(problems, errors.New("at least one chain is required"))
	}
	seenChains := map[string]bool{}
	for i := range cfg.Chains {
		chain := &cfg.Chains[i]
		if chain.Name == "" || seenChains[chain.Name] {
			problems = append(problems, fmt.Errorf("chain %d has an empty or duplicate name", i))
		}
		seenChains[chain.Name] = true
		if chain.Family != "evm" && chain.Family != "solana" {
			problems = append(problems, fmt.Errorf("chain %q: family must be evm or solana", chain.Name))
		}
		if chain.Family == "evm" {
			if _, err := utils.ParseEVMQuantity(chain.ChainID); err != nil {
				problems = append(problems, fmt.Errorf("chain %q: invalid chain_id: %w", chain.Name, err))
			}
		}
		if chain.Family == "solana" && chain.GenesisHash == "" {
			problems = append(problems, fmt.Errorf("chain %q: genesis_hash is required", chain.Name))
		}
		if chain.BlockTime.Value() <= 0 || chain.PollInterval.Value() <= 0 || chain.MaxHeadAge.Value() <= 0 || chain.ReorgDepth <= 0 {
			problems = append(problems, fmt.Errorf("chain %q: timing values and reorg_depth must be positive", chain.Name))
		}
		if chain.HeadRequestTimeout.Value() <= 0 {
			problems = append(problems, fmt.Errorf("chain %q: head_request_timeout must be positive", chain.Name))
		}
		if chain.Family == "evm" {
			if chain.WebsocketIdleTimeout.Value() <= 0 || chain.WebsocketIdleTimeout >= chain.MaxHeadAge {
				problems = append(problems, fmt.Errorf("chain %q: websocket_idle_timeout must be positive and less than max_head_age", chain.Name))
			}
		}
		if len(chain.Upstreams) == 0 {
			problems = append(problems, fmt.Errorf("chain %q: at least one upstream is required", chain.Name))
		}
		seenUpstreams := map[string]bool{}
		for _, up := range chain.Upstreams {
			if up.ID == "" || seenUpstreams[up.ID] {
				problems = append(problems, fmt.Errorf("chain %q has an empty or duplicate upstream id", chain.Name))
			}
			seenUpstreams[up.ID] = true
			if up.MaxConcurrency <= 0 {
				problems = append(problems, fmt.Errorf("chain %q upstream %q: max_concurrency must be positive", chain.Name, up.ID))
			}
			parsed, err := url.ParseRequestURI(up.HTTPURL)
			if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
				problems = append(problems, fmt.Errorf("chain %q upstream %q has invalid http url", chain.Name, up.ID))
			}
			if up.WebsocketURL != "" {
				parsed, err = url.ParseRequestURI(up.WebsocketURL)
				if err != nil || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
					problems = append(problems, fmt.Errorf("chain %q upstream %q has invalid websocket url", chain.Name, up.ID))
				}
			}
		}
	}
	return errors.Join(problems...)
}
