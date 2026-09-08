// Package quicknode builds network endpoints from one multichain-enabled RPC URL.
package quicknode

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

// Service retains the shared endpoint name and credentials used by each network.
type Service struct {
	endpoint url.URL
	name     string
	network  *regexp.Regexp
}

// Endpoints contains the HTTP and WebSocket URLs for one QuickNode network.
type Endpoints struct {
	HTTPURL      string
	WebsocketURL string
}

// NewService accepts a standard HTTPS QuickNode RPC URL from any source network
// so startup can derive other networks without requiring an Admin API key.
func NewService(endpointURL string) (*Service, error) {
	endpoint, err := url.Parse(endpointURL)
	if err != nil {
		// URL parsing errors include the input, which contains the provider token.
		return nil, errors.New("invalid QuickNode endpoint URL")
	}
	network := regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	hostname := strings.Split(endpoint.Host, ".")
	if endpoint.Scheme != "https" || endpoint.User != nil || endpoint.Fragment != "" ||
		(len(hostname) != 3 && len(hostname) != 4) ||
		hostname[len(hostname)-2] != "quiknode" || hostname[len(hostname)-1] != "pro" ||
		!network.MatchString(hostname[0]) || len(hostname) == 4 && !network.MatchString(hostname[1]) {
		return nil, errors.New("QuickNode http_url must use https://ENDPOINT[.NETWORK].quiknode.pro/TOKEN")
	}
	path := endpoint.EscapedPath()
	if len(hostname) == 4 && (hostname[1] == "avalanche-mainnet" || hostname[1] == "avalanche-testnet") {
		// Avalanche's C-Chain transport suffix belongs to the network, not the token.
		if strings.HasSuffix(path, "/ext/bc/C/rpc/") {
			path = strings.TrimSuffix(path, "/ext/bc/C/rpc/")
		} else {
			path = strings.TrimSuffix(path, "/ext/bc/C/rpc")
		}
	}
	if !regexp.MustCompile(`^/[A-Za-z0-9_-]+/?$`).MatchString(path) {
		return nil, errors.New("QuickNode http_url must contain one token path, optionally followed by the Avalanche C-Chain RPC path")
	}
	endpoint.Path = path
	endpoint.RawPath = ""
	return &Service{endpoint: *endpoint, name: hostname[0], network: network}, nil
}

// GetEndpoints derives HTTP and WebSocket URLs for a QuickNode network slug
// (for example mainnet, base-mainnet, or solana-mainnet), retaining shared credentials.
func (s *Service) GetEndpoints(network string) (Endpoints, error) {
	if !s.network.MatchString(network) {
		return Endpoints{}, errors.New("QuickNode network must be a lowercase DNS label of at most 63 characters")
	}
	endpoint := s.endpoint
	endpoint.Host = s.name + ".quiknode.pro"
	if network != "mainnet" {
		endpoint.Host = s.name + "." + network + ".quiknode.pro"
	}
	websocketEndpoint := endpoint
	websocketEndpoint.Scheme = "wss"
	if network == "avalanche-mainnet" || network == "avalanche-testnet" {
		tokenPath := strings.TrimSuffix(endpoint.Path, "/")
		endpoint.Path = tokenPath + "/ext/bc/C/rpc"
		websocketEndpoint.Path = tokenPath + "/ext/bc/C/ws"
	}
	return Endpoints{HTTPURL: endpoint.String(), WebsocketURL: websocketEndpoint.String()}, nil
}
