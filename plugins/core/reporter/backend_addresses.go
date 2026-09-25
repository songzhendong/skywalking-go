// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reporter

import (
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc/resolver"
)

// parseBackendServiceList splits a comma-separated backend_service config into
// normalized host:port entries. Empty segments are skipped; duplicates removed.
func parseBackendServiceList(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("backend service address is empty")
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		host, port, err := splitBackendServiceAddress(part)
		if err != nil {
			return nil, err
		}
		normalized := net.JoinHostPort(host, port)
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("backend service address is empty")
	}
	return out, nil
}

// isMultiBackendService is true only when backend_service normalizes to two or
// more distinct host:port entries. Trailing commas / duplicates that collapse
// to one address are not multi-backend.
func isMultiBackendService(serverAddr string) bool {
	backends, err := parseBackendServiceList(serverAddr)
	return err == nil && len(backends) >= 2
}

func splitBackendServiceAddress(serverAddr string) (host, port string, err error) {
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		return "", "", fmt.Errorf("backend service address is empty")
	}
	host, port, err = net.SplitHostPort(serverAddr)
	if err != nil {
		return "", "", fmt.Errorf("invalid backend service address %q: %w", serverAddr, err)
	}
	if host == "" || port == "" {
		return "", "", fmt.Errorf("invalid backend service address %q", serverAddr)
	}
	return host, port, nil
}

func isIPLiteralHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	return net.ParseIP(host) != nil
}

// firstHostnameAuthority returns the first non-IP hostname in the configured
// backend list (Node-like). Used as TLS ServerName/SNI when dialing IP literals
// so mixed "ip,hostname" lists still present a DNS name to the certificate.
func firstHostnameAuthority(backends []string) string {
	for _, cfg := range backends {
		host, _, err := splitBackendServiceAddress(cfg)
		if err != nil {
			continue
		}
		if !isIPLiteralHost(host) {
			return host
		}
	}
	return ""
}

func serverNameForDialHost(host, firstHostname string) string {
	if isIPLiteralHost(host) && firstHostname != "" {
		return firstHostname
	}
	return host
}

func newResolverAddress(addressHost, serverName, port string) resolver.Address {
	return resolver.Address{
		Addr:       net.JoinHostPort(addressHost, port),
		ServerName: serverName,
	}
}

func configuredAddressesAsResolverState(backends []string) []resolver.Address {
	firstHostname := firstHostnameAuthority(backends)
	addresses := make([]resolver.Address, 0, len(backends))
	for _, cfg := range backends {
		host, port, err := splitBackendServiceAddress(cfg)
		if err != nil {
			addresses = append(addresses, resolver.Address{Addr: cfg, ServerName: cfg})
			continue
		}
		addresses = append(addresses, newResolverAddress(host, serverNameForDialHost(host, firstHostname), port))
	}
	return addresses
}
