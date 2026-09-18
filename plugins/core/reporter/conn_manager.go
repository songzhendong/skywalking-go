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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package reporter

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/connectivity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/apache/skywalking-go/plugins/core/operator"
)

var authKey = "Authentication"

func NewConnectionManager(logger operator.LogOperator, checkInterval time.Duration,
	serverAddr string, auth string, creds credentials.TransportCredentials, opts ...ConnectionManagerOption) (*ConnectionManager, error) {
	c := &ConnectionManager{
		logger:        logger,
		checkInterval: checkInterval,
		serverAddr:    serverAddr,
		md:            metadata.New(map[string]string{authKey: auth}),
		creds:         creds,
		connManager:   make(map[string]*ManagedConnection),
		mu:            sync.RWMutex{},
		dnsLookup:     netLookupHost,
		shutdownCh:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.dnsResolveInterval < 0 {
		return nil, fmt.Errorf("periodic DNS resolve interval must not be negative")
	}
	return c, nil
}

type ConnectionManager struct {
	logger                 operator.LogOperator
	checkInterval          time.Duration
	serverAddr             string
	md                     metadata.MD
	creds                  credentials.TransportCredentials
	resolveDNSPeriodically bool
	dnsResolveInterval     time.Duration // 0 means use checkInterval
	dnsLookup              dnsLookupFunc
	connManager            map[string]*ManagedConnection
	mu                     sync.RWMutex
	closed                 bool
	closeOnce              sync.Once
	// shutdownCh is closed by SignalShutdown/Close to wake interruptible waits
	// before ClientConns are torn down, so send pipelines can still drain.
	shutdownCh chan struct{}
}

type ManagedConnection struct {
	connection *grpc.ClientConn
	status     ConnectionStatus
	refCount   int
}

type ConnectionManagerOption func(*ConnectionManager)

func WithPeriodicDNSResolver(enabled bool) ConnectionManagerOption {
	return func(cm *ConnectionManager) {
		cm.resolveDNSPeriodically = enabled
	}
}

// WithPeriodicDNSResolveInterval sets the DNS refresh period. If d <= 0,
// createConnection uses checkInterval instead.
func WithPeriodicDNSResolveInterval(d time.Duration) ConnectionManagerOption {
	return func(cm *ConnectionManager) {
		cm.dnsResolveInterval = d
	}
}

// withDNSLookup replaces the DNS lookup used by the periodic resolver, so unit
// tests do not depend on the host resolver.
func withDNSLookup(lookup dnsLookupFunc) ConnectionManagerOption {
	return func(cm *ConnectionManager) {
		if lookup != nil {
			cm.dnsLookup = lookup
		}
	}
}

func (cm *ConnectionManager) periodicDNSInterval() time.Duration {
	if cm.dnsResolveInterval > 0 {
		return cm.dnsResolveInterval
	}
	return cm.checkInterval
}

func (cm *ConnectionManager) GetMD() metadata.MD {
	return cm.md
}

func (cm *ConnectionManager) GetConnection(serverAddr string) (*grpc.ClientConn, error) {
	cm.mu.Lock()
	if cm.closed {
		cm.mu.Unlock()
		return nil, fmt.Errorf("connection manager is closed")
	}
	if managed, exists := cm.connManager[serverAddr]; exists {
		managed.refCount++
		conn := managed.connection
		cm.mu.Unlock()
		return conn, nil
	}
	cm.mu.Unlock()

	// Dial without holding mu: Dial may block on the resolver/network.
	conn, err := cm.createConnection()
	if err != nil {
		return nil, err
	}

	cm.mu.Lock()
	if cm.closed {
		cm.mu.Unlock()
		_ = conn.Close()
		return nil, fmt.Errorf("connection manager is closed")
	}
	if managed, exists := cm.connManager[serverAddr]; exists {
		managed.refCount++
		existing := managed.connection
		cm.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	cm.connManager[serverAddr] = &ManagedConnection{
		connection: conn,
		status:     ConnectionStatusConnected,
		refCount:   1,
	}
	cm.mu.Unlock()
	go cm.checkConnectionStatus(serverAddr)
	return conn, nil
}

// ShutdownNotify is closed when SignalShutdown or Close runs. Pipelines should
// select on it instead of fixed sleeps so Close can drain without waiting out
// disconnect/retry backoffs.
func (cm *ConnectionManager) ShutdownNotify() <-chan struct{} {
	return cm.shutdownCh
}

// Wait sleeps for d, or returns early when shutdown is signaled.
// Returns false if shutdown was signaled.
func (cm *ConnectionManager) Wait(d time.Duration) bool {
	if d <= 0 {
		select {
		case <-cm.shutdownCh:
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-cm.shutdownCh:
		return false
	case <-timer.C:
		return true
	}
}

// SignalShutdown marks the manager closed and wakes Wait callers without
// tearing down ClientConns yet, so buffered telemetry can still flush.
func (cm *ConnectionManager) SignalShutdown() {
	cm.closeOnce.Do(func() {
		cm.mu.Lock()
		cm.closed = true
		cm.mu.Unlock()
		close(cm.shutdownCh)
	})
}

func (cm *ConnectionManager) createConnection() (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if cm.creds != nil {
		// use tls
		opts = append(opts, grpc.WithTransportCredentials(cm.creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	target := cm.serverAddr
	if cm.resolveDNSPeriodically {
		resolverBuilder, err := newPeriodicDNSResolverBuilder(cm.logger, cm.serverAddr,
			cm.periodicDNSInterval(), cm.dnsLookup)
		if err != nil {
			return nil, fmt.Errorf("create periodic DNS resolver: %w", err)
		}
		target = resolverBuilder.target()
		// The resolver lifetime is owned by ClientConn, so closing the connection
		// through ReleaseConnection or Close also stops the DNS refresh.
		opts = append(opts, grpc.WithResolvers(resolverBuilder))
	}

	opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{
		// update the max backoff delay interval
		Backoff: backoff.Config{
			BaseDelay:  1.0 * time.Second,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   cm.checkInterval,
		},
	}))

	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		if cm.resolveDNSPeriodically {
			return nil, fmt.Errorf("dial backend %q via periodic DNS resolver: %w", target, err)
		}
		return nil, err
	}
	return conn, nil
}

func (cm *ConnectionManager) checkConnectionStatus(serverAddr string) {
	for {
		cm.mu.Lock()
		if cm.closed {
			cm.mu.Unlock()
			return
		}
		managed, exists := cm.connManager[serverAddr]
		if !exists {
			cm.mu.Unlock()
			return
		}
		conn := managed.connection
		oldStatus := managed.status
		cm.mu.Unlock()

		state := conn.GetState()
		var newStatus ConnectionStatus
		switch state {
		case connectivity.TransientFailure:
			newStatus = ConnectionStatusDisconnect
		case connectivity.Shutdown:
			newStatus = ConnectionStatusShutdown
		default:
			newStatus = ConnectionStatusConnected
		}
		if newStatus != oldStatus {
			cm.mu.Lock()
			if managed, exists := cm.connManager[serverAddr]; exists {
				managed.status = newStatus
			}
			cm.mu.Unlock()
		}
		if !cm.Wait(5 * time.Second) {
			return
		}
	}
}

func (cm *ConnectionManager) ReleaseConnection(serverAddr string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.closed {
		return nil
	}
	managed, exists := cm.connManager[serverAddr]
	if !exists {
		return nil
	}
	managed.refCount--
	if managed.refCount <= 0 {
		if err := managed.connection.Close(); err != nil {
			if cm.logger != nil {
				cm.logger.Error(err)
			}
		}
		delete(cm.connManager, serverAddr)
	}
	return nil
}

// Close signals shutdown (waking Wait callers) then force-closes every managed
// ClientConn regardless of refCount, which stops periodic DNS resolvers.
// Removing the entries also makes GetConnectionStatus report Shutdown.
// Safe to call multiple times; GetConnection fails after the first call.
func (cm *ConnectionManager) Close() {
	cm.SignalShutdown()
	cm.closeConnections()
}

func (cm *ConnectionManager) closeConnections() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for addr, managed := range cm.connManager {
		if err := managed.connection.Close(); err != nil && cm.logger != nil {
			cm.logger.Error(err)
		}
		delete(cm.connManager, addr)
	}
}

func (cm *ConnectionManager) GetConnectionStatus(serverAddr string) ConnectionStatus {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// Do not treat SignalShutdown alone as Shutdown: send pipelines still need
	// the real connection status while draining closed channels.
	managed, exists := cm.connManager[serverAddr]
	if !exists {
		return ConnectionStatusShutdown
	}
	return managed.status
}

// nolint
func generateTLSCredential(caPath, clientKeyPath, clientCertChainPath string, skipVerify bool) (tc credentials.TransportCredentials, tlsErr error) {
	if err := checkTLSFile(caPath); err != nil {
		return nil, err
	}
	tlsConfig := new(tls.Config)
	tlsConfig.Renegotiation = tls.RenegotiateNever
	tlsConfig.InsecureSkipVerify = skipVerify
	caPem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("failed to append certificates")
	}
	tlsConfig.RootCAs = certPool

	if clientKeyPath != "" && clientCertChainPath != "" {
		if err := checkTLSFile(clientKeyPath); err != nil {
			return nil, err
		}
		if err := checkTLSFile(clientCertChainPath); err != nil {
			return nil, err
		}
		clientPem, err := tls.LoadX509KeyPair(clientCertChainPath, clientKeyPath)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{clientPem}
	}
	return credentials.NewTLS(tlsConfig), nil
}

// checkTLSFile checks the TLS files.
func checkTLSFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return fmt.Errorf("the TLS file is illegal: %s", path)
	}
	return nil
}
