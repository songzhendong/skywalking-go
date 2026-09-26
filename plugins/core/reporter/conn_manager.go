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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
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

// multiBackendServiceConfig mirrors Node multi-backend channel policy:
// pick_first + UNAVAILABLE retries only on unary reportInstanceProperties.
// Client-streaming Collect (trace/meter/log) must not retry — the write buffer
// can be replayed and OAP does not dedupe segments (same rationale as Node).
const multiBackendServiceConfig = `{
  "loadBalancingConfig": [{"pick_first":{}}],
  "methodConfig": [
    {
      "name": [{}],
      "waitForReady": false
    },
    {
      "name": [{"service": "skywalking.v3.ManagementService", "method": "reportInstanceProperties"}],
      "waitForReady": false,
      "retryPolicy": {
        "maxAttempts": 3,
        "initialBackoff": "1s",
        "maxBackoff": "10s",
        "backoffMultiplier": 2,
        "retryableStatusCodes": ["UNAVAILABLE"]
      }
    }
  ]
}`

const multiBackendDialTimeout = 5 * time.Second

func NewConnectionManager(logger operator.LogOperator, checkInterval time.Duration,
	serverAddr string, auth string, creds credentials.TransportCredentials) (*ConnectionManager, error) {
	c := &ConnectionManager{
		logger:        logger,
		checkInterval: checkInterval,
		serverAddr:    serverAddr,
		md:            metadata.New(map[string]string{authKey: auth}),
		creds:         creds,
		connManager:   make(map[string]*ManagedConnection),
		mu:            sync.RWMutex{},
		multiBackend:  isMultiBackendService(serverAddr),
	}
	// Auth-failure throttled logs are multi-backend only (same path as the interceptor).
	if c.multiBackend {
		c.authFailures = &authFailureLogger{logger: logger}
	}
	return c, nil
}

type ConnectionManager struct {
	logger        operator.LogOperator
	checkInterval time.Duration
	serverAddr    string
	md            metadata.MD
	creds         credentials.TransportCredentials
	connManager   map[string]*ManagedConnection
	mu            sync.RWMutex

	// multi-backend only (true when config normalizes to ≥2 addresses)
	multiBackend         bool
	resolvedMu           sync.RWMutex
	resolvedBackendAddrs []string
	authFailures         *authFailureLogger
}

type ManagedConnection struct {
	connection *grpc.ClientConn
	status     ConnectionStatus
	refCount   int
	// recreating is set while RecreateConnection dials so GetConnection waits
	// instead of returning a ClientConn that is about to be closed.
	recreating bool
}

func (cm *ConnectionManager) GetMD() metadata.MD {
	return cm.md
}

// IsMultiBackend reports whether this manager was configured with two or more
// distinct backend addresses after normalize.
func (cm *ConnectionManager) IsMultiBackend() bool {
	return cm.multiBackend
}

func (cm *ConnectionManager) GetConnection(serverAddr string) (*grpc.ClientConn, error) {
	// Same mutex as RecreateConnection / ReleaseConnection / PeekConnection:
	// unlocked map+refCount access raced with multi-backend recreate at runtime
	// and with concurrent GetConnection from reporter/CDS/pprof at boot.
	for {
		cm.mu.Lock()
		managed, exists := cm.connManager[serverAddr]
		if !exists {
			cm.mu.Unlock()
			break
		}
		if managed.recreating {
			cm.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		managed.refCount++
		conn := managed.connection
		cm.mu.Unlock()
		return conn, nil
	}

	conn, err := cm.createConnection()
	if err != nil {
		return nil, err
	}

	cm.mu.Lock()
	if managed, exists := cm.connManager[serverAddr]; exists {
		if managed.recreating {
			cm.mu.Unlock()
			_ = conn.Close()
			return cm.GetConnection(serverAddr)
		}
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

func (cm *ConnectionManager) createConnection() (*grpc.ClientConn, error) {
	// Historical single-address path: dial the configured string unchanged.
	if !strings.Contains(cm.serverAddr, ",") {
		var credsDialOption grpc.DialOption
		if cm.creds != nil {
			// use tls
			credsDialOption = grpc.WithTransportCredentials(cm.creds)
		} else {
			credsDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
		}

		conn, err := grpc.Dial(cm.serverAddr, credsDialOption, grpc.WithConnectParams(grpc.ConnectParams{
			// update the max backoff delay interval
			Backoff: backoff.Config{
				BaseDelay:  1.0 * time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   cm.checkInterval,
			},
		}))
		return conn, err
	}

	backends, parseErr := parseBackendServiceList(cm.serverAddr)
	if parseErr != nil {
		if cm.logger != nil {
			cm.logger.Errorf("parse multi-backend service %q failed: %v", cm.serverAddr, parseErr)
		}
		return nil, fmt.Errorf("parse backend service: %w", parseErr)
	}
	// Comma present but only one address left after normalize: dial that host
	// with the same ConnectParams shape as the historical single-address path.
	if len(backends) == 1 {
		var credsDialOption grpc.DialOption
		if cm.creds != nil {
			credsDialOption = grpc.WithTransportCredentials(cm.creds)
		} else {
			credsDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
		}
		return grpc.Dial(backends[0], credsDialOption, grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  1.0 * time.Second,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   cm.checkInterval,
			},
		}))
	}

	return cm.dialMultiBackend(backends)
}

func (cm *ConnectionManager) dialMultiBackend(backends []string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if cm.creds != nil {
		opts = append(opts, grpc.WithTransportCredentials(cm.creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	target, multiOpts, multiErr := cm.multiBackendDialOptions(backends)
	if multiErr != nil {
		return nil, multiErr
	}
	opts = append(opts, multiOpts...)
	opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{
		MinConnectTimeout: multiBackendDialTimeout,
		Backoff: backoff.Config{
			BaseDelay:  1.0 * time.Second,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   cm.checkInterval,
		},
	}))
	conn, dialErr := grpc.Dial(target, opts...)
	if dialErr != nil {
		if cm.logger != nil {
			cm.logger.Errorf("dial multi-backend target %q failed: %v", target, dialErr)
		}
		return nil, fmt.Errorf("dial backend %q via static multi-backend resolver: %w", target, dialErr)
	}
	return conn, nil
}

// multiBackendDialOptions configures the static pick_first resolver path used
// only when backend_service lists two or more addresses.
func (cm *ConnectionManager) multiBackendDialOptions(backends []string) (string, []grpc.DialOption, error) {
	if cm.creds != nil && firstHostnameAuthority(backends) == "" && cm.logger != nil {
		cm.logger.Warnf("multi-backend TLS list %q has no hostname; "+
			"SNI/ServerName may not match certificates (prefer at least one hostname entry)",
			cm.serverAddr)
	}
	builder, buildErr := newStaticBackendResolverBuilder(cm.logger, cm.serverAddr, cm.storeResolvedBackendAddresses)
	if buildErr != nil {
		if cm.logger != nil {
			cm.logger.Errorf("create static multi-backend resolver for %q failed: %v",
				cm.serverAddr, buildErr)
		}
		return "", nil, fmt.Errorf("create static backend resolver: %w", buildErr)
	}
	opts := []grpc.DialOption{
		grpc.WithResolvers(builder),
		grpc.WithDefaultServiceConfig(multiBackendServiceConfig),
		// Node sets grpc.enable_http_proxy=0 for multi-address channels.
		grpc.WithContextDialer(directTCPContextDialer),
	}
	if cm.authFailures != nil {
		opts = append(opts,
			grpc.WithUnaryInterceptor(cm.authFailures.unaryInterceptor()),
			grpc.WithStreamInterceptor(cm.authFailures.streamInterceptor()),
		)
	}
	if cm.logger != nil {
		cm.logger.Infof("using static multi-backend pick_first resolver (%d addresses): %s",
			len(backends), strings.Join(backends, ","))
	}
	return builder.target(), opts, nil
}

func (cm *ConnectionManager) storeResolvedBackendAddresses(addrs []string) {
	copied := append([]string(nil), addrs...)
	cm.resolvedMu.Lock()
	cm.resolvedBackendAddrs = copied
	cm.resolvedMu.Unlock()
}

// ResolvedBackendAddresses returns the last address list published to gRPC.
// Empty when the static multi-backend resolver is unused. Intended for tests
// and diagnostics (Node-like /debug/resolved-backends).
func (cm *ConnectionManager) ResolvedBackendAddresses() []string {
	cm.resolvedMu.RLock()
	defer cm.resolvedMu.RUnlock()
	return append([]string(nil), cm.resolvedBackendAddrs...)
}

func directTCPContextDialer(ctx context.Context, addr string) (net.Conn, error) {
	// Bound dial so pick_first can leave a blackhole first address quickly.
	// TCP keepalive helps, but half-open peers can still look Ready for a long
	// time — MultiBackendSend bounds stream Send so failover is not stuck.
	d := &net.Dialer{Timeout: multiBackendDialTimeout, KeepAlive: 10 * time.Second}
	return d.DialContext(ctx, "tcp", addr)
}

// multiBackendSendTimeout is how long a Collect Send/CloseAndRecv may block
// before the stream context is canceled so pick_first can reopen on standby.
// Mutable for tests that exercise failover without waiting the production 8s.
var multiBackendSendTimeout = 8 * time.Second

// multiBackendSendCancelGrace is how long to wait for send() to return after
// cancel. Half-open TCP can ignore cancel; the reporter must not stall forever
// on <-done (that was the kill→standby hang).
var multiBackendSendCancelGrace = 2 * time.Second

var errMultiBackendSendTimeout = fmt.Errorf("multi-backend send timed out")

// errMultiBackendSendPanic is returned when the send goroutine panics so callers
// can treat it like sendWithRecover (skip message) instead of crashing the process.
var errMultiBackendSendPanic = fmt.Errorf("multi-backend send panic")

// IsMultiBackendSendPanic reports whether err was produced by recovering a panic
// inside MultiBackendSend's helper goroutine.
func IsMultiBackendSendPanic(err error) bool {
	return err != nil && errors.Is(err, errMultiBackendSendPanic)
}

// MultiBackendSendTimeoutForTest / SetMultiBackendSendTimeoutForTest let unit
// tests exercise kill→standby without waiting the production 8s bound.
func MultiBackendSendTimeoutForTest() time.Duration { return multiBackendSendTimeout }
func SetMultiBackendSendTimeoutForTest(d time.Duration) {
	multiBackendSendTimeout = d
}
func MultiBackendSendCancelGraceForTest() time.Duration { return multiBackendSendCancelGrace }
func SetMultiBackendSendCancelGraceForTest(d time.Duration) {
	multiBackendSendCancelGrace = d
}

// MultiBackendSend bounds a Collect Send/CloseAndRecv with a stream-context
// deadline: after timeout it invokes cancel (same effect as ctx deadline).
// Send still runs in one helper goroutine so a half-open peer that ignores
// cancel cannot block the reporter past timeout+grace.
//
// done is buffered so that if we return after grace while send is still
// blocked, the send goroutine can later exit without a second drain goroutine
// (Codex P2). RecreateConnection closes the old ClientConn to unblock it.
//
// Panics from send are recovered in the helper goroutine: caller's
// sendWithRecover cannot see them across goroutine boundaries.
func MultiBackendSend(cancel context.CancelFunc, send func() error, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = multiBackendSendTimeout
	}
	done := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("%w: %v", errMultiBackendSendPanic, rec)
			}
			done <- err
		}()
		err = send()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		if cancel != nil {
			cancel()
		}
		grace := time.NewTimer(multiBackendSendCancelGrace)
		defer grace.Stop()
		select {
		case err := <-done:
			if err != nil {
				return err
			}
			return errMultiBackendSendTimeout
		case <-grace.C:
			return errMultiBackendSendTimeout
		}
	}
}

// BackendRPCContext returns the context for an outbound unary backend RPC.
// Multi-address only; callers should keep historical context.Background() for
// single-address paths.
func BackendRPCContext(serverAddr string, atLeast time.Duration) (context.Context, context.CancelFunc) {
	_ = serverAddr // multi-only; callers gate on comma-separated backend_service
	timeout := 30 * time.Second
	if atLeast > timeout {
		timeout = atLeast
	}
	return context.WithTimeout(context.Background(), timeout)
}

// BackendStreamContext is for long-lived Collect streams on multi-address
// channels. Call stopOpenTimer immediately after Collect returns; if it
// reports timedOut, discard the stream. After a successful open, callers
// should start WatchConnCancelOnUnready so a hung Send unblocks when the
// channel leaves Ready.
func BackendStreamContext(serverAddr string, atLeast time.Duration) (
	ctx context.Context, cancel context.CancelFunc, stopOpenTimer func() (timedOut bool),
) {
	_ = serverAddr // multi-only; callers gate on comma-separated backend_service
	ctx, cancel = context.WithCancel(context.Background())
	timeout := 30 * time.Second
	if atLeast > timeout {
		timeout = atLeast
	}
	var mu sync.Mutex
	opened := false
	timer := time.AfterFunc(timeout, func() {
		mu.Lock()
		defer mu.Unlock()
		if !opened {
			cancel()
		}
	})
	stopOpenTimer = func() bool {
		mu.Lock()
		opened = true
		timedOut := ctx.Err() != nil
		mu.Unlock()
		timer.Stop()
		return timedOut
	}
	return ctx, cancel, stopOpenTimer
}

// WatchConnCancelOnUnready cancels ctx once conn has been Ready and later
// enters TransientFailure or Shutdown. Start only after Collect succeeds so
// pick_first can finish Connecting → Ready on the standby without being
// canceled early.
func WatchConnCancelOnUnready(ctx context.Context, cancel context.CancelFunc, conn *grpc.ClientConn) {
	if conn == nil {
		return
	}
	seenReady := false
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			seenReady = true
		}
		if seenReady && (state == connectivity.TransientFailure || state == connectivity.Shutdown) {
			cancel()
			return
		}
		if !conn.WaitForStateChange(ctx, state) {
			return
		}
	}
}

// PeekConnection returns the managed ClientConn for serverAddr without
// changing refCount. Nil when missing.
func (cm *ConnectionManager) PeekConnection(serverAddr string) *grpc.ClientConn {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	managed, exists := cm.connManager[serverAddr]
	if !exists {
		return nil
	}
	return managed.connection
}

func (cm *ConnectionManager) checkConnectionStatus(serverAddr string) {
	if cm.multiBackend {
		cm.checkMultiBackendConnectionStatus(serverAddr)
		return
	}
	for {
		cm.mu.Lock()
		managed, exists := cm.connManager[serverAddr]
		cm.mu.Unlock()
		if !exists {
			return
		}
		state := managed.connection.GetState()
		var newStatus ConnectionStatus
		switch state {
		case connectivity.TransientFailure:
			newStatus = ConnectionStatusDisconnect
		case connectivity.Shutdown:
			newStatus = ConnectionStatusShutdown
		default:
			newStatus = ConnectionStatusConnected
		}
		if newStatus != managed.status {
			cm.mu.Lock()
			managed.status = newStatus
			cm.mu.Unlock()
		}
		time.Sleep(5 * time.Second)
	}
}

// checkMultiBackendConnectionStatus keeps reporter RPCs eligible while pick_first
// moves off a dead backend, and kicks Idle/TF with Connect().
func (cm *ConnectionManager) checkMultiBackendConnectionStatus(serverAddr string) {
	for {
		cm.mu.Lock()
		managed, exists := cm.connManager[serverAddr]
		if !exists {
			cm.mu.Unlock()
			return
		}
		conn := managed.connection
		oldStatus := managed.status
		checkInterval := cm.checkInterval
		cm.mu.Unlock()

		state := conn.GetState()
		// Never Connect() a Shutdown ClientConn (closed after Recreate swap or
		// Close). Recovery is RecreateConnection on the send path, or map delete
		// on Release. Still report Connected while the map entry exists so
		// CDS/pprof/send loops do not exit permanently mid-failover.
		if state == connectivity.Idle || state == connectivity.TransientFailure {
			conn.Connect()
		}
		newStatus := ConnectionStatusConnected
		if newStatus != oldStatus {
			cm.mu.Lock()
			if managed, exists := cm.connManager[serverAddr]; exists && managed.connection == conn {
				managed.status = newStatus
			}
			cm.mu.Unlock()
		}
		interval := 5 * time.Second
		if checkInterval > 0 {
			interval = checkInterval
		}
		time.Sleep(interval)
	}
}

func (cm *ConnectionManager) ReleaseConnection(serverAddr string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	managed, exists := cm.connManager[serverAddr]
	if !exists {
		return nil
	}
	managed.refCount--
	if managed.refCount <= 0 {
		if err := managed.connection.Close(); err != nil {
			cm.logger.Error(err)
		}
		delete(cm.connManager, serverAddr)
	}
	return nil
}

// RecreateConnection closes the managed ClientConn and dials again. Multi-backend
// only: used when Collect Send times out on a half-open peer so pick_first can
// try the standby on a fresh channel. Swaps the connection in place so
// GetConnectionStatus never returns Shutdown mid-recreate (that would make
// send loops exit forever and drop post-failover probe spans).
//
// Dial runs outside the map mutex, but the entry is marked recreating so
// GetConnection waits instead of returning a ClientConn that is about to be
// closed. If Release/Close removes the entry during dial, the new connection
// is discarded (no resurrect).
func (cm *ConnectionManager) RecreateConnection(serverAddr string) error {
	if !cm.multiBackend {
		return fmt.Errorf("RecreateConnection is multi-backend only")
	}

	cm.mu.Lock()
	old, exists := cm.connManager[serverAddr]
	if !exists {
		cm.mu.Unlock()
		return fmt.Errorf("no managed connection to recreate")
	}
	if old.recreating {
		cm.mu.Unlock()
		return nil
	}
	old.recreating = true
	cm.mu.Unlock()

	conn, err := cm.createConnection()
	if err != nil {
		cm.mu.Lock()
		if managed, ok := cm.connManager[serverAddr]; ok {
			managed.recreating = false
		}
		cm.mu.Unlock()
		return err
	}

	cm.mu.Lock()
	managed, exists := cm.connManager[serverAddr]
	if !exists {
		cm.mu.Unlock()
		_ = conn.Close()
		return fmt.Errorf("connection released during recreate")
	}
	prev := managed.connection
	managed.connection = conn
	managed.status = ConnectionStatusConnected
	managed.recreating = false
	cm.mu.Unlock()

	if prev != nil && prev != conn {
		_ = prev.Close()
	}
	if cm.logger != nil {
		cm.logger.Infof("recreated multi-backend gRPC connection after send/open failure")
	}
	return nil
}

// Close force-closes every managed ClientConn. Test helper for multi-backend
// cases; production reporter shutdown uses ReleaseConnection.
func (cm *ConnectionManager) Close() {
	cm.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(cm.connManager))
	for addr, managed := range cm.connManager {
		conns = append(conns, managed.connection)
		delete(cm.connManager, addr)
	}
	cm.mu.Unlock()
	for _, conn := range conns {
		if err := conn.Close(); err != nil && cm.logger != nil {
			cm.logger.Error(err)
		}
	}
}

func (cm *ConnectionManager) GetConnectionStatus(serverAddr string) ConnectionStatus {
	cm.mu.Lock()
	defer cm.mu.Unlock()
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
