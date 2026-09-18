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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const (
	testBackendHost = "oap.example.com"
	testBackendAddr = testBackendHost + ":11800"
)

type testResolverClientConn struct {
	mu     sync.Mutex
	states []resolver.State
	errors []error
}

func (c *testResolverClientConn) UpdateState(state resolver.State) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states = append(c.states, state)
	return nil
}

func (c *testResolverClientConn) ReportError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, err)
}

func (c *testResolverClientConn) NewAddress(addresses []resolver.Address) {}

func (c *testResolverClientConn) NewServiceConfig(serviceConfig string) {}

func (c *testResolverClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}

func (c *testResolverClientConn) stateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.states)
}

func (c *testResolverClientConn) errorCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errors)
}

func (c *testResolverClientConn) lastState() resolver.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.states[len(c.states)-1]
}

func TestPeriodicDNSResolverResolveNowUpdatesAddresses(t *testing.T) {
	lookupResults := [][]string{
		{"10.0.0.1"},
		{"10.0.0.2", "10.0.0.2", ""},
	}
	var lookupCalls atomic.Int32
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		if host != testBackendHost {
			t.Fatalf("lookup host = %s, want %s", host, testBackendHost)
		}
		idx := int(lookupCalls.Add(1) - 1)
		return lookupResults[idx], nil
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}
	defer r.Close()

	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.1:11800"})
	}, time.Second)
	assertStateServerNames(t, cc.lastState(), []string{testBackendHost})

	r.ResolveNow(resolver.ResolveNowOptions{})
	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.2:11800"})
	}, time.Second)

	assertStateServerNames(t, cc.lastState(), []string{testBackendHost})
	if lookupCalls.Load() != 2 {
		t.Fatalf("lookup calls = %d, want 2", lookupCalls.Load())
	}
}

func TestPeriodicDNSResolverKeepsLastGoodAddressesOnLookupError(t *testing.T) {
	var lookupCalls atomic.Int32
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		if lookupCalls.Add(1) == 1 {
			return []string{"10.0.0.1", "10.0.0.2"}, nil
		}
		return nil, errors.New("dns unavailable")
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}
	defer r.Close()

	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.1:11800", "10.0.0.2:11800"})
	}, time.Second)
	firstCount := cc.stateCount()

	r.ResolveNow(resolver.ResolveNowOptions{})
	waitFor(t, func() bool { return lookupCalls.Load() >= 2 }, time.Second)

	// Lookup failed but last-good addresses are kept; UpdateState is skipped as unchanged.
	if cc.stateCount() != firstCount {
		t.Fatalf("state updates = %d, want %d (unchanged last-good)", cc.stateCount(), firstCount)
	}
	assertStateAddresses(t, cc.lastState(), []string{"10.0.0.1:11800", "10.0.0.2:11800"})
}

func TestPeriodicDNSResolverFallsBackToBackendServiceWhenNoLastGood(t *testing.T) {
	r := &periodicDNSResolver{
		serverAddr: testBackendAddr,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			return nil, errors.New("dns unavailable")
		},
		lookupCtx: context.Background(),
	}

	addresses := r.resolveBackendServiceAddressesLocked()
	assertAddresses(t, addresses, []string{testBackendAddr})
}

func TestPeriodicDNSResolverSkipsUnchangedUpdateState(t *testing.T) {
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		return []string{"10.0.0.1"}, nil
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}
	defer r.Close()

	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.1:11800"})
	}, time.Second)
	afterDNS := cc.stateCount()

	res := r.(*periodicDNSResolver)
	if d := res.resolveAndUpdate(); d != 0 {
		t.Fatalf("backoff = %v, want 0", d)
	}
	if cc.stateCount() != afterDNS {
		t.Fatalf("state updates after unchanged resolve = %d, want %d", cc.stateCount(), afterDNS)
	}
	if cc.errorCount() != 0 {
		t.Fatalf("ReportError calls = %d, want 0", cc.errorCount())
	}
}

func TestPeriodicDNSResolverBuildFailsWhenInitialUpdateStateRejected(t *testing.T) {
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		return []string{"10.0.0.1"}, nil
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &failingUpdateClientConn{err: errors.New("bad resolver state")}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err == nil {
		t.Fatal("expected build error when initial UpdateState is rejected")
	}
	if r != nil {
		t.Fatal("expected nil resolver when build fails")
	}
	if cc.reportCount.Load() != 0 {
		t.Fatalf("ReportError calls = %d, want 0", cc.reportCount.Load())
	}
}

func TestPeriodicDNSResolverDoesNotReportErrorOnUpdateStateFailure(t *testing.T) {
	cc := &failingUpdateClientConn{err: errors.New("bad resolver state")}
	r := &periodicDNSResolver{
		serverAddr: testBackendAddr,
		interval:   time.Hour,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			return []string{"10.0.0.1"}, nil
		},
		lookupCtx: context.Background(),
		cc:        cc,
		done:      make(chan struct{}),
	}

	if d := r.resolveAndUpdate(); d <= 0 {
		t.Fatalf("backoff = %v, want > 0", d)
	}
	if cc.reportCount.Load() != 0 {
		t.Fatalf("ReportError calls = %d, want 0", cc.reportCount.Load())
	}
}

func TestPeriodicDNSResolverUpdateStateFailureReturnsBackoff(t *testing.T) {
	cc := &failingUpdateClientConn{err: errors.New("bad resolver state")}
	r := &periodicDNSResolver{
		serverAddr: testBackendAddr,
		interval:   20 * time.Second,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			return []string{"10.0.0.1"}, nil
		},
		lookupCtx: context.Background(),
		cc:        cc,
		done:      make(chan struct{}),
	}

	d1 := r.resolveAndUpdate()
	if d1 != updateStateMinBackoff {
		t.Fatalf("first backoff = %v, want %v", d1, updateStateMinBackoff)
	}
	d2 := r.resolveAndUpdate()
	if d2 != 2*updateStateMinBackoff {
		t.Fatalf("second backoff = %v, want %v", d2, 2*updateStateMinBackoff)
	}
	d3 := r.resolveAndUpdate()
	if d3 != 4*updateStateMinBackoff {
		t.Fatalf("third backoff = %v, want %v", d3, 4*updateStateMinBackoff)
	}
	if cc.reportCount.Load() != 0 {
		t.Fatalf("ReportError calls = %d, want 0", cc.reportCount.Load())
	}
}

func TestNextUpdateStateBackoffCapsAtInterval(t *testing.T) {
	got := nextUpdateStateBackoff(16*time.Second, 10*time.Second)
	if got != 10*time.Second {
		t.Fatalf("backoff = %v, want 10s", got)
	}
}

func TestPeriodicDNSResolverIgnoresDNSOrderChanges(t *testing.T) {
	var lookupCalls atomic.Int32
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		if lookupCalls.Add(1) == 1 {
			return []string{"10.0.0.2", "10.0.0.1"}, nil
		}
		return []string{"10.0.0.1", "10.0.0.2"}, nil
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}
	defer r.Close()

	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.2:11800", "10.0.0.1:11800"})
	}, time.Second)
	firstCount := cc.stateCount()

	res := r.(*periodicDNSResolver)
	res.resolveAndUpdate()
	if cc.stateCount() != firstCount {
		t.Fatalf("state updates = %d, want %d after DNS order-only change", cc.stateCount(), firstCount)
	}
	if lookupCalls.Load() != 2 {
		t.Fatalf("lookup calls = %d, want 2", lookupCalls.Load())
	}
	// Published order must stay the original DNS order (not sorted).
	assertStateAddresses(t, cc.lastState(), []string{"10.0.0.2:11800", "10.0.0.1:11800"})
}

func TestPeriodicDNSResolverCoalescesResolveNowHints(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var lookupCalls atomic.Int32
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		n := lookupCalls.Add(1)
		if n == 1 {
			return []string{"10.0.0.1"}, nil
		}
		if n == 2 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []string{"10.0.0.2"}, nil
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}
	defer r.Close()

	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.1:11800"})
	}, time.Second)

	r.ResolveNow(resolver.ResolveNowOptions{})
	waitFor(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, time.Second)

	// Flood hints while a resolve is in progress; they must coalesce on the trigger channel.
	for i := 0; i < 32; i++ {
		r.ResolveNow(resolver.ResolveNowOptions{})
	}
	close(release)

	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.2:11800"})
	}, time.Second)
	// First watch resolve + one blocked resolve + at most one coalesced follow-up.
	if got := lookupCalls.Load(); got > 3 {
		t.Fatalf("lookup calls = %d, want <= 3 (coalesced ResolveNow)", got)
	}
}

func TestPeriodicDNSResolverCloseCancelsLookupCtx(t *testing.T) {
	var canceled atomic.Bool
	started := make(chan struct{})
	var startOnce sync.Once
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		canceled.Store(true)
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}

	waitFor(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, time.Second)

	r.Close()
	waitFor(t, canceled.Load, time.Second)
}

func TestPeriodicDNSResolverNoUpdateStateAfterCloseReturns(t *testing.T) {
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		return []string{"10.0.0.1"}, nil
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}
	res := r.(*periodicDNSResolver)
	r.Close()

	before := cc.stateCount()
	res.lookup = func(ctx context.Context, host string) ([]string, error) {
		return []string{"10.0.0.9"}, nil
	}
	if d := res.resolveAndUpdate(); d != 0 {
		t.Fatalf("backoff after close = %v, want 0", d)
	}
	if cc.stateCount() != before {
		t.Fatalf("UpdateState after Close: count=%d, want %d", cc.stateCount(), before)
	}
}

func TestPeriodicDNSResolverCloseJoinsWatchGoroutine(t *testing.T) {
	entered := make(chan struct{})
	var enterOnce sync.Once
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour, func(ctx context.Context, host string) ([]string, error) {
		enterOnce.Do(func() { close(entered) })
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}

	waitFor(t, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	}, time.Second)

	res := r.(*periodicDNSResolver)
	done := make(chan struct{})
	go func() {
		r.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close joined watch: watchDone is closed and receive returned.
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after joining watch goroutine")
	}
	select {
	case <-res.watchDone:
	default:
		t.Fatal("watchDone should be closed after Close joins watch")
	}
}

func TestPeriodicDNSResolverIntervalRefreshesChangedAddresses(t *testing.T) {
	var lookupCalls atomic.Int32
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, 40*time.Millisecond,
		func(ctx context.Context, host string) ([]string, error) {
			if lookupCalls.Add(1) == 1 {
				return []string{"10.0.0.1"}, nil
			}
			return []string{"10.0.0.2", "10.0.0.3"}, nil
		})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build resolver error: %v", err)
	}
	defer r.Close()

	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.1:11800"})
	}, time.Second)
	// Mid-lifecycle refresh must pick up changed DNS answers without ResolveNow.
	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.2:11800", "10.0.0.3:11800"})
	}, time.Second)
	assertStateServerNames(t, cc.lastState(), []string{testBackendHost, testBackendHost})
}

func TestPeriodicDNSResolverDialRecoversAfterDNSAddressChange(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listen addr: %v", err)
	}

	gs := grpc.NewServer()
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	var lookupCalls atomic.Int32
	backend := net.JoinHostPort("backend.test", port)
	builder, err := newPeriodicDNSResolverBuilder(nil, backend, 50*time.Millisecond, func(ctx context.Context, host string) ([]string, error) {
		if host != "backend.test" {
			t.Fatalf("lookup host = %s, want backend.test", host)
		}
		// First answer is a blackhole TEST-NET address; later answers point at the real listener.
		if lookupCalls.Add(1) == 1 {
			return []string{"203.0.113.1"}, nil
		}
		return []string{"127.0.0.1"}, nil
	})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	conn, err := grpc.Dial(builder.target(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
	)
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}
	defer conn.Close()

	waitFor(t, func() bool {
		return conn.GetState() == connectivity.Ready
	}, 5*time.Second)
}

func TestPeriodicDNSResolverStopsLookupsAfterClientConnClose(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listen addr: %v", err)
	}
	gs := grpc.NewServer()
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	var lookups atomic.Int32
	backend := net.JoinHostPort("backend.test", port)
	builder, err := newPeriodicDNSResolverBuilder(nil, backend, 40*time.Millisecond,
		func(ctx context.Context, host string) ([]string, error) {
			lookups.Add(1)
			return []string{"127.0.0.1"}, nil
		})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	conn, err := grpc.Dial(builder.target(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
	)
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}

	waitFor(t, func() bool { return lookups.Load() >= 2 }, 5*time.Second)
	if err := conn.Close(); err != nil {
		t.Fatalf("conn.Close: %v", err)
	}
	// ClientConn.Close must stop the periodic resolver; allow at most one in-flight lookup.
	before := lookups.Load()
	time.Sleep(250 * time.Millisecond)
	after := lookups.Load()
	if after-before > 1 {
		t.Fatalf("DNS lookups continued after ClientConn.Close: before=%d after=%d", before, after)
	}
}

func TestPeriodicDNSResolverRejectsInvalidBackendService(t *testing.T) {
	_, err := newPeriodicDNSResolverBuilder(nil, testBackendHost, time.Second, nil)
	if err == nil {
		t.Fatal("expected invalid backend service address error")
	}
}

func TestPeriodicDNSResolverSupportsIPv6BackendService(t *testing.T) {
	r := &periodicDNSResolver{
		serverAddr: "[2001:db8::1]:11800",
		lookup: func(ctx context.Context, host string) ([]string, error) {
			return []string{host}, nil
		},
		lookupCtx: context.Background(),
	}

	addresses := r.resolveBackendServiceAddressesLocked()
	assertAddresses(t, addresses, []string{"[2001:db8::1]:11800"})
}

func TestPeriodicDNSResolverLookupRespectsTimeout(t *testing.T) {
	r := &periodicDNSResolver{
		serverAddr: testBackendAddr,
		interval:   time.Hour,
		lookupCtx:  context.Background(),
		lookup: func(ctx context.Context, host string) ([]string, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("lookup context missing deadline")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > dnsLookupTimeout {
				t.Fatalf("lookup timeout remaining = %v, want (0, %v]", remaining, dnsLookupTimeout)
			}
			return []string{"10.0.0.1"}, nil
		},
	}
	addresses := r.resolveBackendServiceAddressesLocked()
	assertAddresses(t, addresses, []string{"10.0.0.1:11800"})
}

func TestPeriodicDNSResolverLookupTimeoutCappedByInterval(t *testing.T) {
	r := &periodicDNSResolver{
		serverAddr: testBackendAddr,
		interval:   time.Second,
		lookupCtx:  context.Background(),
		lookup: func(ctx context.Context, host string) ([]string, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("lookup context missing deadline")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > time.Second {
				t.Fatalf("lookup timeout remaining = %v, want (0, 1s]", remaining)
			}
			return []string{"10.0.0.1"}, nil
		},
	}
	addresses := r.resolveBackendServiceAddressesLocked()
	assertAddresses(t, addresses, []string{"10.0.0.1:11800"})
}

func TestConnectionManagerCreateConnectionFailsWhenDNSBuilderInvalid(t *testing.T) {
	cm, err := NewConnectionManager(nil, 0, testBackendAddr, "", nil, WithPeriodicDNSResolver(true))
	if err != nil {
		t.Fatalf("NewConnectionManager error: %v", err)
	}
	_, err = cm.createConnection()
	if err == nil {
		t.Fatal("expected createConnection error when DNS interval is invalid")
	}
}

func TestConnectionManagerRejectsNegativeDNSResolveInterval(t *testing.T) {
	_, err := NewConnectionManager(nil, 20*time.Second, testBackendAddr, "", nil,
		WithPeriodicDNSResolver(true), WithPeriodicDNSResolveInterval(-time.Second))
	if err == nil {
		t.Fatal("expected NewConnectionManager error for negative DNS resolve interval")
	}
}

func TestConnectionManagerUsesDedicatedDNSResolveInterval(t *testing.T) {
	cm, err := NewConnectionManager(nil, 20*time.Second, testBackendAddr, "", nil,
		WithPeriodicDNSResolver(true), WithPeriodicDNSResolveInterval(5*time.Second))
	if err != nil {
		t.Fatalf("NewConnectionManager error: %v", err)
	}
	if got := cm.periodicDNSInterval(); got != 5*time.Second {
		t.Fatalf("periodicDNSInterval = %v, want 5s", got)
	}

	cm2, err := NewConnectionManager(nil, 20*time.Second, testBackendAddr, "", nil,
		WithPeriodicDNSResolver(true), WithPeriodicDNSResolveInterval(0))
	if err != nil {
		t.Fatalf("NewConnectionManager error: %v", err)
	}
	if got := cm2.periodicDNSInterval(); got != 20*time.Second {
		t.Fatalf("periodicDNSInterval = %v, want 20s", got)
	}
}

func TestPeriodicDNSResolverBuildDoesNotBlockOnSlowDNS(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	builder, err := newPeriodicDNSResolverBuilder(nil, testBackendAddr, time.Hour,
		func(ctx context.Context, host string) ([]string, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
				return []string{"10.0.0.1"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	cc := &testResolverClientConn{}
	done := make(chan struct{})
	var buildErr error
	var r resolver.Resolver
	go func() {
		r, buildErr = builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Build blocked on slow DNS lookup")
	}
	if buildErr != nil {
		t.Fatalf("build resolver error: %v", buildErr)
	}
	defer r.Close()
	assertStateAddresses(t, cc.lastState(), []string{testBackendAddr})

	waitFor(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, time.Second)
	close(release)
	waitFor(t, func() bool {
		return hasExactAddresses(cc.lastState(), []string{"10.0.0.1:11800"})
	}, time.Second)
}

func TestPeriodicDNSResolverShrinksAddressListAndMigratesConnection(t *testing.T) {
	lisA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen A: %v", err)
	}
	defer lisA.Close()
	_, port, err := net.SplitHostPort(lisA.Addr().String())
	if err != nil {
		t.Fatalf("split listen addr: %v", err)
	}
	lisB, err := net.Listen("tcp", net.JoinHostPort("127.0.0.2", port))
	if err != nil {
		t.Skipf("cannot listen on 127.0.0.2:%s: %v", port, err)
	}
	defer lisB.Close()

	gsA := grpc.NewServer()
	gsB := grpc.NewServer()
	go func() { _ = gsA.Serve(lisA) }()
	go func() { _ = gsB.Serve(lisB) }()
	defer gsA.Stop()
	defer gsB.Stop()

	var lookupCalls atomic.Int32
	backend := net.JoinHostPort("backend.test", port)
	builder, err := newPeriodicDNSResolverBuilder(nil, backend, 40*time.Millisecond,
		func(ctx context.Context, host string) ([]string, error) {
			if lookupCalls.Add(1) == 1 {
				return []string{"127.0.0.1", "127.0.0.2"}, nil
			}
			return []string{"127.0.0.2"}, nil
		})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	conn, err := grpc.Dial(builder.target(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
	)
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}
	defer conn.Close()

	waitFor(t, func() bool { return conn.GetState() == connectivity.Ready }, 5*time.Second)
	waitFor(t, func() bool { return lookupCalls.Load() >= 2 }, 5*time.Second)
	// Drop the removed address so ClientConn must use the remaining IP.
	_ = lisA.Close()
	gsA.Stop()

	waitFor(t, func() bool {
		if conn.GetState() == connectivity.Ready {
			return true
		}
		conn.Connect()
		return false
	}, 5*time.Second)
}

func TestPeriodicDNSResolverTLSUsesHostnameServerNameNotIP(t *testing.T) {
	ca, certPEM, keyPEM := mustTestTLSMaterial(t, testBackendHost)
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("load key pair: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	_, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listen addr: %v", err)
	}

	serverCreds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	})
	gs := grpc.NewServer(grpc.Creds(serverCreds))
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		t.Fatal("append CA")
	}
	clientTLS := credentials.NewTLS(&tls.Config{
		RootCAs: pool,
		// Leave ServerName empty so gRPC uses resolver Address.ServerName (the hostname).
		MinVersion: tls.VersionTLS12,
	})

	backend := net.JoinHostPort(testBackendHost, port)
	builder, err := newPeriodicDNSResolverBuilder(nil, backend, time.Hour,
		func(ctx context.Context, host string) ([]string, error) {
			return []string{"127.0.0.1"}, nil
		})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}

	ccProbe := &testResolverClientConn{}
	rProbe, err := builder.Build(resolver.Target{}, ccProbe, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build probe resolver: %v", err)
	}
	waitFor(t, func() bool {
		return hasExactAddresses(ccProbe.lastState(), []string{net.JoinHostPort("127.0.0.1", port)})
	}, time.Second)
	for _, addr := range ccProbe.lastState().Addresses {
		if addr.ServerName != testBackendHost {
			t.Fatalf("ServerName = %q, want %s", addr.ServerName, testBackendHost)
		}
	}
	rProbe.Close()

	builder2, err := newPeriodicDNSResolverBuilder(nil, backend, 50*time.Millisecond,
		func(ctx context.Context, host string) ([]string, error) {
			return []string{"127.0.0.1"}, nil
		})
	if err != nil {
		t.Fatalf("new resolver builder error: %v", err)
	}
	conn, err := grpc.Dial(builder2.target(),
		grpc.WithTransportCredentials(clientTLS),
		grpc.WithResolvers(builder2),
	)
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}
	defer conn.Close()
	waitFor(t, func() bool { return conn.GetState() == connectivity.Ready }, 5*time.Second)
}

func mustTestTLSMaterial(t *testing.T, dnsName string) (caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("server key: %v", err)
	}
	serverTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTpl, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	return caPEM, certPEM, keyPEM
}

type failingUpdateClientConn struct {
	testResolverClientConn
	err         error
	reportCount atomic.Int32
}

func (c *failingUpdateClientConn) UpdateState(state resolver.State) error {
	return c.err
}

func (c *failingUpdateClientConn) ReportError(err error) {
	c.reportCount.Add(1)
}

func assertStateAddresses(t *testing.T, state resolver.State, want []string) {
	t.Helper()
	assertAddresses(t, state.Addresses, want)
}

func TestConnectionManagerReleaseWithNilLoggerDoesNotPanic(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:9", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager error: %v", err)
	}
	conn, err := grpc.Dial("127.0.0.1:9", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}
	cm.mu.Lock()
	cm.connManager["127.0.0.1:9"] = &ManagedConnection{connection: conn, refCount: 1, status: ConnectionStatusConnected}
	cm.mu.Unlock()
	_ = conn.Close()
	// Second close from ReleaseConnection may return an error; nil logger must not panic.
	if err := cm.ReleaseConnection("127.0.0.1:9"); err != nil {
		t.Fatalf("ReleaseConnection error: %v", err)
	}
}

func TestPeriodicDNSResolverRateLimitsRepeatedLookupErrorLogs(t *testing.T) {
	log := &captureLogger{}
	cc := &testResolverClientConn{}
	var n atomic.Int32
	r := &periodicDNSResolver{
		logger:     log,
		serverAddr: testBackendAddr,
		interval:   time.Second,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			i := n.Add(1)
			return nil, &net.DNSError{
				Err:        fmt.Sprintf("no such host via ephemeral :%d", 40000+i),
				Name:       host,
				IsNotFound: true,
			}
		},
		lookupCtx: context.Background(),
		cc:        cc,
		done:      make(chan struct{}),
	}
	r.lastReported = []resolver.Address{newResolverAddress(testBackendHost, testBackendHost, "11800")}
	r.lastGoodAddrs = cloneResolverAddresses(r.lastReported)

	for i := 0; i < 5; i++ {
		_ = r.resolveAndUpdate()
	}
	if got := log.errorCount(); got != 1 {
		t.Fatalf("error logs = %d, want 1 for repeated identical failure class", got)
	}
	if !strings.Contains(log.lastError(), "keep last-good") {
		t.Fatalf("error log = %q, want keep last-good", log.lastError())
	}
}

func TestPeriodicDNSResolverLogsAgainWhenLookupErrorTypeChanges(t *testing.T) {
	log := &captureLogger{}
	cc := &testResolverClientConn{}
	var calls atomic.Int32
	r := &periodicDNSResolver{
		logger:     log,
		serverAddr: testBackendAddr,
		interval:   time.Second,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			if calls.Add(1) == 1 {
				return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
			}
			return nil, &net.DNSError{Err: "i/o timeout", Name: host, IsTimeout: true}
		},
		lookupCtx: context.Background(),
		cc:        cc,
		done:      make(chan struct{}),
	}
	r.lastReported = []resolver.Address{newResolverAddress(testBackendHost, testBackendHost, "11800")}
	r.lastGoodAddrs = cloneResolverAddresses(r.lastReported)

	_ = r.resolveAndUpdate()
	_ = r.resolveAndUpdate()
	if got := log.errorCount(); got != 2 {
		t.Fatalf("error logs = %d, want 2 when error class changes", got)
	}
}

func TestPeriodicDNSResolverLogsRecoveryEvenWhenAddressesUnchanged(t *testing.T) {
	log := &captureLogger{}
	cc := &testResolverClientConn{}
	var calls atomic.Int32
	r := &periodicDNSResolver{
		logger:     log,
		serverAddr: testBackendAddr,
		interval:   time.Second,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("dns unavailable")
			}
			return []string{"10.0.0.1"}, nil
		},
		lookupCtx: context.Background(),
		cc:        cc,
		done:      make(chan struct{}),
	}
	r.lastReported = []resolver.Address{newResolverAddress("10.0.0.1", testBackendHost, "11800")}
	r.lastGoodAddrs = cloneResolverAddresses(r.lastReported)

	_ = r.resolveAndUpdate() // failure
	_ = r.resolveAndUpdate() // recovery, same addresses as last-good
	if got := log.errorCount(); got != 1 {
		t.Fatalf("error logs = %d, want 1", got)
	}
	infos := log.infoMessages()
	if len(infos) != 1 || !strings.Contains(infos[0], "recovered") {
		t.Fatalf("info logs = %v, want one recovery log", infos)
	}
	if strings.Contains(infos[0], "10.0.0.1") {
		t.Fatalf("recovery log should not expose address list: %q", infos[0])
	}
}

func TestPeriodicDNSResolverAddressUpdateLogDoesNotExposeIPs(t *testing.T) {
	log := &captureLogger{}
	cc := &testResolverClientConn{}
	r := &periodicDNSResolver{
		logger:     log,
		serverAddr: testBackendAddr,
		interval:   time.Second,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			return []string{"10.0.0.2"}, nil
		},
		lookupCtx: context.Background(),
		cc:        cc,
		done:      make(chan struct{}),
	}
	r.lastReported = []resolver.Address{newResolverAddress("10.0.0.1", testBackendHost, "11800")}

	_ = r.resolveAndUpdate()
	infos := log.infoMessages()
	if len(infos) != 1 {
		t.Fatalf("info logs = %v, want 1 address-update log", infos)
	}
	if !strings.Contains(infos[0], "count=1") || !strings.Contains(infos[0], "added=1") {
		t.Fatalf("update log = %q, want count/added summary", infos[0])
	}
	if strings.Contains(infos[0], "10.0.0.") {
		t.Fatalf("update log exposed IPs: %q", infos[0])
	}
}

func TestPeriodicDNSResolverCloseStopsLookupGrowth(t *testing.T) {
	log := &captureLogger{}
	var lookups atomic.Int32
	builder, err := newPeriodicDNSResolverBuilder(log, testBackendAddr, 20*time.Millisecond,
		func(ctx context.Context, host string) ([]string, error) {
			lookups.Add(1)
			return nil, errors.New("dns unavailable")
		})
	if err != nil {
		t.Fatalf("newPeriodicDNSResolverBuilder: %v", err)
	}
	cc := &testResolverClientConn{}
	res, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	waitFor(t, func() bool { return lookups.Load() >= 2 }, time.Second)
	res.Close()
	afterClose := lookups.Load()
	errorsAfterClose := log.errorCount()
	time.Sleep(80 * time.Millisecond)
	if lookups.Load() != afterClose {
		t.Fatalf("lookups grew after Close: before=%d after=%d", afterClose, lookups.Load())
	}
	if log.errorCount() != errorsAfterClose {
		t.Fatalf("error logs grew after Close: before=%d after=%d", errorsAfterClose, log.errorCount())
	}
	if errorsAfterClose != 1 {
		t.Fatalf("error logs = %d, want 1 while unhealthy before Close", errorsAfterClose)
	}
}

func TestGetConnectionConcurrentSafe(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:19876", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			if _, err := cm.GetConnection("127.0.0.1:19876"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("GetConnection: %v", err)
	}
	cm.mu.Lock()
	managed := cm.connManager["127.0.0.1:19876"]
	refs := 0
	if managed != nil {
		refs = managed.refCount
	}
	cm.mu.Unlock()
	if refs != n {
		t.Fatalf("refCount = %d, want %d", refs, n)
	}
}

func TestGetConnectionRejectedAfterClose(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:19877", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	if _, err := cm.GetConnection("127.0.0.1:19877"); err != nil {
		t.Fatalf("GetConnection before Close: %v", err)
	}
	cm.Close()
	if _, err := cm.GetConnection("127.0.0.1:19877"); err == nil {
		t.Fatal("GetConnection after Close succeeded, want error")
	}
}

func TestConnectionManagerWaitInterruptedBySignalShutdown(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:19878", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	if _, err := cm.GetConnection("127.0.0.1:19878"); err != nil {
		t.Fatalf("GetConnection: %v", err)
	}

	done := make(chan bool, 1)
	go func() {
		done <- cm.Wait(time.Minute)
	}()
	// Status must stay connected until force-close so send pipelines can drain.
	if st := cm.GetConnectionStatus("127.0.0.1:19878"); st != ConnectionStatusConnected {
		t.Fatalf("status before SignalShutdown = %v, want Connected", st)
	}
	start := time.Now()
	cm.SignalShutdown()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Wait returned true after SignalShutdown, want false")
		}
		if time.Since(start) > 2*time.Second {
			t.Fatalf("Wait took %s, want interrupt promptly", time.Since(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Wait was not interrupted by SignalShutdown")
	}
	if st := cm.GetConnectionStatus("127.0.0.1:19878"); st != ConnectionStatusConnected {
		t.Fatalf("status after SignalShutdown = %v, want Connected until Close", st)
	}
	cm.Close()
	if st := cm.GetConnectionStatus("127.0.0.1:19878"); st != ConnectionStatusShutdown {
		t.Fatalf("status after Close = %v, want Shutdown", st)
	}
}

func TestConnectionManagerCloseStopsDespiteExtraRefs(t *testing.T) {
	var lookups atomic.Int32
	cm, err := NewConnectionManager(nil, time.Second, testBackendAddr, "", nil,
		WithPeriodicDNSResolver(true),
		WithPeriodicDNSResolveInterval(20*time.Millisecond),
		withDNSLookup(func(ctx context.Context, host string) ([]string, error) {
			lookups.Add(1)
			return []string{"127.0.0.1"}, nil
		}),
	)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	// Simulate CDS + pprof + reporter each taking a ref.
	for i := 0; i < 3; i++ {
		if _, err := cm.GetConnection(testBackendAddr); err != nil {
			t.Fatalf("GetConnection: %v", err)
		}
	}
	waitFor(t, func() bool { return lookups.Load() >= 1 }, time.Second)
	cm.Close()
	if st := cm.GetConnectionStatus(testBackendAddr); st != ConnectionStatusShutdown {
		t.Fatalf("status after Close = %v, want Shutdown", st)
	}
	after := lookups.Load()
	time.Sleep(80 * time.Millisecond)
	if lookups.Load() != after {
		t.Fatalf("DNS lookups continued after ConnectionManager.Close: before=%d after=%d", after, lookups.Load())
	}
	// ReleaseConnection after force-close must be a no-op.
	if err := cm.ReleaseConnection(testBackendAddr); err != nil {
		t.Fatalf("ReleaseConnection after Close: %v", err)
	}
}

func TestPeriodicDNSResolverRateLimitsUpdateStateErrors(t *testing.T) {
	log := &captureLogger{}
	cc := &failingUpdateClientConn{err: errors.New("update rejected")}
	r := &periodicDNSResolver{
		logger:     log,
		serverAddr: testBackendAddr,
		interval:   time.Second,
		lookup: func(ctx context.Context, host string) ([]string, error) {
			return []string{"10.0.0.1"}, nil
		},
		lookupCtx: context.Background(),
		cc:        cc,
		done:      make(chan struct{}),
	}
	r.lastReported = []resolver.Address{newResolverAddress(testBackendHost, testBackendHost, "11800")}

	for i := 0; i < 5; i++ {
		if d := r.resolveAndUpdate(); d <= 0 {
			t.Fatalf("expected backoff on UpdateState failure, got %v", d)
		}
	}
	if got := log.errorCount(); got != 1 {
		t.Fatalf("UpdateState error logs = %d, want 1", got)
	}
}

func TestPeriodicDNSResolverWatchRecoversFromLookupPanic(t *testing.T) {
	log := &captureLogger{}
	var panics atomic.Int32
	builder, err := newPeriodicDNSResolverBuilder(log, testBackendAddr, time.Hour,
		func(ctx context.Context, host string) ([]string, error) {
			panics.Add(1)
			panic("injected lookup panic")
		})
	if err != nil {
		t.Fatalf("newPeriodicDNSResolverBuilder: %v", err)
	}
	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	res := r.(*periodicDNSResolver)
	waitFor(t, func() bool { return log.errorCount() >= 1 }, time.Second)
	if !strings.Contains(log.lastError(), "resolver stopped") {
		t.Fatalf("panic log = %q, want resolver stopped", log.lastError())
	}
	select {
	case <-res.watchDone:
	case <-time.After(time.Second):
		t.Fatal("watch should exit after panic recover")
	}
	r.Close()
	if panics.Load() < 1 {
		t.Fatal("expected lookup panic to run at least once")
	}
}

func TestDNSLookupErrorKeyIgnoresEphemeralMessageNoise(t *testing.T) {
	a := &net.DNSError{Err: "no such host from :54321", Name: "x", IsNotFound: true}
	b := &net.DNSError{Err: "no such host from :54322", Name: "x", IsNotFound: true}
	if dnsLookupErrorKey(a) != dnsLookupErrorKey(b) {
		t.Fatalf("keys differ for same DNS class: %q vs %q", dnsLookupErrorKey(a), dnsLookupErrorKey(b))
	}
	timeout := &net.DNSError{Err: "i/o timeout", Name: "x", IsTimeout: true}
	if dnsLookupErrorKey(a) == dnsLookupErrorKey(timeout) {
		t.Fatal("not-found and timeout should use different keys")
	}
}

type captureLogger struct {
	mu     sync.Mutex
	errors []string
	infos  []string
}

func (l *captureLogger) WithField(key string, value interface{}) interface{} { return l }
func (l *captureLogger) Info(args ...interface{})                            {}
func (l *captureLogger) Warn(args ...interface{})                            {}
func (l *captureLogger) Error(args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprint(args...))
}
func (l *captureLogger) Infof(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, fmt.Sprintf(format, args...))
}
func (l *captureLogger) Warnf(format string, args ...interface{}) {}
func (l *captureLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}
func (l *captureLogger) errorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errors)
}
func (l *captureLogger) lastError() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.errors) == 0 {
		return ""
	}
	return l.errors[len(l.errors)-1]
}
func (l *captureLogger) infoMessages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.infos))
	copy(out, l.infos)
	return out
}

func hasExactAddresses(state resolver.State, want []string) bool {
	if len(state.Addresses) != len(want) {
		return false
	}
	for i := range want {
		if state.Addresses[i].Addr != want[i] {
			return false
		}
	}
	return true
}

func assertStateServerNames(t *testing.T, state resolver.State, want []string) {
	t.Helper()
	got := make([]string, 0, len(state.Addresses))
	for _, address := range state.Addresses {
		got = append(got, address.ServerName)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("server names = %v, want %v", got, want)
	}
}

func assertAddresses(t *testing.T, addresses []resolver.Address, want []string) {
	t.Helper()
	got := make([]string, 0, len(addresses))
	for _, address := range addresses {
		got = append(got, address.Addr)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("addresses = %v, want %v", got, want)
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
