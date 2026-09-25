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
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

const (
	testBackendHost     = "oap.example.com"
	testBackendAddr     = testBackendHost + ":11800"
	testIPLiteralAddr   = "10.0.0.1:11800"
	testBackendHostA    = "a.example.com"
	testBackendHostB    = "b.example.com"
	testBackendAddrA    = testBackendHostA + ":11800"
	testBackendAddrB    = testBackendHostB + ":11800"
	testMultiBackendCSV = testBackendAddrA + "," + testBackendAddrB
)

type testResolverClientConn struct {
	mu    sync.Mutex
	state resolver.State
	count int
}

func (c *testResolverClientConn) UpdateState(s resolver.State) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
	c.count++
	return nil
}
func (c *testResolverClientConn) ReportError(error)             {}
func (c *testResolverClientConn) NewAddress([]resolver.Address) {}
func (c *testResolverClientConn) NewServiceConfig(string)       {}
func (c *testResolverClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}

func (c *testResolverClientConn) lastState() resolver.State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
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

func TestFirstHostnameAuthority(t *testing.T) {
	if got := firstHostnameAuthority([]string{testIPLiteralAddr, testBackendAddr}); got != testBackendHost {
		t.Fatalf("got %q", got)
	}
	if got := firstHostnameAuthority([]string{testIPLiteralAddr, "10.0.0.2:11800"}); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestConfiguredAddressesUseHostnameSNIForIPLiterals(t *testing.T) {
	addrs := configuredAddressesAsResolverState([]string{testIPLiteralAddr, testBackendAddr})
	if len(addrs) != 2 {
		t.Fatalf("len=%d", len(addrs))
	}
	if addrs[0].Addr != testIPLiteralAddr || addrs[0].ServerName != testBackendHost {
		t.Fatalf("ip entry = %+v", addrs[0])
	}
	if addrs[1].ServerName != testBackendHost {
		t.Fatalf("host entry ServerName = %q", addrs[1].ServerName)
	}
}

func TestParseBackendServiceList(t *testing.T) {
	got, err := parseBackendServiceList(" " + testBackendAddrA + " , " + testIPLiteralAddr + ", " + testBackendAddrA + " ")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != 2 || got[0] != testBackendAddrA || got[1] != testIPLiteralAddr {
		t.Fatalf("got %#v", got)
	}
	if _, emptyErr := parseBackendServiceList(""); emptyErr == nil {
		t.Fatal("expected empty error")
	}
	if _, badErr := parseBackendServiceList("bad"); badErr == nil {
		t.Fatal("expected invalid address error")
	}
	v6, err := parseBackendServiceList("[2001:db8::1]:11800")
	if err != nil {
		t.Fatalf("ipv6: %v", err)
	}
	if len(v6) != 1 || v6[0] != "[2001:db8::1]:11800" {
		t.Fatalf("ipv6 got %#v", v6)
	}
}

func TestIsIPLiteralHost(t *testing.T) {
	if !isIPLiteralHost("127.0.0.1") || !isIPLiteralHost("::1") || !isIPLiteralHost("[::1]") {
		t.Fatal("expected IP literals")
	}
	if isIPLiteralHost(testBackendHost) || isIPLiteralHost("") {
		t.Fatal("expected non-IP")
	}
}

func TestStaticMultiBackendPublishesLiterals(t *testing.T) {
	builder, err := newStaticBackendResolverBuilder(nil, testMultiBackendCSV, nil)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer r.Close()

	if !hasExactAddresses(cc.lastState(), []string{testBackendAddrA, testBackendAddrB}) {
		t.Fatalf("state=%+v", cc.lastState())
	}
}

func TestStaticMultiBackendDoesNotExpandDNS(t *testing.T) {
	builder, err := newStaticBackendResolverBuilder(nil, testIPLiteralAddr+",10.0.0.2:11800", nil)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	cc := &testResolverClientConn{}
	r, err := builder.Build(resolver.Target{}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer r.Close()
	if !hasExactAddresses(cc.lastState(), []string{testIPLiteralAddr, "10.0.0.2:11800"}) {
		t.Fatalf("state=%+v", cc.lastState())
	}
}

func TestMultiBackendPickFirstFailsOver(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	goodAddr := lis.Addr().String()
	gs := grpc.NewServer()
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	badLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bad listen: %v", err)
	}
	badAddr := badLis.Addr().String()
	_ = badLis.Close()

	backends := badAddr + "," + goodAddr
	builder, err := newStaticBackendResolverBuilder(nil, backends, nil)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	conn, err := grpc.Dial(builder.target(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(builder),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	waitFor(t, func() bool {
		return conn.GetState() == connectivity.Ready
	}, 8*time.Second)
}

func TestConnectionManagerMultiBackendPickFirst(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	goodAddr := lis.Addr().String()
	gs := grpc.NewServer()
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	badLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bad listen: %v", err)
	}
	badAddr := badLis.Addr().String()
	_ = badLis.Close()

	backends := badAddr + "," + goodAddr
	cm, err := NewConnectionManager(nil, time.Second, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	conn, err := cm.GetConnection(backends)
	if err != nil {
		t.Fatalf("GetConnection: %v", err)
	}
	waitFor(t, func() bool {
		return conn.GetState() == connectivity.Ready
	}, 8*time.Second)
}

func TestConnectionManagerFailoverAfterActiveStops(t *testing.T) {
	aLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen a: %v", err)
	}
	aAddr := aLis.Addr().String()
	aGS := grpc.NewServer()
	go func() { _ = aGS.Serve(aLis) }()

	bLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen b: %v", err)
	}
	defer bLis.Close()
	bAddr := bLis.Addr().String()
	bGS := grpc.NewServer()
	go func() { _ = bGS.Serve(bLis) }()
	defer bGS.Stop()

	backends := aAddr + "," + bAddr
	cm, err := NewConnectionManager(nil, time.Second, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()
	conn, err := cm.GetConnection(backends)
	if err != nil {
		t.Fatalf("GetConnection: %v", err)
	}
	waitFor(t, func() bool {
		return conn.GetState() == connectivity.Ready
	}, 8*time.Second)

	aGS.Stop()
	_ = aLis.Close()

	waitFor(t, func() bool {
		return conn.GetState() == connectivity.Ready
	}, 15*time.Second)
	if status := cm.GetConnectionStatus(backends); status == ConnectionStatusShutdown {
		t.Fatalf("status=%v after failover", status)
	}
}

func TestConnectionManagerNormalizedSingleBackendDial(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	addr := lis.Addr().String()
	gs := grpc.NewServer()
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	cases := []string{
		addr + "," + addr, // duplicate collapses to one
		addr + ",",        // trailing comma
	}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			cm, err := NewConnectionManager(nil, time.Second, raw, "", nil)
			if err != nil {
				t.Fatalf("NewConnectionManager: %v", err)
			}
			defer cm.Close()
			conn, err := cm.GetConnection(raw)
			if err != nil {
				t.Fatalf("GetConnection: %v", err)
			}
			waitFor(t, func() bool {
				return conn.GetState() == connectivity.Ready
			}, 8*time.Second)
		})
	}
}
