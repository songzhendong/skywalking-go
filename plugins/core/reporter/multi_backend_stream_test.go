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
	"encoding/json"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"

	v3 "github.com/apache/skywalking-go/protocols/collect/common/v3"
	agentv3 "github.com/apache/skywalking-go/protocols/collect/language/agent/v3"
)

type countingTraceServer struct {
	agentv3.UnimplementedTraceSegmentReportServiceServer
	count atomic.Int64
}

func (s *countingTraceServer) Collect(stream agentv3.TraceSegmentReportService_CollectServer) error {
	for {
		_, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&v3.Commands{})
			}
			return err
		}
		s.count.Add(1)
	}
}

func TestMultiBackendCollectSendFailsOverAfterActiveStops(t *testing.T) {
	aSrv := &countingTraceServer{}
	bSrv := &countingTraceServer{}

	aLis, aGS := serveTrace(t, aSrv)
	aAddr := aLis.Addr().String()
	bLis, bGS := serveTrace(t, bSrv)
	defer bLis.Close()
	defer bGS.Stop()
	bAddr := bLis.Addr().String()

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
	client := agentv3.NewTraceSegmentReportServiceClient(conn)

	ctx, cancel, stopOpen := BackendStreamContext(backends, time.Second)
	stream, err := client.Collect(metadata.NewOutgoingContext(ctx, cm.GetMD()))
	if timedOut := stopOpen(); err != nil || timedOut {
		cancel()
		t.Fatalf("open Collect: err=%v timedOut=%v", err, timedOut)
	}
	go WatchConnCancelOnUnready(ctx, cancel, conn)

	seg := &agentv3.SegmentObject{TraceId: "t1", TraceSegmentId: "s1"}
	if sendErr := MultiBackendSend(cancel, func() error { return stream.Send(seg) }, time.Second); sendErr != nil {
		t.Fatalf("initial Send: %v", sendErr)
	}
	waitFor(t, func() bool { return aSrv.count.Load() >= 1 }, 5*time.Second)

	// Stop active like docker kill: close listener hard so the peer half-opens.
	aGS.Stop()
	_ = aLis.Close()

	// Bound Send: either errors quickly or is canceled by MultiBackendSend.
	_ = MultiBackendSend(cancel, func() error {
		return stream.Send(&agentv3.SegmentObject{TraceId: "t2", TraceSegmentId: "s2"})
	}, 2*time.Second)
	cancel()

	// Mirror production: recreate ClientConn so pick_first can leave the dead peer.
	if recreateErr := cm.RecreateConnection(backends); recreateErr != nil {
		t.Fatalf("RecreateConnection: %v", recreateErr)
	}
	conn = cm.PeekConnection(backends)
	if conn == nil {
		t.Fatal("PeekConnection nil after recreate")
	}
	client = agentv3.NewTraceSegmentReportServiceClient(conn)

	ctx2, cancel2, stopOpen2 := BackendStreamContext(backends, 5*time.Second)
	defer cancel2()
	conn.Connect()
	stream2, collectErr := client.Collect(metadata.NewOutgoingContext(ctx2, cm.GetMD()))
	if timedOut := stopOpen2(); collectErr != nil || timedOut {
		t.Fatalf("reopen Collect: err=%v timedOut=%v", collectErr, timedOut)
	}
	before := bSrv.count.Load()
	if sendErr := MultiBackendSend(cancel2, func() error {
		return stream2.Send(&agentv3.SegmentObject{TraceId: "t3", TraceSegmentId: "s3"})
	}, 5*time.Second); sendErr != nil {
		t.Fatalf("standby Send: %v", sendErr)
	}
	waitFor(t, func() bool { return bSrv.count.Load() > before }, 15*time.Second)
}

func serveTrace(t *testing.T, srv agentv3.TraceSegmentReportServiceServer) (net.Listener, *grpc.Server) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	agentv3.RegisterTraceSegmentReportServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	return lis, gs
}

func TestMultiBackendSendCancelsOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := MultiBackendSend(cancel, func() error {
		<-ctx.Done()
		return ctx.Err()
	}, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout cancel error")
	}
}

func TestMultiBackendSendReturnsWhenSendIgnoresCancel(t *testing.T) {
	// Half-open peers may not unblock Send/CloseAndRecv after ctx cancel.
	// MultiBackendSend must still return so the reporter can recreate + standby.
	start := time.Now()
	err := MultiBackendSend(func() {}, func() error {
		time.Sleep(30 * time.Second)
		return nil
	}, 50*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error when send ignores cancel")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("MultiBackendSend blocked too long after cancel: %v", elapsed)
	}
}

func TestRecreateConnectionKeepsStatusConnected(t *testing.T) {
	aLis, aGS := serveTrace(t, &countingTraceServer{})
	defer aLis.Close()
	defer aGS.Stop()
	bLis, bGS := serveTrace(t, &countingTraceServer{})
	defer bLis.Close()
	defer bGS.Stop()

	backends := aLis.Addr().String() + "," + bLis.Addr().String()
	cm, err := NewConnectionManager(nil, time.Second, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()
	if _, getErr := cm.GetConnection(backends); getErr != nil {
		t.Fatalf("GetConnection: %v", getErr)
	}

	var sawShutdown atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if cm.GetConnectionStatus(backends) == ConnectionStatusShutdown {
					sawShutdown.Store(true)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	for i := 0; i < 5; i++ {
		if recreateErr := cm.RecreateConnection(backends); recreateErr != nil {
			close(stop)
			wg.Wait()
			t.Fatalf("RecreateConnection: %v", recreateErr)
		}
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	if sawShutdown.Load() {
		t.Fatal("GetConnectionStatus returned Shutdown during RecreateConnection")
	}
	if got := cm.GetConnectionStatus(backends); got != ConnectionStatusConnected {
		t.Fatalf("status after recreate = %v, want Connected", got)
	}
}

func TestMultiBackendStatusStaysConnectedAfterConnClose(t *testing.T) {
	aLis, aGS := serveTrace(t, &countingTraceServer{})
	defer aLis.Close()
	defer aGS.Stop()
	bLis, bGS := serveTrace(t, &countingTraceServer{})
	defer bLis.Close()
	defer bGS.Stop()

	backends := aLis.Addr().String() + "," + bLis.Addr().String()
	cm, err := NewConnectionManager(nil, 50*time.Millisecond, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()
	conn, err := cm.GetConnection(backends)
	if err != nil {
		t.Fatalf("GetConnection: %v", err)
	}
	_ = conn.Close()
	// Status watcher must not publish Shutdown while the map entry remains.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cm.GetConnectionStatus(backends) == ConnectionStatusShutdown {
			t.Fatal("multi-backend status became Shutdown after ClientConn.Close")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := cm.GetConnectionStatus(backends); got != ConnectionStatusConnected {
		t.Fatalf("status=%v want Connected", got)
	}
}

// Concurrent GetConnection / RecreateConnection / ReleaseConnection used to race
// on the unlocked connManager map (Codex P1). Stress the locked paths.
func TestConcurrentGetRecreateRelease(t *testing.T) {
	aLis, aGS := serveTrace(t, &countingTraceServer{})
	defer aLis.Close()
	defer aGS.Stop()
	bLis, bGS := serveTrace(t, &countingTraceServer{})
	defer bLis.Close()
	defer bGS.Stop()

	backends := aLis.Addr().String() + "," + bLis.Addr().String()
	cm, err := NewConnectionManager(nil, 50*time.Millisecond, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, getErr := cm.GetConnection(backends); getErr != nil {
					t.Errorf("GetConnection: %v", getErr)
					return
				}
				_ = cm.RecreateConnection(backends)
				_ = cm.ReleaseConnection(backends)
				_ = cm.PeekConnection(backends)
				_ = cm.GetConnectionStatus(backends)
			}
		}()
	}
	wg.Wait()
}

func TestRecreateConnectionUnblocksHungMultiBackendSend(t *testing.T) {
	// Codex P2: MultiBackendSend may detach a Send goroutine; RecreateConnection
	// must Close the old ClientConn so that goroutine can exit.
	aLis, aGS := serveTrace(t, &countingTraceServer{})
	defer aLis.Close()
	defer aGS.Stop()
	bLis, bGS := serveTrace(t, &countingTraceServer{})
	defer bLis.Close()
	defer bGS.Stop()

	backends := aLis.Addr().String() + "," + bLis.Addr().String()
	cm, err := NewConnectionManager(nil, 50*time.Millisecond, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()
	if _, getErr := cm.GetConnection(backends); getErr != nil {
		t.Fatalf("GetConnection: %v", getErr)
	}

	oldTimeout, oldGrace := multiBackendSendTimeout, multiBackendSendCancelGrace
	multiBackendSendTimeout = 200 * time.Millisecond
	multiBackendSendCancelGrace = 50 * time.Millisecond
	defer func() {
		multiBackendSendTimeout = oldTimeout
		multiBackendSendCancelGrace = oldGrace
	}()

	started := make(chan struct{})
	unblocked := make(chan struct{})
	go func() {
		_ = MultiBackendSend(nil, func() error {
			close(started)
			conn := cm.PeekConnection(backends)
			if conn == nil {
				return nil
			}
			// Block until the ClientConn is closed by RecreateConnection.
			for conn.GetState() != connectivity.Shutdown {
				if !conn.WaitForStateChange(context.Background(), conn.GetState()) {
					break
				}
			}
			close(unblocked)
			return errMultiBackendSendTimeout
		}, 0)
	}()
	<-started
	if recreateErr := cm.RecreateConnection(backends); recreateErr != nil {
		t.Fatalf("RecreateConnection: %v", recreateErr)
	}
	select {
	case <-unblocked:
	case <-time.After(3 * time.Second):
		t.Fatal("hung send did not unblock after RecreateConnection closed old conn")
	}
}

func TestMultiBackendServiceConfigRetryOnlyProperties(t *testing.T) {
	var cfg struct {
		MethodConfig []struct {
			Name []struct {
				Service string `json:"service"`
				Method  string `json:"method"`
			} `json:"name"`
			RetryPolicy *struct {
				RetryableStatusCodes []string `json:"retryableStatusCodes"`
			} `json:"retryPolicy"`
		} `json:"methodConfig"`
	}
	if err := json.Unmarshal([]byte(multiBackendServiceConfig), &cfg); err != nil {
		t.Fatalf("service config JSON: %v", err)
	}
	var withRetry int
	for _, mc := range cfg.MethodConfig {
		if mc.RetryPolicy == nil {
			continue
		}
		withRetry++
		if len(mc.Name) != 1 ||
			mc.Name[0].Service != "skywalking.v3.ManagementService" ||
			mc.Name[0].Method != "reportInstanceProperties" {
			t.Fatalf("retryPolicy must only target reportInstanceProperties, got %+v", mc.Name)
		}
		if len(mc.RetryPolicy.RetryableStatusCodes) != 1 ||
			mc.RetryPolicy.RetryableStatusCodes[0] != "UNAVAILABLE" {
			t.Fatalf("unexpected retryableStatusCodes: %+v", mc.RetryPolicy.RetryableStatusCodes)
		}
	}
	if withRetry != 1 {
		t.Fatalf("want exactly 1 methodConfig with retryPolicy, got %d", withRetry)
	}
}

// TestRaceConnManagerGetRecreate is picked up by `make test-race` (-run '^TestRace').
func TestRaceConnManagerGetRecreate(t *testing.T) {
	TestConcurrentGetRecreateRelease(t)
}

// TestRacePprofClientSwap is picked up by `make test-race` (-run '^TestRace').
// Concurrent recreate and currentPprofClient/load must not race on the client field.
func TestRacePprofClientSwap(t *testing.T) {
	aLis, aGS := serveTrace(t, &countingTraceServer{})
	defer aLis.Close()
	defer aGS.Stop()
	bLis, bGS := serveTrace(t, &countingTraceServer{})
	defer bLis.Close()
	defer bGS.Stop()

	backends := aLis.Addr().String() + "," + bLis.Addr().String()
	cm, err := NewConnectionManager(nil, 50*time.Millisecond, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()
	pm, err := NewPprofTaskManager(nil, backends, time.Hour, cm, t.TempDir())
	if err != nil {
		t.Fatalf("NewPprofTaskManager: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = cm.RecreateConnection(backends)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = pm.currentPprofClient()
			_ = pm.loadPprofClient()
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestMultiBackendSendRecoversPanic(t *testing.T) {
	err := MultiBackendSend(nil, func() error {
		panic("boom")
	}, time.Second)
	if !IsMultiBackendSendPanic(err) {
		t.Fatalf("want panic error, got %v", err)
	}
}

func TestRecreateConnectionDoesNotResurrectAfterClose(t *testing.T) {
	aLis, aGS := serveTrace(t, &countingTraceServer{})
	defer aLis.Close()
	defer aGS.Stop()
	bLis, bGS := serveTrace(t, &countingTraceServer{})
	defer bLis.Close()
	defer bGS.Stop()

	backends := aLis.Addr().String() + "," + bLis.Addr().String()
	cm, err := NewConnectionManager(nil, 50*time.Millisecond, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	if _, getErr := cm.GetConnection(backends); getErr != nil {
		t.Fatalf("GetConnection: %v", getErr)
	}
	cm.Close()
	if recreateErr := cm.RecreateConnection(backends); recreateErr == nil {
		t.Fatal("expected recreate to fail after Close removed the entry")
	}
	if cm.PeekConnection(backends) != nil {
		t.Fatal("recreate must not resurrect a closed connection manager entry")
	}
}
