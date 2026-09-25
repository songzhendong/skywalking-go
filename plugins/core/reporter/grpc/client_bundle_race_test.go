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

package grpc

import (
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/apache/skywalking-go/plugins/core/reporter"
	agentv3 "github.com/apache/skywalking-go/protocols/collect/language/agent/v3"
)

// TestRaceGrpcServiceClientsSwap is exercised by `make test-race` (-run '^TestRace').
// Concurrent bind (recreate) and load+RPC-open must not race on client fields.
func TestRaceGrpcServiceClientsSwap(t *testing.T) {
	a := serveBackendMocks(t)
	b := serveBackendMocks(t)
	defer a.stop()
	defer b.stop()
	backends := a.addr() + "," + b.addr()

	logger := &capturingLogger{}
	cm, err := reporter.NewConnectionManager(logger, time.Second, backends, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()
	cds, err := reporter.NewCDSManager(logger, backends, 0, cm)
	if err != nil {
		t.Fatalf("NewCDSManager: %v", err)
	}
	pprof, err := reporter.NewPprofTaskManager(logger, backends, time.Hour, cm, t.TempDir())
	if err != nil {
		t.Fatalf("NewPprofTaskManager: %v", err)
	}
	rep, err := NewGRPCReporter(logger, backends, time.Second, time.Hour, cm, cds, pprof)
	if err != nil {
		t.Fatalf("NewGRPCReporter: %v", err)
	}
	gr := rep.(*gRPCReporter)

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
				gr.reconnectMultiBackend()
				time.Sleep(time.Millisecond)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			c := gr.clients()
			if c == nil {
				continue
			}
			ctx, cancel, stopOpen := reporter.BackendStreamContext(backends, time.Second)
			stream, err := c.trace.Collect(metadata.NewOutgoingContext(ctx, cm.GetMD()))
			_ = stopOpen()
			if err == nil && stream != nil {
				_ = stream.Send(&agentv3.SegmentObject{TraceId: "race", TraceSegmentId: "s"})
				_, _ = stream.CloseAndRecv()
			}
			cancel()
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}
