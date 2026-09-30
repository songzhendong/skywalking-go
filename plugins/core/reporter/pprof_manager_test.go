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
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	pprofv10 "github.com/apache/skywalking-go/protocols/collect/pprof/v10"
)

type nopPprofServer struct {
	pprofv10.UnimplementedPprofTaskServer
}

func TestPprofCloseSendChUnblocksPipeline(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	gs := grpc.NewServer()
	pprofv10.RegisterPprofTaskServer(gs, &nopPprofServer{})
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	addr := lis.Addr().String()
	cm, err := NewConnectionManager(nil, 50*time.Millisecond, addr, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	pm, err := NewPprofTaskManager(nil, addr, time.Hour, cm, t.TempDir())
	if err != nil {
		t.Fatalf("NewPprofTaskManager: %v", err)
	}
	pm.entity = &Entity{ServiceName: "svc", ServiceInstanceName: "inst"}
	pm.initPprofSendPipeline()

	done := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		pm.closeSendCh()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("closeSendCh blocked")
	}

	// Enqueue after close must not panic.
	pm.ReportPprof("t1", []byte("x"))
	pm.ReportPprofError("t1", errors.New("test"))
}

func TestPprofTryEnqueueAfterClose(t *testing.T) {
	pm := &PprofTaskManager{
		logger:      nil,
		pprofSendCh: make(chan *pprofv10.PprofData, 1),
	}
	pm.entity = &Entity{ServiceName: "svc", ServiceInstanceName: "inst"}
	pm.closeSendCh()
	pm.closeSendCh() // idempotent
	pm.ReportPprof("t1", []byte("x"))
	pm.ReportPprofError("t1", errors.New("test"))
}
