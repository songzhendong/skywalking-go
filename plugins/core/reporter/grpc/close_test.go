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

package grpc

import (
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/apache/skywalking-go/plugins/core/reporter"

	agentv3 "github.com/apache/skywalking-go/protocols/collect/language/agent/v3"
	logv3 "github.com/apache/skywalking-go/protocols/collect/logging/v3"
)

const closeTestBackend = "127.0.0.1:19800"

func newCloseTestReporter(t *testing.T, booted, withTraceClient bool) (*gRPCReporter, *reporter.ConnectionManager) {
	t.Helper()
	connManager, err := reporter.NewConnectionManager(nil, time.Second, closeTestBackend, "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	// Three references, as CDS, pprof and the reporter itself take one each.
	var clientConn *grpc.ClientConn
	for i := 0; i < 3; i++ {
		clientConn, err = connManager.GetConnection(closeTestBackend)
		if err != nil {
			t.Fatalf("GetConnection: %v", err)
		}
	}
	r := &gRPCReporter{
		logger:        &capturingLogger{},
		serverAddr:    closeTestBackend,
		connManager:   connManager,
		tracingSendCh: make(chan *agentv3.SegmentObject, 1),
		metricsSendCh: make(chan []*agentv3.MeterData, 1),
		logSendCh:     make(chan *logv3.LogData, 1),
		bootFlag:      booted,
	}
	if withTraceClient {
		r.traceClient = agentv3.NewTraceSegmentReportServiceClient(clientConn)
	}
	return r, connManager
}

// Close always force-closes the shared ClientConn after draining send pipelines
// (when Boot ran with a trace client), so CDS/pprof refs cannot keep DNS alive.
func TestCloseForceClosesAfterDrain(t *testing.T) {
	r, connManager := newCloseTestReporter(t, true, true)
	r.initSendPipeline()

	r.Close()
	if status := connManager.GetConnectionStatus(closeTestBackend); status != reporter.ConnectionStatusShutdown {
		t.Fatalf("status after Close = %v, want Shutdown", status)
	}
}

// Without a tracing pipeline nothing else would ever close the connection, so
// Close has to do it, even though CDS and pprof still hold references.
func TestCloseClosesConnectionWhenNoPipelineDrains(t *testing.T) {
	for _, tt := range []struct {
		name            string
		booted          bool
		withTraceClient bool
	}{
		{name: "not booted", booted: false, withTraceClient: true},
		{name: "booted without trace client", booted: true, withTraceClient: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, connManager := newCloseTestReporter(t, tt.booted, tt.withTraceClient)
			r.Close()
			if status := connManager.GetConnectionStatus(closeTestBackend); status != reporter.ConnectionStatusShutdown {
				t.Fatalf("status after Close = %v, want Shutdown", status)
			}
		})
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	r, connManager := newCloseTestReporter(t, true, true)
	r.initSendPipeline()
	r.Close()
	r.Close()
	if status := connManager.GetConnectionStatus(closeTestBackend); status != reporter.ConnectionStatusShutdown {
		t.Fatalf("status after double Close = %v, want Shutdown", status)
	}
}
