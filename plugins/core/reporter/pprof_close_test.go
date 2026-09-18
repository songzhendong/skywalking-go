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
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonv3 "github.com/apache/skywalking-go/protocols/collect/common/v3"
)

type nopPprofLogger struct{}

func (nopPprofLogger) WithField(key string, value interface{}) interface{} { return nopPprofLogger{} }
func (nopPprofLogger) Info(args ...interface{})                            {}
func (nopPprofLogger) Infof(format string, args ...interface{})            {}
func (nopPprofLogger) Warn(args ...interface{})                            {}
func (nopPprofLogger) Warnf(format string, args ...interface{})            {}
func (nopPprofLogger) Error(args ...interface{})                           {}
func (nopPprofLogger) Errorf(format string, args ...interface{})           {}

// Late ReportPprof after Close (e.g. AfterFunc StopTask) must not panic the host.
func TestReportPprofAfterCloseDoesNotPanic(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:19890", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	mgr, err := NewPprofTaskManager(nopPprofLogger{}, "127.0.0.1:19890", time.Second, cm, "")
	if err != nil {
		t.Fatalf("NewPprofTaskManager: %v", err)
	}
	mgr.entity = &Entity{ServiceName: "svc", ServiceInstanceName: "inst"}
	mgr.InitPprofTask(mgr.entity)
	mgr.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.ReportPprof("task-1", []byte("profile-data"))
		mgr.ReportPprofError("task-1", errClosedForTest{})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ReportPprof after Close blocked or panicked")
	}
	mgr.WaitSendPipeline()
}

type errClosedForTest struct{}

func (errClosedForTest) Error() string { return "closed" }

func TestPprofCloseStopsActiveTask(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:19891", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	mgr, err := NewPprofTaskManager(nopPprofLogger{}, "127.0.0.1:19891", time.Second, cm, "")
	if err != nil {
		t.Fatalf("NewPprofTaskManager: %v", err)
	}
	mgr.entity = &Entity{ServiceName: "svc", ServiceInstanceName: "inst"}

	var stops int32
	var reported int32
	cmd := &mockDurationPprofCommand{
		taskID:   "cpu-1",
		duration: time.Hour,
		stops:    &stops,
		onStop: func() {
			mgr.ReportPprof("cpu-1", []byte("final"))
			atomic.AddInt32(&reported, 1)
		},
	}
	writer, err := cmd.StartTask()
	if err != nil {
		t.Fatalf("StartTask: %v", err)
	}
	tracked := &activePprofTask{command: cmd, writer: writer}
	timer := time.AfterFunc(time.Hour, func() { mgr.finishActiveTask(tracked) })
	tracked.timer = timer
	mgr.timerMu.Lock()
	mgr.activeTasks = append(mgr.activeTasks, tracked)
	mgr.timerMu.Unlock()

	mgr.Close()
	if atomic.LoadInt32(&stops) != 1 {
		t.Fatalf("Close must StopTask active profiling, stops=%d", stops)
	}
	if atomic.LoadInt32(&reported) != 1 {
		t.Fatal("StopTask during Close must still be able to ReportPprof")
	}
}

type mockDurationPprofCommand struct {
	taskID   string
	duration time.Duration
	stops    *int32
	onStop   func()
}

func (m *mockDurationPprofCommand) GetTaskID() string             { return m.taskID }
func (m *mockDurationPprofCommand) GetCreateTime() int64          { return time.Now().UnixMilli() }
func (m *mockDurationPprofCommand) GetDuration() time.Duration    { return m.duration }
func (m *mockDurationPprofCommand) GetDumpPeriod() int            { return 0 }
func (m *mockDurationPprofCommand) StartTask() (io.Writer, error) { return &bytes.Buffer{}, nil }
func (m *mockDurationPprofCommand) StopTask(io.Writer) {
	atomic.AddInt32(m.stops, 1)
	if m.onStop != nil {
		m.onStop()
	}
}
func (m *mockDurationPprofCommand) IsDirectSamplingType() bool { return false }
func (m *mockDurationPprofCommand) IsInvalidEvent() bool       { return false }
func (m *mockDurationPprofCommand) HasDumpPeriod() bool        { return false }

// A timer-driven finishActiveTask racing Close must not send on a closed
// channel and must still be able to enqueue its final report.
func TestPprofCloseRacesFinishActiveTask(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:19893", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	mgr, err := NewPprofTaskManager(nopPprofLogger{}, "127.0.0.1:19893", time.Second, cm, "")
	if err != nil {
		t.Fatalf("NewPprofTaskManager: %v", err)
	}
	mgr.entity = &Entity{ServiceName: "svc", ServiceInstanceName: "inst"}

	var stops int32
	tracked := make([]*activePprofTask, 0, 8)
	for i := 0; i < 8; i++ {
		cmd := &mockDurationPprofCommand{
			taskID:   "cpu-race",
			duration: time.Hour,
			stops:    &stops,
			onStop:   func() { mgr.ReportPprof("cpu-race", []byte("final")) },
		}
		writer, startErr := cmd.StartTask()
		if startErr != nil {
			t.Fatalf("StartTask: %v", startErr)
		}
		tr := &activePprofTask{command: cmd, writer: writer}
		mgr.timerMu.Lock()
		mgr.activeTasks = append(mgr.activeTasks, tr)
		mgr.timerMu.Unlock()
		tracked = append(tracked, tr)
	}

	var wg sync.WaitGroup
	for _, tr := range tracked {
		wg.Add(1)
		go func(tr *activePprofTask) {
			defer wg.Done()
			mgr.finishActiveTask(tr)
		}(tr)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		mgr.Close()
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close raced with finishActiveTask and blocked")
	}
	if atomic.LoadInt32(&stops) != int32(len(tracked)) {
		t.Fatalf("every task must be stopped once, stops=%d", stops)
	}
	mgr.WaitSendPipeline()
}

func TestPprofHandleCommandRejectedAfterClose(t *testing.T) {
	cm, err := NewConnectionManager(nil, time.Second, "127.0.0.1:19892", "", nil)
	if err != nil {
		t.Fatalf("NewConnectionManager: %v", err)
	}
	defer cm.Close()

	mgr, err := NewPprofTaskManager(nopPprofLogger{}, "127.0.0.1:19892", time.Second, cm, "")
	if err != nil {
		t.Fatalf("NewPprofTaskManager: %v", err)
	}
	mgr.entity = &Entity{ServiceName: "svc", ServiceInstanceName: "inst"}
	mgr.Close()

	// Must return before StartTask when already closed.
	mgr.HandleCommand(&commonv3.Command{
		Command: "PprofTaskQuery",
		Args: []*commonv3.KeyStringValuePair{
			{Key: "TaskId", Value: "late"},
			{Key: "Events", Value: "cpu"},
			{Key: "Duration", Value: "1"},
			{Key: "CreateTime", Value: "9999999999999"},
		},
	})
	mgr.timerMu.Lock()
	n := len(mgr.activeTasks)
	mgr.timerMu.Unlock()
	if n != 0 {
		t.Fatalf("HandleCommand after Close must not leave active tasks, got %d", n)
	}
}
