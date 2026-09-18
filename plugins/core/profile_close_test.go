// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package core

import (
	"sync"
	"testing"
	"time"

	"github.com/apache/skywalking-go/plugins/core/reporter"
)

// Close must stop the producer and close the results channel so the reporter
// can drain without racing late writes.
func TestProfileManagerCloseClosesResultsChannel(t *testing.T) {
	pm := NewProfileManager(nil)
	results := pm.GetProfileResults()

	pm.mu.Lock()
	pm.currentTask = &currentTask{taskID: "t1"}
	pm.mu.Unlock()

	// Enqueue one result before Close.
	pm.enqueueProfileResult(reporter.ProfileResult{TaskID: "t1", Payload: []byte("a"), IsLast: false})

	done := make(chan struct{})
	go func() {
		defer close(done)
		pm.Close()
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ProfileManager.Close blocked")
	}

	// Drain until the results channel is closed.
	var sawT1 bool
	for {
		got, ok := <-results
		if !ok {
			break
		}
		if got.TaskID == "t1" {
			sawT1 = true
		}
	}
	if !sawT1 {
		t.Fatal("did not receive buffered profile result for t1 before close")
	}

	// Late enqueue after Close must not panic.
	pm.enqueueProfileResult(reporter.ProfileResult{TaskID: "late", IsLast: true})
}

func TestProfileManagerCloseCancelsPendingStart(t *testing.T) {
	pm := NewProfileManager(nil)
	pm.mu.Lock()
	pm.addTask(&reporter.TraceProfileTask{
		TaskID:    "future",
		StartTime: time.Now().Add(time.Hour),
		Duration:  1,
		EndTime:   time.Now().Add(2 * time.Hour),
		Status:    reporter.Pending,
	})
	pm.mu.Unlock()

	pm.Close()
	time.Sleep(50 * time.Millisecond)
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.TraceProfileTask != nil || pm.cpuProfileOwned {
		t.Fatal("Close should cancel pending profile start")
	}
}

func TestProfileManagerMonitorSafeAfterClose(t *testing.T) {
	pm := NewProfileManager(nil)
	pm.mu.Lock()
	pm.currentTask = &currentTask{taskID: "t1", duration: 0}
	pm.cpuProfileOwned = false
	pm.mu.Unlock()
	pm.Close()
	// Must not panic when monitor races with Close clearing currentTask.
	pm.monitor()
}

// Close must not wait for the full monitor duration when a profile is running.
func TestProfileManagerCloseUnblocksMonitor(t *testing.T) {
	pm := NewProfileManager(nil)
	pm.mu.Lock()
	pm.currentTask = &currentTask{taskID: "long", duration: 60}
	pm.cpuProfileOwned = false
	pm.mu.Unlock()
	pm.monitorWG.Add(1)
	go func() {
		defer pm.monitorWG.Done()
		pm.monitor()
	}()
	// Give monitor time to block on its timer.
	time.Sleep(20 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		pm.Close()
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked on monitor timer")
	}
}

// Close must Flush while currentTask is still set so IsLast is enqueued.
func TestProfileManagerCloseFlushesIsLast(t *testing.T) {
	pm := NewProfileManager(nil)
	results := pm.GetProfileResults()

	pm.mu.Lock()
	pm.currentTask = &currentTask{taskID: "flush-task"}
	pm.mu.Unlock()

	// Simulate buffered profile bytes waiting for Flush.
	if _, err := pm.profilingWriter.Write([]byte("profile-bytes")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		pm.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked")
	}

	var sawIsLast bool
	for got := range results {
		if got.TaskID == "flush-task" && got.IsLast {
			sawIsLast = true
		}
	}
	if !sawIsLast {
		t.Fatal("Close Flush IsLast must reach FinalReportResults")
	}
}

// TryToAddSegmentLabelSet must not panic if currentTask is cleared concurrently.
func TestTryToAddSegmentLabelSetConcurrentNilSafe(t *testing.T) {
	pm := NewProfileManager(nil)
	pm.mu.Lock()
	pm.currentTask = &currentTask{taskID: "t1", minDurationThreshold: 10}
	pm.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			pm.TryToAddSegmentLabelSet("seg")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			pm.mu.Lock()
			if i%2 == 0 {
				pm.currentTask = nil
			} else {
				pm.currentTask = &currentTask{taskID: "t1", minDurationThreshold: 10}
			}
			pm.mu.Unlock()
		}
	}()
	wg.Wait()
	pm.Close()
}

func TestProfileFinishNilSafe(t *testing.T) {
	pm := NewProfileManager(nil)
	pm.ProfileFinish() // must not panic when TraceProfileTask is nil
	pm.Close()
}

func TestCPUProfilingLockSharedWithPprof(t *testing.T) {
	if !tryAcquireCPUProfiling() {
		t.Fatal("expected to acquire CPU profiling lock")
	}
	defer releaseCPUProfiling()
	if tryAcquireCPUProfiling() {
		t.Fatal("second acquire must fail while held")
	}
	releaseCPUProfiling()
	if !tryAcquireCPUProfiling() {
		t.Fatal("expected to re-acquire after release")
	}
	releaseCPUProfiling()
}
