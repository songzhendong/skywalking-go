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
	"testing"
	"time"
)

// Write must not block forever when the raw channel is full (StopCPUProfile /
// ProfileManager.Close depend on Write returning). Chunks are retained in pending.
func TestProfilingWriterWriteDoesNotBlockOnFullChannel(t *testing.T) {
	ch := make(chan profileRawData, 1)
	w := NewProfilingWriter(8, ch)
	// Fill the only buffer slot.
	ch <- profileRawData{data: []byte("x"), isLast: false}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// 16 bytes => at least one full chunk that hits a full channel.
		_, _ = w.Write(make([]byte, 16))
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Write blocked on full report channel")
	}
	w.mu.Lock()
	pendingLen := len(w.pending)
	bufLen := len(w.buf)
	w.mu.Unlock()
	if pendingLen == 0 && bufLen == 0 {
		t.Fatal("expected retained chunk in pending or buf after backpressure")
	}
}

// DrainPendingBlocking must deliver spilled chunks once the consumer reads.
func TestProfilingWriterDrainPendingBlocking(t *testing.T) {
	ch := make(chan profileRawData, 1)
	w := NewProfilingWriter(8, ch)
	ch <- profileRawData{data: []byte("x"), isLast: false}
	_, _ = w.Write(make([]byte, 16))

	var sawLast bool
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for d := range ch {
			if d.isLast {
				sawLast = true
				return
			}
		}
	}()

	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		w.DrainPendingBlocking()
		w.Flush()
		w.DrainPendingBlocking()
		w.Close()
		close(ch)
	}()

	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		t.Fatal("DrainPendingBlocking blocked")
	}
	select {
	case <-consumerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("consumer did not receive IsLast")
	}
	if !sawLast {
		t.Fatal("expected IsLast after Flush+Drain")
	}
}
