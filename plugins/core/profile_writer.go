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
	"time"
)

type ProfilingWriter struct {
	mu        sync.Mutex            // Ensures concurrent safety
	buf       []byte                // Temporary buffer for current chunk
	pending   []profileRawData      // Spill when reportCh is full (never drop / never block Write)
	chunkSize int                   // Threshold size for chunking (e.g., 1MB)
	reportCh  chan<- profileRawData // Channel for sending data chunks
}

type profileRawData struct {
	data   []byte
	isLast bool
}

// NewProfilingWriter initializes a ProfilingWriter with specified chunk size and report channel
func NewProfilingWriter(chunkSize int, reportCh chan<- profileRawData) *ProfilingWriter {
	return &ProfilingWriter{
		chunkSize: chunkSize,
		reportCh:  reportCh,
		buf:       make([]byte, 0, chunkSize), // Preallocate buffer for efficiency
	}
}

// enqueueLocked pushes to reportCh without blocking. On backpressure the chunk
// is retained in pending for DrainPendingBlocking (Close / task end).
// Caller must hold w.mu.
func (w *ProfilingWriter) enqueueLocked(d profileRawData) {
	select {
	case w.reportCh <- d:
	default:
		w.pending = append(w.pending, d)
	}
}

// Write implements io.Writer, handles data chunking and sending.
// Never blocks on a full rawCh: overflow goes to pending so runtime/pprof
// (and StopCPUProfile / ProfileManager.Close) cannot hang. Chunks are not dropped.
func (w *ProfilingWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.reportCh == nil {
		return len(p), nil
	}
	w.buf = append(w.buf, p...)

	for len(w.buf) >= w.chunkSize {
		chunk := append([]byte(nil), w.buf[:w.chunkSize]...)
		w.buf = w.buf[w.chunkSize:]
		w.enqueueLocked(profileRawData{data: chunk, isLast: false})
	}

	return len(p), nil
}

// Flush sends remaining data in the buffer. Does not block: may spill to pending.
// Call DrainPendingBlocking afterward while the rawCh consumer is still running.
func (w *ProfilingWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.reportCh == nil {
		return
	}

	var payload []byte
	if len(w.buf) > 0 {
		payload = w.buf
	}
	w.buf = nil
	w.enqueueLocked(profileRawData{
		data:   payload,
		isLast: true,
	})
}

// DrainPendingBlocking delivers every pending chunk into reportCh.
// Must run while the rawCh consumer is alive and before reportCh is closed.
// Does not hold mu across the channel send so the consumer can progress.
// Bounded so Close cannot hang if the consumer is stuck.
func (w *ProfilingWriter) DrainPendingBlocking() {
	deadline := time.Now().Add(profileCloseFlushTimeout)
	for {
		w.mu.Lock()
		if w.reportCh == nil || len(w.pending) == 0 {
			w.mu.Unlock()
			return
		}
		d := w.pending[0]
		w.pending = w.pending[1:]
		ch := w.reportCh
		w.mu.Unlock()

		remain := time.Until(deadline)
		if remain <= 0 {
			w.mu.Lock()
			w.pending = append([]profileRawData{d}, w.pending...)
			w.mu.Unlock()
			return
		}
		timer := time.NewTimer(remain)
		select {
		case ch <- d:
			timer.Stop()
		case <-timer.C:
			w.mu.Lock()
			w.pending = append([]profileRawData{d}, w.pending...)
			w.mu.Unlock()
			return
		}
	}
}

// Close stops further writes/flushes so ProfileManager can close rawCh safely.
// Pending must already have been drained.
func (w *ProfilingWriter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reportCh = nil
	w.buf = nil
	w.pending = nil
}
