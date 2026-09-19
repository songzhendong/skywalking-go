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

package core

import (
	"fmt"
	"sync"
	"testing"
)

type testNoInitHistogramBucket struct {
	bucket float64
	value  *int64
}

func (b *testNoInitHistogramBucket) Bucket() float64 { return b.bucket }
func (b *testNoInitHistogramBucket) Value() *int64   { return b.value }

// TestNewHistogramFromExistingBucketsLen guards the NoInit conversion path:
// make([]*T, 0, n) then index assignment panics with index out of range.
func TestNewHistogramFromExistingBucketsLen(t *testing.T) {
	v0, v1, v2 := int64(1), int64(2), int64(3)
	buckets := []interface{}{
		&testNoInitHistogramBucket{bucket: 0, value: &v0},
		&testNoInitHistogramBucket{bucket: 10, value: &v1},
		&testNoInitHistogramBucket{bucket: 100, value: &v2},
	}
	h := newHistogramFromExistingBuckets("request_duration", map[string]string{"k": "v"}, buckets)
	if h == nil {
		t.Fatal("histogram is nil")
	}
	if got := len(h.buckets); got != 3 {
		t.Fatalf("len(buckets)=%d, want 3", got)
	}
	if h.buckets[0].bucket != 0 || h.buckets[1].bucket != 10 || h.buckets[2].bucket != 100 {
		t.Fatalf("bucket boundaries wrong: %+v", h.buckets)
	}
	if h.buckets[0].value != &v0 || h.buckets[1].value != &v1 || h.buckets[2].value != &v2 {
		t.Fatal("bucket value pointers were not preserved")
	}
	h.Observe(15)
	if got := *h.buckets[1].value; got != 3 { // 2 + 1
		t.Fatalf("Observe did not hit expected bucket, value=%d", got)
	}
}

// TestRaceRuntimeContextConcurrentCloneAndSet is picked up by `make test-race`
// (-run '^TestRace'). It stresses Get/Set/clone under the unit-test GLS
// (shared tlsData across goroutines). Production TLS is per-g, so this is a
// lock regression test more than a byte-for-byte production race repro.
func TestRaceRuntimeContextConcurrentCloneAndSet(t *testing.T) {
	ResetTracingContext()
	defer ResetTracingContext()

	Tracing.SetRuntimeContextValue("seed", "1")

	const workers = 8
	const iters = 2000
	var wg sync.WaitGroup
	wg.Add(workers * 2)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				Tracing.SetRuntimeContextValue("k", fmt.Sprintf("%d-%d", id, i))
			}
		}(w)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				_ = Tracing.CaptureContext()
				_ = Tracing.GetRuntimeContextValue("seed")
			}
		}()
	}
	wg.Wait()
}
