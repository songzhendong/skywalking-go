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

package logging

import (
	"testing"

	"github.com/apache/skywalking-go/plugins/core/operator"
)

type stubLogReporter struct {
	reported int
}

func (s *stubLogReporter) GetLogContext(bool) interface{} { return nil }
func (s *stubLogReporter) ReportLog(interface{}, interface{}, string, string, map[string]string) {
	s.reported++
}

type stubOperator struct {
	reporter operator.LogReporter
}

func (s *stubOperator) Tracing() interface{}     { return nil }
func (s *stubOperator) Logger() interface{}      { return nil }
func (s *stubOperator) Profiler() interface{}    { return nil }
func (s *stubOperator) Tools() interface{}       { return nil }
func (s *stubOperator) DebugStack() []byte       { return nil }
func (s *stubOperator) Entity() interface{}      { return nil }
func (s *stubOperator) Metrics() interface{}     { return nil }
func (s *stubOperator) LogReporter() interface{} { return s.reporter }
func (s *stubOperator) So11y() interface{}       { return nil }

func TestSendLogEntryNilOperator(t *testing.T) {
	prev := operator.GetOperator
	t.Cleanup(func() { operator.GetOperator = prev })
	operator.GetOperator = func() operator.Operator { return nil }

	// Must not panic when the global operator is unset.
	sendLogEntry(infoLevel, "msg", []string{"k", "v"})
}

func TestSendLogEntryOddLabels(t *testing.T) {
	prev := operator.GetOperator
	t.Cleanup(func() { operator.GetOperator = prev })
	reporter := &stubLogReporter{}
	operator.GetOperator = func() operator.Operator {
		return &stubOperator{reporter: reporter}
	}

	// Odd-length keyValues must not panic; the trailing key is ignored.
	sendLogEntry(infoLevel, "msg", []string{"onlyKey"})
	sendLogEntry(infoLevel, "msg", []string{"a", "b", "orphan"})
	if reporter.reported != 2 {
		t.Fatalf("reported=%d want 2", reporter.reported)
	}
}

func TestParseLabels(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want map[string]string
	}{
		{name: "nil", in: nil, want: nil},
		{name: "too short", in: []string{"a"}, want: nil},
		{name: "pair", in: []string{"a", "b"}, want: map[string]string{"a": "b"}},
		{name: "odd trailing dropped", in: []string{"a", "b", "c"}, want: map[string]string{"a": "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseLabels(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("got[%s]=%q want %q", k, got[k], v)
				}
			}
		})
	}
}
