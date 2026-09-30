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

package logger

import "testing"

func TestGetLogContextNilOperator(t *testing.T) {
	prev := GetOperator
	t.Cleanup(func() { GetOperator = prev })
	GetOperator = func() Operator { return nil }

	if got := GetLogContext(true); got != nil {
		t.Fatalf("GetLogContext with nil operator = %v, want nil", got)
	}
	if got := GetLogContextString(); got != "" {
		t.Fatalf("GetLogContextString with nil operator = %q, want empty", got)
	}
}

type stubOperatorNilReporter struct{}

func (stubOperatorNilReporter) Tracing() interface{}     { return nil }
func (stubOperatorNilReporter) ChangeLogger(interface{}) {}
func (stubOperatorNilReporter) Entity() interface{}      { return nil }
func (stubOperatorNilReporter) LogReporter() interface{} { return nil }

func TestGetLogContextNilLogReporter(t *testing.T) {
	prev := GetOperator
	t.Cleanup(func() { GetOperator = prev })
	GetOperator = func() Operator { return stubOperatorNilReporter{} }

	if got := GetLogContext(false); got != nil {
		t.Fatalf("GetLogContext with nil LogReporter = %v, want nil", got)
	}
}
