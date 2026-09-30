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

package echov4

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
)

// TestMiddlewareContinuesWhenEntrySpanFails documents the contract that a
// CreateEntrySpan failure must not abort the HTTP request. The real span
// creation path is covered by plugin scenarios; here we assert the fallback
// helper used when span creation fails.
func TestContinueWithoutSpanOnEntryError(t *testing.T) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/ping", http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	called := false
	next := func(echo.Context) error {
		called = true
		return c.String(http.StatusOK, "ok")
	}

	// Same control flow as middleware when CreateEntrySpan returns an error.
	err := continueWithoutSpan(next, c, errors.New("malformed sw8"))
	if err != nil {
		t.Fatalf("continueWithoutSpan returned %v, want nil from next", err)
	}
	if !called {
		t.Fatal("next handler was not invoked")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
}
