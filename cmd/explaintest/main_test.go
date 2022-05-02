MilevaDB Copyright (c) 2022 MilevaDB Authors: Karl Whitford, Spencer Fogelman, Josh Leder
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import "testing"

func TestIsQuery(t *testing.T) {
	tbl := []struct {
		allegrosql string
		ok         bool
	}{
		{"/*comment*/ select 1;", true},
		{"/*comment*/ /*comment*/ select 1;", true},
		{"select /*comment*/ 1 /*comment*/;", true},
		{"(select /*comment*/ 1 /*comment*/);", true},
	}
	for _, tb := range tbl {
		if isQuery(tb.allegrosql) != tb.ok {
			t.Fatalf("%s", tb.allegrosql)
		}
	}
}

func TestTrimALLEGROSQL(t *testing.T) {
	tbl := []struct {
		allegrosql string
		target     string
	}{
		{"/*comment*/ select 1; ", "select 1;"},
		{"/*comment*/ /*comment*/ select 1;", "select 1;"},
		{"select /*comment*/ 1 /*comment*/;", "select /*comment*/ 1 /*comment*/;"},
		{"/*comment select 1; ", "/*comment select 1;"},
	}
	for _, tb := range tbl {
		if trimALLEGROSQL(tb.allegrosql) != tb.target {
			t.Fatalf("%s", tb.allegrosql)
		}
	}
}
