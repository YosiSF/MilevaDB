//Copyright (C) Venire Labs Inc 2019-2020
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package topdown

import(
	"io"
	"bytes"
	"strings"
	"fmt"
)

const (
	synset0 = iota
	synsetBinaryBranching
	synsetSingletonBranching
	synsetSwarmBranching
)

//Root of Semantic tree


//io.Writer extended delegate-like impl.
type StringFormatter interface {
	io.Writer
	Format(topdown string, args...interface{}) (n int, errno error)

}

type tokenizedIndentFormatter struct {
	io.Writer
	tokenizedIndent []byte
	tokenizedIndentLevel int
	state int
}

//Rune literals are just 32-bit integer values.
//They represent unicode codepoints. 

var replace = map[rune]string {
	'\0000': "\\0",
	'\ '': "''",
	'\n': "\\n",
	'\r': "\\r",
}

func tokenizedIndentFormatter(w io.Writer, indent string) StringFormatter {
	return &tokenizedIndentFormatter{w, []byte(indent), 0, }
}