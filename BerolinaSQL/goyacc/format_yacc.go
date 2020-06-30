//WHTCORPS INC 2020 All Rights Reserved

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

package main

import (
  "bufio"
  "fmt"
  gofmt "go/format"
  "go/token"
  "io/ioutil"
  "os"
  "regexp"
  "strings"

  parser "github.com/cznic/parser/yacc"
  "github.com/MilevaDB/BerolinaSQL/errors"
  "github.com/MilevaDB/BerolinaSQL/format"
)

func Format(inputFilename string, goldenFilename string) (err error) {
	spec, err := parseFileToSpec(inputFilename)
	if err != nil {
		return err
	}

	yFmt := &OutputFormatter{}
	if err = yFmt.Setup(goldenFilename); err != nil {
		return err
	}
	defer func() {
		teardownErr := yFmt.Teardown()
		if err == nil {
			err = teardownErr
		}
	}()

	if err = printDefinitions(yFmt, spec.Defs); err != nil {
		return err
	}

	if err = printRules(yFmt, spec.Rules); err != nil {
		return err
	}
	return nil
}

unc parseFileToSpec(inputFilename string) (*parser.Specification, error) {
	src, err := ioutil.ReadFile(inputFilename)
	if err != nil {
		return nil, err
	}
	return parser.Parse(token.NewFileSet(), inputFilename, src)
}

// Definition represents data reduced by productions:
//
//	Definition:
//	        START IDENTIFIER
//	|       UNION                      // Case 1
//	|       LCURL RCURL                // Case 2
//	|       ReservedWord Tag NameList  // Case 3
//	|       ReservedWord Tag           // Case 4
//	|       ERROR_VERBOSE              // Case 5
const (
	StartIdentifierCase = iota
	UnionDefinitionCase
	LCURLRCURLCase
	ReservedWordTagNameListCase
	ReservedWordTagCase
)

func printDefinitions(formatter format.Formatter, definitions []*parser.Definition) error {
	for _, def := range definitions {
		var err error
		switch def.Case {
		case StartIdentifierCase:
			err = handleStart(formatter, def)
		case UnionDefinitionCase:
			err = handleUnion(formatter, def)
		case LCURLRCURLCase:
			err = handleProlog(formatter, def)
		case ReservedWordTagNameListCase, ReservedWordTagCase:
			err = handleReservedWordTagNameList(formatter, def)
		}
		if err != nil {
			return err
		}
	}
	_, err := formatter.Format("\n%%%%")
	return err
}

func handleStart(f format.Formatter, definition *parser.Definition) error {
	if err := Ensure(definition).
		and(definition.Token2).
		and(definition.Token2).NotNil(); err != nil {
		return err
	}
	cmt1 := strings.Join(definition.Token.Comments, "\n")
	cmt2 := strings.Join(definition.Token2.Comments, "\n")
	_, err := f.Format("\n%s%s\t%s%s\n", cmt1, definition.Token.Val, cmt2, definition.Token2.Val)
	return err
}

func handleUnion(f format.Formatter, definition *parser.Definition) error {
	if err := Ensure(definition).
		and(definition.Value).NotNil(); err != nil {
		return err
	}
	if len(definition.Value) != 0 {
		_, err := f.Format("%%union%i%s%u\n\n", definition.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
