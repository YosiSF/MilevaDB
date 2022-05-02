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

import (
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/berolinaAllegroSQL"
	"go/token"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/whtcorpsinc/MilevaDB-Prod/soliton/replog"
)

func main() {
	err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "import check fail: %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	if flag.NArg() != 1 {
		return errors.New("need given root folder param")
	}

	root, err := filepath.EvalSymlinks(flag.Arg(0))
	if err != nil {
		return fmt.Errorf("eval symlinks error: %s", err)
	}

	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		return checkFile(path)
	})
}

func checkFile(path string) error {
	src, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	file, err := berolinaAllegroSQL.ParseFile(token.NewFileSet(), path, src, berolinaAllegroSQL.AllErrors|berolinaAllegroSQL.ParseComments)
	if err != nil {
		return err
	}

	var importSpecs []*ast.ImportSpec
	for _, d := range file.Decls {
		if genDecl, ok := d.(*ast.GenDecl); ok {
			if genDecl.Tok != token.IMPORT {
				continue
			}
			for _, spec := range genDecl.Specs {
				if importSpec, ok := spec.(*ast.ImportSpec); ok {
					importSpecs = append(importSpecs, importSpec)
				}
			}
		}
	}

	var preIsStd bool
	for i, im := range importSpecs {
		stdImport := !strings.Contains(im.Path.Value, ".")
		if stdImport {
			// std import
			if i == 0 {
				preIsStd = true
				continue
			}
			if !preIsStd {
				return errors.New(fmt.Sprintf("stdlib %s need be group together and before non-stdlib group in %s", im.Path.Value, path))
			}
			continue
		}
		// non-std import
		if i != 0 {
			if !preIsStd {
				continue
			}
			if !checkSepWithNewline(src, importSpecs[i-1].Path.Pos(), im.Path.Pos()) {
				return errors.New(fmt.Sprintf("non-stdlib %s need be group together and after stdlib group in %s", im.Path.Value, path))
			}
			preIsStd = false
		}
	}

	return nil
}

func checkSepWithNewline(src []byte, pre token.Pos, cur token.Pos) bool {
	preSrc := src[pre:cur]
	newLine := strings.Count(string(replog.String(preSrc)), "\n")
	return newLine == 2
}
