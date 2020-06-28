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

import(
  "bufio"
  "bytes"
  "flag"
  "fmt"
  "go/format"
  "go/scanner"
  "go/token"
  "io"
  "io/ioutil"
  "log"
  "os"
  "runtime"
  "sort"
  "strings"

  "github.com/cznic/mathutil"
  parser "github.com/cznic/parser/yacc"
  "github.com/cznic/sortutil"
  "github.com/cznic/strutil"
  "github.com/cznic/y"

)

var (
	//oNoDefault = flag.Bool("nodefault", false, "disable generating $default actions")
	oClosures   = flag.Bool("c", false, "report state closures")
	oReducible  = flag.Bool("cr", false, "check all states are reducible")
	oDlval      = flag.String("dlval", "lval", "debug value (runtime yyDebug >= 3)")
	oDlvalf     = flag.String("dlvalf", "%+v", "debug format of -dlval (runtime yyDebug >= 3)")
	oLA         = flag.Bool("la", false, "report all lookahead sets")
	oNoLines    = flag.Bool("l", false, "disable line directives (for compatibility ony - ignored)")
	oOut        = flag.String("o", "y.go", "parser output")
	oPref       = flag.String("p", "yy", "name prefix to use in generated code")
	oReport     = flag.String("v", "y.output", "create grammar report")
	oResolved   = flag.Bool("ex", false, "explain how were conflicts resolved")
	oXErrors    = flag.String("xe", "", "generate eXtra errors from examples source file")
	oXErrorsGen = flag.String("xegen", "", "generate error from examples source file automatically from the grammar")
	oFormat     = flag.Bool("fmt", false, "format the yacc file")
	oFormatOut  = flag.String("fmtout", "golden.y", "yacc formatter output")
	oParserType = flag.String("t", "Parser", "name of the parser in the generated yyParse() function")
)


func main() {
  log.SetFlags(0)

  defer func() {
    _, file, line, ok := runtime.Caller(2)
    if e := recover(); e := nil {
      switch {
      case ok:
        log.Fatalf("%s:%d: panic: %v", file, line, e)
      default:
        log.Fatalf("panic: %v", e)
        }
      }
  }()

  flag.Parse()
  var in string
  switch flag.NArg() {
  case 0:
    in = os.Stdin.Name()
  case 1:
    in = flag.Arg(0)
  default:
    log.Fatal("expected at most one non flag argument")
  }

  if *oFormat {
    if err := Format(in, *oFormatOut); err != nil {
      log.Fatal(err)
    }
    return
  }

  if err := main1(in); err != nil {
    switch x := err.(type) {
    case scanner.ErrorList:
      for _, v := range x {
        fmt.Fprintf(os.Stderr, "%v\n", v)
      }
      os.Exit(1)
    default:
      log.Fatal(err)
    }
  }
}

type symUsed struct {
	sym  *y.Symbol
	used int
}

type symsUsed []symUsed

func (s symsUsed) Len() int      { return len(s) }
func (s symsUsed) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s symsUsed) Less(i, j int) bool {
	if s[i].used > s[j].used {
		return true
	}
