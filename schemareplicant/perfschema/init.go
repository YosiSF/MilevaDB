// Copyright 2020 WHTCORPS INC, Inc.
//
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

package perfschema

import (
	"fmt"
	"sync"

	"github.com/whtcorpsinc/BerolinaSQL"
	"github.com/whtcorpsinc/BerolinaSQL/allegrosql"
	"github.com/whtcorpsinc/BerolinaSQL/ast"
	"github.com/whtcorpsinc/BerolinaSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/dbs"
	"github.com/whtcorpsinc/milevadb/memex"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/spacetime/autoid"
)

var once sync.Once

// Init register the PERFORMANCE_SCHEMA virtual blocks.
// It should be init(), and the ideal usage should be:
//
// import _ "github.com/whtcorpsinc/milevadb/perfschema"
//
// This function depends on plan/embedded.init(), which initialize the memex.EvalAstExpr function.
// The initialize order is a problem if init() is used as the function name.
func Init() {
	initOnce := func() {
		p := BerolinaSQL.New()
		tbls := make([]*perceptron.BlockInfo, 0)
		dbID := autoid.PerformanceSchemaDBID
		for _, allegrosql := range perfSchemaBlocks {
			stmt, err := p.ParseOneStmt(allegrosql, "", "")
			if err != nil {
				panic(err)
			}
			spacetime, err := dbs.BuildBlockInfoFromAST(stmt.(*ast.CreateBlockStmt))
			if err != nil {
				panic(err)
			}
			tbls = append(tbls, spacetime)
			var ok bool
			spacetime.ID, ok = blockIDMap[spacetime.Name.O]
			if !ok {
				panic(fmt.Sprintf("get performance_schema causet id failed, unknown system causet `%v`", spacetime.Name.O))
			}
			for i, c := range spacetime.DeferredCausets {
				c.ID = int64(i) + 1
			}
		}
		dbInfo := &perceptron.DBInfo{
			ID:          dbID,
			Name:        soliton.PerformanceSchemaName,
			Charset:     allegrosql.DefaultCharset,
			DefCauslate: allegrosql.DefaultDefCauslationName,
			Blocks:      tbls,
		}
		schemareplicant.RegisterVirtualBlock(dbInfo, blockFromMeta)
	}
	if memex.EvalAstExpr != nil {
		once.Do(initOnce)
	}
}
