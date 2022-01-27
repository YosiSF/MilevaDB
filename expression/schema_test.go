// INTERLOCKyright 2020 WHTCORPS INC, Inc.
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

package expression

import (
	"fmt"

	. "github.com/whtcorpsinc/check"
)

// generateKeys4Schema will generate keys for a given schemaReplicant. Used only in this file.
func generateKeys4Schema(schemaReplicant *Schema) {
	keyCount := len(schemaReplicant.DeferredCausets) - 1
	keys := make([]KeyInfo, 0, keyCount)
	for i := 0; i < keyCount; i++ {
		keys = append(keys, []*DeferredCauset{schemaReplicant.DeferredCausets[i]})
	}
	schemaReplicant.Keys = keys
}

// generateSchema will generate a schemaReplicant for test. Used only in this file.
func (s *testEvalSuite) generateSchema(defCausCount int) *Schema {
	defcaus := make([]*DeferredCauset, 0, defCausCount)
	for i := 0; i < defCausCount; i++ {
		defcaus = append(defcaus, &DeferredCauset{
			UniqueID: s.allocDefCausID(),
		})
	}
	return NewSchema(defcaus...)
}

func (s *testEvalSuite) TestSchemaString(c *C) {
	schemaReplicant := s.generateSchema(5)
	c.Assert(schemaReplicant.String(), Equals, "DeferredCauset: [DeferredCauset#1,DeferredCauset#2,DeferredCauset#3,DeferredCauset#4,DeferredCauset#5] Unique key: []")
	generateKeys4Schema(schemaReplicant)
	c.Assert(schemaReplicant.String(), Equals, "DeferredCauset: [DeferredCauset#1,DeferredCauset#2,DeferredCauset#3,DeferredCauset#4,DeferredCauset#5] Unique key: [[DeferredCauset#1],[DeferredCauset#2],[DeferredCauset#3],[DeferredCauset#4]]")
}

func (s *testEvalSuite) TestSchemaRetrieveDeferredCauset(c *C) {
	schemaReplicant := s.generateSchema(5)
	defCausOutSchema := &DeferredCauset{
		UniqueID: 100,
	}
	for _, defCaus := range schemaReplicant.DeferredCausets {
		c.Assert(schemaReplicant.RetrieveDeferredCauset(defCaus), Equals, defCaus)
	}
	c.Assert(schemaReplicant.RetrieveDeferredCauset(defCausOutSchema), IsNil)
}

func (s *testEvalSuite) TestSchemaIsUniqueKey(c *C) {
	schemaReplicant := s.generateSchema(5)
	generateKeys4Schema(schemaReplicant)
	defCausOutSchema := &DeferredCauset{
		UniqueID: 100,
	}
	for i, defCaus := range schemaReplicant.DeferredCausets {
		if i < len(schemaReplicant.DeferredCausets)-1 {
			c.Assert(schemaReplicant.IsUniqueKey(defCaus), Equals, true)
		} else {
			c.Assert(schemaReplicant.IsUniqueKey(defCaus), Equals, false)
		}
	}
	c.Assert(schemaReplicant.IsUniqueKey(defCausOutSchema), Equals, false)
}

func (s *testEvalSuite) TestSchemaContains(c *C) {
	schemaReplicant := s.generateSchema(5)
	defCausOutSchema := &DeferredCauset{
		UniqueID: 100,
	}
	for _, defCaus := range schemaReplicant.DeferredCausets {
		c.Assert(schemaReplicant.Contains(defCaus), Equals, true)
	}
	c.Assert(schemaReplicant.Contains(defCausOutSchema), Equals, false)
}

func (s *testEvalSuite) TestSchemaDeferredCausetsIndices(c *C) {
	schemaReplicant := s.generateSchema(5)
	defCausOutSchema := &DeferredCauset{
		UniqueID: 100,
	}
	for i := 0; i < len(schemaReplicant.DeferredCausets)-1; i++ {
		defCausIndices := schemaReplicant.DeferredCausetsIndices([]*DeferredCauset{schemaReplicant.DeferredCausets[i], schemaReplicant.DeferredCausets[i+1]})
		for j, res := range defCausIndices {
			c.Assert(res, Equals, i+j)
		}
	}
	c.Assert(schemaReplicant.DeferredCausetsIndices([]*DeferredCauset{schemaReplicant.DeferredCausets[0], schemaReplicant.DeferredCausets[1], defCausOutSchema, schemaReplicant.DeferredCausets[2]}), IsNil)
}

func (s *testEvalSuite) TestSchemaDeferredCausetsByIndices(c *C) {
	schemaReplicant := s.generateSchema(5)
	indices := []int{0, 1, 2, 3}
	retDefCauss := schemaReplicant.DeferredCausetsByIndices(indices)
	for i, ret := range retDefCauss {
		c.Assert(fmt.Sprintf("%p", schemaReplicant.DeferredCausets[i]), Equals, fmt.Sprintf("%p", ret))
	}
}

func (s *testEvalSuite) TestSchemaMergeSchema(c *C) {
	lSchema := s.generateSchema(5)
	generateKeys4Schema(lSchema)

	rSchema := s.generateSchema(5)
	generateKeys4Schema(rSchema)

	c.Assert(MergeSchema(nil, nil), IsNil)
	c.Assert(MergeSchema(lSchema, nil).String(), Equals, lSchema.String())
	c.Assert(MergeSchema(nil, rSchema).String(), Equals, rSchema.String())

	schemaReplicant := MergeSchema(lSchema, rSchema)
	for i := 0; i < len(lSchema.DeferredCausets); i++ {
		c.Assert(schemaReplicant.DeferredCausets[i].UniqueID, Equals, lSchema.DeferredCausets[i].UniqueID)
	}
	for i := 0; i < len(rSchema.DeferredCausets); i++ {
		c.Assert(schemaReplicant.DeferredCausets[i+len(lSchema.DeferredCausets)].UniqueID, Equals, rSchema.DeferredCausets[i].UniqueID)
	}
}

func (s *testEvalSuite) TestGetUsedList(c *C) {
	schemaReplicant := s.generateSchema(5)
	var usedDefCauss []*DeferredCauset
	usedDefCauss = append(usedDefCauss, schemaReplicant.DeferredCausets[3])
	usedDefCauss = append(usedDefCauss, s.generateSchema(2).DeferredCausets...)
	usedDefCauss = append(usedDefCauss, schemaReplicant.DeferredCausets[1])
	usedDefCauss = append(usedDefCauss, schemaReplicant.DeferredCausets[3])

	used := GetUsedList(usedDefCauss, schemaReplicant)
	c.Assert(used, DeepEquals, []bool{false, true, false, true, false})
}
