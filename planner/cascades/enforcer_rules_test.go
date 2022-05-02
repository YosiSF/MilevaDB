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

package cascades

import (
	"github.com/whtcorpsinc/MilevaDB-Prod/expression"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/memo"
	"github.com/whtcorpsinc/MilevaDB-Prod/planner/property"
	. "github.com/whtcorpsinc/check"
)

func (s *testCascadesSuite) TestGetEnforcerMemrules(c *C) {
	prop := &property.PhysicalProperty{}
	group := memo.NewGroupWithSchema(nil, expression.NewSchema())
	enforcers := GetEnforcerMemrules(group, prop)
	c.Assert(enforcers, IsNil)
	col := &expression.DeferredCauset{}
	prop.Items = append(prop.Items, property.Item{DefCaus: col})
	enforcers = GetEnforcerMemrules(group, prop)
	c.Assert(enforcers, NotNil)
	c.Assert(len(enforcers), Equals, 1)
	_, ok := enforcers[0].(*OrderEnforcer)
	c.Assert(ok, IsTrue)
}

func (s *testCascadesSuite) TestNewProperties(c *C) {
	prop := &property.PhysicalProperty{}
	col := &expression.DeferredCauset{}
	group := memo.NewGroupWithSchema(nil, expression.NewSchema())
	prop.Items = append(prop.Items, property.Item{DefCaus: col})
	enforcers := GetEnforcerMemrules(group, prop)
	orderEnforcer, _ := enforcers[0].(*OrderEnforcer)
	newProp := orderEnforcer.NewProperty(prop)
	c.Assert(newProp.Items, IsNil)
}
