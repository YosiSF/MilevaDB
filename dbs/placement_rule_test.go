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

package dbs

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/ast"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/MilevaDB-Prod/dbs/memristed"
)

var _ = Suite(&testPlacementSuite{})

type testPlacementSuite struct {
}

func (s *testPlacementSuite) compareMemruleOp(n, o *memristed.MemruleOp) bool {
	ok1, _ := DeepEquals.Check([]interface{}{n.CausetAction, o.CausetAction}, nil)
	ok2, _ := DeepEquals.Check([]interface{}{n.DeleteByIDPrefix, o.DeleteByIDPrefix}, nil)
	ok3, _ := DeepEquals.Check([]interface{}{n.Memrule, o.Memrule}, nil)
	return ok1 && ok2 && ok3
}

func (s *testPlacementSuite) TestPlacementBuild(c *C) {
	tests := []struct {
		input  []*ast.PlacementSpec
		output []*memristed.MemruleOp
		err    string
	}{
		{
			input:  []*ast.PlacementSpec{},
			output: []*memristed.MemruleOp{},
		},

		{
			input: []*ast.PlacementSpec{{
				Role:        ast.PlacementRoleVoter,
				Tp:          ast.PlacementAdd,
				Replicas:    3,
				Constraints: `["+  zone=sh", "-zone = bj"]`,
			}},
			output: []*memristed.MemruleOp{
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:  memristed.MemruleDefaultGroupID,
						Role:     memristed.Voter,
						Override: true,
						Count:    3,
						LabelConstraints: []memristed.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"sh"}},
							{Key: "zone", Op: "notIn", Values: []string{"bj"}},
						},
					},
				}},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role:        ast.PlacementRoleFollower,
					Tp:          ast.PlacementAdd,
					Replicas:    2,
					Constraints: `["-  zone=sh", "+zone = bj"]`,
				},
			},
			output: []*memristed.MemruleOp{
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:  memristed.MemruleDefaultGroupID,
						Role:     memristed.Voter,
						Override: true,
						Count:    3,
						LabelConstraints: []memristed.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"sh"}},
							{Key: "zone", Op: "notIn", Values: []string{"bj"}},
						},
					},
				},
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:  memristed.MemruleDefaultGroupID,
						Role:     memristed.Follower,
						Override: true,
						Count:    2,
						LabelConstraints: []memristed.LabelConstraint{
							{Key: "zone", Op: "notIn", Values: []string{"sh"}},
							{Key: "zone", Op: "in", Values: []string{"bj"}},
						},
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAlter,
					Replicas:    2,
					Constraints: `["-  zone=sh", "+zone = bj"]`,
				},
			},
			output: []*memristed.MemruleOp{
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						Role:    memristed.Voter,
					},
				},
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:  memristed.MemruleDefaultGroupID,
						Role:     memristed.Voter,
						Override: true,
						Count:    2,
						LabelConstraints: []memristed.LabelConstraint{
							{Key: "zone", Op: "notIn", Values: []string{"sh"}},
							{Key: "zone", Op: "in", Values: []string{"bj"}},
						},
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAlter,
					Replicas:    3,
					Constraints: `{"-  zone=sh":1, "+zone = bj":1}`,
				},
			},
			output: []*memristed.MemruleOp{
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						Role:    memristed.Voter,
					},
				},
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:          memristed.MemruleDefaultGroupID,
						Role:             memristed.Voter,
						Override:         true,
						Count:            1,
						LabelConstraints: []memristed.LabelConstraint{{Key: "zone", Op: "in", Values: []string{"bj"}}},
					},
				},
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:          memristed.MemruleDefaultGroupID,
						Role:             memristed.Voter,
						Override:         true,
						Count:            1,
						LabelConstraints: []memristed.LabelConstraint{{Key: "zone", Op: "notIn", Values: []string{"sh"}}},
					},
				},
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:  memristed.MemruleDefaultGroupID,
						Role:     memristed.Voter,
						Override: true,
						Count:    1,
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleVoter,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role: ast.PlacementRoleVoter,
					Tp:   ast.PlacementDrop,
				},
			},
			output: []*memristed.MemruleOp{
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						Role:    memristed.Voter,
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role: ast.PlacementRoleLearner,
					Tp:   ast.PlacementDrop,
				},
				{
					Role: ast.PlacementRoleVoter,
					Tp:   ast.PlacementDrop,
				},
			},
			output: []*memristed.MemruleOp{
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						Role:    memristed.Voter,
					},
				},
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						Role:    memristed.Learner,
					},
				},
			},
		},

		{
			input: []*ast.PlacementSpec{
				{
					Role:        ast.PlacementRoleLearner,
					Tp:          ast.PlacementAdd,
					Replicas:    3,
					Constraints: `["+  zone=sh", "-zone = bj"]`,
				},
				{
					Role: ast.PlacementRoleVoter,
					Tp:   ast.PlacementDrop,
				},
			},
			output: []*memristed.MemruleOp{
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						Role:    memristed.Voter,
					},
				},
				{
					CausetAction: memristed.MemruleOpAdd,
					Memrule: &memristed.Memrule{
						GroupID:  memristed.MemruleDefaultGroupID,
						Role:     memristed.Learner,
						Override: true,
						Count:    3,
						LabelConstraints: []memristed.LabelConstraint{
							{Key: "zone", Op: "in", Values: []string{"sh"}},
							{Key: "zone", Op: "notIn", Values: []string{"bj"}},
						},
					},
				},
			},
		},
	}
	for k, t := range tests {
		out, err := buildPlacementSpecs(t.input)
		if err == nil {
			for i := range t.output {
				found := false
				for j := range out {
					if s.compareMemruleOp(out[j], t.output[i]) {
						found = true
						break
					}
				}
				if !found {
					c.Logf("test %d, %d-th output", k, i)
					c.Logf("\texcept %+v\n\tbut got", t.output[i])
					for j := range out {
						c.Logf("\t%+v", out[j])
					}
					c.Fail()
				}
			}
		} else {
			c.Assert(err.Error(), ErrorMatches, t.err)
		}
	}
}

func (s *testPlacementSuite) TestPlacementBuildDrop(c *C) {
	tests := []struct {
		input  []int64
		output []*memristed.MemruleOp
	}{
		{
			input: []int64{2},
			output: []*memristed.MemruleOp{
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						ID:      "0_t0_p2",
					},
				},
			},
		},
		{
			input: []int64{1, 2},
			output: []*memristed.MemruleOp{
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						ID:      "0_t0_p1",
					},
				},
				{
					CausetAction:     memristed.MemruleOFIDelel,
					DeleteByIDPrefix: true,
					Memrule: &memristed.Memrule{
						GroupID: memristed.MemruleDefaultGroupID,
						ID:      "0_t0_p2",
					},
				},
			},
		},
	}
	for _, t := range tests {
		out := buildPlacementDropMemrules(0, 0, t.input)
		c.Assert(len(out), Equals, len(t.output))
		for i := range t.output {
			c.Assert(s.compareMemruleOp(out[i], t.output[i]), IsTrue, Commentf("except: %+v, obtained: %+v", t.output[i], out[i]))
		}
	}
}

func (s *testPlacementSuite) TestPlacementBuildTruncate(c *C) {
	rules := []*memristed.MemruleOp{
		{Memrule: &memristed.Memrule{ID: "0_t0_p1"}},
		{Memrule: &memristed.Memrule{ID: "0_t0_p94"}},
		{Memrule: &memristed.Memrule{ID: "0_t0_p48"}},
	}

	tests := []struct {
		input  []int64
		output []*memristed.MemruleOp
	}{
		{
			input: []int64{1},
			output: []*memristed.MemruleOp{
				{CausetAction: memristed.MemruleOFIDelel, Memrule: &memristed.Memrule{GroupID: memristed.MemruleDefaultGroupID, ID: "0_t0_p1"}},
				{CausetAction: memristed.MemruleOpAdd, Memrule: &memristed.Memrule{ID: "0_t0_p2__0_0"}},
			},
		},
		{
			input: []int64{94, 48},
			output: []*memristed.MemruleOp{
				{CausetAction: memristed.MemruleOFIDelel, Memrule: &memristed.Memrule{GroupID: memristed.MemruleDefaultGroupID, ID: "0_t0_p94"}},
				{CausetAction: memristed.MemruleOpAdd, Memrule: &memristed.Memrule{ID: "0_t0_p95__0_0"}},
				{CausetAction: memristed.MemruleOFIDelel, Memrule: &memristed.Memrule{GroupID: memristed.MemruleDefaultGroupID, ID: "0_t0_p48"}},
				{CausetAction: memristed.MemruleOpAdd, Memrule: &memristed.Memrule{ID: "0_t0_p49__0_1"}},
			},
		},
	}
	for _, t := range tests {
		INTERLOCKyMemrules := make([]*memristed.MemruleOp, len(rules))
		for _, rule := range rules {
			INTERLOCKyMemrules = append(INTERLOCKyMemrules, rule.Clone())
		}

		newPartitions := make([]perceptron.PartitionDefinition, 0, len(t.input))
		for _, j := range t.input {
			newPartitions = append(newPartitions, perceptron.PartitionDefinition{ID: j + 1})
		}

		out := buildPlacementTruncateMemrules(rules, 0, 0, 0, t.input, newPartitions)

		c.Assert(len(out), Equals, len(t.output))
		for i := range t.output {
			c.Assert(s.compareMemruleOp(out[i], t.output[i]), IsTrue, Commentf("except: %+v, obtained: %+v", t.output[i], out[i]))
		}
	}
}
