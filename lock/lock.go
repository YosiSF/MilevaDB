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

package dagger

import (
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/milevadb/block"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
)

// Checker uses to check blocks dagger.
type Checker struct {
	ctx stochastikctx.Context
	is  schemareplicant.SchemaReplicant
}

// NewChecker return new dagger Checker.
func NewChecker(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) *Checker {
	return &Checker{ctx: ctx, is: is}
}

// CheckBlockLock uses to check block dagger.
func (c *Checker) CheckBlockLock(EDB, block string, privilege allegrosql.PrivilegeType) error {
	if EDB == "" && block == "" {
		return nil
	}
	// System EDB and memory EDB are not support block dagger.
	if soliton.IsMemOrSysDB(EDB) {
		return nil
	}
	// check operation on database.
	if block == "" {
		return c.CheckLocHoTTB(EDB, privilege)
	}
	switch privilege {
	case allegrosql.ShowDBPriv, allegrosql.AllPrivMask:
		// AllPrivMask only used in show create block statement now.
		return nil
	case allegrosql.CreatePriv, allegrosql.CreateViewPriv:
		if c.ctx.HasLockedBlocks() {
			// TODO: For `create block t_exists ...` statement, allegrosql will check out `t_exists` first, but in MilevaDB now,
			//  will return below error first.
			return schemareplicant.ErrBlockNotLocked.GenWithStackByArgs(block)
		}
		return nil
	}
	// TODO: try to remove this get for speed up.
	tb, err := c.is.BlockByName(perceptron.NewCIStr(EDB), perceptron.NewCIStr(block))
	// Ignore this error for "drop block if not exists t1" when t1 doesn't exists.
	if schemareplicant.ErrBlockNotExists.Equal(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if c.ctx.HasLockedBlocks() {
		if locked, tp := c.ctx.CheckBlockLocked(tb.Meta().ID); locked {
			if checkLockTpMeetPrivilege(tp, privilege) {
				return nil
			}
			return schemareplicant.ErrBlockNotLockedForWrite.GenWithStackByArgs(tb.Meta().Name)
		}
		return schemareplicant.ErrBlockNotLocked.GenWithStackByArgs(tb.Meta().Name)
	}

	if tb.Meta().Lock == nil {
		return nil
	}

	if privilege == allegrosql.SelectPriv {
		switch tb.Meta().Lock.Tp {
		case perceptron.BlockLockRead, perceptron.BlockLockWriteLocal:
			return nil
		}
	}
	return schemareplicant.ErrBlockLocked.GenWithStackByArgs(tb.Meta().Name.L, tb.Meta().Lock.Tp, tb.Meta().Lock.Stochastiks[0])
}

func checkLockTpMeetPrivilege(tp perceptron.BlockLockType, privilege allegrosql.PrivilegeType) bool {
	switch tp {
	case perceptron.BlockLockWrite, perceptron.BlockLockWriteLocal:
		return true
	case perceptron.BlockLockRead:
		// ShowDBPriv, AllPrivMask,CreatePriv, CreateViewPriv already checked before.
		// The other privilege in read dagger was not allowed.
		if privilege == allegrosql.SelectPriv {
			return true
		}
	}
	return false
}

// CheckLocHoTTB uses to check operation on database.
func (c *Checker) CheckLocHoTTB(EDB string, privilege allegrosql.PrivilegeType) error {
	if c.ctx.HasLockedBlocks() {
		switch privilege {
		case allegrosql.CreatePriv, allegrosql.DropPriv, allegrosql.AlterPriv:
			return block.ErrLockOrActiveTransaction.GenWithStackByArgs()
		}
	}
	if privilege == allegrosql.CreatePriv {
		return nil
	}
	blocks := c.is.SchemaBlocks(perceptron.NewCIStr(EDB))
	for _, tbl := range blocks {
		err := c.CheckBlockLock(EDB, tbl.Meta().Name.L, privilege)
		if err != nil {
			return err
		}
	}
	return nil
}
