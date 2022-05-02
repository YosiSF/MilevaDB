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
	"context"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	. "github.com/whtcorpsinc/check"
	"github.com/whtcorpsinc/log"
	"github.com/whtcorpsinc/MilevaDB-Prod/schemareplicant"
	"github.com/whtcorpsinc/MilevaDB-Prod/stochastikctx"
	"go.uber.org/zap"
)

type TestInterceptor struct {
	*BaseInterceptor

	OnGetSchemaReplicantExported func(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) schemareplicant.SchemaReplicant
}

func (ti *TestInterceptor) OnGetSchemaReplicant(ctx stochastikctx.Context, is schemareplicant.SchemaReplicant) schemareplicant.SchemaReplicant {
	if ti.OnGetSchemaReplicantExported != nil {
		return ti.OnGetSchemaReplicantExported(ctx, is)
	}

	return ti.BaseInterceptor.OnGetSchemaReplicant(ctx, is)
}

type TestDBSCallback struct {
	*BaseCallback

	onJobRunBefore          func(*perceptron.Job)
	OnJobRunBeforeExported  func(*perceptron.Job)
	onJobUFIDelated         func(*perceptron.Job)
	OnJobUFIDelatedExported func(*perceptron.Job)
	onWatched               func(ctx context.Context)
}

func (tc *TestDBSCallback) OnJobRunBefore(job *perceptron.Job) {
	log.Info("on job run before", zap.String("job", job.String()))
	if tc.OnJobRunBeforeExported != nil {
		tc.OnJobRunBeforeExported(job)
		return
	}
	if tc.onJobRunBefore != nil {
		tc.onJobRunBefore(job)
		return
	}

	tc.BaseCallback.OnJobRunBefore(job)
}

func (tc *TestDBSCallback) OnJobUFIDelated(job *perceptron.Job) {
	log.Info("on job uFIDelated", zap.String("job", job.String()))
	if tc.OnJobUFIDelatedExported != nil {
		tc.OnJobUFIDelatedExported(job)
		return
	}
	if tc.onJobUFIDelated != nil {
		tc.onJobUFIDelated(job)
		return
	}

	tc.BaseCallback.OnJobUFIDelated(job)
}

func (tc *TestDBSCallback) OnWatched(ctx context.Context) {
	if tc.onWatched != nil {
		tc.onWatched(ctx)
		return
	}

	tc.BaseCallback.OnWatched(ctx)
}

func (s *testDBSSuite) TestCallback(c *C) {
	cb := &BaseCallback{}
	c.Assert(cb.OnChanged(nil), IsNil)
	cb.OnJobRunBefore(nil)
	cb.OnJobUFIDelated(nil)
	cb.OnWatched(context.TODO())
}
