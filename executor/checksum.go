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

package executor

import (
	"context"
	"strconv"

	"github.com/whtcorpsinc/berolinaAllegroSQL/perceptron"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/distsql"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/logutil"
	"github.com/whtcorpsinc/milevadb/soliton/ranger"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/stochastikctx/variable"
	"go.uber.org/zap"
)

var _ Executor = &ChecksumBlockExec{}

// ChecksumBlockExec represents ChecksumBlock executor.
type ChecksumBlockExec struct {
	baseExecutor

	blocks map[int64]*checksumContext
	done   bool
}

// Open implements the Executor Open interface.
func (e *ChecksumBlockExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	concurrency, err := getChecksumBlockConcurrency(e.ctx)
	if err != nil {
		return err
	}

	tasks, err := e.buildTasks()
	if err != nil {
		return err
	}

	taskCh := make(chan *checksumTask, len(tasks))
	resultCh := make(chan *checksumResult, len(tasks))
	for i := 0; i < concurrency; i++ {
		go e.checksumWorker(taskCh, resultCh)
	}

	for _, task := range tasks {
		taskCh <- task
	}
	close(taskCh)

	for i := 0; i < len(tasks); i++ {
		result := <-resultCh
		if result.Error != nil {
			err = result.Error
			logutil.Logger(ctx).Error("checksum failed", zap.Error(err))
			continue
		}
		e.handleResult(result)
	}
	if err != nil {
		return err
	}

	return nil
}

// Next implements the Executor Next interface.
func (e *ChecksumBlockExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	for _, t := range e.blocks {
		req.AppendString(0, t.DBInfo.Name.O)
		req.AppendString(1, t.BlockInfo.Name.O)
		req.AppendUint64(2, t.Response.Checksum)
		req.AppendUint64(3, t.Response.TotalKvs)
		req.AppendUint64(4, t.Response.TotalBytes)
	}
	e.done = true
	return nil
}

func (e *ChecksumBlockExec) buildTasks() ([]*checksumTask, error) {
	var tasks []*checksumTask
	for id, t := range e.blocks {
		reqs, err := t.BuildRequests(e.ctx)
		if err != nil {
			return nil, err
		}
		for _, req := range reqs {
			tasks = append(tasks, &checksumTask{id, req})
		}
	}
	return tasks, nil
}

func (e *ChecksumBlockExec) handleResult(result *checksumResult) {
	block := e.blocks[result.BlockID]
	block.HandleResponse(result.Response)
}

func (e *ChecksumBlockExec) checksumWorker(taskCh <-chan *checksumTask, resultCh chan<- *checksumResult) {
	for task := range taskCh {
		result := &checksumResult{BlockID: task.BlockID}
		result.Response, result.Error = e.handleChecksumRequest(task.Request)
		resultCh <- result
	}
}

func (e *ChecksumBlockExec) handleChecksumRequest(req *ekv.Request) (resp *fidelpb.ChecksumResponse, err error) {
	ctx := context.TODO()
	res, err := distsql.Checksum(ctx, e.ctx.GetClient(), req, e.ctx.GetStochastikVars().KVVars)
	if err != nil {
		return nil, err
	}
	res.Fetch(ctx)
	defer func() {
		if err1 := res.Close(); err1 != nil {
			err = err1
		}
	}()

	resp = &fidelpb.ChecksumResponse{}

	for {
		data, err := res.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		checksum := &fidelpb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			return nil, err
		}
		uFIDelateChecksumResponse(resp, checksum)
	}

	return resp, nil
}

type checksumTask struct {
	BlockID int64
	Request *ekv.Request
}

type checksumResult struct {
	Error    error
	BlockID  int64
	Response *fidelpb.ChecksumResponse
}

type checksumContext struct {
	DBInfo    *perceptron.DBInfo
	BlockInfo *perceptron.BlockInfo
	StartTs   uint64
	Response  *fidelpb.ChecksumResponse
}

func newChecksumContext(EDB *perceptron.DBInfo, block *perceptron.BlockInfo, startTs uint64) *checksumContext {
	return &checksumContext{
		DBInfo:    EDB,
		BlockInfo: block,
		StartTs:   startTs,
		Response:  &fidelpb.ChecksumResponse{},
	}
}

func (c *checksumContext) BuildRequests(ctx stochastikctx.Context) ([]*ekv.Request, error) {
	var partDefs []perceptron.PartitionDefinition
	if part := c.BlockInfo.Partition; part != nil {
		partDefs = part.Definitions
	}

	reqs := make([]*ekv.Request, 0, (len(c.BlockInfo.Indices)+1)*(len(partDefs)+1))
	if err := c.appendRequest(ctx, c.BlockInfo.ID, &reqs); err != nil {
		return nil, err
	}

	for _, partDef := range partDefs {
		if err := c.appendRequest(ctx, partDef.ID, &reqs); err != nil {
			return nil, err
		}
	}

	return reqs, nil
}

func (c *checksumContext) appendRequest(ctx stochastikctx.Context, blockID int64, reqs *[]*ekv.Request) error {
	req, err := c.buildBlockRequest(ctx, blockID)
	if err != nil {
		return err
	}

	*reqs = append(*reqs, req)
	for _, indexInfo := range c.BlockInfo.Indices {
		if indexInfo.State != perceptron.StatePublic {
			continue
		}
		req, err = c.buildIndexRequest(ctx, blockID, indexInfo)
		if err != nil {
			return err
		}
		*reqs = append(*reqs, req)
	}

	return nil
}

func (c *checksumContext) buildBlockRequest(ctx stochastikctx.Context, blockID int64) (*ekv.Request, error) {
	checksum := &fidelpb.ChecksumRequest{
		ScanOn:    fidelpb.ChecksumScanOn_Block,
		Algorithm: fidelpb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullIntRange(false)

	var builder distsql.RequestBuilder
	return builder.SetBlockRanges(blockID, ranges, nil).
		SetChecksumRequest(checksum).
		SetStartTS(c.StartTs).
		SetConcurrency(ctx.GetStochastikVars().DistALLEGROSQLScanConcurrency()).
		Build()
}

func (c *checksumContext) buildIndexRequest(ctx stochastikctx.Context, blockID int64, indexInfo *perceptron.IndexInfo) (*ekv.Request, error) {
	checksum := &fidelpb.ChecksumRequest{
		ScanOn:    fidelpb.ChecksumScanOn_Index,
		Algorithm: fidelpb.ChecksumAlgorithm_Crc64_Xor,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	return builder.SetIndexRanges(ctx.GetStochastikVars().StmtCtx, blockID, indexInfo.ID, ranges).
		SetChecksumRequest(checksum).
		SetStartTS(c.StartTs).
		SetConcurrency(ctx.GetStochastikVars().DistALLEGROSQLScanConcurrency()).
		Build()
}

func (c *checksumContext) HandleResponse(uFIDelate *fidelpb.ChecksumResponse) {
	uFIDelateChecksumResponse(c.Response, uFIDelate)
}

func getChecksumBlockConcurrency(ctx stochastikctx.Context) (int, error) {
	stochastikVars := ctx.GetStochastikVars()
	concurrency, err := variable.GetStochastikSystemVar(stochastikVars, variable.MilevaDBChecksumBlockConcurrency)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

func uFIDelateChecksumResponse(resp, uFIDelate *fidelpb.ChecksumResponse) {
	resp.Checksum ^= uFIDelate.Checksum
	resp.TotalKvs += uFIDelate.TotalKvs
	resp.TotalBytes += uFIDelate.TotalBytes
}
