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

	"github.com/gogo/protobuf/proto"
	"github.com/whtcorpsinc/berolinaAllegroSQL/auth"
	"github.com/whtcorpsinc/ekvproto/pkg/einsteindbpb"
	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
	"github.com/whtcorpsinc/milevadb/ekv"
	"github.com/whtcorpsinc/milevadb/planner/core"
	"github.com/whtcorpsinc/milevadb/privilege"
	"github.com/whtcorpsinc/milevadb/schemareplicant"
	"github.com/whtcorpsinc/milevadb/soliton/chunk"
	"github.com/whtcorpsinc/milevadb/soliton/codec"
	"github.com/whtcorpsinc/milevadb/soliton/timeutil"
	"github.com/whtcorpsinc/milevadb/stochastikctx"
	"github.com/whtcorpsinc/milevadb/types"
)

// interlocking_directoratePosetDagHandler uses to handle INTERLOCK posetPosetDag request.
type interlocking_directoratePosetDagHandler struct {
	sctx             stochastikctx.Context
	posetPosetDagReq *fidelpb.PosetDagRequest
}

// Newinterlocking_directoratePosetDagHandler creates a new interlocking_directoratePosetDagHandler.
func Newinterlocking_directoratePosetDagHandler(sctx stochastikctx.Context) *interlocking_directoratePosetDagHandler {
	return &interlocking_directoratePosetDagHandler{
		sctx: sctx,
	}
}

// HandleRequest handles the interlock request.
func (h *interlocking_directoratePosetDagHandler) HandleRequest(ctx context.Context, req *interlock.Request) *interlock.Response {
	e, err := h.buildPosetDagExecutor(req)
	if err != nil {
		return h.buildErrorResponse(err)
	}

	err = e.Open(ctx)
	if err != nil {
		return h.buildErrorResponse(err)
	}

	chk := newFirstChunk(e)
	tps := e.base().retFieldTypes
	var totalChunks, partChunks []fidelpb.Chunk
	for {
		chk.Reset()
		err = Next(ctx, e, chk)
		if err != nil {
			return h.buildErrorResponse(err)
		}
		if chk.NumEvents() == 0 {
			break
		}
		partChunks, err = h.buildChunk(chk, tps)
		if err != nil {
			return h.buildErrorResponse(err)
		}
		totalChunks = append(totalChunks, partChunks...)
	}
	if err := e.Close(); err != nil {
		return h.buildErrorResponse(err)
	}
	return h.buildUnaryResponse(totalChunks)
}

// HandleStreamRequest handles the interlock stream request.
func (h *interlocking_directoratePosetDagHandler) HandleStreamRequest(ctx context.Context, req *interlock.Request, stream einsteindbpb.EinsteinDB_interlocking_directorateStreamServer) error {
	e, err := h.buildPosetDagExecutor(req)
	if err != nil {
		return stream.Send(h.buildErrorResponse(err))
	}

	err = e.Open(ctx)
	if err != nil {
		return stream.Send(h.buildErrorResponse(err))
	}

	chk := newFirstChunk(e)
	tps := e.base().retFieldTypes
	for {
		chk.Reset()
		if err = Next(ctx, e, chk); err != nil {
			return stream.Send(h.buildErrorResponse(err))
		}
		if chk.NumEvents() == 0 {
			return h.buildResponseAndSendToStream(chk, tps, stream)
		}
		if err = h.buildResponseAndSendToStream(chk, tps, stream); err != nil {
			return stream.Send(h.buildErrorResponse(err))
		}
	}
}

func (h *interlocking_directoratePosetDagHandler) buildResponseAndSendToStream(chk *chunk.Chunk, tps []*types.FieldType, stream einsteindbpb.EinsteinDB_interlocking_directorateStreamServer) error {
	chunks, err := h.buildChunk(chk, tps)
	if err != nil {
		return stream.Send(h.buildErrorResponse(err))
	}

	for _, c := range chunks {
		resp := h.buildStreamResponse(&c)
		if err = stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

func (h *interlocking_directoratePosetDagHandler) buildPosetDagExecutor(req *interlock.Request) (Executor, error) {
	if req.GetTp() != ekv.ReqTypePosetDag {
		return nil, errors.Errorf("unsupported request type %d", req.GetTp())
	}
	posetPosetDagReq := new(fidelpb.PosetDagRequest)
	err := proto.Unmarshal(req.Data, posetPosetDagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if posetPosetDagReq.User != nil {
		pm := privilege.GetPrivilegeManager(h.sctx)
		if pm != nil {
			h.sctx.GetStochastikVars().User = &auth.UserIdentity{
				Username: posetPosetDagReq.User.UserName,
				Hostname: posetPosetDagReq.User.UserHost,
			}
			authName, authHost, success := pm.GetAuthWithoutVerification(posetPosetDagReq.User.UserName, posetPosetDagReq.User.UserHost)
			if success {
				h.sctx.GetStochastikVars().User.AuthUsername = authName
				h.sctx.GetStochastikVars().User.AuthHostname = authHost
				h.sctx.GetStochastikVars().ActiveRoles = pm.GetDefaultRoles(authName, authHost)
			}
		}
	}

	stmtCtx := h.sctx.GetStochastikVars().StmtCtx
	stmtCtx.SetFlagsFromPBFlag(posetPosetDagReq.Flags)
	stmtCtx.TimeZone, err = timeutil.ConstructTimeZone(posetPosetDagReq.TimeZoneName, int(posetPosetDagReq.TimeZoneOffset))
	h.sctx.GetStochastikVars().TimeZone = stmtCtx.TimeZone
	if err != nil {
		return nil, errors.Trace(err)
	}
	h.posetPosetDagReq = posetPosetDagReq
	is := h.sctx.GetStochastikVars().TxnCtx.SchemaReplicant.(schemareplicant.SchemaReplicant)
	// Build physical plan.
	bp := core.NewPBPlanBuilder(h.sctx, is)
	plan, err := bp.Build(posetPosetDagReq.Executors)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Build executor.
	b := newExecutorBuilder(h.sctx, is)
	return b.build(plan), nil
}

func (h *interlocking_directoratePosetDagHandler) buildChunk(chk *chunk.Chunk, tps []*types.FieldType) (chunks []fidelpb.Chunk, err error) {
	switch h.posetPosetDagReq.EncodeType {
	case fidelpb.EncodeType_TypeDefault:
		chunks, err = h.encodeDefault(chk, tps)
	case fidelpb.EncodeType_TypeChunk:
		chunks, err = h.encodeChunk(chk, tps)
	default:
		return nil, errors.Errorf("unknown PosetDag encode type: %v", h.posetPosetDagReq.EncodeType)
	}
	return chunks, err
}

func (h *interlocking_directoratePosetDagHandler) buildUnaryResponse(chunks []fidelpb.Chunk) *interlock.Response {
	selResp := fidelpb.SelectResponse{
		Chunks:     chunks,
		EncodeType: h.posetPosetDagReq.EncodeType,
	}
	if h.posetPosetDagReq.DefCauslectExecutionSummaries != nil && *h.posetPosetDagReq.DefCauslectExecutionSummaries {
		execSummary := make([]*fidelpb.ExecutorExecutionSummary, len(h.posetPosetDagReq.Executors))
		for i := range execSummary {
			// TODO: Add real executor execution summary information.
			execSummary[i] = &fidelpb.ExecutorExecutionSummary{}
		}
		selResp.ExecutionSummaries = execSummary
	}
	data, err := proto.Marshal(&selResp)
	if err != nil {
		return h.buildErrorResponse(err)
	}
	return &interlock.Response{
		Data: data,
	}
}

func (h *interlocking_directoratePosetDagHandler) buildStreamResponse(chunk *fidelpb.Chunk) *interlock.Response {
	data, err := chunk.Marshal()
	if err != nil {
		return h.buildErrorResponse(err)
	}
	streamResponse := fidelpb.StreamResponse{
		Data: data,
	}
	var resp = &interlock.Response{}
	resp.Data, err = proto.Marshal(&streamResponse)
	if err != nil {
		resp.OtherError = err.Error()
	}
	return resp
}

func (h *interlocking_directoratePosetDagHandler) buildErrorResponse(err error) *interlock.Response {
	return &interlock.Response{
		OtherError: err.Error(),
	}
}

func (h *interlocking_directoratePosetDagHandler) encodeChunk(chk *chunk.Chunk, defCausTypes []*types.FieldType) ([]fidelpb.Chunk, error) {
	defCausOrdinal := h.posetPosetDagReq.OutputOffsets
	respDefCausTypes := make([]*types.FieldType, 0, len(defCausOrdinal))
	for _, ordinal := range defCausOrdinal {
		respDefCausTypes = append(respDefCausTypes, defCausTypes[ordinal])
	}
	encoder := chunk.NewCodec(respDefCausTypes)
	cur := fidelpb.Chunk{}
	cur.EventsData = append(cur.EventsData, encoder.Encode(chk)...)
	return []fidelpb.Chunk{cur}, nil
}

func (h *interlocking_directoratePosetDagHandler) encodeDefault(chk *chunk.Chunk, tps []*types.FieldType) ([]fidelpb.Chunk, error) {
	defCausOrdinal := h.posetPosetDagReq.OutputOffsets
	stmtCtx := h.sctx.GetStochastikVars().StmtCtx
	requestedEvent := make([]byte, 0)
	chunks := []fidelpb.Chunk{}
	for i := 0; i < chk.NumEvents(); i++ {
		requestedEvent = requestedEvent[:0]
		event := chk.GetEvent(i)
		for _, ordinal := range defCausOrdinal {
			data, err := codec.EncodeValue(stmtCtx, nil, event.GetCauset(int(ordinal), tps[ordinal]))
			if err != nil {
				return nil, err
			}
			requestedEvent = append(requestedEvent, data...)
		}
		chunks = h.appendEvent(chunks, requestedEvent, i)
	}
	return chunks, nil
}

const rowsPerChunk = 64

func (h *interlocking_directoratePosetDagHandler) appendEvent(chunks []fidelpb.Chunk, data []byte, rowCnt int) []fidelpb.Chunk {
	if rowCnt%rowsPerChunk == 0 {
		chunks = append(chunks, fidelpb.Chunk{})
	}
	cur := &chunks[len(chunks)-1]
	cur.EventsData = append(cur.EventsData, data...)
	return chunks
}
