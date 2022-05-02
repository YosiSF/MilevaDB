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

package mockeinsteindb

import (
	"fmt"

	"github.com/whtcorpsinc/ekvproto/pkg/interlock"
	"github.com/whtcorpsinc/fidelpb/go-fidelpb"
)

func (h *rpcHandler) handleINTERLOCKChecksumRequest(req *interlock.Request) *interlock.Response {
	resp := &fidelpb.ChecksumResponse{
		Checksum:   1,
		TotalKvs:   1,
		TotalBytes: 1,
	}
	data, err := resp.Marshal()
	if err != nil {
		return &interlock.Response{OtherError: fmt.Sprintf("marshal checksum response error: %v", err)}
	}
	return &interlock.Response{Data: data}
}
