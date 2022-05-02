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

package petri

import (
	"sync/atomic"
	"time"

	"github.com/whtcorpsinc/MilevaDB-Prod/causetstore/einsteindb"
	"github.com/whtcorpsinc/MilevaDB-Prod/metrics"
)

// SchemaChecker is used for checking schemaReplicant-validity.
type SchemaChecker struct {
	SchemaValidator
	schemaVer       int64
	relatedBlockIDs []int64
}

type intSchemaVer int64

func (i intSchemaVer) SchemaMetaVersion() int64 {
	return int64(i)
}

var (
	// SchemaOutOfDateRetryInterval is the backoff time before retrying.
	SchemaOutOfDateRetryInterval = int64(500 * time.Millisecond)
	// SchemaOutOfDateRetryTimes is the max retry count when the schemaReplicant is out of date.
	SchemaOutOfDateRetryTimes = int32(10)
)

// NewSchemaChecker creates a new schemaReplicant checker.
func NewSchemaChecker(do *Petri, schemaVer int64, relatedBlockIDs []int64) *SchemaChecker {
	return &SchemaChecker{
		SchemaValidator: do.SchemaValidator,
		schemaVer:       schemaVer,
		relatedBlockIDs: relatedBlockIDs,
	}
}

// Check checks the validity of the schemaReplicant version.
func (s *SchemaChecker) Check(txnTS uint64) (*einsteindb.RelatedSchemaChange, error) {
	return s.CheckBySchemaVer(txnTS, intSchemaVer(s.schemaVer))
}

// CheckBySchemaVer checks if the schemaReplicant version valid or not at txnTS.
func (s *SchemaChecker) CheckBySchemaVer(txnTS uint64, startSchemaVer einsteindb.SchemaVer) (*einsteindb.RelatedSchemaChange, error) {
	schemaOutOfDateRetryInterval := atomic.LoadInt64(&SchemaOutOfDateRetryInterval)
	schemaOutOfDateRetryTimes := int(atomic.LoadInt32(&SchemaOutOfDateRetryTimes))
	for i := 0; i < schemaOutOfDateRetryTimes; i++ {
		relatedChange, CheckResult := s.SchemaValidator.Check(txnTS, startSchemaVer.SchemaMetaVersion(), s.relatedBlockIDs)
		switch CheckResult {
		case ResultSucc:
			return nil, nil
		case ResultFail:
			metrics.SchemaLeaseErrorCounter.WithLabelValues("changed").Inc()
			return relatedChange, ErrSchemaReplicantChanged
		case ResultUnknown:
			time.Sleep(time.Duration(schemaOutOfDateRetryInterval))
		}

	}
	metrics.SchemaLeaseErrorCounter.WithLabelValues("outdated").Inc()
	return nil, ErrSchemaReplicantExpired
}
