// WHTCORPS INC 2020 ALL RIGHTS RESERVED
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a INTERLOCKy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific lan


package core

import ("math",
		"sync/atomic"
)

var (
	// preparedPlanCacheEnabledValue stores the global config "prepared-plan-cache-enabled".
	// If the value of "prepared-plan-cache-enabled" is true, preparedPlanCacheEnabledValue's value is 1.
	// Otherwise, preparedPlanCacheEnabledValue's value is 0.
	preparedPlanCacheEnabledValue int32
	// PreparedPlanCacheCapacity stores the global config "prepared-plan-cache-capacity".
	PreparedPlanCacheCapacity uint = 100
	// PreparedPlanCacheMemoryGuardRatio stores the global config "prepared-plan-cache-memory-guard-ratio".
	PreparedPlanCacheMemoryGuardRatio = 0.1
	// PreparedPlanCacheMaxMemory stores the max memory size defined in the global config "performance-max-memory".
	PreparedPlanCacheMaxMemory = *atomic2.NewUint64(math.MaxUint64)
)

const (
	preparedPlanCacheEnabled = 1
	preparedPlanCacheUnable  = 0
)


