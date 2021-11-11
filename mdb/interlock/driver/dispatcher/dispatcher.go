package interlock

import (
    MilevaDB/mdb/interlock/types"
    	"fmt"
    	"time"
)

type DispatchPolicyInterface interface {
    //DispatchDuration provides us with a monotonic time till
    DispatchDuration() time.Duration

}

//DriverDispatcher issues latency policy for the soliton cluster managed by FIDel