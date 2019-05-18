package block

import (
	"fmt"
	"strconv"
	"strings"
	"go.uber.org/zap"
	"github.com/YosiSF/MilevaDB/core/stochastik"
	"github.com/YosiSF/MilevaDB/core/util/binlog"
	"github.com/YosiSF/MilevaDB/BerolinaSQL/query"
)

//Table columns will be considered Blocks for convenience
type Block struct {
	*soliton.BlockInfo
}


