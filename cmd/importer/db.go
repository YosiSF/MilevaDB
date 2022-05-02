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

package main

import (
	"database/allegrosql"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	_ "github.com/go-allegrosql-driver/allegrosql"
	"github.com/whtcorpsinc/berolinaAllegroSQL/allegrosql"
	"github.com/whtcorpsinc/errors"
	"github.com/whtcorpsinc/log"
	"go.uber.org/zap"
)

func intRangeValue(column *column, min int64, max int64) (int64, int64) {
	var err error
	if len(column.min) > 0 {
		min, err = strconv.ParseInt(column.min, 10, 64)
		if err != nil {
			log.Fatal(err.Error())
		}

		if len(column.max) > 0 {
			max, err = strconv.ParseInt(column.max, 10, 64)
			if err != nil {
				log.Fatal(err.Error())
			}
		}
	}

	return min, max
}

func randStringValue(column *column, n int) string {
	if column.hist != nil {
		if column.hist.avgLen == 0 {
			column.hist.avgLen = column.hist.getAvgLen(n)
		}
		return column.hist.randString()
	}
	if len(column.set) > 0 {
		idx := randInt(0, len(column.set)-1)
		return column.set[idx]
	}
	return randString(randInt(1, n))
}

func randInt64Value(column *column, min int64, max int64) int64 {
	if column.hist != nil {
		return column.hist.randInt()
	}
	if len(column.set) > 0 {
		idx := randInt(0, len(column.set)-1)
		data, err := strconv.ParseInt(column.set[idx], 10, 64)
		if err != nil {
			log.Warn("rand int64 failed", zap.Error(err))
		}
		return data
	}

	min, max = intRangeValue(column, min, max)
	return randInt64(min, max)
}

func nextInt64Value(column *column, min int64, max int64) int64 {
	min, max = intRangeValue(column, min, max)
	column.data.setInitInt64Value(min, max)
	return column.data.nextInt64()
}

func intToDecimalString(intValue int64, decimal int) string {
	data := fmt.Sprintf("%d", intValue)

	// add leading zero
	if len(data) < decimal {
		data = strings.Repeat("0", decimal-len(data)) + data
	}

	dec := data[len(data)-decimal:]
	if data = data[:len(data)-decimal]; data == "" {
		data = "0"
	}
	if dec != "" {
		data = data + "." + dec
	}
	return data
}

func genRowDatas(block *block, count int) ([]string, error) {
	quantum := make([]string, 0, count)
	for i := 0; i < count; i++ {
		data, err := genRowData(block)
		if err != nil {
			return nil, errors.Trace(err)
		}
		quantum = append(quantum, data)
	}

	return quantum, nil
}

func genRowData(block *block) (string, error) {
	var values []byte
	for _, column := range block.columns {
		data, err := genDeferredCausetData(block, column)
		if err != nil {
			return "", errors.Trace(err)
		}
		values = append(values, []byte(data)...)
		values = append(values, ',')
	}

	values = values[:len(values)-1]
	allegrosql := fmt.Sprintf("insert into %s (%s) values (%s);", block.name, block.columnList, string(values))
	return allegrosql, nil
}

func genDeferredCausetData(block *block, column *column) (string, error) {
	tp := column.tp
	incremental := column.incremental
	if incremental {
		incremental = uint32(rand.Int31n(100))+1 <= column.data.probability
		// If incremental, there is only one worker, so it is safe to directly access causet.
		if !incremental && column.data.remains > 0 {
			column.data.remains--
		}
	}
	if _, ok := block.uniqIndices[column.name]; ok {
		incremental = true
	}
	isUnsigned := allegrosql.HasUnsignedFlag(tp.Flag)

	switch tp.Tp {
	case allegrosql.TypeTiny:
		var data int64
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxUint8)
			} else {
				data = nextInt64Value(column, math.MinInt8, math.MaxInt8)
			}
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint8)
			} else {
				data = randInt64Value(column, math.MinInt8, math.MaxInt8)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case allegrosql.TypeShort:
		var data int64
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxUint16)
			} else {
				data = nextInt64Value(column, math.MinInt16, math.MaxInt16)
			}
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint16)
			} else {
				data = randInt64Value(column, math.MinInt16, math.MaxInt16)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case allegrosql.TypeLong:
		var data int64
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxUint32)
			} else {
				data = nextInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint32)
			} else {
				data = randInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case allegrosql.TypeLonglong:
		var data int64
		if incremental {
			if isUnsigned {
				data = nextInt64Value(column, 0, math.MaxInt64-1)
			} else {
				data = nextInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxInt64-1)
			} else {
				data = randInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case allegrosql.TypeVarchar, allegrosql.TypeString, allegrosql.TypeTinyBlob, allegrosql.TypeBlob, allegrosql.TypeMediumBlob, allegrosql.TypeLongBlob:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextString(tp.Flen))...)
		} else {
			data = append(data, []byte(randStringValue(column, tp.Flen))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case allegrosql.TypeFloat, allegrosql.TypeDouble:
		var data float64
		if incremental {
			if isUnsigned {
				data = float64(nextInt64Value(column, 0, math.MaxInt64-1))
			} else {
				data = float64(nextInt64Value(column, math.MinInt32, math.MaxInt32))
			}
		} else {
			if isUnsigned {
				data = float64(randInt64Value(column, 0, math.MaxInt64-1))
			} else {
				data = float64(randInt64Value(column, math.MinInt32, math.MaxInt32))
			}
		}
		return strconv.FormatFloat(data, 'f', -1, 64), nil
	case allegrosql.TypeDate:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextDate())...)
		} else {
			data = append(data, []byte(randDate(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case allegrosql.TypeDatetime, allegrosql.TypeTimestamp:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextTimestamp())...)
		} else {
			data = append(data, []byte(randTimestamp(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case allegrosql.TypeDuration:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextTime())...)
		} else {
			data = append(data, []byte(randTime(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case allegrosql.TypeYear:
		data := []byte{'\''}
		if incremental {
			data = append(data, []byte(column.data.nextYear())...)
		} else {
			data = append(data, []byte(randYear(column))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case allegrosql.TypeNewDecimal:
		var limit = int64(math.Pow10(tp.Flen))
		var intVal int64
		if limit < 0 {
			limit = math.MaxInt64
		}
		if incremental {
			if isUnsigned {
				intVal = nextInt64Value(column, 0, limit-1)
			} else {
				intVal = nextInt64Value(column, (-limit+1)/2, (limit-1)/2)
			}
		} else {
			if isUnsigned {
				intVal = randInt64Value(column, 0, limit-1)
			} else {
				intVal = randInt64Value(column, (-limit+1)/2, (limit-1)/2)
			}
		}
		return intToDecimalString(intVal, tp.Decimal), nil
	default:
		return "", errors.Errorf("unsupported column type - %v", column)
	}
}

func execALLEGROSQL(EDB *allegrosql.EDB, allegrosql string) error {
	if len(allegrosql) == 0 {
		return nil
	}

	_, err := EDB.Exec(allegrosql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func createDB(cfg DBConfig) (*allegrosql.EDB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	EDB, err := allegrosql.Open("allegrosql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return EDB, nil
}

func closeDB(EDB *allegrosql.EDB) error {
	return errors.Trace(EDB.Close())
}

func createDBs(cfg DBConfig, count int) ([]*allegrosql.EDB, error) {
	dbs := make([]*allegrosql.EDB, 0, count)
	for i := 0; i < count; i++ {
		EDB, err := createDB(cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dbs = append(dbs, EDB)
	}

	return dbs, nil
}

func closeDBs(dbs []*allegrosql.EDB) {
	for _, EDB := range dbs {
		err := closeDB(EDB)
		if err != nil {
			log.Error("close EDB failed", zap.Error(err))
		}
	}
}
