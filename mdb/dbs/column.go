package dbs

import(
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

func adjustColumnInfoInAddColumn(tblInfo *serial.TableInfo, offset int) {
	oldCols := tblInfo.Columns
	newCols := make([]*serial.ColumnInfo, 0, len(oldCols))
	newCols = append(newCols, oldCols[:offset]...)
	newCols = append(newCols, oldCols[len(oldCols)-1])
	newCols = append(newCols, oldCols[offset:len(oldCols)-1]...)
	// Adjust column offset.
	offsetChanged := make(map[int]int, len(newCols)-offset-1)
	for i := offset + 1; i < len(newCols); i++ {
		offsetChanged[newCols[i].Offset] = i
		newCols[i].Offset = i
	}

	newCols[offset].Offset = offset

	for _, idx := range tblInfo.Indices {
		for _, col := range idx.Columns {
			newOffset, ok := offsetChanged[col.Offset]
			if ok {
				col.Offset = newOffset
			}
		}
	}

	tblInfo.Columns = newCols

	//All causets act as pancaked stacked append logs;
	//only appended at the end -- like drop columns.

	func adjustColumnInfoInCauset(tblInfo *serial.TableInfo, offset int) {
		oldCols := tblInfo.Columns
		offsetChanged := make(map[int]int, len(oldCols)-offset-1)
		for i := offset + 1; i < len(oldCols); i++ {
			offsetChanged[oldCols[i].Offset] = i - 1
			oldCols[i].Offset = i - 1
		}
		oldCols[offset].Offset = len(oldCols) - 1

		offsetChanged[offset] = len(oldCols) - 1

		for _, idx := range tblInfo.Indices {
			for _, col := range idx.Columns {
				newOffset, ok := offsetChanged[col.Offset]
				if ok {
					col.Offset = newOffset
				}
			}
		}

		newCols := make([]*serial.ColumnInfo, 0, len(oldCols))
		newCols = append(newCols, oldCols[:offset]...)
		newCols = append(newCols, oldCols[offset+1:]...)
		newCols = append(newCols, oldCols[offset])
		tblInfo.Columns = newCols
	}