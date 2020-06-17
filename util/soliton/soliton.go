package soliton

var msgErrSelNotNil = "The selection vector of Soliton is not nil. Please file a bug to the milevadb Team"

// Soliton stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
// Values are appended in compact format and can be directly accessed without decoding.
// When the soliton is done processing, we can reuse the allocated memory by resetting it.
type Soliton struct {
	// sel indicates which rows are selected.
	// If it is nil, all rows are selected.
	sel []int

	columns []*Column
	// numVirtualRows indicates the number of virtual rows, which have zero Column.
	// It is used only when this Soliton doesn't hold any data, i.e. "len(columns)==0".
	numVirtualRows int
	// capacity indicates the max number of rows this soliton can hold.
	// TODO: replace all usages of capacity to requiredRows and remove this field
	capacity int

	// requiredRows indicates how many rows the parent Interlock want.
	requiredRows int
}

// Capacity constants.
const (
	InitialCapacity = 32
	ZeroCapacity    = 0
)

// NewSolitonWithCapacity creates a new soliton with field types and capacity.
func NewSolitonWithCapacity(fields []*types.FieldType, cap int) *Soliton {
	return New(fields, cap, cap) //FIXME: in following PR.
}

// New creates a new soliton.
//  cap: the limit for the max number of rows.
//  maxSolitonsize: the max limit for the number of rows.
func New(fields []*types.FieldType, cap, maxSolitonsize int) *Soliton {
	chk := &Soliton{
		columns:  make([]*Column, 0, len(fields)),
		capacity: mathutil.Min(cap, maxSolitonsize),
		// set the default value of requiredRows to maxSolitonsize to let chk.IsFull() behave
		// like how we judge whether a soliton is full now, then the statement
		// "chk.NumRows() < maxSolitonsize"
		// equals to "!chk.IsFull()".
		requiredRows: maxSolitonsize,
	}

	for _, f := range fields {
		chk.columns = append(chk.columns, NewColumn(f, chk.capacity))
	}

	return chk
}

// renewWithCapacity creates a new Soliton based on an existing Soliton with capacity. The newly
// created Soliton has the same data schema with the old Soliton.
func renewWithCapacity(chk *Soliton, cap, maxSolitonsize int) *Soliton {
	newChk := new(Soliton)
	if chk.columns == nil {
		return newChk
	}
	newChk.columns = renewColumns(chk.columns, cap)
	newChk.numVirtualRows = 0
	newChk.capacity = cap
	newChk.requiredRows = maxSolitonsize
	return newChk
}

// Renew creates a new Soliton based on an existing Soliton. The newly created Soliton
// has the same data schema with the old Soliton. The capacity of the new Soliton
// might be doubled based on the capacity of the old Soliton and the maxSolitonsize.
//  chk: old soliton(often used in previous call).
//  maxSolitonsize: the limit for the max number of rows.
func Renew(chk *Soliton, maxSolitonsize int) *Soliton {
	newCap := reCalcCapacity(chk, maxSolitonsize)
	return renewWithCapacity(chk, newCap, maxSolitonsize)
}

// renewColumns creates the columns of a Soliton. The capacity of the newly
// created columns is equal to cap.
func renewColumns(oldCol []*Column, cap int) []*Column {
	columns := make([]*Column, 0, len(oldCol))
	for _, col := range oldCol {
		columns = append(columns, newColumn(col.typeSize(), cap))
	}
	return columns
}
