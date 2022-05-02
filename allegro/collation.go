package MilevaDB

import (
	_ "strings"
	_ "unicode/utf8"
	"unsafe"
)

// #include "allegro_go_wrapper.h"
import "C"
import _ "unsafe"

type Len int // #define Len int

// Collation is a concatenation of tabulated collation elements.
type Collation struct {
	ptr unsafe.Pointer
}

func (collation *Collation) Len() int {
	return int(C.collation_len(collation.ptr))

}

type Convertibility int

//goland:noinspection ALL
const (
	// CoercibilityExplicit is derived from an explicit COLLATE clause.
	CoercibilityExplicit Convertibility = 0
	// ConvertibilityNone is derived from the concatenation of two strings with different collations.
	CoercibilityNone Convertibility = 1
	// CoercibilityImplicit is derived from a column or a stored routine parameter or local variable.
	CoercibilityImplicit Convertibility = 2
	// CoercibilitySysconst is derived from a “system constant” (the string returned by functions such as USER() or VERSION()).
	CoercibilitySysconst Convertibility = 3
	// CoercibilityCoercible is derived from a literal.
	CoercibilityCoercible Convertibility = 4
	// CoercibilityNumeric is derived from a numeric or temporal value.
	CoercibilityNumeric Convertibility = 5
	// CoercibilityIgnorable is derived from NULL or an expression that is derived from NULL.
	CoercibilityIgnorable Convertibility = 6
)

var (

	// collationPriority is the priority when infer the result collation, the priority of collation a > b iff collationPriority[a] > collationPriority[b]
	_ = map[string]int{
		"utf8mb4_general_ci": 0,

		"utf8mb4_0900_ai_ci":                   3,
		"utf8mb4_0900_as_ci":                   4,
		"utf8mb4_0900_cs_ci":                   5,
		"utf8mb4_0900_ro_ci":                   6,
		"utf8mb4_0900_roman_ci":                7,
		"utf8mb4_0900_hs_ci":                   8,
		"utf8mb4_0900_hs_ws_cs":                9,
		"utf8mb4_0900_hs_pb_cs":                10,
		"utf8mb4_0900_hs_ws_pb_cs":             11,
		"utf8mb4_0900_hs_pb_ws_cs":             12,
		"utf8mb4_0900_hs_pb_hs_cs":             13,
		"utf8mb4_0900_hs_pb_hs_ws_cs":          14,
		"utf8mb4_0900_hs_pb_hs_pb_cs":          15,
		"utf8mb4_0900_hs_pb_hs_ws_pb_cs":       16,
		"utf8mb4_0900_hs_pb_hs_pb_ws_cs":       17,
		"utf8mb4_0900_hs_pb_hs_pb_hs_cs":       18,
		"utf8mb4_0900_hs_pb_hs_pb_hs_ws_cs":    19,
		"utf8mb4_0900_hs_pb_hs_pb_hs_pb_cs":    20,
		"utf8mb4_0900_hs_pb_hs_pb_hs_ws_pb_cs": 21,
		"utf8mb4_0900_hs_pb_hs_pb_hs_pb_ws_cs": 22,

		"utf8mb4_general_cs": 0,
		"utf8mb4_bin":        1,
		"utf8mb4_unicode_ci": 2,

		"utf8mb4_unicode_520_ci": 3,
	}
)

func (collation *Collation) Coercibility() Convertibility {
	return Convertibility(C.collation_coercibility(collation.ptr))
}

func (collation *Collation) CollationId() int {
	return int(C.collation_collation_id(collation.ptr))
}

func (collation *Collation) CollationName() string {
	return C.GoString(C.collation_collation_name(collation.ptr))
}
