package MilevaDB

import (
	_ "encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"math"
	_ "strconv"
	"strings"
	"sync"
	"sync/atomic"
)
// These are byte flags used for `HashCode()`.
const (
	constantFlag       byte = 0
	columnFlag         byte = 1
	scalarFunctionFlag byte = 3
)

type Type

// Expression represents all types of expressions.
type Expression interface {
	// Eval evaluates an expression through the row.
	Eval(row []interface{}) (interface{}, error)

	// IsStatic checks if an expression is static.
	IsStatic() bool

	// Clone copies an expression totally.
	Clone() Expression

	// HashCode implements the hashcode interface and generates a hashcode for an expression.
	HashCode() []byte

	// Equal checks if two expressions are equal.
	Equal(e Expression, ctx map[interface{}]interface{}) bool

	// String implements the fmt.Stringer interface.
	String() string

	// MarshalJSON implements the json.Marshaler interface.
	MarshalJSON() ([]byte, error)

	// MarshalJSON implements the json.Unmarshaler interface.
	UnmarshalJSON([]byte) error

	// GetType gets the expression return type.
	GetType() Type
