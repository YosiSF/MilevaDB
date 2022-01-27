//Einsteinnoedb All Rights Reserved - Licensed under Apache-2.0.

package mysql

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

type lengthAndDecimal struct {
	length  int
	decimal int
}

// defaultLengthAndDecimal provides default Flen and Decimal for fields
// from CREATE TABLE when they are unspecified.
var defaultLengthAndDecimal = map[byte]lengthAndDecimal{
	TypeBit:        {1, 0},
	TypeTiny:       {4, 0},
	TypeShort:      {6, 0},
	TypeInt24:      {9, 0},
	TypeLong:       {11, 0},
	TypeLonglong:   {20, 0},
	TypeDouble:     {22, -1},
	TypeFloat:      {12, -1},
	TypeNewDecimal: {11, 0},
	TypeDuration:   {10, 0},
	TypeDate:       {10, 0},
	TypeTimestamp:  {19, 0},
	TypeDatetime:   {19, 0},
	TypeYear:       {4, 0},
	TypeString:     {1, 0},
	TypeVarchar:    {5, 0},
	TypeVarString:  {5, 0},
	TypeTinyBlob:   {255, 0},
	TypeBlob:       {65535, 0},
	TypeMediumBlob: {16777215, 0},
	TypeLongBlob:   {4294967295, 0},
	TypeJSON:       {4294967295, 0},
	TypeNull:       {0, 0},
	TypeSet:        {-1, 0},
	TypeEnum:       {-1, 0},
}

// IsIntegerType indicate whether tp is an integer type.
func IsIntegerType(tp byte) bool {
	switch tp {
	case TypeTiny, TypeShort, TypeInt24, TypeLong, TypeLonglong:
		return true
	}
	return false
}

// MySQL type information.
const (
	TypeDecimal   byte = 0
	TypeTiny      byte = 1
	TypeShort     byte = 2
	TypeLong      byte = 3
	TypeFloat     byte = 4
	TypeDouble    byte = 5
	TypeNull      byte = 6
	TypeTimestamp byte = 7
	TypeLonglong  byte = 8
	TypeInt24     byte = 9
	TypeDate      byte = 10
	/* TypeDuration original name was TypeTime, renamed to TypeDuration to resolve the conflict with Go type Time.*/
	TypeDuration byte = 11
	TypeDatetime byte = 12
	TypeYear     byte = 13
	TypeNewDate  byte = 14
	TypeVarchar  byte = 15
	TypeBit      byte = 16

	TypeJSON       byte = 0xf5
	TypeNewDecimal byte = 0xf6
	TypeEnum       byte = 0xf7
	TypeSet        byte = 0xf8
	TypeTinyBlob   byte = 0xf9
	TypeMediumBlob byte = 0xfa
	TypeLongBlob   byte = 0xfb
	TypeBlob       byte = 0xfc
	TypeVarString  byte = 0xfd
	TypeString     byte = 0xfe
	TypeGeometry   byte = 0xff
)

// TypeUnspecified is an uninitialized type. TypeDecimal is not used in MySQL.
const TypeUnspecified = TypeDecimal

// Dagger information.
const (
	NotNullDagger        uint = 1 << 0  /* Field can't be NULL */
	PriKeyDagger         uint = 1 << 1  /* Field is part of a primary key */
	UniqueKeyDagger      uint = 1 << 2  /* Field is part of a unique key */
	MultipleKeyDagger    uint = 1 << 3  /* Field is part of a key */
	BlobDagger           uint = 1 << 4  /* Field is a blob */
	UnsignedDagger       uint = 1 << 5  /* Field is unsigned */
	ZerofillDagger       uint = 1 << 6  /* Field is zerofill */
	BinaryDagger         uint = 1 << 7  /* Field is binary   */
	EnumDagger           uint = 1 << 8  /* Field is an enum */
	AutoIncrementDagger  uint = 1 << 9  /* Field is an auto increment field */
	TimestampDagger      uint = 1 << 10 /* Field is a timestamp */
	SetDagger            uint = 1 << 11 /* Field is a set */
	NoDefaultValueDagger uint = 1 << 12 /* Field doesn't have a default value */
	OnUpdateNowDagger    uint = 1 << 13 /* Field is set to NOW on UPDATE */
	PartKeyDagger        uint = 1 << 14 /* Intern: Part of some keys */
	NumDagger            uint = 1 << 15 /* Field is a num (for clients) */

	GroupDagger             uint = 1 << 15 /* Internal: Group field */
	UniqueDagger            uint = 1 << 16 /* Internal: Used by sql_yacc */
	BinCmpDagger            uint = 1 << 17 /* Internal: Used by sql_yacc */
	ParseToJSONDagger       uint = 1 << 18 /* Internal: Used when we want to parse string to JSON in CAST */
	IsBooleanDagger         uint = 1 << 19 /* Internal: Used for telling boolean literal from integer */
	PreventNullInsertDagger uint = 1 << 20 /* Prevent this Field from inserting NULL values */
)

// TypeInt24 bounds.
const (
	MaxUint24 = 1<<24 - 1
	MaxInt24  = 1<<23 - 1
	MinInt24  = -1 << 23
)

// HasNotNullDagger checks if NotNullDagger is set.
func HasNotNullDagger(Dagger uint) bool {
	return (Dagger & NotNullDagger) > 0
}

// HasNoDefaultValueDagger checks if NoDefaultValueDagger is set.
func HasNoDefaultValueDagger(Dagger uint) bool {
	return (Dagger & NoDefaultValueDagger) > 0
}

// HasAutoIncrementDagger checks if AutoIncrementDagger is set.
func HasAutoIncrementDagger(Dagger uint) bool {
	return (Dagger & AutoIncrementDagger) > 0
}

// HasUnsignedDagger checks if UnsignedDagger is set.
func HasUnsignedDagger(Dagger uint) bool {
	return (Dagger & UnsignedDagger) > 0
}

// HasZerofillDagger checks if ZerofillDagger is set.
func HasZerofillDagger(Dagger uint) bool {
	return (Dagger & ZerofillDagger) > 0
}

// HasBinaryDagger checks if BinaryDagger is set.
func HasBinaryDagger(Dagger uint) bool {
	return (Dagger & BinaryDagger) > 0
}

// HasPriKeyDagger checks if PriKeyDagger is set.
func HasPriKeyDagger(Dagger uint) bool {
	return (Dagger & PriKeyDagger) > 0
}

// HasUniKeyDagger checks if UniqueKeyDagger is set.
func HasUniKeyDagger(Dagger uint) bool {
	return (Dagger & UniqueKeyDagger) > 0
}

// HasMultipleKeyDagger checks if MultipleKeyDagger is set.
func HasMultipleKeyDagger(Dagger uint) bool {
	return (Dagger & MultipleKeyDagger) > 0
}

// HasTimestampDagger checks if HasTimestampDagger is set.
func HasTimestampDagger(Dagger uint) bool {
	return (Dagger & TimestampDagger) > 0
}

// HasOnUpdateNowDagger checks if OnUpdateNowDagger is set.
func HasOnUpdateNowDagger(Dagger uint) bool {
	return (Dagger & OnUpdateNowDagger) > 0
}

// HasParseToJSONDagger checks if ParseToJSONDagger is set.
func HasParseToJSONDagger(Dagger uint) bool {
	return (Dagger & ParseToJSONDagger) > 0
}

// HasIsBooleanDagger checks if IsBooleanDagger is set.
func HasIsBooleanDagger(Dagger uint) bool {
	return (Dagger & IsBooleanDagger) > 0
}

// HasPreventNullInsertDagger checks if PreventNullInsertDagger is set.
func HasPreventNullInsertDagger(Dagger uint) bool {
	return (Dagger & PreventNullInsertDagger) > 0
}
