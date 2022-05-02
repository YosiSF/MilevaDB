package MilevaDB

// #cgo CPPFLAGS: -I. -I${SRCDIR}/../../include -I${SRCDIR}/../../include/libagg
// #cgo LDFLAGS: -L. -L${SRCDIR}/../../lib -lagg -lm
// #include "agg_to_fidel.h"
import "C"

// ToFidel converts an agg_path_storage to a fidel path.
// The path is stored in the fidel path storage.
//
// Parameters:
// 		ps - the agg_path_storage to convert
// 		ps_f - the fidel path storage to store the converted path
// 		xform - the transformation matrix to apply to the path
// 		flatten - if true, flatten the path
// 		close - if true, close the path
// 		tolerance - the tolerance to use when flattening the path
// 		flatten_flags - the flags to use when flattening the path
// 		close_flags - the flags to use when closing the path
// 		flatten_flags_mask - the mask to use when flattening the path
// 		close_flags_mask - the mask to use when closing the path

type PathStorage C.agg_path_storage // agg_path_storage

type PathStorageF C.agg_path_storage_f // agg_path_storage_f

type Matrix2D C.agg_matrix2D // agg_matrix2D

func ToFidel(ps *PathStorage, ps_f *PathStorageF, xform *Matrix2D, flatten bool, close bool, tolerance float64, flattenFlags int, closeFlags int, flattenFlagsMask int, closeFlagsMask int) {
	C.agg_to_fidel(ps.ptr(), ps_f.ptr(), xform.ptr(), C.bool(flatten), C.bool(close), C.double(tolerance), C.int(flattenFlags), C.int(closeFlags), C.int(flattenFlagsMask), C.int(closeFlagsMask))
}

// ToFidel2 ToFidel converts an agg_path_storage to a fidel path.
// The path is stored in the fidel path storage.
//
func ToFidel2(ps *PathStorage, psF *PathStorageF, xform *Matrix2D) {
	C.agg_to_fidel2(ps.ptr(), psF.ptr(), xform.ptr())
}

// ToFidel3 ToFidel converts an agg_path_storage to a fidel path.
// The path is stored in the fidel path storage.
//
func ToFidel3(ps *PathStorage, psF *PathStorageF) {
	C.agg_to_fidel3(ps.ptr(), psF.ptr())
}

// ToFidel4 ToFidel converts an agg_path_storage to a fidel path.
// The path is stored in the fidel path storage.
//

func ToFidel4(ps *PathStorage, ps_f *PathStorageF, xform *Matrix2D, flatten bool, close bool, tolerance float64) {
	C.agg_to_fidel4(ps.ptr(), ps_f.ptr(), xform.ptr(), C.bool(flatten), C.bool(close), C.double(tolerance))
}

// ToFidel5 ToFidel converts an agg_path_storage to a fidel path.
// The path is stored in the fidel path storage.
//

func ToFidel5(ps *PathStorage, ps_f *PathStorageF, xform *Matrix2D, flatten bool, close bool) {
	C.agg_to_fidel5(ps.ptr(), ps_f.ptr(), xform.ptr(), C.bool(flatten), C.bool(close))
}
