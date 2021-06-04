// slices.go -- convert slices to/from various types
//
// (c) Sudhi Herle 2018
//
// License GPLv2
//
// If you need a commercial license for this work, please contact
// the author.
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

package mph

import (
	"reflect"
	"unsafe"
)

// byte-slice to uint16 slice
func bsToUint16Slice(b []byte) []uint16 {
	n := len(b) / 2
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []uint16

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n
	sh.Cap = n

	return v
}

// uint32 slice to byte-slice
func u16sToByteSlice(b []uint16) []byte {
	n := len(b)
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []byte

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n * 2
	sh.Cap = n * 2

	return v
}

// byte-slice to uint32 slice
func bsToUint32Slice(b []byte) []uint32 {
	n := len(b) / 4
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []uint32

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n
	sh.Cap = n

	return v
}

// uint32 slice to byte-slice
func u32sToByteSlice(b []uint32) []byte {
	n := len(b)
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []byte

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n * 4
	sh.Cap = n * 4

	return v
}

// byte-slice to uint64 slice
func bsToUint64Slice(b []byte) []uint64 {
	n := len(b) / 8
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []uint64

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n
	sh.Cap = n

	return v
}

// uint64 slice to byte-slice
func u64sToByteSlice(b []uint64) []byte {
	n := len(b)
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var v []byte

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n * 8
	sh.Cap = n * 8

	return v
}
