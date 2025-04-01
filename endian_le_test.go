// endian_le_test.go -- test suite for endian-convertors:
// Run this on Little-endian machines!
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

//go:build 386 || amd64 || arm || arm64 || ppc64le || mipsle || mips64le
// +build 386 amd64 arm arm64 ppc64le mipsle mips64le

package mph

import (
	"testing"
)

func TestEndianOnLE(t *testing.T) {
	assert := newAsserter(t) // this is in bitvector_test.go

	a0 := uint32(0xabcd1234)
	b0 := toLEUint32(a0)
	assert(a0 == b0, "uint32 %d != %d", a0, b0)

	a1 := uint64(0xabcd1234baadf00d)
	b1 := toLEUint64(a1)
	assert(a1 == b1, "uint64 %d != %d", a1, b1)

	a2 := uint16(0xabcd)
	b2 := toLEUint16(a2)
	assert(a2 == b2, "uint16 %d != %d", a2, b2)

	b0 = toBEUint32(a0)
	assert(b0 == 0x3412cdab, "uint32-be %d != %d", a0, b0)

	b1 = toBEUint64(a1)
	assert(b1 == 0x0df0adba3412cdab, "uint64-be %d != %d", a1, b1)

	b2 = toBEUint16(a2)
	assert(b2 == 0xcdab, "uint16 %d != %d", a2, b2)
}
