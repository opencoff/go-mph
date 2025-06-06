// endian_be.go -- endian conversion routines for big-endian archs.
//
// This file is for big-endian systems; thus conversion _to_ big-endian
// format is idempotent.
// We build this file into all arch's that are BE. We list them in the build
// constraints below
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

//go:build ppc64 || mips || mips64
// +build ppc64 mips mips64

package mph

func toLEUint64(v uint64) uint64 {
	return ((v & 0x00000000000000ff) << 56) |
		((v & 0x000000000000ff00) << 40) |
		((v & 0x0000000000ff0000) << 24) |
		((v & 0x00000000ff000000) << 8) |
		((v & 0x000000ff00000000) >> 8) |
		((v & 0x0000ff0000000000) >> 24) |
		((v & 0x00ff000000000000) >> 40) |
		((v & 0xff00000000000000) >> 56)
}

func toLEUint32(v uint32) uint32 {
	return ((v & 0x000000ff) << 24) |
		((v & 0x0000ff00) << 8) |
		((v & 0x00ff0000) >> 8) |
		((v & 0xff000000) >> 24)
}

func toLEUint16(v uint16) uint16 {
	return ((v & 0x00ff) << 8) |
		((v & 0xff00) >> 8)
}

func toBEUint64(v uint64) uint64 {
	return v
}

func toBEUint32(v uint32) uint32 {
	return v
}

// From LE -> BE: swap bytes all the way around
func toBEUint16(v uint16) uint16 {
	return v
}
