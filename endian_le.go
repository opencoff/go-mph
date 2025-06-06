// endian_le.go -- endian conversion routines for little-endian arch.
// This file is for little endian systems; thus conversion _to_ little-endian
// format is idempotent.
// We build this file into all arch's that are LE. We list them in the build
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

//go:build 386 || amd64 || arm || arm64 || ppc64le || mipsle || mips64le
// +build 386 amd64 arm arm64 ppc64le mipsle mips64le

package mph

func toLEUint64(v uint64) uint64 {
	return v
}

func toLEUint32(v uint32) uint32 {
	return v
}

func toLEUint16(v uint16) uint16 {
	return v
}

// From LE -> BE: swap bytes all the way around
func toBEUint64(v uint64) uint64 {
	return ((v & 0x00000000000000ff) << 56) |
		((v & 0x000000000000ff00) << 40) |
		((v & 0x0000000000ff0000) << 24) |
		((v & 0x00000000ff000000) << 8) |
		((v & 0x000000ff00000000) >> 8) |
		((v & 0x0000ff0000000000) >> 24) |
		((v & 0x00ff000000000000) >> 40) |
		((v & 0xff00000000000000) >> 56)
}

// From LE -> BE: swap bytes all the way around
func toBEUint32(v uint32) uint32 {
	return ((v & 0x000000ff) << 24) |
		((v & 0x0000ff00) << 8) |
		((v & 0x00ff0000) >> 8) |
		((v & 0xff000000) >> 24)
}

// From LE -> BE: swap bytes all the way around
func toBEUint16(v uint16) uint16 {
	return ((v & 0x00ff) << 8) |
		((v & 0xff00) >> 8)
}
