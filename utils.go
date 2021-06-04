// utils.go -- utility functions
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
	"crypto/rand"
	"encoding/binary"
	"io"
)

// compression function for fasthash
func mix(h uint64) uint64 {
	h ^= h >> 23
	h *= 0x2127599bf4325c37
	h ^= h >> 47
	return h
}

func randbytes(n int) []byte {
	b := make([]byte, n)

	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		panic("can't read crypto/rand")
	}
	return b
}

func rand32() uint32 {
	var b [4]byte

	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic("can't read crypto/rand")
	}

	return binary.BigEndian.Uint32(b[:])
}

func rand64() uint64 {
	var b [8]byte

	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic("can't read crypto/rand")
	}

	return binary.BigEndian.Uint64(b[:])
}
