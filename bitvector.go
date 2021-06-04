// bitvector.go -- simple bitvector implementation
//
// (c) Sudhi Herle 2018
//
// License GPLv2
// If you need a commercial license for this work, please contact
// the author.
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

package mph

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"sync"
)

// bitVector represents a bit vector in an efficient manner
type bitVector struct {
	sync.Mutex
	v []uint64

	// XXX Other fields to pre-compute rank
}

// newbitVector creates a bitvector to hold atleast 'size * g' bits.
// The value 'g' is an expansion factor (typically > 1.0). The resulting size
// is rounded-up to the next multiple of 64.
func newBitVector(sz uint64) *bitVector {
	sz += 63
	sz &= ^(uint64(63))
	words := sz / 64
	bv := &bitVector{
		v: make([]uint64, words),
	}

	return bv
}

// Size returns the number of bits in this bitvector
func (b *bitVector) Size() uint64 {
	return uint64(len(b.v)) * 64
}

// Words returns the number of words in the array
func (b *bitVector) Words() uint64 {
	return uint64(len(b.v))
}

// Set sets the bit 'i' in the bitvector
func (b *bitVector) Set(i uint64) {
	v := uint64(1) << (i % 64)

	b.Lock()
	b.v[i/64] |= v
	b.Unlock()
}

// IsSet() returns true if the bit 'i' is set, false otherwise
func (b *bitVector) IsSet(i uint64) bool {
	b.Lock()
	w := b.v[i/64]
	b.Unlock()
	return 1 == (1 & (w >> (i % 64)))
}

// Reset() clears all the bits in the bitvector
func (b *bitVector) Reset() {
	v := b.v
	b.Lock()
	for i := range v {
		v[i] = 0
	}
	b.Unlock()
}

// Merge merges contents of 'o' into 'b'
// Both bitvectors must be the same size
func (b *bitVector) Merge(o *bitVector) *bitVector {
	v := b.v
	b.Lock()
	for i, z := range o.v {
		v[i] |= z
	}
	b.Unlock()
	return b
}

// ComputeRanks memoizes rank calculation for future rank queries
// One must not modify the bitvector after calling this function.
// Returns the population count of the bitvector.
func (b *bitVector) ComputeRank() uint64 {
	var p uint64

	b.Lock()
	for i := range b.v {
		p += popcount(b.v[i])
	}
	b.Unlock()
	return p
}

// Rank calculates the rank on bit 'i'
// (Rank is the number of bits set before it).
func (b *bitVector) Rank(i uint64) uint64 {
	x := i / 64
	y := i % 64

	var r uint64
	var k uint64

	b.Lock()
	for k = 0; k < x; k++ {
		r += popcount(b.v[k])
	}
	v := b.v[x]
	b.Unlock()

	r += popcount(v << (64 - y))
	return r
}

// Marshal writes the bitvector in a portable format to writer 'w'.
func (b *bitVector) MarshalBinary(w io.Writer) (int, error) {
	var x [8]byte

	b.Lock()
	defer b.Unlock()

	bs := u64sToByteSlice(b.v)
	binary.LittleEndian.PutUint64(x[:], b.Words())

	n, err := writeAll(w, x[:])
	if err != nil {
		return 0, err
	}
	m, err := writeAll(w, bs)
	return n + m, err
}

// unmarshalbitVector reads a previously encoded bitvector and reconstructs
// the in-memory version.
func unmarshalBitVector(buf []byte) (*bitVector, uint64, error) {
	bvlen := binary.LittleEndian.Uint64(buf[:8])
	if bvlen == 0 || bvlen > (1<<32) {
		return nil, 0, fmt.Errorf("bitvect length %d is invalid", bvlen)
	}

	bv := bsToUint64Slice(buf[8:])
	b := &bitVector{
		v: bv[:bvlen],
	}
	return b, 8 + (bvlen * 8), nil
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

// population count - from Hacker's Delight
func popcount_slow(x uint64) uint64 {
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return x >> 56
}
