// bitvector_test.go -- test suite for bitvector
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
	"bytes"
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

func TestBV(t *testing.T) {
	assert := newAsserter(t)

	bv := newBitVector(100)
	assert(bv.Size() == 128, "size mismatch; exp 128, saw %d", bv.Size())

	var i uint64
	for i = 0; i < bv.Size(); i++ {
		if 1 == (i & 1) {
			bv.Set(i)
		}
	}

	for i = 0; i < bv.Size(); i++ {
		if 1 == (i & 1) {
			assert(bv.IsSet(i), "%d not set", i)
		} else {
			assert(!bv.IsSet(i), "%d is set", i)
		}
	}
}

// Test concurrent bitvector stuff
func TestBVConcurrentRandom(t *testing.T) {
	assert := newAsserter(t)
	ncpu := runtime.NumCPU() * 2

	br := newBitVector(1000)
	bw := newBitVector(1000)
	n := br.Size()

	for i := uint64(0); i < n; i++ {
		if 1 == (i & 1) {
			br.Set(i)
		}
	}

	verify := make([][]uint64, ncpu)
	var w sync.WaitGroup
	w.Add(ncpu)
	for i := 0; i < ncpu; i++ {
		go func(i int, a, b *bitVector) {
			defer w.Done()

			n := uint64(a.Size()) * 16
			idx := make([]uint64, 0, n)
			sz := a.Size()

			for j := uint64(0); j < n; j++ {
				r := rand.Uint64() % sz
				if a.IsSet(r) {
					b.Set(r)
					idx = append(idx, r)
				}
			}

			verify[i] = idx
		}(i, br, bw)
	}

	w.Wait()

	// Now every entry in verify is set.
	for _, v := range verify {
		for _, k := range v {
			assert(bw.IsSet(k), "%d is not set", k)
		}
	}
}

func TestBVConcurrent(t *testing.T) {
	assert := newAsserter(t)
	ncpu := runtime.NumCPU() * 1

	br := newBitVector(1000)
	bw := newBitVector(1000)
	n := br.Size()

	for i := uint64(0); i < n; i++ {
		if 1 == (i & 1) {
			br.Set(i)
		}
	}

	var w sync.WaitGroup
	w.Add(ncpu)
	for i := 0; i < ncpu; i++ {
		go func(i int, a, b *bitVector) {
			defer w.Done()

			n := uint64(a.Size())
			for j := uint64(0); j < n; j++ {
				if a.IsSet(j) {
					b.Set(j)
				}
			}
		}(i, br, bw)
	}

	w.Wait()

	// Now every entry in verify is set.
	for i := uint64(0); i < n; i++ {
		if br.IsSet(i) {
			assert(bw.IsSet(i), "%d is not set", i)
		}
	}
}

func TestBVMarshal(t *testing.T) {
	assert := newAsserter(t)

	var b bytes.Buffer

	bv := newBitVector(100)
	assert(bv.Size() == 128, "size mismatch; exp 128, saw %d", bv.Size())

	var i uint64
	for i = 0; i < bv.Size(); i++ {
		if 1 == (i & 1) {
			bv.Set(i)
		}
	}

	bv.MarshalBinary(&b)
	expsz := 8 * (1 + bv.Words())
	assert(uint64(b.Len()) == expsz, "marshal size incorrect; exp %d, saw %d", expsz, b.Len())

	bn, n, err := unmarshalBitVector(b.Bytes())
	assert(err == nil, "unmarshal failed: %s", err)
	assert(bn.Size() == bv.Size(), "unmarshal size error; exp %d, saw %d", bv.Size(), bn.Size())
	assert(n == uint64(b.Len()), "unmarshal: not enough bytes consumed; exp %d, saw %d", b.Len(), n)

	for i = 0; i < bv.Size(); i++ {
		if bv.IsSet(i) {
			assert(bn.IsSet(i), "unmarshal %d is unset", i)
		} else {
			assert(!bn.IsSet(i), "unmarshal %d is set", i)
		}
	}

}
