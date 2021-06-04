// bbhash_test.go -- test suite for bbhash
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
	"testing"

	"github.com/opencoff/go-fasthash"
)

func makeBBHash(t *testing.T, load float64, keys []uint64) MPH {
	assert := newAsserter(t)

	b, err := NewBBHashBuilder(load)
	assert(err == nil, "bbhash: construction failed: %s", err)

	for i, k := range keys {
		err = b.Add(k)
		assert(err == nil, "bbhash: can't add [%d] %s: %x", i, k, err)
	}

	mp, err := b.Freeze()
	assert(err == nil, "bbhash: can't freeze: %s", err)
	return mp
}

func TestBBHashSimple(t *testing.T) {
	assert := newAsserter(t)

	keys := make([]uint64, len(keyw))

	for i, s := range keyw {
		h := fasthash.Hash64(0xdeadbeefbaadf00d, []byte(s))
		keys[i] = h
	}

	b := makeBBHash(t, 2.0, keys)

	kmap := make(map[uint64]uint64)
	for i, k := range keys {
		j, ok := b.Find(k)
		assert(ok, "can't find key[%d] %x", i, k)
		assert(j < uint64(len(keys)), "key %d <%#x> mapping %d out-of-bounds", i, k, j)

		x, ok := kmap[j]
		assert(!ok, "index %d already mapped to key %#x", j, x)

		kmap[j] = k
	}
}

func TestBBMarshal(t *testing.T) {
	assert := newAsserter(t)

	keys := make([]uint64, len(keyw))

	for i, s := range keyw {
		keys[i] = fasthash.Hash64(0xdeadbeefbaadf00d, []byte(s))
	}

	mp := makeBBHash(t, 2.0, keys)
	b := mp.(*bbHash)

	var buf bytes.Buffer

	_, err := b.MarshalBinary(&buf)
	assert(err == nil, "marshal failed: %s", err)

	mp, err = newBBHash(buf.Bytes())
	assert(err == nil, "unmarshal failed: %s", err)

	b2 := mp.(*bbHash)

	assert(len(b.bits) == len(b2.bits), "rank-vector len mismatch (exp %d, saw %d)",
		len(b.bits), len(b2.bits))

	assert(len(b.ranks) == len(b2.ranks), "rank-helper len mismatch (exp %d, saw %d)",
		len(b.ranks), len(b2.ranks))

	assert(b.salt == b2.salt, "salt mismatch (exp %#x, saw %#x)", b.salt, b2.salt)

	for i := range b.bits {
		av := b.bits[i]
		bv := b2.bits[i]

		assert(av.Size() == bv.Size(), "level-%d, bitvector len mismatch (exp %d, saw %d)",
			i, av.Size(), bv.Size())

		var j uint64
		for j = 0; j < av.Words(); j++ {
			assert(av.v[j] == bv.v[j], "level-%d: bitvector content mismatch (exp %#x, saw %#x)",
				i, av.v[j], bv.v[j])
		}
	}

	for i := range b.ranks {
		ar := b.ranks[i]
		br := b2.ranks[i]

		assert(ar == br, "level-%d: rank mismatch (exp %d, saw %d)", i, ar, br)
	}

	for i, k := range keys {
		x, ok := b.Find(k)
		assert(ok, "can't find key[%d] %x in b", i, k)
		y, ok := b2.Find(k)
		assert(ok, "can't find key[%d] %x in b2", i, k)
		assert(x < uint64(len(keys)), "key %d <%#x> mapping %d out-of-bounds", i, k, x)
		assert(y < uint64(len(keys)), "b2: key %d <%#x> mapping %d out-of-bounds", i, k, y)
		assert(x == y, "b and b2 mapped key %d <%#x>: %d vs. %d", i, k, x, y)
	}

}
