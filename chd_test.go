// chd_test.go -- test suite for chd
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
	"bytes"
	"testing"

	"github.com/opencoff/go-fasthash"
)

func TestCHDSimple(t *testing.T) {
	assert := newAsserter(t)

	c, err := NewChdBuilder(0.9)
	assert(err == nil, "construction failed: %s", err)
	kvmap := make(map[uint64]string) // map of hash to string
	kmap := make(map[uint64]uint64)  // map of index to hashval

	hseed := rand64()
	for _, s := range keyw {
		h := fasthash.Hash64(hseed, []byte(s))
		kvmap[h] = s
		c.Add(h)
	}

	lookup, err := c.Freeze()
	assert(err == nil, "freeze: %s", err)
	nkeys := uint64(lookup.Len())

	for h, s := range kvmap {
		j, ok := lookup.Find(h)
		assert(ok, "can't find key %x", h)
		assert(j < nkeys, "key %s <%#x> mapping %d out-of-bounds", s, h, j)

		x, ok := kmap[j]
		assert(!ok, "index %d already mapped to key %#x", j, x)

		//t.Logf("key %x -> %d\n", h, j)
		kmap[j] = h
	}
}

func TestCHDMarshal(t *testing.T) {
	assert := newAsserter(t)

	b, err := NewChdBuilder(0.9)
	assert(err == nil, "construction failed: %s", err)

	hseed := rand64()
	keys := make([]uint64, len(keyw))
	for i, s := range keyw {
		keys[i] = fasthash.Hash64(hseed, []byte(s))
		b.Add(keys[i])
	}

	c, err := b.Freeze()
	assert(err == nil, "freeze failed: %s", err)

	var buf bytes.Buffer

	_, err = c.MarshalBinary(&buf)
	assert(err == nil, "marshal failed: %s", err)

	mp, err := newChd(buf.Bytes())
	assert(err == nil, "unmarshal failed: %s", err)

	for i, k := range keys {
		x, ok := c.Find(k)
		assert(ok, "can't find key[%d] %x in c", i, k)
		y, ok := mp.Find(k)
		assert(ok, "can't find key[%d] %x in mp", i, k)
		assert(x == y, "b and b2 mapped key %d <%#x>: %d vs. %d", i, k, x, y)
	}
}
