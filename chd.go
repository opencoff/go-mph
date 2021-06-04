// chd.go - fast minimal perfect hashing for massive key sets
//
// This is an implementation of CHD in http://cmph.sourceforge.net/papers/esa09.pdf -
// inspired by this https://gist.github.com/pervognsen/b21f6dd13f4bcb4ff2123f0d78fcfd17
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
	"fmt"
	"io"
	"sort"
)

const (
	// number of times we will try to build the table
	_MaxSeed uint32 = 65536 * 2
)

// chdBuilder is used to create a MPHF from a given set of uint64 keys using
// the Compress Hash Displace algorithm: http://cmph.sourceforge.net/papers/esa09.pdf
type chdBuilder struct {
	keys []uint64
	salt uint64
	load float64
}

// NewChdBuilder enables creation of a minimal perfect hash function via the
// Compress Hash Displace algorithm. Once created, callers can
// add keys to it before Freezing the MPH and generating a constant time
// lookup table.
// Once the construction is frozen, callers can use "Find()" to find the
// unique mapping for each key in 'keys'.
func NewChdBuilder(load float64) (MPHBuilder, error) {
	if load < 0 || load > 1 {
		return nil, fmt.Errorf("chd: invalid load factor %f", load)
	}

	c := &chdBuilder{
		keys: make([]uint64, 0, 1024),
		salt: rand64(),
		load: load,
	}

	return c, nil
}

// Add a new key to the MPH builder
func (c *chdBuilder) Add(key uint64) error {
	c.keys = append(c.keys, key)
	return nil
}

type bucket struct {
	slot uint64
	keys []uint64
}
type buckets []bucket

func (b buckets) Len() int {
	return len(b)
}

func (b buckets) Less(i, j int) bool {
	return len(b[i].keys) > len(b[j].keys)
}

func (b buckets) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

// Freeze builds a constant-time lookup table using the CMD algorithm and
// the given load factor. Lower load factors speeds up the construction
// of the MPHF. Suggested value for load is between 0.75-0.9
func (c *chdBuilder) Freeze() (MPH, error) {
	m := uint64(float64(len(c.keys)) / c.load)
	m = nextpow2(m)
	buckets := make(buckets, m)
	seeds := make([]uint32, m)

	for i := range buckets {
		b := &buckets[i]
		b.slot = uint64(i)
	}

	for _, key := range c.keys {
		j := rhash(0, key, m, c.salt)
		b := &buckets[j]
		b.keys = append(b.keys, key)
	}

	occ := newBitVector(m)
	bOcc := newBitVector(m)

	// sort buckets in decreasing order of occupancy-size
	sort.Sort(buckets)

	tries := 0
	var maxseed uint32
	for i := range buckets {
		b := &buckets[i]
		for s := uint32(1); s < _MaxSeed; s++ {
			bOcc.Reset()
			for _, key := range b.keys {
				h := rhash(s, key, m, c.salt)
				if occ.IsSet(h) || bOcc.IsSet(h) {
					goto nextSeed // try next seed
				}
				bOcc.Set(h)
			}
			occ.Merge(bOcc)
			seeds[b.slot] = s
			if s > maxseed {
				maxseed = s
			}
			goto nextBucket

		nextSeed:
			tries++
		}

		return nil, fmt.Errorf("chd: No MPH after %d tries", _MaxSeed)
	nextBucket:
	}

	chd := &chd{
		seed:  makeSeeds(seeds, maxseed),
		salt:  c.salt,
		tries: tries,
	}

	return chd, nil
}

func makeSeeds(s []uint32, max uint32) seeder {
	switch {
	case max < 256:
		return newU8(s)

	case max < 65536:
		return newU16(s)

	default:
		return newU32(s)
	}
}

// chd represents a frozen PHF for the given set of keys
type chd struct {
	seed  seeder
	salt  uint64
	tries int
}

// Len returns the actual length of the PHF lookup table
func (c *chd) Len() int {
	return c.seed.length()
}

// Find returns a unique integer representing the minimal hash for key 'k'.
// The return value is meaningful ONLY for keys in the original key set (provided
// at the time of construction of the minimal-hash).
// Callers should verify that the key at the returned index == k.
func (c *chd) Find(k uint64) (uint64, bool) {
	m := uint64(c.seed.length())
	h := rhash(0, k, m, c.salt)
	return rhash(c.seed.seed(h), k, m, c.salt), true
}

func (c *chd) seedSize() byte {
	return c.seed.seedsize()
}

// CHD Marshalled header - 2 x 64-bit words
const _chdHeaderSize = 16

// To compress the seed table, we will use the interface below to abstract
// seed table of different sizes: 1, 2, 4
type seeder interface {
	// given a hash index, return the seed at the index
	seed(uint64) uint32

	// marshal to writer 'w'
	marshal(w io.Writer) (int, error)

	// unmarshal from mem-mapped byte slice 'b'
	unmarshal(b []byte) error

	// size of each seed in bytes (1, 2, 4)
	seedsize() byte

	// # of seeds
	length() int
}

// ensure each of these types implement the seeder interface above.
var (
	_ seeder = &u8Seeder{}
	_ seeder = &u16Seeder{}
	_ seeder = &u32Seeder{}
)

// 8 bit seed
type u8Seeder struct {
	seeds []uint8
}

func newU8(v []uint32) seeder {
	bs := make([]byte, len(v))
	for i, a := range v {
		bs[i] = byte(a & 0xff)
	}

	s := &u8Seeder{
		seeds: bs,
	}
	return s
}

func (u *u8Seeder) seed(v uint64) uint32 {
	return uint32(u.seeds[v])
}

func (u *u8Seeder) length() int {
	return len(u.seeds)
}

func (u *u8Seeder) seedsize() byte {
	return 1
}

func (u *u8Seeder) marshal(w io.Writer) (int, error) {
	return writeAll(w, u.seeds)
}

func (u *u8Seeder) unmarshal(b []byte) error {
	u.seeds = b
	return nil
}

// 16 bit seed
type u16Seeder struct {
	seeds []uint16
}

func newU16(v []uint32) seeder {
	us := make([]uint16, len(v))
	for i, a := range v {
		us[i] = uint16(a & 0xffff)
	}

	s := &u16Seeder{
		seeds: us,
	}
	return s
}

func (u *u16Seeder) seed(v uint64) uint32 {
	return uint32(u.seeds[v])
}

func (u *u16Seeder) length() int {
	return len(u.seeds)
}
func (u *u16Seeder) seedsize() byte {
	return 2
}

func (u *u16Seeder) marshal(w io.Writer) (int, error) {
	bs := u16sToByteSlice(u.seeds)
	return writeAll(w, bs)
}

func (u *u16Seeder) unmarshal(b []byte) error {
	u.seeds = bsToUint16Slice(b)
	return nil
}

// 32 bit seed
type u32Seeder struct {
	seeds []uint32
}

func newU32(v []uint32) seeder {
	s := &u32Seeder{
		seeds: v,
	}
	return s
}

func (u *u32Seeder) seed(v uint64) uint32 {
	return uint32(u.seeds[v])
}

func (u *u32Seeder) length() int {
	return len(u.seeds)
}

func (u *u32Seeder) seedsize() byte {
	return 4
}

func (u *u32Seeder) marshal(w io.Writer) (int, error) {
	bs := u32sToByteSlice(u.seeds)
	return writeAll(w, bs)
}

func (u *u32Seeder) unmarshal(b []byte) error {
	u.seeds = bsToUint32Slice(b)
	return nil
}

// Dump CHD meta-data to io.Writer 'w'
func (c *chd) DumpMeta(w io.Writer) {
	switch c.seed.(type) {
	case *u8Seeder:
		fmt.Fprintf(w, "  CHD with 8-bit seeds <salt %#x>\n", c.salt)
	case *u16Seeder:
		fmt.Fprintf(w, "  CHD with 16-bit seeds <salt %#x>\n", c.salt)
	case *u32Seeder:
		fmt.Fprintf(w, "  CHD with 32-bit seeds <salt %#x>\n", c.salt)

	default:
		panic("Unknown seed type!")
	}
}

// hash key with a given seed and return the result modulo 'sz'.
// 'sz' is guarantted to be a power of 2; so, modulo can be fast.
// borrowed from Zi Long Tan's superfast hash
func rhash(seed uint32, key, sz, salt uint64) uint64 {
	const m uint64 = 0x880355f21e6d1965
	var h uint64 = key

	h *= m
	h ^= mix(salt)
	h *= m
	h ^= mix(uint64(seed))
	h *= m

	// sz is a power of 2; if this is not true - replace this with modulus:
	// return mix(h) % sz
	return mix(h) & (sz - 1)
}

// return next power of 2
func nextpow2(n uint64) uint64 {
	n = n - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}
