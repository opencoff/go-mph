// bbhash.go - fast minimal perfect hashing for massive key sets
//
// Implements the BBHash algorithm in: https://arxiv.org/abs/1702.03154
//
// Inspired by D Gryski's implementation of bbHash (https://github.com/dgryski/go-boomphf)
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
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

// bbHash represents a computed minimal perfect hash for a given set of keys using
// the bbHash algorithm: https://arxiv.org/abs/1702.03154.
type bbHash struct {
	bits  []*bitVector
	ranks []uint64
	salt  uint64
	g     float64 // gamma - rankvector size expansion factor
	n     int     // number of keys
}

// state used by go-routines when we concurrentize the algorithm
type state struct {
	sync.Mutex

	A    *bitVector
	coll *bitVector
	redo []uint64

	lvl uint32

	bb *bbHash
}

// Gamma is an expansion factor for each of the bitvectors we build.
// Empirically, 2.0 is found to be a good balance between speed and
// space usage. See paper for more details.
const _Gamma float64 = 2.0

// Maximum number of attempts (level) at making a perfect hash function.
// Per the paper, each successive level exponentially reduces the
// probability of collision.
const _MaxLevel uint32 = 4000

// Minimum number of keys before bbhash switches to a concurrent
// construction algorithm
const MinParallelKeys int = 20000

// set to true for verbose debug
const debug bool = false

type bbHashBuilder struct {
	keys []uint64
	g    float64
}

// NewBBHashBuilder enables creation of a minimal perfect hash function via the
// BBHash algorithm. Once created, callers can add keys to it before Freezing
// the MPH and generating a constant time lookup table. The parameter 'g'
// is "Gamma" from the paper; the recommended value is >= 2.0; larger values
// increase the constructed table size and also decreases probability of
// construction failure.
// Once the construction is frozen, callers can use "Find()" to find the
// unique mapping for each key in 'keys'.
func NewBBHashBuilder(g float64) (MPHBuilder, error) {
	b := &bbHashBuilder{
		keys: make([]uint64, 0, 1024),
		g:    g,
	}
	return b, nil
}

// Add a new key to the MPH builder
func (b *bbHashBuilder) Add(key uint64) error {
	b.keys = append(b.keys, key)
	return nil
}

// New creates a new minimal hash function to represent the keys in 'keys'.
// This constructor selects a faster concurrent algorithm if the number of
// keys are greater than 'MinParallelKeys'.
// Once the construction is complete, callers can use "Find()" to find the
// unique mapping for each key in 'keys'.
func (b *bbHashBuilder) Freeze() (MPH, error) {
	bb := &bbHash{
		salt: rand64(),
		g:    b.g,
		n:    len(b.keys),
	}

	s := bb.newState()

	var err error

	if bb.n > MinParallelKeys {
		err = s.concurrent(b.keys)
	} else {
		err = s.singleThread(b.keys)
	}

	if err != nil {
		return nil, err
	}

	return bb, nil
}

func (bb *bbHash) Len() int {
	return bb.n
}

// Find returns a unique integer representing the minimal hash for key 'k'.
// The return value is meaningful ONLY for keys in the original key set (provided
// at the time of construction of the minimal-hash).
// If the key is in the original key-set
func (bb *bbHash) Find(k uint64) (uint64, bool) {
	for lvl, bv := range bb.bits {
		i := bhash(k, bb.salt, uint32(lvl)) % bv.Size()

		if !bv.IsSet(i) {
			continue
		}

		rank := 1 + bb.ranks[lvl] + bv.Rank(i)

		// bbhash returns a 1-based index.
		return rank - 1, true
	}

	return 0, false
}

// DumpMeta dumps the metadata of the underlying bbhash
func (bb *bbHash) DumpMeta(w io.Writer) {
	var b bytes.Buffer

	b.WriteString(fmt.Sprintf("bbHash: salt %#x; %d levels\n", bb.salt, len(bb.bits)))

	for i, bv := range bb.bits {
		sz := humansize(bv.Words() * 8)
		b.WriteString(fmt.Sprintf("  %d: %d bits (%s)\n", i, bv.Size(), sz))
	}

	w.Write(b.Bytes())
}

// NewSerial creates a new minimal hash function to represent the keys in 'keys'.
// This constructor explicitly uses a single-threaded (non-concurrent) construction.
func newSerial(g float64, keys []uint64) (*bbHash, error) {
	if g <= 1.0 {
		g = 2.0
	}
	bb := &bbHash{
		salt: rand64(),
		g:    g,
		n:    len(keys),
	}
	s := bb.newState()
	err := s.singleThread(keys)
	if err != nil {
		return nil, err
	}
	return bb, nil
}

// NewConcurrent creates a new minimal hash function to represent the keys in 'keys'.
// This gives callers explicit control over when to use a concurrent algorithm vs. serial.
func newConcurrent(g float64, keys []uint64) (*bbHash, error) {
	if g <= 1.0 {
		g = 2.0
	}
	bb := &bbHash{
		salt: rand64(),
		g:    g,
		n:    len(keys),
	}
	s := bb.newState()
	err := s.concurrent(keys)
	if err != nil {
		return nil, err
	}
	return bb, nil
}

// return optimal size for bitvector
func (bb *bbHash) bvSize() uint64 {
	return uint64(float64(bb.n) * bb.g)
}

// setup state for serial or concurrent execution
func (bb *bbHash) newState() *state {
	sz := bb.bvSize()
	s := &state{
		A:    newBitVector(sz),
		coll: newBitVector(sz),
		redo: make([]uint64, 0, sz),
		bb:   bb,
	}

	//printf("bbhash: salt %#x, gamma %4.2f %d keys A %d bits", bb.salt, bb.g, nkeys, s.A.Size())
	return s
}

// single-threaded serial invocation of the bbHash algorithm
func (s *state) singleThread(keys []uint64) error {
	A := s.A

	for {
		//printf("lvl %d: %d keys A %d bits", s.lvl, len(keys), A.Size())
		preprocess(s, keys)
		A.Reset()
		assign(s, keys)

		keys, A = s.nextLevel()
		if keys == nil {
			break
		}

		if s.lvl > _MaxLevel {
			return fmt.Errorf("can't find minimal perf hash after %d tries", s.lvl)
		}
	}
	s.bb.preComputeRank()
	return nil
}

// run the bbHash algorithm concurrently on a sharded set of keys.
// entry: len(keys) > MinParallelKeys
func (s *state) concurrent(keys []uint64) error {

	ncpu := runtime.NumCPU()
	A := s.A

	for {
		nkey := uint64(len(keys))
		z := nkey / uint64(ncpu)
		r := nkey % uint64(ncpu)

		var wg sync.WaitGroup

		// Pre-process keys and detect colliding entries
		wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			i := i
			x := z * uint64(i)
			y := x + z
			if i == (ncpu - 1) {
				y += r
			}
			go func(x, y uint64) {
				//printf("lvl %d: cpu %d; Pre-process shard %d:%d", s.lvl, i, x, y)
				preprocess(s, keys[x:y])
				wg.Done()
			}(x, y)
		}

		// synchronization point
		wg.Wait()

		// Assignment step
		A.Reset()
		wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			i := i
			x := z * uint64(i)
			y := x + z
			if i == (ncpu - 1) {
				y += r
			}
			go func(x, y uint64) {
				//printf("lvl %d: cpu %d; Assign shard %d:%d", s.lvl, i, x, y)
				assign(s, keys[x:y])
				wg.Done()
			}(x, y)
		}

		// synchronization point #2
		wg.Wait()
		keys, A = s.nextLevel()
		if keys == nil {
			break
		}

		// Now, see if we have enough keys to concurrentize
		if len(keys) < MinParallelKeys {
			return s.singleThread(keys)
		}

		if s.lvl > _MaxLevel {
			return fmt.Errorf("can't find minimal perf hash after %d tries", s.lvl)
		}

	}

	s.bb.preComputeRank()

	return nil
}

// pre-process to detect colliding bits
func preprocess(s *state, keys []uint64) {
	A := s.A
	coll := s.coll
	salt := s.bb.salt
	sz := A.Size()
	//printf("lvl %d => sz %d", s.lvl, sz)
	for _, k := range keys {
		//printf("   key %#x..", k)
		i := bhash(k, salt, s.lvl) % sz

		if coll.IsSet(i) {
			continue
		}
		if A.IsSet(i) {
			coll.Set(i)
			continue
		}
		A.Set(i)
	}
}

// phase-2 -- assign non-colliding bits; this too can be concurrentized
// the redo-list can be local until we finish scanning all the keys.
// XXX "A" could also be kept local and finally merged via bitwise-union.
func assign(s *state, keys []uint64) {
	A := s.A
	coll := s.coll
	salt := s.bb.salt
	sz := A.Size()
	redo := make([]uint64, 0, len(keys)/4)
	for _, k := range keys {
		i := bhash(k, salt, s.lvl) % sz

		if coll.IsSet(i) {
			redo = append(redo, k)
			continue
		}
		A.Set(i)
	}

	if len(redo) > 0 {
		s.appendRedo(redo)
	}
}

// add the local copy of 'redo' list to the central list.
func (s *state) appendRedo(k []uint64) {

	s.Lock()
	s.redo = append(s.redo, k...)
	//printf("lvl %d: redo += %d keys", s.lvl, len(k))
	s.Unlock()
}

// append the current A to the bits vector and begin new iteration
// return new keys and a new A.
// NB: This is *always* called from a single-threaded context
//
//	(i.e., synchronization point).
func (s *state) nextLevel() ([]uint64, *bitVector) {
	s.bb.bits = append(s.bb.bits, s.A)
	s.A = nil

	//printf("lvl %d: next-step: remaining: %d keys", s.lvl, len(s.redo))
	keys := s.redo
	if len(keys) == 0 {
		return nil, nil
	}

	s.redo = s.redo[:0]
	s.A = newBitVector(s.bb.bvSize())
	s.coll.Reset()
	s.lvl++
	return keys, s.A
}

// Precompute ranks for each level so we can answer queries quickly.
func (bb *bbHash) preComputeRank() {
	var pop uint64
	bb.ranks = make([]uint64, len(bb.bits))

	// We omit the first level in rank calculation; this avoids a special
	// case in Find() when we are looking at elements in level-0.
	for l, bv := range bb.bits {
		bb.ranks[l] = pop
		pop += bv.ComputeRank()
	}
}

// One round of Zi Long Tan's superfast hash
func bhash(key, salt uint64, lvl uint32) uint64 {
	const m uint64 = 0x880355f21e6d1965
	var h uint64 = m

	h ^= mix(key)
	h *= m
	h ^= mix(salt)
	h *= m
	h ^= mix(uint64(lvl))
	h *= m
	h = mix(h)
	return h
}

func printf(f string, v ...interface{}) {
	if !debug {
		return
	}

	s := fmt.Sprintf(f, v...)
	if n := len(s); s[n-1] != '\n' {
		s += "\n"
	}

	os.Stdout.WriteString(s)
	os.Stdout.Sync()
}
