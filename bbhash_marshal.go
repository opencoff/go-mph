// marshal.go - Marshal/Unmarshal for bbHash datastructure
//
// Implements the bbHash algorithm in: https://arxiv.org/abs/1702.03154
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
	"fmt"
	"io"

	"encoding/binary"
)

// MarshalBinary encodes the hash into a binary form suitable for durable storage.
// A subsequent call to UnmarshalBinary() will reconstruct the bbHash instance.
func (bb *bbHash) MarshalBinary(w io.Writer) (int, error) {

	// Header: 2 64-bit words:
	//   o byte version
	//   o byte[3] resv
	//   o uint32 n-bitvectors
	//   o uint64 salt
	//
	// Body:
	//   o <n> bitvectors laid out consecutively

	var x [16]byte

	le := binary.LittleEndian

	x[0] = 1
	le.PutUint32(x[4:8], uint32(len(bb.bits)))
	le.PutUint64(x[8:], bb.salt)

	wr := newErrWriter(w)
	n, _ := wr.Write(x[:])
	// Now, write the bitvectors themselves
	for _, bv := range bb.bits {
		m, _ := bv.MarshalBinary(wr)
		n += m
	}

	return n + 16, wr.Error()
}

// NewbbHash reads a previously marshalled binary from buffer 'buf' into
// an in-memory instance of bbHash. 'buf' is assumed to be memory mapped.
func newBBHash(buf []byte) (MPH, error) {
	// header is 16 bytes
	le := binary.LittleEndian
	ver := buf[0]
	bv := le.Uint32(buf[4:8])
	salt := le.Uint64(buf[8:16])
	if ver != 1 {
		return nil, fmt.Errorf("bbhash: no support to un-marshal version %d", ver)
	}
	if bv == 0 || bv > _MaxLevel {
		return nil, fmt.Errorf("bbhash: too many levels %d (max %d)", bv, _MaxLevel)
	}

	bb := &bbHash{
		bits: make([]*bitVector, bv),
		salt: salt,
	}

	buf = buf[16:]
	for i := uint32(0); i < bv; i++ {
		bv, n, err := unmarshalBitVector(buf)
		if err != nil {
			return nil, err
		}

		bb.bits[i] = bv
		buf = buf[n:]
	}

	bb.preComputeRank()
	return bb, nil
}
