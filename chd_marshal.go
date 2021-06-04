// chd_marshal.go -- marshal/unmarshal a CHD instance
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
	"encoding/binary"
	"fmt"
	"io"
)

// MarshalBinary encodes the hash into a binary form suitable for durable storage.
// A subsequent call to UnmarshalBinary() will reconstruct the CHD instance.
func (c *chd) MarshalBinary(w io.Writer) (int, error) {
	// Header: 2 64-bit words:
	//   o version byte
	//   o CHD_Seed_Size byte
	//   o resv [2]byte
	//   o nseeds uint32
	//   o salt 8 bytes
	//
	// Body:
	//   o <n> seeds laid out sequentially

	var x [_chdHeaderSize]byte // 4 x 64-bit words

	x[0] = 1
	x[1] = c.seedSize()
	binary.LittleEndian.PutUint32(x[4:8], uint32(c.Len()))
	binary.LittleEndian.PutUint64(x[8:], c.salt)
	nw, err := writeAll(w, x[:])
	if err != nil {
		return 0, err
	}

	m, err := c.seed.marshal(w)
	return nw + m, err
}

// Newchd reads a previously marshalled chd instance and returns
// a lookup table. It assumes that buf is memory-mapped and aligned at the
// right boundaries.
func newChd(buf []byte) (MPH, error) {
	if len(buf) < _chdHeaderSize {
		return nil, ErrTooSmall
	}

	hdr := buf[:_chdHeaderSize]
	buf = buf[_chdHeaderSize:]
	if hdr[0] != 1 {
		return nil, fmt.Errorf("chd: no support to un-marshal version %d", hdr[0])
	}

	var seed seeder

	size := uint32(hdr[1])
	n := binary.LittleEndian.Uint32(hdr[4:8])
	salt := binary.LittleEndian.Uint64(hdr[8:])
	vals := buf[:n*size]

	switch size {
	case 1:
		u8 := &u8Seeder{}
		if err := u8.unmarshal(vals); err != nil {
			return nil, nil
		}
		seed = u8
	case 2:
		if (len(vals) % 2) != 0 {
			return nil, fmt.Errorf("chd: partial seeds of size 2 (exp %d, saw %d)",
				len(vals)+1, len(vals))
		}

		u16 := &u16Seeder{}
		if err := u16.unmarshal(vals); err != nil {
			return nil, err
		}
		seed = u16

	case 4:
		if (len(vals) % 4) != 0 {
			return nil, fmt.Errorf("chd: partial seeds of size 2 (exp %d, saw %d)",
				len(vals)+3/4, len(vals))
		}
		u32 := &u32Seeder{}
		if err := u32.unmarshal(vals); err != nil {
			return nil, err
		}
		seed = u32

	default:
		return nil, fmt.Errorf("chd: unknown seed-size %d", size)
	}

	if n != uint32(seed.length()) {
		return nil, fmt.Errorf("chd: mismatch in number of seeds: exp %d, saw %d", n, seed.length())
	}

	c := &chd{
		seed: seed,
		salt: salt,
	}
	return c, nil
}
