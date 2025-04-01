// dbreader.go -- Constant DB built on top of the MPHF
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
	"os"
	"strings"

	"crypto/sha512"
	"crypto/subtle"

	"github.com/dchest/siphash"
	"github.com/hashicorp/golang-lru/arc/v2"
	"github.com/opencoff/go-mmap"
)

// DBReader represents the query interface for a previously constructed
// constant database (built using NewDBWriter()). The only meaningful
// operation on such a database is Lookup().
type DBReader struct {
	mph MPH

	cache *arc.ARCCache[uint64, []byte]

	flags uint32

	// memory mapped offset+hashkey table
	offset []uint64

	// memory mapped vlen table
	vlen []uint32

	nkeys  uint64
	salt   []byte
	offtbl uint64

	// original mmap slice
	mm *mmap.Mapping
	fd *os.File
	fn string
}

// NewDBReader reads a previously construct database in file 'fn'
// and prepares it for querying. Value records are opportunistically
// cached after reading from disk.  We retain upto 'cache' number
// of records in memory (default 128).
func NewDBReader(fn string, cache int) (rd *DBReader, err error) {
	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	// Number of records to cache
	if cache <= 0 {
		cache = 128
	}

	rd = &DBReader{
		salt: make([]byte, 16),
		fd:   fd,
		fn:   fn,
	}

	var st os.FileInfo

	st, err = fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("%s: can't stat: %w", fn, err)
	}

	if st.Size() < (64 + 32) {
		return nil, fmt.Errorf("%s: file too small or corrupted", fn)
	}

	var hdrb [64]byte

	_, err = io.ReadFull(fd, hdrb[:])
	if err != nil {
		return nil, fmt.Errorf("%s: can't read header: %w", fn, err)
	}

	offtbl, magic, err := rd.decodeHeader(hdrb[:], st.Size())
	if err != nil {
		return nil, err
	}

	err = rd.verifyChecksum(hdrb[:], offtbl, st.Size())
	if err != nil {
		return nil, err
	}

	// 8 + 8 + 4: offset, hashkey, vlen
	tblsz := rd.nkeys * (8 + 8 + 4)
	if (rd.flags & _DB_KeysOnly) > 0 {
		tblsz = rd.nkeys * 8
	}

	// All metadata is now verified.
	// sanity check - even though we have verified the strong checksum
	// 64 + 32: 64 bytes of header, 32 bytes of sha trailer
	if uint64(st.Size()) < (64 + 32 + tblsz) {
		return nil, fmt.Errorf("%s: corrupt header1", fn)
	}

	rd.cache, err = arc.NewARC[uint64, []byte](cache)
	if err != nil {
		return nil, err
	}

	// Now, we are certain that the header, the offset-table and MPH bits are
	// all valid and uncorrupted.

	// mmap the offset table
	mmapsz := st.Size() - int64(offtbl) - 32
	mm := mmap.New(fd)

	mapping, err := mm.Map(mmapsz, int64(offtbl), mmap.PROT_READ, mmap.F_READAHEAD)
	if err != nil {
		return nil, fmt.Errorf("%s: can't mmap %d bytes at off %d: %w",
			fn, mmapsz, offtbl, err)
	}

	// if this DB has only keys, then the offtbl is just u64 hash keys
	offsz := rd.nkeys * (8 + 8)
	vlensz := rd.nkeys * 4
	if (rd.flags & _DB_KeysOnly) > 0 {
		offsz = rd.nkeys * 8
		vlensz = 0
	}

	bs := mapping.Bytes()
	rd.mm = mapping
	rd.offset = bsToUint64Slice(bs[:offsz])
	if vlensz > 0 {
		rd.vlen = bsToUint32Slice(bs[offsz : offsz+vlensz])
	}

	// The MPH table starts here
	var mph MPH
	switch magic {
	case _Magic_CHD:
		mph, err = newChd(bs[offsz+vlensz:])

	case _Magic_BBHash:
		mph, err = newBBHash(bs[offsz+vlensz:])

	default:
		return nil, fmt.Errorf("unknown MPH DB type '%s'", magic)
	}

	if err != nil {
		return nil, fmt.Errorf("%s: can't unmarshal MPH index: %w", fn, err)
	}

	rd.mph = mph
	return rd, nil
}

// Len returns the size of the MPH key space; it is not exactly the
// total number of keys.
func (rd *DBReader) Len() int {
	return int(rd.nkeys)
}

// Close closes the db
func (rd *DBReader) Close() {
	rd.mm.Unmap()
	rd.fd.Close()
	rd.cache.Purge()
	rd.salt = nil
	rd.mph = nil
	rd.fd = nil
	rd.fn = ""
}

// Lookup looks up 'key' in the table and returns the corresponding value.
// If the key is not found, value is nil and returns false.
func (rd *DBReader) Lookup(key uint64) ([]byte, bool) {
	v, err := rd.Find(key)
	if err != nil {
		return nil, false
	}

	return v, true
}

// Dump the metadata to io.Writer 'w'
func (rd *DBReader) DumpMeta(w io.Writer) {
	fmt.Fprintf(w, "%s", rd.Desc())

	if (rd.flags & _DB_KeysOnly) > 0 {
		for i := uint64(0); i < rd.nkeys; i++ {
			fmt.Fprintf(w, "  %3d: %x\n", i, rd.offset[i])
		}
	} else {
		for i := uint64(0); i < rd.nkeys; i++ {
			j := i * 2
			h := rd.offset[j]
			o := rd.offset[j+1]
			fmt.Fprintf(w, "  %3d: %#x, %d bytes at %#x\n", i, h, rd.vlen[i], o)
		}
	}
}

// Desc provides a human description of the MPH db
func (rd *DBReader) Desc() string {
	var w strings.Builder

	if (rd.flags & _DB_KeysOnly) > 0 {
		fmt.Fprintf(&w, "MPH: <KEYS> %d keys, hash-salt %#x, offtbl at %#x\n",
			rd.nkeys, rd.salt, rd.offtbl)
	} else {
		fmt.Fprintf(&w, "MPH: <KEYS+VALS> %d keys, hash-salt %#x, offtbl at %#x\n",
			rd.nkeys, rd.salt, rd.offtbl)
	}
	rd.mph.DumpMeta(&w)
	return w.String()
}

// Find looks up 'key' in the table and returns the corresponding value.
// It returns an error if the key is not found or the disk i/o failed or
// the record checksum failed.
func (rd *DBReader) Find(key uint64) ([]byte, error) {
	if v, ok := rd.cache.Get(key); ok {
		return v, nil
	}

	// Not in cache. So, go to disk and find it.
	// We are guaranteed that: 0 <= i < rd.nkeys
	i, ok := rd.mph.Find(key)
	if !ok {
		return nil, ErrNoKey
	}
	if (rd.flags & _DB_KeysOnly) > 0 {
		// offtbl is just the keys; no values.
		if hash := toLittleEndianUint64(rd.offset[i]); hash != key {
			return nil, ErrNoKey
		}

		rd.cache.Add(key, nil)
		return nil, nil
	}

	// we have keys _and_ values

	j := i * 2
	if hash := toLittleEndianUint64(rd.offset[j]); hash != key {
		return nil, ErrNoKey
	}

	var val []byte
	var err error

	vlen := toLittleEndianUint32(rd.vlen[i])
	off := toLittleEndianUint64(rd.offset[j+1])
	if val, err = rd.decodeRecord(off, vlen); err != nil {
		return nil, err
	}

	rd.cache.Add(key, val)
	return val, nil
}

// IterFunc iterates through every record of the MPH db and
// calls 'fp' on each. If the called function returns non-nil,
// it stops the iteration and the error is propogated to the caller.
func (rd *DBReader) IterFunc(fp func(k uint64, v []byte) error) error {

	switch {
	case rd.flags&_DB_KeysOnly > 0:
		for i := uint64(0); i < rd.nkeys; i++ {
			k := rd.offset[i]
			if k == 0 {
				continue
			}
			if err := fp(k, nil); err != nil {
				return err
			}
		}
	default:
		// iter keys + values
		for i := uint64(0); i < rd.nkeys; i++ {
			j := i * 2
			k := rd.offset[j]
			if k == 0 {
				continue
			}
			vl := rd.vlen[i]
			off := rd.offset[j+1]
			val, err := rd.decodeRecord(off, vl)
			if err != nil {
				return fmt.Errorf("iter: key %x: read-record: %w", k, err)
			}
			if err := fp(k, val); err != nil {
				return err
			}
		}
	}
	return nil
}

// read the next full record at offset 'off' - by seeking to that offset.
// calculate the record checksum, validate it and so on.
func (rd *DBReader) decodeRecord(off uint64, vlen uint32) ([]byte, error) {
	_, err := rd.fd.Seek(int64(off), 0)
	if err != nil {
		return nil, err
	}

	data := make([]byte, vlen+8)

	_, err = io.ReadFull(rd.fd, data)
	if err != nil {
		return nil, err
	}

	be := binary.BigEndian
	csum := be.Uint64(data[:8])

	var o [8]byte

	be.PutUint64(o[:], off)

	h := siphash.New(rd.salt)
	h.Write(o[:])
	h.Write(data[8:])
	exp := h.Sum64()

	if csum != exp {
		return nil, fmt.Errorf("%s: corrupted record at off %d (exp %#x, saw %#x)", rd.fn, off, exp, csum)
	}
	return data[8:], nil
}

// Verify checksum of all metadata: offset table, chd bits and the file header.
// We know that offtbl is within the size bounds of the file - see decodeHeader() below.
// sz is the actual file size (includes the header we already read)
func (rd *DBReader) verifyChecksum(hdrb []byte, offtbl uint64, sz int64) error {
	h := sha512.New512_256()
	h.Write(hdrb[:])

	// remsz is the size of the remaining metadata (which begins at offset 'offtbl')
	// 32 bytes of SHA512_256 and the values already recorded.
	remsz := sz - int64(offtbl) - 32

	rd.fd.Seek(int64(offtbl), 0)

	nw, err := io.CopyN(h, rd.fd, remsz)
	if err != nil {
		return fmt.Errorf("%s: metadata i/o error: %w", rd.fn, err)
	}
	if nw != remsz {
		return fmt.Errorf("%s: partial read while verifying checksum, exp %d, saw %d", rd.fn, remsz, nw)
	}

	var expsum [32]byte

	// Read the trailer -- which is the expected checksum
	rd.fd.Seek(sz-32, 0)
	_, err = io.ReadFull(rd.fd, expsum[:])
	if err != nil {
		return fmt.Errorf("%s: checksum i/o error: %w", rd.fn, err)
	}

	csum := h.Sum(nil)
	if subtle.ConstantTimeCompare(csum[:], expsum[:]) != 1 {
		return fmt.Errorf("%s: checksum failure; exp %#x, saw %#x", rd.fn, expsum[:], csum[:])
	}

	rd.fd.Seek(int64(offtbl), 0)
	return nil
}

// entry condition: b is 64 bytes long.
func (rd *DBReader) decodeHeader(b []byte, sz int64) (uint64, string, error) {
	magic := string(b[:4])
	switch magic {
	case _Magic_CHD, _Magic_BBHash:

	default:
		return 0, "", fmt.Errorf("%s: bad file magic <%s>", rd.fn, magic)
	}

	be := binary.BigEndian
	i := 4

	rd.flags = be.Uint32(b[i : i+4])
	i += 4

	rd.salt = b[i : i+16]
	i += 16
	rd.nkeys = be.Uint64(b[i : i+8])
	i += 8
	rd.offtbl = be.Uint64(b[i : i+8])

	if rd.offtbl < 64 || rd.offtbl >= uint64(sz-32) {
		return 0, "", fmt.Errorf("%s: corrupt header0", rd.fn)
	}

	return rd.offtbl, magic, nil
}
