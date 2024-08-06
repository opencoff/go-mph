// dbwriter.go -- Constant DB built on top of the CHD MPH
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
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/dchest/siphash"
)

// The on-disk DB has the following general structure:
//   - 64 byte file header: big-endian encoding of all multibyte ints
//      * magic    [4]byte
//      * flags    uint32 (indicates if DB is keys-only or keys+vals)
//      * salt     [16]byte random salt for siphash record integrity
//      * nkeys    uint64  Number of keys in the DB
//      * offtbl   uint64  File offset of MPH table (page-aligned)
//
//   - Contiguous series of records; each record is a key/value pair:
//      * cksum    uint64  Siphash checksum of value, offset (big endian)
//      * val      []byte  value bytes
//
//   - Possibly a gap until the next PageSize boundary (4096 bytes)
//   - The offset table is one of two things (exclusive-or):
//      * keys only ([]uint64)
//      * key ([]uint64), valuelen ([]uint32), offset ([]uint64)
//     The offset table is memory mapped and all entries are little-endian encoded
//     to solve for the common case of x86/arm64 archs.
//   - Marshaled MPH table(s)
//   - 32 bytes of strong checksum (SHA512_256); this checksum is done over
//     the file header, offset-table and marshaled MPH.
// Most data is serialized as big-endian integers. The exceptions are:
// Offset table:
//     This is mmap'd into the process and written as a little-endian uint64.
//     This is arguably an optimization -- most systems we work with are
//     little-endian. On big-endian systems, the DBReader code will convert
//     it on the fly to native-endian.

const (
	// Flags
	_DB_KeysOnly = 1 << iota

	_Magic_CHD    = "MPHC"
	_Magic_BBHash = "MPHB"
)

// writer state
type wstate int

const (
	_Aborted = -1
	_Open    = 0
	_Frozen  = 1
)

// DBWriter represents an abstraction to construct a read-only MPH database.
// The underlying MPHF is either CHD or BBHash. Keys and values are represented
// as arbitrary byte sequences ([]byte). The values are stored sequentially in
// the DB along with a checksum protecting the integrity of the data via
// siphash-2-4.  We don't want to use SHA512-256 over the entire file - because
// it will mean reading a potentially large file in DBReader(). By using
// checksums separately per record, we increase the overhead a bit - but
// speeds up DBReader initialization for the common case: we will be
// verifying actual records opportunistically.
//
// The DB meta-data and MPH tables are protected by strong checksum (SHA512-256).
type DBWriter struct {
	fd *os.File
	bb MPHBuilder

	// to detect duplicates
	keymap map[uint64]*value

	// siphash key: just binary encoded salt
	salt []byte

	// running count of current offset within fd where we are writing
	// records
	off uint64

	valSize uint64

	fntmp string // tmp file name
	fn    string // final file holding the PHF
	state wstate
	magic string
}

// things associated with each key/value pair
type value struct {
	off  uint64
	vlen uint32
}

// NewDBWriter prepares file 'fn' to hold a constant DB built using
// CHD minimal perfect hash function. Once written, the DB is "frozen"
// and readers will open it using NewDBReader() to do constant time lookups
// of key to value.
func NewChdDBWriter(fn string, load float64) (*DBWriter, error) {
	bb, err := NewChdBuilder(load)
	if err != nil {
		return nil, err
	}

	return newDBWriter(bb, fn, _Magic_CHD)
}

func NewBBHashDBWriter(fn string, g float64) (*DBWriter, error) {
	bb, err := NewBBHashBuilder(g)
	if err != nil {
		return nil, err
	}

	return newDBWriter(bb, fn, _Magic_BBHash)
}

func newDBWriter(bb MPHBuilder, fn string, magic string) (*DBWriter, error) {
	tmp := fmt.Sprintf("%s.tmp.%d", fn, rand32())
	fd, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}

	w := &DBWriter{
		fd:     fd,
		bb:     bb,
		keymap: make(map[uint64]*value),
		salt:   randbytes(16),
		off:    64, // starting offset past the header
		fn:     fn,
		fntmp:  tmp,
		magic:  magic,
	}

	// Leave some space for a header; we will fill this in when we
	// are done Freezing.
	var z [64]byte
	if _, err := writeAll(fd, z[:]); err != nil {
		return nil, err
	}

	return w, nil
}

// Len returns the total number of distinct keys in the DB
func (w *DBWriter) Len() int {
	return len(w.keymap)
}

// Return the filename of the underlying db
func (w *DBWriter) Filename() string {
	return w.fn
}

// AddKeyVals adds a series of key-value matched pairs to the db. If they are of
// unequal length, only the smaller of the lengths are used. Records with duplicate
// keys are discarded.
// Returns number of records added.
func (w *DBWriter) AddKeyVals(keys []uint64, vals [][]byte) (int, error) {
	if w.state != _Open {
		return 0, ErrFrozen
	}

	n := len(keys)
	if len(vals) < n {
		n = len(vals)
	}

	var z int
	for i := 0; i < n; i++ {
		if ok, err := w.addRecord(keys[i], vals[i]); err != nil {
			return z, err
		} else if ok {
			z++
		}
	}

	return z, nil
}

// Adds adds a single key,value pair.
func (w *DBWriter) Add(key uint64, val []byte) error {
	if w.state != _Open {
		return ErrFrozen
	}

	if _, err := w.addRecord(key, val); err != nil {
		return err
	}
	return nil
}

// Abort a construction
func (w *DBWriter) Abort() error {
	if w.state != _Open {
		return ErrFrozen
	}

	return w.abort()
}

func (w *DBWriter) abort() error {
	if err := os.Remove(w.fd.Name()); err != nil {
		return err
	}

	if err := w.fd.Close(); err != nil {
		return err
	}
	w.state = _Aborted
	return nil
}

// Freeze builds the minimal perfect hash, writes the DB and closes it.
func (w *DBWriter) Freeze() (err error) {
	defer func(e *error) {
		// undo the tmpfile
		if *e != nil {
			w.abort()
		}
	}(&err)

	if w.state != _Open {
		return ErrFrozen
	}

	var mp MPH

	mp, err = w.bb.Freeze()
	if err != nil {
		return err
	}

	// calculate strong checksum for all data from this point on.
	h := sha512.New512_256()

	tee := io.MultiWriter(w.fd, h)

	// We align the offset table to pagesize - so we can mmap it when we read it back.
	pgsz := uint64(os.Getpagesize())
	pgsz_m1 := pgsz - 1
	offtbl := w.off + pgsz_m1
	offtbl &= ^pgsz_m1

	if offtbl > w.off {
		zeroes := make([]byte, offtbl-w.off)
		if _, err = writeAll(w.fd, zeroes); err != nil {
			return err
		}
		w.off = offtbl
	}

	// Now offset is at a page boundary.

	var ehdr [64]byte

	// header is encoded in big-endian format
	// 4 byte magic
	// 4 byte flags
	// 8 byte salt
	// 8 byte nkeys
	// 8 byte offtbl
	be := binary.BigEndian
	copy(ehdr[:4], w.magic)

	i := 4
	if w.valSize == 0 {
		be.PutUint32(ehdr[i:i+4], uint32(_DB_KeysOnly))
	}
	i += 4

	i += copy(ehdr[i:], w.salt)
	be.PutUint64(ehdr[i:i+8], uint64(mp.Len()))
	i += 8
	be.PutUint64(ehdr[i:i+8], offtbl)

	// add header to checksum
	h.Write(ehdr[:])

	// write to file and checksum together
	if err := w.marshalOffsets(tee, mp); err != nil {
		return err
	}

	// align the offset to next 64 bit boundary
	offtbl = w.off + 7
	offtbl &= ^uint64(7)
	if offtbl > w.off {
		zeroes := make([]byte, offtbl-w.off)
		if _, err = writeAll(tee, zeroes); err != nil {
			return err
		}
		w.off = offtbl
	}

	// Next, we now encode the mph and write to disk.
	var nw int
	nw, err = mp.MarshalBinary(tee)
	if err != nil {
		return err
	}
	w.off += uint64(nw)

	// Trailer is the checksum of everything
	cksum := h.Sum(nil)
	if _, err = writeAll(w.fd, cksum[:]); err != nil {
		return err
	}

	// Finally, write the header at start of file
	w.fd.Seek(0, 0)
	if _, err = writeAll(w.fd, ehdr[:]); err != nil {
		return err
	}

	if err = w.fd.Sync(); err != nil {
		return err
	}

	if err = w.fd.Close(); err != nil {
		return err
	}

	if err = os.Rename(w.fntmp, w.fn); err != nil {
		return err
	}
	w.state = _Frozen
	return nil
}

// write the offset mapping table and value-len table
func (w *DBWriter) marshalOffsets(tee io.Writer, mp MPH) error {
	if w.valSize == 0 {
		return w.marshalKeys(tee, mp)
	}

	n := uint64(mp.Len())
	offset := make([]uint64, 2*n)
	vlen := make([]uint32, n)

	for k, r := range w.keymap {
		i, ok := mp.Find(k)
		if !ok {
			return fmt.Errorf("dbwriter: panic: can't find key %x", k)
		}

		vlen[i] = r.vlen

		// each entry is 2 64-bit words
		j := i * 2
		offset[j] = k
		offset[j+1] = r.off
	}

	bs := u64sToByteSlice(offset)
	if _, err := writeAll(tee, bs); err != nil {
		return err
	}

	// Now write the value-length table
	bs = u32sToByteSlice(vlen)
	if _, err := writeAll(tee, bs); err != nil {
		return err
	}

	w.off += uint64(n * (8 + 8 + 4))
	return nil
}

// write just the keys - since we don't have values
func (w *DBWriter) marshalKeys(tee io.Writer, bb MPH) error {
	n := uint64(bb.Len())
	offset := make([]uint64, n)
	for k := range w.keymap {
		i, ok := bb.Find(k)
		if !ok {
			return fmt.Errorf("dbwriter: panic: can't find key %x", k)
		}
		offset[i] = k
	}

	bs := u64sToByteSlice(offset)
	if _, err := writeAll(tee, bs); err != nil {
		return err
	}
	w.off += uint64(n * 8)
	return nil
}

// compute checksums and add a record to the file at the current offset.
func (w *DBWriter) addRecord(key uint64, val []byte) (bool, error) {
	if uint64(len(val)) > uint64(1<<32)-1 {
		return false, ErrValueTooLarge
	}

	_, ok := w.keymap[key]
	if ok {
		return false, ErrExists
	}

	// first add to the underlying PHF constructor
	if err := w.bb.Add(key); err != nil {
		return false, err
	}

	v := &value{
		off:  w.off,
		vlen: uint32(len(val)),
	}
	w.keymap[key] = v

	// Don't write values if we don't need to
	if len(val) > 0 {
		if err := w.writeRecord(val, v.off); err != nil {
			return false, err
		}

		w.valSize += uint64(len(val))
	}

	return true, nil
}

// writeRecord writes a record and checksum at the offset, updates the
// offset in the offset table
func (w *DBWriter) writeRecord(val []byte, off uint64) error {
	var o [8]byte
	var c [8]byte

	be := binary.BigEndian
	be.PutUint64(o[:], off)

	h := siphash.New(w.salt)
	h.Write(o[:])
	h.Write(val)
	be.PutUint64(c[:], h.Sum64())

	// Checksum at the start of record
	if _, err := writeAll(w.fd, c[:]); err != nil {
		return err
	}

	if _, err := writeAll(w.fd, val); err != nil {
		return err
	}

	w.off += uint64(len(val)) + 8
	return nil
}

// write all bytes
func writeAll(w io.Writer, buf []byte) (int, error) {
	n, err := w.Write(buf)
	if err != nil {
		return 0, err
	}
	if n != len(buf) {
		return n, errShortWrite("db", n)
	}
	return n, nil
}
