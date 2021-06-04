// mph.go - Minimal perfect hash function interface
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
	"io"
)

// MPHBuilder is the common interface for constructing a MPH
// from a large number of keys
type MPHBuilder interface {
	// Add a new key
	Add(key uint64) error

	// Freeze the DB
	Freeze() (MPH, error)
}

type MPH interface {
	// Marshal the MPH into io.Writer 'w'; the writer is
	// guaranteed to start at a uint64 aligned boundary
	MarshalBinary(w io.Writer) (int, error)

	// Find the key and return a 0 based index - a perfect hash index
	// Return true if we find the key, false otherwise
	Find(key uint64) (uint64, bool)

	// Dump metadata about the constructed MPH to io.writer 'w'
	DumpMeta(w io.Writer)

	// Return number of entries in the MPH
	Len() int
}

// chd and bbhash both must satisfy these two interfaces
var _ MPHBuilder = &chdBuilder{}
var _ MPH = &chd{}

var _ MPHBuilder = &bbHashBuilder{}
var _ MPH = &bbHash{}
