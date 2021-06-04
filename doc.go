// doc.go - top level documentation
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

// Package mph implements two different perfect hash functions for
// large data sets:
//    1. Compress Hash Displace: http://cmph.sourceforge.net/papers/esa09.pdf
//    2. BBHash: https://arxiv.org/abs/1702.03154).
//
// mph exposes a convenient way to serialize keys and values OR just keys
// into an on-disk single-file database. This serialized MPH DB is useful
// in situations where the reading from such a "constant" DB is much more
// frequent compared to updates to the DB.
//
// The primary user interface for this package is via the 'DBWriter' and
// 'DBReader' objects. Each objected added to the MPH is a <key, value> pair.
// The key is identified by a uint64 value - most commonly obtained by hashing
// a user specific object. The caller must ensure that they use a good
// hash function (eg siphash) that produces a random distribution of the keys.
// The 'DBWriter'
package mph
