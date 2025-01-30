[![GoDoc](https://godoc.org/github.com/opencoff/go-mph?status.svg)](https://godoc.org/github.com/opencoff/go-mph)
[![Go Report Card](https://goreportcard.com/badge/github.com/opencoff/go-mph)](https://goreportcard.com/report/github.com/opencoff/go-mph)

# go-mph - Minimal Perfect Hash Functions with persistence

## What is it?
A library to create, query and serialize/de-serialize minimal perfect hash function ("MPHF").
There are two implementations of MPH's for large data sets:

1. [CHD](http://cmph.sourceforge.net/papers/esa09.pdf) -
   inspired by this [gist](https://gist.github.com/pervognsen/b21f6dd13f4bcb4ff2123f0d78fcfd17).

2. [BBHash](https://arxiv.org/abs/1702.03154). It is in part inspired by
   Damien Gryski's [Boomphf](https://github.com/dgryski/go-boomphf)

One can construct an on-disk constant-time lookup using `go-mph` and
one of the MPHFs.  Such a DB is useful in situations
where the key/value pairs are NOT changed frequently; i.e.,
read-dominant workloads. The typical pattern in such situations is
to build the constant-DB _once_ for efficient retrieval and do
lookups multiple times.

*NOTE* Minimal Perfect Hash functions take a fixed input and
generate a mapping to lookup the items in constant time. In
particular, they are NOT a replacement for a traditional hash-table;
i.e., it may yield false-positives when queried using keys not
present during construction. In concrete terms:

   Let S = {k0, k1, ... kn}  be your input key set.

   If H: S -> {0, .. n} is a minimal perfect hash function, then
   H(kx) for kx NOT in S may yield an integer result (indicating
   that kx was successfully "looked up").

The way one deals with this is to compare the actual keys stored
against that index. `DBReader()`'s `Find()` method demonstrates how
this is done.

`go-mph` uses cryptographically strong checksum on the entire MPH DB *metadata*.
Additionally, tt uses siphash-2-4 checksums on each individual key-val 
record. This siphash checksum is verified opportunistically when keys
are looked up in the MPH DB.  The DB reader uses
an in-memory cache for speeding up lookups.



## How do I use it?
Like any other golang library: `go get github.com/opencoff/go-mph`.
The library exposes the following types:

* `DBWriter`: Used to construct a constant database of key-value
  pairs - where the lookup of a given key is done in constant time
  using CHD or BBHash. This type can be created by one of two
  functions: `NewChdDBWriter()` or `NewBBHashDBWriter()`.

  Once created, you add keys & values to it via the `Add()` method.
  After all the entries are added, you freeze the database by
  calling the `Freeze()` method.

  `DBWriter` optimizes the database if there are no values present -
  i.e., keys-only. This optimization significantly reduces the
  file-size.

* `DBReader`: Used to read a pre-constructed perfect-hash database and
  use it for constant-time lookups. The DBReader class comes with its
  own key/val cache to reduce disk accesses. The number of cache
  entries is configurable.

  After initializing the DB, key lookups are done primarily with the
  `Find()` method. A convenience method `Lookup()` elides errors and
  only returns the value and a boolean.

First, lets run some tests and make sure mph is working fine:

```sh

  $ git clone https://github.com/opencoff/go-mph
  $ cd go-mph
  $ go test .

```

## Example Program
There is a working example of the `DBWriter` and `DBReader` APIs in 
the `example/` sub directory. This example demonstrates the following
functionality:

- add one or more space delimited key/value files (first field is key, second
  field is value)
- add one or more CSV files (first field is key, second field is value)
- Write the resulting MPH DB to disk
- Read the DB and verify its integrity
- Dump the contents of the DB or the DB "meta data"

Now, lets build and run the example program:
```sh

  $ make
  $ go build -o mphdb ./example
  $ ./mphdb -V make foo.db -t txt chd /usr/share/dict/words
  $ ./mphdb -V fsck foo.db
  $ ./mphdb -V dump -m foo.db
  $ ./mphdb -V dump -a foo.db
```

This example above stores the words in the system dictionary into
a fast-lookup table using the CHD algorithm. `mphdb -h` shows you a helpful usage for what
else you can do with the example program.

There is a helper python script to generate a very large text file of
hostnames and IP addresses: `genhosts.py`. You can run it like so:

```sh

  $ python ./example/genhosts.py 192.168.0.0/16 > a.txt
```

The above example generates 65535 hostnames and corresponding IP addresses; each of the
IP addresses is sequentially drawn from the given subnet.

**NOTE** If you use a "/8" subnet mask you will generate a _lot_ of data (~430MB in size).

Once you have the input generated, you can feed it to the `example` program above to generate
a MPH DB:
```sh

  $ ./mphdb make foo.db chd a.txt
  $ ./mphdb fsck foo.db
```

It is possible that "mphdb" fails to construct a DB after trying 1,000,000 times. In that case,
try lowering the "load" factor (default is 0.85).

```sh

  $ ./mphdb make -l 0.75 foo.db chd a.txt
```

The example program in `example/` has helper routines to add from a
text or CSV delimited file: see `example/text.go`. In fact is is a more-or-less complete
usage of the MPH library API.

## Implementation Notes

* *bbhash.go*: Main implementation of the BBHash algorithm. This
  file implements the `MPHBuilder` and `MPH` interfaces (defined in
  *mph.go*).

* *bbhash_marshal.go*: Marshaling/Unmarshaling bbhash MPHF tables.

* *bitvector.go*: thread-safe bitvector implementation including a
  simple rank algorithm.

* *chd.go*: The main implementation of the CHD algorithm. This
  file implements the `MPHBuilder` and `MPH` interfaces (defined in
  *mph.go*).

* *chd_marshal.go*: Marshaling/Unmarshaling CHD MPHF tables.

* *dbreader.go*: Provides a constant-time lookup of a previously
  constructed MPH DB. DB reads use `mmap(2)` for reading the MPH
  metadata.  For little-endian architectures, there is no data
  "parsing" of the lookup tables, offset tables etc. They are 
  interpreted in-situ from the mmap'd data. To keep the code
  generic, every multi-byte int is converted to little-endian order
  before use. These conversion routines are in *endian_XX.go*.

* *dbwriter.go*: Create a read-only, constant-time MPH lookup DB. It 
  can store arbitrary byte stream "values" - each of which is
  identified by a unique `uint64` key. The DB structure is optimized
  for reading on the most common architectures - little-endian:
  amd64, arm64 etc.

* *slices.go*: Non-copying type conversion to/from byte-slices to
  uints of different widths.

* *utils.go*: Random number utils and other bits

## License
GPL v2.0
