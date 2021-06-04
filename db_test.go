// db_test.go -- test suite for dbreader/dbwriter
//
// (c) Sudhi Herle 2018
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

package mph

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/opencoff/go-fasthash"
)

var keep bool

func init() {
	flag.BoolVar(&keep, "keep", false, "Keep test DB")
}

func testDB(t *testing.T, wr *DBWriter) {
	assert := newAsserter(t)

	hseed := rand64()
	kvmap := make(map[uint64]string)
	for _, s := range keyw {
		h := fasthash.Hash64(hseed, []byte(s))
		err := wr.Add(h, []byte(s))
		assert(err == nil, "can't add key %x: %s", h, err)
		kvmap[h] = s
	}

	err := wr.Freeze()
	assert(err == nil, "freeze failed: %s", err)

	rd, err := NewDBReader(wr.Filename(), 10)
	assert(err == nil, "read failed: %s", err)

	//rd.DumpMeta(os.Stdout)
	for h, v := range kvmap {
		s, err := rd.Find(h)
		assert(err == nil, "can't find key %#x: %s", h, err)

		assert(string(s) == v, "key %x: value mismatch; exp '%s', saw '%s'", h, v, string(s))
	}

	// now look for keys not in the DB
	for i := 0; i < 10; i++ {
		v, err := rd.Find(uint64(i))
		assert(err != nil, "whoa: found key %d => %s", i, string(v))
	}
}

func TestDB(t *testing.T) {
	assert := newAsserter(t)

	salt := rand.Int()
	chdFn := fmt.Sprintf("%s/chd%d.db", os.TempDir(), salt)
	bbhFn := fmt.Sprintf("%s/bbhash%d.db", os.TempDir(), salt)

	cr, err := NewChdDBWriter(chdFn, 0.9)
	assert(err == nil, "can't create db %s: %s", chdFn, err)

	br, err := NewBBHashDBWriter(chdFn, 2.0)
	assert(err == nil, "can't create db %s: %s", bbhFn, err)

	defer func() {
		if keep {
			t.Logf("DB in %s, %s retained after test\n", chdFn, bbhFn)
		} else {
			os.Remove(chdFn)
			os.Remove(bbhFn)
		}
	}()

	cr = cr
	//testDB(t, cr)
	testDB(t, br)
}

func TestDBKeysOnly(t *testing.T) {
	assert := newAsserter(t)

	salt := rand.Int()
	chdFn := fmt.Sprintf("%s/chd%d.db", os.TempDir(), salt)
	bbhFn := fmt.Sprintf("%s/bbhash%d.db", os.TempDir(), salt)

	cr, err := NewChdDBWriter(chdFn, 0.9)
	assert(err == nil, "can't create db %s: %s", chdFn, err)

	br, err := NewBBHashDBWriter(chdFn, 1.7)
	assert(err == nil, "can't create db %s: %s", bbhFn, err)

	defer func() {
		if keep {
			t.Logf("DB in %s, %s retained after test\n", chdFn, bbhFn)
		} else {
			os.Remove(chdFn)
			os.Remove(bbhFn)
		}
	}()

	testOnlyKeys(t, cr)
	testOnlyKeys(t, br)
}

func testOnlyKeys(t *testing.T, wr *DBWriter) {
	assert := newAsserter(t)

	hseed := rand64()
	kvmap := make(map[uint64]string)
	for _, s := range keyw {
		h := fasthash.Hash64(hseed, []byte(s))
		err := wr.Add(h, nil)
		assert(err == nil, "can't add key %x: %s", h, err)
		kvmap[h] = s
	}

	err := wr.Freeze()
	assert(err == nil, "freeze failed: %s", err)

	rd, err := NewDBReader(wr.Filename(), 10)
	assert(err == nil, "read failed: %s", err)

	//rd.DumpMeta(os.Stdout)

	for h := range kvmap {
		s, err := rd.Find(h)
		assert(err == nil, "can't find key %#x: %s", h, err)
		assert(s == nil, "key %x: value mismatch; exp nil, saw '%s'", h, string(s))
	}

	// now look for keys not in the DB
	for i := 0; i < 10; i++ {
		j := rand64()
		v, err := rd.Find(j)
		assert(err != nil, "whoa: found key %d => %s", j, string(v))
	}
}
