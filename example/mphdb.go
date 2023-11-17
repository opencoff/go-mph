// mphdb.go -- Build a Constant DB based on BBHash MPH
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

// mphdb.go is an example of using bbhash:DBWriter() and DBReader.
// One can construct the on-disk MPH DB using a variety of input:
//   - white space delimited text file: first field is key, second field is value
//   - Comma Separated text file (CSV): first field is key, second field is value
//
// Sometimes, bbhash gets into a pathological state while constructing MPH out of very
// large data sets. This can be alleviated by using a larger "gamma". mphdb tries to
// bump the gamma to "4.0" whenever we have more than 1M keys.

package main

import (
	"fmt"
	"os"
	"strings"

	"time"

	"github.com/opencoff/go-mph"

	flag "github.com/opencoff/pflag"
)

type value struct {
	hash uint64
	key  string
	val  string
}

func main() {
	var load, gamma float64
	var verify, dump, text bool

	usage := fmt.Sprintf(
		`%s - make a MPH DB from one or more inputs

Usage: %s [options] TYPE OUTPUT [INPUT ...]
       %s -d|-v FILENAME

The first form is used to create an MPH DB from one or more INPUTs.
TYPE is 'chd' OR 'bbhash', OUTPUT is the output MPH DB name.
INPUT can be a file ending in:
   .txt: a key,value per line delimited by white space or just
         keys on each line
   .csv: a CSV text file

The second form is used to dump a MPH DB or verify its integrity.

Options:
`, os.Args[0], os.Args[0], os.Args[0])

	flag.Float64VarP(&load, "load", "l", 0.85, "Use `L` as the CHD hash table load factor")
	flag.Float64VarP(&gamma, "gamma", "g", 2.0, "Use `G` as the 'gamma' for BBHash")
	flag.BoolVarP(&verify, "verify", "V", false, "Verify a constant DB")
	flag.BoolVarP(&dump, "dump-meta", "d", false, "Dump db meta-data")
	flag.BoolVarP(&text, "text", "t", false, "Assume the input file(s) are text")
	flag.Usage = func() {
		fmt.Printf("mphdb - create MPH DB from txt or CSV files using CHD\nUsage: %s\n   TYPE must be 'chd' or 'bbhash'\n", usage)
		flag.PrintDefaults()
	}

	flag.Parse()
	args := flag.Args()

	if verify || dump {
		if len(args) < 1 {
			die("No file name to dump!\nUsage: %s\n", usage)
		}

		fn := args[0]
		db, err := mph.NewDBReader(fn, 1000)
		if err != nil {
			die("Can't read %s: %s", fn, err)
		}

		if verify {
			fmt.Printf("%s: %d records\n", fn, db.Len())
		} else {
			db.DumpMeta(os.Stdout)
		}

		db.Close()
		return
	}

	if len(args) < 2 {
		die("No type or output file name!\nUsage: %s\n", usage)
	}

	typ := args[0]
	fn := args[1]
	args = args[2:]

	var db *mph.DBWriter
	var err error

	switch typ {
	case "chd":
		db, err = mph.NewChdDBWriter(fn, load)

	case "bbhash":
		db, err = mph.NewBBHashDBWriter(fn, gamma)

	default:
		die("Unknown MPH type '%s' (allowed: 'chd' or 'bbhash')", typ)
	}

	if err != nil {
		die("can't create %s MPH DB: %s", typ, err)
	}

	var tot uint64
	if len(args) > 0 {
		var n uint64
		for _, f := range args {
			switch {
			case strings.HasSuffix(f, ".txt"):
				n, err = AddTextFile(db, f, " \t")

			case strings.HasSuffix(f, ".csv"):
				n, err = AddCSVFile(db, f, ',', '#', 0, 1)

			default:
				if !text {
					warn("Don't know how to add %s", f)
					continue
				}
				n, err = AddTextFile(db, f, " \t")
			}

			if err != nil {
				warn("can't add %s: %s", f, err)
				continue
			}

			fmt.Printf("+ %s: %d records\n", f, n)
			tot += n
		}
	} else {
		var n uint64

		n, err = AddTextStream(db, os.Stdin, " \t")
		if err != nil {
			db.Abort()
			die("can't add STDIN: %s", err)
		}

		fmt.Printf("+ <STDIN>: %d records\n", n)
		tot += n
	}

	start := time.Now()
	err = db.Freeze()
	if err != nil {
		db.Abort()
		die("can't write db %s: %s", fn, err)
	}
	delta := time.Now().Sub(start)
	speed := (1.0e6 * float64(tot)) / float64(delta.Microseconds())
	fmt.Printf("%d keys, %s (%3.2f keys/sec)\n", tot, delta, speed)
}

// die with error
func die(f string, v ...interface{}) {
	warn(f, v...)
	os.Exit(1)
}

func warn(f string, v ...interface{}) {
	z := fmt.Sprintf("%s: %s", os.Args[0], f)
	s := fmt.Sprintf(z, v...)
	if n := len(s); s[n-1] != '\n' {
		s += "\n"
	}

	os.Stderr.WriteString(s)
	os.Stderr.Sync()
}

// vim: ft=go:sw=4:ts=4:noexpandtab:tw=78:
