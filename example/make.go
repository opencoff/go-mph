// make.go -- 'make' command implementation
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

package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/opencoff/go-mph"
	flag "github.com/opencoff/pflag"
)

type makeCommand struct{}

func init() {
	m := makeCommand{}
	registerCommand("make", &m)
}

func (m *makeCommand) run(args []string, opt *Option) (err error) {
	var load, gamma float64
	var db *mph.DBWriter

	defer func(e *error) {
		if *e != nil && db != nil {
			db.Abort()
		}
	}(&err)

	fs := flag.NewFlagSet("make", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	fs.Float64VarP(&load, "load", "l", 0.85, "Use `L` as the CHD hash table load factor")
	fs.Float64VarP(&gamma, "gamma", "g", 2.0, "Use `G` as the 'gamma' for BBHash")
	fs.Usage = func() {
		fmt.Printf(`Usage: make [options] DB TYPE [INPUT...]

where:
   DB	    is the name of the output MPH database file
   TYPE	    should be one of 'chd' or 'bbhash'
   INPUT    is one or more optional input files

The input file(s) must have a name suffix of one of the following:
   .txt	    A key,value per-line delimited by white space 
   .txt     one key per line (no embedded whitespace)
   .csv	    A comma-separated key,value file

options:
`)
		fs.PrintDefaults()
		os.Exit(0)
	}

	err = fs.Parse(args[1:])
	if err != nil {
		return fmt.Errorf("make: %w", err)
	}

	args = fs.Args()
	if len(args) < 2 {
		return fmt.Errorf("make: insufficient args")
	}

	fn := args[0]
	typ := args[1]
	args = args[2:]

	switch typ {
	case "chd":
		db, err = mph.NewChdDBWriter(fn, load)

	case "bbhash":
		db, err = mph.NewBBHashDBWriter(fn, gamma)

	default:
		return fmt.Errorf("make: unknown MPH type '%s'", typ)
	}

	if err != nil {
		return fmt.Errorf("make: can't create %s MPH DB: %w", typ, err)
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
				return fmt.Errorf("make: don't know how to add %s", f)
			}

			if err != nil {
				return fmt.Errorf("make: can't add %s: %s", f, err)
			}

			opt.Printf("+ %s: %d records\n", f, n)
			tot += n
		}
	} else {
		var n uint64

		n, err = AddTextStream(db, os.Stdin, " \t")
		if err != nil {
			return fmt.Errorf("make: can't add text from stdin: %w", err)
		}

		opt.Printf("+ <STDIN>: %d records\n", n)
		tot += n
	}

	start := time.Now()
	err = db.Freeze()
	if err != nil {
		return fmt.Errorf("make: can't write db %s: %s", fn, err)
	}
	delta := time.Now().Sub(start)
	speed := (1.0e6 * float64(tot)) / float64(delta.Microseconds())
	opt.Printf("%d keys, %s (%3.1f keys/sec)\n", tot, delta.Truncate(time.Millisecond).String(), speed)

	return nil
}
