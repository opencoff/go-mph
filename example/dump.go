// dump.go -- 'dump' command implementation
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

	"github.com/opencoff/go-mph"
	flag "github.com/opencoff/pflag"
)

type dumpCommand struct{}

func init() {
	m := dumpCommand{}
	registerCommand("dump", &m)
}

func (m *dumpCommand) run(args []string, opt *Option) (err error) {
	var all, meta bool
	var db *mph.DBReader

	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	fs.BoolVarP(&all, "all", "a", false, "Dump keys and values")
	fs.BoolVarP(&meta, "meta", "m", false, "Dump only metadata")
	fs.Usage = func() {
		fmt.Printf(`Usage: dump [options] DB

where  'DB' is the name of MPH db

Options:
`)
		fs.PrintDefaults()
		os.Exit(0)
	}

	err = fs.Parse(args[1:])
	if err != nil {
		return fmt.Errorf("dump: %w", err)
	}

	args = fs.Args()
	if len(args) < 1 {
		return fmt.Errorf("dump: insufficient args")
	}

	fn := args[0]
	db, err = mph.NewDBReader(fn, 1000)
	if err != nil {
		return fmt.Errorf("dump: %w", err)
	}

	defer db.Close()

	if meta {
		db.DumpMeta(os.Stdout)
	} else if all {
		db.IterFunc(func(k uint64, v []byte) error {
			fmt.Printf("%#x: %x\n", k, v)
			return nil
		})
	} else {
		db.IterFunc(func(k uint64, _ []byte) error {
			fmt.Printf("%#x\n", k)
			return nil
		})
	}
	return nil
}
