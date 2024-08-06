// fsck.go -- 'fsck' command implementation
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

type fsckCommand struct{}

func init() {
	m := fsckCommand{}
	registerCommand("fsck", &m)
}

func (m *fsckCommand) run(args []string, opt *Option) (err error) {
	var db *mph.DBReader

	fs := flag.NewFlagSet("fsck", flag.ExitOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		fmt.Printf(`Usage: fsck [options] DB

where  'DB' is the name of MPH db

Options:
`)
		fs.PrintDefaults()
		os.Exit(0)
	}

	err = fs.Parse(args[1:])
	if err != nil {
		return fmt.Errorf("fsck: %w", err)
	}

	args = fs.Args()
	if len(args) < 1 {
		return fmt.Errorf("fsck: insufficient args")
	}

	fn := args[0]
	db, err = mph.NewDBReader(fn, 1000)
	if err != nil {
		return fmt.Errorf("fsck: %w", err)
	}

	defer db.Close()

	opt.Printf(db.Desc())
	return nil
}
