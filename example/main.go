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

	flag "github.com/opencoff/pflag"
)

type value struct {
	hash uint64
	key  string
	val  string
}

func main() {
	var opt Option

	usage := fmt.Sprintf(
		`%s - make a MPH DB from one or more inputs

Usage: %s [global-options] CMD CMD-ARGS...

CMD is an operation to be performed and CMD-ARGS are operation specific 
arguments. The list of supported operations are:

  make [options] DB MPH_TYPE [INPUTS...]  -- Make a new MPH db from the inputs
  dump [options] DB                       -- Dump a MPH db
  fsck [options] DB                       -- Verify the integrity of the DB

Options:
`, os.Args[0], os.Args[0])

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.SetInterspersed(false)
	fs.SetOutput(os.Stdout)
	fs.BoolVarP(&opt.verbose, "verbose", "V", false, "Show verbose output")
	fs.Usage = func() {
		fmt.Printf(usage)
		fs.PrintDefaults()
		os.Exit(0)
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		die("%s", err)
	}

	args := fs.Args()
	if len(args) < 2 {
		fmt.Printf(usage)
		fs.PrintDefaults()
		os.Exit(0)
	}

	err := runCommand(fs.Args(), &opt)
	if err != nil {
		die("%s", err)
	}
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
