// cmds.go -- commands abstraction
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
	"sync"
)

type command interface {
	run(args []string, opt *Option) error
}

var cmds = struct {
	sync.Mutex
	m map[string]command
}{
	m: make(map[string]command),
}

func registerCommand(nm string, cmd command) {
	cmds.Lock()
	if _, ok := cmds.m[nm]; ok {
		panic(fmt.Sprintf("%s already registered", nm))
	}
	cmds.m[nm] = cmd
	cmds.Unlock()
}

func runCommand(args []string, o *Option) error {
	nm := args[0]

	cmds.Lock()
	defer cmds.Unlock()
	cmd, ok := cmds.m[nm]
	if !ok {
		return fmt.Errorf("unknown command %s", nm)
	}

	return cmd.run(args, o)
}

type Option struct {
	verbose bool
}

func (o *Option) Printf(s string, v ...interface{}) {
	if o.verbose {
		fmt.Printf(s, v...)
	}
}
