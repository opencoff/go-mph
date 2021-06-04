// helpers_test.go - helper routines for tests
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
	"fmt"
	"runtime"
	"testing"
)

func newAsserter(t *testing.T) func(cond bool, msg string, args ...interface{}) {
	return func(cond bool, msg string, args ...interface{}) {
		if cond {
			return
		}

		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 0
		}

		s := fmt.Sprintf(msg, args...)
		t.Fatalf("%s: %d: Assertion failed: %s\n", file, line, s)
	}
}

var keyw = []string{
	"expectoration",
	"mizzenmastman",
	"stockfather",
	"pictorialness",
	"villainous",
	"unquality",
	"sized",
	"Tarahumari",
	"endocrinotherapy",
	"quicksandy",
	"heretics",
	"pediment",
	"spleen's",
	"Shepard's",
	"paralyzed",
	"megahertzes",
	"Richardson's",
	"mechanics's",
	"Springfield",
	"burlesques",
}
