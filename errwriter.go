// writer.go -- io.writer that handles errors gracefully
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
	"io"
)

type errWriter struct {
	w   io.Writer
	err error
}

func newErrWriter(w io.Writer) *errWriter {
	e := &errWriter{
		w: w,
	}
	return e
}

func (e *errWriter) Write(b []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}

	n, err := e.w.Write(b)
	if err != nil {
		e.err = err
		return n, err
	}
	if n != len(b) {
		e.err = shortWrite(n, len(b))
		return n, e.err
	}

	return n, nil
}

func (e *errWriter) Error() error {
	return e.err
}

func shortWrite(saw, exp int) error {
	return fmt.Errorf("short write: exp %d, wrote %d", exp, saw)
}
