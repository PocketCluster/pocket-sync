// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Pipe adapter to connect code expecting an io.Reader
// with code expecting an io.Writer.

package showpipe

import (
    "io"
    "sync"
    "time"

    "github.com/pkg/errors"
)

// ErrClosedPipe is the error used for read or write operations on a closed pipe.
var ErrClosedPipe = errors.New("io: read/write on closed pipe")

// A pipe is the shared pipe structure underlying PipeReader and PipeWriter.
type pipe struct {
    rl         sync.Mutex // gates readers one at a time
    wl         sync.Mutex // gates writers one at a time
    l          sync.Mutex // protects remaining fields
    data       []byte     // data remaining in pending write
    rwait      sync.Cond  // waiting reader
    wwait      sync.Cond  // waiting writer
    rerr       error      // if reader closed, error to give writes
    werr       error      // if writer closed, error to give reads

    reportC    chan PipeProgress
    totalSize  uint64
    accumlated uint64
    pipeUpdate time.Time
    pipeSpeed  float32
}

type PipeProgress struct {
    TotalSize   uint64
    Accumulated uint64
    Remaining   uint64
    DonePercent float32
    Speed       float32
}

func reportProgress(p *pipe, transferred int) {
    if p.reportC != nil {
        p.accumlated += uint64(transferred)
        var done = float32(float64(p.accumlated) / float64(p.totalSize))

        p.reportC <- PipeProgress{
            TotalSize:      p.totalSize,
            Accumulated:    p.accumlated,
            Remaining:      p.totalSize - p.accumlated,
            DonePercent:    done,
            Speed:          p.pipeSpeed,
        }
    }
}

func checkSpeed(p *pipe, submitted int) {
    var speed float32 = 0.0
    if p.pipeUpdate.IsZero() {
        speed = float32(submitted)
    } else {
        now := time.Now()
        dt := float64(now.Sub(p.pipeUpdate))
        speed = float32(float64(submitted) / dt)
        p.pipeUpdate = now
    }
    p.pipeSpeed = speed
}

func (p *pipe) read(b []byte) (n int, err error) {
    // One reader at a time.
    p.rl.Lock()
    defer p.rl.Unlock()

    p.l.Lock()
    defer p.l.Unlock()
    for {
        if p.rerr != nil {
            return 0, ErrClosedPipe
        }
        if p.data != nil {
            break
        }
        if p.werr != nil {
            return 0, p.werr
        }
        p.rwait.Wait()
    }
    n = copy(b, p.data)

    // report to progress
    reportProgress(p, n)

    p.data = p.data[n:]
    if len(p.data) == 0 {
        p.data = nil
        p.wwait.Signal()
    }
    return
}

var zero [0]byte

func (p *pipe) write(b []byte) (n int, err error) {
    // pipe uses nil to mean not available
    if b == nil {
        b = zero[:]
    }

    // One writer at a time.
    p.wl.Lock()
    defer p.wl.Unlock()

    p.l.Lock()
    defer p.l.Unlock()

    // this is a greate place to check speed
    checkSpeed(p, len(b))
    if p.werr != nil {
        err = ErrClosedPipe
        return
    }
    p.data = b
    p.rwait.Signal()
    for {
        if p.data == nil {
            break
        }
        if p.rerr != nil {
            err = p.rerr
            break
        }
        if p.werr != nil {
            err = ErrClosedPipe
        }
        p.wwait.Wait()
    }
    n = len(b) - len(p.data)
    p.data = nil // in case of rerr or werr
    return
}

func (p *pipe) rclose(err error) {
    if err == nil {
        err = ErrClosedPipe
    }
    p.l.Lock()
    defer p.l.Unlock()
    p.rerr = err
    // need to close channel at this place
    if p.reportC != nil {
        close(p.reportC)
        p.reportC = nil
    }
    p.rwait.Signal()
    p.wwait.Signal()
}

func (p *pipe) wclose(err error) {
    if err == nil {
        err = io.EOF
    }
    p.l.Lock()
    defer p.l.Unlock()
    p.werr = err
    p.rwait.Signal()
    p.wwait.Signal()
}

// A PipeReader is the read half of a pipe.
type PipeReader struct {
    p *pipe
}

// Read implements the standard Read interface:
// it reads data from the pipe, blocking until a writer
// arrives or the write end is closed.
// If the write end is closed with an error, that error is
// returned as err; otherwise err is EOF.
func (r *PipeReader) Read(data []byte) (n int, err error) {
    return r.p.read(data)
}

// Close closes the reader; subsequent writes to the
// write half of the pipe will return the error ErrClosedPipe.
func (r *PipeReader) Close() error {
    return r.CloseWithError(nil)
}

// CloseWithError closes the reader; subsequent writes
// to the write half of the pipe will return the error err.
func (r *PipeReader) CloseWithError(err error) error {
    r.p.rclose(err)
    return nil
}

// A PipeWriter is the write half of a pipe.
type PipeWriter struct {
    p *pipe
}

// Write implements the standard Write interface:
// it writes data to the pipe, blocking until readers
// have consumed all the data or the read end is closed.
// If the read end is closed with an error, that err is
// returned as err; otherwise err is ErrClosedPipe.
func (w *PipeWriter) Write(data []byte) (n int, err error) {
    return w.p.write(data)
}

// Close closes the writer; subsequent reads from the
// read half of the pipe will return no bytes and EOF.
func (w *PipeWriter) Close() error {
    return w.CloseWithError(nil)
}

// CloseWithError closes the writer; subsequent reads from the
// read half of the pipe will return no bytes and the error err,
// or EOF if err is nil.
//
// CloseWithError always returns nil.
func (w *PipeWriter) CloseWithError(err error) error {
    w.p.wclose(err)
    return nil
}

// Pipe creates a synchronous in-memory pipe.
// It can be used to connect code expecting an io.Reader
// with code expecting an io.Writer.
// Reads on one end are matched with writes on the other,
// copying data directly between the two; there is no internal buffering.
// It is safe to call Read and Write in parallel with each other or with
// Close. Close will complete once pending I/O is done. Parallel calls to
// Read, and parallel calls to Write, are also safe:
// the individual calls will be gated sequentially.
func Pipe() (*PipeReader, *PipeWriter) {
    p := new(pipe)
    p.rwait.L = &p.l
    p.wwait.L = &p.l
    r := &PipeReader{p}
    w := &PipeWriter{p}
    return r, w
}

func PipeWithReport(totalSize uint64) (*PipeReader, *PipeWriter, <- chan PipeProgress) {
    p := new(pipe)
    p.rwait.L = &p.l
    p.wwait.L = &p.l
    r := &PipeReader{p}
    w := &PipeWriter{p}

    p.reportC    = make(chan PipeProgress)
    p.totalSize  = totalSize
    p.accumlated = 0
    p.pipeUpdate = time.Time{}
    p.pipeSpeed  = 0.0

    return r, w, p.reportC
}
