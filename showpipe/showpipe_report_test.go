// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package showpipe

import (
    "io"
    "testing"
)

// Test a single read/write pair.
func TestPipeReport1(t *testing.T) {
    var (
        message = []byte("hello, world")
        msgLen = uint64(len(message))
        c = make(chan int)
        r, w, p = PipeWithReport(msgLen)
        buf = make([]byte, 64)
    )
    go checkWrite(t, w, message, c)
    go func() {
        for pr := range p {
            if pr.TotalSize != msgLen {
                t.Error("invalid total size")
            }
            if pr.Received != msgLen {
                t.Error("invalid accumulated size")
            }
            if pr.Remaining != 0 {
                t.Error("invalid remaining size")
            }
        }
    }()
    n, err := r.Read(buf)
    if err != nil {
        t.Errorf("read: %v", err)
    } else if n != 12 || string(buf[0:12]) != "hello, world" {
        t.Errorf("bad read: got %q", buf[0:n])
    }
    <-c
    r.Close()
    w.Close()
}

// Test a sequence of read/write pairs.
func TestPipeReport2(t *testing.T) {
    const (
        totLen = uint64(120)
    )
    var (
        c = make(chan int)
        r, w, pr = PipeWithReport(totLen)
        readerTest = func(t *testing.T, r io.Reader, c chan int) {
            var buf = make([]byte, 64)
            for {
                n, err := r.Read(buf)
                if err == io.EOF {
                    c <- 0
                    break
                }
                if err != nil {
                    t.Errorf("read: %v", err)
                }
                c <- n
            }
        }
        reportTest = func(rpt <- chan PipeProgress) {
            var (
                i, acc uint64 = 0, 0

            )
            for p := range rpt {
                if p.TotalSize != totLen {
                    t.Errorf("invalid total size")
                }
                acc += (5 + i * 10); i++
                if p.Received != acc {
                    t.Errorf("invalid received size %v vs %v", p.Received, acc)
                }

            }
        }
    )

    go readerTest(t, r, c)
    go reportTest(pr)
    var buf = make([]byte, 64)
    for i := 0; i < 5; i++ {
        p := buf[0 : 5 + i * 10]
        n, err := w.Write(p)
        if n != len(p) {
            t.Errorf("wrote %d, got %d", len(p), n)
        }
        if err != nil {
            t.Errorf("write: %v", err)
        }
        nn := <-c
        if nn != n {
            t.Errorf("wrote %d, read got %d", n, nn)
        }
    }
    w.Close()
    nn := <-c
    if nn != 0 {
        t.Errorf("final read got %d", nn)
    }
}

func TestPipeReport3(t *testing.T) {
    const (
        totLen uint64 = 128
    )
    var (
        c = make(chan pipeReturn)
        r, w, p = PipeWithReport(totLen)
        wdat = make([]byte, 128)
        rdat = make([]byte, 1024)
        tot = 0

        reportTest = func(rpt <- chan PipeProgress) {
            for p := range rpt {
                if p.TotalSize != totLen {
                    t.Errorf("invalid total size")
                }
                t.Logf("received size %v", p.Received)
            }
        }
    )
    for i := 0; i < len(wdat); i++ {
        wdat[i] = byte(i)
    }
    go writer(w, wdat, c)
    go reportTest(p)

    for n := 1; n <= 256; n *= 2 {
        nn, err := r.Read(rdat[tot : tot+n])
        if err != nil && err != io.EOF {
            t.Fatalf("read: %v", err)
        }

        // only final two reads should be short - 1 byte, then 0
        expect := n
        if n == 128 {
            expect = 1
        } else if n == 256 {
            expect = 0
            if err != io.EOF {
                t.Fatalf("read at end: %v", err)
            }
        }
        if nn != expect {
            t.Fatalf("read %d, expected %d, got %d", n, expect, nn)
        }
        tot += nn
    }
    pr := <-c
    if pr.n != 128 || pr.err != nil {
        t.Fatalf("write 128: %d, %v", pr.n, pr.err)
    }
    if tot != 128 {
        t.Fatalf("total read %d != 128", tot)
    }
    for i := 0; i < 128; i++ {
        if rdat[i] != byte(i) {
            t.Fatalf("rdat[%d] = %d", i, rdat[i])
        }
    }
}

func TestPipeReportReadClose(t *testing.T) {
    for _, tt := range pipeTests {
        c := make(chan int, 1)
        r, w, p := PipeWithReport(10)
        go func() {
            pr := <- p
            if pr.Received != 0 {
                t.Error("invalid received size")
            }
        }()
        if tt.async {
            go delayClose(t, w, c, tt)
        } else {
            delayClose(t, w, c, tt)
        }
        var buf = make([]byte, 64)
        n, err := r.Read(buf)
        <-c
        want := tt.err
        if want == nil {
            want = io.EOF
        }
        if err != want {
            t.Errorf("read from closed pipe: %v want %v", err, want)
        }
        if n != 0 {
            t.Errorf("read on closed pipe returned %d", n)
        }
        if err = r.Close(); err != nil {
            t.Errorf("r.Close: %v", err)
        }
    }
}

// Test close on Read side during Read.
func TestPipeReportReadClose2(t *testing.T) {
    c := make(chan int, 1)
    r, _, p := PipeWithReport(64)
    go func() {
       for _ = range p {
           t.Error("shouldn't have triggered at all")
       }
    }()
    go delayClose(t, r, c, pipeTest{})
    n, err := r.Read(make([]byte, 64))
    <-c
    if n != 0 || err != ErrClosedPipe {
        t.Errorf("read from closed pipe: %v, %v want %v, %v", n, err, 0, ErrClosedPipe)
    }
}

// Test write after/before reader close.

func TestPipeReportWriteClose(t *testing.T) {
    for _, tt := range pipeTests {
        c := make(chan int, 1)
        r, w, _ := PipeWithReport(uint64(len("hello, world")))
        if tt.async {
            go delayClose(t, r, c, tt)
        } else {
            delayClose(t, r, c, tt)
        }
        n, err := io.WriteString(w, "hello, world")
        <-c
        expect := tt.err
        if expect == nil {
            expect = ErrClosedPipe
        }
        if err != expect {
            t.Errorf("write on closed pipe: %v want %v", err, expect)
        }
        if n != 0 {
            t.Errorf("write on closed pipe returned %d", n)
        }
        if err = w.Close(); err != nil {
            t.Errorf("w.Close: %v", err)
        }
    }
}

func TestReportWriteEmpty(t *testing.T) {
    r, w, p := PipeWithReport(10)
    go func() {
        w.Write([]byte{})
        w.Close()
    }()
    go func() {
        pr := <- p
        if pr.Received != 0 {
            t.Error("invalid received size")
        }
    }()
    var b [2]byte
    io.ReadFull(r, b[0:2])
    r.Close()
}

func TestReportWriteNil(t *testing.T) {
    r, w, p := PipeWithReport(10)
    go func() {
        w.Write(nil)
        w.Close()
    }()
    go func() {
        pr := <- p
        if pr.Received != 0 {
            t.Error("invalid received size")
        }
    }()
    var b [2]byte
    io.ReadFull(r, b[0:2])
    r.Close()
}

func TestReportWriteAfterWriterClose(t *testing.T) {
    var (
        hello = []byte("hello")
        hLen = uint64(len(hello))
    )
    r, w, p := PipeWithReport(hLen)

    done := make(chan bool)
    var writeErr error
    go func() {
        _, err := w.Write(hello)
        if err != nil {
            t.Errorf("got error: %q; expected none", err)
        }
        w.Close()
        _, writeErr = w.Write([]byte("world"))
        done <- true
    }()
    go func() {
        pr := <- p
        if pr.TotalSize != hLen {
            t.Error("invalid total size")
        }
        if pr.Received != hLen {
            t.Error("invalid received size")
        }
    }()

    buf := make([]byte, 100)
    var result string
    n, err := io.ReadFull(r, buf)
    if err != nil && err != io.ErrUnexpectedEOF {
        t.Fatalf("got: %q; want: %q", err, io.ErrUnexpectedEOF)
    }
    result = string(buf[0:n])
    <-done

    if result != "hello" {
        t.Errorf("got: %q; want: %q", result, "hello")
    }
    if writeErr != ErrClosedPipe {
        t.Errorf("got: %q; want: %q", writeErr, ErrClosedPipe)
    }
}
