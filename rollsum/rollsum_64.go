/*
 * rollsum provides an implementation of a rolling checksum - a checksum that's efficient to advance a byte or more at a
 * time. It is inspired by the rollsum in rsync, but differs in that the internal values used are 64bit integers - to
 * accommodate the need of handling large (< 1GB) file transmission and sync.
 * To make a conformant implementation, a find a replace on "64" should be almost sufficient
 * (although it would be highly recommended to test against known values from the original implementation).
 *
 * Rollsum64 supports the hash.Hash implementation, but is not used much in go-sync, mostly in order to share and access
 * the underlying circular buffer storage, and use the implementation as efficiently as possible.
 */
package rollsum

import (
    "github.com/Redundancy/go-sync/circularbuffer"
)

func NewRollsum64(blocksize uint) *Rollsum64 {
    // TODO block size alignment
    return &Rollsum64{
        Rollsum64Base: Rollsum64Base{
            blockSize: blocksize,
        },
        buffer: circularbuffer.MakeC2Buffer(int(blocksize)),
    }
}

// Rollsum64 is a rolling checksum implemenation inspired by rsync, but with 64bit internal values
// Create one using NewRollsum64
type Rollsum64 struct {
    Rollsum64Base
    buffer *circularbuffer.C2
}

// cannot be called concurrently
func (r *Rollsum64) Write(p []byte) (n int, err error) {
    var (
        ulen_p = uint(len(p))
    )

    if ulen_p >= r.blockSize {
        // if it's really long, we can just ignore a load of it
        remaining := p[ulen_p - r.blockSize:]
        r.buffer.Write(remaining)
        r.Rollsum64Base.SetBlock(remaining)
    } else {
        b_len := r.buffer.Len()
        r.buffer.Write(p)
        evicted := r.buffer.Evicted()
        r.Rollsum64Base.AddAndRemoveBytes(p, evicted, b_len)
    }

    return len(p), nil
}

// The most efficient byte length to call Write with
func (r *Rollsum64) BlockSize() int {
    return int(r.blockSize)
}

// the number of bytes
func (r *Rollsum64) Size() int {
    return rolsum64_size
}

func (r *Rollsum64) Reset() {
    r.Rollsum64Base.Reset()
    r.buffer.Reset()
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.
// Note that this is to allow Sum() to reuse a preallocated buffer
func (r *Rollsum64) Sum(b []byte) []byte {
    if b != nil && cap(b) - len(b) >= rolsum64_size {
        p := len(b)
        b = b[:len(b) + rolsum64_size]
        r.Rollsum64Base.GetSum(b[p:])
        return b
    } else {
        result := []byte{0, 0, 0, 0, 0, 0, 0, 0}
        r.Rollsum64Base.GetSum(result)
        return append(b, result...)
    }
}

func (r *Rollsum64) GetLastBlock() []byte {
    return r.buffer.GetBlock()
}
