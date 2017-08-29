package rollsum

import (
    "encoding/binary"
)

const (
    full_bytes_32 = (1 << 32) - 1
    rolsum64_size = 8
)

// Rollsum64Base decouples the rollsum algorithm from the implementation of hash.Hash and the storage the rolling
// checksum window. this allows us to write different versions of the storage for the distinctly different use-cases and
// optimize the storage with the usage pattern.
func NewRollsum64Base(blockSize uint) *Rollsum64Base {
    return &Rollsum64Base{blockSize: blockSize}
}

// The specification of hash.Hash is such that it cannot be implemented without implementing storage but the most
// optimal storage scheme depends on usage of the circular buffer & hash
type Rollsum64Base struct {
    blockSize uint
    a, b      uint64
}

// Add a single byte into the rollsum
func (r *Rollsum64Base) AddByte(b byte) {
    r.a += uint64(b)
    r.b += r.a
}

func (r *Rollsum64Base) AddBytes(bs []byte) {
    for _, b := range bs {
        r.a += uint64(b)
        r.b += r.a
    }
}

// Remove a byte from the end of the rollsum
// Use the previous length (before removal)
func (r *Rollsum64Base) RemoveByte(b byte, length int) {
    r.a -= uint64(b)
    r.b -= uint64(uint(length) * uint(b))
}

func (r *Rollsum64Base) RemoveBytes(bs []byte, length int) {
    for _, b := range bs {
        r.a -= uint64(b)
        r.b -= uint64(uint(length) * uint(b))
        length -= 1
    }
}

func (r *Rollsum64Base) AddAndRemoveBytes(add []byte, remove []byte, length int) {
    var (
        len_added = len(add)
        len_removed = len(remove)
        startEvicted = len_added - len_removed
    )

    r.AddBytes(add[:startEvicted])
    length += startEvicted

    for i := startEvicted; i < len_added; i++ {
        r.RemoveByte(remove[i-startEvicted], length)
        r.AddByte(add[i])
    }
}

// Set a whole block of blockSize
func (r *Rollsum64Base) SetBlock(block []byte) {
    r.Reset()
    r.AddBytes(block)
}

// Reset the hash to the initial state
func (r *Rollsum64Base) Reset() {
    r.a, r.b = 0, 0
}

// size of the hash in bytes
func (r *Rollsum64Base) Size() int {
    return rolsum64_size
}

// Puts the sum into b. Avoids allocation. b must have length >= 8
func (r *Rollsum64Base) GetSum(b []byte) {
    value := uint64((r.a & full_bytes_32) + ((r.b & full_bytes_32) << 32))
    binary.LittleEndian.PutUint64(b, value)
}
