package rollsum

import (
    "bytes"
    "hash"
    "io"
    "testing"

    . "gopkg.in/check.v1"
    "github.com/Redundancy/go-sync/circularbuffer"
)

func TestRollSum64(t *testing.T) { TestingT(t) }

type Rollsum64Suite struct {
}

var _ = Suite(&Rollsum64Suite{})

func (s *Rollsum64Suite) SetUpSuite(c *C) {
}

func (s *Rollsum64Suite) TearDownSuite(c *C) {
}

func (s *Rollsum64Suite) SetUpTest(c *C) {
    c.Log("--- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---")
}

func (s *Rollsum64Suite) TearDownTest(c *C) {
    c.Log("\n\n")
}

/// ---

func (s *Rollsum64Suite) Test_SatisfiesHashInterface(c *C) {
    var i hash.Hash = NewRollsum64(1024)
    i.Reset()
}

func (s *Rollsum64Suite) Test_SatisfiedWriterInterface(c *C) {
    var i io.Writer = NewRollsum64(1024)
    n, err := i.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8})
    c.Assert(err, IsNil)
    c.Assert(n, Equals, 8)
}

func (s *Rollsum64Suite) Test_IsTheSameAfterBlockSizeBytes(c *C) {
    r1 := NewRollsum64(8)
    r2 := NewRollsum64(8)

    r1.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8})

    r2.Write([]byte{7, 6, 12, 5, 21})
    r2.Write([]byte{9, 3, 5, 1, 2, 3, 4})
    r2.Write([]byte{5, 6, 7, 8})

    sum1 := r1.Sum(nil)
    sum2 := r2.Sum(nil)

    c.Assert(bytes.Compare(sum1, sum2), Equals, 0)
}

func (s *Rollsum64Suite) Test_Regression2(c *C) {
    const A = "The quick br"
    const B = "The qwik br"

    r1 := NewRollsum64(4)
    r2 := NewRollsum64(4)

    r1.Write([]byte(A[:4]))
    r1.Reset()
    r1.Write([]byte(A[4:8]))
    r1.Reset()
    r1.Write([]byte(A[8:12]))

    r2.Write([]byte(B[:4]))
    r2.Write([]byte(B[4:8]))
    for _, c := range B[8:] {
        r2.Write([]byte{byte(c)})
    }

    sum1 := r1.Sum(nil)
    sum2 := r2.Sum(nil)

    c.Assert(bytes.Compare(sum1, sum2), Equals, 0)
}

func (s *Rollsum64Suite) Test_RemovesBytesCorrectly(c *C) {
    r1 := NewRollsum64Base(2)

    r1.AddByte(255)
    r1.AddByte(10)
    r1.RemoveByte(255, 2)
    r1.AddByte(0)
    r1.RemoveByte(10, 2)
    r1.AddByte(0)

    c.Assert(r1.a, Equals, uint64(0))
    c.Assert(r1.b, Equals, uint64(0))
}


func (s *Rollsum64Suite) Test_IsDifferentForDifferentInput(c *C) {
    r1 := NewRollsum64(8)
    r2 := NewRollsum64(8)

    r1.Write([]byte{1, 2, 3, 4, 5, 6, 7, 8})
    r2.Write([]byte{13, 11, 9, 8, 7, 6, 5, 1})

    sum1 := r1.Sum(nil)
    sum2 := r2.Sum(nil)

    c.Assert(bytes.Compare(sum1, sum2), Not(Equals), 0)
}


func (s *Rollsum64Suite) Test_Resetting(c *C) {
    r1 := NewRollsum64(8)
    r2 := NewRollsum64(8)

    r1.Write([]byte{1, 2, 3, 4, 5, 6, 7})

    r2.Write([]byte{7, 6, 5, 3, 2})
    r2.Reset()
    r2.Write([]byte{1, 2, 3, 4, 5, 6, 7})

    sum1 := r1.Sum(nil)
    sum2 := r2.Sum(nil)

    c.Assert(bytes.Compare(sum1, sum2), Equals, 0)
}

func (s *Rollsum64Suite) Test_TruncatingPartiallyFilledBufferResultsInSameState(c *C) {
    r1 := NewRollsum64Base(8)
    r2 := NewRollsum64Base(8)

    r1.AddByte(2)
    sum1 := make([]byte, 8)
    r1.GetSum(sum1)

    r2.AddByte(1)
    r2.AddByte(2)

    // Removal works from the left
    r2.RemoveByte(1, 2)
    sum2 := make([]byte, 8)
    r2.GetSum(sum2)

    c.Assert(bytes.Compare(sum1, sum2), Equals, 0)
}

func (s *Rollsum64Suite) Test_64SumDoesNotChangeTheHashState(c *C) {
    r1 := NewRollsum64(8)

    sum1 := r1.Sum([]byte{1, 2, 3, 4, 5, 6, 7})
    sum2 := r1.Sum([]byte{7, 8, 9, 0, 1, 2, 3})

    c.Assert(bytes.Compare(sum1[7:], sum2[7:]), Equals, 0)
}

func (s *Rollsum64Suite) Test_64OutputLengthMatchesSize(c *C) {
    r1 := NewRollsum64(8)
    sumLength := len(r1.Sum(nil))

    c.Assert(sumLength, Equals, r1.Size())
}

func BenchmarkRollsum64(b *testing.B) {
    r := NewRollsum64(100)
    buffer := make([]byte, 100)
    b.ReportAllocs()
    b.SetBytes(int64(len(buffer)))
    checksum := make([]byte, 16)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Write(buffer)
        r.Sum(checksum)
        checksum = checksum[:0]
    }
    b.StopTimer()
}

func BenchmarkRollsum64_8096(b *testing.B) {
    r := NewRollsum64(8096)
    buffer := make([]byte, 8096)
    b.ReportAllocs()
    b.SetBytes(int64(len(buffer)))
    checksum := make([]byte, 16)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Write(buffer)
        r.Sum(checksum)
        checksum = checksum[:0]
    }
    b.StopTimer()
}

func BenchmarkRollsum64_16192(b *testing.B) {
    r := NewRollsum64(16192)
    buffer := make([]byte, 16192)
    b.ReportAllocs()
    b.SetBytes(int64(len(buffer)))
    checksum := make([]byte, 32)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Write(buffer)
        r.Sum(checksum)
        checksum = checksum[:0]
    }
    b.StopTimer()
}

func BenchmarkRollsum64Base(b *testing.B) {
    r := Rollsum64Base{blockSize: 100}
    buffer := make([]byte, 100)
    checksum := make([]byte, 16)
    b.ReportAllocs()
    b.SetBytes(int64(len(buffer)))

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.SetBlock(buffer)
        r.GetSum(checksum)
    }
    b.StopTimer()

}

// This is the benchmark where Rollsum should beat a full RIPEMD-160 for each blocksize
func BenchmarkIncrementalRollsum64(b *testing.B) {
    r := NewRollsum64(100)
    buffer := make([]byte, 100)
    r.Write(buffer)
    b.SetBytes(1)

    b.ReportAllocs()
    checksum := make([]byte, 16)
    increment := make([]byte, 1)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        r.Write(increment)
        r.Sum(checksum)
        checksum = checksum[:0]
    }
    b.StopTimer()
}

// The C2 veersion should avoid all allocations in the main loop, and beat the pants off the
// other versions
func BenchmarkIncrementalRollsum64WithC2(b *testing.B) {
    const BLOCK_SIZE = 100
    r := NewRollsum64Base(BLOCK_SIZE)
    buffer := make([]byte, BLOCK_SIZE)
    b.SetBytes(1)
    cbuffer := circularbuffer.MakeC2Buffer(BLOCK_SIZE)

    r.AddBytes(buffer)
    cbuffer.Write(buffer)

    b.ReportAllocs()
    checksum := make([]byte, 16)
    increment := make([]byte, 1)

    b.StartTimer()
    for i := 0; i < b.N; i++ {
        cbuffer.Write(increment)
        r.AddAndRemoveBytes(increment, cbuffer.Evicted(), BLOCK_SIZE)
        r.GetSum(checksum)
    }
    b.StopTimer()
}

/*
    (2017/08/26) bench between uint32 vs uint64
    OK: 10 passed
    BenchmarkRollsum32-8                    	10000000	       224 ns/op	 444.94 MB/s	       0 B/op	       0 allocs/op
    BenchmarkRollsum32_8096-8               	  100000	     16025 ns/op	 505.19 MB/s	       0 B/op	       0 allocs/op
    BenchmarkRollsum32Base-8                	10000000	       192 ns/op	 520.02 MB/s	       0 B/op	       0 allocs/op
    BenchmarkIncrementalRollsum32-8         	50000000	        35.5 ns/op	  28.14 MB/s	       0 B/op	       0 allocs/op
    BenchmarkIncrementalRollsum32WithC2-8   	50000000	        31.5 ns/op	  31.71 MB/s	       0 B/op	       0 allocs/op

    BenchmarkRollsum64-8                    	10000000	       235 ns/op	 423.85 MB/s	       0 B/op	       0 allocs/op
    BenchmarkRollsum64_8096-8               	  100000	     17049 ns/op	 474.84 MB/s	       0 B/op	       0 allocs/op
    BenchmarkRollsum64_16192-8              	   50000	     34881 ns/op	 464.20 MB/s	       1 B/op	       0 allocs/op
    BenchmarkRollsum64Base-8                	10000000	       205 ns/op	 485.58 MB/s	       0 B/op	       0 allocs/op
    BenchmarkIncrementalRollsum64-8         	50000000	        34.6 ns/op	  28.92 MB/s	       0 B/op	       0 allocs/op
    BenchmarkIncrementalRollsum64WithC2-8   	50000000	        29.5 ns/op	  33.89 MB/s	       0 B/op	       0 allocs/op

    PASS
    ok  	github.com/Redundancy/go-sync/rollsum	21.894s
 */