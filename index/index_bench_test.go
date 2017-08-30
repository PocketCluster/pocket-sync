package index

import (
    "math/rand"
    "sort"
    "testing"

    "github.com/Redundancy/go-sync/chunks"
)

var T = []byte{1, 2, 3, 4, 5, 6, 7, 8}

func BenchmarkIndex1024(b *testing.B) {
    i := ChecksumIndex{}
    i.weakChecksumLookup = make([]map[uint64]chunks.StrongChecksumList, indexLookupMapSize)

    for x := 0; x < 1024; x++ {
        w := uint64(rand.Int63())

        if i.weakChecksumLookup[w & indexOffsetFilter] == nil {
            i.weakChecksumLookup[w & indexOffsetFilter] = make(map[uint64]chunks.StrongChecksumList)
        }

        i.weakChecksumLookup[w & indexOffsetFilter][w] = append(
            i.weakChecksumLookup[w & indexOffsetFilter][w],
            chunks.ChunkChecksum{},
        )
    }

    b.SetBytes(1)
    b.StartTimer()
    for x := 0; x < b.N; x++ {
        i.FindWeakChecksum2(T)
    }
    b.StopTimer()
}

func BenchmarkIndex8192(b *testing.B) {
    i := ChecksumIndex{}
    i.weakChecksumLookup = make([]map[uint64]chunks.StrongChecksumList, indexLookupMapSize)

    for x := 0; x < 8192; x++ {
        w := uint64(rand.Int63())

        if i.weakChecksumLookup[w & indexOffsetFilter] == nil {
            i.weakChecksumLookup[w & indexOffsetFilter] = make(map[uint64]chunks.StrongChecksumList)
        }

        i.weakChecksumLookup[w & indexOffsetFilter][w] = append(
            i.weakChecksumLookup[w & indexOffsetFilter][w],
            chunks.ChunkChecksum{},
        )
    }

    b.SetBytes(1)
    b.StartTimer()
    for x := 0; x < b.N; x++ {
        i.FindWeakChecksum2(T)
    }
    b.StopTimer()
}

// Check how fast a sorted list of 8192 items would be
func BenchmarkIndexAsListBinarySearch8192(b *testing.B) {
    b.SkipNow()

    s := make([]int, 8192)
    for x := 0; x < 8192; x++ {
        s[x] = rand.Int()
    }

    sort.Ints(s)

    b.StartTimer()
    for x := 0; x < b.N; x++ {
        sort.SearchInts(s, rand.Int())
    }
    b.StopTimer()
}

// Check how fast a sorted list of 8192 items would be
// Checking for cache coherency gains
func BenchmarkIndexAsListLinearSearch8192(b *testing.B) {
    s := make([]int, 8192)
    for x := 0; x < 8192; x++ {
        s[x] = rand.Int()
    }

    sort.Ints(s)

    l := len(s)
    b.StartTimer()
    for x := 0; x < b.N; x++ {
        v := rand.Int()
        for i := 0; i < l; i++ {
            if v < s[i] {
                break
            }
        }
    }
    b.StopTimer()
}

func Benchmark_256SplitBinarySearch(b *testing.B) {
    a := make([][]int, indexLookupMapSize)
    for x := 0; x < 8192; x++ {
        i := rand.Int()
        a[i & 0xFF] = append(
            a[i & 0xFF],
            i,
        )
    }

    for x := 0; x < indexLookupMapSize; x++ {
        sort.Ints(a[x])
    }

    b.StartTimer()
    for x := 0; x < b.N; x++ {
        v := rand.Int()
        sort.SearchInts(a[v & 0xFF], v)
    }
    b.StopTimer()
}

// This is currently the best performing contender for the index data structure for weak checksum lookups.
func Benchmark_256Split_Map(b *testing.B) {
    a := make([]map[int]interface{}, indexLookupMapSize)
    for x := 0; x < 8192; x++ {
        i := rand.Int()
        if a[i & 0xFF] == nil {
            a[i & 0xFF] = make(map[int]interface{})
        }
        a[i & 0xFF][i] = nil
    }

    b.StartTimer()
    for x := 0; x < b.N; x++ {
        v := rand.Int()
        if _, ok := a[v & 0xFF][v]; ok {

        }
    }
    b.StopTimer()
}

/*
    (2017/08/26) bench result between uint32 vs uint64

    BenchmarkIndex1024-8                        100000000           12.8 ns/op    77.93 MB/s
    BenchmarkIndex8192-8                        100000000           21.2 ns/op    47.17 MB/s
    BenchmarkIndexAsListLinearSearch8192-8       1000000          1511 ns/op
    Benchmark_256SplitBinarySearch-8            20000000            81.1 ns/op
    Benchmark_256Split_Map-8                    30000000            51.9 ns/op
    ok      github.com/Redundancy/go-sync/index 8.311s

    BenchmarkIndex1024-8                     	100000000	        11.3 ns/op	  88.72 MB/s
    BenchmarkIndex8192-8                     	50000000	        27.9 ns/op	  35.81 MB/s
    BenchmarkIndexAsListLinearSearch8192-8   	 1000000	      1619 ns/op
    Benchmark_256SplitBinarySearch-8         	20000000	        79.6 ns/op
    Benchmark_256Split_Map-8                 	30000000	        53.5 ns/op
    PASS
    ok  	github.com/Redundancy/go-sync/index	7.567s

 */