package index

import (
    "encoding/binary"
    "reflect"
    "testing"

    "github.com/Redundancy/go-sync/chunks"
)

// Weak checksums must be 8 bytes
var WEAK_A = []byte("aaaaaaaa")
var WEAK_B = []byte("bbbbbbbb")

/*
ChunkOffset uint
// the size of the block
Size           int64
WeakChecksum   []byte
StrongChecksum []byte
*/

func TestMakeIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
        },
    )

    if i.Count != 2 {
        t.Fatalf("Wrong count on index %v", i.Count)
    }
}

func TestFindWeakInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    result := i.FindWeakChecksumInIndex(WEAK_B)

    if result == nil {
        t.Error("Did not find lookfor in the index")
    } else if len(result) != 2 {
        t.Errorf("Wrong number of possible matches found: %v", len(result))
    } else if result[0].ChunkOffset != 1 {
        t.Errorf("Found chunk had offset %v expected 1", result[0].ChunkOffset)
    }
}

func TestWeakNotInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    result := i.FindWeakChecksumInIndex([]byte("afghqocq"))

    if result != nil {
        t.Error("Result from FindWeakChecksumInIndex should be nil")
    }

    result2 := i.FindWeakChecksum2([]byte("afghqocq"))

    if result2 != nil {
        t.Errorf("Result from FindWeakChecksum2 should be nil: %#v", result2)
    }
}

func TestWeakNotInIndex2(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    result := i.FindWeakChecksumInIndex([]byte("llllllll"))

    if result != nil {
        t.Error("Result should be nil")
    }
}

func TestFindStrongInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    // builds upon TestFindWeakInIndex
    result := i.FindWeakChecksumInIndex(WEAK_B)
    strongs := result.FindStrongChecksum([]byte("c"))

    if len(strongs) != 1 {
        t.Errorf("Incorrect number of strong checksums found: %v", len(strongs))
    } else if strongs[0].ChunkOffset != 1 {
        t.Errorf("Wrong chunk found, had offset %v", strongs[0].ChunkOffset)
    }
}

func TestNotFoundStrongInIndexAtEnd(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    // builds upon TestFindWeakInIndex
    result := i.FindWeakChecksumInIndex(WEAK_B)
    strongs := result.FindStrongChecksum([]byte("e"))

    if len(strongs) != 0 {
        t.Errorf("Incorrect number of strong checksums found: %v", strongs)
    }
}

func TestNotFoundStrongInIndexInCenter(t *testing.T) {
    // The strong checksum we're looking for is not found
    // but is < another checksum in the strong list

    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
            {ChunkOffset: 3, WeakChecksum: WEAK_B, StrongChecksum: []byte("f")},
        },
    )

    // builds upon TestFindWeakInIndex
    result := i.FindWeakChecksumInIndex(WEAK_B)
    strongs := result.FindStrongChecksum([]byte("e"))

    if len(strongs) != 0 {
        t.Errorf("Incorrect number of strong checksums found: %v", strongs)
    }
}

func TestFindDuplicatedBlocksInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 3, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    // builds upon TestFindWeakInIndex
    result := i.FindWeakChecksumInIndex(WEAK_B)
    strongs := result.FindStrongChecksum([]byte("c"))

    if len(strongs) != 2 {
        t.Fatalf("Incorrect number of strong checksums found: %v", strongs)
    }

    first := strongs[0]
    if first.ChunkOffset != 1 {
        t.Errorf("Wrong chunk found, had offset %v", first.ChunkOffset)
    }

    second := strongs[1]
    if second.ChunkOffset != 3 {
        t.Errorf("Wrong chunk found, had offset %v", second.ChunkOffset)
    }
}

func TestFindTwoDuplicatedBlocksInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
        },
    )

    // builds upon TestFindWeakInIndex
    result := i.FindWeakChecksumInIndex(WEAK_B)
    strongs := result.FindStrongChecksum([]byte("c"))

    if len(strongs) != 2 {
        t.Fatalf("Incorrect number of strong checksums found: %v", strongs)
    }

    first := strongs[0]
    if first.ChunkOffset != 1 {
        t.Errorf("Wrong chunk found, had offset %v", first.ChunkOffset)
    }

    second := strongs[1]
    if second.ChunkOffset != 2 {
        t.Errorf("Wrong chunk found, had offset %v", second.ChunkOffset)
    }
}

func TestSequentialBlocksInIndex(t *testing.T) {
    var (
        strong_checksums = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        checksums        = []chunks.ChunkChecksum{}
    )
    for i := len(strong_checksums) - 1; 0 <= i; i-- {
        weakSum := make([]byte, 8, 8)
        binary.LittleEndian.PutUint64(weakSum, uint64(i))

        checksums = append(
            checksums,
            chunks.ChunkChecksum{
                ChunkOffset:    uint(i),
                WeakChecksum:   weakSum,
                StrongChecksum: []byte{strong_checksums[i]},
            })
    }
    ci := MakeChecksumIndex(checksums)

    // check weak checksum map
    for i := 0; i < len(strong_checksums); i++ {
        weakSum := make([]byte, 8, 8)
        binary.LittleEndian.PutUint64(weakSum, uint64(i))

        result := ci.FindWeakChecksumInIndex(weakSum)
        strongs := result.FindStrongChecksum([]byte{strong_checksums[i]})

        if len(strongs) != 1 {
            t.Fatalf("Incorrect number of strong checksums found: %v", strongs)
        }
        if !reflect.DeepEqual(strongs[0].StrongChecksum, []byte{strong_checksums[i]}) {
            t.Fatalf("Incorrect strong checksums found: %v", strongs)
        }
        if ci.SequentialChecksumList()[i].ChunkOffset != uint(i) {
            t.Fatalf("Incorrect checksums index found: %v", strongs)
        }
        if !reflect.DeepEqual(ci.SequentialChecksumList()[i].WeakChecksum, weakSum) {
            t.Fatalf("Incorrect strong checksums found: %v", strongs)
        }
        if !reflect.DeepEqual(ci.SequentialChecksumList()[i].StrongChecksum, []byte{strong_checksums[i]}) {
            t.Fatalf("Incorrect strong checksums found: %v", strongs)
        }
    }
}

func TestPartialSequentialBlocksInIndex(t *testing.T) {
    var (
        ci = NewChecksumIndex()
        strong_checksums = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        appendChecksums = func(csindex *ChecksumIndex, start, end int) {
            checksums := []chunks.ChunkChecksum{}
            for i := end; start <= i; i-- {
                weakSum := make([]byte, 8, 8)
                binary.LittleEndian.PutUint64(weakSum, uint64(i))

                checksums = append(
                    checksums,
                    chunks.ChunkChecksum{
                        ChunkOffset:    uint(i),
                        WeakChecksum:   weakSum,
                        StrongChecksum: []byte{strong_checksums[i]},
                    })
            }
            csindex.AppendChecksums(checksums)
        }
    )
    appendChecksums(ci, 0, 4)
    appendChecksums(ci, 5, 9)
    appendChecksums(ci, 10, 14)
    appendChecksums(ci, 15, 19)
    appendChecksums(ci, 20, len(strong_checksums) - 1)

    // check weak checksum map
    for i := 0; i < len(strong_checksums); i++ {
        weakSum := make([]byte, 8, 8)
        binary.LittleEndian.PutUint64(weakSum, uint64(i))

        result := ci.FindWeakChecksumInIndex(weakSum)
        strongs := result.FindStrongChecksum([]byte{strong_checksums[i]})

        if len(strongs) != 1 {
            t.Fatalf("Incorrect number of strong checksums found: %v", strongs)
        }
        if !reflect.DeepEqual(strongs[0].StrongChecksum, []byte{strong_checksums[i]}) {
            t.Fatalf("Incorrect strong checksums found: %v", strongs)
        }
        if ci.SequentialChecksumList()[i].ChunkOffset != uint(i) {
            t.Fatalf("Incorrect checksums index found: %v", strongs)
        }
        if !reflect.DeepEqual(ci.SequentialChecksumList()[i].WeakChecksum, weakSum) {
            t.Fatalf("Incorrect strong checksums found: %v", strongs)
        }
        if !reflect.DeepEqual(ci.SequentialChecksumList()[i].StrongChecksum, []byte{strong_checksums[i]}) {
            t.Fatalf("Incorrect strong checksums found: %v", strongs)
        }
    }
}
