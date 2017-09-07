package index

import (
    "bytes"
    "testing"

    "github.com/Redundancy/go-sync/chunks"
)

func TestFindSeqChksumInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 0xF01, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
            {ChunkOffset: 2, WeakChecksum: WEAK_A, StrongChecksum: []byte("e")},
        },
    )

    result, err := i.FindChecksumWithBlockID(1)
    if err != nil {
        t.Error(err.Error())
    }
    if result.ChunkOffset != 1 {
        t.Errorf("Found chunk had offset %v expected 1", result.ChunkOffset)
    }
}

func TestSeqChksumNotInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("a")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("b")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 0xF02, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    result, err := i.FindChecksumWithBlockID(3)
    if err == nil {
        t.Error("error from FindChecksumWithBlockID should not be nil")
    }
    if result != nil {
        t.Error("Result from FindChecksumWithBlockID should be nil")
    }
    result, err = i.FindChecksumWithBlockID(0xA02)
    if err == nil {
        t.Error("error from FindChecksumWithBlockID should not be nil")
    }
    if result != nil {
        t.Error("Result from FindChecksumWithBlockID should be nil")
    }
}

func TestSeqChksumDuplicatedIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("a")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("b")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )

    result, err := i.FindChecksumWithBlockID(2)
    if err != nil {
        t.Error(err.Error())
    }
    if result.ChunkOffset != 2 {
        t.Errorf("Found chunk had offset %v expected 1", result.ChunkOffset)
    }
    if bytes.Compare(result.StrongChecksum, []byte("c")) != 0 {
        t.Errorf("Preceeding strong checksums should be found: %v", result.StrongChecksum)
    }
}


func TestSeqChksumStrongInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
            {ChunkOffset: 0xF02, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
        },
    )

    result, err := i.FindChecksumWithBlockID(1)
    if err != nil {
        t.Error(err.Error())
    }
    if result.ChunkOffset != 1 {
        t.Errorf("Found chunk had offset %v expected 1", result.ChunkOffset)
    }
    if bytes.Compare(result.StrongChecksum, []byte("c")) != 0 {
        t.Errorf("Mismatching strong checksums found: %v", result.StrongChecksum)
    }
}

func TestSeqChksumNotFoundStrongInIndexAtEnd(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: WEAK_A, StrongChecksum: []byte("b")},
            {ChunkOffset: 1, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("d")},
        },
    )
    result, err := i.FindChecksumWithBlockID(2)
    if err != nil {
        t.Error(err.Error())
    }
    if result.ChunkOffset != 2 {
        t.Errorf("Found chunk had offset %v expected 2", result.ChunkOffset)
    }
    if bytes.Compare(result.StrongChecksum, []byte("d")) != 0 {
        t.Errorf("Mismatching strong checksums found: %v", result.StrongChecksum)
    }
}

func TestSeqChksumNotFoundStrongInIndexInCenter(t *testing.T) {
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

    result, err := i.FindChecksumWithBlockID(1)
    if err != nil {
        t.Error(err.Error())
    }
    if result.ChunkOffset != 1 {
        t.Errorf("Found chunk had offset %v expected 1", result.ChunkOffset)
    }
}

func TestSeqChksumFindTwoDuplicatedBlocksInIndex(t *testing.T) {
    i := MakeChecksumIndex(
        []chunks.ChunkChecksum{
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
            {ChunkOffset: 2, WeakChecksum: WEAK_B, StrongChecksum: []byte("c")},
        },
    )

    result, err := i.FindChecksumWithBlockID(2)
    if err != nil {
        t.Error(err.Error())
    }
    if result.ChunkOffset != 2 {
        t.Errorf("Found chunk had offset %v expected 2", result.ChunkOffset)
    }
    if bytes.Compare(result.StrongChecksum, []byte("c")) != 0 {
        t.Errorf("Mismatching strong checksums found: %v", result.StrongChecksum)
    }
}
