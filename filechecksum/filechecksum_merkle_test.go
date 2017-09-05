package filechecksum

import (
    "bytes"
    "io"
    "testing"

    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/index"
    "github.com/Redundancy/go-sync/util/readers"
)

func Test_BuildSeqRootChecksums_EndsWithFilechecksum(t *testing.T) {
    const (
        BLOCKSIZE = 100
        BLOCK_COUNT = 20
    )
    var (
        emptybuffer = bytes.NewBuffer(make([]byte, BLOCK_COUNT * BLOCKSIZE))
        checksum    = NewFileChecksumGenerator(BLOCKSIZE)
        lastResult  = ChecksumResults{}
    )

    for lastResult = range checksum.startBuildChecksum(emptybuffer, 10) {
    }

    if lastResult.Checksums != nil {
        t.Errorf("Last result had checksums: %#v", lastResult)
    }

    if lastResult.Filechecksum == nil {
        t.Errorf("Last result did not contain the filechecksum: %#v", lastResult)
    } else {
        t.Logf("FileChecksum %v", lastResult.Filechecksum)
    }
}

func Test_BuildSeqRootChecksums_ReturnsCorrectChecksumCount(t *testing.T) {
    const(
        BLOCKSIZE = 100
        BLOCK_COUNT = 20
    )
    var (
        emptybuffer = bytes.NewBuffer(make([]byte, BLOCK_COUNT * BLOCKSIZE))
        checksum    = NewFileChecksumGenerator(BLOCKSIZE)
        resultCount = 0
    )

    for r := range checksum.startBuildChecksum(emptybuffer, 10) {
        resultCount += len(r.Checksums)
    }

    if resultCount != BLOCK_COUNT {
        t.Errorf("Unexpected block count returned: %v", resultCount)
    }
}

func Test_BuildSeqRootChecksums_ContainsHashes(t *testing.T) {
    const (
        BLOCKSIZE = 100
        BLOCK_COUNT = 20
    )
    var (
        emptybuffer = bytes.NewBuffer(make([]byte, BLOCK_COUNT * BLOCKSIZE))
        checksum    = NewFileChecksumGenerator(BLOCKSIZE)
    )

    for r := range checksum.startBuildChecksum(emptybuffer, 10) {
        for _, r2 := range r.Checksums {
            if len(r2.WeakChecksum) != checksum.GetWeakRollingHash().Size() {
                t.Fatalf(
                    "Wrong length weak checksum: %v vs %v",
                    len(r2.WeakChecksum),
                    checksum.GetWeakRollingHash().Size(),
                )
            }

            if len(r2.StrongChecksum) != checksum.GetStrongHash().Size() {
                t.Fatalf(
                    "Wrong length strong checksum: %v vs %v",
                    len(r2.StrongChecksum),
                    checksum.GetStrongHash().Size(),
                )
            }

        }
    }
}

func Test_BuildSeqRootChecksums_ExpectedLength(t *testing.T) {
    const (
        BLOCKSIZE = 100
        BLOCK_COUNT = 20
    )
    var (
        emptybuffer = bytes.NewBuffer(make([]byte, BLOCK_COUNT * BLOCKSIZE))
        output      = bytes.NewBuffer(nil)
        checksum    = NewFileChecksumGenerator(BLOCKSIZE)

        // output length is expected to be 20 blocks
        expectedLength = (BLOCK_COUNT * checksum.GetStrongHash().Size()) +
                         (BLOCK_COUNT * checksum.GetWeakRollingHash().Size())
    )

    _, count, err := checksum.BuildSequentialAndRootChecksum(emptybuffer, output)
    if err != nil {
        t.Fatal(err)
    }
    if count != BLOCK_COUNT {
        t.Fatalf("invalid sequential checksum size. acquired %v vs expecetd %v", count, BLOCK_COUNT)
    }

    if output.Len() != expectedLength {
        t.Errorf(
            "output length (%v) did not match expected length (%v)",
            output.Len(),
            expectedLength,
        )
    }
}

func Test_BuildSeqRootChecksums_ExpectedLengthWithPartialBlockAtEnd(t *testing.T) {
    const (
        BLOCKSIZE = 100
        FULL_BLOCK_COUNT = 20
        BLOCK_COUNT = FULL_BLOCK_COUNT + 1
    )
    var (
        emptybuffer = bytes.NewBuffer(make([]byte, FULL_BLOCK_COUNT*BLOCKSIZE+50))
        output      = bytes.NewBuffer(nil)
        checksum    = NewFileChecksumGenerator(BLOCKSIZE)

        // output length is expected to be 20 blocks
        expectedLength = (BLOCK_COUNT * checksum.GetStrongHash().Size()) +
                         (BLOCK_COUNT * checksum.GetWeakRollingHash().Size())
    )

    _, count, err := checksum.BuildSequentialAndRootChecksum(emptybuffer, output)
    if err != nil {
        t.Fatal(err)
    }
    if count != BLOCK_COUNT {
        t.Fatalf("invalid sequential checksum size. acquired %v vs expecetd %v", count, BLOCK_COUNT)
    }

    if output.Len() != expectedLength {
        t.Errorf(
            "output length (%v) did not match expected length (%v)",
            output.Len(),
            expectedLength,
        )
    }
}

// Each of the data blocks is the same, so the checksums for the blocks should be the same
func Test_BuildSeqRootChecksums_BlocksTheSame(t *testing.T) {
    const (
        BLOCKSIZE = 100
        BLOCK_COUNT = 20
    )
    var (
        checksum = NewFileChecksumGenerator(BLOCKSIZE)
        output = bytes.NewBuffer(nil)
    )

    orcs, count, err := checksum.BuildSequentialAndRootChecksum(
        readers.OneReader(BLOCKSIZE*BLOCK_COUNT),
        output,
    )
    if err != nil {
        t.Fatal(err)
    }
    if count != BLOCK_COUNT {
        t.Fatalf("invalid sequential checksum size. acquired %v vs expecetd %v", count, BLOCK_COUNT)
    }

    weakSize, strongSize := checksum.GetChecksumSizes()

    if output.Len() != BLOCK_COUNT*(strongSize+weakSize) {
        t.Errorf(
            "Unexpected output length: %v, expected %v",
            output.Len(),
            BLOCK_COUNT*(strongSize+weakSize),
        )
    }

    results, err := chunks.SizedLoadChecksumsFromReader(output, uint(BLOCK_COUNT), weakSize, strongSize)
    if err != nil {
        t.Fatal(err)
    }
    if len(results) != BLOCK_COUNT {
        t.Fatalf("Results too short! %v", len(results))
    }

    // Make an index that we can use against our local checksums
    ircs, err := index.MakeChecksumIndex(results).SequentialChecksumList().RootHash()
    if err != nil {
        t.Fatal(err)
    }
    if bytes.Compare(ircs, orcs) != 0 {
        t.Errorf("loaded root checksum is different from generated checksum")
    }

    first := results[0]
    for i, chk := range results {
        if chk.ChunkOffset != uint(i) {
            t.Errorf("Unexpected offset %v on chunk %v", chk.ChunkOffset, i)
        }
        if !first.Match(chk) {
            t.Fatalf("Chunks have different checksums on %v", i)
        }
    }
}

func Test_BuildSeqRootChecksums_PrependedBlocks(t *testing.T) {
    const (
        BLOCKSIZE = 100
        BLOCK_COUNT = 20
    )
    var (
        checksum = NewFileChecksumGenerator(BLOCKSIZE)

        file1 = io.LimitReader(
            readers.NewNonRepeatingSequence(0),
            BLOCKSIZE*BLOCK_COUNT,
        )

        file2 = io.LimitReader(
            io.MultiReader(
                readers.OneReader(BLOCKSIZE), // Off by one block
                readers.NewNonRepeatingSequence(0),
            ),
            BLOCKSIZE*BLOCK_COUNT,
        )
    )

    output1 := bytes.NewBuffer(nil)
    chksum1, _, _ := checksum.BuildSequentialAndRootChecksum(file1, output1)

    output2 := bytes.NewBuffer(nil)
    chksum2, _, _ := checksum.BuildSequentialAndRootChecksum(file2, output2)

    if bytes.Compare(chksum1, chksum2) == 0 {
        t.Fatal("Checksums should be different")
    }

    weaksize, strongSize := checksum.GetChecksumSizes()
    sums1, _ := chunks.SizedLoadChecksumsFromReader(output1, uint(BLOCK_COUNT), weaksize, strongSize)
    sums2, _ := chunks.SizedLoadChecksumsFromReader(output2, uint(BLOCK_COUNT), weaksize, strongSize)

    if len(sums1) != len(sums2) {
        t.Fatalf("Checksum lengths differ %v vs %v", len(sums1), len(sums2))
    }

    if sums1[0].Match(sums2[0]) {
        t.Error("Chunk sums1[0] should differ from sums2[0]")
    }

    for i, _ := range sums2 {
        if i == 0 {
            continue
        }

        if !sums1[i-1].Match(sums2[i]) {
            t.Errorf("Chunk sums1[%v] equal sums2[%v]", i-1, i)
        }

    }
}
