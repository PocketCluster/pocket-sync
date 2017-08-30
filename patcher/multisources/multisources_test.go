package multisources

import (
    "bytes"
    "io"
    "io/ioutil"
    "strings"
    "testing"

    "golang.org/x/crypto/ripemd160"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/blockrepository"
    "github.com/Redundancy/go-sync/patcher"
)

const (
    BLOCKSIZE        = 8
    REFERENCE_STRING = "The quick brown fox jumped over the lazy dog | The quick brown fox jumped over the lazy dog"
)

var (
    REFERENCE_BUFFER = bytes.NewBufferString(REFERENCE_STRING)
    REFERENCE_BLOCKS []string
    BLOCK_COUNT      int
    REFERENCE_HASHES [][]byte
)

func init() {
    maxLen := len(REFERENCE_STRING)
    m := ripemd160.New()
    for i := 0; i < maxLen; i += BLOCKSIZE {
        last := i + BLOCKSIZE

        if last >= maxLen {
            last = maxLen - 1
        }

        block := REFERENCE_STRING[i:last]

        REFERENCE_BLOCKS = append(REFERENCE_BLOCKS, block)
        m.Write([]byte(block))
        REFERENCE_HASHES = append(REFERENCE_HASHES, m.Sum(nil))
        m.Reset()
    }

    BLOCK_COUNT = len(REFERENCE_BLOCKS)
}

func stringToReadSeeker(input string) io.ReadSeeker {
    return bytes.NewReader([]byte(input))
}

func TestPatchingStart(t *testing.T) {
    var (
        local = bytes.NewReader([]byte("48 brown fox jumped over the lazy dog"))
        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{
            blockrepository.NewReadSeekerBlockRepository(
                0,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
        }
        chksums = []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: []byte("a"), StrongChecksum: []byte("a")},
            {ChunkOffset: 1, WeakChecksum: []byte("b"), StrongChecksum: []byte("b")},
            {ChunkOffset: 2, WeakChecksum: []byte("c"), StrongChecksum: []byte("c")},
            {ChunkOffset: 3, WeakChecksum: []byte("d"), StrongChecksum: []byte("d")},
        }
    )

    src, err := NewMultiSourcePatcher(
        local,
        out,
        repos,
        chksums,
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        if bytes.Compare(result, []byte(REFERENCE_STRING)) != 0 {
            t.Errorf("Result does not equal reference: \"%s\" vs \"%v\"", result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

func Test_PatchingEnd(t *testing.T) {
    var (
        local = bytes.NewReader([]byte("The quick brown fox jumped over the l4zy d0g"))
        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{
            blockrepository.NewReadSeekerBlockRepository(
                0,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
        }
        chksums = []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: []byte("a"), StrongChecksum: []byte("a")},
            {ChunkOffset: 1, WeakChecksum: []byte("b"), StrongChecksum: []byte("b")},
            {ChunkOffset: 2, WeakChecksum: []byte("c"), StrongChecksum: []byte("c")},
            {ChunkOffset: 3, WeakChecksum: []byte("d"), StrongChecksum: []byte("d")},
        }

    )

    src, err := NewMultiSourcePatcher(
        local,
        out,
        repos,
        chksums,
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }

    if result, err := ioutil.ReadAll(out); err == nil {
        if bytes.Compare(result, []byte(REFERENCE_STRING)) != 0 {
            t.Errorf("Result does not equal reference: \"%s\" vs \"%v\"", result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

func Test_PatchingEntirelyMissing(t *testing.T) {
    var (
        local = bytes.NewReader([]byte(""))
        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{
            blockrepository.NewReadSeekerBlockRepository(
                0,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
        }
        chksums = []chunks.ChunkChecksum{
            {ChunkOffset: 0, WeakChecksum: []byte("a"), StrongChecksum: []byte("a")},
            {ChunkOffset: 1, WeakChecksum: []byte("b"), StrongChecksum: []byte("b")},
            {ChunkOffset: 2, WeakChecksum: []byte("c"), StrongChecksum: []byte("c")},
            {ChunkOffset: 3, WeakChecksum: []byte("d"), StrongChecksum: []byte("d")},
        }
    )

    src, err := NewMultiSourcePatcher(
        local,
        out,
        repos,
        chksums,
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }

    if result, err := ioutil.ReadAll(out); err == nil {
        if bytes.Compare(result, []byte(REFERENCE_STRING)) != 0 {
            t.Errorf("Result does not equal reference: \"%s\" vs \"%v\"", result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}
