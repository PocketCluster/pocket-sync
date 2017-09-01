package multisources

import (
    "bytes"
    "io"
    "io/ioutil"
    "math/rand"
    "strings"
    "reflect"
    "testing"
    "time"

    log "github.com/Sirupsen/logrus"
    "golang.org/x/crypto/ripemd160"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/blockrepository"
    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/rollsum"
    "sync"
)

const (
    BLOCKSIZE        = 8
    REFERENCE_STRING = "The quick brown fox jumped over the lazy dog | The quick brown fox jumped over the lazy dog"
)

var (
    REFERENCE_BUFFER *bytes.Buffer = nil
    REFERENCE_BLOCKS []string      = nil
    REFERENCE_HASHES [][]byte      = nil
    REFERENCE_CHKSEQ chunks.SequentialChecksumList = nil
    BLOCK_COUNT      int           = 0
)

func setup() {
    REFERENCE_BUFFER = bytes.NewBufferString(REFERENCE_STRING)
    REFERENCE_BLOCKS = []string{}
    REFERENCE_HASHES = [][]byte{}
    BLOCK_COUNT      = 0

    maxLen          := len(REFERENCE_STRING)
    m               := ripemd160.New()

    log.SetLevel(log.DebugLevel)
    for i := 0; i < maxLen; i += BLOCKSIZE {
        last := i + BLOCKSIZE

        if last >= maxLen {
            last = maxLen
        }

        block := REFERENCE_STRING[i:last]

        REFERENCE_BLOCKS = append(REFERENCE_BLOCKS, block)
        m.Write([]byte(block))
        REFERENCE_HASHES = append(REFERENCE_HASHES, m.Sum(nil))
        m.Reset()
    }

    BLOCK_COUNT = len(REFERENCE_BLOCKS)
    REFERENCE_CHKSEQ = buildSequentialChecksum(REFERENCE_BLOCKS, BLOCKSIZE)
}

func clean() {
    REFERENCE_BUFFER = nil
    REFERENCE_BLOCKS = nil
    REFERENCE_HASHES = nil
    REFERENCE_CHKSEQ = nil
    BLOCK_COUNT      = 0
}

func stringToReadSeeker(input string) io.ReadSeeker {
    return bytes.NewReader([]byte(input))
}

func buildSequentialChecksum(refBlks []string, blocksize int) chunks.SequentialChecksumList {
    var (
        chksum = chunks.SequentialChecksumList{}
        rsum   = rollsum.NewRollsum64(uint(blocksize))
        ssum   = ripemd160.New()
    )

    for i := 0; i < len(refBlks); i++ {
        var (
            wsum = make([]byte, blocksize)
            blk     = []byte(refBlks[i])
        )
        rsum.SetBlock(blk)
        rsum.GetSum(wsum)
        ssum.Write(blk)

        chksum = append(
            chksum,
            chunks.ChunkChecksum{
                ChunkOffset:    uint(i),
                WeakChecksum:   wsum,
                StrongChecksum: ssum.Sum(nil),
            })
    }
    return chksum
}

func Test_Available_Pool_Addition(t *testing.T) {
    var (
        poolMap = map[uint]patcher.BlockRepository{
            0:  &blockrepository.BlockRepositoryBase{},
            1:  &blockrepository.BlockRepositoryBase{},
            4:  &blockrepository.BlockRepositoryBase{},
            7:  &blockrepository.BlockRepositoryBase{},
            13: &blockrepository.BlockRepositoryBase{},
            42: &blockrepository.BlockRepositoryBase{},
            92: &blockrepository.BlockRepositoryBase{},
        }
        ids blocksources.UintSlice = makeRepositoryPoolFromMap(poolMap)
    )

    if reflect.DeepEqual(ids, []uint{0, 1, 4, 7, 13, 42, 92}) {
        t.Errorf("findAllAvailableRepoID should find all ids")
    }

    ids = addIdentityToAvailablePool(ids, 4)
    if reflect.DeepEqual(ids, []uint{0, 1, 4, 7, 13, 42, 92}) {
        t.Errorf("addIdentityToAvailablePool should not add duplicated id")
    }

    ids = addIdentityToAvailablePool(ids, 77)
    if reflect.DeepEqual(ids, []uint{0, 1, 4, 7, 13, 42, 77, 92}) {
        t.Errorf("addIdentityToAvailablePool should add new id")
    }
}

func Test_Available_Pool_Deletion(t *testing.T) {
    var (
        ids blocksources.UintSlice = []uint{0, 1, 4, 7, 13, 42, 92}
    )

    ids = delIdentityFromAvailablePool(ids, 11)
    if reflect.DeepEqual(ids, []uint{0, 1, 7, 13, 42, 92}) {
        t.Errorf("delIdentityFromAvailablePool should not delete absent element %v", ids)
    }

    ids = delIdentityFromAvailablePool(ids, 4)
    if reflect.DeepEqual(ids, []uint{0, 1, 7, 13, 42, 92}) {
        t.Errorf("delIdentityFromAvailablePool only delete one id %v", ids)
    }

    ids = delIdentityFromAvailablePool(ids, 7)
    if reflect.DeepEqual(ids, []uint{0, 1, 13, 42, 92}) {
        t.Errorf("delIdentityFromAvailablePool only delete one id %v", ids)
    }

    ids = delIdentityFromAvailablePool(ids, 92)
    if reflect.DeepEqual(ids, []uint{0, 1, 7, 13, 42}) {
        t.Errorf("delIdentityFromAvailablePool only delete one id %v", ids)
    }
}

func Test_SingleSource_Basic_Patching(t *testing.T) {
    setup()
    defer clean()

    var (
        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{
            blockrepository.NewReadSeekerBlockRepository(
                0,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
        }
    )

    src, err := NewMultiSourcePatcher(
        out,
        repos,
        REFERENCE_CHKSEQ,
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.closeRepositories()

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        if bytes.Compare(result, []byte(REFERENCE_STRING)) != 0 {
            t.Errorf("Result does not equal reference: \"%s\" vs \"%v\"", result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

func Test_MultiSource_Basic_Patching(t *testing.T) {
    setup()
    defer clean()

    var (
        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{
            blockrepository.NewReadSeekerBlockRepository(
                0,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
            blockrepository.NewReadSeekerBlockRepository(
                1,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
            blockrepository.NewReadSeekerBlockRepository(
                2,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
            blockrepository.NewReadSeekerBlockRepository(
                3,
                stringToReadSeeker(REFERENCE_STRING),
                blocksources.MakeNullFixedSizeResolver(BLOCKSIZE),
            ),
        }
    )

    src, err := NewMultiSourcePatcher(
        out,
        repos,
        REFERENCE_CHKSEQ,
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.closeRepositories()

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        if bytes.Compare(result, []byte(REFERENCE_STRING)) != 0 {
            t.Errorf("Result does not equal reference: \"%s\" vs \"%v\"", result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

func Test_MultiRandom_Source_Patching(t *testing.T) {
    setup()
    defer clean()

    type repoDelay struct {
        rID int
        sleep time.Duration
    }

    var (
        waiter   sync.WaitGroup
        countC   = make(chan repoDelay)
        hitCount = []int{0, 0, 0, 0}
        slpCount = []time.Duration{0, 0, 0, 0}

        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{

            blockrepository.NewBlockRepositoryBase(
                0,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{0,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blocksources.MakeFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                nil),

            blockrepository.NewBlockRepositoryBase(
                1,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{1,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blocksources.MakeFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                nil),

            blockrepository.NewBlockRepositoryBase(
                2,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{2,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blocksources.MakeFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                nil),

            blockrepository.NewBlockRepositoryBase(
                3,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{3,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blocksources.MakeFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                nil),
        }
    )
    waiter.Add(1)
    go func(h []int, s []time.Duration) {
        defer waiter.Done()
        for r := range countC {
            h[r.rID]++
            s[r.rID] += r.sleep
        }
    }(hitCount, slpCount)

    src, err := NewMultiSourcePatcher(
        out,
        repos,
        REFERENCE_CHKSEQ,
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.closeRepositories()
    close(countC)
    waiter.Wait()

    for c := range hitCount {
        t.Logf("Repo #%d hit %v times | sleep %v", c, hitCount[c], slpCount[c]/time.Duration(hitCount[c]))
    }

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        diff := bytes.Compare(result, []byte(REFERENCE_STRING))
        if diff != 0 {
            t.Errorf("[%d] Result does not equal reference: \"%s\" vs \"%v\"", diff, result, REFERENCE_STRING)

        }
    } else {
        t.Fatal(err)
    }
}
