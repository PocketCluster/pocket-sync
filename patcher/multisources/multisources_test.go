package multisources

import (
    "bytes"
    "io"
    "io/ioutil"
    "math/rand"
    "reflect"
    "strings"
    "sync"
    "testing"
    "time"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "golang.org/x/crypto/ripemd160"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/blockrepository"
    "github.com/Redundancy/go-sync/merkle"
    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/rollsum"
    "github.com/Redundancy/go-sync/util/uslice"
)

const (
    BLOCKSIZE        = 8
    REFERENCE_STRING = "The quick brown fox jumped over the lazy dog | The quick brown fox jumped over the lazy dog"
)

var (
    REFERENCE_BUFFER *bytes.Buffer = nil
    REFERENCE_BLOCKS []string      = nil
    REFERENCE_HASHES [][]byte      = nil
    REFERENCE_RTHASH []byte        = nil
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
    REFERENCE_CHKSEQ = buildSequentialChecksum(REFERENCE_BLOCKS, REFERENCE_HASHES, BLOCKSIZE)
    rootchksum, err := REFERENCE_CHKSEQ.RootHash()
    if err != nil {
        log.Panic(err.Error())
    }
    REFERENCE_RTHASH = rootchksum
    log.Debugf("Root Merkle Hash %v", REFERENCE_RTHASH)
}

func clean() {
    REFERENCE_BUFFER = nil
    REFERENCE_BLOCKS = nil
    REFERENCE_HASHES = nil
    REFERENCE_RTHASH = nil
    REFERENCE_CHKSEQ = nil
    BLOCK_COUNT      = 0
}

func stringToReadSeeker(input string) io.ReadSeeker {
    return bytes.NewReader([]byte(input))
}

func buildSequentialChecksum(refBlks []string, sChksums [][]byte, blocksize int) chunks.SequentialChecksumList {
    var (
        chksum = chunks.SequentialChecksumList{}
        rsum   = rollsum.NewRollsum64(uint(blocksize))
    )

    for i := 0; i < len(refBlks); i++ {
        var (
            wsum = make([]byte, blocksize)
            blk     = []byte(refBlks[i])
        )
        rsum.Reset()
        rsum.SetBlock(blk)
        rsum.GetSum(wsum)

        chksum = append(
            chksum,
            chunks.ChunkChecksum{
                ChunkOffset:    uint(i),
                WeakChecksum:   wsum,
                StrongChecksum: sChksums[i],
            })
    }
    return chksum
}

type testBlkRef struct{}
func (t *testBlkRef) EndBlockID() uint {
    return REFERENCE_CHKSEQ[len(REFERENCE_CHKSEQ) - 1].ChunkOffset
}

func (t *testBlkRef) MissingBlockSpanForID(blockID uint) (patcher.MissingBlockSpan, error) {
    for _, c := range REFERENCE_CHKSEQ {
        if c.ChunkOffset == blockID {
            return patcher.MissingBlockSpan{
                BlockSize:     c.Size,
                StartBlock:    c.ChunkOffset,
                EndBlock:      c.ChunkOffset,
            }, nil
        }
    }
    return patcher.MissingBlockSpan{}, errors.Errorf("[ERR] invalid missing block index %v", blockID)
}

func (t *testBlkRef) VerifyRootHash(hashes [][]byte) error {
    hToCheck, err := merkle.SimpleHashFromHashes(hashes)
    if err != nil {
        return err
    }
    if bytes.Compare(hToCheck, REFERENCE_RTHASH) != 0 {
        return errors.Errorf("[ERR] calculated root hash different from referenece")
    }
    return nil
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
        ids uslice.UintSlice = makeRepositoryPoolFromMap(poolMap)
    )
    if reflect.DeepEqual(ids, []uint{0, 1, 4, 7, 13, 42, 92}) {
        t.Errorf("findAllAvailableRepoID should find all ids")
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
                blockrepository.MakeNullUniformSizeResolver(BLOCKSIZE),
                blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
                    m := ripemd160.New()
                    m.Write(data)
                    return m.Sum(nil), nil
                }),
            ),
        }
    )

    src, err := NewMultiSourcePatcher(
        out,
        repos,
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.Close()

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        if bytes.Compare(result, []byte(REFERENCE_STRING)) != 0 {
            t.Errorf("result is not equal reference: \"%s\" vs \"%v\"", result, REFERENCE_STRING)
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
        verifier = blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
            m := ripemd160.New()
            m.Write(data)
            return m.Sum(nil), nil
        })

        repos = []patcher.BlockRepository{
            blockrepository.NewReadSeekerBlockRepository(
                0,
                stringToReadSeeker(REFERENCE_STRING),
                blockrepository.MakeNullUniformSizeResolver(BLOCKSIZE),
                verifier),
            blockrepository.NewReadSeekerBlockRepository(
                1,
                stringToReadSeeker(REFERENCE_STRING),
                blockrepository.MakeNullUniformSizeResolver(BLOCKSIZE),
                verifier),
            blockrepository.NewReadSeekerBlockRepository(
                2,
                stringToReadSeeker(REFERENCE_STRING),
                blockrepository.MakeNullUniformSizeResolver(BLOCKSIZE),
                verifier),
            blockrepository.NewReadSeekerBlockRepository(
                3,
                stringToReadSeeker(REFERENCE_STRING),
                blockrepository.MakeNullUniformSizeResolver(BLOCKSIZE),
                verifier),
        }
    )

    src, err := NewMultiSourcePatcher(
        out,
        repos,
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.Close()

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        if bytes.Compare(result, []byte(REFERENCE_STRING)) != 0 {
            t.Errorf("result is not equal reference: \"%s\" vs \"%v\"", result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

// TODO : this should work but due to how block repo works, this will panic. need to fix it later
func Skip_Cancelled_Patcher(t *testing.T) {
    setup()
    defer clean()

    var (
        verifier = blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
            return nil, errors.Errorf("test")
        })

        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{

            blockrepository.NewBlockRepositoryBase(
                0,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    log.Infof("src #0 requested")
                    return []byte{0x00}, nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),
        }
    )
    src, err := NewMultiSourcePatcher(
        out,
        repos,
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }
    // exit all repos and then patch
    src.Close()
    err = src.Patch()
    if err == nil {
        t.Fatal(err)
    }
    if !IsInterruptError(err) {
        t.Fatalf("patch error should be interrupt error : Error", err.Error())
    }
}

// TODO : even when cancel signal is dispatched, repo source retrial keeps going. We need to fix & test that.
func Test_MultiSource_Cancel(t *testing.T) {
    setup()
    defer clean()

    var (
        cancelReadyC = make(chan struct{})
        verifier     = blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
            return nil, errors.Errorf("test")
        })

        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{

            blockrepository.NewBlockRepositoryBase(
                0,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    <-cancelReadyC
                    log.Infof("src #0 requested")
                    return []byte{0x00}, nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                1,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    <-cancelReadyC
                    log.Infof("src #1 requested")
                    return []byte{0x00}, nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                2,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    <-cancelReadyC
                    log.Infof("src #2 requested")
                    return []byte{0x00}, nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                3,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    <-cancelReadyC
                    log.Infof("src #3 requested")
                    return []byte{0x00}, nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),
        }
    )
    src, err := NewMultiSourcePatcher(
        out,
        repos,
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }
    go func() {
        // we need to be sure all the repos are on the go
        <-cancelReadyC
        <- time.After(time.Second)
        src.Close()
    }()
    close(cancelReadyC)
    err = src.Patch()
    if err == nil {
        t.Fatal("there should be user halt error")
    }
    if !IsInterruptError(err) {
        t.Fatalf("should be user halt error. Reason : %v", err.Error())
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
        verifier = blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
            m := ripemd160.New()
            m.Write(data)
            return m.Sum(nil), nil
        })

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
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                1,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{1,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                2,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{2,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                3,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{3,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),
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
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.Close()
    close(countC)
    waiter.Wait()

    for c := range hitCount {
        t.Logf("Repo #%d hit %v times | sleep %v", c, hitCount[c], slpCount[c]/time.Duration(hitCount[c]))
    }

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        diff := bytes.Compare(result, []byte(REFERENCE_STRING))
        if diff != 0 {
            t.Errorf("[%d] result is not equal reference: \"%s\" vs \"%v\"", diff, result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

func Test_Multi_OutOfOrder_Source_Patching(t *testing.T) {
    setup()
    defer clean()

    const lowestStartblk int64 = -2
    type repoStartBlk struct {
        rID int
        sBlk int64
    }

    var (
        waiter   sync.WaitGroup
        hitCount = []int{0, 0, 0, 0}
        blkCount = []int64{lowestStartblk, lowestStartblk, lowestStartblk, lowestStartblk}
        countC   = make(chan repoStartBlk)
        waiterC  = []chan bool {
            make(chan bool),
            make(chan bool),
            make(chan bool),
            make(chan bool),
        }

        out   = bytes.NewBuffer(nil)
        verifier = blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
            m := ripemd160.New()
            m.Write(data)
            return m.Sum(nil), nil
        })

        repos = []patcher.BlockRepository{

            blockrepository.NewBlockRepositoryBase(
                0,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    countC <- repoStartBlk{0, start}
                    <- waiterC[0]
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                1,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    countC <- repoStartBlk{1, start}
                    <- waiterC[1]
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                2,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    countC <- repoStartBlk{2, start}
                    <- waiterC[2]
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                3,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    countC <- repoStartBlk{3, start}
                    <- waiterC[3]
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),
        }

        isAllRepoInquired = func(sblks []int64) bool {
            for _, s := range sblks {
                if s == lowestStartblk  {
                    return false
                }
            }
            return true
        }

        findHeightStartBlk = func(sblks []int64) (int64, int) {
            var (
                highest int64 = lowestStartblk
                index   int = -1
            )
            for i, s := range sblks {
                if highest < s {
                    highest = s
                    index = i
                }
            }
            if highest != lowestStartblk {
                sblks[index] = lowestStartblk
            }
            return highest, index
        }
    )
    waiter.Add(1)
    // this logic works with an assumption that every repo will gets hit
    go func(h []int, b []int64, w []chan bool) {
        defer waiter.Done()

        for r := range countC {
            h[r.rID]++
            b[r.rID] = r.sBlk

            if isAllRepoInquired(b) {
                var (
                    highest int64 = 0
                    index int = 0
                )
                for highest != lowestStartblk {
                    highest, index = findHeightStartBlk(b)
                    if highest != lowestStartblk {
                        w[index] <- true
                        time.Sleep(time.Millisecond * 300)
                        log.Debugf("repo #%d highest blk #%d", index, highest)
                    }
                }
                log.Debugf("all waiting repo flushed!\n")
            }
        }
    }(hitCount, blkCount, waiterC)

    src, err := NewMultiSourcePatcher(
        out,
        repos,
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.Close()
    close(countC)
    waiter.Wait()

    for c := range hitCount {
        t.Logf("Repo #%d hit %v times ", c, hitCount[c])
    }

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        diff := bytes.Compare(result, []byte(REFERENCE_STRING))
        if diff != 0 {
            t.Errorf("[%d] result is not equal reference: \"%s\" vs \"%v\"", diff, result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

func Test_Single_Repository_Failure(t *testing.T) {
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
        verifier = blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
            m := ripemd160.New()
            m.Write(data)
            return m.Sum(nil), nil
        })

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
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                1,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    if start < 40 {
                        sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                        countC <- repoDelay{1,sl}
                        time.Sleep(sl)
                        return []byte(REFERENCE_STRING)[start:end], nil
                    }
                    return nil, &blocksources.TestError{}
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                2,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{2,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                3,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                    countC <- repoDelay{3,sl}
                    time.Sleep(sl)
                    return []byte(REFERENCE_STRING)[start:end], nil
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),
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
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err != nil {
        t.Fatal(err)
    }
    src.Close()
    close(countC)
    waiter.Wait()

    for c := range hitCount {
        t.Logf("Repo #%d hit %v times | sleep %v", c, hitCount[c], slpCount[c]/time.Duration(hitCount[c]))
    }

    if result, err := ioutil.ReadAll(out); err == nil {
        t.Logf("String split is: \"%v\"", strings.Join(REFERENCE_BLOCKS, "\", \""))
        diff := bytes.Compare(result, []byte(REFERENCE_STRING))
        if diff != 0 {
            t.Errorf("[%d] result is not equal to reference: \"%s\" vs \"%v\"", diff, result, REFERENCE_STRING)
        }
    } else {
        t.Fatal(err)
    }
}

func Test_All_Repositories_Failure(t *testing.T) {
    setup()
    defer clean()

    const (
        error_trigger_offset int64 = 60
    )

    type repoDelay struct {
        rID int
        sleep time.Duration
    }

    var (
        waiter   sync.WaitGroup
        countC   = make(chan repoDelay)
        hitCount = []int{0, 0, 0, 0}
        slpCount = []time.Duration{0, 0, 0, 0}
        verifier = blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
            m := ripemd160.New()
            m.Write(data)
            return m.Sum(nil), nil
        })

        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{

            blockrepository.NewBlockRepositoryBase(
                0,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    if start < error_trigger_offset {
                        sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                        countC <- repoDelay{0,sl}
                        time.Sleep(sl)
                        return []byte(REFERENCE_STRING)[start:end], nil
                    }
                    return nil, &blocksources.TestError{}
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                1,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    if start < error_trigger_offset {
                        sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                        countC <- repoDelay{1,sl}
                        time.Sleep(sl)
                        return []byte(REFERENCE_STRING)[start:end], nil
                    }
                    return nil, &blocksources.TestError{}
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                2,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    if start < error_trigger_offset {
                        sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                        countC <- repoDelay{2,sl}
                        time.Sleep(sl)
                        return []byte(REFERENCE_STRING)[start:end], nil
                    }
                    return nil, &blocksources.TestError{}
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),

            blockrepository.NewBlockRepositoryBase(
                3,
                blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                    if start < error_trigger_offset {
                        sl := time.Millisecond * time.Duration(100 * rand.Intn(10))
                        countC <- repoDelay{3,sl}
                        time.Sleep(sl)
                        return []byte(REFERENCE_STRING)[start:end], nil
                    }
                    return nil, &blocksources.TestError{}
                }),
                blockrepository.MakeKnownFileSizedBlockResolver(BLOCKSIZE, int64(len(REFERENCE_STRING))),
                verifier),
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
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err == nil {
        t.Fatal("Patch should create error")
    } else {
        t.Log(err.Error())
    }

    src.Close()
    close(countC)
    waiter.Wait()

    for c := range hitCount {
        t.Logf("Repo #%d hit %v times | sleep %v", c, hitCount[c], slpCount[c]/time.Duration(hitCount[c]))
    }
}

func Test_RootChecksum_Failure(t *testing.T) {
    setup()
    defer clean()
    REFERENCE_RTHASH = []byte{0xFF, 0xA1}

    var (
        out   = bytes.NewBuffer(nil)
        repos = []patcher.BlockRepository{
            blockrepository.NewReadSeekerBlockRepository(
                0,
                stringToReadSeeker(REFERENCE_STRING),
                blockrepository.MakeNullUniformSizeResolver(BLOCKSIZE),
                blockrepository.FunctionChecksumVerifier(func(startBlockID uint, data []byte) ([]byte, error){
                    m := ripemd160.New()
                    m.Write(data)
                    return m.Sum(nil), nil
                }),
            ),
        }
    )

    src, err := NewMultiSourcePatcher(
        out,
        repos,
        &testBlkRef{},
    )
    if err != nil {
        t.Fatal(err)
    }

    err = src.Patch()
    if err == nil {
        t.Fatal("Patch should fail as root checksum is alternated")
    } else {
        t.Log(err.Error())
    }
    src.Close()
}