package multisources

import (
    "io"
    "math/rand"
    "sort"
    "sync"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/util/uslice"
)

func newInterruptError(msg string) error {
    return &intError{s:msg}
}

type intError struct {
    s string
}

func (i *intError) Error() string {
    return i.s
}

func IsInterruptError(err error) bool {
    _, ok := err.(*intError)
    return ok
}

/*
 * MultiSources Patcher will stream the patched version of the file to output from multiple sources, since it works
 * strictly in order, it cannot patch the local file directly (since it might overwrite a block needed later), so there
 * would have to be a final copy once the patching was done.
 */

func NewMultiSourcePatcher(
    output       io.Writer,
    repositories []patcher.BlockRepository,
    blockRef     patcher.SeqChecksumReference,
) (*MultiSourcePatcher, error) {
    // error check
    if output == nil {
        return nil, errors.Errorf("no output to save retrieved blocks")
    }
    if repositories == nil || len(repositories) == 0 {
        return nil, errors.Errorf("No BlockSource set for obtaining reference blocks")
    }

    rMap := map[uint]patcher.BlockRepository{}
    for _, r := range repositories {
        rMap[r.RepositoryID()] = r
    }

    return &MultiSourcePatcher{
        output:        output,
        repositories:  rMap,
        blockRef:      blockRef,

        waiter:        &sync.WaitGroup{},
        exitC:         make(chan struct{}),

        repoWaiter:    &sync.WaitGroup{},
        repoExitC:     make(chan bool),
        repoErrorC:    make(chan *patcher.RepositoryError),
        repoResponseC: make(chan patcher.RepositoryResponse),
    }, nil
}

type MultiSourcePatcher struct {
    sync.Mutex
    isClosed         bool
    output           io.Writer
    repositories     map[uint]patcher.BlockRepository
    blockRef         patcher.SeqChecksumReference

    waiter           *sync.WaitGroup
    exitC            chan struct{}

    // repository handling
    repoWaiter       *sync.WaitGroup
    repoExitC        chan bool
    repoErrorC       chan *patcher.RepositoryError
    repoResponseC    chan patcher.RepositoryResponse
}

// It is presumed that `Close()` and `Patch()` works on different routines
func (m *MultiSourcePatcher) Close() error {
    m.Lock()
    defer m.Unlock()
    if m.isClosed {
        return nil
    }
    m.isClosed = true

    // close patcher first so we don't make request to closed repos
    close(m.exitC)
    m.waiter.Wait()

    // at this point it's safe to
    close(m.repoExitC)
    m.repoWaiter.Wait()

    // now close all other channels
    close(m.repoErrorC)
    close(m.repoResponseC)
    return nil
}

func (m *MultiSourcePatcher) Patch() error {
    var (
        targetBlock    uint = 0
        finishedBlock  uint = 0
        endBlock       uint = m.blockRef.EndBlockID()

        repositoryPool      = makeRepositoryPoolFromMap(m.repositories)
        poolSize            = len(repositoryPool)

        // collect strong checksums
        sChksumSequence     = make([][]byte, 0, 1)

        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(uslice.UintSlice,          0, poolSize)
        responseOrdering    = make(patcher.StackedReponse,    0, poolSize)
        retryRequests       = make(patcher.QueuedRequestList, 0, poolSize)
    )
    // this is for Patch() itself
    m.waiter.Add(1)
    defer m.waiter.Done()

    // launch repository pool
    for _, r := range m.repositories {
        m.repoWaiter.Add(1)
        go r.HandleRequest(m.repoWaiter, m.repoExitC, m.repoErrorC, m.repoResponseC)
    }

    for {
        // we don't check 'endBlock == finishedBlock' condition here as it should only happen at one specific place
        if len(m.repositories) == 0 {
            return errors.Errorf("[ERR] failed to retrieve blocks from all repositories.")
        }

        // retry failed block
        poolSize = len(repositoryPool)
        if 0 < poolSize && len(retryRequests) != 0 {
            for i := 0; i < poolSize && 0 < len(retryRequests); i++ {
                var (
                    rTarget = retryRequests[len(retryRequests) - 1]
                    pIndex  = rand.Intn(poolSize - i)
                    poolID  = repositoryPool[pIndex]
                )
                // reduce repo pool
                repositoryPool = uslice.DelElementFromSlice(repositoryPool, poolID)
                // reduce retry quest
                retryRequests = retryRequests[:len(retryRequests) - 1]
                // retry failed target
                m.repositories[poolID].RequestBlocks(rTarget)
            }
        }

        // try next in queue
        poolSize = len(repositoryPool)
        if 0 < poolSize {
            for i := 0; i < poolSize && targetBlock <= endBlock; i++ {
                var (
                    pIndex  = rand.Intn(poolSize - i)
                    poolID  = repositoryPool[pIndex]
                )
                repositoryPool = uslice.DelElementFromSlice(repositoryPool, poolID)

                // this is a critical error
                // TODO test error conditions & make sure all channels closed properly
                missing, err := m.blockRef.MissingBlockSpanForID(targetBlock)
                if err != nil {
                    return errors.WithStack(err)
                }

                // We'll request only one block to a repository.
                m.repositories[poolID].RequestBlocks(missing)

                // append requested block id
                requestOrdering = append(requestOrdering, missing.StartBlock)

                // increment current target block by 1
                targetBlock += 1
            }

            sort.Sort(sort.Reverse(requestOrdering))
        }

        // Once we satuated pool with request, we'd like to see if response can be written into output.
        // in "request" -> "write to disk" sequence could give some some window where network & disk ops going same time.
        if 0 < len(requestOrdering) && 0 < len(responseOrdering) {
            var (
                responseSize   int  = len(responseOrdering) // this needs to be fixated for the loop below. Don't use it otherwise.
                lowestRequest  uint = requestOrdering[len(requestOrdering) - 1]
                lowestResponse uint = responseOrdering[len(responseOrdering) - 1].BlockID
            )

            if lowestRequest == lowestResponse {

                for i := 0; i < responseSize; i++ {
                    // save result to output
                    result := responseOrdering[len(responseOrdering) - 1]
                    if _, err := m.output.Write(result.Data); err != nil {
                        log.Errorf("[PATCHER] could not write data to output: %v", err)
                    }
                    // save Strong checksum to list
                    if result.StrongChecksum != nil {
                        sChksumSequence = append(sChksumSequence, result.StrongChecksum)
                    }
                    // save finished block
                    finishedBlock    = requestOrdering[len(requestOrdering) - 1]

                    // remove the lowest response queue
                    requestOrdering  = requestOrdering[:len(requestOrdering) - 1]
                    responseOrdering = responseOrdering[:len(responseOrdering) - 1]

                    // Loop if the next responded block is subsequent block. Halt otherwise.
                    if 0 < len(responseOrdering) && responseOrdering[len(responseOrdering) - 1].BlockID != (finishedBlock + 1) {
                        break
                    }
                }
            }

            // *** PERFECT END CONDITION ***
            if endBlock == finishedBlock {
                return m.blockRef.VerifyRootHash(sChksumSequence)
            }
        }

        select {
            case <- m.exitC:
                // TODO : this is interruption. test error conditions & make sure all channels closed properly
                return newInterruptError("sync halted by interruption")

            // at this point, the erronous repository will not be added back to available pool and removed from pool
            case err := <- m.repoErrorC: {
                log.Errorf("repository #%v reports error %v", err.RepositoryID(), err.Error())

                // re-stack failed request
                retryRequests = append(retryRequests, err.MissingBlock())
                sort.Sort(sort.Reverse(retryRequests))

                // remove repository from repository pool
                delete(m.repositories, err.RepositoryID())
            }

            case result := <- m.repoResponseC: {
                // enqueue result to response queue & sort
                responseOrdering = append(responseOrdering, result)
                sort.Sort(sort.Reverse(responseOrdering))

                // put back the repo id into available pool
                repositoryPool = uslice.AddElementToSlice(repositoryPool, result.RepositoryID)
            }
        }
    }

    return errors.Errorf("patch() should not end at this point")
}

func makeRepositoryPoolFromMap(repos map[uint]patcher.BlockRepository) uslice.UintSlice {
    var rID = uslice.UintSlice{}
    for id, _ := range repos {
        rID = append(rID, id)
    }
    sort.Sort(rID)
    return rID
}
