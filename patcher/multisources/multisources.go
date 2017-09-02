package multisources

import (
    "io"
    "math/rand"
    "sort"
    "sync"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/patcher"
)

/*
 * MultiSources Patcher will stream the patched version of the file to output from multiple sources, since it works
 * strictly in order, it cannot patch the local file directly (since it might overwrite a block needed later), so there
 * would have to be a final copy once the patching was done.
 */

func NewMultiSourcePatcher(
    output           io.Writer,
    repositories     []patcher.BlockRepository,
    blockSequence    chunks.SequentialChecksumList,
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
        output:           output,
        repositories:     rMap,
        blockSequence:    blockSequence,

        repoWaiter:       &sync.WaitGroup{},
        repoExitC:        make(chan bool),
        repoErrorC:       make(chan error),
        repoResponseC:    make(chan patcher.RepositoryResponse),
    }, nil
}

type MultiSourcePatcher struct {
    output            io.Writer
    repositories      map[uint]patcher.BlockRepository
    blockSequence     chunks.SequentialChecksumList

    // repository handling
    repoWaiter        *sync.WaitGroup
    repoExitC         chan bool
    repoErrorC        chan error
    repoResponseC     chan patcher.RepositoryResponse
}

func (m *MultiSourcePatcher) closeRepositories() error {
    close(m.repoExitC)
    m.repoWaiter.Wait()
    close(m.repoErrorC)
    close(m.repoResponseC)
    return nil
}

func (m *MultiSourcePatcher) Patch() error {
    var (
        targetBlock    uint = 0
        finishedBlock  uint = 0
        endBlock       uint = m.blockSequence[len(m.blockSequence) - 1].ChunkOffset

        repositoryPool      = makeRepositoryPoolFromMap(m.repositories)
        poolSize            = len(repositoryPool)

        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(blocksources.UintSlice, 0, poolSize)
        responseOrdering    = make(patcher.StackedReponse, 0, poolSize)
    )

    // launch repository pool
    for _, r := range m.repositories {
        m.repoWaiter.Add(1)
        go r.HandleRequest(m.repoWaiter, m.repoExitC, m.repoErrorC, m.repoResponseC)
    }

    for {
        poolSize = len(repositoryPool)
        if 0 < poolSize {
            for i := 0; i < poolSize && targetBlock <= endBlock; i++ {
                var (
                    missing = m.blockSequence[targetBlock]
                    pIndex  = rand.Intn(poolSize - i)
                    poolID  = repositoryPool[pIndex]
                )
                repositoryPool = delIdentityFromAvailablePool(repositoryPool, poolID)

                // We'll request only one block to a repository.
                m.repositories[poolID].RequestBlocks(patcher.MissingBlockSpan{
                    BlockSize:     missing.Size,
                    StartBlock:    missing.ChunkOffset,
                    EndBlock:      missing.ChunkOffset,
                })

                // append requested block id
                requestOrdering = append(requestOrdering, missing.ChunkOffset)

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
                    result := responseOrdering[len(responseOrdering) - 1]
                    if _, err := m.output.Write(result.Data); err != nil {
                        log.Errorf("[PATCHER] could not write data to output: %v", err)
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
                return nil
            }
        }

        select {
            // handle error first
            case err := <- m.repoErrorC: {
                log.Errorf("repository reports error %v", err.Error())
                return errors.Errorf("Failed to read from reference file: %v", err)
            }

            case result := <- m.repoResponseC: {
                // enqueue result to response queue & sort
                responseOrdering = append(responseOrdering, result)
                sort.Sort(sort.Reverse(responseOrdering))

                // put back the repo id into available pool
                repositoryPool = addIdentityToAvailablePool(repositoryPool, result.RepositoryID)
            }
        }
    }

    return errors.Errorf("patch() should not end at this point")
}

func makeRepositoryPoolFromMap(repos map[uint]patcher.BlockRepository) blocksources.UintSlice {
    var rID = blocksources.UintSlice{}
    for id, _ := range repos {
        rID = append(rID, id)
    }
    sort.Sort(rID)
    return rID
}

func delIdentityFromAvailablePool(rID blocksources.UintSlice, id uint) blocksources.UintSlice {
    var newID = rID[:0]
    for _, r := range rID {
        if r != id {
            newID = append(newID, r)
        }
    }
    sort.Sort(newID)
    return newID
}

func addIdentityToAvailablePool(rID blocksources.UintSlice, id uint) blocksources.UintSlice {
    for _, r := range rID {
        if r == id {
            sort.Sort(rID)
            return rID
        }
    }
    rID = append(rID, id)
    sort.Sort(rID)
    return rID
}
