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
    localFile        io.ReadSeeker,
    output           io.Writer,
    repositories     []patcher.BlockRepository,
    blockSequence    chunks.SequentialChecksumList,
) (*MultiSourcePatcher, error) {
    // error check
    if localFile == nil {
        return nil, errors.Errorf("no localfile to seek available blocks")
    }
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
//        localFile:        localFile,
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
//    localFile         io.ReadSeeker
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
        currentBlock   uint = 0
        endBlock       uint = uint(len(m.blockSequence) - 1)
        repositoryPool      = makeRepositoryPoolFromMap(m.repositories)
        poolSize            = len(repositoryPool)

        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(blocksources.UintSlice, 0, poolSize)
        responseOrdering    = make(patcher.StackedReponse, 0, poolSize)
    )

    // launch repository pool
    for _, r := range m.repositories {
        log.Debugf("[PATCHER] repo %v starting into pool...", r.RepositoryID())
        m.repoWaiter.Add(1)
        go r.HandleRequest(m.repoWaiter, m.repoExitC, m.repoErrorC, m.repoResponseC)
    }

    log.Debugf("[PATCHER] Patching while currentBlock %v <= endBlock %v", currentBlock, endBlock)
    // loop until current block reaches to end
    for currentBlock <= endBlock {

        poolSize = len(repositoryPool)
        if 0 < poolSize {
            log.Debugf("[PATCHER] Pool available %d", poolSize)

            for i := 0; i < poolSize; i++ {
                var (
                    missing = m.blockSequence[currentBlock]
                    pIndex  = rand.Intn(poolSize - i)
                    poolID  = repositoryPool[pIndex]
                )
                log.Debugf("[PATCHER] missing blk %v | pool %v | pool index %v | pool id %v", missing, repositoryPool, pIndex, poolID)
                repositoryPool = delIdentityFromAvailablePool(repositoryPool, poolID)

                // We'll request only one block to a repository.
                m.repositories[poolID].RequestBlocks(patcher.MissingBlockSpan{
                    BlockSize:     missing.Size,
                    StartBlock:    missing.ChunkOffset,
                    EndBlock:      missing.ChunkOffset,
                })
                log.Debugf("[PATCHER] missing blk %v | pool %v | requested pool #%v | currentBlock %v", missing, repositoryPool, pIndex, currentBlock)

                requestOrdering = append(requestOrdering, missing.ChunkOffset)

                // increment current target block by 1
                currentBlock += 1
            }

            sort.Sort(sort.Reverse(requestOrdering))
        }

        select {
            // handle error first
            case err := <- m.repoErrorC: {
                log.Debugf("[PATCHER] error detected %v", err.Error())
                return errors.Errorf("Failed to read from reference file: %v", err)
            }

            case result := <- m.repoResponseC: {
                log.Debugf("[PATCHER] received result %v | payload [%s]", result, string(result.Data))
                // enqueue result to response queue & sort
                responseOrdering = append(responseOrdering, result)
                sort.Sort(sort.Reverse(responseOrdering))

                // put back the repo id into available pool
                repositoryPool = addIdentityToAvailablePool(repositoryPool, result.RepositoryID)

                // now see if this is to be written into output
                lowestRequest := requestOrdering[len(requestOrdering) - 1]
                if lowestRequest != responseOrdering[len(responseOrdering) - 1].BlockID {
                    continue
                }

                // if there is subsequent saved responses,
                for i := 0; i < len(responseOrdering); i++ {

                    result := responseOrdering[len(responseOrdering) - 1]
                    if _, err := m.output.Write(result.Data); err != nil {
                        log.Errorf("[PATCHER] Could not write data to output: %v", err)
                    }

                    // remove the lowest response queue
                    requestOrdering  = requestOrdering[:len(requestOrdering) - 1]
                    responseOrdering = responseOrdering[:len(responseOrdering) - 1]

                    // Loop if the next block is subsequent block. Halt otherwise.
                    if 0 < len(responseOrdering) && responseOrdering[len(responseOrdering) - 1].BlockID != (lowestRequest + uint(i) + 1) {
                        break
                    }
                }
            }
        }
        log.Debugf("\n\n")
    }

    return nil
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

// ------ conditional select-case trigger -------
/*
 * The idea is to catch the lowest block when condition is met and trigger "case" statement in select like below.
 * Somehow, this doesn't work. :(
 *
 * case alertPendingResponse(readyC, requestOrdering, responseOrdering) <- pullLowestResponse(requestOrdering, responseOrdering):
 *
 */
func alertPendingResponse(
    readyC      chan patcher.RepositoryResponse,
    request     blocksources.UintSlice,
    response    patcher.StackedReponse,
) chan <- patcher.RepositoryResponse {
    log.Debugf("[PATCHER] See if we're ready to receive alert...")
    if len(request) == 0 || len(response) == 0 {
        return nil
    }
    // see if the lowest order is the same as the lowest response block
    if request[len(request)-1] != response[len(response)-1].BlockID {
        return nil
    }
    log.Debugf("[PATCHER] Oh, we are! send response here %v", readyC)
    return readyC
}

func pullLowestResponse(
    request     blocksources.UintSlice,
    response    patcher.StackedReponse,
) patcher.RepositoryResponse {
    // see if the lowest order is the same as the lowest response block
    if len(request) == 0 || len(response) == 0 {
        return patcher.RepositoryResponse{}
    }
    if request[len(request)-1] != response[len(response)-1].BlockID {
        return patcher.RepositoryResponse{}
    }
    log.Debugf("[PATCHER] returning the lowest response...")
    return response[len(response)-1]
}