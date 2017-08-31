package multisources

import (
    "io"
    "math/rand"
    "sort"
    "sync"

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
        localFile:        localFile,
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
    localFile        io.ReadSeeker
    output           io.Writer
    repositories     map[uint]patcher.BlockRepository
    blockSequence    chunks.SequentialChecksumList

    repoWaiter       *sync.WaitGroup
    repoExitC        chan bool
    repoErrorC       chan error
    repoResponseC    chan patcher.RepositoryResponse
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
    for _, repo := range m.repositories {
        go repo.HandleRequest(m.repoWaiter, m.repoExitC, m.repoErrorC, m.repoResponseC)
    }

    for currentBlock <= endBlock {

        poolSize = len(repositoryPool)
        if 0 < poolSize {

            for i := 0; i < poolSize; i++ {
                missing := m.blockSequence[currentBlock + uint(i)]
                requestOrdering = append(requestOrdering, missing.ChunkOffset)

                pIndex := rand.Intn(poolSize - i)
                pID    := repositoryPool[pIndex]
                repositoryPool = delIdentityFromAvailablePool(repositoryPool, pID)

                // We'll request only one block to a repository.
                // It might/might not splitted into smaller request size
                m.repositories[pID].RequestBlocks(patcher.MissingBlockSpan{
                    BlockSize:     missing.Size,
                    StartBlock:    missing.ChunkOffset,
                    EndBlock:      missing.ChunkOffset,
                })
            }

            sort.Sort(requestOrdering)
        }

        select {

            case result := <-m.repoResponseC: {

                // find the lowest id
                lowestRequest := requestOrdering[0]

                // remove received id from
                delIdentityFromAvailablePool(requestOrdering, result.BlockID)

                // enqueue result to response & sort
                responseOrdering = append(responseOrdering, result)
                sort.Sort(responseOrdering)

                // put back the repo id into pool
                repositoryPool = addIdentityToAvailablePool(repositoryPool, result.RepositoryID)

                // if this is the lowest block, then write it to output
                if lowestRequest == result.BlockID {

                }
            }

            case err := <-m.repoErrorC: {
                return errors.Errorf("Failed to read from reference file: %v", err)
            }
        }
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