package multisources

import (
    "io"
//    "sort"
    "sync"

    "github.com/pkg/errors"
//    "github.com/Redundancy/go-sync/blocksources"
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
/*
    var (
        currentBlock       uint = 0
        endBlock           uint = uint(len(m.blockSequence) - 1)
        poolSize           int  = len(m.repositories)

        // enable us to order responses for the active requests, lowest to highest
        requestOrdering    = make(blocksources.UintSlice, 0, poolSize)
        responseOrdering   = make([]patcher.BlockReponse, 0, poolSize)
    )
    // launch repository pool
    for _, repo := range m.repositories {
        go repo.HandleRequest(m.repoWaiter, m.repoExitC, m.repoErrorC, m.repoResponseC)
    }

    for currentBlock <= endBlock {

        // 1. pool available | 2. available slot in the pool
        if 0 < poolSize && len(requestOrdering) == 0 {

            for i := 0; i < poolSize; i++ {
                missing := m.blockSequence[currentBlock + uint(i)]
                requestOrdering = append(requestOrdering, missing.ChunkOffset)
                sort.Sort(sort.Reverse(requestOrdering))

                // We'll request only one block to a repository.
                // It might/might not splitted into smaller request size
                m.repoRequestC <- patcher.MissingBlockSpan{
                    BlockSize:     missing.Size,
                    StartBlock:    missing.ChunkOffset,
                    EndBlock:      missing.ChunkOffset,
                }
            }
        }

        select {

            case result := <-m.repoResponseC: {
                // error check
                if result.StartBlock != currentBlock {
                    return errors.Errorf("Received unexpected block: %v", result.StartBlock)
                }
                _, err := m.output.Write(result.Data)
                if err != nil {
                    return errors.Errorf("Could not write data to output: %v", err)
                }

                // calculate # of blocks completed
                completed := calculateNumberOfCompletedBlocks(uint64(len(result.Data)), uint64(firstMissing.BlockSize))
                if completed != (firstMissing.EndBlock - firstMissing.StartBlock) + 1 {
                    return errors.Errorf(
                        "Unexpected reponse length from remote source: blocks %v-%v (got %v blocks)",
                        firstMissing.StartBlock,
                        firstMissing.EndBlock,
                        completed)
                }

                // move iterator
                currentBlock += completed
                m.requiredRemoteBlocks = m.requiredRemoteBlocks[1:]
            }

            case err := <-m.repoErrorC: {
                return errors.Errorf("Failed to read from reference file: %v", err)
            }
        }
    }
*/
    return nil
}

func calculateNumberOfCompletedBlocks(resultLength uint64, blockSize uint64) uint {
    var completedBlockCount uint64 = resultLength / blockSize

    // round up in the case of a partial block (last block may not be full sized)
    if resultLength % blockSize != 0 {
        completedBlockCount += 1
    }

    return uint(completedBlockCount)
}

func findAllAvailableRepoID(repos map[uint]patcher.BlockRepository) []uint {
    var rID = []uint{}
    for id, _ := range repos {
        rID = append(rID, id)
    }
    return rID
}

func delRepoFromAvailablePool(rID []uint, id uint) []uint {
    var newID = rID[:0]
    for _, r := range rID {
        if r != id {
            newID = append(newID, r)
        }
    }
    return newID
}

func addRepoToAvailablePool(rID []uint, id uint) []uint {
    rID = append(rID, id)
    return rID
}