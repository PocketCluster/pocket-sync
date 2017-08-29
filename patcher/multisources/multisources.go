package multisources

import (
    "io"
    "math/rand"

    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
)

/*
 * MultiSources Patcher will stream the patched version of the file to output from multiple sources, since it works
 * strictly in order, it cannot patch the local file directly (since it might overwrite a block needed later), so there
 * would have to be a final copy once the patching was done.
 */

func NewMultiSourcePatcher(
    localFile              io.ReadSeeker,
    references             []patcher.BlockSource,
    requiredRemoteBlocks   []patcher.MissingBlockSpan,
    locallyAvailableBlocks []patcher.FoundBlockSpan,
    maxBlockStorage        uint64,
    output                 io.Writer,
) (*MultiSourcePatcher, error) {
    // error check
    if localFile == nil {
        return nil, errors.Errorf("no localfile to seek available blocks")
    }
    if output == nil {
        return nil, errors.Errorf("no output to save retrieved blocks")
    }
    if references == nil || len(references) == 0 {
        return nil, errors.Errorf("No BlockSource set for obtaining reference blocks")
    }
    return &MultiSourcePatcher{
        localFile:              localFile,
        output:                 output,
        references:             references,
        requiredRemoteBlocks:   requiredRemoteBlocks,
        locallyAvailableBlocks: locallyAvailableBlocks,
        maxBlockStorage:        maxBlockStorage,
    }, nil
}

type MultiSourcePatcher struct {
    localFile              io.ReadSeeker
    output                 io.Writer
    references             []patcher.BlockSource
    requiredRemoteBlocks   []patcher.MissingBlockSpan
    locallyAvailableBlocks []patcher.FoundBlockSpan

    // the amount of memory we're allowed to use for temporary data storage
    maxBlockStorage        uint64
}

func (m *MultiSourcePatcher) Close() error {

    return nil
}

func (m *MultiSourcePatcher) Patch() error {
    var (
        endBlockMissing    uint = 0
        endBlockAvailable  uint = 0
        endBlock           uint = 0
        currentBlock       uint = 0

        // enable us to order responses for the active requests, lowest to highest
        requestOrdering    = make([]uint,                0, 1)
        responseOrdering   = make(patcher.BlockReponse,  0, 1)
    )

    // adjust blocks
    if len(m.requiredRemoteBlocks) > 0 {
        endBlockMissing = m.requiredRemoteBlocks[len(m.requiredRemoteBlocks) - 1].EndBlock
    }
    if len(m.locallyAvailableBlocks) > 0 {
        endBlockAvailable = m.locallyAvailableBlocks[len(m.locallyAvailableBlocks) - 1].EndBlock
    }
    endBlock = endBlockMissing
    if endBlockAvailable > endBlock {
        endBlock = endBlockAvailable
    }

    for currentBlock <= endBlock {
        firstMissing := m.requiredRemoteBlocks[0]
        reference := pickAvaiableReference(m.references)
        reference.RequestBlocks(firstMissing)

        select {

            case result := <-reference.GetResultChannel(): {
                if result.StartBlock == currentBlock {
                    if _, err := m.output.Write(result.Data); err != nil {
                        return errors.Errorf("Could not write data to output: %v", err)

                    } else {
                        completed := calculateNumberOfCompletedBlocks(uint(len(result.Data)), uint(firstMissing.BlockSize))

                        if completed != (firstMissing.EndBlock - firstMissing.StartBlock) + 1 {
                            return errors.Errorf(
                                "Unexpected reponse length from remote source: blocks %v-%v (got %v blocks)",
                                firstMissing.StartBlock,
                                firstMissing.EndBlock,
                                completed)
                        }

                        currentBlock += completed
                        m.requiredRemoteBlocks = m.requiredRemoteBlocks[1:]
                    }

                } else {
                    return errors.Errorf("Received unexpected block: %v", result.StartBlock)
                }
            }
            case err := <-reference.EncounteredError(): {
                return errors.Errorf("Failed to read from reference file: %v", err)
            }
        }
    }

    return nil
}


func withinFirstBlockOfLocalBlocks(currentBlock uint, localBlocks []patcher.FoundBlockSpan) bool {
    return len(localBlocks) > 0 && localBlocks[0].StartBlock <= currentBlock && localBlocks[0].EndBlock >= currentBlock
}

func withinFirstBlockOfRemoteBlocks(currentBlock uint, remoteBlocks []patcher.MissingBlockSpan) bool {
    return len(remoteBlocks) > 0 && remoteBlocks[0].StartBlock <= currentBlock && remoteBlocks[0].EndBlock >= currentBlock
}

func calculateNumberOfCompletedBlocks(resultLength uint, blockSize uint) uint {
    var completedBlockCount uint = resultLength / blockSize

    // round up in the case of a partial block (last block may not be full sized)
    if resultLength % blockSize != 0 {
        completedBlockCount += 1
    }

    return completedBlockCount
}

func pickAvaiableReference(references []patcher.BlockSource) patcher.BlockSource {
    var (
        poolSize = len(references)
        pickedIndex int = 0
        source patcher.BlockSource
    )

    for {
        pickedIndex = rand.Intn(poolSize)
        source = references[pickedIndex]
        avail := <- source.CheckAvailable()
        if avail {
            return source
        }
    }
}
