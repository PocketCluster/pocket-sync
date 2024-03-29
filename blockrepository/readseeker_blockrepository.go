package blockrepository

import (
    "github.com/Redundancy/go-sync/blocksources"
)

func NewReadSeekerBlockRepository(
    repositoryID uint,
    readSeeker   blocksources.ReadSeeker,
    resolver     BlockRepositoryOffsetResolver,
    verifier     BlockChecksumVerifier,
) *BlockRepositoryBase {
    return NewBlockRepositoryBase(
        repositoryID,
        blocksources.NewReadSeekerRequester(readSeeker),
        resolver,
        verifier,
    )
}
