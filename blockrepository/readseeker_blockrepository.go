package blockrepository

import (
    "github.com/Redundancy/go-sync/blocksources"
)

func NewReadSeekerBlockRepository(
    repositoryID uint,
    readSeeker   blocksources.ReadSeeker,
    resolver     BlockRepositoryOffsetResolver,
) *BlockRepositoryBase {
    return NewBlockRepositoryBase(
        repositoryID,
        blocksources.NewReadSeekerRequester(readSeeker),
        resolver,
        nil, // TODO: No verifier!
    )
}
