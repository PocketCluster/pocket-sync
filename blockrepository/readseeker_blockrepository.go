package blockrepository

import (
    "github.com/Redundancy/go-sync/blocksources"
)

func NewReadSeekerBlockRepository(
    r blocksources.ReadSeeker,
    resolver blocksources.BlockSourceOffsetResolver,
) *BlockRepositoryBase {
    return NewBlockRepositoryBase(
        blocksources.NewReadSeekerRequester(r),
        resolver,
        nil, // TODO: No verifier!
    )
}
