package patcher

import (
    "sync"
)

/*
 * BlockRepository is an interface used by the patchers to obtain blocks from one repository.
 * It does not stipulate where the reference data might be (it could be local, in a pre-built patch file, on S3 or
 * somewhere else)
 *
 * It is assumed that the BlockRepository may be slow, and may benefit from request pipelining & concurrency.
 * Therefore patchers should feel free to request as many consecutive block spans as they can handle.
 *
 * A BlockRepository may be a view onto a larger transport concept, so that multiple files can be handled with wider
 * knowledge of the number of simultaneous requests allowed, etc. The BlockRepository may decide to split BlockSpans
 * into smaller sizes if it wants.
 *
 * It is up to the patcher to receive blocks in a timely manner, and decide what to do with them, rather than
 * bother the BlockRepository with more memory management and buffering logic.
 *
 * Since these interfaces require specific structs to satisfy, it's expected that implementers will import this module.
 */
type BlockRepository interface {
    RepositoryID() uint
    RequestBlocks(MissingBlockSpan) error
    HandleRequest(
        waiter      *sync.WaitGroup,
        exitC       chan bool,
        errorC      chan *RepositoryError,
        responseC   chan RepositoryResponse,
    )
}

type RepositoryResponse struct {
    RepositoryID    uint
    BlockID         uint
    Data            []byte
}

type StackedReponse []RepositoryResponse

func (r StackedReponse) Len() int {
    return len(r)
}

func (r StackedReponse) Swap(i, j int) {
    r[i], r[j] = r[j], r[i]
}

func (r StackedReponse) Less(i, j int) bool {
    return r[i].BlockID < r[j].BlockID
}
