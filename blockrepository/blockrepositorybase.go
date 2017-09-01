package blockrepository

import (
    "sort"
    "sync"

    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/blocksources"
)

/*
 * BlockRepositoryRequester does synchronous requests on a remote source of blocks.
 * Concurrency is handled by the BlockRepositoryRequester.
 * This provides a simple way of implementing a particular
 */
type BlockRepositoryRequester interface {
    // This method is called on multiple goroutines, and must support simultaneous requests
    DoRequest(startOffset int64, endOffset int64) (data []byte, err error)

    // If an error raised by DoRequest should cause BlockSourceBase to give up, return true
    IsFatal(err error) bool
}

/*
 * BlockRepositoryBase provides an implementation of blocksource that takes care of every aspect of block handling from
 * a single repository except for the actual syncronous request.
 *
 * It is vastly similar to BlockSourceBase but differs in which detailed aspects (cancle, progress, & etc) of only
 * single request managed in monotonous, synchronous manner.
 *
 * BlockRepositoryBase implements patcher.BlockSource.
 */

func NewBlockRepositoryBase(
    repositoryID uint,
    requester    BlockRepositoryRequester,
    resolver     blocksources.BlockSourceOffsetResolver,
    verifier     blocksources.BlockVerifier,
) *BlockRepositoryBase {
    b := &BlockRepositoryBase{
        Requester:           requester,
        BlockSourceResolver: resolver,
        Verifier:            verifier,
        requestChannel:      make(chan patcher.MissingBlockSpan),
        repositoryID:        repositoryID,
    }
    return b
}

type BlockRepositoryBase struct {
    Requester              BlockRepositoryRequester
    BlockSourceResolver    blocksources.BlockSourceOffsetResolver
    Verifier               blocksources.BlockVerifier

    requestChannel         chan patcher.MissingBlockSpan
    repositoryID           uint
}

func (b *BlockRepositoryBase) RepositoryID() uint {
    return b.repositoryID
}

func (b *BlockRepositoryBase) RequestBlocks(block patcher.MissingBlockSpan) error {
    b.requestChannel <- block
    return nil
}

func (b *BlockRepositoryBase) HandleRequest(
    waiter      *sync.WaitGroup,
    exitC       chan bool,
    errorC      chan error,
    responseC   chan patcher.RepositoryResponse,
) {
    var (
        pendingErrors       = &blocksources.ErrorWatcher{
            ErrorChannel: errorC,
        }
        pendingResponse     = &PendingResponseHelper{
            ResponseChannel: responseC,
        }
        requestQueue        = make(blocksources.QueuedRequestList, 0, 2)
        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(blocksources.UintSlice,         0, 1)
        responseOrdering    = make(PendingResponses,               0, 1)
    )

    defer func() {
        close(b.requestChannel)
        waiter.Done()
    }()

    for {
        if len(requestQueue) != 0 {
            // dispatch queued request
            nextRequest := requestQueue[len(requestQueue)-1]
            requestOrdering = append(requestOrdering, nextRequest.StartBlockID)
            sort.Sort(sort.Reverse(requestOrdering))

            startOffset := b.BlockSourceResolver.GetBlockStartOffset(nextRequest.StartBlockID)
            endOffset := b.BlockSourceResolver.GetBlockEndOffset(nextRequest.EndBlockID)

            response, err := b.Requester.DoRequest(startOffset, endOffset)
            result := blocksources.AsyncResult{
                StartBlockID: nextRequest.StartBlockID,
                EndBlockID:   nextRequest.EndBlockID,
                Data:         response,
                Err:          err,
            }

            // remove dispatched request
            requestQueue = requestQueue[:len(requestQueue)-1]

            // set the error
            if result.Err != nil {
                pendingErrors.SetError(result.Err)
                pendingResponse.Clear()
                continue
            }

            // verify hash
            if b.Verifier != nil && !b.Verifier.VerifyBlockRange(result.StartBlockID, result.Data) {
                pendingErrors.SetError(
                    errors.Errorf(
                        "The returned block range (%v-%v) did not match the expected checksum for the blocks",
                        result.StartBlockID,
                        result.EndBlockID))
                pendingResponse.Clear()
                continue
            }

            // enqueue result
            responseOrdering = append(responseOrdering,
                patcher.RepositoryResponse{
                    RepositoryID: b.repositoryID,
                    BlockID:      result.StartBlockID,
                    Data:         result.Data,
                })

            // sort high to low
            sort.Sort(sort.Reverse(responseOrdering))

            // if we just got the lowest requested block, we can set the response. Otherwise, wait.
            lowestRequest := requestOrdering[len(requestOrdering)-1]

            if lowestRequest == result.StartBlockID {
                lowestResponse := responseOrdering[len(responseOrdering)-1]
                pendingResponse.Clear()
                pendingResponse.SetResponse(&lowestResponse)
            }
        }

        select {
            case <-exitC: {
                return
            }

            case pendingErrors.SendIfSet() <- pendingErrors.Err(): {
                pendingErrors.Clear()
            }

            case pendingResponse.SendIfPending() <- pendingResponse.Response(): {
                pendingResponse.Clear()
                responseOrdering = responseOrdering[:len(responseOrdering)-1]
                requestOrdering = requestOrdering[:len(requestOrdering)-1]

                // check if there's another response to enqueue
                if len(responseOrdering) > 0 {
                    lowestResponse := responseOrdering[len(responseOrdering)-1]
                    lowestRequest := requestOrdering[len(requestOrdering)-1]

                    if lowestRequest == lowestResponse.BlockID {
                        pendingResponse.SetResponse(&lowestResponse)
                    }
                }
            }

            case newRequest := <- b.requestChannel: {
                requestQueue = append(
                    requestQueue,
                    b.BlockSourceResolver.SplitBlockRangeToDesiredSize(
                        newRequest.StartBlock,
                        newRequest.EndBlock,
                    )...)

                sort.Sort(sort.Reverse(requestQueue))
            }
        }
    }
}