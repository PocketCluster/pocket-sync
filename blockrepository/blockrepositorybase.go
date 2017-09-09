package blockrepository

import (
    "sort"
    "sync"
    "time"

    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/util/uslice"
)

const (
    REPOSITORY_RETRY_LIMIT int = 5
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
 * A BlockRepositoryOffsetResolver resolves a blockID to a start offset and an end offset in a file.
 * It also handles splitting up ranges of blocks into multiple requests, allowing requests to be split down to the
 * block size, and handling of compressed blocks (given a resolver that can work out the correct range to query for,
 * and a BlockSourceRequester that will decompress the result into a full sized block)
 */
type BlockRepositoryOffsetResolver interface {
    GetBlockStartOffset(blockID uint) int64
    GetBlockEndOffset(blockID uint) int64
    SplitBlockRangeToDesiredSize(reqBlk patcher.MissingBlockSpan) patcher.QueuedRequestList
}

// Checks blocks against their expected checksum
type BlockChecksumVerifier interface {
    // return results in order of strong checksum & error
    BlockChecksumForRange(startBlockID uint, data []byte) ([]byte, error)
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
    resolver     BlockRepositoryOffsetResolver,
    verifier     BlockChecksumVerifier,
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
    BlockSourceResolver    BlockRepositoryOffsetResolver
    Verifier               BlockChecksumVerifier

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
    errorC      chan *patcher.RepositoryError,
    responseC   chan patcher.RepositoryResponse,
) {
    var (
        retryCount   int    = 0
        sChksum      []byte = nil
        pendingErrors       = &ErrorWatcher{
            ErrorChannel: errorC,
        }
        pendingResponse     = &PendingResponseHelper{
            ResponseChannel: responseC,
        }
        requestQueue        = make(patcher.QueuedRequestList, 0, 2)
        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(uslice.UintSlice,          0, 1)
        responseOrdering    = make(PendingResponses,          0, 1)
    )

    defer func() {
        close(b.requestChannel)
        waiter.Done()
    }()

    // TODO : even when cancel signal is dispatched, repo source retrial keeps going. We need to fix & test that.
    requestLoop: for {
        if len(requestQueue) != 0 {
            // dispatch queued request
            nextRequest := requestQueue[len(requestQueue)-1]

            // if this is not a retrial
            if retryCount == 0 {
                requestOrdering = append(requestOrdering, nextRequest.StartBlock)
                sort.Sort(sort.Reverse(requestOrdering))
            }

            startOffset := b.BlockSourceResolver.GetBlockStartOffset(nextRequest.StartBlock)
            endOffset := b.BlockSourceResolver.GetBlockEndOffset(nextRequest.EndBlock)

            retryCount += 1
            response, err := b.Requester.DoRequest(startOffset, endOffset)
            result := blocksources.AsyncResult{
                StartBlockID: nextRequest.StartBlock,
                EndBlockID:   nextRequest.EndBlock,
                Data:         response,
                Err:          err,
            }

            // set the error
            if result.Err != nil {
                // if error present and hasn't been retried for REPOSITORY_RETRY_LIMIT...
                if retryCount < REPOSITORY_RETRY_LIMIT {
                    time.Sleep(time.Second)
                    continue requestLoop
                }

                // retryCount exceed limit. Report the error and continue to the next one
                requestQueue = requestQueue[:len(requestQueue)-1]
                requestOrdering = requestOrdering[:len(requestOrdering)-1]
                pendingResponse.Clear()
                pendingErrors.SetError(patcher.NewRepositoryError(
                    b.repositoryID,
                    nextRequest,
                    result.Err))
                goto resultReport
            }

            // verify hash
            if b.Verifier != nil {
                chksum, herr := b.Verifier.BlockChecksumForRange(result.StartBlockID, result.Data)
                if herr != nil {
                    // clear checksum vars for reuse
                    sChksum = nil

                    // if error present and hasn't been retried for REPOSITORY_RETRY_LIMIT...
                    if retryCount < REPOSITORY_RETRY_LIMIT {
                        time.Sleep(time.Second)
                        continue requestLoop
                    }

                    // retryCount exceed limit. Report the error and continue to the next one
                    requestQueue = requestQueue[:len(requestQueue)-1]
                    requestOrdering = requestOrdering[:len(requestOrdering)-1]
                    pendingResponse.Clear()
                    pendingErrors.SetError(patcher.NewRepositoryError(
                        b.repositoryID,
                        nextRequest,
                        errors.Errorf("The returned block range (%v-%v) did not match the expected checksum for the blocks. %v",
                            result.StartBlockID, result.EndBlockID, herr.Error())))
                    goto resultReport
                } else {
                    // when there is no error, take the checksums
                    sChksum = chksum
                }
            } else {
                sChksum = nil
            }

            // everything works great. remove request from queue and reset retryCount
            retryCount = 0
            requestQueue = requestQueue[:len(requestQueue)-1]

            // enqueue result
            responseOrdering = append(responseOrdering,
                patcher.RepositoryResponse{
                    RepositoryID:   b.repositoryID,
                    BlockID:        result.StartBlockID,
                    Data:           result.Data,
                    StrongChecksum: sChksum,
                })
            // sort high to low
            sort.Sort(sort.Reverse(responseOrdering))

            // clear checksum vars for reuse
            sChksum = nil

            // if we just got the lowest requested block, we can set the response. Otherwise, wait.
            lowestRequest := requestOrdering[len(requestOrdering)-1]

            if lowestRequest == result.StartBlockID {
                lowestResponse := responseOrdering[len(responseOrdering)-1]
                pendingResponse.Clear()
                pendingResponse.SetResponse(&lowestResponse)
            }
        }

        resultReport: select {
            case <-exitC: {
                return
            }

            case pendingErrors.SendIfSet() <- pendingErrors.Err(): {
                pendingErrors.Clear()
                return
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
                    b.BlockSourceResolver.SplitBlockRangeToDesiredSize(newRequest)...)

                sort.Sort(sort.Reverse(requestQueue))
            }
        }
    }
}
