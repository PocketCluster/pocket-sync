package blocksources

import (
    "sort"
    "sync"

    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
)

/*
 * BlockRepositoryBase provides an implementation of blocksource that takes care of every aspect of block handling from
 * a single repository except for the actual syncronous request.

 * It is vastly similar to BlockSourceBase but differs in which detailed aspects (cancle, progress, & etc) of only
 * single request managed in monotonous, synchronous manner.
 *
 * BlockRepositoryBase implements patcher.BlockSource.
 */

func NewBlockRepositoryBase(
    requester    BlockSourceRequester,
    resolver     BlockSourceOffsetResolver,
    verifier     BlockVerifier,
) *BlockRepositoryBase {
    b := &BlockRepositoryBase{
        Requester:           requester,
        BlockSourceResolver: resolver,
        Verifier:            verifier,
        exitChannel:         make(chan bool),
        errorChannel:        make(chan error),
        responseChannel:     make(chan patcher.BlockReponse),
        requestChannel:      make(chan patcher.MissingBlockSpan),
        iResultC:            make(chan asyncResult),
        iRequestC:           make(chan QueuedRequest),
    }

    b.closeWaiter.Add(2)

    go b.loopRequest()
    go b.loopReorder()

    return b
}

type BlockRepositoryBase struct {
    Requester              BlockSourceRequester
    BlockSourceResolver    BlockSourceOffsetResolver
    Verifier               BlockVerifier

    hasQuit                bool
    bytesRequested         int64

    exitChannel            chan bool
    errorChannel           chan error
    responseChannel        chan patcher.BlockReponse
    requestChannel         chan patcher.MissingBlockSpan

    // these two are to exchange result/request
    iResultC               chan asyncResult
    iRequestC              chan QueuedRequest

    closeWaiter            sync.WaitGroup
}

func (b *BlockRepositoryBase) ReadBytes() int64 {
    return b.bytesRequested
}

func (b *BlockRepositoryBase) RequestBlocks(block patcher.MissingBlockSpan) error {
    b.requestChannel <- block
    return nil
}

func (b *BlockRepositoryBase) GetResultChannel() <-chan patcher.BlockReponse {
    return b.responseChannel
}

// If the block source encounters an unsurmountable problem
func (b *BlockRepositoryBase) EncounteredError() <-chan error {
    return b.errorChannel
}

// Close function has to be super simple.
// If something goes wrong after closing the exitChannel, it's caller's fault.
func (b *BlockRepositoryBase) Close() (err error) {
    close(b.exitChannel)

    b.closeWaiter.Wait()

    close(b.errorChannel)
    close(b.requestChannel)
    close(b.responseChannel)
    close(b.iResultC)
    close(b.iRequestC)
    return
}

func (b *BlockRepositoryBase) loopRequest() {
    var (
        resolver   = b.BlockSourceResolver
        requester  = b.Requester
    )

    defer b.closeWaiter.Done()

    for {
        select {
            case <- b.exitChannel: {
                return
            }
            case nextRequest := <- b.iRequestC: {
                var (
                    startOffset = resolver.GetBlockStartOffset(nextRequest.StartBlockID)
                    endOffset   = resolver.GetBlockEndOffset(nextRequest.EndBlockID)
                    result, err = requester.DoRequest(startOffset, endOffset)
                )
                b.iResultC <- asyncResult{
                    startBlockID: nextRequest.StartBlockID,
                    endBlockID:   nextRequest.EndBlockID,
                    data:         result,
                    err:          err,
                }
            }
        }
    }
}

func (b *BlockRepositoryBase) loopReorder() {
    var (
        state               = STATE_RUNNING
        inflightRequests    = 0
        pendingErrors       = &errorWatcher{
            errorChannel: b.errorChannel,
        }
        pendingResponse     = &pendingResponseHelper{
            responseChannel: b.responseChannel,
        }

        requestQueue        = make(QueuedRequestList, 0, 2)
        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(UintSlice,         0, 1)
        responseOrdering    = make(PendingResponses,  0, 1)
    )

    defer func() {
        b.hasQuit = true
        b.closeWaiter.Done()
    }()

    for state == STATE_RUNNING || pendingErrors.Err() != nil {

        select {
            case <-b.exitChannel: {
                state = STATE_EXITING
                return
            }

            case newRequest := <-b.requestChannel: {
                requestQueue = append(
                    requestQueue,
                    b.BlockSourceResolver.SplitBlockRangeToDesiredSize(
                        newRequest.StartBlock,
                        newRequest.EndBlock,
                    )...)

                sort.Sort(sort.Reverse(requestQueue))
            }

            case pendingErrors.sendIfSet() <- pendingErrors.Err(): {
                pendingErrors.clear()
            }

            case pendingResponse.sendIfPending() <- pendingResponse.Response(): {
                pendingResponse.clear()
                responseOrdering = responseOrdering[:len(responseOrdering)-1]
                requestOrdering = requestOrdering[:len(requestOrdering)-1]

                // check if there's another response to enqueue
                if len(responseOrdering) > 0 {
                    lowestResponse := responseOrdering[len(responseOrdering)-1]
                    lowestRequest := requestOrdering[len(requestOrdering)-1]

                    if lowestRequest == lowestResponse.StartBlock {
                        pendingResponse.setResponse(&lowestResponse)
                    }
                }
            }

            case result := <- b.iResultC: {
                inflightRequests -= 1

                if result.err != nil {
                    pendingErrors.setError(result.err)
                    pendingResponse.clear()
                    state = STATE_EXITING
                    break
                }

                b.bytesRequested += int64(len(result.data))

                if b.Verifier != nil && !b.Verifier.VerifyBlockRange(result.startBlockID, result.data) {
                    pendingErrors.setError(
                        errors.Errorf(
                            "The returned block range (%v-%v) did not match the expected checksum for the blocks",
                            result.startBlockID,
                            result.endBlockID))
                    pendingResponse.clear()
                    state = STATE_EXITING
                    break
                }

                responseOrdering = append(responseOrdering,
                    patcher.BlockReponse{
                        StartBlock: result.startBlockID,
                        Data:       result.data,
                    })

                // sort high to low
                sort.Sort(sort.Reverse(responseOrdering))

                // if we just got the lowest requested block, we can set
                // the response. Otherwise, wait.
                lowestRequest := requestOrdering[len(requestOrdering)-1]

                if lowestRequest == result.startBlockID {
                    lowestResponse := responseOrdering[len(responseOrdering)-1]
                    pendingResponse.clear()
                    pendingResponse.setResponse(&lowestResponse)
                }
            }

            default: {
                if len(requestQueue) == 0 || inflightRequests != 0 {
                    continue
                }
                nextRequest := requestQueue[len(requestQueue)-1]

                requestOrdering = append(requestOrdering, nextRequest.StartBlockID)
                sort.Sort(sort.Reverse(requestOrdering))

                // dispatch queued request
                inflightRequests += 1
                b.iRequestC <- nextRequest

                // remove dispatched request
                requestQueue = requestQueue[:len(requestQueue)-1]
            }
        }
    }
}