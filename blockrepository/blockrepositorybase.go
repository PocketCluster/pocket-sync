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

type BlockRepositorySourceState int
const (
    STATE_RUNNING            BlockRepositorySourceState = iota
    STATE_EXITING
)

func NewBlockRepositoryBase(
    requester    BlockRepositoryRequester,
    resolver     blocksources.BlockSourceOffsetResolver,
    verifier     blocksources.BlockVerifier,
) *BlockRepositoryBase {
    b := &BlockRepositoryBase{
        Requester:           requester,
        BlockSourceResolver: resolver,
        Verifier:            verifier,
        exitChannel:         make(chan bool),
        errorChannel:        make(chan error),
        responseChannel:     make(chan patcher.BlockReponse),
        requestChannel:      make(chan patcher.MissingBlockSpan),
    }

    b.closeWaiter.Add(1)
    go b.loopReorder()
    return b
}

type BlockRepositoryBase struct {
    Requester              BlockRepositoryRequester
    BlockSourceResolver    blocksources.BlockSourceOffsetResolver
    Verifier               blocksources.BlockVerifier

    hasQuit                bool
    bytesRequested         int64

    exitChannel            chan bool
    errorChannel           chan error
    responseChannel        chan patcher.BlockReponse
    requestChannel         chan patcher.MissingBlockSpan

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
func (b *BlockRepositoryBase) Close() error {
    close(b.exitChannel)
    b.closeWaiter.Wait()
    return nil
}

func (b *BlockRepositoryBase) loopReorder() {
    var (
        state               = STATE_RUNNING
        inflightRequests    = 0
        pendingErrors       = &blocksources.ErrorWatcher{
            ErrorChannel: b.errorChannel,
        }
        pendingResponse     = &blocksources.PendingResponseHelper{
            ResponseChannel: b.responseChannel,
        }

        requestQueue        = make(blocksources.QueuedRequestList, 0, 2)
        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(blocksources.UintSlice,         0, 1)
        responseOrdering    = make(blocksources.PendingResponses,  0, 1)
    )

    defer func() {
        b.hasQuit = true
        close(b.errorChannel)
        close(b.requestChannel)
        close(b.responseChannel)
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

                    if lowestRequest == lowestResponse.StartBlock {
                        pendingResponse.SetResponse(&lowestResponse)
                    }
                }
            }

            default: {
                if len(requestQueue) == 0 || inflightRequests != 0 {
                    continue
                }

                nextRequest := requestQueue[len(requestQueue)-1]

                requestOrdering = append(requestOrdering, nextRequest.StartBlockID)
                sort.Sort(sort.Reverse(requestOrdering))

                inflightRequests += 1

                // dispatch queued request
                startOffset := b.BlockSourceResolver.GetBlockStartOffset(nextRequest.StartBlockID)
                endOffset   := b.BlockSourceResolver.GetBlockEndOffset(nextRequest.EndBlockID)
                response, err := b.Requester.DoRequest(startOffset, endOffset)
                result := blocksources.AsyncResult{
                    StartBlockID: nextRequest.StartBlockID,
                    EndBlockID:   nextRequest.EndBlockID,
                    Data:         response,
                    Err:          err,
                }

                // remove dispatched request
                requestQueue = requestQueue[:len(requestQueue)-1]
                inflightRequests -= 1

                if result.Err != nil {
                    pendingErrors.SetError(result.Err)
                    pendingResponse.Clear()
                    state = STATE_EXITING
                    break
                }

                b.bytesRequested += int64(len(result.Data))

                if b.Verifier != nil && !b.Verifier.VerifyBlockRange(result.StartBlockID, result.Data) {
                    pendingErrors.SetError(
                        errors.Errorf(
                            "The returned block range (%v-%v) did not match the expected checksum for the blocks",
                            result.StartBlockID,
                            result.EndBlockID))
                    pendingResponse.Clear()
                    state = STATE_EXITING
                    break
                }

                responseOrdering = append(responseOrdering,
                    patcher.BlockReponse{
                        StartBlock: result.StartBlockID,
                        Data:       result.Data,
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
        }
    }
}