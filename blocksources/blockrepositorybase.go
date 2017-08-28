package blocksources

import (
    "fmt"
    "sort"

//    log "github.com/Sirupsen/logrus"
//    "github.com/pkg/errors"
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
    return &BlockRepositoryBase{
        Requester:           requester,
        BlockSourceResolver: resolver,
        Verifier:            verifier,
        exitChannel:         make(chan bool),
        errorChannel:        make(chan error),
        responseChannel:     make(chan patcher.BlockReponse),
        requestChannel:      make(chan patcher.MissingBlockSpan),
    }
}

type BlockRepositoryBase struct {
    Requester              BlockSourceRequester
    BlockSourceResolver    BlockSourceOffsetResolver
    Verifier               BlockVerifier

    hasQuit                bool
    exitChannel            chan bool
    errorChannel           chan error
    responseChannel        chan patcher.BlockReponse
    requestChannel         chan patcher.MissingBlockSpan

    bytesRequested         int64
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
    return
}

func (b *BlockRepositoryBase) Patch() {
    var (
        state               = STATE_RUNNING

        pendingErrors       = &errorWatcher{
            errorChannel: b.errorChannel,
        }
        pendingResponse     = &pendingResponseHelper{
            responseChannel: b.responseChannel,
        }
        resultChan          = make(chan asyncResult)

        requestQueue        = make(QueuedRequestList, 0, 2)
        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(UintSlice,         0, 1)
        responseOrdering    = make(PendingResponses,  0, 1)
    )

    defer func() {
        b.hasQuit = true
        close(b.errorChannel)
        close(b.requestChannel)
        close(b.responseChannel)
        close(resultChan)
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

            default: {
                // when there is no work to proceed...
                if len(requestQueue) == 0 {
                    continue
                }

                nextRequest := requestQueue[len(requestQueue)-1]

                requestOrdering = append(requestOrdering, nextRequest.StartBlockID)
                sort.Sort(sort.Reverse(requestOrdering))

                startOffset := b.BlockSourceResolver.GetBlockStartOffset(
                    nextRequest.StartBlockID,
                )

                endOffset := b.BlockSourceResolver.GetBlockEndOffset(
                    nextRequest.EndBlockID,
                )

                response, err := b.Requester.DoRequest(
                    startOffset,
                    endOffset,
                )

                result := asyncResult{
                    startBlockID: nextRequest.StartBlockID,
                    endBlockID:   nextRequest.EndBlockID,
                    data:         response,
                    err:          err,
                }

                // remove dispatched request
                requestQueue = requestQueue[:len(requestQueue)-1]

                // TODO : add retry here and reject
                if result.err != nil {
                    pendingErrors.setError(result.err)
                    pendingResponse.clear()
                    state = STATE_EXITING
                    break
                }

                b.bytesRequested += int64(len(result.data))

                if b.Verifier != nil && !b.Verifier.VerifyBlockRange(result.startBlockID, result.data) {
                    pendingErrors.setError(
                        fmt.Errorf(
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
        }
    }
}