package blocksources

import (
    "sort"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
    "fmt"
)

/*
 * SingleBlockRepository provides an implementation of blocksource that takes care of every aspect of block handling
 * from a repository except for the actual syncronous request. It is vastly similar to BlockSourceBase but differs in
 * which detailed aspects (cancle, progress, & etc) of only single request managed in monotonous, synchronous manner.
 *
 * SingleBlockRepository implements patcher.BlockSource.
 */

func NewSingleBlockRepository(
    requester    BlockSourceRequester,
    resolver     BlockSourceOffsetResolver,
    verifier     BlockVerifier,
) *SingleBlockRepository {
    return &SingleBlockRepository{
        Requester:           requester,
        BlockSourceResolver: resolver,
        Verifier:            verifier,
        exitChannel:         make(chan bool),
        errorChannel:        make(chan error),
        responseChannel:     make(chan patcher.BlockReponse),
        requestChannel:      make(chan patcher.MissingBlockSpan),
    }
}

type SingleBlockRepository struct {
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

func (s *SingleBlockRepository) ReadBytes() int64 {
    return s.bytesRequested
}

func (s *SingleBlockRepository) RequestBlocks(block patcher.MissingBlockSpan) error {
    s.requestChannel <- block
    return nil
}

func (s *SingleBlockRepository) GetResultChannel() <-chan patcher.BlockReponse {
    return s.responseChannel
}

// If the block source encounters an unsurmountable problem
func (s *SingleBlockRepository) EncounteredError() <-chan error {
    return s.errorChannel
}

func (s *SingleBlockRepository) Close() (err error) {
    // if it has already been closed, just recover
    // however, let the caller know
    defer func() {
        if recover() != nil {
            err = BlockSourceAlreadyClosedError
        }
    }()

    if !s.hasQuit {
        s.exitChannel <- true
    }

    return
}

func (s *SingleBlockRepository) Patch() {
    var (
        state               = STATE_RUNNING
        inflightRequests    = 0

        pendingErrors       = &errorWatcher{
            errorChannel: s.errorChannel,
        }
        pendingResponse     = &pendingResponseHelper{
            responseChannel: s.responseChannel,
        }
        resultChan          = make(chan asyncResult)
        requestQueue        = make(QueuedRequestList, 0, s.ConcurrentRequests * 2)

        // enable us to order responses for the active requests, lowest to highest
        requestOrdering     = make(UintSlice,         0, s.ConcurrentRequests)
        responseOrdering    = make(PendingResponses,  0, s.ConcurrentRequests)
    )

    defer func() {
        s.hasQuit = true
        close(s.exitChannel)
        close(s.errorChannel)
        close(s.requestChannel)
        close(s.responseChannel)
        close(resultChan)
    }()


    for state == STATE_RUNNING || inflightRequests > 0 || pendingErrors.Err() != nil {

        select {
            case <-s.exitChannel: {
                state = STATE_EXITING
            }

            case newRequest := <-s.requestChannel: {
                requestQueue = append(
                    requestQueue,
                    s.BlockSourceResolver.SplitBlockRangeToDesiredSize(
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

                startOffset := s.BlockSourceResolver.GetBlockStartOffset(
                    nextRequest.StartBlockID,
                )

                endOffset := s.BlockSourceResolver.GetBlockEndOffset(
                    nextRequest.EndBlockID,
                )

                response, err := s.Requester.DoRequest(
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

                if result.err != nil {
                    pendingErrors.setError(result.err)
                    pendingResponse.clear()
                    state = STATE_EXITING
                    break
                }

                s.bytesRequested += int64(len(result.data))

                if s.Verifier != nil && !s.Verifier.VerifyBlockRange(result.startBlockID, result.data) {
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

                // ???? what situation do we fall in this?
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