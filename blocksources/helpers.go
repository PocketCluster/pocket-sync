package blocksources

import (
    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
)

/*
 * ErrorWatcher is a small helper object.
 * SendIfSet will only return a channel if there is an error set, so w.SendIfSet() <- w.Error() is always safe in a select
 * statement even if there is no error set
 */
type ErrorWatcher struct {
    ErrorChannel chan error
    LastError    error
}

func (w *ErrorWatcher) SetError(e error) {
    if w.LastError != nil {
        log.Errorf(errors.Errorf("cannot set a new error when one is already set!").Error())
    }
    w.LastError = e
}

func (w *ErrorWatcher) Clear() {
    w.LastError = nil
}

func (w *ErrorWatcher) Err() error {
    return w.LastError
}

func (w *ErrorWatcher) SendIfSet() chan<- error {
    if w.LastError != nil {
        return w.ErrorChannel
    } else {
        return nil
    }
}

//-----------------------------------------------------------------------------
type PendingResponseHelper struct {
    ResponseChannel chan patcher.BlockReponse
    PendingResponse *patcher.BlockReponse
}

func (w *PendingResponseHelper) SetResponse(r *patcher.BlockReponse) {
    if w.PendingResponse != nil {
        log.Errorf(errors.Errorf("Setting a response when one is already set! Had startblock %v, got %v", r.StartBlock, w.PendingResponse.StartBlock).Error())
    }
    w.PendingResponse = r
}

func (w *PendingResponseHelper) Clear() {
    w.PendingResponse = nil
}

func (w *PendingResponseHelper) Response() patcher.BlockReponse {
    if w.PendingResponse == nil {
        return patcher.BlockReponse{}
    }
    return *w.PendingResponse
}

func (w *PendingResponseHelper) SendIfPending() chan<- patcher.BlockReponse {
    if w.PendingResponse != nil {
        return w.ResponseChannel
    } else {
        return nil
    }
}

//-----------------------------------------------------------------------------
type UintSlice []uint

func (r UintSlice) Len() int {
    return len(r)
}

func (r UintSlice) Swap(i, j int) {
    r[i], r[j] = r[j], r[i]
}

func (r UintSlice) Less(i, j int) bool {
    return r[i] < r[j]
}

//-----------------------------------------------------------------------------
type AsyncResult struct {
    StartBlockID uint
    EndBlockID   uint
    Data         []byte
    Err          error
}

//-----------------------------------------------------------------------------
type QueuedRequest struct {
    StartBlockID uint
    EndBlockID   uint
}

type QueuedRequestList []QueuedRequest

func (r QueuedRequestList) Len() int {
    return len(r)
}

func (r QueuedRequestList) Swap(i, j int) {
    r[i], r[j] = r[j], r[i]
}

func (r QueuedRequestList) Less(i, j int) bool {
    return r[i].StartBlockID < r[j].StartBlockID
}

//-----------------------------------------------------------------------------
func MakeNullFixedSizeResolver(blockSize uint64) BlockSourceOffsetResolver {
    return &FixedSizeBlockResolver{
        BlockSize: blockSize,
    }
}

func MakeFileSizedBlockResolver(blockSize uint64, filesize int64) BlockSourceOffsetResolver {
    return &FixedSizeBlockResolver{
        BlockSize: blockSize,
        FileSize:  filesize,
    }
}
