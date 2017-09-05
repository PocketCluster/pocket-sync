package blockrepository

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
    ErrorChannel chan *patcher.RepositoryError
    LastError    *patcher.RepositoryError
}

func (w *ErrorWatcher) SetError(e *patcher.RepositoryError) {
    if w.LastError != nil {
        log.Errorf(errors.Errorf("cannot set a new error when one is already set!").Error())
    }
    w.LastError = e
}

func (w *ErrorWatcher) Clear() {
    w.LastError = nil
}

func (w *ErrorWatcher) Err() *patcher.RepositoryError {
    return w.LastError
}

func (w *ErrorWatcher) SendIfSet() chan<- *patcher.RepositoryError {
    if w.LastError != nil {
        return w.ErrorChannel
    } else {
        return nil
    }
}

//-----------------------------------------------------------------------------
type PendingResponseHelper struct {
    ResponseChannel chan patcher.RepositoryResponse
    PendingResponse *patcher.RepositoryResponse
}

func (w *PendingResponseHelper) SetResponse(r *patcher.RepositoryResponse) {
    if w.PendingResponse != nil {
        log.Errorf(errors.Errorf("Setting a response when one is already set! Had blockID %v, got %v", r.BlockID, w.PendingResponse.BlockID).Error())
    }
    w.PendingResponse = r
}

func (w *PendingResponseHelper) Clear() {
    w.PendingResponse = nil
}

func (w *PendingResponseHelper) Response() patcher.RepositoryResponse {
    if w.PendingResponse == nil {
        return patcher.RepositoryResponse{}
    }
    return *w.PendingResponse
}

func (w *PendingResponseHelper) SendIfPending() chan<- patcher.RepositoryResponse {
    if w.PendingResponse != nil {
        return w.ResponseChannel
    } else {
        return nil
    }
}

//-----------------------------------------------------------------------------
type PendingResponses []patcher.RepositoryResponse

func (r PendingResponses) Len() int {
    return len(r)
}

func (r PendingResponses) Swap(i, j int) {
    r[i], r[j] = r[j], r[i]
}

func (r PendingResponses) Less(i, j int) bool {
    return r[i].BlockID < r[j].BlockID
}

//-----------------------------------------------------------------------------
func MakeNullUniformSizeResolver(blockSize int64) *UniformSizeBlockResolver {
    return &UniformSizeBlockResolver{
        BlockSize: blockSize,
    }
}

func MakeKnownFileSizedBlockResolver(blockSize int64, filesize int64) *UniformSizeBlockResolver {
    return &UniformSizeBlockResolver{
        BlockSize: blockSize,
        FileSize:  filesize,
    }
}

//-----------------------------------------------------------------------------
type FunctionChecksumVerifier func(startBlockID uint, data []byte) ([]byte, error)

func (f FunctionChecksumVerifier) BlockChecksumForRange(startBlockID uint, data []byte) ([]byte, error) {
    return f(startBlockID, data)
}