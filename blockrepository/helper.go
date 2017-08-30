package blockrepository

import (
    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
)

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

