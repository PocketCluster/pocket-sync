package patcher

type RepositoryError struct{
    repositoryID    uint
    startBlockID    uint
    endBlockID      uint
    reasonErr       error
}

func NewRepositoryError(
    repositoryID    uint,
    startBlockID    uint,
    endBlockID      uint,
    reasonErr       error,
) *RepositoryError {
    return &RepositoryError{
        repositoryID:    repositoryID,
        startBlockID:    startBlockID,
        endBlockID:      endBlockID,
        reasonErr:       reasonErr,
    }
}

func (e *RepositoryError) Error() string {
    return e.reasonErr.Error()
}

func (e *RepositoryError) RepositoryID() uint {
    return e.repositoryID
}

func (e *RepositoryError) StartBlockID() uint {
    return e.startBlockID
}

func (e *RepositoryError) EndBlockID() uint {
    return e.endBlockID
}
