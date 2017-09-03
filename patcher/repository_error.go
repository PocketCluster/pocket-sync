package patcher

type RepositoryError struct{
    repositoryID    uint
    missingBlk      MissingBlockSpan
    reasonErr       error
}

func NewRepositoryError(
    repositoryID    uint,
    missingBlock    MissingBlockSpan,
    reasonErr       error,
) *RepositoryError {
    return &RepositoryError{
        repositoryID: repositoryID,
        missingBlk:   missingBlock,
        reasonErr:    reasonErr,
    }
}

func (e *RepositoryError) Error() string {
    return e.reasonErr.Error()
}

func (e *RepositoryError) RepositoryID() uint {
    return e.repositoryID
}

func (e *RepositoryError) MissingBlock() MissingBlockSpan {
    return e.missingBlk
}
