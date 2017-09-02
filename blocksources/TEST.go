package blocksources

//-----------------------------------------------------------------------------
type ErroringRequester struct{
    requestCount uint
}

type TestError struct{}

func (e *TestError) Error() string {
    return "test"
}

func (e *ErroringRequester) DoRequest(startOffset int64, endOffset int64) (data []byte, err error) {
    e.requestCount += 1
    return nil, &TestError{}
}

func (e *ErroringRequester) IsFatal(err error) bool {
    return true
}

func (e *ErroringRequester) RequestCount() uint {
    return e.requestCount
}

//-----------------------------------------------------------------------------
type FunctionRequester func(a, b int64) ([]byte, error)

func (f FunctionRequester) DoRequest(startOffset int64, endOffset int64) (data []byte, err error) {
    return f(startOffset, endOffset)
}

func (f FunctionRequester) IsFatal(err error) bool {
    return true
}
