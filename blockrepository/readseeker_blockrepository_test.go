package blockrepository

import (
    "bytes"
    "testing"
    "sync"

    "github.com/Redundancy/go-sync/patcher"
)

func TestReadFirstBlock(t *testing.T) {
    const (
        STRING_DATA = "abcdefghijklmnopqrst"
        BLOCK_SIZE = 8
    )
    var (
        b = NewReadSeekerBlockRepository(
            0,
            bytes.NewReader(
                []byte(STRING_DATA),
            ),
            MakeNullUniformSizeResolver(BLOCK_SIZE),
        )
        waiter      = sync.WaitGroup{}
        exitC       = make(chan bool)
        errorC      = make(chan *patcher.RepositoryError)
        responseC   = make(chan patcher.RepositoryResponse)
    )
    defer func() {
        close(exitC)
        waiter.Wait()
        close(errorC)
        close(responseC)
    }()
    waiter.Add(1)
    go func() {
        b.HandleRequest(&waiter, exitC, errorC, responseC)
    }()

    b.RequestBlocks(patcher.MissingBlockSpan{
            BlockSize:  BLOCK_SIZE,
            StartBlock: 0,
            EndBlock:   0,
    })

    result := <-responseC

    if result.BlockID != 0 {
        t.Errorf("Wrong start block: %v", result.BlockID)
    }

    EXPECTED := STRING_DATA[:BLOCK_SIZE]
    if bytes.Compare(result.Data, []byte(EXPECTED)) != 0 {
        t.Errorf(
            "Unexpected result data: \"%v\" expected: \"%v\"",
            string(result.Data),
            EXPECTED,
        )
    }
}
