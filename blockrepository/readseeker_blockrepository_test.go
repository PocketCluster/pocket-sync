package blockrepository

import (
    "bytes"
    "testing"
    "sync"

    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/blocksources"
)

func TestReadFirstBlock(t *testing.T) {
    const (
        STRING_DATA = "abcdefghijklmnopqrst"
        BLOCK_SIZE = 8
    )
    var (
        b = NewReadSeekerBlockRepository(
            bytes.NewReader(
                []byte(STRING_DATA),
            ),
            blocksources.MakeNullFixedSizeResolver(BLOCK_SIZE),
        )
        waiter      = sync.WaitGroup{}
        exitC       = make(chan bool)
        errorC      = make(chan error)
        responseC   = make(chan patcher.BlockReponse)
        requestC    = make(chan patcher.MissingBlockSpan)
    )
    defer func() {
        close(exitC)
        waiter.Wait()
        close(errorC)
        close(responseC)
        close(requestC)
    }()
    waiter.Add(1)
    go func() {
        b.HandleRequest(&waiter, exitC, errorC, responseC, requestC)
    }()

    requestC <- patcher.MissingBlockSpan{
            BlockSize:  BLOCK_SIZE,
            StartBlock: 0,
            EndBlock:   0,
        }

    result := <-responseC

    if result.StartBlock != 0 {
        t.Errorf("Wrong start block: %v", result.StartBlock)
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
