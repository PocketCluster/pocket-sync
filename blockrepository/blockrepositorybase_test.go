package blockrepository

import (
    "bytes"
    "sync"
    "testing"
    "time"

    "github.com/Redundancy/go-sync/patcher"
    "github.com/Redundancy/go-sync/blocksources"
)

const (
    block_size uint64 = 8
)

func Test_BlockRepositoryBase_CreateAndClose(t *testing.T) {
    var (
        b = NewBlockRepositoryBase(0, nil,
            blocksources.MakeNullFixedSizeResolver(block_size),
            nil)
        waiter      = sync.WaitGroup{}
        exitC       = make(chan bool)
        errorC      = make(chan *patcher.RepositoryError)
        responseC   = make(chan patcher.RepositoryResponse)
        requestC    = make(chan patcher.MissingBlockSpan)
    )
    defer func() {
        close(errorC)
        close(responseC)
        close(requestC)
    }()
    waiter.Add(1)
    go func() {
        b.HandleRequest(&waiter, exitC, errorC, responseC)
    }()

    close(exitC)
    waiter.Wait()
}

func Test_BlockRepository_Basic_Error(t *testing.T) {
    var (
        r = &blocksources.ErroringRequester{}
        b = NewBlockRepositoryBase(0,
            r,
            blocksources.MakeNullFixedSizeResolver(block_size),
            nil)
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
        BlockSize:  4,
        StartBlock: 1,
        EndBlock:   1,
    })

    select {
        case <-time.After(time.Second * 10):
            t.Fatal("Timed out waiting for error")
        case err := <-errorC:
            t.Log(err.Error())
    }
    if r.RequestCount() != REPOSITORY_RETRY_LIMIT {
        t.Fatalf("RequestCount should be equal to repository retry limit RequestCount = %v", r.RequestCount())
    }
}

func Test_BlockRepository_Retry_Verify_Error(t *testing.T) {
    var (
        errorCount = 0
        errorCountC = make(chan int)
        b = NewBlockRepositoryBase(0,
            blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                errorCountC <- 1
                return nil, &blocksources.TestError{}
            }),
            blocksources.MakeNullFixedSizeResolver(block_size),
            blocksources.FunctionVerifier(func(startBlockID uint, data []byte) bool {
                errorCountC <- 1
                return false
            }))
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
        close(errorCountC)
    }()
    waiter.Add(1)
    go func() {
        b.HandleRequest(&waiter, exitC, errorC, responseC)
    }()

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  4,
        StartBlock: 1,
        EndBlock:   1,
    })


    resultCheck: for {
        select {
            case <-time.After(time.Second * 10):
                t.Fatal("Timed out waiting for error")
                return
            case err := <-errorC:
                t.Log(err.Error())
                break resultCheck
            case e := <- errorCountC:
                errorCount += e
        }
    }

    if errorCount != REPOSITORY_RETRY_LIMIT {
        t.Fatalf("RequestCount should be equal to repository retry limit RequestCount = %v", errorCount)
    }
}

func Test_BlockRepository_Retry_Verify_Partial_Error(t *testing.T) {
    const (
        partial_error_limit = 4
    )
    var (
        errorCount = 0
        errorCountC = make(chan int)
        errorLimitC = make(chan int)
        b = NewBlockRepositoryBase(0,
            blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                if partial_error_limit <= <- errorLimitC {
                    return []byte{0x1A}, nil
                }

                errorCountC <- 1
                return nil, &blocksources.TestError{}
            }),
            blocksources.MakeNullFixedSizeResolver(block_size),
            blocksources.FunctionVerifier(func(startBlockID uint, data []byte) bool {
                if partial_error_limit <= <- errorLimitC {
                    return true
                }

                errorCountC <- 1
                return false
            }))
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
        close(errorCountC)
        close(errorLimitC)
    }()
    waiter.Add(1)
    go func() {
        b.HandleRequest(&waiter, exitC, errorC, responseC)
    }()

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  4,
        StartBlock: 1,
        EndBlock:   1,
    })

    resultCheck: for {
        select {
            case <-time.After(time.Second * 10): {
                t.Fatal("Timed out waiting for error")
                return
            }
            case err := <-errorC: {
                t.Log(err.Error())
                break resultCheck
            }
            case e := <- errorCountC: {
                errorCount += e
            }
            case errorLimitC <- errorCount: {
            }
            case r := <- responseC: {
                if bytes.Compare(r.Data, []byte{0x1A}) != 0 {
                    t.Fatalf("unexpected result %v", r.Data)
                }
                break resultCheck
            }
        }
    }

    if errorCount != partial_error_limit {
        t.Fatalf("RequestCount should be equal to partial error limit | RequestCount = %v", errorCount)
    }
}

func Test_BlockRepositoryBase_Request(t *testing.T) {
    var (
        expected = []byte("test")
        b = NewBlockRepositoryBase(0,
            blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                return expected, nil
            }),
            blocksources.MakeNullFixedSizeResolver(block_size),
            nil,
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
        BlockSize:  4,
        StartBlock: 1,
        EndBlock:   1,
    })

    result := <- responseC

    if result.BlockID != 1 {
        t.Errorf("Unexpected start block in result: %v", result.BlockID)
    }
    if bytes.Compare(result.Data, expected) != 0 {
        t.Errorf("Unexpected data in result: %v", result.Data)
    }
}

func Test_BlockRepositoryBase_Consequent_Request(t *testing.T) {
    var (
        content = []byte("test")

        b = NewBlockRepositoryBase(0,
            blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                return content[start:end], nil
            }),
            blocksources.MakeNullFixedSizeResolver(2),
            nil,
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

    for i := uint(0); i < 2; i++ {
        b.RequestBlocks(patcher.MissingBlockSpan{
            BlockSize:  2,
            StartBlock: uint(i),
            EndBlock:   uint(i),
        })

        select {
            case r := <- responseC: {
                if r.BlockID != i {
                    t.Errorf("Wrong start block: %v", r.BlockID)
                }
                if bytes.Compare(r.Data, content[i*2:(i+1)*2]) != 0 {
                    t.Errorf("Unexpected result content for result %v: %v", i+1, string(r.Data))
                }
            }
            case <-time.After(time.Second): {
                t.Fatal("Timed out on request", i+1)
            }
        }
    }
}

func Test_BlockRepositoryBase_OrderedRequestCompletion(t *testing.T) {
    var (
        content = []byte("test")

        channeler = []chan bool{
            make(chan bool),
            make(chan bool),
        }

        b = NewBlockRepositoryBase(0,
            blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                // read from the channel based on the start
                <-(channeler[start])
                return content[start:end], nil
            }),
            blocksources.MakeNullFixedSizeResolver(1),
            nil,
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

    for i := uint(0); i < 2; i++ {

        b.RequestBlocks(patcher.MissingBlockSpan{
            BlockSize:  1,
            StartBlock: i,
            EndBlock:   i,
        })

        channeler[i] <- true

        select {
            case r := <- responseC:
                if r.BlockID != i {
                    t.Errorf(
                        "Wrong start block: %v on result %v",
                        r.BlockID,
                        i+1,
                    )
                }
            case <-time.After(time.Second):
                t.Fatal("Timed out on request", i+1)
        }
    }
}

func Test_BlockRepositoryBase_RequestCountLimiting(t *testing.T) {
    const (
        REQUESTS     = 4
    )
    var (
        call_counter = 0
        count        = 0
        max          = 0
        counterC     = make(chan int)
        waiterC      = make(chan bool)

        b = NewBlockRepositoryBase(0,
            blocksources.FunctionRequester(func(start, end int64) (data []byte, err error) {
                t.Logf("FunctionRequester start %d", start)
                counterC <- 1
                call_counter += 1
                <-waiterC
                counterC <- -1
                return []byte{0, 0}, nil
            }),
            blocksources.MakeNullFixedSizeResolver(1),
            nil,
        )

        waiter       = sync.WaitGroup{}
        exitC        = make(chan bool)
        errorC       = make(chan *patcher.RepositoryError)
        responseC    = make(chan patcher.RepositoryResponse)
    )
    defer func() {
        close(exitC)
        waiter.Wait()
        close(errorC)
        close(responseC)
        close(counterC)
        close(waiterC)
    }()
    waiter.Add(1)
    go func() {
        b.HandleRequest(&waiter, exitC, errorC, responseC)
    }()

    go func() {
        for {
            select {
                case <- exitC: {
                    return
                }
                case change, ok := <-counterC: {
                    if !ok {
                        return
                    }

                    count += change

                    if count > max {
                        max = count
                    }
                }

            }
        }
    }()

    for i := 0; i < REQUESTS; i++ {
        t.Logf("RequestBlocks %d", i)
        b.RequestBlocks(patcher.MissingBlockSpan{
            BlockSize:  1,
            StartBlock: uint(i),
            EndBlock:   uint(i),
        })

        waiterC <- true
        <- responseC

        if max > 1 {
            t.Errorf("Maximum requests in flight was greater than the requested concurrency: %v", max)
        }
    }
    if call_counter != REQUESTS {
        t.Errorf("Total number of requests is not expected: %v", call_counter)
    }
}
