package blocksources

import (
    "bytes"
    "testing"
    "time"

    "github.com/Redundancy/go-sync/patcher"
)

func Test_BlockRepositoryBase_CreateAndClose(t *testing.T) {
    var (
        b = NewBlockRepositoryBase(nil, nil, nil)
    )

    err := b.Close()
    if err != nil {
        t.Error(err.Error())
    }

    if !b.hasQuit {
        t.Fatal("Block source base did not exit")
    }
}

func Test_BlockRepositoryBase_Error(t *testing.T) {
    b := NewBlockRepositoryBase(
        &erroringRequester{},
        MakeNullFixedSizeResolver(4),
        nil,
    )
    defer b.Close()

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  4,
        StartBlock: 1,
        EndBlock:   1,
    })

    select {
        case <-time.After(time.Second):
            t.Fatal("Timed out waiting for error")
        case err := <-b.EncounteredError():
            t.Log(err.Error())
    }
}

func Test_BlockRepositoryBase_Request(t *testing.T) {
    var (
        expected = []byte("test")
        b = NewBlockRepositoryBase(
            FunctionRequester(func(start, end int64) (data []byte, err error) {
                return expected, nil
            }),
            MakeNullFixedSizeResolver(4),
            nil,
        )
    )
    defer b.Close()

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  4,
        StartBlock: 1,
        EndBlock:   1,
    })

    result := <-b.GetResultChannel()

    if result.StartBlock != 1 {
        t.Errorf("Unexpected start block in result: %v", result.StartBlock)
    }
    if bytes.Compare(result.Data, expected) != 0 {
        t.Errorf("Unexpected data in result: %v", result.Data)
    }
}

func Test_BlockRepositoryBase_Consequent_Request(t *testing.T) {
    var (
        content = []byte("test")

        b = NewBlockRepositoryBase(
            FunctionRequester(func(start, end int64) (data []byte, err error) {
                return content[start:end], nil
            }),
            MakeNullFixedSizeResolver(2),
            nil,
        )
    )
    defer b.Close()

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  2,
        StartBlock: 0,
        EndBlock:   0,
    })

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  2,
        StartBlock: 1,
        EndBlock:   1,
    })

    for i := uint(0); i < 2; i++ {
        select {
            case r := <-b.GetResultChannel(): {
                if r.StartBlock != i {
                    t.Errorf("Wrong start block: %v", r.StartBlock)
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


func test_BlockRepositoryBase_OutOfOrderRequestCompletion(t *testing.T) {
    var (
        content = []byte("test")

        channeler = []chan bool{
            make(chan bool),
            make(chan bool),
        }

        b = NewBlockRepositoryBase(
            FunctionRequester(func(start, end int64) (data []byte, err error) {
                // read from the channel based on the start
                <-(channeler[start])
                return content[start:end], nil
            }),
            MakeNullFixedSizeResolver(1),
            nil,
        )
    )
    defer b.Close()

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  1,
        StartBlock: 0,
        EndBlock:   0,
    })

    b.RequestBlocks(patcher.MissingBlockSpan{
        BlockSize:  1,
        StartBlock: 1,
        EndBlock:   1,
    })

    // finish the second request
    channeler[1] <- true

    select {
        case <-b.GetResultChannel():
            t.Error("Should not deliver any blocks yet")
        case <-time.After(time.Second):
    }

    // once the first block completes, we're ready to send both
    channeler[0] <- true

    for i := uint(0); i < 2; i++ {
        select {
            case r := <-b.GetResultChannel():
                if r.StartBlock != i {
                    t.Errorf(
                        "Wrong start block: %v on result %v",
                        r.StartBlock,
                        i+1,
                    )
                }
            case <-time.After(time.Second):
                t.Fatal("Timed out on request", i+1)
        }
    }
}

func test_BlockRepositoryBase_RequestCountLimiting(t *testing.T) {
    const (
        REQUESTS        = 4
    )
    var (
        call_counter = 0
        count        = 0
        max          = 0

        counter      = make(chan int)
        waiter       = make(chan bool)
        exitC        = make(chan struct{})

        b = NewBlockRepositoryBase(
            FunctionRequester(func(start, end int64) (data []byte, err error) {
                t.Logf("FunctionRequester start %d", start)
                counter <- 1
                call_counter += 1
                <-waiter
                counter <- -1
                return []byte{0, 0}, nil
            }),
            MakeNullFixedSizeResolver(1),
            nil,
        )
    )
    t.Logf("deferred close calls")
    defer func() {
        b.Close()
        close(exitC)
        close(counter)
        close(waiter)
    }()

    t.Logf("goroutine to count requests")
    go func() {
        for {
            select {
                case <- exitC: {
                    return
                }
                case change, ok := <-counter: {
                    if !ok {
                        break
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
        t.Logf("make %dth request ", i)
        b.RequestBlocks(patcher.MissingBlockSpan{
            BlockSize:  1,
            StartBlock: uint(i),
            EndBlock:   uint(i),
        })
    }

    for i := 0; i < REQUESTS; i++ {
        waiter <- true
    }

    if max > 1 {
        t.Errorf("Maximum requests in flight was greater than the requested concurrency: %v", max)
    }
    if call_counter != REQUESTS {
        t.Errorf("Total number of requests is not expected: %v", call_counter)
    }
}
