package gosync

import (
    "bytes"
    "fmt"
    "net"
    "net/http"
    "time"

    "github.com/Redundancy/go-sync/blocksources"
    "github.com/Redundancy/go-sync/comparer"
    "github.com/Redundancy/go-sync/filechecksum"
    "github.com/Redundancy/go-sync/patcher"
)

const (
    // due to short example strings, use a very small block size
    // using one this small in practice would increase your file transfer!
    BLOCK_SIZE = 4

    // This is the "file" as described by the authoritive version
    REFERENCE = "The quick brown fox jumped over the lazy dog"

    // This is what we have locally. Not too far off, but not correct.
    LOCAL_VERSION = "The qwik brown fox jumped 0v3r the lazy"
)

var (
    content = bytes.NewReader([]byte(REFERENCE))

    handler = func(w http.ResponseWriter, req *http.Request) {
        http.ServeContent(w, req, "", time.Now(), content)
    }
)

// set up a http server locally that will respond predictably to ranged requests
func setupServer() <-chan int {
    var PORT = 8000
    s := http.NewServeMux()
    s.HandleFunc("/content", handler)

    portChan := make(chan int)

    go func() {
        var listener net.Listener
        var err error

        for {
            PORT++
            p := fmt.Sprintf(":%v", PORT)
            listener, err = net.Listen("tcp", p)

            if err == nil {
                break
            }
        }
        portChan <- PORT
        http.Serve(listener, s)
    }()

    return portChan
}

// This is exceedingly similar to the module Example, but uses the http blocksource and a local http server
func Example_httpBlockSource() {
    PORT := <-setupServer()
    LOCAL_URL := fmt.Sprintf("http://localhost:%v/content", PORT)

    generator := filechecksum.NewFileChecksumGenerator(BLOCK_SIZE)
    _, referenceFileIndex, checksumLookup, err := filechecksum.BuildIndexFromString(generator, REFERENCE)

    if err != nil {
        return
    }

    fileSize := int64(len([]byte(REFERENCE)))

    // This would normally be saved in a file

    blockCount := fileSize / BLOCK_SIZE
    if fileSize%BLOCK_SIZE != 0 {
        blockCount++
    }

    fs := &BasicSummary{
        ChecksumIndex:  referenceFileIndex,
        ChecksumLookup: checksumLookup,
        BlockCount:     uint(blockCount),
        BlockSize:      uint(BLOCK_SIZE),
        FileSize:       fileSize,
    }

/*
    // Normally, this would be:
    rsync, err := MakeRSync(
        "toPatch.file",
        "http://localhost/content",
        "out.file",
        fs,
    )
*/
    // Need to replace the output and the input
    inputFile := bytes.NewReader([]byte(LOCAL_VERSION))
    patchedFile := bytes.NewBuffer(nil)

    resolver := blocksources.MakeFileSizedBlockResolver(
        uint64(fs.GetBlockSize()),
        fs.GetFileSize(),
    )

    rsync := &RSync{
        Input:  inputFile,
        Output: patchedFile,
        Source: blocksources.NewHttpBlockSource(
            LOCAL_URL,
            1,
            resolver,
            &filechecksum.HashVerifier{
                Hash:                filechecksum.DefaultFileHashGenerator(),
                BlockSize:           fs.GetBlockSize(),
                BlockChecksumGetter: fs,
            },
        ),
        Summary: fs,
        OnClose: nil,
    }

    err = rsync.Patch()

    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    err = rsync.Close()

    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    fmt.Printf("Patched content: \"%v\"\n", patchedFile.String())

    // Just for inspection
    remoteReferenceSource := rsync.Source.(*blocksources.BlockSourceBase)
    fmt.Printf("Downloaded Bytes: %v\n", remoteReferenceSource.ReadBytes())

    // Output:
    // Patched content: "The quick brown fox jumped over the lazy dog"
    // Downloaded Bytes: 16
}

func ToPatcherFoundSpan(sl comparer.BlockSpanList, blockSize int64) []patcher.FoundBlockSpan {
    result := make([]patcher.FoundBlockSpan, len(sl))

    for i, v := range sl {
        result[i].StartBlock = v.StartBlock
        result[i].EndBlock = v.EndBlock
        result[i].MatchOffset = v.ComparisonStartOffset
        result[i].BlockSize = blockSize
    }

    return result
}

func ToPatcherMissingSpan(sl comparer.BlockSpanList, blockSize int64) []patcher.MissingBlockSpan {
    result := make([]patcher.MissingBlockSpan, len(sl))

    for i, v := range sl {
        result[i].StartBlock = v.StartBlock
        result[i].EndBlock = v.EndBlock
        result[i].BlockSize = blockSize
    }

    return result
}
