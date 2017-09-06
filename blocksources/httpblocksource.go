package blocksources

import (
    "bytes"
    "fmt"
    "net/http"
    "strings"

    "github.com/pkg/errors"
)

const (
    MB = 1024 * 1024
)

var (
    RangedRequestNotSupportedError = errors.New("Ranged request not supported (Server did not respond with 206 Status)")
    ResponseFromServerWasGZiped = errors.New("HTTP response was gzip encoded. Ranges may not match those requested.")
)

var ClientNoCompression = &http.Client{
    Transport: &http.Transport{},
}

func NewHttpBlockSource(
    url      string,
    concurrentRequests int,
    resolver BlockSourceOffsetResolver,
    verifier BlockVerifier,
) *BlockSourceBase {
    return NewBlockSourceBase(
        &HttpRequester{
            url:    url,
            client: http.DefaultClient,
        },
        resolver,
        verifier,
        concurrentRequests,
        4*MB,
    )
}

type URLNotFoundError string

func (url URLNotFoundError) Error() string {
    return "404 Error on URL: " + string(url)
}

// This class provides the implementation of BlockSourceRequester for BlockSourceBase
// this simplifies creating new BlockSources that satisfy the requirements down to
// writing a request function
type HttpRequester struct {
    client *http.Client
    url    string
}

func (r *HttpRequester) DoRequest(startOffset int64, endOffset int64) ([]byte, error) {
    rangedRequest, err := http.NewRequest("GET", r.url, nil)
    if err != nil {
        return nil, errors.Errorf("Error creating request for \"%v\": %v", r.url, err)
    }

    rangeSpecifier := fmt.Sprintf("bytes=%v-%v", startOffset, endOffset-1)
    rangedRequest.ProtoAtLeast(1, 1)
    rangedRequest.Header.Add("Range", rangeSpecifier)
    rangedRequest.Header.Add("Accept-Encoding", "identity")
    rangedResponse, err := r.client.Do(rangedRequest)
    if err != nil {
        return nil, errors.Errorf("Error executing request for \"%v\": %v", r.url, err)
    }
    defer rangedResponse.Body.Close()

    if rangedResponse.StatusCode == 404 {
        return nil, URLNotFoundError(r.url)

    } else if rangedResponse.StatusCode != 206 {
        return nil, RangedRequestNotSupportedError

    } else if strings.Contains(rangedResponse.Header.Get("Content-Encoding"), "gzip") {
        return nil, ResponseFromServerWasGZiped

    } else {
        buf := bytes.NewBuffer(make([]byte, 0, endOffset - startOffset))
        _, err = buf.ReadFrom(rangedResponse.Body)
        if err != nil {
            return nil, errors.Errorf("Failed to read response body for %v (%v-%v): %v",
                r.url, startOffset, endOffset-1, err)
        }
        if int64(buf.Len()) != endOffset - startOffset {
            return nil, errors.Errorf("Unexpected response length %v (%v): %v",
                r.url, endOffset-startOffset+1, buf.Len())
        }

        return buf.Bytes(), nil
    }
}

func (r *HttpRequester) IsFatal(err error) bool {
    return true
}
