package filechecksum

import (
    "io"

    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/merkle"
)

func (check *FileChecksumGenerator) BuildSequentialAndRootChecksum(inputFile io.Reader, output io.Writer) ([]byte, uint32, error) {
    for chunkResult := range check.startBuildChecksum(inputFile, 64) {
        if chunkResult.Err != nil {
            return nil, 0, chunkResult.Err
        } else if chunkResult.Filechecksum != nil {
            return chunkResult.Filechecksum, chunkResult.SequenceSize, nil
        }

        for _, chunk := range chunkResult.Checksums {
            output.Write(chunk.WeakChecksum)
            output.Write(chunk.StrongChecksum)
        }
    }

    return nil, 0, errors.Errorf("sequential checksum building reached an unexpected end")
}

func (check *FileChecksumGenerator) startBuildChecksum(
    inputFile       io.Reader,
    blocksPerResult uint,
) <-chan ChecksumResults {
    resultChan := make(chan ChecksumResults)
    go check.buildSeqAndRootChecksum(inputFile, resultChan, blocksPerResult)
    return resultChan
}

func (check *FileChecksumGenerator) buildSeqAndRootChecksum(
    inputFile       io.Reader,
    resultChan      chan ChecksumResults,
    blocksPerResult uint,
) {

    defer close(resultChan)

    var (
        blockSize   = int64(check.BlockSize())
        buffer      = make([]byte, check.BlockSize())
        results     = make([]chunks.ChunkChecksum, 0, blocksPerResult)
        sChksum     = make([][]byte, 0, 1)

        // these hashes are generated and reset to make it clean
        // As these are for one-time use only, throw away as soon as you're done
        rollingHash = check.GetWeakRollingHash()
        strongHash  = check.GetStrongHash()
    )

    blockID := uint(0)
    for {
        n, err := io.ReadFull(inputFile, buffer)
        // loop only breaks when there is nothing read (so partial reads gets handled)
        if n == 0 {
            break
        }

        var (
            section = buffer[:n]
            weakChecksumValue   = make([]byte, rollingHash.Size())
            strongChecksumValue = make([]byte, 0, strongHash.Size())
        )

        // As hashes, the assumption is that they never error.
        // Additionally, we assume that the only reason not to write a full block would be reaching the end of the file.
        rollingHash.SetBlock(section)
        strongHash.Write(section)

        // get checksum
        rollingHash.GetSum(weakChecksumValue)
        strongChecksumValue = strongHash.Sum(strongChecksumValue)

        // append to result
        results = append(
            results,
            chunks.ChunkChecksum{
                ChunkOffset:    blockID,
                Size:           blockSize,
                WeakChecksum:   weakChecksumValue,
                StrongChecksum: strongChecksumValue,
            })

        // save strong checksum for root hash
        sChksum = append(sChksum, strongChecksumValue)

        // dispatch collected result
        if len(results) == cap(results) {
            resultChan <- ChecksumResults{
                Checksums: results,
            }
            results = make([]chunks.ChunkChecksum, 0, blocksPerResult)
        }

        // advance iterator
        blockID++

        // Reset the strong (we don't reset rollingsum as it needs previous data)
        strongHash.Reset()

        // stops at io.EOF, io.ErrUnexpectedEOF
        if err != nil {
            break
        }
    }

    // end
    if len(results) > 0 {
        resultChan <- ChecksumResults{
            Checksums: results,
        }
    }

    // make root hash
    rcs, err := merkle.SimpleHashFromHashes(sChksum)
    if err != nil {
        resultChan <- ChecksumResults{
            Err: err,
        }
    } else {
        resultChan <- ChecksumResults{
            Filechecksum: rcs,
            // blockID has increased by one at the end of loop.
            SequenceSize: uint32(blockID),
        }
    }

    return
}