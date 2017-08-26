/*
 * package filechecksum provides the FileChecksumGenerator, whose main responsibility is to read a file, and generate
 * both weak and strong checksums for every block. It is also used by the comparer, which will generate weak checksums
 * for potential byte ranges that could match the index, and strong checksums if needed.
 */
package filechecksum

import (
    "hash"
    "io"

    "golang.org/x/crypto/ripemd160"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/rollsum"
)

// Uses all default hashes (RIPEMD-160 & RollSum64)
func NewFileChecksumGenerator(blocksize uint) *FileChecksumGenerator {
    return &FileChecksumGenerator{
        blockSize:        blocksize,
    }
}

type RollingHash interface {
    // the size of the hash output
    Size() int

    AddByte(b byte)
    RemoveByte(b byte, length int)

    AddBytes(bs []byte)
    RemoveBytes(bs []byte, length int)

    // pairs up bytes to do remove/add in the right order
    AddAndRemoveBytes(add []byte, remove []byte, length int)

    SetBlock(block []byte)

    GetSum(b []byte)
    Reset()
}

type HashGeneratorFunc func() hash.Hash

/*
 * FileChecksumGenerator provides a description of what hashing functions to use to evaluate a file. Since the hashes
 * store state, it is NOT safe to use a generator concurrently for different things.
*/
type FileChecksumGenerator struct {
    blockSize        uint
}

func (check *FileChecksumGenerator) BlockSize() uint {
    return check.blockSize
}

func (check *FileChecksumGenerator) ChecksumSize() int {
    return check.GetWeakRollingHash().Size() + check.GetStrongHash().Size()
}

func (check *FileChecksumGenerator) GetChecksumSizes() (int, int) {
    return check.GetWeakRollingHash().Size(), check.GetStrongHash().Size()
}

// Gets the fresh, clean Weak Hash function for the overall file used on each block
func (check *FileChecksumGenerator) GetWeakRollingHash() RollingHash {
    return rollsum.NewRollsum64Base(check.BlockSize())
}

/*
 * Gets the fresh, clean Hash function for the overall file used on each block
 * We provide an overall hash of individual files
 * defaults to RipeMD-160
 */
func (check *FileChecksumGenerator) GetFileHash() hash.Hash {
    return ripemd160.New()
}

/*
 * Gets the fresh, clean Hash function for the strong hash used on each block
 * This is a factory function, because we don't actually want to share hash state
 * defaults to RipeMD-160
 */
func (check *FileChecksumGenerator) GetStrongHash() hash.Hash {
    return ripemd160.New()
}

/*
 * GenerateChecksums reads each block of the input file, and outputs first the weak, then the strong checksum to the
 * output writer. It will return a checksum for the whole file. Potentially speaking, this might be better producing
 * a channel of blocks, which would remove the need for io from a number of other places.
 */
func (check *FileChecksumGenerator) GenerateChecksums(inputFile io.Reader, output io.Writer) (fileChecksum []byte, err error) {
    for chunkResult := range check.StartChecksumGeneration(inputFile, 64, nil) {
        if chunkResult.Err != nil {
            return nil, chunkResult.Err
        } else if chunkResult.Filechecksum != nil {
            return chunkResult.Filechecksum, nil
        }

        for _, chunk := range chunkResult.Checksums {
            output.Write(chunk.WeakChecksum)
            output.Write(chunk.StrongChecksum)
        }
    }

    return nil, nil
}

type ChecksumResults struct {
    // Return multiple chunks at once for performance
    Checksums []chunks.ChunkChecksum
    // only used for the last item
    Filechecksum []byte
    // signals that this is the last item
    Err error
}

// A function or object that can compress blocks
// the compression function must also write out the compressed blocks somewhere!
// Compressed blocks should be independently inflatable
type CompressionFunction func([]byte) (compressedSize int64, err error)

func (check *FileChecksumGenerator) StartChecksumGeneration(
    inputFile io.Reader,
    blocksPerResult uint,
    compressionFunction CompressionFunction,
) <-chan ChecksumResults {
    resultChan := make(chan ChecksumResults)
    go check.generate(resultChan, blocksPerResult, compressionFunction, inputFile)
    return resultChan
}

func (check *FileChecksumGenerator) generate(
    resultChan chan ChecksumResults,
    blocksPerResult uint,
    compressionFunction CompressionFunction,
    inputFile io.Reader,
) {

    defer close(resultChan)

    var (
        buffer = make([]byte, check.BlockSize())
        results = make([]chunks.ChunkChecksum, 0, blocksPerResult)

        // these hashes are generated and reset to make it clean
        // As these are for one-time use only, throw away as soon as you're done
        rollingHash = check.GetWeakRollingHash()
        fullChecksum = check.GetFileHash()
        strongHash = check.GetStrongHash()
    )

    i := uint(0)
    for {
        n, err := io.ReadFull(inputFile, buffer)
        section := buffer[:n]

        if n == 0 {
            break
        }

        // As hashes, the assumption is that they never error
        // Additionally, we assume that the only reason not
        // to write a full block would be reaching the end of the file
        fullChecksum.Write(section)
        rollingHash.SetBlock(section)
        strongHash.Write(section)

        strongChecksumValue := make([]byte, 0, strongHash.Size())
        weakChecksumValue := make([]byte, rollingHash.Size())

        rollingHash.GetSum(weakChecksumValue)
        strongChecksumValue = strongHash.Sum(strongChecksumValue)

        blockSize := int64(check.BlockSize())

        if compressionFunction != nil {
            blockSize, err = compressionFunction(section)
        }

        results = append(
            results,
            chunks.ChunkChecksum{
                ChunkOffset:    i,
                Size:           blockSize,
                WeakChecksum:   weakChecksumValue,
                StrongChecksum: strongChecksumValue,
            },
        )

        i++

        if len(results) == cap(results) {
            resultChan <- ChecksumResults{
                Checksums: results,
            }
            results = make([]chunks.ChunkChecksum, 0, blocksPerResult)
        }

        // clear it again
        strongChecksumValue = strongChecksumValue[:0]

        // Reset the strong
        strongHash.Reset()

        if n != len(buffer) || err == io.EOF {
            break
        }
    }

    if len(results) > 0 {
        resultChan <- ChecksumResults{
            Checksums: results,
        }
    }

    resultChan <- ChecksumResults{
        Filechecksum: fullChecksum.Sum(nil),
    }

    return
}
