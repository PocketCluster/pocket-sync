package filechecksum

import (
    "bytes"
	"io"

    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/index"
)

/*
 * indexbuilder provides a few shortcuts to building a checksum index by generating and then loading the checksums, and
 * building an index from that. It's potentially a sign that the responsibilities here need refactoring.
 *
 * Generates an index from a reader.
 * This is mostly a utility function to avoid being overly verbose in tests that need an index to work, but don't want
 * to construct one by hand in order to avoid the dependencies.
 * Obviously this means that those tests are likely to fail if there are issues with any of the other modules, which is
 * not ideal.
 */
func BuildChecksumIndex(check *FileChecksumGenerator, r io.Reader) (
    fcheck []byte,
    i *index.ChecksumIndex,
    lookup ChecksumLookup,
    err error,
) {
    var (
        b = bytes.NewBuffer(nil)
        weakSize = check.GetWeakRollingHash().Size()
        strongSize = check.GetStrongHash().Size()
    )

    fcheck, err = check.GenerateChecksums(r, b)
    if err != nil {
        return
    }

    readChunks, err := chunks.LoadChecksumsFromReader(b, weakSize, strongSize)
    if err != nil {
        return
    }

    i = index.MakeChecksumIndex(readChunks)
    lookup = chunks.StrongChecksumGetter(readChunks)

    return
}

func BuildIndexFromString(generator *FileChecksumGenerator, reference string) (
    fileCheckSum []byte,
    referenceIndex *index.ChecksumIndex,
    lookup ChecksumLookup,
    err error,
) {
    return BuildChecksumIndex(generator, bytes.NewBufferString(reference))
}
