/*
 * Package index provides the functionality to describe a reference 'file' and its contents in terms of the weak and
 * strong checksums, in such a way that you can check if a weak checksum is present, then check if there is a strong
 * checksum that matches.
 *
 * It also allows lookups in terms of block offsets, so that upon finding a match, you can more efficiently check if
 * the next block follows it.

 * The index structure does not lend itself to being an interface - the pattern of taking the result of looking for
 * the weak checksum and looking up the strong checksum in that requires us to return an object matching an interface
 * which both packages must know about.
 *
 * Here's the interface:
 *
 * type Index interface {
 *     FindWeakChecksum(chk []byte) interface{}
 *     FindStrongChecksum(chk []byte, weak interface{}) []chunks.ChunkChecksum
 * }
 *
 * This allows the implementation to rely on a previously generated value, without the users knowing what it is.
 * This breaks the dependency that requires so many packages to import index.
 */
package index

import (
    "bytes"
    "encoding/binary"
    "sort"

    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/chunks"
    "github.com/Redundancy/go-sync/merkle"
    "github.com/Redundancy/go-sync/patcher"
)

const (
    // (2017/08/26) weakChecksumLookup map will have 256 elements with assumption that
    // each transmission block size would be ~ 10MB and the total size would be less than 2.5GB
    indexOffsetFilter uint64 = 0xFF
    indexLookupMapSize int = 256

    seqLookupOffsetFlt uint = 0xFF
    seqLookupArraySize  int = 256
)

/*
 * This datastructure is based on some benchmarking that indicates that it outperforms a basic map 30ns vs ~1600ns
 * for ~8192 checksums (which is reasonably large - say 64 MB with no weak collisions @8192 bytes per block).
 *
 * We use a 256 element slice, and the value of the least significant byte to determine which map to look up into.
 */
type ChecksumIndex struct {
    weakChecksumLookup     []map[uint64]chunks.StrongChecksumList
    seqChecksumLookup      []map[uint]chunks.SequentialChecksumList
    checkSumSequence       chunks.SequentialChecksumList
    AverageStrongLength    float64
    BlockCount             int
    MaxStrongLength        int
    Count                  int
}

// Builds an index in which chunks can be found, with their corresponding offsets
// We use this for the
func MakeChecksumIndex(checksums []chunks.ChunkChecksum) *ChecksumIndex {
    // for an empty checksums, it's not necessary to create anything
    if checksums == nil || len(checksums) == 0 {
        return nil
    }

    var (
        n = &ChecksumIndex{
            BlockCount:         len(checksums),
            weakChecksumLookup: make([]map[uint64]chunks.StrongChecksumList, indexLookupMapSize),
            seqChecksumLookup:  make([]map[uint]chunks.SequentialChecksumList, seqLookupArraySize),
            checkSumSequence:   nil,
        }
        sum = 0
        count = 0
    )

    // copy/ append/ sort the checksum slice
    chksumCopy := make(chunks.SequentialChecksumList, len(checksums))
    copy(chksumCopy, checksums)
    sort.Sort(chksumCopy)
    n.checkSumSequence = chksumCopy

    // build seq chunk lookup map
    for _, c := range checksums {
        blockID := c.ChunkOffset
        arrayOffset := blockID & seqLookupOffsetFlt

        if n.seqChecksumLookup[arrayOffset] == nil {
            n.seqChecksumLookup[arrayOffset] = make(map[uint]chunks.SequentialChecksumList)
        }
        n.seqChecksumLookup[arrayOffset][blockID] = append(n.seqChecksumLookup[arrayOffset][blockID], c)
    }
    for _, cl := range n.seqChecksumLookup {
        for _, c := range cl {
            sort.Sort(c)
        }
    }


    // create weak checksum map
    for _, chunk := range checksums {
        weakChecksumAsInt := binary.LittleEndian.Uint64(chunk.WeakChecksum)
        arrayOffset := weakChecksumAsInt & indexOffsetFilter

        if n.weakChecksumLookup[arrayOffset] == nil {
            n.weakChecksumLookup[arrayOffset] = make(map[uint64]chunks.StrongChecksumList)
        }

        n.weakChecksumLookup[arrayOffset][weakChecksumAsInt] = append(
            n.weakChecksumLookup[arrayOffset][weakChecksumAsInt],
            chunk,
        )
    }
    for _, a := range n.weakChecksumLookup {
        for _, c := range a {
            sort.Sort(c)
            if len(c) > n.MaxStrongLength {
                n.MaxStrongLength = len(c)
            }
            sum += len(c)
            count += 1
            n.Count += len(c)
        }
    }

    n.AverageStrongLength = float64(sum) / float64(count)

    return n
}

func NewChecksumIndex() *ChecksumIndex {
    return  &ChecksumIndex{
        weakChecksumLookup: make([]map[uint64]chunks.StrongChecksumList, indexLookupMapSize),
        seqChecksumLookup:  make([]map[uint]chunks.SequentialChecksumList, seqLookupArraySize),
        checkSumSequence:   make(chunks.SequentialChecksumList, 0, 1),
    }
}

func (n *ChecksumIndex) AppendChecksums(checksums []chunks.ChunkChecksum) error {
    if checksums == nil || len(checksums) == 0 {
        return errors.Errorf("unable to append an empty checksum slice")
    }

    var (
        sum = 0
        count = 0
    )

    // copy/ append/ sort the checksum slice
    chksumCopy := make(chunks.SequentialChecksumList, len(checksums))
    copy(chksumCopy, checksums)
    n.checkSumSequence = append(n.checkSumSequence, chksumCopy...)
    sort.Sort(n.checkSumSequence)

    // build seq chunk lookup map
    for _, c := range checksums {
        blockID := c.ChunkOffset
        arrayOffset := blockID & seqLookupOffsetFlt

        if n.seqChecksumLookup[arrayOffset] == nil {
            n.seqChecksumLookup[arrayOffset] = make(map[uint]chunks.SequentialChecksumList)
        }
        n.seqChecksumLookup[arrayOffset][blockID] = append(n.seqChecksumLookup[arrayOffset][blockID], c)
    }
    for _, cl := range n.seqChecksumLookup {
        for _, c := range cl {
            sort.Sort(c)
        }
    }


    // create weak checksum map
    for _, chunk := range checksums {
        weakChecksumAsInt := binary.LittleEndian.Uint64(chunk.WeakChecksum)
        arrayOffset := weakChecksumAsInt & indexOffsetFilter

        if n.weakChecksumLookup[arrayOffset] == nil {
            n.weakChecksumLookup[arrayOffset] = make(map[uint64]chunks.StrongChecksumList)
        }

        n.weakChecksumLookup[arrayOffset][weakChecksumAsInt] = append(
            n.weakChecksumLookup[arrayOffset][weakChecksumAsInt],
            chunk,
        )
    }
    for _, a := range n.weakChecksumLookup {
        for _, c := range a {
            sort.Sort(c)
            if len(c) > n.MaxStrongLength {
                n.MaxStrongLength = len(c)
            }
            sum += len(c)
            count += 1
            n.Count += len(c)
        }
    }

    n.AverageStrongLength = float64(sum) / float64(count)
    n.BlockCount += len(checksums)
    return nil
}

func (n *ChecksumIndex) WeakCount() int {
    return n.Count
}

func (n *ChecksumIndex) FindWeakChecksumInIndex(weak []byte) chunks.StrongChecksumList {
    x := binary.LittleEndian.Uint64(weak)
    if n.weakChecksumLookup[x & indexOffsetFilter] != nil {
        if v, ok := n.weakChecksumLookup[x & indexOffsetFilter][x]; ok {
            return v
        }
    }
    return nil
}

func (n *ChecksumIndex) FindWeakChecksum2(chk []byte) interface{} {
    w := n.FindWeakChecksumInIndex(chk)

    if len(w) == 0 {
        return nil
    } else {
        return w
    }
}

func (n *ChecksumIndex) FindStrongChecksum2(chk []byte, weak interface{}) []chunks.ChunkChecksum {
    if strongList, ok := weak.(chunks.StrongChecksumList); ok {
        return strongList.FindStrongChecksum(chk)
    } else {
        return nil
    }
}

// ------------------------------ SequentialChecksumSupervisor interface -----------------------------------------------
func (n *ChecksumIndex) SequentialChecksumList() chunks.SequentialChecksumList {
    return n.checkSumSequence
}

func (n *ChecksumIndex) EndBlockID() uint {
    return n.checkSumSequence[len(n.checkSumSequence) - 1].ChunkOffset
}

func (n *ChecksumIndex) FindChecksumWithBlockID(blockID uint) (*chunks.ChunkChecksum, error) {
    if n.seqChecksumLookup[blockID & seqLookupOffsetFlt] != nil {
        if cl, ok := n.seqChecksumLookup[blockID & seqLookupOffsetFlt][blockID]; ok {
            for i, c := range cl {
                if c.ChunkOffset == blockID {
                    return &(cl[i]), nil
                }
            }
            return nil, errors.Errorf("[ERR] fail to iterate block list with ID")
        } else {
            return nil, errors.Errorf("[ERR] unable to find block for ID")
        }
    }
    return nil, errors.Errorf("[ERR] fail to find block for ID with offset")
}

func (n *ChecksumIndex) MissingBlockSpanForID(blockID uint) (patcher.MissingBlockSpan, error) {
    c, err := n.FindChecksumWithBlockID(blockID)
    if err != nil {
        return patcher.MissingBlockSpan{}, errors.Errorf("[ERR] unable to find block w/ index %v", blockID)
    }
    return patcher.MissingBlockSpan{
        BlockSize:     c.Size,
        StartBlock:    c.ChunkOffset,
        EndBlock:      c.ChunkOffset,
    }, nil
}

func (n *ChecksumIndex) VerifyRootHash(hashes [][]byte) error {
    hToCheck, err := merkle.SimpleHashFromHashes(hashes)
    if err != nil {
        return err
    }
    hAsRefer, err := n.checkSumSequence.RootHash()
    if err != nil {
        return err
    }
    if bytes.Compare(hToCheck, hAsRefer) != 0 {
        return errors.Errorf("[ERR] calculated root hash different from referenece")
    }
    return nil
}

// ChecksumLookup impl.
func (n *ChecksumIndex) GetStrongChecksumForBlock(blockID uint) []byte {
    c, err := n.FindChecksumWithBlockID(blockID)
    if err != nil {
        return nil
    }
    return c.StrongChecksum
}