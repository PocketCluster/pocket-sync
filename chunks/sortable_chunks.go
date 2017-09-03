package chunks

import (
    "bytes"
    "sort"
)

// --------------------------------------------- StrongChecksumList ----------------------------------------------------
type StrongChecksumList []ChunkChecksum

// Sortable interface
func (s StrongChecksumList) Len() int {
    return len(s)
}

// Sortable interface
func (s StrongChecksumList) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

// Sortable interface
func (s StrongChecksumList) Less(i, j int) bool {
    return bytes.Compare(s[i].StrongChecksum, s[j].StrongChecksum) == -1
}

func (s StrongChecksumList) FindStrongChecksum(strong []byte) (result []ChunkChecksum) {
    n := len(s)

    // average length is 1, so fast path comparison
    if n == 1 {
        if bytes.Compare(s[0].StrongChecksum, strong) == 0 {
            return s
        } else {
            return nil
        }
    }

    // find the first possible occurance
    first_gte_checksum := sort.Search(
        n,
        func(i int) bool {
            return bytes.Compare(s[i].StrongChecksum, strong) >= 0
        },
    )

    // out of bounds
    if first_gte_checksum == -1 || first_gte_checksum == n {
        return nil
    }

    // Somewhere in the middle, but the next one didn't match
    if bytes.Compare(s[first_gte_checksum].StrongChecksum, strong) != 0 {
        return nil
    }

    end := first_gte_checksum + 1
    for end < n {
        if bytes.Compare(s[end].StrongChecksum, strong) == 0 {
            end += 1
        } else {
            break
        }

    }

    return s[first_gte_checksum:end]
}

// --------------------------------------------- SequentialChecksumList ------------------------------------------------
type SequentialChecksumList []ChunkChecksum

// Sortable interface
func (s SequentialChecksumList) Len() int {
    return len(s)
}

// Sortable interface
func (s SequentialChecksumList) Swap(i, j int) {
    s[i], s[j] = s[j], s[i]
}

// Sortable interface
func (s SequentialChecksumList) Less(i, j int) bool {
    return s[i].ChunkOffset < s[j].ChunkOffset
}

func (s SequentialChecksumList) HashList() [][]byte {
    var (
        hashes [][]byte = make([][]byte, 0, len(s))
    )
    for i := 0; i < len(s); i++ {
        hashes = append(hashes, s[i].StrongChecksum)
    }
    return hashes
}