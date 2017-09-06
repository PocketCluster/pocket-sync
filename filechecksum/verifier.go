package filechecksum

import (
    "bytes"
    "hash"

    "github.com/pkg/errors"
)

type ChecksumLookup interface {
    GetStrongChecksumForBlock(blockID int) []byte
}

type HashVerifier struct {
    BlockSize           uint
    Hash                hash.Hash
    BlockChecksumGetter ChecksumLookup
}

func (v *HashVerifier) VerifyBlockRange(startBlockID uint, data []byte) bool {
    for i := 0; i*int(v.BlockSize) < len(data); i++ {
        start := i * int(v.BlockSize)
        end := start + int(v.BlockSize)

        if end > len(data) {
            end = len(data)
        }

        blockData := data[start:end]

        expectedChecksum := v.BlockChecksumGetter.GetStrongChecksumForBlock(
            int(startBlockID) + i,
        )

        if expectedChecksum == nil {
            return true
        }

        v.Hash.Write(blockData)
        hashedData := v.Hash.Sum(nil)

        if bytes.Compare(expectedChecksum, hashedData) != 0 {
            return false
        }

        v.Hash.Reset()
    }

    return true
}

func (v *HashVerifier) BlockChecksumForRange(startBlockID uint, data []byte) ([]byte, error) {
    var (
        idx     uint = 0
        lenData uint = uint(len(data))

    )

    for idx * v.BlockSize < lenData {
        var (
            start = idx * v.BlockSize
            end   = start + v.BlockSize
        )

        if end > lenData {
            end = lenData
        }
        blockData := data[start:end]
        expectedChecksum := v.BlockChecksumGetter.GetStrongChecksumForBlock(int(startBlockID + idx))
        if expectedChecksum == nil {
            return nil, errors.Errorf("[ERR] unable to verify checksum as expected checksum is empty")
        }

        v.Hash.Write(blockData)
        hashedData := v.Hash.Sum(nil)

        if bytes.Compare(expectedChecksum, hashedData) != 0 {
            return nil, errors.Errorf("[ERR] calculated checksum mismatches expected")
        }

        v.Hash.Reset()
        idx++
    }

    return nil, errors.Errorf("[ERR] unable to find an expected checksum")
}