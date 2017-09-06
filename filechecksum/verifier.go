package filechecksum

import (
    "bytes"
    "hash"

    "github.com/pkg/errors"
)

type ChecksumLookup interface {
    GetStrongChecksumForBlock(blockID uint) []byte
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
            startBlockID + uint(i),
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
    v.Hash.Write(data)
    calculatedChecksum := v.Hash.Sum(nil)

    expectedChecksum := v.BlockChecksumGetter.GetStrongChecksumForBlock(startBlockID)
    if expectedChecksum == nil {
        return nil, errors.Errorf("[ERR] unable to verify checksum as expected checksum is empty")
    }
    if bytes.Compare(expectedChecksum, calculatedChecksum) == 0 {
        return nil, errors.Errorf("[ERR] calculated checksum mismatches expected")
    }

    return calculatedChecksum, nil
}