package chunks

import (
    "golang.org/x/crypto/ripemd160"
    "github.com/Redundancy/go-sync/rollsum"
)

func BuildSequentialChecksum(refBlks []string, blocksize int) SequentialChecksumList {
    var (
        chksum = SequentialChecksumList{}
        rsum   = rollsum.NewRollsum64(uint(blocksize))
        ssum   = ripemd160.New()
    )

    for i := 0; i < len(refBlks); i++ {
        var (
            wsum = make([]byte, blocksize)
            blk     = []byte(refBlks[i])
        )
        rsum.SetBlock(blk)
        rsum.GetSum(wsum)
        ssum.Write(blk)

        chksum = append(
            chksum,
            ChunkChecksum{
                ChunkOffset:    uint(i),
                WeakChecksum:   wsum,
                StrongChecksum: ssum.Sum(nil),
            })
    }
    return chksum
}