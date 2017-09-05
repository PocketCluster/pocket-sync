package blockrepository

import (
    "github.com/Redundancy/go-sync/patcher"
)

type UniformSizeBlockResolver struct {
    BlockSize             int64
    FileSize              int64
    MaxDesiredRequestSize int64
}

func (r *UniformSizeBlockResolver) GetBlockStartOffset(blockID uint) int64 {
    if off := int64(int64(blockID) * r.BlockSize); r.FileSize != 0 && off > r.FileSize {
        return r.FileSize
    } else {
        return off
    }
}

func (r *UniformSizeBlockResolver) GetBlockEndOffset(blockID uint) int64 {
    if off := int64(int64(blockID+1) * r.BlockSize); r.FileSize != 0 && off > r.FileSize {
        return r.FileSize
    } else {
        return off
    }
}

// Split blocks into chunks of the desired size, or less. This implementation assumes a fixed block size at the source.
func (r *UniformSizeBlockResolver) SplitBlockRangeToDesiredSize(reqBlk patcher.MissingBlockSpan) patcher.QueuedRequestList {

    if r.MaxDesiredRequestSize == 0 && r.BlockSize == reqBlk.BlockSize {
        return patcher.QueuedRequestList{reqBlk}
    }

    maxSize := r.MaxDesiredRequestSize
    if r.MaxDesiredRequestSize < r.BlockSize {
        maxSize = r.BlockSize
    }

    // how many blocks is the desired size?
    blockCountPerRequest := uint(maxSize / r.BlockSize)

    requests := make(patcher.QueuedRequestList, 0, (reqBlk.EndBlock - reqBlk.StartBlock ) / blockCountPerRequest + 1)
    currentBlockID := reqBlk.StartBlock

    for {
        maxEndBlock := currentBlockID + blockCountPerRequest

        if maxEndBlock > reqBlk.EndBlock {
            requests = append(
                requests,
                patcher.MissingBlockSpan{
                    BlockSize:    reqBlk.BlockSize,
                    StartBlock:   currentBlockID,
                    EndBlock:     reqBlk.EndBlock,
                })
            return requests

        } else {
            requests = append(
                requests,
                patcher.MissingBlockSpan{
                    BlockSize:    reqBlk.BlockSize,
                    StartBlock:   currentBlockID,
                    EndBlock:     maxEndBlock - 1,
                })

            currentBlockID = maxEndBlock
        }
    }
}
