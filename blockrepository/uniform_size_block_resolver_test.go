package blockrepository

import (
    "testing"
    "github.com/Redundancy/go-sync/patcher"
)

func TestNullResolverGivesBackTheSameBlocks(t *testing.T) {
    n := MakeNullUniformSizeResolver(5)
    result := n.SplitBlockRangeToDesiredSize(patcher.MissingBlockSpan{
        StartBlock: 0,
        EndBlock:   10000,
        BlockSize:  5})

    if len(result) != 1 {
        t.Fatalf("Unexpected result length (expected 1): %v", result)
    }

    r := result[0]

    if r.StartBlock != 0 {
        t.Errorf("Unexpected start block ID: %v", r)
    }

    if r.EndBlock != 10000 {
        t.Errorf("Unexpected end block ID: %v", r)
    }
}

func TestFixedSizeResolverSplitsBlocksOfDesiredSize(t *testing.T) {
    res := &UniformSizeBlockResolver{
        BlockSize:             5,
        MaxDesiredRequestSize: 5,
        FileSize:              20000,
    }

    // Should split two blocks, each of the desired request size into two requests
    result := res.SplitBlockRangeToDesiredSize(patcher.MissingBlockSpan{
        StartBlock: 0,
        EndBlock:   1,
        BlockSize:  5})

    if len(result) != 2 {
        t.Fatalf("Unexpected result length (expected 2): %v", result)
    }

    if result[0].StartBlock != 0 {
        t.Errorf("Unexpected start blockID: %v", result[0])
    }
    if result[0].EndBlock != 0 {
        t.Errorf("Unexpected end blockID: %v", result[0])
    }

    if result[1].StartBlock != 1 {
        t.Errorf("Unexpected start blockID: %v", result[1])
    }
    if result[1].EndBlock != 1 {
        t.Errorf("Unexpected end blockID: %v", result[1])
    }
}

func TestThatMultipleBlocksAreSplitByRoundingDown(t *testing.T) {
    res := &UniformSizeBlockResolver{
        BlockSize:             5,
        MaxDesiredRequestSize: 12,
        FileSize:              20000,
    }

    // 0,1 (10) - 2-3 (10)
    result := res.SplitBlockRangeToDesiredSize(patcher.MissingBlockSpan{
        StartBlock: 0,
        EndBlock:   3,
        BlockSize:  5})

    if len(result) != 2 {
        t.Fatalf("Unexpected result length (expected 2): %v", result)
    }

    if result[0].StartBlock != 0 {
        t.Errorf("Unexpected start blockID: %v", result[0])
    }
    if result[0].EndBlock != 1 {
        t.Errorf("Unexpected end blockID: %v", result[0])
    }

    if result[1].StartBlock != 2 {
        t.Errorf("Unexpected start blockID: %v", result[1])
    }
    if result[1].EndBlock != 3 {
        t.Errorf("Unexpected end blockID: %v", result[1])
    }
}

func TestThatADesiredSizeSmallerThanABlockResultsInSingleBlocks(t *testing.T) {
    res := &UniformSizeBlockResolver{
        BlockSize:             5,
        MaxDesiredRequestSize: 4,
        FileSize:              20000,
    }

    // Should split two blocks
    result := res.SplitBlockRangeToDesiredSize(patcher.MissingBlockSpan{
        StartBlock: 0,
        EndBlock:   1,
        BlockSize:  5})

    if len(result) != 2 {
        t.Fatalf("Unexpected result length (expected 2): %v", result)
    }

    if result[0].StartBlock != 0 {
        t.Errorf("Unexpected start blockID: %v", result[0])
    }
    if result[0].EndBlock != 0 {
        t.Errorf("Unexpected end blockID: %v", result[0])
    }

    if result[1].StartBlock != 1 {
        t.Errorf("Unexpected start blockID: %v", result[1])
    }
    if result[1].EndBlock != 1 {
        t.Errorf("Unexpected end blockID: %v", result[1])
    }
}

func TestThatFileSizeTruncatesBlockEnds(t *testing.T) {
    res := &UniformSizeBlockResolver{
        BlockSize:             5,
        MaxDesiredRequestSize: 100,
        FileSize:              13,
    }

    // Should split two blocks
    result := res.GetBlockEndOffset(3)

    if result != 13 {
        t.Errorf("Unexpected BlockEnd Offset:", result)
    }
}
