package blocksources

import (
    "sort"

    log "github.com/Sirupsen/logrus"
    "github.com/pkg/errors"
    "github.com/Redundancy/go-sync/patcher"
)

/*
 * SingleBlockRepository provides an implementation of blocksource that takes care of every aspect of block handling
 * from a repository except for the actual asyncronous request. It is vastly similar to BlockSourceBase but differs in
 * which detailed aspects (cancle, progress, & etc) of only single request managed in monotonous manner.
 *
 * SingleBlockRepository implements patcher.BlockSource.
 */

type SingleBlockRepository struct {

}