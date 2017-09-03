package uslice

import (
    "sort"
)

//-----------------------------------------------------------------------------
type UintSlice []uint

func (r UintSlice) Len() int {
    return len(r)
}

func (r UintSlice) Swap(i, j int) {
    r[i], r[j] = r[j], r[i]
}

func (r UintSlice) Less(i, j int) bool {
    return r[i] < r[j]
}

func DelElementFromSlice(uslice UintSlice, elem uint) UintSlice {
    var newID = uslice[:0]
    for _, r := range uslice {
        if r != elem {
            newID = append(newID, r)
        }
    }
    sort.Sort(newID)
    return newID
}

func AddElementToSlice(uslice UintSlice, elem uint) UintSlice {
    for _, r := range uslice {
        if r == elem {
            sort.Sort(uslice)
            return uslice
        }
    }
    uslice = append(uslice, elem)
    sort.Sort(uslice)
    return uslice
}
