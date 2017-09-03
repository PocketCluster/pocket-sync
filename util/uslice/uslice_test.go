package uslice

import (
    "reflect"
    "testing"
)

func Test_Available_Pool_Addition(t *testing.T) {
    var (
        ids UintSlice = []uint{0, 1, 4, 7, 13, 42, 92}
    )

    ids = AddElementToSlice(ids, 4)
    if reflect.DeepEqual(ids, []uint{0, 1, 4, 7, 13, 42, 92}) {
        t.Errorf("addIdentityToAvailablePool should not add duplicated id")
    }

    ids = AddElementToSlice(ids, 77)
    if reflect.DeepEqual(ids, []uint{0, 1, 4, 7, 13, 42, 77, 92}) {
        t.Errorf("addIdentityToAvailablePool should add new id")
    }
}

func Test_Available_Pool_Deletion(t *testing.T) {
    var (
        ids UintSlice = []uint{0, 1, 4, 7, 13, 42, 92}
    )

    ids = DelElementFromSlice(ids, 11)
    if reflect.DeepEqual(ids, []uint{0, 1, 7, 13, 42, 92}) {
        t.Errorf("delIdentityFromAvailablePool should not delete absent element %v", ids)
    }

    ids = DelElementFromSlice(ids, 4)
    if reflect.DeepEqual(ids, []uint{0, 1, 7, 13, 42, 92}) {
        t.Errorf("delIdentityFromAvailablePool only delete one id %v", ids)
    }

    ids = DelElementFromSlice(ids, 7)
    if reflect.DeepEqual(ids, []uint{0, 1, 13, 42, 92}) {
        t.Errorf("delIdentityFromAvailablePool only delete one id %v", ids)
    }

    ids = DelElementFromSlice(ids, 92)
    if reflect.DeepEqual(ids, []uint{0, 1, 7, 13, 42}) {
        t.Errorf("delIdentityFromAvailablePool only delete one id %v", ids)
    }
}
