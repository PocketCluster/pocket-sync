package merkle

import (
    "bytes"
    "math/rand"
    "testing"
)

func RandBytes(n int) []byte {
    bs := make([]byte, n)
    for i := 0; i < n; i++ {
        bs[i] = byte(rand.Intn(256))
    }
    return bs
}

func MutateByteSlice(bytez []byte) []byte {
    // If bytez is empty, panic
    if len(bytez) == 0 {
        panic("Cannot mutate an empty bytez")
    }

    // Copy bytez
    mBytez := make([]byte, len(bytez))
    copy(mBytez, bytez)
    bytez = mBytez

    // Try a random mutation
    switch rand.Int() % 2 {
    case 0: // Mutate a single byte
        bytez[rand.Int() % len(bytez)] += byte(rand.Int() % 255 + 1)
    case 1: // Remove an arbitrary byte
        pos := rand.Int() % len(bytez)
        bytez = append(bytez[:pos], bytez[pos+1:]...)
    }
    return bytez
}

type testItem []byte

func (tI testItem) Hash() []byte {
    return []byte(tI)
}

func TestSimpleProof(t *testing.T) {

    total := 100

    items := make([]Hashable, total)
    for i := 0; i < total; i++ {
        items[i] = testItem(RandBytes(32))
    }

    rootHash, err := SimpleHashFromHashables(items)
    if err != nil {
        t.Fatal(err.Error())
    }

    rootHash2, proofs := SimpleProofsFromHashables(items)

    if !bytes.Equal(rootHash, rootHash2) {
        t.Errorf("Unmatched root hashes: %X vs %X", rootHash, rootHash2)
    }

    // For each item, check the trail.
    for i, item := range items {
        itemHash := item.Hash()
        proof := proofs[i]

        // Verify success
        ok := proof.Verify(i, total, itemHash, rootHash)
        if !ok {
            t.Errorf("Verification failed for index %v.", i)
        }

        // Wrong item index should make it fail
        {
            ok = proof.Verify((i+1)%total, total, itemHash, rootHash)
            if ok {
                t.Errorf("Expected verification to fail for wrong index %v.", i)
            }
        }

        // Trail too long should make it fail
        origAunts := proof.Aunts
        proof.Aunts = append(proof.Aunts, RandBytes(32))
        {
            ok = proof.Verify(i, total, itemHash, rootHash)
            if ok {
                t.Errorf("Expected verification to fail for wrong trail length.")
            }
        }
        proof.Aunts = origAunts

        // Trail too short should make it fail
        proof.Aunts = proof.Aunts[0 : len(proof.Aunts)-1]
        {
            ok = proof.Verify(i, total, itemHash, rootHash)
            if ok {
                t.Errorf("Expected verification to fail for wrong trail length.")
            }
        }
        proof.Aunts = origAunts

        // Mutating the itemHash should make it fail.
        ok = proof.Verify(i, total, MutateByteSlice(itemHash), rootHash)
        if ok {
            t.Errorf("Expected verification to fail for mutated leaf hash")
        }

        // Mutating the rootHash should make it fail.
        ok = proof.Verify(i, total, itemHash, MutateByteSlice(rootHash))
        if ok {
            t.Errorf("Expected verification to fail for mutated root hash")
        }
    }
}
