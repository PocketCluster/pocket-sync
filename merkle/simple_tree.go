/*
 * Computes a deterministic minimal height merkle tree hash.
 * If the number of items is not a power of two, some leaves will be at different levels. Tries to keep both sides of
 * the tree the same size, but the left may be one greater.

 * Use this for short deterministic trees, such as the validator list.

                        *
                       / \
                     /     \
                   /         \
                 /             \
                *               *
               / \             / \
              /   \           /   \
             /     \         /     \
            *       *       *       h6
           / \     / \     / \
          h0  h1  h2  h3  h4  h5

 */

package merkle

import (
    "bytes"
    "fmt"

	"github.com/pkg/errors"
    "golang.org/x/crypto/ripemd160"
)

type Hashable interface {
    Hash() []byte
}

func SimpleHashFromTwoHashes(left []byte, right []byte) ([]byte, error) {
	var (
		err error = nil
		hasher = ripemd160.New()
	)
	_, err = hasher.Write(left)
    if err != nil {
		return nil, err
	}
	_, err = hasher.Write(right)
    if err != nil {
		return nil, err
    }
    return hasher.Sum(nil), nil
}

func SimpleHashFromHashes(hashes [][]byte) ([]byte, error) {
    // Recursive impl.
    switch len(hashes) {
        case 0: {
            return nil, errors.Errorf("[ERR] invalid number of child hashes")
        }
        case 1: {
            return hashes[0], nil
        }
        default: {
            left, err := SimpleHashFromHashes(hashes[:(len(hashes)+1)/2])
    		if err != nil {
    			return nil, err
    		}
            right, err := SimpleHashFromHashes(hashes[(len(hashes)+1)/2:])
    		if err != nil {
    			return nil, err
    		}
            return SimpleHashFromTwoHashes(left, right)
        }
    }
}

// Convenience for SimpleHashFromHashes.
func SimpleHashFromHashables(items []Hashable) ([]byte, error) {
    hashes := make([][]byte, len(items))
    for i, item := range items {
        hash := item.Hash()
        hashes[i] = hash
    }
    return SimpleHashFromHashes(hashes)
}

//--------------------------------------------------------------------------------
type SimpleProof struct {
    Aunts [][]byte `json:"aunts"` // Hashes from leaf's sibling to a root's child.
}

// proofs[0] is the proof for items[0].
func SimpleProofsFromHashables(items []Hashable) ([]byte, []*SimpleProof) {
    var (
        trails []*SimpleProofNode = nil
        rootSPN *SimpleProofNode  = nil
        rootHash []byte           = nil
        proofs []*SimpleProof     = nil
    )
    trails, rootSPN = trailsFromHashables(items)
    rootHash = rootSPN.Hash
    proofs = make([]*SimpleProof, len(items))
    for i, trail := range trails {
        proofs[i] = &SimpleProof{
            Aunts: trail.FlattenAunts(),
        }
    }
    return rootHash, proofs
}

// Verify that leafHash is a leaf hash of the simple-merkle-tree
// which hashes to rootHash.
func (sp *SimpleProof) Verify(index int, total int, leafHash []byte, rootHash []byte) bool {
    computedHash, err := computeHashFromAunts(index, total, leafHash, sp.Aunts)
    if err != nil || computedHash == nil {
        return false
    }
    if !bytes.Equal(computedHash, rootHash) {
        return false
    }
    return true
}

func (sp *SimpleProof) String() string {
    return sp.StringIndented("")
}

func (sp *SimpleProof) StringIndented(indent string) string {
    return fmt.Sprintf(`SimpleProof{
%s  Aunts: %X
%s}`,
        indent, sp.Aunts,
        indent)
}

// Use the leafHash and innerHashes to get the root merkle hash.
// If the length of the innerHashes slice isn't exactly correct, the result is nil.
func computeHashFromAunts(index, total int, leafHash []byte, innerHashes [][]byte) ([]byte, error) {
    // Recursive impl.
    if index >= total {
        return nil, errors.Errorf("invalid index total <= index")
    }
    switch total {
        case 0:{
            return nil, errors.Errorf("Cannot call computeHashFromAunts() with 0 total")
        }
        case 1: {
            if len(innerHashes) != 0 {
                return nil, errors.Errorf("non-empty innerHashes")
            }
            return leafHash, nil
        }
        default: {
            if len(innerHashes) == 0 {
                return nil, errors.Errorf("non-empty innerHashes")
            }

            var numLeft int = int((total + 1) / 2)
            if index < numLeft {
                leftHash, err := computeHashFromAunts(index, numLeft, leafHash, innerHashes[:len(innerHashes)-1])
                if err != nil {
                    return nil, err
                }
                return SimpleHashFromTwoHashes(leftHash, innerHashes[len(innerHashes)-1])
            } else {
                rightHash, err := computeHashFromAunts(index - numLeft, total - numLeft, leafHash, innerHashes[:len(innerHashes)-1])
                if err != nil {
                    return nil, err
                }
                return SimpleHashFromTwoHashes(innerHashes[len(innerHashes)-1], rightHash)
            }
        }
    }
}

// Helper structure to construct merkle proof.
// The node and the tree is thrown away afterwards.
// Exactly one of node.Left and node.Right is nil, unless node is the root, in which case both are nil.
// node.Parent.Hash = hash(node.Hash, node.Right.Hash) or hash(node.Left.Hash, node.Hash), depending
// on whether node is a left/right child.
type SimpleProofNode struct {
    Hash   []byte
    Parent *SimpleProofNode
    Left   *SimpleProofNode // Left sibling  (only one of Left,Right is set)
    Right  *SimpleProofNode // Right sibling (only one of Left,Right is set)
}

// Starting from a leaf SimpleProofNode, FlattenAunts() will return
// the inner hashes for the item corresponding to the leaf.
func (spn *SimpleProofNode) FlattenAunts() [][]byte {
    // Nonrecursive impl.
    innerHashes := [][]byte{}
    for spn != nil {
        if spn.Left != nil {
            innerHashes = append(innerHashes, spn.Left.Hash)
        } else if spn.Right != nil {
            innerHashes = append(innerHashes, spn.Right.Hash)
        } else {
            break
        }
        spn = spn.Parent
    }
    return innerHashes
}

// trails[0].Hash is the leaf hash for items[0].
// trails[i].Parent.Parent....Parent == root for all i.
func trailsFromHashables(items []Hashable) (trails []*SimpleProofNode, root *SimpleProofNode) {
    // Recursive impl.
    switch len(items) {
        case 0: {
            return nil, nil
        }
        case 1: {
            trail := &SimpleProofNode{items[0].Hash(), nil, nil, nil}
            return []*SimpleProofNode{trail}, trail
        }
        default: {
            lefts, leftRoot := trailsFromHashables(items[:(len(items)+1)/2])
            rights, rightRoot := trailsFromHashables(items[(len(items)+1)/2:])
            rootHash, _ := SimpleHashFromTwoHashes(leftRoot.Hash, rightRoot.Hash)
            root := &SimpleProofNode{rootHash, nil, nil, nil}
            leftRoot.Parent = root
            leftRoot.Right = rightRoot
            rightRoot.Parent = root
            rightRoot.Left = leftRoot
            return append(lefts, rights...), root
        }
    }
}