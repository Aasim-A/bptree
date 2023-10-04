package bptree

import (
	"bytes"
	"fmt"
)

func NewTree() *BTree {
	return &BTree{root: nil}
}

// Order must not be less than 4.
const ORDER = 4

// Ceil the division. eg. 7/2 = 3, 7%2 = 1. 3+1 = 4.
const ORDER_HALF = ORDER/2 + (ORDER % 2)

type Record struct {
	Value []byte
}

type BTreeNode struct {
	Keys     [][]byte
	Numkeys  int
	Pointers []interface{}
	IsLeaf   bool
	Parent   *BTreeNode
	Next     *BTreeNode
	Prev     *BTreeNode
}

type BTree struct {
	root    *BTreeNode
	keySize int
}

func (t *BTree) Find(key []byte) (*Record, error) {
	// We do this before findLeaf for performance reasons.
	if key == nil {
		return nil, KEY_NOT_FOUND_ERROR
	}

	leaf := t.findLeaf(key)
	if leaf == nil {
		return nil, KEY_NOT_FOUND_ERROR
	}

	idx := getKeyIndex(leaf, key)
	if idx < 0 {
		return nil, KEY_NOT_FOUND_ERROR
	}

	return leaf.Pointers[idx].(*Record), nil
}

func (t *BTree) Update(key, newValue []byte) error {
	// We do this before findLeaf for performance reasons.
	if key == nil {
		return KEY_NOT_FOUND_ERROR
	}

	leaf := t.findLeaf(key)
	if leaf == nil {
		return KEY_NOT_FOUND_ERROR
	}

	idx := getKeyIndex(leaf, key)
	if idx < 0 {
		return KEY_NOT_FOUND_ERROR
	}

	newValueRecord := &Record{newValue}
	leaf.Pointers[idx] = newValueRecord

	return nil
}

func (t *BTree) Insert(key, value []byte) error {
	// We do this before findLeaf for performance reasons.
	if key == nil {
		return KEY_NOT_FOUND_ERROR
	}

	leaf := t.findLeaf(key)
	if leaf != nil {
		idx := getKeyIndex(leaf, key)
		if idx > -1 {
			return KEY_ALREADY_EXISTS_ERROR
		}
	}

	pointer := &Record{Value: value}
	if t.root == nil {
		t.root = makeLeaf()
		t.root.Keys[0] = key
		t.root.Pointers[0] = pointer
		t.root.Numkeys++
		t.keySize = len(key)

		return nil
	}

	if len(key) != t.keySize {
		return INVALID_KEY_SIZE_ERROR
	}

	if leaf.Numkeys < ORDER-1 {
		insertIntoNode(leaf, key, pointer)
		return nil
	}

	t.recursivelySplitAndInsert(leaf, key, pointer)
	return nil
}

func (t *BTree) findLeaf(key []byte) *BTreeNode {
	node := t.root
	if node == nil || key == nil {
		return nil
	}

	for !node.IsLeaf {
		i := 0
		for i < node.Numkeys {
			if bytes.Compare(key, node.Keys[i]) >= 0 {
				i++
			} else {
				break
			}
		}

		node = node.Pointers[i].(*BTreeNode)
	}

	return node
}

func (t *BTree) recursivelySplitAndInsert(node *BTreeNode, key []byte, pointer interface{}) {
	var newNode *BTreeNode
	if node.IsLeaf {
		newNode = makeLeaf()
		newNode.Next = node.Next
		newNode.Prev = node
		node.Next = newNode
	} else {
		newNode = makeNode()
	}

	newNode.Parent = node.Parent
	tempNode := &BTreeNode{
		Keys:     make([][]byte, ORDER),
		Pointers: make([]interface{}, ORDER+1),
		IsLeaf:   node.IsLeaf,
		Numkeys:  node.Numkeys,
	}

	i := 0
	for i = 0; i < node.Numkeys; i++ {
		tempNode.Keys[i] = node.Keys[i]
		tempNode.Pointers[i] = node.Pointers[i]
	}
	// Add the extra pointer since the pointers slice is larger that the keys slice by one.
	// `i` will be increased by one after the loop finishes because it increases then checks the condition.
	tempNode.Pointers[i] = node.Pointers[i]

	insertIntoNode(tempNode, key, pointer)
	// Reset numkeys to reflect new content.
	node.Numkeys = 0
	node.Keys = make([][]byte, ORDER-1)
	node.Pointers = make([]interface{}, ORDER)
	for i = 0; i < ORDER_HALF; i++ {
		node.Keys[i] = tempNode.Keys[i]
		node.Pointers[i] = tempNode.Pointers[i]
		node.Numkeys++
	}

	nodePointerAdjustment := 0
	if !node.IsLeaf {
		// Add the extra pointer since the pointers slice is larger that the keys slice by one.
		// `i` will be increased by one after the loop finishes because it increases then checks the condition.
		node.Pointers[i] = tempNode.Pointers[i]
		nodePointerAdjustment = 1
	}

	for i = ORDER_HALF; i < ORDER; i++ {
		if node.IsLeaf {
			newNode.Keys[i-ORDER_HALF] = tempNode.Keys[i]
			newNode.Numkeys++
		} else {
			if i > ORDER_HALF {
				newNode.Keys[i-ORDER_HALF-1] = tempNode.Keys[i]
				newNode.Numkeys++
			}

			tempNode.Pointers[i+nodePointerAdjustment].(*BTreeNode).Parent = newNode
		}
		newNode.Pointers[i-ORDER_HALF] = tempNode.Pointers[i+nodePointerAdjustment]
	}

	if node == t.root {
		t.splitRootAndInsert(node, newNode, tempNode.Keys[ORDER_HALF])
		return
	}

	if node.Parent.Numkeys < ORDER-1 {
		if node.IsLeaf {
			insertIntoNode(node.Parent, newNode.Keys[0], newNode)
			return
		}

		insertIntoNode(node.Parent, tempNode.Keys[ORDER_HALF], newNode)
		return
	}

	if node.IsLeaf {
		t.recursivelySplitAndInsert(node.Parent, newNode.Keys[0], newNode)
		return
	}

	t.recursivelySplitAndInsert(node.Parent, tempNode.Keys[ORDER_HALF], newNode)
}

func (t *BTree) splitRootAndInsert(node, newNode *BTreeNode, nonLeafKeyToAddToParent []byte) {
	newParent := makeNode()
	if node.IsLeaf {
		newParent.Keys[0] = newNode.Keys[0]
	} else {
		newParent.Keys[0] = nonLeafKeyToAddToParent
	}

	newParent.Pointers[0] = node
	newParent.Pointers[1] = newNode
	newParent.Numkeys++
	node.Parent = newParent
	newNode.Parent = newParent
	t.root = newParent
}

func (t *BTree) Delete(key []byte) error {
	// We do this before findLeaf for performance reasons.
	if key == nil {
		return KEY_NOT_FOUND_ERROR
	}

	leaf := t.findLeaf(key)
	if leaf == nil {
		return KEY_NOT_FOUND_ERROR
	}

	idx := getKeyIndex(leaf, key)
	if idx < 0 {
		return KEY_NOT_FOUND_ERROR
	}

	return t.deleteEntry(leaf, key, leaf.Pointers[idx])
}

func (t *BTree) deleteEntry(node *BTreeNode, key []byte, pointer interface{}) error {
	err := removeFromNode(node, key, pointer)
	if err != nil {
		return err
	}

	if node == t.root {
		t.adjustRoot()
		return nil
	}

	minKeys := ORDER_HALF - 1
	// We subtracted 1 to avoid '>='
	if node.Numkeys > minKeys-1 {
		return nil
	}

	siblingIdx := getSiblingIndex(node)
	nodeIdx := siblingIdx + 1
	kPrimeIdx := siblingIdx
	if siblingIdx < 0 {
		siblingIdx = 1
		kPrimeIdx = 0
	}

	// ChatGPT: In a B+ tree, the term "k_prime" typically refers to the minimum
	// key value in a node that separates it from its right sibling.
	// This value is used to maintain the order and balance of the tree.
	kPrime := node.Parent.Keys[kPrimeIdx]
	sibling, ok := node.Parent.Pointers[siblingIdx].(*BTreeNode)
	if !ok {
		fmt.Println("hhh")
	}

	if sibling.Numkeys > minKeys {
		borrowFromSibling(node, sibling, siblingIdx < nodeIdx, kPrime, kPrimeIdx)
		return nil
	}

	return t.mergeNodes(node, sibling, siblingIdx < nodeIdx, kPrime)
}

func getSiblingIndex(node *BTreeNode) int {
	siblingIdx := -1
	for i := 0; i < node.Parent.Numkeys+1; i++ {
		if node.Parent.Pointers[i] == node {
			siblingIdx = i - 1
			break
		}
	}

	return siblingIdx
}

func (t *BTree) adjustRoot() {
	if t.root.Numkeys > 0 {
		return
	}

	var newRoot *BTreeNode
	if !t.root.IsLeaf {
		newRoot = t.root.Pointers[0].(*BTreeNode)
		newRoot.Parent = nil
	}

	t.root = newRoot
}

func getSiblings(parent *BTreeNode, nodePointerIndex int) (*BTreeNode, *BTreeNode) {
	var left, right *BTreeNode
	if nodePointerIndex-1 > -1 {
		left = parent.Pointers[nodePointerIndex-1].(*BTreeNode)
	}

	numPointers := parent.Numkeys
	if !parent.IsLeaf {
		numPointers++
	}

	if nodePointerIndex+1 < numPointers {
		right = parent.Pointers[nodePointerIndex+1].(*BTreeNode)
	}

	return left, right
}

func borrowFromSibling(node, sibling *BTreeNode, isLeftSibling bool, kPrime []byte, kPrimeIdx int) {
	if !node.IsLeaf {
		if isLeftSibling {
			// Sibling is on the left.
			// We need to shift node's keys & pointers to the right by one.
			i := node.Numkeys
			for ; i > 0; i-- {
				node.Keys[i] = node.Keys[i-1]
				node.Pointers[i+1] = node.Pointers[i]
			}
			// We need to account for the extra pointer since this is a non leaf node.
			node.Pointers[i+1] = node.Pointers[i]

			// The key to be inserted is `kPrime` because this is a non leaf node.
			// Inserting `kPrime` instead of the first key of sibling ensures that
			// tree traversal still works. Otherwise, parts of the tree will become
			// inaccessible.
			node.Keys[0] = kPrime
			node.Pointers[0] = sibling.Pointers[sibling.Numkeys]
			// We need to set the parent of the borrowed pointer to node since its
			// parent is changing.
			node.Pointers[0].(*BTreeNode).Parent = node
			// Update the parent key with the key to be removed from sibling.
			node.Parent.Keys[kPrimeIdx] = sibling.Keys[sibling.Numkeys-1]
			node.Numkeys++
			// Resetting the borrowed key & pointer.
			sibling.Keys[sibling.Numkeys-1] = nil
			sibling.Pointers[sibling.Numkeys] = nil
			sibling.Numkeys--

			return
		}

		// Sibling is on the right.
		// The key to be inserted into node is also `kPrime` for the above mentioned reasons.
		node.Keys[node.Numkeys] = kPrime
		node.Pointers[node.Numkeys+1] = sibling.Pointers[0]
		// We need to set the parent of the borrowed pointer to node since its
		// parent is changing.
		node.Pointers[node.Numkeys+1].(*BTreeNode).Parent = node
		// Update the parent key with the key to be removed from sibling.
		node.Parent.Keys[kPrimeIdx] = sibling.Keys[0]
		node.Numkeys++
		// We need to shift sibling's keys & pointers to the left by one.
		i := 0
		for ; i < sibling.Numkeys-1; i++ {
			sibling.Keys[i] = sibling.Keys[i+1]
			sibling.Pointers[i] = sibling.Pointers[i+1]
			sibling.Keys[i+1] = nil
			sibling.Pointers[i+1] = nil
		}
		// We need to account for the extra pointer since this is a non leaf node.
		sibling.Pointers[i] = sibling.Pointers[i+1]
		sibling.Pointers[i+1] = nil

		// Set borrowed key & pointer to nil.
		sibling.Keys[i] = nil
		sibling.Pointers[i+1] = nil
		sibling.Numkeys--

		return
	}

	// Leaf node operations
	if isLeftSibling {
		// Sibling is on the left.
		// Shifting node's keys & pointers to the right to make room for the
		// key & pointer to be inserted.
		for i := node.Numkeys; i > 0; i-- {
			node.Keys[i] = node.Keys[i-1]
			node.Pointers[i] = node.Pointers[i-1]
		}

		// Since this is a leaf node, we don't need to use `kPrime`.
		node.Keys[0] = sibling.Keys[sibling.Numkeys-1]
		node.Pointers[0] = sibling.Pointers[sibling.Numkeys-1]
		// We need to update the parent's key to the newly inserted key
		// since it'll be placed in index 0.
		node.Parent.Keys[kPrimeIdx] = node.Keys[0]
		node.Numkeys++
		// Set the borrowed key & pointer to nil.
		sibling.Keys[sibling.Numkeys-1] = nil
		sibling.Pointers[sibling.Numkeys-1] = nil
		sibling.Numkeys--

		return
	}

	// Sibling is on the right.
	node.Keys[node.Numkeys] = sibling.Keys[0]
	node.Pointers[node.Numkeys] = sibling.Pointers[0]
	// Updating the key is required since sibling's index 0 key is changing.
	// Sibling's index 1 key will become index 0 key after shifting.
	node.Parent.Keys[kPrimeIdx] = sibling.Keys[1]
	node.Numkeys++
	// Shifting sibling's keys & pointers to the left by one.
	for i := 0; i < sibling.Numkeys-1; i++ {
		sibling.Keys[i] = sibling.Keys[i+1]
		sibling.Pointers[i] = sibling.Pointers[i+1]
		sibling.Keys[i+1] = nil
		sibling.Pointers[i+1] = nil
	}

	sibling.Numkeys--
}

func (t *BTree) mergeNodes(node, sibling *BTreeNode, isLeftSibling bool, kPrime []byte) error {
	if !isLeftSibling {
		tmp := node
		node = sibling
		sibling = tmp
	}

	insertionIndex := sibling.Numkeys
	if !node.IsLeaf {
		// `kPrime` needs to be added first to ensure tree balance,
		// and to make the final sum of the keys less than pointers by 1.
		// If we add keys & pointers without `kPrime`, not only will the
		// tree balance break, the final keys length will be less than
		// pointers by 2.
		sibling.Keys[insertionIndex] = kPrime
		sibling.Numkeys++

		j := 0
		i := insertionIndex + 1
		for ; j < node.Numkeys; j++ {
			sibling.Keys[i] = node.Keys[j]
			sibling.Pointers[i] = node.Pointers[j]
			sibling.Pointers[i].(*BTreeNode).Parent = sibling
			i++
		}
		sibling.Pointers[i] = node.Pointers[j]
		sibling.Pointers[i].(*BTreeNode).Parent = sibling
	} else {
		i := insertionIndex
		for j := 0; j < node.Numkeys; j++ {
			sibling.Keys[i] = node.Keys[j]
			sibling.Pointers[i] = node.Pointers[j]
			i++
		}
	}

	sibling.Numkeys += node.Numkeys
	return t.deleteEntry(node.Parent, kPrime, node)
}

func removeFromNode(node *BTreeNode, key []byte, pointer interface{}) error {
	keyIdx := getKeyIndex(node, key)
	if keyIdx < 0 {
		return INVALID_KEY_INDEX_ERROR
	}

	for i := keyIdx + 1; i < node.Numkeys; i++ {
		node.Keys[i-1] = node.Keys[i]
	}
	// Reset the removed key
	node.Keys[node.Numkeys-1] = nil

	numPointers := node.Numkeys
	if !node.IsLeaf {
		numPointers++
	}

	pointerIdx := getPointerIndex(node, pointer)
	if pointerIdx < 0 {
		return INVALID_POINTER_INDEX_ERROR
	}

	for i := pointerIdx + 1; i < numPointers; i++ {
		node.Pointers[i-1] = node.Pointers[i]
	}

	// Reset the removed pointer
	node.Pointers[numPointers-1] = nil
	node.Numkeys--

	if node.IsLeaf && node.Parent != nil && keyIdx == 0 && node.Numkeys > 0 {
		// We need to set the parent key to node key in index 0 since
		// it has changed.
		oldKeyIdxInParent := getKeyIndex(node.Parent, key)
		if oldKeyIdxInParent > 0 {
			node.Parent.Keys[oldKeyIdxInParent] = node.Keys[0]
		}
	}

	return nil
}

func (t *BTree) Print(withPointers bool) {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return
	}

	queue := []*BTreeNode{t.root}
	for len(queue) > 0 {
		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			node := queue[0]
			queue = queue[1:]
			fmt.Printf("%s", node.Keys[:node.Numkeys])
			if withPointers {
				if !node.IsLeaf {
					fmt.Printf("%p ", node)
				}
				fmt.Printf("%p", node.Parent)
			}

			if !node.IsLeaf {
				nodes := make([]*BTreeNode, node.Numkeys+1)
				for i := range nodes {
					nodes[i] = node.Pointers[i].(*BTreeNode)
				}

				queue = append(queue, nodes...)
			}

			if i < levelSize-1 {
				fmt.Print(", ")
			}
		}

		fmt.Println()
	}
}

func (t *BTree) PrintLeaves() {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return
	}

	leaf := t.root
	for !leaf.IsLeaf {
		leaf = leaf.Pointers[0].(*BTreeNode)
	}

	for leaf != nil {
		fmt.Print(leaf.Keys[:leaf.Numkeys])
		leaf = leaf.Next
	}
	fmt.Println()
}

func (t *BTree) PrintLeavesBackwards() {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return
	}

	leaf := t.root
	for !leaf.IsLeaf {
		leaf = leaf.Pointers[leaf.Numkeys].(*BTreeNode)
	}

	for leaf != nil {
		fmt.Print(leaf.Keys[:leaf.Numkeys])
		leaf = leaf.Prev
	}
	fmt.Println()
}

func makeNode() *BTreeNode {
	return &BTreeNode{
		Keys:     make([][]byte, ORDER-1),
		Numkeys:  0,
		Pointers: make([]interface{}, ORDER),
		IsLeaf:   false,
		Parent:   nil,
		Next:     nil,
		Prev:     nil,
	}
}

func makeLeaf() *BTreeNode {
	node := makeNode()
	node.IsLeaf = true

	return node
}

func insertIntoNode(node *BTreeNode, key []byte, pointer interface{}) {
	insertionIndex := getInsertionIndex(node, key)
	nonLeafNodeAdjustment := 0
	if !node.IsLeaf {
		nonLeafNodeAdjustment = 1
	}

	for i := node.Numkeys; i > insertionIndex; i-- {
		node.Keys[i] = node.Keys[i-1]
		node.Pointers[i+nonLeafNodeAdjustment] = node.Pointers[i-1+nonLeafNodeAdjustment]
	}

	node.Keys[insertionIndex] = key
	node.Pointers[insertionIndex+nonLeafNodeAdjustment] = pointer
	node.Numkeys++
}

// Gets the index that `key` needs to be inserted into.
// Returns -1 if `node` or `key` is nil.
func getInsertionIndex(node *BTreeNode, key []byte) int {
	insertionIndex := 0
	for insertionIndex < node.Numkeys && bytes.Compare(key, node.Keys[insertionIndex]) >= 0 {
		insertionIndex++
	}

	return insertionIndex
}

// Returns the index of `key`.
// If key is not found, it returns -1
func getKeyIndex(node *BTreeNode, key []byte) int {
	idx := -1
	if key == nil {
		return idx
	}

	for i := 0; i < node.Numkeys; i++ {
		if bytes.Compare(key, node.Keys[i]) == 0 {
			idx = i
			break
		}
	}

	return idx
}

// Returns the index of `pointer`.
// If pointer is not found, it returns -1
func getPointerIndex(node *BTreeNode, pointer interface{}) int {
	idx := -1
	if node == nil || pointer == nil {
		return idx
	}

	nonLeafNodeAdjustment := 0
	if !node.IsLeaf {
		nonLeafNodeAdjustment = 1
	}

	for i := 0; i < node.Numkeys+nonLeafNodeAdjustment; i++ {
		if node.Pointers[i] == pointer {
			idx = i
			break
		}
	}

	return idx
}
