package bptree

import (
	"bytes"
	"fmt"
)

func NewTree() *BTree {
	return &BTree{root: nil}
}

// Order must not be less than 3.
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
	kPrimeIdx := siblingIdx
	if siblingIdx < 0 {
		siblingIdx = 1
		kPrimeIdx = 0
	}

	kPrime := node.Parent.Keys[kPrimeIdx]
	sibling := node.Parent.Pointers[siblingIdx].(*BTreeNode)

	if sibling.Numkeys > minKeys {
		borrowFromSibling(sibling, node, kPrime)
		return nil
	}

	mergeNodes(node, sibling, kPrime)
	return nil
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

func (t *BTree) _deleteEntry(node *BTreeNode, key []byte, pointer interface{}) error {
	// If the root node is a leaf node and it only has `key` in it,
	// we delete the root node.
	if node == t.root && node.Numkeys == 1 {
		t.root = nil
		return nil
	}

	removeFromNode(node, key, pointer)
	// The minimum number of keys that a leaf can have is ORDER_HALF - 1 unless it's the root node.
	// And the minimum for a non leaf node is ORDER_HALF unless it's the root node.
	minKeyCount := ORDER_HALF - 1
	if !node.IsLeaf {
		minKeyCount = ORDER_HALF
	}

	// We subtracted 1 from minKeyCount to avoid `>=`.
	if node == t.root || node.Numkeys > minKeyCount-1 {
		// If the index is 0, we need to replace the parent key with the
		// key next to the removed one.
		// Eg. given the leaf [7, 8], and its parent [5, 7].
		// If we remove `7`, the leaf will be [8] and the parent will be [5, 8]
		if node.Parent != nil && bytes.Compare(node.Parent.Keys[0], key) == 0 {
			nodeIndexInParent := getKeyIndex(node.Parent, key)
			if nodeIndexInParent < 0 {
				return INVALID_KEY_INDEX_ERROR
			}

			node.Parent.Keys[nodeIndexInParent] = node.Keys[0]
		}

		return nil
	}

	parent := node.Parent
	nodePointerIndexInParent := getPointerIndex(parent, node)
	if nodePointerIndexInParent < 0 {
		return INVALID_KEY_INDEX_ERROR
	}

	leftSibling, rightSibling := getSiblings(parent, nodePointerIndexInParent)

	// If sibling has excess keys, we take one and insert it into our node.
	// We subtracted 1 from minKeyCount to avoid `>=`.
	if leftSibling != nil && leftSibling.Numkeys > minKeyCount {
		borrowFromSibling(leftSibling, node)
		// We need to change the parent key into the borrowed key
		if node.IsLeaf {
			// nodePointerIndexInParent-1 because the parent is a non-leaf node, thus the
			// pointers are more than keys by 1.
			parent.Keys[nodePointerIndexInParent-1] = node.Keys[0]
		}

		return nil
	}

	if rightSibling != nil && rightSibling.Numkeys > minKeyCount {
		borrowFromSibling(rightSibling, node)
		if node.IsLeaf {
			// We have to update the key for the right sibling in the parent as the
			// first index in keys had changed.
			parent.Keys[nodePointerIndexInParent] = rightSibling.Keys[0]
			if node.Numkeys == 1 {
				// We have to update the key for `node` in the parent as the
				// first index in keys had changed.
				parent.Keys[nodePointerIndexInParent-1] = node.Keys[0]
			}
		}

		return nil
	}

	// We need to merge the node with its sibling as there are no keys to borrow.
	var node1, node2 *BTreeNode
	if leftSibling != nil {
		// We need to reset next and prev to account for the leaf being removed.
		if node.IsLeaf {
			leftSibling.Next = node.Next
			node.Next.Prev = leftSibling
		}

		node1 = leftSibling
		node2 = node
	} else {
		if node.IsLeaf {
			rightSibling.Prev = node.Prev
			node.Prev.Next = rightSibling
		}

		node1 = node
		node2 = rightSibling
	}

	newNumKeys := node1.Numkeys + node2.Numkeys
	i := node1.Numkeys
	for ; i < newNumKeys; i++ {
		node1.Keys[i] = node2.Keys[i-node1.Numkeys]
		node1.Pointers[i] = node2.Pointers[i-node1.Numkeys]
	}
	if !node.IsLeaf {
		node1.Pointers[i] = node2.Pointers[i-node1.Numkeys]
	}

	var keyToRemove []byte = nil
	if nodePointerIndexInParent-1 > -1 {
		keyToRemove = node.Keys[nodePointerIndexInParent-1]
	}
	return t.deleteEntry(node.Parent, keyToRemove, node)
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

func borrowFromSibling(sibling, node *BTreeNode) {
	lastSiblingKeyIdx := sibling.Numkeys - 1
	nonLeafNodeAdjustment := 0
	if !node.IsLeaf {
		nonLeafNodeAdjustment = 1
	}

	if bytes.Compare(sibling.Keys[lastSiblingKeyIdx], node.Keys[0]) < 0 { // Sibling is a left sibling
		i := node.Numkeys
		for ; i > 0; i-- {
			node.Keys[i] = node.Keys[i-1]
			node.Pointers[i+nonLeafNodeAdjustment] = node.Pointers[i-1+nonLeafNodeAdjustment]
		}
		if !node.IsLeaf {
			node.Pointers[i+nonLeafNodeAdjustment] = node.Pointers[i]
		}

		node.Keys[0] = sibling.Keys[lastSiblingKeyIdx]
		node.Pointers[0] = sibling.Pointers[lastSiblingKeyIdx+nonLeafNodeAdjustment]
		node.Numkeys++
		sibling.Numkeys--
		return
	}

	node.Keys[node.Numkeys] = sibling.Keys[0]
	node.Pointers[node.Numkeys+nonLeafNodeAdjustment] = sibling.Pointers[0]
	i := 0
	for ; i < sibling.Numkeys-1; i++ {
		sibling.Keys[i] = sibling.Keys[i+1]
		sibling.Pointers[i] = sibling.Pointers[i+1]
	}
	if !node.IsLeaf {
		node.Pointers[i] = node.Pointers[i+1]
	}

	node.Numkeys++
	sibling.Numkeys--
}

func removeFromNode(node *BTreeNode, key []byte, pointer interface{}) error {
	keyIdx := getKeyIndex(node, key)
	if keyIdx < 0 {
		return INVALID_KEY_INDEX_ERROR
	}

	for i := keyIdx + 1; i < node.Numkeys; i++ {
		node.Keys[i-1] = node.Keys[i]
	}
	// Reset the remvoed key
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

	// Pointers length is larger than keys length by 1
	for i := 0; i < node.Numkeys+1; i++ {
		if node.Pointers[i] == pointer {
			idx = i
			break
		}
	}

	return idx
}
