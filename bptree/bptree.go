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

	// If the root node is a leaf node and it only has `key` in it,
	// we delete the root node.
	if leaf == t.root && leaf.Numkeys == 1 {
		t.root = nil
		return nil
	}

	removeFromLeaf(leaf, idx)

	// The minimum number of keys that a leaf can have is ORDER_HALF - 1 unless it's the root node.
	if leaf == t.root || leaf.Numkeys > ORDER_HALF-1 {
		// If the index is 0, we need to replace the parent key with the
		// key next to the removed one.
		// Eg. given the leaf [7, 8], and its parent [5, 7].
		// If we remove `7`, the leaf will be [8] and the parent will be [5, 8]
		if leaf.Parent != nil && idx == 0 {
			leafIndexInParent := getKeyIndex(leaf.Parent, key)
			if leafIndexInParent < 0 {
				return INVALID_KEY_INDEX_ERROR
			}

			leaf.Parent.Keys[leafIndexInParent] = leaf.Keys[0]
		}

		return nil
	}

	parent := leaf.Parent
	leafPointerIndexInParent := getPointerIndex(parent, leaf)
	if leafPointerIndexInParent < 0 {
		return INVALID_KEY_INDEX_ERROR
	}

	// Sibling indices can be -1 meaning that the sibling does not exist.
	leftSiblingIndex := leafPointerIndexInParent - 1
	var leftSibling *BTreeNode
	if leftSiblingIndex > -1 {
		leftSibling = parent.Pointers[leftSiblingIndex].(*BTreeNode)
	}

	// If the left sibling has excess keys, we take one and insert it into our node.
	if leftSibling != nil && leftSibling.Numkeys > ORDER_HALF-1 {
		borrowFromSibling(leftSibling, leaf)
		// We need to change the parent key into the borrowed key
		parent.Keys[leafPointerIndexInParent-1] = leaf.Keys[0]
		return nil
	}

	rightSiblingIndex := leafPointerIndexInParent + 1
	var rightSibling *BTreeNode
	if rightSiblingIndex < parent.Numkeys+1 {
		rightSibling = parent.Pointers[rightSiblingIndex].(*BTreeNode)
	}

	// If the right sibling has excess keys, we take one and insert it into our node.
	if rightSibling != nil && rightSibling.Numkeys > ORDER_HALF-1 {
		borrowFromSibling(rightSibling, leaf)
		siblingKeyIndexInParent := getKeyIndex(parent, leaf.Keys[leaf.Numkeys-1])
		if siblingKeyIndexInParent < 0 {
			return INVALID_KEY_INDEX_ERROR
		}

		// We have to update the key for the right sibling in the parent as the
		// first index in the sibling had changed.
		parent.Keys[siblingKeyIndexInParent] = rightSibling.Keys[0]
		return nil
	}

	// We need to merge the node with its sibling as there are no keys to borrow.
	if leftSibling != nil {

	}

	return nil
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

func removeFromLeaf(node *BTreeNode, idx int) {
	node.Keys[idx] = nil
	node.Pointers[idx] = nil

	// It's safe for i to be larger than Numkeys because
	// the loop will not execute if `i` is larger than Numkeys.
	for i := idx + 1; i < node.Numkeys; i++ {
		node.Keys[i-1] = node.Keys[i]
		node.Pointers[i-1] = node.Pointers[i]
		node.Keys[i] = nil
		node.Pointers[i] = nil
	}

	node.Numkeys--
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
			fmt.Print(node.Keys[:node.Numkeys])
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
