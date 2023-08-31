package bptree

import (
	"bytes"
	"errors"
	"fmt"
)

func NewTree() *BTree {
	return &BTree{root: nil}
}

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
	node := t.findLeaf(key)
	if node == nil {
		return nil, errors.New("Key not found")
	}

	var i int
	found := false
	for i = node.Numkeys - 1; i >= 0; i-- {
		if bytes.Compare(node.Keys[i], key) == 0 {
			found = true
			break
		}
	}

	if !found {
		return nil, errors.New("Key not found")
	}

	return node.Pointers[i].(*Record), nil
}

func (t *BTree) Update(key, newValue []byte) error {
	leaf := t.findLeaf(key)
	if leaf == nil {
		return errors.New("Key not found")
	}

	var i int
	found := false
	for i = leaf.Numkeys - 1; i >= 0; i-- {
		if bytes.Compare(leaf.Keys[i], key) == 0 {
			found = true
			newValueRecord := &Record{newValue}
			leaf.Pointers[i] = newValueRecord
			break
		}
	}

	if !found {
		return errors.New("Key not found")
	}

	return nil
}

func (t *BTree) Insert(key, value []byte) error {
	val, _ := t.Find(key)
	if val != nil {
		return errors.New("Key already exists")
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
		return errors.New("Invalid key size. All keys must have the same length.")
	}

	leaf := t.findLeaf(key)
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
	newNode := &BTreeNode{}
	if node.IsLeaf {
		newNode = makeLeaf()
		newNode.Next = node.Next
		newNode.Prev = node
		node.Next = newNode
	} else {
		newNode = makeNode()
	}

	newNode.Parent = node.Parent

	adjustedOrderHalf := ORDER_HALF
	insertionIndex := getInsertionIndex(node, key)
	if insertionIndex < ORDER_HALF {
		adjustedOrderHalf--
	}

	nodePointerAdjustment := 0
	if !node.IsLeaf {
		nodePointerAdjustment = 1
	}

	var nonLeafKeyToAddToParent []byte
	if !node.IsLeaf {
		nonLeafKeyToAddToParent = node.Keys[adjustedOrderHalf]
	}

	for i := adjustedOrderHalf; i < node.Numkeys; i++ {
		if node.IsLeaf {
			newNode.Keys[i-adjustedOrderHalf] = node.Keys[i]
			newNode.Numkeys++
		} else if i > adjustedOrderHalf {
			newNode.Keys[i-adjustedOrderHalf-1] = node.Keys[i]
			newNode.Numkeys++
		}

		newNode.Pointers[i-adjustedOrderHalf] = node.Pointers[i+nodePointerAdjustment]
		pointerNode, ok := newNode.Pointers[i-adjustedOrderHalf].(*BTreeNode)
		if ok {
			pointerNode.Parent = newNode
		}

		node.Keys[i] = nil
		node.Pointers[i+nodePointerAdjustment] = nil
	}

	node.Numkeys = adjustedOrderHalf

	if insertionIndex < ORDER_HALF {
		insertIntoNode(node, key, pointer)
	} else {
		insertIntoNode(newNode, key, pointer)
	}

	if node == t.root {
		t.splitRootAndInsert(node, newNode, nonLeafKeyToAddToParent)
		return
	}

	if node.Parent.Numkeys < ORDER-1 {
		if node.IsLeaf {
			insertIntoNode(node.Parent, newNode.Keys[0], newNode)
			return
		}

		insertIntoNode(node.Parent, nonLeafKeyToAddToParent, newNode)
		return
	}

	if node.IsLeaf {
		t.recursivelySplitAndInsert(node.Parent, newNode.Keys[0], newNode)
		return
	}

	t.recursivelySplitAndInsert(node.Parent, nonLeafKeyToAddToParent, newNode)
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

func (t *BTree) Print() {
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
		fmt.Printf("%s, ", leaf.Keys[:leaf.Numkeys])
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
		fmt.Printf("%s, ", leaf.Keys[:leaf.Numkeys])
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

	if !node.IsLeaf {
		pointer.(*BTreeNode).Parent = node
	}

	node.Keys[insertionIndex] = key
	node.Pointers[insertionIndex+nonLeafNodeAdjustment] = pointer
	node.Numkeys++
}

func getInsertionIndex(leaf *BTreeNode, key []byte) int {
	insertionIndex := 0
	for insertionIndex < leaf.Numkeys && bytes.Compare(key, leaf.Keys[insertionIndex]) >= 0 {
		insertionIndex++
	}

	return insertionIndex
}
