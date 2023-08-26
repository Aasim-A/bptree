package bptree

import (
	"bytes"
	"errors"
	"fmt"
)

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
}

type BTree struct {
	Root *BTreeNode
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

func (t *BTree) Insert(key, value []byte) error {
	val, _ := t.Find(key)
	if val != nil {
		return errors.New("Key already exists")
	}

	pointer := &Record{Value: value}
	if t.Root == nil {
		t.Root = makeLeaf()
		t.Root.Keys[0] = key
		t.Root.Pointers[0] = pointer
		t.Root.Numkeys++

		return nil
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
	node := t.Root
	if node == nil {
		return node
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
	sibling := &BTreeNode{}
	if node.IsLeaf {
		sibling = makeLeaf()
		sibling.Next = node.Next
		node.Next = sibling
	} else {
		sibling = makeNode()
	}
	sibling.Parent = node.Parent

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
			sibling.Keys[i-adjustedOrderHalf] = node.Keys[i]
			sibling.Numkeys++
			node.Keys[i] = nil
		} else if i > adjustedOrderHalf {
			sibling.Keys[i-adjustedOrderHalf-1] = node.Keys[i]
			sibling.Numkeys++
			node.Keys[i] = nil
		}
		sibling.Pointers[i-adjustedOrderHalf] = node.Pointers[i+nodePointerAdjustment]
	}
	node.Numkeys = adjustedOrderHalf

	if insertionIndex < ORDER_HALF {
		insertIntoNode(node, key, pointer)
	} else {
		insertIntoNode(sibling, key, pointer)
	}

	if node == t.Root {
		newParent := makeNode()
		if node.IsLeaf {
			newParent.Keys[0] = sibling.Keys[0]
		} else {
			newParent.Keys[0] = nonLeafKeyToAddToParent
		}
		newParent.Pointers[0] = node
		newParent.Pointers[1] = sibling
		newParent.Numkeys++
		node.Parent = newParent
		sibling.Parent = newParent
		t.Root = newParent
		return
	}

	if node.Parent.Numkeys < ORDER-1 {
		if node.IsLeaf {
			insertIntoNode(node.Parent, sibling.Keys[0], sibling)
			return
		}

		insertIntoNode(node.Parent, nonLeafKeyToAddToParent, sibling)
		return
	}

	if node.IsLeaf {
		t.recursivelySplitAndInsert(node.Parent, sibling.Keys[0], sibling)
		return
	}

	t.recursivelySplitAndInsert(node.Parent, nonLeafKeyToAddToParent, sibling)
}

func (t *BTree) Print() {
	node := t.Root
	printNode(node)
}

func printNode(node interface{}) {
	isEnd := printPointer(node)
	if isEnd {
		return
	}

	fmt.Println()
	nd := node.(*BTreeNode)
	if !nd.IsLeaf {
		nd.Numkeys++
	}
	for i := 0; i < nd.Numkeys; i++ {
		printNode(nd.Pointers[i])
	}
	fmt.Println()
	fmt.Println()
}

func printPointer(node interface{}) bool {
	btreeNode, ok := node.(*BTreeNode)
	if ok {
		for i := 0; i < btreeNode.Numkeys; i++ {
			fmt.Print(string(btreeNode.Keys[i]), " ")
		}
		return false
	} else {
		fmt.Print(string(node.(*Record).Value), " ")
		return true
	}
}

func makeNode() *BTreeNode {
	return &BTreeNode{
		Keys:     make([][]byte, ORDER-1),
		Numkeys:  0,
		Pointers: make([]interface{}, ORDER),
		IsLeaf:   false,
		Parent:   nil,
		Next:     nil,
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

func getInsertionIndex(leaf *BTreeNode, key []byte) int {
	insertionIndex := 0
	for insertionIndex < leaf.Numkeys && bytes.Compare(key, leaf.Keys[insertionIndex]) >= 0 {
		insertionIndex++
	}

	return insertionIndex
}
