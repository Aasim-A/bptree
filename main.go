package main

import (
	"bytes"
	"errors"
	"fmt"
)

const ORDER = 4

var ORDER_HALF = cut()

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
		return nil, errors.New("Tree is empty")
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

func (t *BTree) Insert(key, value []byte) {
	if t.Root == nil {
		t.Root = makeLeaf()
		t.Root.Keys[0] = key
		t.Root.Pointers[0] = &Record{Value: value}

		return
	}

	leaf := t.findLeaf(key)
	if leaf.Numkeys < ORDER-1 {
		insertIntoLeaf(leaf, key, value)
		return
	}
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

func insertIntoLeaf(leaf *BTreeNode, key, value []byte) {
	insertionIndex := getInsertionIndex(leaf, key)

	for i := leaf.Numkeys; i > insertionIndex; i-- {
		leaf.Keys[i] = leaf.Keys[i-1]
		leaf.Pointers[i] = leaf.Pointers[i-1]
	}

	leaf.Keys[insertionIndex] = key
	leaf.Pointers[insertionIndex] = &Record{Value: value}
	leaf.Numkeys++
}

func (t *BTree) insertIntoLeafAfterSplitting(leaf *BTreeNode, key, value []byte) {
	newLeaf := makeLeaf()
	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf

	insertionIndex := getInsertionIndex(leaf, key)
	var adjustedOrderHalf int
	if insertionIndex < ORDER_HALF {
		adjustedOrderHalf = ORDER_HALF - 1
	}

	for i := adjustedOrderHalf; i < leaf.Numkeys; i++ {
		newLeaf.Keys[i-adjustedOrderHalf] = leaf.Keys[i]
		newLeaf.Pointers[i-adjustedOrderHalf] = leaf.Pointers[i]
		newLeaf.Numkeys++
	}
	leaf.Numkeys = adjustedOrderHalf

	if insertionIndex < ORDER_HALF {
		insertIntoLeaf(leaf, key, value)
	} else {
		insertIntoLeaf(newLeaf, key, value)
	}

	if t.Root == leaf {
		newParent := makeNode()
		newParent.Keys[0] = newLeaf.Keys[0]
		newParent.Pointers[0] = leaf
		newParent.Pointers[1] = newLeaf
		newParent.Numkeys++
		leaf.Parent = newParent
		newLeaf.Parent = newParent
		t.Root = newParent
		return
	}
}

func getInsertionIndex(leaf *BTreeNode, key []byte) int {
	insertionIndex := 0
	for insertionIndex < leaf.Numkeys && bytes.Compare(key, leaf.Keys[insertionIndex]) >= 0 {
		insertionIndex++
	}

	return insertionIndex
}

func cut() int {
	res := ORDER / 2

	if ORDER%2 == 1 {
		res++
	}

	return res
}

func main() {
	node := makeLeaf()
	node.Keys[0] = []byte("0")
	node.Keys[1] = []byte("1")
	node.Keys[2] = []byte("2")
	node.Numkeys = 2
	fmt.Println(getInsertionIndex(node, []byte("3")))
}
