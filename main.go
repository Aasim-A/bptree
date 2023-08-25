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
	pointer := &Record{Value: value}
	if t.Root == nil {
		t.Root = makeLeaf()
		t.Root.Keys[0] = key
		t.Root.Pointers[0] = pointer
		t.Root.Numkeys++

		return
	}

	leaf := t.findLeaf(key)
	if leaf.Numkeys < ORDER-1 {
		insertIntoLeaf(leaf, key, pointer)
		return
	}

	t.insertIntoLeafAfterSplitting(leaf, key, pointer)
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

func (t *BTree) insertIntoLeafAfterSplitting(leaf *BTreeNode, key []byte, pointer interface{}) {
	t.recursivelySplitAndInsertNode(leaf, key, pointer)
}

func (t *BTree) recursivelySplitAndInsertNode(node *BTreeNode, key []byte, pointer interface{}) {
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
	for i := adjustedOrderHalf; i < node.Numkeys; i++ {
		if node.IsLeaf || i > adjustedOrderHalf {
			sibling.Keys[i-adjustedOrderHalf] = node.Keys[i]
			sibling.Numkeys++
		}
		sibling.Pointers[i-adjustedOrderHalf] = node.Pointers[i+nodePointerAdjustment]
	}
	node.Numkeys = adjustedOrderHalf

	if insertionIndex < ORDER_HALF {
		if node.IsLeaf {
			insertIntoLeaf(node, key, pointer)
		} else {
			insertIntoNode(node, key, pointer)
		}
	} else {
		if node.IsLeaf {
			insertIntoLeaf(sibling, key, pointer)
		} else {
			insertIntoNode(sibling, key, pointer)
		}
	}

	if node == t.Root {
		newParent := makeNode()
		if node.IsLeaf {
			newParent.Keys[0] = sibling.Keys[0]
		} else {
			newParent.Keys[0] = node.Keys[adjustedOrderHalf]
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

		insertIntoNode(node.Parent, node.Keys[adjustedOrderHalf], sibling)
		return
	}

	if node.IsLeaf {
		t.recursivelySplitAndInsertNode(node.Parent, sibling.Keys[0], sibling)
		return
	}

	t.recursivelySplitAndInsertNode(node.Parent, node.Keys[adjustedOrderHalf], sibling)
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

func insertIntoLeaf(leaf *BTreeNode, key []byte, pointer interface{}) {
	insertionIndex := getInsertionIndex(leaf, key)

	for i := leaf.Numkeys; i > insertionIndex; i-- {
		leaf.Keys[i] = leaf.Keys[i-1]
		leaf.Pointers[i] = leaf.Pointers[i-1]
	}

	leaf.Keys[insertionIndex] = key
	leaf.Pointers[insertionIndex] = pointer
	leaf.Numkeys++
}

func insertIntoNode(node *BTreeNode, key []byte, pointer interface{}) {
	insertionIndex := getInsertionIndex(node, key)

	for i := node.Numkeys; i > insertionIndex; i-- {
		node.Keys[i] = node.Keys[i-1]
		node.Pointers[i+1] = node.Pointers[i]
	}

	node.Keys[insertionIndex] = key
	node.Pointers[insertionIndex+1] = pointer
	node.Numkeys++
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
	node.Pointers[0] = &Record{[]byte("v0")}
	node.Keys[1] = []byte("1")
	node.Pointers[1] = &Record{[]byte("v1")}
	node.Numkeys = 2

	node2 := makeLeaf()
	node2.Keys[0] = []byte("2")
	node2.Pointers[0] = &Record{[]byte("v2")}
	node2.Keys[1] = []byte("3")
	node2.Pointers[1] = &Record{[]byte("v3")}
	node2.Numkeys = 2

	node3 := makeLeaf()
	node3.Keys[0] = []byte("4")
	node3.Pointers[0] = &Record{[]byte("v4")}
	node3.Keys[1] = []byte("5")
	node3.Pointers[1] = &Record{[]byte("v5")}
	node3.Numkeys = 2

	node4 := makeLeaf()
	node4.Keys[0] = []byte("6")
	node4.Pointers[0] = &Record{[]byte("v6")}
	node4.Keys[1] = []byte("7")
	node4.Pointers[1] = &Record{[]byte("v7")}
	node4.Keys[2] = []byte("8")
	node4.Pointers[2] = &Record{[]byte("v8")}
	node4.Numkeys = 3

	root := makeNode()
	root.Keys[0] = node2.Keys[0]
	root.Pointers[0] = node
	root.Keys[1] = node3.Keys[0]
	root.Pointers[1] = node2
	root.Keys[2] = node4.Keys[0]
	root.Pointers[2] = node3
	root.Pointers[3] = node4
	root.Numkeys = 3
	// tree := BTree{root}
	// tree.Print()
	// fmt.Println("------------------------")
	tree2 := BTree{}
	tree2.Insert([]byte("0"), []byte("v0"))
	tree2.Insert([]byte("1"), []byte("v1"))
	tree2.Insert([]byte("2"), []byte("v2"))
	tree2.Insert([]byte("3"), []byte("v3"))
	tree2.Insert([]byte("4"), []byte("v4"))
	tree2.Insert([]byte("5"), []byte("v5"))
	tree2.Insert([]byte("6"), []byte("v6"))
	tree2.Insert([]byte("7"), []byte("v7"))
	tree2.Insert([]byte("8"), []byte("v8"))
	tree2.Insert([]byte("9"), []byte("v9"))
	tree2.Insert([]byte("10"), []byte("v10"))
	tree2.Insert([]byte("11"), []byte("v11"))
	tree2.Insert([]byte("12"), []byte("v12"))
	tree2.Insert([]byte("13"), []byte("v13"))
	tree2.Insert([]byte("14"), []byte("v14"))
	tree2.Insert([]byte("15"), []byte("v15"))
	tree2.Print()
}
