package diskbptree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"bptree/bptree"
)

// This DB consists of 3 parts, Head, one Master tree, and many subtrees.
//
// To make our db ACID (Atomicity, Consistency, Isolation, and Durability), we can divide the db into
// multiple sub trees with a fixed size, let's say 100MB. On the condition that the upper (above the subtrees)
// hierarchy is similar to that small size.
// To make a new change, a copy of the target subtree will be created, when copying is finished, the pointer to the
// old subtree will be changed to the newly created subtree, and the old one will become unused space. At the same time
// the master tree will be copied as well, then the pointers of all the subtrees and the head will be linked to the new master tree.
// If a subtree exceeds the fixed size, it will split into two subtrees. In general, the idea is that vertical distance
// will be short above the subtrees and normal within the subtrees.

// Order must not be less than 4.
const m_ORDER = 4

// Ceil the division. eg. 7/2 = 3, 7%2 = 1. 3+1 = 4.
const m_ORDER_HALF = m_ORDER/2 + (m_ORDER % 2)

const m_MASTER_PAGE_SIZE = 4096
const m_PAGE_SIZE = 8192
const m_GCM_IV_SIZE = 12
const m_GCM_AUTH_SIZE = 16
const m_MASTER_PAGE_DATA_SIZE = m_MASTER_PAGE_SIZE - m_GCM_IV_SIZE - m_GCM_AUTH_SIZE
const m_PAGE_DATA_SIZE = m_PAGE_SIZE - m_GCM_IV_SIZE - m_GCM_AUTH_SIZE

type DiskBTree struct {
	keySize    int
	dbFile     *os.File
	masterPage *MasterPage
}

func NewDiskTree(filePath string) (*DiskBTree, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}

	stats, err := f.Stat()
	if err != nil {
		return nil, err
	}

	diskBTree := DiskBTree{
		dbFile: f,
	}

	if stats.Size() > m_MASTER_PAGE_SIZE {
		err = diskBTree.readMasterPage()
		if err != nil {
			return nil, err
		}
	}

	return &diskBTree, nil
}

func (t *DiskBTree) readMasterPage() error {
	_, err := t.dbFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	masterpageBytes := make([]byte, m_MASTER_PAGE_SIZE)
	_, err = t.dbFile.Read(masterpageBytes)
	if err != nil {
		return err
	}

	rootPtr := binary.BigEndian.Uint64(masterpageBytes[0:8])
	pageCount := binary.BigEndian.Uint64(masterpageBytes[8:16])

	t.masterPage = &MasterPage{root: rootPtr, pageCount: pageCount}

	return nil
}

func (t *DiskBTree) writeMasterPage() error {
	_, err := t.dbFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	masterpageBytes := make([]byte, 4096)
	binary.BigEndian.PutUint64(masterpageBytes[0:8], t.masterPage.root)
	binary.BigEndian.PutUint64(masterpageBytes[8:16], t.masterPage.pageCount)

	_, err = t.dbFile.Write(masterpageBytes)

	return err
}

func (t *DiskBTree) readNode(ptr uint64) (*DiskBTreeNode, error) {
	nodeBytes := make([]byte, m_PAGE_SIZE)
	_, err := t.dbFile.Seek(int64(ptr), io.SeekStart)
	if err != nil {
		return nil, err
	}

	n, err := t.dbFile.Read(nodeBytes)
	if err != nil {
		return nil, err
	}

	if n != len(nodeBytes) {
		return nil, errors.New("Unexpected size was read")
	}

	return BytesToNode(nodeBytes, ptr), nil
}

func (t *DiskBTree) writeNode(nodeBytes []byte, ptr uint64) error {
	_, err := t.dbFile.Seek(int64(ptr), io.SeekStart)
	if err != nil {
		return err
	}

	n, err := t.dbFile.Write(nodeBytes)
	if err != nil {
		return err
	}

	if n != len(nodeBytes) {
		return errors.New("Unexpected size was written")
	}

	return nil
}

func (t *DiskBTree) Close() error {
	return t.dbFile.Close()
}

type MasterPage struct {
	root      uint64
	pageCount uint64
}

type DiskBTreeNode struct {
	Ptr      uint64
	IsLeaf   bool
	Numkeys  uint16
	Parent   uint64
	Next     uint64
	Prev     uint64
	Keysize  uint16
	Keys     [][]byte
	Pointers []interface{}
}

func (n *DiskBTreeNode) ToBytes() []byte {
	nodeBytes := make([]byte, m_PAGE_DATA_SIZE)
	if n.IsLeaf {
		nodeBytes[0] = 1
	}

	binary.BigEndian.PutUint16(nodeBytes[1:3], n.Numkeys)
	binary.BigEndian.PutUint64(nodeBytes[3:11], n.Parent)
	binary.BigEndian.PutUint64(nodeBytes[11:19], n.Next)
	binary.BigEndian.PutUint64(nodeBytes[19:27], n.Prev)
	binary.BigEndian.PutUint16(nodeBytes[27:29], n.Keysize)

	// Keys encoding
	start := uint16(29)
	end := start + n.Keysize
	for _, key := range n.Keys {
		copy(nodeBytes[start:end], key)

		start = end
		end += n.Keysize
	}

	// Pointers encoding
	if n.IsLeaf {
		for _, valInterface := range n.Pointers {
			val := valInterface.([]byte)
			dataLength := uint16(len(val))
			end = start + 2
			binary.BigEndian.PutUint16(nodeBytes[start:end], dataLength)

			start = end // Resetting start to write the value
			end += dataLength
			copy(nodeBytes[start:end], val)
			start = end // Resettings start to write the length
		}

	} else {
		// In non-leaf nodes, we're storing pointers which are 8 bytes long
		// so, we need to set the end accordingly
		end = start + 8
		for _, ptr := range n.Pointers {
			binary.BigEndian.PutUint64(nodeBytes[start:end], ptr.(uint64))

			start = end
			end += 8
		}
	}

	return nodeBytes
}

func BytesToNode(b []byte, ptr uint64) *DiskBTreeNode {
	node := DiskBTreeNode{}

	node.Ptr = ptr
	node.IsLeaf = b[0] == 1
	node.Numkeys = binary.BigEndian.Uint16(b[1:3])
	node.Parent = binary.BigEndian.Uint64(b[3:11])
	node.Next = binary.BigEndian.Uint64(b[11:19])
	node.Prev = binary.BigEndian.Uint64(b[19:27])
	node.Keysize = binary.BigEndian.Uint16(b[27:29])
	node.Keys = make([][]byte, node.Numkeys)
	node.Pointers = make([]interface{}, node.Numkeys)

	start := uint16(29)
	end := start + node.Keysize
	for i := uint16(0); i < node.Numkeys; i++ {
		node.Keys[i] = make([]byte, node.Keysize)
		copy(node.Keys[i], b[start:end])

		start = end
		end += node.Keysize
	}

	if node.IsLeaf {
		for i := uint16(0); i < node.Numkeys; i++ {
			end = start + 2
			valueLength := binary.BigEndian.Uint16(b[start:end])
			start = end
			end += valueLength
			node.Pointers[i] = b[start:end]
			start = end
		}
	} else {
		end = start + 8
		for i := uint16(0); i < node.Numkeys; i++ {
			node.Pointers[i] = binary.BigEndian.Uint64(b[start:end])
			start = end
			end += 8
		}
	}

	return &node
}

func (t *DiskBTree) Find(key []byte) ([]byte, error) {
	if t.masterPage == nil || key == nil {
		return nil, bptree.KEY_NOT_FOUND_ERROR
	}

	if len(key) != t.keySize {
		return nil, bptree.INVALID_KEY_SIZE_ERROR
	}

	leaf, err := t.findLeaf(key)
	if err != nil {
		return nil, err
	}

	idx := getKeyIndex(leaf, key)
	if idx < 0 {
		return nil, bptree.KEY_NOT_FOUND_ERROR
	}

	val, ok := leaf.Pointers[idx].([]byte)
	if !ok {
		return nil, bptree.TYPE_CONVERSION_ERROR
	}

	return val, nil
}

func (t *DiskBTree) Update(key, newValue []byte) error {
	if t.masterPage == nil || key == nil {
		return bptree.KEY_NOT_FOUND_ERROR
	}

	if len(key) != t.keySize {
		return bptree.INVALID_KEY_SIZE_ERROR
	}

	leaf, err := t.findLeaf(key)
	if err != nil {
		return err
	}

	idx := getKeyIndex(leaf, key)
	if idx < 0 {
		return bptree.KEY_NOT_FOUND_ERROR
	}

	leaf.Pointers[idx] = newValue

	return t.writeNode(leaf.ToBytes(), leaf.Ptr)
}

func (t *DiskBTree) Insert(key, value []byte) error {
	if key == nil || value == nil {
		return bptree.INVALID_DATA_ERROR
	}

	if len(key) > math.MaxUint16 {
		return bptree.KEY_SIZE_TOO_LARGE
	}

	if t.masterPage == nil {
		rootNode := makeLeaf(m_MASTER_PAGE_SIZE)
		rootNode.Keys[0] = key
		rootNode.Pointers[0] = value
		rootNode.Numkeys++
		t.keySize = len(key)

		t.masterPage.root = rootNode.Ptr
		t.masterPage.pageCount = 1
		err := t.writeMasterPage()
		if err != nil {
			return err
		}

		return t.writeNode(rootNode.ToBytes(), rootNode.Ptr)
	}

	leaf, err := t.findLeaf(key)
	if err == nil {
		idx := getKeyIndex(leaf, key)
		if idx > -1 {
			return bptree.KEY_ALREADY_EXISTS_ERROR
		}
	}

	if len(key) != t.keySize {
		return bptree.INVALID_KEY_SIZE_ERROR
	}

	if leaf.Numkeys < m_ORDER-1 {
		insertIntoNode(leaf, key, value)
		err = t.writeNode(leaf.ToBytes(), leaf.Ptr)
		if err != nil {
			return err
		}

		t.masterPage.pageCount++
		return t.writeMasterPage()
	}

	return t.recursivelySplitAndInsert(leaf, key, value)
}

func (t *DiskBTree) findLeaf(key []byte) (*DiskBTreeNode, error) {
	node, err := t.readNode(t.masterPage.root)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		i := uint16(0)
		for i < node.Numkeys {
			if bytes.Compare(key, node.Keys[i]) >= 0 {
				i++
			} else {
				break
			}
		}

		ptr, ok := node.Pointers[i].(uint64)
		if !ok {
			return nil, bptree.TYPE_CONVERSION_ERROR
		}

		node, err = t.readNode(ptr)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

func (t *DiskBTree) newPagePtr() uint64 {
	return m_MASTER_PAGE_SIZE + t.masterPage.pageCount*m_PAGE_SIZE
}

func (t *DiskBTree) recursivelySplitAndInsert(node *DiskBTreeNode, key []byte, pointer interface{}) error {
	var newNode *DiskBTreeNode
	newNodePtr := t.newPagePtr()
	if node.IsLeaf {
		newNode = makeLeaf(newNodePtr)
		newNode.Next = node.Next
		newNode.Prev = node.Ptr
		node.Next = newNode.Ptr
	} else {
		newNode = makeNode(newNodePtr)
	}

	newNode.Parent = node.Parent
	tempNode := &DiskBTreeNode{
		Keys:     make([][]byte, m_ORDER),
		Pointers: make([]interface{}, m_ORDER+1),
		IsLeaf:   node.IsLeaf,
		Numkeys:  node.Numkeys,
	}

	i := uint16(0)
	for i = 0; i < node.Numkeys; i++ {
		tempNode.Keys[i] = node.Keys[i]
		tempNode.Pointers[i] = node.Pointers[i]
	}
	// Add the extra pointer since the pointers slice is larger that the keys slice by one.
	// `i` will be increased by one after the loop finishes because it increases then checks the condition.
	tempNode.Pointers[i] = node.Pointers[i]

	// We don't want to write to disk since this is just a temp node.
	insertIntoNode(tempNode, key, pointer)
	// Reset numkeys to reflect new content.
	node.Numkeys = 0
	node.Keys = make([][]byte, m_ORDER-1)
	node.Pointers = make([]interface{}, m_ORDER)
	for i = 0; i < m_ORDER_HALF; i++ {
		node.Keys[i] = tempNode.Keys[i]
		node.Pointers[i] = tempNode.Pointers[i]
		node.Numkeys++
	}

	nodePointerAdjustment := uint16(0)
	if !node.IsLeaf {
		// Add the extra pointer since the pointers slice is larger that the keys slice by one.
		// `i` will be increased by one after the loop finishes because it increases then checks the condition.
		node.Pointers[i] = tempNode.Pointers[i]
		nodePointerAdjustment = 1
	}

	for i = m_ORDER_HALF; i < m_ORDER; i++ {
		if node.IsLeaf {
			newNode.Keys[i-m_ORDER_HALF] = tempNode.Keys[i]
			newNode.Numkeys++
		} else {
			if i > m_ORDER_HALF {
				newNode.Keys[i-m_ORDER_HALF-1] = tempNode.Keys[i]
				newNode.Numkeys++
			}

			ptr, ok := tempNode.Pointers[i+nodePointerAdjustment].(uint64)
			if !ok {
				return bptree.TYPE_CONVERSION_ERROR
			}

			childNode, err := t.readNode(ptr)
			if err != nil {
				return err
			}

			childNode.Parent = newNode.Ptr
			err = t.writeNode(childNode.ToBytes(), childNode.Ptr)
			if err != nil {
				return err
			}
		}
		newNode.Pointers[i-m_ORDER_HALF] = tempNode.Pointers[i+nodePointerAdjustment]
	}

	if node.Ptr == t.masterPage.root {
		// node and newNode are written to disk inside splitRootAndInsert
		return t.splitRootAndInsert(node, newNode, tempNode.Keys[m_ORDER_HALF])
	}

	// We need to write node and newNode to disk to persist changes
	err := t.writeNode(node.ToBytes(), node.Ptr)
	if err != nil {
		return err
	}

	err = t.writeNode(newNode.ToBytes(), newNode.Ptr)
	if err != nil {
		return err
	}

	t.masterPage.pageCount++
	err = t.writeMasterPage()
	if err != nil {
		return err
	}

	nodeParent, err := t.readNode(node.Parent)
	if err != nil {
		return err
	}

	if nodeParent.Numkeys < m_ORDER-1 {
		if node.IsLeaf {
			insertIntoNode(nodeParent, newNode.Keys[0], newNode)
		} else {
			insertIntoNode(nodeParent, tempNode.Keys[m_ORDER_HALF], newNode)
		}

		return t.writeNode(nodeParent.ToBytes(), nodeParent.Ptr)
	}

	if node.IsLeaf {
		return t.recursivelySplitAndInsert(nodeParent, newNode.Keys[0], newNode)
	}

	return t.recursivelySplitAndInsert(nodeParent, tempNode.Keys[m_ORDER_HALF], newNode)
}

func (t *DiskBTree) splitRootAndInsert(node, newNode *DiskBTreeNode, nonLeafKeyToAddToParent []byte) error {
	newParent := makeNode(t.newPagePtr())
	if node.IsLeaf {
		newParent.Keys[0] = newNode.Keys[0]
	} else {
		newParent.Keys[0] = nonLeafKeyToAddToParent
	}

	newParent.Pointers[0] = node
	newParent.Pointers[1] = newNode
	newParent.Numkeys++
	node.Parent = newParent.Ptr
	newNode.Parent = newParent.Ptr

	err := t.writeNode(newParent.ToBytes(), newParent.Ptr)
	if err != nil {
		return err
	}

	err = t.writeNode(node.ToBytes(), node.Ptr)
	if err != nil {
		return err
	}

	err = t.writeNode(newNode.ToBytes(), newNode.Ptr)
	if err != nil {
		return err
	}

	t.masterPage.root = newParent.Ptr
	t.masterPage.pageCount++
	return t.writeMasterPage()
}

func (t *DiskBTree) Delete(key []byte) error {
	if t.masterPage == nil || key == nil {
		return bptree.KEY_NOT_FOUND_ERROR
	}

	leaf, err := t.findLeaf(key)
	if err != nil {
		return err
	}

	idx := getKeyIndex(leaf, key)
	if idx < 0 {
		return bptree.KEY_NOT_FOUND_ERROR
	}

	return t.deleteEntry(leaf, key, leaf.Pointers[idx])
}

func (t *DiskBTree) deleteEntry(node *DiskBTreeNode, key []byte, pointer interface{}) error {
	err := t.removeFromNode(node, key, pointer)
	if err != nil {
		return err
	}

	if node.Ptr == t.masterPage.root {
		return t.adjustRoot()
	}

	minKeys := uint16(m_ORDER_HALF - 1)
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

	// ChatGPT: k_prime is often used to denote the key from the parent node that
	// separates the keys in the current node from the keys in the sibling node.
	// It helps in the redistribution or merging process.
	// For example, if a node and its right sibling are being merged, k_prime is
	// the key from the parent that previously separated these two nodes.
	// This key will be brought down to the merging nodes.
	// TODO: group parent writes to avoid writing multiple times.
	nodeParent, err := t.readNode(node.Parent)
	if err != nil {
		return err
	}

	kPrime := nodeParent.Keys[kPrimeIdx]
	siblingPtr, ok := nodeParent.Pointers[siblingIdx].(uint64)
	if !ok {
		return bptree.TYPE_CONVERSION_ERROR
	}

	sibling, err := t.readNode(siblingPtr)
	if err != nil {
		return err
	}

	if sibling.Numkeys > minKeys {
		// here boiz
		return borrowFromSibling(node, sibling, siblingIdx < nodeIdx, kPrime, kPrimeIdx)
	}

	return t.mergeNodes(node, sibling, siblingIdx < nodeIdx, kPrime)
}

func getSiblingIndex(node *DiskBTreeNode) int {
	siblingIdx := -1
	for i := 0; i < node.Parent.Numkeys+1; i++ {
		if node.Parent.Pointers[i] == node {
			siblingIdx = i - 1
			break
		}
	}

	return siblingIdx
}

func (t *DiskBTree) adjustRoot() error {
	rootNode, err := t.readNode(t.masterPage.root)
	if err != nil {
		return err
	}

	if rootNode.Numkeys > 0 {
		return nil
	}

	if !rootNode.IsLeaf {
		newRootPtr, ok := rootNode.Pointers[0].(uint64)
		if !ok {
			return bptree.TYPE_CONVERSION_ERROR
		}

		newRoot, err := t.readNode(newRootPtr)
		if err != nil {
			return err
		}

		// TODO: handle node deletion and pageCount decrementing
		t.masterPage.root = newRootPtr
		newRoot.Parent = 0
		err = t.writeNode(newRoot.ToBytes(), newRootPtr)
		if err != nil {
			return err
		}

		return t.writeMasterPage()
	}

	// We set the db file size to 0 i.e. deleting everything since the db is now empty.
	// We want to avoid writing data with all zeros to avoid enc key prediction.
	return t.dbFile.Truncate(0)
}

func borrowFromSibling(node, sibling *DiskBTreeNode, isLeftSibling bool, kPrime []byte, kPrimeIdx int) error {
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
			ptr, ok := node.Pointers[0].(*BTreeNode)
			if !ok {
				return bptree.TYPE_CONVERSION_ERROR
			}

			ptr.Parent = node
			// Update the parent key with the key to be removed from sibling.
			node.Parent.Keys[kPrimeIdx] = sibling.Keys[sibling.Numkeys-1]
			node.Numkeys++
			// Resetting the borrowed key & pointer.
			sibling.Keys[sibling.Numkeys-1] = nil
			sibling.Pointers[sibling.Numkeys] = nil
			sibling.Numkeys--

			return nil
		}

		// Sibling is on the right.
		// The key to be inserted into node is also `kPrime` for the above mentioned reasons.
		node.Keys[node.Numkeys] = kPrime
		node.Pointers[node.Numkeys+1] = sibling.Pointers[0]
		// We need to set the parent of the borrowed pointer to node since its
		// parent is changing.
		ptr, ok := node.Pointers[node.Numkeys+1].(*BTreeNode)
		if !ok {
			return bptree.TYPE_CONVERSION_ERROR
		}

		ptr.Parent = node
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

		return nil
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

		return nil
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
	return nil
}

func (t *DiskBTree) mergeNodes(node, sibling *DiskBTreeNode, isLeftSibling bool, kPrime []byte) error {
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
			ptr, ok := sibling.Pointers[i].(*BTreeNode)
			if !ok {
				return bptree.TYPE_CONVERSION_ERROR
			}

			ptr.Parent = sibling
			i++
		}
		sibling.Pointers[i] = node.Pointers[j]
		ptr, ok := sibling.Pointers[i].(*BTreeNode)
		if !ok {
			return bptree.TYPE_CONVERSION_ERROR
		}

		ptr.Parent = sibling
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

func (t *DiskBTree) removeFromNode(node *DiskBTreeNode, key []byte, pointer interface{}) error {
	keyIdx := getKeyIndex(node, key)
	if keyIdx < 0 {
		return bptree.INVALID_KEY_INDEX_ERROR
	}

	for i := uint16(keyIdx + 1); i < node.Numkeys; i++ {
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
		return bptree.INVALID_POINTER_INDEX_ERROR
	}

	for i := uint16(pointerIdx + 1); i < numPointers; i++ {
		node.Pointers[i-1] = node.Pointers[i]
	}

	// Reset the removed pointer
	node.Pointers[numPointers-1] = nil
	node.Numkeys--

	if node.IsLeaf && node.Parent != 0 && keyIdx == 0 && node.Numkeys > 0 {
		// If the node still has keys after the deletion, we need to update the parent
		// keys.
		// If the first key of `node` was stored in the parent keys meaning the index
		// of `key` is more than -1, then we need to update it to the key in index
		// 0 of `node` since it has changed.
		nodeParent, err := t.readNode(node.Parent)
		if err != nil {
			return err
		}

		oldKeyIdxInParent := getKeyIndex(nodeParent, key)
		if oldKeyIdxInParent > -1 {
			nodeParent.Keys[oldKeyIdxInParent] = node.Keys[0]
			return t.writeNode(nodeParent.ToBytes(), nodeParent.Ptr)
		}
	}

	return nil
}

func (t *DiskBTree) Print(withPointers bool) error {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return nil
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
					n, ok := node.Pointers[i].(*BTreeNode)
					if !ok {
						return bptree.TYPE_CONVERSION_ERROR
					}

					nodes[i] = n
				}

				queue = append(queue, nodes...)
			}

			if i < levelSize-1 {
				fmt.Print(", ")
			}
		}

		fmt.Println()
	}

	return nil
}

func (t *DiskBTree) PrintLeaves() error {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return nil
	}

	leaf := t.root
	for !leaf.IsLeaf {
		l, ok := leaf.Pointers[0].(*BTreeNode)
		if !ok {
			return bptree.TYPE_CONVERSION_ERROR
		}

		leaf = l
	}

	for leaf != nil {
		fmt.Print(leaf.Keys[:leaf.Numkeys])
		leaf = leaf.Next
	}
	fmt.Println()

	return nil
}

func (t *DiskBTree) PrintLeavesBackwards() error {
	if t.root == nil {
		fmt.Println("Tree is empty")
		return nil
	}

	leaf := t.root
	for !leaf.IsLeaf {
		l, ok := leaf.Pointers[leaf.Numkeys].(*BTreeNode)
		if !ok {
			return bptree.TYPE_CONVERSION_ERROR
		}

		leaf = l
	}

	for leaf != nil {
		fmt.Print(leaf.Keys[:leaf.Numkeys])
		leaf = leaf.Prev
	}
	fmt.Println()

	return nil
}

func makeNode(ptr uint64) *DiskBTreeNode {
	return &DiskBTreeNode{
		Ptr:      ptr,
		Keys:     make([][]byte, m_ORDER-1),
		Numkeys:  0,
		Pointers: make([]interface{}, m_ORDER),
		IsLeaf:   false,
		Parent:   0,
		Next:     0,
		Prev:     0,
	}
}

func makeLeaf(ptr uint64) *DiskBTreeNode {
	node := makeNode(ptr)
	node.IsLeaf = true

	return node
}

func insertIntoNode(node *DiskBTreeNode, key []byte, pointer interface{}) error {
	insertionIndex := getInsertionIndex(node, key)
	nonLeafNodeAdjustment := 0
	if !node.IsLeaf {
		nonLeafNodeAdjustment = 1
	}

	for i := int(node.Numkeys); i > insertionIndex; i-- {
		node.Keys[i] = node.Keys[i-1]
		node.Pointers[i+nonLeafNodeAdjustment] = node.Pointers[i-1+nonLeafNodeAdjustment]
	}

	node.Keys[insertionIndex] = key
	node.Pointers[insertionIndex+nonLeafNodeAdjustment] = pointer
	node.Numkeys++
}

// Gets the index that `key` needs to be inserted into.
// Returns -1 if `node` or `key` is nil.
func getInsertionIndex(node *DiskBTreeNode, key []byte) int {
	insertionIndex := 0
	for insertionIndex < node.Numkeys && bytes.Compare(key, node.Keys[insertionIndex]) >= 0 {
		insertionIndex++
	}

	return insertionIndex
}

// Returns the index of `key`.
// If key is not found, it returns -1
func getKeyIndex(node *DiskBTreeNode, key []byte) int {
	idx := -1
	if key == nil {
		return idx
	}

	for i := 0; i < int(node.Numkeys); i++ {
		if bytes.Compare(key, node.Keys[i]) == 0 {
			idx = i
			break
		}
	}

	return idx
}

// Returns the index of `pointer`.
// If pointer is not found, it returns -1
func getPointerIndex(node *DiskBTreeNode, pointer interface{}) int {
	idx := -1
	if node == nil || pointer == nil {
		return idx
	}

	nonLeafNodeAdjustment := 0
	if !node.IsLeaf {
		nonLeafNodeAdjustment = 1
	}

	for i := 0; i < node.Numkeys+nonLeafNodeAdjustment; i++ {
		// We do this because pointer can either be []byte or uint64. []byte can't be compared using ==
		val, ok := pointer.([]byte)
		if ok {
			if bytes.Compare(node.Pointers[i].([]byte), val) == 0 {
				idx = i
				break
			}
		} else if node.Pointers[i] == pointer {
			idx = i
			break
		}
	}

	return idx
}
