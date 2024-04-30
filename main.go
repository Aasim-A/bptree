package main

import (
	"bptree/bptree"
	"fmt"
)

const COUNT = 16

func main() {
	// tree := bptree.BTree{}
	// arr := make([][]byte, 0, COUNT)
	// for i := 0; i < COUNT; i++ {
	// 	key := []byte(fmt.Sprintf("%02d", i))
	// 	arr = append(arr, key)
	// 	err := tree.Insert(key, []byte("v"+fmt.Sprint(i)))
	// 	if err != nil {
	// 		fmt.Println(i, err)
	// 		break
	// 	}
	// }
	//
	// tree.Print(false)
	// fmt.Println()
	// for i := 0; i < COUNT; i++ {
	// 	key := arr[i]
	// 	fmt.Println(i, string(key))
	// 	if err := tree.Delete(key); err != nil {
	// 		panic(err)
	// 	}
	// 	tree.Print(false)
	// 	fmt.Println()
	// }

	node := bptree.BTreeNode2{
		IsLeaf:  true,
		Numkeys: 3,
		Parent:  55,
		Next:    22,
		Prev:    11,
		Keysize: 4,
		Keys: [][]byte{
			{'a', 'b', 'c', '1'},
			{'a', 'b', 'c', '2'},
			{'a', 'b', 'c', '3'},
		},
		Pointers: []interface{}{
			[]byte{'v', 'a', 'l', '1'},
			[]byte{'v', 'a', 'l', 'u', 'e', '2'},
			[]byte{'v', '3'},
		},
	}

	node2 := *bptree.BytesToNode(node.ToBytes())

	fmt.Printf("%+v\n", node)
	fmt.Printf("%+v\n", node2)
}
