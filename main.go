package main

import (
	"bptree/bptree"
	"fmt"
)

const COUNT = 16

func main() {
	tree := bptree.BTree{}
	arr := make([][]byte, 0, COUNT)
	for i := 0; i < COUNT; i++ {
		key := []byte(fmt.Sprintf("%02d", i))
		arr = append(arr, key)
		err := tree.Insert(key, []byte("v"+fmt.Sprint(i)))
		if err != nil {
			fmt.Println(i, err)
			break
		}
	}

	tree.Print(false)
	fmt.Println()
	for i := 0; i < COUNT; i++ {
		key := arr[i]
		fmt.Println(i, string(key))
		if err := tree.Delete(key); err != nil {
			panic(err)
		}
		tree.Print(false)
		fmt.Println()
	}
}
