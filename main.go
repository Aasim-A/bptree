package main

import (
	"bptree/bptree"
	"fmt"
)

func main() {
	tree := bptree.BTree{}
	for i := 0; i < 16; i++ {
		err := tree.Insert([]byte(fmt.Sprintf("%02d", i)), []byte("v"+fmt.Sprint(i)))
		if err != nil {
			fmt.Println(i, err)
			break
		}
	}
	tree.Print(false)
	fmt.Println()
	fmt.Println()
	fmt.Println()
	err := tree.Delete([]byte("08"))
	if err != nil {
		panic(err)
	}
	tree.Print(false)
	fmt.Println()
	err = tree.Delete([]byte("09"))
	if err != nil {
		panic(err)
	}
	tree.Print(false)
	fmt.Println()
	err = tree.Delete([]byte("07"))
	if err != nil {
		panic(err)
	}
	tree.Print(false)
}
