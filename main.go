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
	// fmt.Println("-------------------------")
	tree.Print()
	// fmt.Println("-------------------------")
	// val, err := tree.Find([]byte("05"))
	// fmt.Println(err)
	// if err == nil {
	// 	fmt.Println(string(val.Value))
	// }
}
