package main

import "fmt"

func main() {
	// tree := bptree.BTree{}
	// for i := 0; i < 16; i++ {
	// 	err := tree.Insert([]byte(fmt.Sprintf("%02d", i)), []byte("v"+fmt.Sprint(i)))
	// 	if err != nil {
	// 		fmt.Println(i, err)
	// 		break
	// 	}
	// }
	t1 := make([]int, 4)
	t2 := make([]int, 6)
	for i := 0; i < 4; i++ {
		t1[i] = i + 1
	}
	fmt.Println(t1)
	fmt.Println(t2)
	fmt.Println(len(t2), cap(t2))
}
