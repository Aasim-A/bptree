# bptree
A B+ tree implementation in pure Go supporting both in-memory and disk backed trees.

# Installation

With [Go's module support](https://go.dev/wiki/Modules#how-to-use-modules), `go [build|run|test]` automatically fetches the necessary dependencies when you add the import in your code:

```go
import "github.com/Aasim-A/bptree/[memory|disk]"
```

Alternatively, use `go get`:
```bash
go get -U github.com/Aasim-A/bptree
```

# Example

```go
package main

import (
	"fmt"

	"github.com/Aasim-A/bptree/memory"
)

const COUNT = 16

func main() {
	tree := memory.NewTree()
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
}

/*
  This will print the tree like so:
    [06 12] <-- Top level
    [02 04], [08 10], [14] <-- Second level
    [00 01], [02 03], [04 05], [06 07], [08 09], [10 11], [12 13], [14 15] <-- Third level
*/
```

# API

### Find the value associated with a key
```go
func (t *BTree) Find(key []byte) ([]byte, error)
```

### Update the value of an existing key in the tree
```go
func (t *BTree) Update(key, newValue []byte) error
```

### Insert a new key/value into the tree
```go
func (t *BTree) Insert(key, value []byte) error
```

### Delete an entry from the tree with the given `key`
```go
func (t *BTree) Delete(key []byte) error
```

### Print the tree
```go
func (t *BTree) Print(withPointers bool) error
```

### Print the leaves of the tree
```go
func (t *BTree) PrintLeaves() error
```

### Print the leaves of the tree in reverse, i.e. starting from the end to the beginning
```go
func (t *BTree) PrintLeavesBackwards() error
```
