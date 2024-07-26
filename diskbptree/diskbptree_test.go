package diskbptree

import (
	rand "crypto/rand"
	"errors"
	"fmt"
	mathRand "math/rand"
	"os"
	"reflect"
	"testing"
)

const MULTIPLE_TEST_COUNT = 1000
const RAND_KEY_LEN = 16
const treeFilePath = "./db.db"

func getTree() (*DiskBTree, error) {
	err := os.Truncate(treeFilePath, 0)
	if err != nil {
		return nil, err
	}

	return NewTree(treeFilePath)
}

func TestFindNilRoot(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key := []byte("1")
	res, err := tree.Find(key)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if res != nil {
		t.Fatalf("expected nil but got %v \n", res)
	}
}

func TestFind(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err = tree.Insert(key, val)
	if err != nil {
		t.Fatal(err)
	}

	res, err := tree.Find(key2)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if res != nil {
		t.Fatalf("expected nil but got %v \n", res)
	}
}

func TestInsertNilRoot(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key, val := []byte("1"), []byte("v1")
	err = tree.Insert(key, val)
	if err != nil {
		t.Fatal(err)
	}

	res, err := tree.Find(key)
	if err != nil {
		t.Fatal(err)
	}

	if res == nil {
		t.Fatal("Expected value but got nil")
	}

	if !reflect.DeepEqual(res, val) {
		t.Fatalf("expected %v but got %v \n", val, res)
	}

	rootNode, err := tree.readNode(tree.masterPage.root)
	if err != nil {
		t.Fatal(err)
	}

	if rootNode.Numkeys != 1 {
		t.Fatalf("expected 1 key but got %d", rootNode.Numkeys)
	}
}

func TestInsertVariableKeySize(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key1, key2, val := []byte("1"), []byte("key 2"), []byte("v1")
	err = tree.Insert(key1, val)
	if err != nil {
		t.Fatal(err)
	}

	if len(key1) != tree.keySize {
		t.Fatalf("Expected keySize to be %d but got %d", len(key1), tree.keySize)
	}

	err = tree.Insert(key2, val)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if len(key1) != tree.keySize {
		t.Fatalf("Expected keySize to be %d but got %d", len(key1), tree.keySize)
	}
}

func TestMultipleInsertAscendingKeys(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = ascendingLoop(func(key, val []byte) error {
		res, err := tree.Find(key)
		if err != nil {
			return err
		}

		if res == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, res))
		}

		if !reflect.DeepEqual(res, val) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, res))
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestMultipleInsertDescendingKeys(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = descendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = descendingLoop(func(key, val []byte) error {
		res, err := tree.Find(key)
		if err != nil {
			return err
		}

		if res == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, res))
		}

		if !reflect.DeepEqual(res, val) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, res))
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestMultipleInsertRandomKeys(t *testing.T) {
	randKeys, err := getRandomKeys()
	if err != nil {
		t.Fatal(err)
	}

	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		err := tree.Insert(key, append([]byte("v"), key...))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		val := append([]byte("v"), key...)
		res, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		if res == nil {
			t.Fatal("Expected result but got nil")
		}

		if !reflect.DeepEqual(res, val) {
			t.Fatalf("expected %v but got %v \n", val, res)
		}
	}
}

func TestInsertSameKeyTwice(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key, val := []byte("1"), []byte("v1")
	err = tree.Insert(key, val)
	if err != nil {
		t.Fatal(err)
	}

	err = tree.Insert(key, append(val, []byte("2")...))
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	res, err := tree.Find(key)
	if err != nil {
		t.Fatal(err)
	}

	if res == nil {
		t.Fatalf("expected %v but got %v \n", val, res)
	}

	if !reflect.DeepEqual(res, val) {
		t.Fatalf("expected %v but got %v \n", val, res)
	}

	rootNode, err := tree.readNode(tree.masterPage.root)
	if err != nil {
		t.Fatal(err)
	}

	if rootNode.Numkeys != 1 {
		t.Fatalf("expected 1 key but got %d", rootNode.Numkeys)
	}
}

func TestInsertSameValueTwice(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key1, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err = tree.Insert(key1, val)
	if err != nil {
		t.Fatal(err)
	}

	err = tree.Insert(key2, val)
	if err != nil {
		t.Fatal(err)
	}

	res1, err := tree.Find(key1)
	if err != nil {
		t.Fatal(err)
	}

	if res1 == nil {
		t.Fatalf("expected %v but got %v \n", val, res1)
	}

	if !reflect.DeepEqual(res1, val) {
		t.Fatalf("expected %v but got %v \n", val, res1)
	}

	res2, err := tree.Find(key2)
	if err != nil {
		t.Fatal(err)
	}

	if res2 == nil {
		t.Fatalf("expected %v but got %v \n", val, res2)
	}

	if !reflect.DeepEqual(res2, val) {
		t.Fatalf("expected %v but got %v \n", val, res2)
	}

	rootNode, err := tree.readNode(tree.masterPage.root)
	if err != nil {
		t.Fatal(err)
	}

	if rootNode.Numkeys != 2 {
		t.Fatalf("expected 2 keys but got %d", rootNode.Numkeys)
	}
}

func TestMultipleUpdateAscending(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = ascendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		return tree.Update(key, newVal)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = ascendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		res, err := tree.Find(key)
		if err != nil {
			return err
		}

		if res == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, res))
		}

		if !reflect.DeepEqual(res, newVal) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, res))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultipleUpdateDescending(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = descendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = descendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		return tree.Update(key, newVal)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = descendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		res, err := tree.Find(key)
		if err != nil {
			return err
		}

		if res == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, res))
		}

		if !reflect.DeepEqual(res, newVal) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, res))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultipleUpdateRandomKeys(t *testing.T) {
	randKeys, err := getRandomKeys()
	if err != nil {
		t.Fatal(err)
	}

	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		err := tree.Insert(key, append([]byte("v"), key...))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		err := tree.Update(key, append([]byte("new v"), key...))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		newVal := append([]byte("new v"), key...)
		res, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		if res == nil {
			t.Fatal("Expected result but got nil")
		}

		if !reflect.DeepEqual(res, newVal) {
			t.Fatalf("expected %v but got %v \n", newVal, res)
		}
	}
}

func TestUpdateNotFoundNilRoot(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key, newVal := []byte("1"), []byte("v1")
	err = tree.Update(key, newVal)
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	if tree.masterPage != nil {
		t.Fatalf("expected nil but got %v", tree.masterPage)
	}
}

func TestUpdateNotFound(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	key1, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err = tree.Insert(key1, val)
	if err != nil {
		t.Fatal(err)
	}

	err = tree.Update(key2, val)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}
}

func TestDeleteEmptyRoot(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = tree.Delete([]byte("01"))
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if err != KEY_NOT_FOUND_ERROR {
		t.Fatalf("expected %v but got %v", KEY_NOT_FOUND_ERROR, err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = tree.Insert([]byte("1"), []byte("v1"))
	if err != nil {
		t.Fatalf("expected nil but got %v", err)
	}

	err = tree.Delete([]byte("2"))
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if err != KEY_NOT_FOUND_ERROR {
		t.Fatalf("expected %v but got %v", KEY_NOT_FOUND_ERROR, err)
	}
}

func TestDeleteAscending(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatalf("Expected nil but got %v", err)
	}

	err = ascendingLoop(func(key, val []byte) error {
		_, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		return tree.Delete(key)
	})
	if err != nil {
		t.Fatalf("Expected nil but got %v", err)
	}
}

func TestDeleteDescending(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	err = descendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatalf("Expected nil but got %v", err)
	}

	err = descendingLoop(func(key, val []byte) error {
		return tree.Delete(key)
	})
	if err != nil {
		t.Fatalf("Expected nil but got %v", err)
	}
}

func TestDeleteRandom(t *testing.T) {
	tree, err := getTree()
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	keys := make([][]byte, 0, MULTIPLE_TEST_COUNT)
	err = descendingLoop(func(key, val []byte) error {
		keys = append(keys, key)
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatalf("Expected nil but got %v", err)
	}

	mathRand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, key := range keys {
		err = tree.Delete(key)
		if err != nil {
			t.Fatalf("Expected nil but got %v", err)
		}
	}
}

func toString(i int) string {
	return fmt.Sprint(i)
}

func getPaddedKey(padding string, i int) []byte {
	return []byte(fmt.Sprintf("%0"+padding+"d", i))
}

func getRandomKeys() ([][]byte, error) {
	randBytes := make([][]byte, MULTIPLE_TEST_COUNT)
	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		buf := make([]byte, RAND_KEY_LEN)
		n, err := rand.Read(buf)
		if err != nil {
			return nil, err
		}

		if n != RAND_KEY_LEN {
			return nil, errors.New(fmt.Sprintf("Expected %d random bytes written but got %d", RAND_KEY_LEN, n))
		}

		randBytes[i] = buf
	}

	return randBytes, nil
}

func ascendingLoop(cb func(key, val []byte) error) error {
	padding := toString(len(toString(MULTIPLE_TEST_COUNT)))
	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key, val := getPaddedKey(padding, i), []byte("v"+fmt.Sprint(i))
		err := cb(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func descendingLoop(cb func(key, val []byte) error) error {
	padding := toString(len(toString(MULTIPLE_TEST_COUNT)))
	for i := MULTIPLE_TEST_COUNT; i > 0; i-- {
		key, val := getPaddedKey(padding, i), []byte("v"+fmt.Sprint(i))
		err := cb(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}
