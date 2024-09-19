package disk

import (
	rand "crypto/rand"
	"errors"
	"fmt"
	mathRand "math/rand"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

const MULTIPLE_TEST_COUNT = 50
const RAND_KEY_LEN = 16

func getTree() (*DiskBTree, error) {
	memFS := afero.NewMemMapFs()
	f, err := memFS.Create("memfile")
	if err != nil {
		return nil, err
	}

	return newTreeFromFile(f)
}

func TestFindNilRoot(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key := []byte("1")
	res, err := tree.Find(key)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestFind(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err = tree.Insert(key, val)
	assert.Nil(t, err)

	res, err := tree.Find(key2)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func TestInsertNilRoot(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key, val := []byte("1"), []byte("v1")
	err = tree.Insert(key, val)
	assert.Nil(t, err)

	res, err := tree.Find(key)
	assert.Nil(t, err)
	assert.Equal(t, res, val)

	rootNode, err := tree.readNode(tree.masterPage.root)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, rootNode.Numkeys)
}

func TestInsertVariableKeySize(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key1, key2, val := []byte("1"), []byte("key 2"), []byte("v1")
	err = tree.Insert(key1, val)
	assert.Nil(t, err)
	assert.Equal(t, len(key1), tree.keySize)

	err = tree.Insert(key2, val)
	assert.NotNil(t, err)
	assert.Equal(t, len(key1), tree.keySize)
}

func TestMultipleInsertAscendingKeys(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	assert.Nil(t, err)

	err = ascendingLoop(func(key, val []byte) error {
		res, err := tree.Find(key)
		assert.Nil(t, err)
		assert.Equal(t, val, res)
		assert.Equal(t, res, val)

		return nil
	})

	assert.Nil(t, err)
}

func TestMultipleInsertDescendingKeys(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = descendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	assert.Nil(t, err)

	err = descendingLoop(func(key, val []byte) error {
		res, err := tree.Find(key)
		assert.Nil(t, err)
		assert.Equal(t, val, res)

		return nil
	})
	assert.Nil(t, err)
}

func TestMultipleInsertRandomKeys(t *testing.T) {
	randKeys, err := getRandomKeys()
	assert.Nil(t, err)

	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		err := tree.Insert(key, append([]byte("v"), key...))
		assert.Nil(t, err)
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		val := append([]byte("v"), key...)
		res, err := tree.Find(key)
		assert.Nil(t, err)
		assert.Equal(t, val, res)
	}
}

func TestInsertSameKeyTwice(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key, val := []byte("1"), []byte("v1")
	err = tree.Insert(key, val)
	assert.Nil(t, err)

	err = tree.Insert(key, append(val, []byte("2")...))
	assert.NotNil(t, err)

	res, err := tree.Find(key)
	assert.Nil(t, err)
	assert.Equal(t, val, res)

	rootNode, err := tree.readNode(tree.masterPage.root)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, rootNode.Numkeys)
}

func TestInsertSameValueTwice(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key1, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err = tree.Insert(key1, val)
	assert.Nil(t, err)

	err = tree.Insert(key2, val)
	assert.Nil(t, err)

	res1, err := tree.Find(key1)
	assert.Nil(t, err)
	assert.Equal(t, val, res1)

	res2, err := tree.Find(key2)
	assert.Nil(t, err)
	assert.Equal(t, val, res2)

	rootNode, err := tree.readNode(tree.masterPage.root)
	assert.Nil(t, err)
	assert.EqualValues(t, 2, rootNode.Numkeys)
}

func TestMultipleUpdateAscending(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	assert.Nil(t, err)

	err = ascendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		return tree.Update(key, newVal)
	})
	assert.Nil(t, err)

	err = ascendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		res, err := tree.Find(key)
		assert.Nil(t, err)
		assert.Equal(t, newVal, res)

		return nil
	})
	assert.Nil(t, err)
}

func TestMultipleUpdateDescending(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = descendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	assert.Nil(t, err)

	err = descendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		return tree.Update(key, newVal)
	})
	assert.Nil(t, err)

	err = descendingLoop(func(key, val []byte) error {
		newVal := append([]byte("new v"), val...)
		res, err := tree.Find(key)
		assert.Nil(t, err)
		assert.Equal(t, newVal, res)

		return nil
	})
	assert.Nil(t, err)
}

func TestMultipleUpdateRandomKeys(t *testing.T) {
	randKeys, err := getRandomKeys()
	assert.Nil(t, err)

	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		err := tree.Insert(key, append([]byte("v"), key...))
		assert.Nil(t, err)
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		err := tree.Update(key, append([]byte("new v"), key...))
		assert.Nil(t, err)
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randKeys[i]
		newVal := append([]byte("new v"), key...)
		res, err := tree.Find(key)
		assert.Nil(t, err)
		assert.Equal(t, newVal, res)
	}
}

func TestUpdateNotFoundNilRoot(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key, newVal := []byte("1"), []byte("v1")
	err = tree.Update(key, newVal)
	assert.NotNil(t, err)
	assert.Nil(t, tree.masterPage)
}

func TestUpdateNotFound(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	key1, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err = tree.Insert(key1, val)
	assert.Nil(t, err)

	err = tree.Update(key2, val)
	assert.NotNil(t, err)
}

func TestDeleteEmptyRoot(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = tree.Delete([]byte("01"))
	assert.NotNil(t, err)
	assert.Equal(t, KEY_NOT_FOUND_ERROR, err)
}

func TestDeleteNotFound(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = tree.Insert([]byte("1"), []byte("v1"))
	assert.Nil(t, err)

	err = tree.Delete([]byte("2"))
	assert.NotNil(t, err)
	assert.Equal(t, KEY_NOT_FOUND_ERROR, err)
}

func TestDeleteAscending(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	assert.Nil(t, err)

	err = ascendingLoop(func(key, val []byte) error {
		_, err := tree.Find(key)
		assert.Nil(t, err)

		return tree.Delete(key)
	})
	assert.Nil(t, err)
}

func TestDeleteDescending(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	err = descendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	assert.Nil(t, err)

	err = descendingLoop(func(key, val []byte) error {
		return tree.Delete(key)
	})
	assert.Nil(t, err)
}

func TestDeleteRandom(t *testing.T) {
	tree, err := getTree()
	assert.Nil(t, err)
	defer tree.Close()

	keys := make([][]byte, 0, MULTIPLE_TEST_COUNT)
	err = descendingLoop(func(key, val []byte) error {
		keys = append(keys, key)
		return tree.Insert(key, val)
	})
	assert.Nil(t, err)

	mathRand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, key := range keys {
		err = tree.Delete(key)
		assert.Nil(t, err)
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
