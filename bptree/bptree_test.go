package bptree

import (
	rand "crypto/rand"
	"errors"
	"fmt"
	mathRand "math/rand"
	"reflect"
	"testing"
)

const MULTIPLE_TEST_COUNT = 1000
const RAND_KEY_LEN = 16

func TestFindNilRoot(t *testing.T) {
	tree := NewTree()
	key := []byte("1")
	rec, err := tree.Find(key)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if rec != nil {
		t.Fatalf("expected nil but got %v \n", rec)
	}
}

func TestFind(t *testing.T) {
	tree := NewTree()
	key, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err := tree.Insert(key, val)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tree.Find(key2)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if rec != nil {
		t.Fatalf("expected nil but got %v \n", rec)
	}
}

func TestInsertNilRoot(t *testing.T) {
	tree := NewTree()
	key, val := []byte("1"), []byte("v1")
	err := tree.Insert(key, val)
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tree.Find(key)
	if err != nil {
		t.Fatal(err)
	}

	if rec == nil {
		t.Fatal("Expected value but got nil")
	}

	if !reflect.DeepEqual(rec.Value, val) {
		t.Fatalf("expected %v but got %v \n", val, rec.Value)
	}

	if tree.root.Numkeys != 1 {
		t.Fatalf("expected 1 key but got %d", tree.root.Numkeys)
	}
}

func TestInsertVariableKeySize(t *testing.T) {
	tree := NewTree()
	key1, key2, val := []byte("1"), []byte("key 2"), []byte("v1")
	err := tree.Insert(key1, val)
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
	tree := NewTree()
	err := ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = ascendingLoop(func(key, val []byte) error {
		rec, err := tree.Find(key)
		if err != nil {
			return err
		}

		if rec == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, rec))
		}

		if !reflect.DeepEqual(rec.Value, val) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, rec.Value))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultipleInsertDescendingKeys(t *testing.T) {
	tree := NewTree()
	err := descendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatal(err)
	}

	err = descendingLoop(func(key, val []byte) error {
		rec, err := tree.Find(key)
		if err != nil {
			return err
		}

		if rec == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, rec))
		}

		if !reflect.DeepEqual(rec.Value, val) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", val, rec.Value))
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

	tree := NewTree()
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
		rec, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		if rec == nil {
			t.Fatal("Expected result but got nil")
		}

		if !reflect.DeepEqual(rec.Value, val) {
			t.Fatalf("expected %v but got %v \n", val, rec.Value)
		}
	}
}

func TestInsertSameKeyTwice(t *testing.T) {
	tree := NewTree()
	key, val := []byte("1"), []byte("v1")
	err := tree.Insert(key, val)
	if err != nil {
		t.Fatal(err)
	}

	err = tree.Insert(key, append(val, []byte("2")...))
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	rec, err := tree.Find(key)
	if err != nil {
		t.Fatal(err)
	}

	if rec == nil {
		t.Fatalf("expected %v but got %v \n", val, rec)
	}

	if !reflect.DeepEqual(rec.Value, val) {
		t.Fatalf("expected %v but got %v \n", val, rec.Value)
	}

	if tree.root.Numkeys != 1 {
		t.Fatalf("expected 1 key but got %d", tree.root.Numkeys)
	}
}

func TestInsertSameValueTwice(t *testing.T) {
	tree := NewTree()
	key1, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err := tree.Insert(key1, val)
	if err != nil {
		t.Fatal(err)
	}

	err = tree.Insert(key2, val)
	if err != nil {
		t.Fatal(err)
	}

	rec1, err := tree.Find(key1)
	if err != nil {
		t.Fatal(err)
	}

	if rec1 == nil {
		t.Fatalf("expected %v but got %v \n", val, rec1)
	}

	if !reflect.DeepEqual(rec1.Value, val) {
		t.Fatalf("expected %v but got %v \n", val, rec1.Value)
	}

	rec2, err := tree.Find(key2)
	if err != nil {
		t.Fatal(err)
	}

	if rec2 == nil {
		t.Fatalf("expected %v but got %v \n", val, rec2)
	}

	if !reflect.DeepEqual(rec2.Value, val) {
		t.Fatalf("expected %v but got %v \n", val, rec2.Value)
	}

	if tree.root.Numkeys != 2 {
		t.Fatalf("expected 2 keys but got %d", tree.root.Numkeys)
	}
}

func TestMultipleUpdateAscending(t *testing.T) {
	tree := NewTree()
	err := ascendingLoop(func(key, val []byte) error {
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
		rec, err := tree.Find(key)
		if err != nil {
			return err
		}

		if rec == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, rec))
		}

		if !reflect.DeepEqual(rec.Value, newVal) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, rec.Value))
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultipleUpdateDescending(t *testing.T) {
	tree := NewTree()
	err := descendingLoop(func(key, val []byte) error {
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
		rec, err := tree.Find(key)
		if err != nil {
			return err
		}

		if rec == nil {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, rec))
		}

		if !reflect.DeepEqual(rec.Value, newVal) {
			return errors.New(fmt.Sprintf("expected %v but got %v \n", newVal, rec.Value))
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

	tree := NewTree()
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
		rec, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		if rec == nil {
			t.Fatal("Expected result but got nil")
		}

		if !reflect.DeepEqual(rec.Value, newVal) {
			t.Fatalf("expected %v but got %v \n", newVal, rec.Value)
		}
	}
}

func TestUpdateNotFoundNilRoot(t *testing.T) {
	tree := NewTree()
	key, newVal := []byte("1"), []byte("v1")
	err := tree.Update(key, newVal)
	if err == nil {
		t.Fatal("expected error but got nil")
	}

	if tree.root != nil {
		t.Fatalf("expected nil but got %v", tree.root)
	}
}

func TestUpdateNotFound(t *testing.T) {
	tree := NewTree()
	key1, key2, val := []byte("1"), []byte("2"), []byte("v1")
	err := tree.Insert(key1, val)
	if err != nil {
		t.Fatal(err)
	}

	err = tree.Update(key2, val)
	if err == nil {
		t.Fatal("Expected error but got nil")
	}
}

func TestDeleteEmptyRoot(t *testing.T) {
	tree := NewTree()
	err := tree.Delete([]byte("01"))
	if err == nil {
		t.Fatal("Expected error but got nil")
	}

	if err != KEY_NOT_FOUND_ERROR {
		t.Fatalf("expected %v but got %v", KEY_NOT_FOUND_ERROR, err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	tree := NewTree()
	err := tree.Insert([]byte("1"), []byte("v1"))
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
	tree := NewTree()
	err := ascendingLoop(func(key, val []byte) error {
		return tree.Insert(key, val)
	})
	if err != nil {
		t.Fatalf("Expected nil but got %v", err)
	}

	err = ascendingLoop(func(key, val []byte) error {
		return tree.Delete(key)
	})
	if err != nil {
		t.Fatalf("Expected nil but got %v", err)
	}
}

func TestDeleteDescending(t *testing.T) {
	tree := NewTree()
	err := descendingLoop(func(key, val []byte) error {
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
	tree := NewTree()
	keys := make([][]byte, 0, MULTIPLE_TEST_COUNT)
	err := descendingLoop(func(key, val []byte) error {
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
