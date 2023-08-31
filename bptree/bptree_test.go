package bptree

import (
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"
)

const MULTIPLE_TEST_COUNT = 15

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
		t.Fatal("Expected value to be returned. Received nil")
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
	padding := toString(len(toString(MULTIPLE_TEST_COUNT)))
	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key, val := getPaddedKey(padding, i), []byte("v"+fmt.Sprint(i))
		err := tree.Insert(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key, val := getPaddedKey(padding, i), []byte("v"+fmt.Sprint(i))
		rec, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		if rec == nil {
			t.Fatalf("expected %v but got %v \n", string(val), rec)
		}

		if !reflect.DeepEqual(rec.Value, val) {
			t.Fatalf("expected %v but got %v \n", val, rec.Value)
		}
	}
}

func TestMultipleInsertDescendingKeys(t *testing.T) {
	tree := NewTree()
	padding := toString(len(toString(MULTIPLE_TEST_COUNT)))
	for i := MULTIPLE_TEST_COUNT; i > 0; i-- {
		key, val := getPaddedKey(padding, i), []byte("v"+fmt.Sprint(i))
		err := tree.Insert(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := MULTIPLE_TEST_COUNT; i > 0; i-- {
		key, val := getPaddedKey(padding, i), []byte("v"+fmt.Sprint(i))
		rec, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		if rec == nil {
			t.Fatalf("expected %v but got %v \n", string(val), rec)
		}

		if !reflect.DeepEqual(rec.Value, val) {
			t.Fatalf("expected %v but got %v \n", val, rec.Value)
		}
	}
}

func TestMultipleInsertRandomKeys(t *testing.T) {
	bufLength := 1
	randBytes := make([][]byte, MULTIPLE_TEST_COUNT)
	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		buf := make([]byte, bufLength)
		n, err := rand.Read(buf)
		if err != nil {
			t.Fatal(err)
		}

		if n != bufLength {
			t.Fatalf("Expected %d random bytes written but got %d", bufLength, n)
		}

		randBytes[i] = buf
	}

	tree := NewTree()
	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randBytes[i]
		err := tree.Insert(key, append([]byte("v-"), key...))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key := randBytes[i]
		val := append([]byte("v-"), key...)
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

func TestMultipleUpdate(t *testing.T) {
	tree := NewTree()
	padding := toString(len(toString(MULTIPLE_TEST_COUNT)))
	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key, val := getPaddedKey(padding, i), []byte("v"+fmt.Sprint(i))
		err := tree.Insert(key, val)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key, newVal := getPaddedKey(padding, i), []byte("new v"+fmt.Sprint(i))
		err := tree.Update(key, newVal)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < MULTIPLE_TEST_COUNT; i++ {
		key, newVal := getPaddedKey(padding, i), []byte("new v"+fmt.Sprint(i))
		rec, err := tree.Find(key)
		if err != nil {
			t.Fatal(err)
		}

		if rec == nil {
			t.Fatalf("expected %v but got %v \n", newVal, rec)
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

func toString(i int) string {
	return fmt.Sprint(i)
}

func getPaddedKey(padding string, i int) []byte {
	return []byte(fmt.Sprintf("%0"+padding+"d", i))
}
