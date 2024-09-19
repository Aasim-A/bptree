package memory

import (
	"crypto/rand"
	"fmt"
	"testing"
)

func BenchmarkInsertAscending(b *testing.B) {
	tree := NewTree()

	padding := toString(len(toString(b.N)))
	for n := 0; n < b.N; n++ {
		key, val := getPaddedKey(padding, n), []byte("v"+fmt.Sprint(n))
		err := tree.Insert(key, val)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertDescending(b *testing.B) {
	tree := NewTree()

	padding := toString(len(toString(b.N)))
	for n := b.N; n > 0; n-- {
		key, val := getPaddedKey(padding, n), []byte("v"+fmt.Sprint(n))
		err := tree.Insert(key, val)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertRandom(b *testing.B) {
	bufLength := 16
	randBytes := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		buf := make([]byte, bufLength)
		n, err := rand.Read(buf)
		if err != nil {
			b.Fatal(err)
		}

		if n != bufLength {
			b.Fatalf("Expected %d random bytes written but got %d", bufLength, n)
		}

		randBytes[i] = buf
	}

	b.ResetTimer() // Reset timer to only measure random inserts

	tree := NewTree()
	for n := 0; n < b.N; n++ {
		key := randBytes[n]
		err := tree.Insert(key, append([]byte("v-"), key...))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertFind(b *testing.B) {
	tree := NewTree()

	padding := toString(len(toString(b.N)))
	for n := 0; n < b.N; n++ {
		key, val := getPaddedKey(padding, n), []byte("v"+fmt.Sprint(n))
		err := tree.Insert(key, val)
		if err != nil {
			b.Fatal(err)
		}
	}

	for n := 0; n < b.N; n++ {
		key := getPaddedKey(padding, n)
		_, err := tree.Find(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}
