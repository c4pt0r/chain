package chain

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"

	_ "github.com/glebarez/sqlite"
)

func TestNewSliceStream(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	stream := NewSliceStream(input)

	result, err := stream.Collect(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(result) != len(input) {
		t.Errorf("expected length %d, got %d", len(input), len(result))
	}

	for i, v := range result {
		if v != input[i] {
			t.Errorf("at index %d: expected %d, got %d", i, input[i], v)
		}
	}
}

func TestMap(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	stream := NewSliceStream(input)

	doubled := stream.Map(func(x int) int {
		return x * 2
	})

	result, err := doubled.Collect(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := []int{2, 4, 6, 8, 10}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestFilter(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	stream := NewSliceStream(input)

	evens := stream.Filter(func(x int) bool {
		return x%2 == 0
	})

	result, err := evens.Collect(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := []int{2, 4}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestReduce(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	stream := NewSliceStream(input)

	sum, err := stream.Reduce(func(a, b int) int {
		return a + b
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := 15
	if sum != expected {
		t.Errorf("expected sum %d, got %d", expected, sum)
	}
}

func TestParallel(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	stream := NewSliceStream(input)

	result, err := stream.Parallel(3).Map(func(x int) int {
		return x * 2
	}).Collect(context.Background())

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := []int{2, 4, 6, 8, 10}
	if len(result) != len(expected) {
		t.Errorf("expected length %d, got %d", len(expected), len(result))
	}
}

func TestGenerator(t *testing.T) {
	count := 0
	gen := func() (int, bool) {
		if count >= 3 {
			return 0, false
		}
		count++
		return count, true
	}

	stream := Generator(gen)
	result, err := stream.Collect(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := []int{1, 2, 3}
	for i, v := range result {
		if v != expected[i] {
			t.Errorf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestEmptyStreamReduce(t *testing.T) {
	stream := NewSliceStream([]int{})

	_, err := stream.Reduce(func(a, b int) int {
		return a + b
	})

	if err != ErrEmptyStream {
		t.Errorf("expected ErrEmptyStream, got %v", err)
	}
}

func TestComplexChaining(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	stream := NewSliceStream(input)

	result, err := stream.
		Parallel(3).
		Filter(func(x int) bool {
			return x%2 == 0 // keep even numbers
		}).
		Map(func(x int) int {
			return x * x // square the numbers
		}).
		Filter(func(x int) bool {
			return x > 20 // keep only squares greater than 20
		}).
		Collect(context.Background())

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expected := []int{36, 64, 100}
	if len(result) != len(expected) {
		t.Errorf("expected length %d, got %d", len(expected), len(result))
	}

	// Sort both slices before comparison
	sort.Ints(result)
	sort.Ints(expected)

	// Compare sorted slices directly
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("at index %d: expected %d, got %d", i, v, result[i])
		}
	}
}

func TestSQLiteChain(t *testing.T) {
	// Set up SQLite database
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table and insert sample data
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			age INTEGER,
			score INTEGER
		);
		INSERT INTO users (age, score) VALUES 
			(25, 80),
			(30, 95),
			(22, 70),
			(35, 85),
			(28, 90);
	`)
	if err != nil {
		t.Fatalf("failed to create table and insert data: %v", err)
	}

	// Query data
	rows, err := db.Query("SELECT age, score FROM users")
	if err != nil {
		t.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	// Create a generator function to read from rows
	gen := func() (User, bool) {
		if rows.Next() {
			var user User
			err := rows.Scan(&user.Age, &user.Score)
			if err != nil {
				return User{}, false
			}
			return user, true
		}
		return User{}, false
	}

	// Create stream from SQL data and process it
	stream := Generator(gen)
	result, err := stream.
		Filter(func(u User) bool {
			fmt.Println("Filtering user:", u)
			return u.Age > 25 // Filter users older than 25
		}).
		Map(func(u User) User {
			fmt.Println("Mapping user:", u)
			return u // Keep the User type throughout the chain
		}).
		Collect(context.Background())

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Expected results for users over 25 with high scores
	expected := []User{
		{Age: 30, Score: 95},
		{Age: 35, Score: 85},
		{Age: 28, Score: 90},
	}

	// Sort both slices by Score for comparison
	sort.Slice(result, func(i, j int) bool {
		return result[i].Score < result[j].Score
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].Score < expected[j].Score
	})

	if len(result) != len(expected) {
		t.Errorf("expected length %d, got %d", len(expected), len(result))
	}

	for i, v := range expected {
		if result[i].Score != v.Score || result[i].Age != v.Age {
			t.Errorf("at index %d: expected %+v, got %+v", i, v, result[i])
		}
	}
}

// User represents a row in the users table
type User struct {
	Age   int
	Score int
}
