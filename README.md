# Chain

Chain is a lightweight, type-safe stream processing library for Go, featuring parallel processing capabilities and fluent chaining APIs. It's designed to make data processing in Go elegant and efficient.

## Features

- 🚀 Fluent chaining API
- 💪 Type-safe using Go generics
- ⚡ Parallel processing support
- 🎯 Context-aware operations
- 🔄 Rich set of operations (Map, Filter, Reduce, etc.)
- 📦 Zero external dependencies

## Quick Start

### Installation

```bash
go get github.com/c4pt0r/chain
```

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/c4pt0r/chain"
)

func main() {
    // Create a stream of numbers
    numbers := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
    
    // Process numbers in parallel:
    // 1. Filter even numbers
    // 2. Square them
    result, err := chain.NewStream(numbers).
        Parallel(3).
        Filter(func(n int) bool {
            return n%2 == 0 // keep even numbers
        }).
        Map(func(n int) int {
            return n * n // square them
        }).
        Collect(context.Background())
        
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Result: %v\n", result)
    // Output: Result: [4 16 36 64 100]
}
```

## Core Concepts

### Stream Operations

Chain provides several core operations for data processing:

- `Map[R any](func(T) R) Stream[R]` - Transform elements from one type to another
- `Filter(func(T) bool) Stream[T]` - Filter elements based on a predicate
- `Collect(context.Context) ([]T, error)` - Gather all elements into a slice
- `Parallel(workers int) Stream[T]` - Enable parallel processing
- `Reduce(func(T, T) T) (T, error)` - Reduce stream to a single value

### Parallel Processing

Enable parallel processing by calling `Parallel(n)` where `n` is the number of workers:

```go
result, err := chain.NewStream(data).
    Parallel(3).  // Use 3 workers
    Map(expensiveOperation).
    Collect(ctx)
```

### Context Support

All operations support context for cancellation and timeout:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

result, err := chain.NewStream(data).
    Map(longOperation).
    Collect(ctx)
```

## Advanced Examples

### Processing Custom Types

```go
type User struct {
    ID   int
    Name string
    Age  int
}

users := []User{
    {1, "Alice", 25},
    {2, "Bob", 30},
    {3, "Charlie", 35},
}

type UserDTO struct {
    Name     string
    IsAdult  bool
}

result, err := chain.NewStream(users).
    Parallel(2).
    Filter(func(u User) bool {
        return u.Age >= 18
    }).
    Map(func(u User) UserDTO {
        return UserDTO{
            Name:    u.Name,
            IsAdult: true,
        }
    }).
    Collect(context.Background())
```

### Data Aggregation

```go
numbers := []int{1, 2, 3, 4, 5}

sum, err := chain.NewStream(numbers).
    Reduce(func(a, b int) int {
        return a + b
    })
```

## License

MIT License - see LICENSE file for details
