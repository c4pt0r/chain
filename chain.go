// Package chain provides a lightweight stream processing library
package chain

import (
	"context"
	"sync"
	"time"
)

// Stream represents a sequence of elements supporting sequential and parallel operations
type Stream[T any, R any] interface {
	// Map transforms elements of type T to type R
	Map(fn func(T) R) Stream[R, R]

	// Filter returns a stream of elements matching the given predicate
	Filter(fn func(T) bool) Stream[T, R]

	// Reduce reduces the stream to a single value using the given function
	Reduce(fn func(T, T) T) (T, error)

	// ForEach performs an action for each element in the stream
	ForEach(fn func(T)) error

	// Collect gathers all elements into a slice
	Collect(ctx context.Context) ([]T, error)

	// Parallel enables parallel processing with the specified number of workers
	Parallel(workers int) Stream[T, R]
}

// stream implements the Stream interface
type stream[T any, R any] struct {
	source  chan T
	workers int
}

// NewSliceStream creates a new stream from a slice
func NewSliceStream[T any](data []T) Stream[T, T] {
	source := make(chan T, len(data))
	go func() {
		defer close(source)
		for _, item := range data {
			source <- item
		}
	}()
	return &stream[T, T]{source: source, workers: 1}
}

// NewChanStream creates a new stream from a channel
func NewChanStream[T any](ch <-chan T) Stream[T, T] {
	source := make(chan T, 1)
	go func() {
		defer close(source)
		for item := range ch {
			source <- item
		}
	}()
	return &stream[T, T]{source: source, workers: 1}
}

// Map implements Stream.Map
func (s *stream[T, R]) Map(fn func(T) R) Stream[R, R] {
	out := make(chan R, s.workers)

	go func() {
		defer close(out)

		if s.workers == 1 {
			// Sequential processing
			for item := range s.source {
				out <- fn(item)
			}
			return
		}

		// Parallel processing
		var wg sync.WaitGroup
		for i := 0; i < s.workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for item := range s.source {
					out <- fn(item)
				}
			}()
		}
		wg.Wait()
	}()

	return &stream[R, R]{source: out, workers: s.workers}
}

// Filter implements Stream.Filter
func (s *stream[T, R]) Filter(fn func(T) bool) Stream[T, R] {
	out := make(chan T, s.workers)

	go func() {
		defer close(out)

		if s.workers == 1 {
			// Sequential processing
			for item := range s.source {
				if fn(item) {
					out <- item
				}
			}
			return
		}

		// Parallel processing
		var wg sync.WaitGroup
		for i := 0; i < s.workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for item := range s.source {
					if fn(item) {
						out <- item
					}
				}
			}()
		}
		wg.Wait()
	}()

	return &stream[T, R]{source: out, workers: s.workers}
}

// Reduce implements Stream.Reduce
func (s *stream[T, R]) Reduce(fn func(T, T) T) (T, error) {
	var result T
	var first bool = true

	for item := range s.source {
		if first {
			result = item
			first = false
			continue
		}
		result = fn(result, item)
	}

	if first {
		return result, ErrEmptyStream
	}
	return result, nil
}

// ForEach implements Stream.ForEach
func (s *stream[T, R]) ForEach(fn func(T)) error {
	for item := range s.source {
		fn(item)
	}
	return nil
}

// Collect implements Stream.Collect
func (s *stream[T, R]) Collect(ctx context.Context) ([]T, error) {
	var result []T

	for {
		select {
		case item, ok := <-s.source:
			if !ok {
				return result, nil
			}
			result = append(result, item)
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Add a small sleep to allow context cancellation to be detected
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// Parallel implements Stream.Parallel
func (s *stream[T, R]) Parallel(workers int) Stream[T, R] {
	if workers <= 0 {
		workers = 1
	}
	s.workers = workers
	return s
}

// Helper functions

// Generator creates a stream from a generator function
func Generator[T any](gen func() (T, bool)) Stream[T, T] {
	source := make(chan T, 1)
	go func() {
		defer close(source)
		for {
			item, ok := gen()
			if !ok {
				return
			}
			source <- item
		}
	}()
	return &stream[T, T]{source: source, workers: 1}
}

// Errors
var ErrEmptyStream = Error("empty stream")

// Error represents a stream error
type Error string

func (e Error) Error() string { return string(e) }
