// Package providertest has a scriptable provider for tests.
package providertest

import (
	"context"
	"strings"
	"sync"

	"github.com/erangamapa/infergate/internal/provider"
)

type Fake struct {
	name string

	mu    sync.Mutex
	errs  []error
	calls int
	Text  string
	// FailAfterChunks makes Stream fail after sending that many chunks, zero disables it
	FailAfterChunks int
	StreamErr       error
}

func New(name, text string) *Fake { return &Fake{name: name, Text: text} }

// FailNext queues errors returned by the next calls, in order.
func (f *Fake) FailNext(errs ...error) *Fake {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, errs...)
	return f
}

func (f *Fake) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *Fake) Name() string { return f.name }

func (f *Fake) next() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.errs) == 0 {
		return nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return err
}

func (f *Fake) Complete(ctx context.Context, req provider.Request) (provider.Response, error) {
	if err := f.next(); err != nil {
		return provider.Response{}, err
	}
	if err := ctx.Err(); err != nil {
		return provider.Response{}, err
	}
	return provider.Response{
		ID: f.name + "-1", Text: f.Text, Finish: provider.FinishStop,
		Usage: provider.Usage{InputTokens: countWords(req.Messages), OutputTokens: len(strings.Fields(f.Text))},
	}, nil
}

func (f *Fake) Stream(ctx context.Context, req provider.Request, emit func(provider.Chunk) error) error {
	if err := f.next(); err != nil {
		return err
	}
	words := strings.Fields(f.Text)
	for i, w := range words {
		if f.FailAfterChunks > 0 && i == f.FailAfterChunks {
			return f.StreamErr
		}
		if err := emit(provider.Chunk{Delta: w + " "}); err != nil {
			return err
		}
	}
	return emit(provider.Chunk{Usage: &provider.Usage{InputTokens: countWords(req.Messages), OutputTokens: len(words)}, Finish: provider.FinishStop})
}

func countWords(msgs []provider.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(strings.Fields(m.Content))
	}
	return n
}

// Overloaded is what a 529 from upstream looks like after the client maps it.
func Overloaded(backend string) error {
	return &provider.Error{Backend: backend, Status: 529, Retryable: true, Err: errOverloaded}
}

// BadRequest is a non retryable failure.
func BadRequest(backend string) error {
	return &provider.Error{Backend: backend, Status: 400, Retryable: false, Err: errBadRequest}
}

type sentinel string

func (s sentinel) Error() string { return string(s) }

const (
	errOverloaded sentinel = "overloaded"
	errBadRequest sentinel = "bad request"
)
