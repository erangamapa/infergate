// Package provider holds the backend clients the gateway can route to.
package provider

import (
	"context"
	"errors"
	"fmt"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

type Message struct {
	Role    Role
	Content string
}

type Request struct {
	Model       string
	Messages    []Message
	MaxTokens   int
	Temperature float32
	Stop        []string
}

type Usage struct {
	InputTokens  int
	OutputTokens int
}

type Finish string

const (
	FinishStop   Finish = "stop"
	FinishLength Finish = "length"
)

type Response struct {
	ID     string
	Text   string
	Usage  Usage
	Finish Finish
}

type Chunk struct {
	Delta  string
	Usage  *Usage
	Finish Finish
}

// Provider is one backend. A provider instance is bound to one upstream model.
type Provider interface {
	Name() string
	Complete(ctx context.Context, req Request) (Response, error)
	Stream(ctx context.Context, req Request, emit func(Chunk) error) error
}

// Error carries enough about an upstream failure for the router to decide
// whether trying the next backend makes sense.
type Error struct {
	Backend   string
	Status    int
	Retryable bool
	Err       error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s returned %d: %v", e.Backend, e.Status, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// IsRetryable is true for rate limits, overload and transport failures.
// A bad request will fail the same way on every backend so we stop early.
func IsRetryable(err error) bool {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Retryable
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func retryableStatus(status int) bool {
	return status == 408 || status == 429 || status == 529 || status >= 500
}
