// Package router picks a backend for a request and falls back down the chain
// when a backend is unhealthy or overloaded.
package router

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/erangamapa/infergate/internal/breaker"
	"github.com/erangamapa/infergate/internal/provider"
)

var (
	ErrUnknownModel = errors.New("unknown model")
	ErrNoBackend    = errors.New("no healthy backend")
)

type Tier int

const (
	TierUnspecified Tier = iota
	TierFast
	TierStandard
	TierDeep
)

type Route struct {
	Alias    string
	Tier     Tier
	Backends []string
}

type backend struct {
	p  provider.Provider
	cb *breaker.Breaker
}

type Router struct {
	routes         map[string]Route
	tierDefault    map[Tier]string
	backends       map[string]*backend
	attemptTimeout time.Duration
}

type Options struct {
	BreakerThreshold int
	BreakerCooldown  time.Duration
	AttemptTimeout   time.Duration
}

func New(providers []provider.Provider, routes []Route, opts Options) (*Router, error) {
	r := &Router{
		routes:         map[string]Route{},
		tierDefault:    map[Tier]string{},
		backends:       map[string]*backend{},
		attemptTimeout: opts.AttemptTimeout,
	}
	for _, p := range providers {
		r.backends[p.Name()] = &backend{p: p, cb: breaker.New(opts.BreakerThreshold, opts.BreakerCooldown)}
	}
	for _, rt := range routes {
		if len(rt.Backends) == 0 {
			return nil, fmt.Errorf("route %s has no backends", rt.Alias)
		}
		for _, b := range rt.Backends {
			if _, ok := r.backends[b]; !ok {
				return nil, fmt.Errorf("route %s points at unknown backend %s", rt.Alias, b)
			}
		}
		r.routes[rt.Alias] = rt
		// first route declared for a tier is the tier default
		if _, taken := r.tierDefault[rt.Tier]; !taken && rt.Tier != TierUnspecified {
			r.tierDefault[rt.Tier] = rt.Alias
		}
	}
	return r, nil
}

func (r *Router) Resolve(model string, tier Tier) (Route, error) {
	if model != "" {
		rt, ok := r.routes[model]
		if !ok {
			return Route{}, fmt.Errorf("%w %q", ErrUnknownModel, model)
		}
		return rt, nil
	}
	alias, ok := r.tierDefault[tier]
	if !ok {
		return Route{}, fmt.Errorf("%w for tier %d", ErrUnknownModel, tier)
	}
	return r.routes[alias], nil
}

func (r *Router) Routes() []Route {
	out := make([]Route, 0, len(r.routes))
	for _, rt := range r.routes {
		out = append(out, rt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Alias < out[j].Alias })
	return out
}

type Result struct {
	Response provider.Response
	ServedBy string
	Attempts int
}

func (r *Router) attemptCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.attemptTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, r.attemptTimeout)
}

// outcome feeds the breaker. Client side errors say nothing about backend
// health so they do not count against it.
func outcome(cb *breaker.Breaker, err error) {
	if err == nil {
		cb.Success()
		return
	}
	if provider.IsRetryable(err) {
		cb.Failure()
		return
	}
	cb.Success()
}

func (r *Router) Complete(ctx context.Context, rt Route, req provider.Request) (Result, error) {
	var lastErr error
	attempts := 0
	for _, name := range rt.Backends {
		b := r.backends[name]
		if !b.cb.Allow() {
			continue
		}
		attempts++
		actx, cancel := r.attemptCtx(ctx)
		resp, err := b.p.Complete(actx, req)
		cancel()
		if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			err = &provider.Error{Backend: name, Retryable: true, Err: err}
		}
		outcome(b.cb, err)
		if err == nil {
			return Result{Response: resp, ServedBy: name, Attempts: attempts}, nil
		}
		lastErr = err
		if ctx.Err() != nil || !provider.IsRetryable(err) {
			return Result{Attempts: attempts}, err
		}
	}
	if lastErr == nil {
		lastErr = ErrNoBackend
	}
	return Result{Attempts: attempts}, fmt.Errorf("%w: %v", ErrNoBackend, lastErr)
}

// Stream only falls back while nothing has been sent to the caller. Once a
// chunk is out the stream is committed to that backend, replaying from a
// different model would hand the client two half answers glued together.
func (r *Router) Stream(ctx context.Context, rt Route, req provider.Request, emit func(provider.Chunk, string) error) (int, error) {
	var lastErr error
	attempts := 0
	for _, name := range rt.Backends {
		b := r.backends[name]
		if !b.cb.Allow() {
			continue
		}
		attempts++
		sent := false
		err := b.p.Stream(ctx, req, func(c provider.Chunk) error {
			sent = true
			return emit(c, name)
		})
		outcome(b.cb, err)
		if err == nil {
			return attempts, nil
		}
		lastErr = err
		if sent || ctx.Err() != nil || !provider.IsRetryable(err) {
			return attempts, err
		}
	}
	if lastErr == nil {
		lastErr = ErrNoBackend
	}
	return attempts, fmt.Errorf("%w: %v", ErrNoBackend, lastErr)
}

func (r *Router) BackendState(name string) (breaker.State, bool) {
	b, ok := r.backends[name]
	if !ok {
		return breaker.Closed, false
	}
	return b.cb.State(), true
}
