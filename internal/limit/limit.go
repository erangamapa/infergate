// Package limit keeps tenants from starving each other.
//
// Two separate controls. A request rate bucket smooths bursts, and a daily
// token budget caps spend. The budget is charged after the response comes
// back because output tokens are only known then, so a tenant can overshoot
// by at most one request.
package limit

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrRateLimited     = errors.New("rate limited")
	ErrBudgetExhausted = errors.New("daily token budget exhausted")
)

type Policy struct {
	RequestsPerSecond float64
	Burst             int
	DailyTokens       int64
}

type tenantState struct {
	tokens float64
	last   time.Time
	day    string
	spent  int64
}

type Limiter struct {
	mu       sync.Mutex
	policies map[string]Policy
	fallback Policy
	state    map[string]*tenantState
	now      func() time.Time
}

func New(policies map[string]Policy, fallback Policy) *Limiter {
	return NewWithClock(policies, fallback, time.Now)
}

func NewWithClock(policies map[string]Policy, fallback Policy, now func() time.Time) *Limiter {
	return &Limiter{policies: policies, fallback: fallback, state: map[string]*tenantState{}, now: now}
}

func (l *Limiter) policy(tenant string) Policy {
	if p, ok := l.policies[tenant]; ok {
		return p
	}
	return l.fallback
}

func (l *Limiter) get(tenant string, p Policy, now time.Time) *tenantState {
	s, ok := l.state[tenant]
	if !ok {
		s = &tenantState{tokens: float64(p.Burst), last: now}
		l.state[tenant] = s
	}
	day := now.UTC().Format("2006-01-02")
	if s.day != day {
		s.day = day
		s.spent = 0
	}
	return s
}

// Acquire takes one request slot for the tenant. It never blocks, callers
// get an error they can turn into RESOURCE_EXHAUSTED straight away.
func (l *Limiter) Acquire(tenant string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	p := l.policy(tenant)
	s := l.get(tenant, p, now)

	if p.DailyTokens > 0 && s.spent >= p.DailyTokens {
		return ErrBudgetExhausted
	}
	if p.RequestsPerSecond > 0 {
		elapsed := now.Sub(s.last).Seconds()
		s.tokens = min(float64(p.Burst), s.tokens+elapsed*p.RequestsPerSecond)
		s.last = now
		if s.tokens < 1 {
			return ErrRateLimited
		}
		s.tokens--
	}
	return nil
}

// Charge records tokens used by a finished request.
func (l *Limiter) Charge(tenant string, tokens int) {
	if tokens <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.get(tenant, l.policy(tenant), l.now())
	s.spent += int64(tokens)
}

// Remaining is the budget left today, or -1 when the tenant is uncapped.
func (l *Limiter) Remaining(tenant string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := l.policy(tenant)
	if p.DailyTokens <= 0 {
		return -1
	}
	s := l.get(tenant, p, l.now())
	return max(0, p.DailyTokens-s.spent)
}
