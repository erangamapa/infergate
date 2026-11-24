package limit

import (
	"errors"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestBurstThenRefill(t *testing.T) {
	c := &clock{t: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)}
	l := NewWithClock(nil, Policy{RequestsPerSecond: 2, Burst: 3}, c.now)
	for i := 0; i < 3; i++ {
		if err := l.Acquire("analyst"); err != nil {
			t.Fatalf("burst request %d rejected: %v", i, err)
		}
	}
	if err := l.Acquire("analyst"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("want rate limited, got %v", err)
	}
	c.t = c.t.Add(500 * time.Millisecond)
	if err := l.Acquire("analyst"); err != nil {
		t.Fatalf("one token should have refilled: %v", err)
	}
}

func TestTenantsAreIsolated(t *testing.T) {
	c := &clock{t: time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)}
	l := NewWithClock(nil, Policy{RequestsPerSecond: 1, Burst: 1}, c.now)
	_ = l.Acquire("a")
	if err := l.Acquire("b"); err != nil {
		t.Fatalf("tenant b should not share a bucket with a: %v", err)
	}
}

func TestDailyBudget(t *testing.T) {
	c := &clock{t: time.Date(2026, 6, 1, 23, 0, 0, 0, time.UTC)}
	l := NewWithClock(map[string]Policy{"vip": {DailyTokens: 1000}}, Policy{}, c.now)
	if err := l.Acquire("vip"); err != nil {
		t.Fatal(err)
	}
	l.Charge("vip", 1200)
	if got := l.Remaining("vip"); got != 0 {
		t.Fatalf("remaining want 0 got %d", got)
	}
	if err := l.Acquire("vip"); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("want budget exhausted, got %v", err)
	}
	c.t = c.t.Add(2 * time.Hour)
	if err := l.Acquire("vip"); err != nil {
		t.Fatalf("budget should reset at utc midnight: %v", err)
	}
}

func TestUncappedRemaining(t *testing.T) {
	l := New(nil, Policy{})
	if l.Remaining("x") != -1 {
		t.Fatal("uncapped tenant should report -1")
	}
}
