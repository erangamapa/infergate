package breaker

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestTripsAfterThreshold(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := NewWithClock(3, time.Minute, c.now)
	for i := 0; i < 2; i++ {
		b.Failure()
	}
	if b.State() != Closed || !b.Allow() {
		t.Fatalf("should still be closed after two failures")
	}
	b.Failure()
	if b.State() != Open {
		t.Fatalf("want open, got %s", b.State())
	}
	if b.Allow() {
		t.Fatalf("open breaker let a request through")
	}
}

func TestSuccessResetsCount(t *testing.T) {
	b := New(2, time.Minute)
	b.Failure()
	b.Success()
	b.Failure()
	if b.State() != Closed {
		t.Fatalf("non consecutive failures must not trip")
	}
}

func TestHalfOpenSingleProbe(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := NewWithClock(1, 30*time.Second, c.now)
	b.Failure()
	c.advance(31 * time.Second)
	if !b.Allow() {
		t.Fatalf("probe should be allowed after cooldown")
	}
	if b.Allow() {
		t.Fatalf("only one probe at a time")
	}
	b.Success()
	if b.State() != Closed {
		t.Fatalf("good probe should close, got %s", b.State())
	}
}

func TestFailedProbeReopens(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := NewWithClock(5, 10*time.Second, c.now)
	for i := 0; i < 5; i++ {
		b.Failure()
	}
	c.advance(11 * time.Second)
	b.Allow()
	b.Failure()
	if b.State() != Open {
		t.Fatalf("failed probe should reopen, got %s", b.State())
	}
	c.advance(5 * time.Second)
	if b.Allow() {
		t.Fatalf("cooldown restarts from the failed probe")
	}
}
