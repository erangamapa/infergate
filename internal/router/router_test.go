package router

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/erangamapa/infergate/internal/breaker"
	"github.com/erangamapa/infergate/internal/provider"
	"github.com/erangamapa/infergate/internal/provider/providertest"
)

func setup(t *testing.T) (*Router, *providertest.Fake, *providertest.Fake) {
	t.Helper()
	primary := providertest.New("sonnet", "lean home side")
	backup := providertest.New("gpt", "lean away side")
	r, err := New(
		[]provider.Provider{primary, backup},
		[]Route{{Alias: "drafter", Tier: TierStandard, Backends: []string{"sonnet", "gpt"}}},
		Options{BreakerThreshold: 2, BreakerCooldown: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	return r, primary, backup
}

var req = provider.Request{Messages: []provider.Message{{Role: provider.RoleUser, Content: "who wins tonight"}}}

func TestResolveByTierAndAlias(t *testing.T) {
	r, _, _ := setup(t)
	if rt, err := r.Resolve("", TierStandard); err != nil || rt.Alias != "drafter" {
		t.Fatalf("tier resolve got %v %v", rt, err)
	}
	if _, err := r.Resolve("nope", TierUnspecified); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("want unknown model, got %v", err)
	}
	if _, err := r.Resolve("", TierDeep); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("tier without a route should fail, got %v", err)
	}
}

func TestFallbackOnOverload(t *testing.T) {
	r, primary, backup := setup(t)
	primary.FailNext(providertest.Overloaded("sonnet"))
	rt, _ := r.Resolve("drafter", TierUnspecified)
	res, err := r.Complete(context.Background(), rt, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.ServedBy != "gpt" || res.Attempts != 2 || backup.Calls() != 1 {
		t.Fatalf("expected fallback to gpt in 2 attempts, got %+v", res)
	}
}

func TestNoFallbackOnBadRequest(t *testing.T) {
	r, primary, backup := setup(t)
	primary.FailNext(providertest.BadRequest("sonnet"))
	rt, _ := r.Resolve("drafter", TierUnspecified)
	if _, err := r.Complete(context.Background(), rt, req); err == nil {
		t.Fatal("bad request should surface")
	}
	if backup.Calls() != 0 {
		t.Fatal("a bad request must not be retried on another backend")
	}
	if s, _ := r.BackendState("sonnet"); s != breaker.Closed {
		t.Fatalf("client errors must not trip the breaker, got %s", s)
	}
}

func TestBreakerSkipsDeadBackend(t *testing.T) {
	r, primary, _ := setup(t)
	primary.FailNext(providertest.Overloaded("sonnet"), providertest.Overloaded("sonnet"))
	rt, _ := r.Resolve("drafter", TierUnspecified)
	for i := 0; i < 2; i++ {
		if _, err := r.Complete(context.Background(), rt, req); err != nil {
			t.Fatal(err)
		}
	}
	if s, _ := r.BackendState("sonnet"); s != breaker.Open {
		t.Fatalf("want open, got %s", s)
	}
	res, err := r.Complete(context.Background(), rt, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Attempts != 1 || primary.Calls() != 2 {
		t.Fatalf("open backend should be skipped, attempts %d calls %d", res.Attempts, primary.Calls())
	}
}

func TestAllBackendsDown(t *testing.T) {
	r, primary, backup := setup(t)
	primary.FailNext(providertest.Overloaded("sonnet"))
	backup.FailNext(providertest.Overloaded("gpt"))
	rt, _ := r.Resolve("drafter", TierUnspecified)
	_, err := r.Complete(context.Background(), rt, req)
	if !errors.Is(err, ErrNoBackend) {
		t.Fatalf("want no backend, got %v", err)
	}
}

func TestStreamFallsBackBeforeFirstChunk(t *testing.T) {
	r, primary, _ := setup(t)
	primary.FailNext(providertest.Overloaded("sonnet"))
	rt, _ := r.Resolve("drafter", TierUnspecified)
	var out strings.Builder
	var served string
	attempts, err := r.Stream(context.Background(), rt, req, func(c provider.Chunk, by string) error {
		out.WriteString(c.Delta)
		served = by
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if served != "gpt" || attempts != 2 || strings.TrimSpace(out.String()) != "lean away side" {
		t.Fatalf("got %q from %s in %d", out.String(), served, attempts)
	}
}

func TestStreamCommitsAfterFirstChunk(t *testing.T) {
	r, primary, backup := setup(t)
	primary.FailAfterChunks = 1
	primary.StreamErr = providertest.Overloaded("sonnet")
	rt, _ := r.Resolve("drafter", TierUnspecified)
	_, err := r.Stream(context.Background(), rt, req, func(provider.Chunk, string) error { return nil })
	if err == nil {
		t.Fatal("mid stream failure should surface")
	}
	if backup.Calls() != 0 {
		t.Fatal("must not switch backend after chunks went out")
	}
}

func TestRejectsUnknownBackend(t *testing.T) {
	_, err := New(nil, []Route{{Alias: "x", Backends: []string{"ghost"}}}, Options{})
	if err == nil {
		t.Fatal("route to unknown backend should fail config")
	}
}
