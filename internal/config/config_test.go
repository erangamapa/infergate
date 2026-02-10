package config

import (
	"os"
	"strings"
	"testing"
)

func TestExampleConfigLoads(t *testing.T) {
	c, err := Load("../../config/gateway.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	env := func(string) string { return "dummy" }
	ps, err := c.Providers(env, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.Router(ps)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Routes()) != len(c.Routes) {
		t.Fatalf("routes lost in translation")
	}
	if len(c.Keys()) != len(c.Tenants) {
		t.Fatalf("keys lost in translation")
	}
}

func TestRejectsUnknownField(t *testing.T) {
	_, err := Parse([]byte("listen: \":1\"\nbreakr:\n  threshold: 2\n"))
	if err == nil || !strings.Contains(err.Error(), "breakr") {
		t.Fatalf("typo in a key should fail loudly, got %v", err)
	}
}

func TestMissingKeyEnv(t *testing.T) {
	c, err := Parse([]byte("backends:\n  - {name: s, kind: anthropic, model: m, api_key_env: NOPE_NOT_SET}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Providers(os.Getenv, nil); err == nil {
		t.Fatal("missing key env should fail at startup, not at first request")
	}
}

func TestBadTier(t *testing.T) {
	_, err := Parse([]byte("routes:\n  - {alias: x, tier: huge, backends: [a]}\n"))
	if err == nil {
		t.Fatal("unknown tier should fail")
	}
}
