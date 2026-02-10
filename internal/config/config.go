// Package config loads the gateway yaml and builds the runtime pieces from it.
package config

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/erangamapa/infergate/internal/auth"
	"github.com/erangamapa/infergate/internal/limit"
	"github.com/erangamapa/infergate/internal/provider"
	"github.com/erangamapa/infergate/internal/router"
)

type Backend struct {
	Name    string `yaml:"name"`
	Kind    string `yaml:"kind"`
	BaseURL string `yaml:"base_url"`
	Model   string `yaml:"model"`
	// name of the env var holding the key, never the key itself
	APIKeyEnv string `yaml:"api_key_env"`
}

type Route struct {
	Alias    string   `yaml:"alias"`
	Tier     string   `yaml:"tier"`
	Backends []string `yaml:"backends"`
}

type Tenant struct {
	Name              string  `yaml:"name"`
	KeySHA256         string  `yaml:"key_sha256"`
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
	DailyTokens       int64   `yaml:"daily_tokens"`
}

type Config struct {
	Listen         string        `yaml:"listen"`
	AttemptTimeout time.Duration `yaml:"attempt_timeout"`
	Breaker        struct {
		Threshold int           `yaml:"threshold"`
		Cooldown  time.Duration `yaml:"cooldown"`
	} `yaml:"breaker"`
	Backends []Backend `yaml:"backends"`
	Routes   []Route   `yaml:"routes"`
	Tenants  []Tenant  `yaml:"tenants"`
	Default  Tenant    `yaml:"default_tenant_policy"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

func Parse(raw []byte) (*Config, error) {
	c := &Config{Listen: ":7443", AttemptTimeout: 60 * time.Second}
	c.Breaker.Threshold = 5
	c.Breaker.Cooldown = 30 * time.Second
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return c, c.validate()
}

func (c *Config) validate() error {
	seen := map[string]bool{}
	for _, b := range c.Backends {
		if b.Name == "" || b.Model == "" {
			return fmt.Errorf("config: backend needs name and model")
		}
		if seen[b.Name] {
			return fmt.Errorf("config: duplicate backend %s", b.Name)
		}
		seen[b.Name] = true
		if b.Kind != "anthropic" && b.Kind != "openai" {
			return fmt.Errorf("config: backend %s has unknown kind %q", b.Name, b.Kind)
		}
	}
	for _, r := range c.Routes {
		if _, err := parseTier(r.Tier); err != nil {
			return fmt.Errorf("config: route %s: %w", r.Alias, err)
		}
	}
	for _, t := range c.Tenants {
		if len(t.KeySHA256) != 64 {
			return fmt.Errorf("config: tenant %s key_sha256 must be a sha256 hex digest", t.Name)
		}
	}
	return nil
}

func parseTier(s string) (router.Tier, error) {
	switch s {
	case "", "unspecified":
		return router.TierUnspecified, nil
	case "fast":
		return router.TierFast, nil
	case "standard":
		return router.TierStandard, nil
	case "deep":
		return router.TierDeep, nil
	}
	return 0, fmt.Errorf("unknown tier %q", s)
}

func (c *Config) Providers(env func(string) string, client *http.Client) ([]provider.Provider, error) {
	out := make([]provider.Provider, 0, len(c.Backends))
	for _, b := range c.Backends {
		key := ""
		if b.APIKeyEnv != "" {
			key = env(b.APIKeyEnv)
			if key == "" {
				return nil, fmt.Errorf("backend %s wants %s but it is not set", b.Name, b.APIKeyEnv)
			}
		}
		switch b.Kind {
		case "anthropic":
			out = append(out, provider.NewAnthropic(b.Name, b.BaseURL, key, b.Model, client))
		case "openai":
			out = append(out, provider.NewOpenAI(b.Name, b.BaseURL, key, b.Model, client))
		}
	}
	return out, nil
}

func (c *Config) Router(providers []provider.Provider) (*router.Router, error) {
	routes := make([]router.Route, 0, len(c.Routes))
	for _, r := range c.Routes {
		tier, _ := parseTier(r.Tier)
		routes = append(routes, router.Route{Alias: r.Alias, Tier: tier, Backends: r.Backends})
	}
	return router.New(providers, routes, router.Options{
		BreakerThreshold: c.Breaker.Threshold, BreakerCooldown: c.Breaker.Cooldown, AttemptTimeout: c.AttemptTimeout,
	})
}

func policyOf(t Tenant) limit.Policy {
	return limit.Policy{RequestsPerSecond: t.RequestsPerSecond, Burst: t.Burst, DailyTokens: t.DailyTokens}
}

func (c *Config) Limiter() *limit.Limiter {
	policies := map[string]limit.Policy{}
	for _, t := range c.Tenants {
		policies[t.Name] = policyOf(t)
	}
	return limit.New(policies, policyOf(c.Default))
}

func (c *Config) Keys() auth.Keys {
	keys := auth.Keys{}
	for _, t := range c.Tenants {
		keys[strings.ToLower(t.KeySHA256)] = t.Name
	}
	return keys
}
