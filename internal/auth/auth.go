// Package auth maps the api key on a call to a tenant.
//
// Keys are stored as sha256 hex digests so the config file can be committed
// to the deploy repo without leaking anything usable.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ctxKey struct{}

// Keys maps sha256 hex of an api key to a tenant name.
type Keys map[string]string

func Digest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (k Keys) lookup(key string) (string, bool) {
	d := Digest(key)
	for digest, tenant := range k {
		if subtle.ConstantTimeCompare([]byte(digest), []byte(d)) == 1 {
			return tenant, true
		}
	}
	return "", false
}

func Tenant(ctx context.Context) string {
	t, _ := ctx.Value(ctxKey{}).(string)
	return t
}

func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, ctxKey{}, tenant)
}

func (k Keys) authenticate(ctx context.Context) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	var key string
	if v := md.Get("authorization"); len(v) > 0 {
		key = strings.TrimPrefix(v[0], "Bearer ")
	}
	if key == "" {
		return nil, status.Error(codes.Unauthenticated, "missing api key")
	}
	tenant, ok := k.lookup(key)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unknown api key")
	}
	return WithTenant(ctx, tenant), nil
}

func (k Keys) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := k.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

type wrapped struct {
	grpc.ServerStream
	ctx context.Context
}

func (w wrapped) Context() context.Context { return w.ctx }

func (k Keys) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := k.authenticate(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, wrapped{ServerStream: ss, ctx: ctx})
	}
}
