package server

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/erangamapa/infergate/gen/infergate/v1"
	"github.com/erangamapa/infergate/internal/auth"
	"github.com/erangamapa/infergate/internal/limit"
	"github.com/erangamapa/infergate/internal/provider"
	"github.com/erangamapa/infergate/internal/provider/providertest"
	"github.com/erangamapa/infergate/internal/router"
)

type harness struct {
	client  pb.InferenceClient
	primary *providertest.Fake
	backup  *providertest.Fake
}

func start(t *testing.T, policy limit.Policy) harness {
	t.Helper()
	primary := providertest.New("qwen-vllm", "home win looks likely")
	backup := providertest.New("haiku", "draw is live")
	r, err := router.New(
		[]provider.Provider{primary, backup},
		[]router.Route{
			{Alias: "router-small", Tier: router.TierFast, Backends: []string{"qwen-vllm", "haiku"}},
		},
		router.Options{BreakerThreshold: 3, BreakerCooldown: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	keys := auth.Keys{auth.Digest("test-key"): "pulse-agents"}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.UnaryInterceptor(keys.Unary()), grpc.StreamInterceptor(keys.Stream()))
	pb.RegisterInferenceServer(gs, New(r, limit.New(nil, policy), nil))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return harness{client: pb.NewInferenceClient(conn), primary: primary, backup: backup}
}

func authed() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer test-key")
}

func userMsg(text string) []*pb.Message {
	return []*pb.Message{{Role: pb.Role_ROLE_USER, Content: text}}
}

func code(err error) codes.Code { return status.Code(err) }

func TestRejectsMissingKey(t *testing.T) {
	h := start(t, limit.Policy{})
	_, err := h.client.Complete(context.Background(), &pb.CompleteRequest{Tier: pb.Tier_TIER_FAST, Messages: userMsg("hi")})
	if code(err) != codes.Unauthenticated {
		t.Fatalf("want unauthenticated, got %v", err)
	}
}

func TestCompleteByTier(t *testing.T) {
	h := start(t, limit.Policy{})
	resp, err := h.client.Complete(authed(), &pb.CompleteRequest{Tier: pb.Tier_TIER_FAST, Messages: userMsg("classify this tweet")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ServedBy != "qwen-vllm" || resp.Text != "home win looks likely" || resp.Usage.OutputTokens != 4 {
		t.Fatalf("unexpected response %+v", resp)
	}
}

func TestFallbackShowsInResponse(t *testing.T) {
	h := start(t, limit.Policy{})
	h.primary.FailNext(providertest.Overloaded("qwen-vllm"))
	resp, err := h.client.Complete(authed(), &pb.CompleteRequest{Model: "router-small", Messages: userMsg("x")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ServedBy != "haiku" || resp.Attempts != 2 {
		t.Fatalf("got %+v", resp)
	}
}

func TestValidation(t *testing.T) {
	h := start(t, limit.Policy{})
	cases := map[string]*pb.CompleteRequest{
		"no messages": {Tier: pb.Tier_TIER_FAST},
		"no role":     {Tier: pb.Tier_TIER_FAST, Messages: []*pb.Message{{Content: "x"}}},
	}
	for name, in := range cases {
		if _, err := h.client.Complete(authed(), in); code(err) != codes.InvalidArgument {
			t.Errorf("%s: want invalid argument, got %v", name, err)
		}
	}
	if _, err := h.client.Complete(authed(), &pb.CompleteRequest{Model: "gpt-9", Messages: userMsg("x")}); code(err) != codes.NotFound {
		t.Errorf("unknown model: want not found, got %v", err)
	}
}

func TestRateLimit(t *testing.T) {
	h := start(t, limit.Policy{RequestsPerSecond: 0.001, Burst: 1})
	in := &pb.CompleteRequest{Tier: pb.Tier_TIER_FAST, Messages: userMsg("x")}
	if _, err := h.client.Complete(authed(), in); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.Complete(authed(), in); code(err) != codes.ResourceExhausted {
		t.Fatalf("want resource exhausted, got %v", err)
	}
}

func TestIdempotentRetryIsFree(t *testing.T) {
	h := start(t, limit.Policy{})
	in := &pb.CompleteRequest{Tier: pb.Tier_TIER_FAST, Messages: userMsg("x"), IdempotencyKey: "step-42"}
	first, err := h.client.Complete(authed(), in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.client.Complete(authed(), in)
	if err != nil {
		t.Fatal(err)
	}
	if h.primary.Calls() != 1 || first.Id != second.Id {
		t.Fatalf("retry should be served from cache, calls %d", h.primary.Calls())
	}
}

func TestStream(t *testing.T) {
	h := start(t, limit.Policy{})
	st, err := h.client.StreamComplete(authed(), &pb.CompleteRequest{Tier: pb.Tier_TIER_FAST, Messages: userMsg("x")})
	if err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	var last *pb.CompleteChunk
	for {
		c, err := st.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(c.Delta)
		last = c
	}
	if strings.TrimSpace(text.String()) != "home win looks likely" {
		t.Fatalf("got %q", text.String())
	}
	if last.Usage == nil || last.FinishReason != pb.FinishReason_FINISH_REASON_STOP {
		t.Fatalf("last chunk should carry usage and finish, got %+v", last)
	}
}

func TestListModels(t *testing.T) {
	h := start(t, limit.Policy{})
	resp, err := h.client.ListModels(authed(), &pb.ListModelsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Models) != 1 || resp.Models[0].Alias != "router-small" || resp.Models[0].Tier != pb.Tier_TIER_FAST {
		t.Fatalf("got %+v", resp.Models)
	}
}
