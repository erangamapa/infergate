// Package server implements the Inference grpc service on top of the router.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/erangamapa/infergate/gen/infergate/v1"
	"github.com/erangamapa/infergate/internal/auth"
	"github.com/erangamapa/infergate/internal/dedupe"
	"github.com/erangamapa/infergate/internal/limit"
	"github.com/erangamapa/infergate/internal/provider"
	"github.com/erangamapa/infergate/internal/router"
)

const maxMessages = 200

type Server struct {
	pb.UnimplementedInferenceServer
	router  *router.Router
	limiter *limit.Limiter
	dedupe  *dedupe.Cache[*pb.CompleteResponse]
	log     *slog.Logger
}

func New(r *router.Router, l *limit.Limiter, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{router: r, limiter: l, dedupe: dedupe.New[*pb.CompleteResponse](10*time.Minute, 50_000), log: log}
}

func toRequest(in *pb.CompleteRequest) (provider.Request, error) {
	if len(in.GetMessages()) == 0 {
		return provider.Request{}, status.Error(codes.InvalidArgument, "messages are required")
	}
	if len(in.GetMessages()) > maxMessages {
		return provider.Request{}, status.Errorf(codes.InvalidArgument, "too many messages, max %d", maxMessages)
	}
	if in.GetMaxTokens() < 0 {
		return provider.Request{}, status.Error(codes.InvalidArgument, "max tokens must not be negative")
	}
	msgs := make([]provider.Message, 0, len(in.GetMessages()))
	for i, m := range in.GetMessages() {
		var role provider.Role
		switch m.GetRole() {
		case pb.Role_ROLE_SYSTEM:
			role = provider.RoleSystem
		case pb.Role_ROLE_USER:
			role = provider.RoleUser
		case pb.Role_ROLE_ASSISTANT:
			role = provider.RoleAssistant
		default:
			return provider.Request{}, status.Errorf(codes.InvalidArgument, "message %d has no role", i)
		}
		msgs = append(msgs, provider.Message{Role: role, Content: m.GetContent()})
	}
	return provider.Request{
		Messages: msgs, MaxTokens: int(in.GetMaxTokens()),
		Temperature: in.GetTemperature(), Stop: in.GetStop(),
	}, nil
}

func finishToPB(f provider.Finish) pb.FinishReason {
	if f == provider.FinishLength {
		return pb.FinishReason_FINISH_REASON_LENGTH
	}
	return pb.FinishReason_FINISH_REASON_STOP
}

func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, limit.ErrRateLimited), errors.Is(err, limit.ErrBudgetExhausted):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, router.ErrUnknownModel):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, router.ErrNoBackend):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	var pe *provider.Error
	if errors.As(err, &pe) && pe.Status >= 400 && pe.Status < 500 {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func (s *Server) admit(ctx context.Context, in *pb.CompleteRequest) (string, router.Route, provider.Request, error) {
	tenant := auth.Tenant(ctx)
	req, err := toRequest(in)
	if err != nil {
		return "", router.Route{}, req, err
	}
	rt, err := s.router.Resolve(in.GetModel(), router.Tier(in.GetTier()))
	if err != nil {
		return "", router.Route{}, req, toStatus(err)
	}
	if err := s.limiter.Acquire(tenant); err != nil {
		return "", router.Route{}, req, toStatus(err)
	}
	return tenant, rt, req, nil
}

func (s *Server) Complete(ctx context.Context, in *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	run := func() (*pb.CompleteResponse, error) {
		tenant, rt, req, err := s.admit(ctx, in)
		if err != nil {
			return nil, err
		}
		start := time.Now()
		res, err := s.router.Complete(ctx, rt, req)
		if err != nil {
			s.log.Warn("complete failed", "tenant", tenant, "route", rt.Alias, "attempts", res.Attempts, "err", err)
			return nil, toStatus(err)
		}
		u := res.Response.Usage
		s.limiter.Charge(tenant, u.InputTokens+u.OutputTokens)
		s.log.Info("complete", "tenant", tenant, "route", rt.Alias, "served_by", res.ServedBy,
			"attempts", res.Attempts, "in", u.InputTokens, "out", u.OutputTokens, "ms", time.Since(start).Milliseconds())
		return &pb.CompleteResponse{
			Id: res.Response.ID, Text: res.Response.Text, ServedBy: res.ServedBy,
			Usage:        &pb.Usage{InputTokens: int32(u.InputTokens), OutputTokens: int32(u.OutputTokens)},
			FinishReason: finishToPB(res.Response.Finish), Attempts: int32(res.Attempts),
		}, nil
	}
	if in.GetIdempotencyKey() == "" {
		return run()
	}
	key := fmt.Sprintf("%s/%s", auth.Tenant(ctx), in.GetIdempotencyKey())
	resp, _, err := s.dedupe.Do(ctx, key, run)
	return resp, err
}

func (s *Server) StreamComplete(in *pb.CompleteRequest, stream pb.Inference_StreamCompleteServer) error {
	ctx := stream.Context()
	tenant, rt, req, err := s.admit(ctx, in)
	if err != nil {
		return err
	}
	var used int
	_, err = s.router.Stream(ctx, rt, req, func(c provider.Chunk, servedBy string) error {
		out := &pb.CompleteChunk{Delta: c.Delta, ServedBy: servedBy}
		if c.Usage != nil {
			used = c.Usage.InputTokens + c.Usage.OutputTokens
			out.Usage = &pb.Usage{InputTokens: int32(c.Usage.InputTokens), OutputTokens: int32(c.Usage.OutputTokens)}
			out.FinishReason = finishToPB(c.Finish)
		}
		return stream.Send(out)
	})
	s.limiter.Charge(tenant, used)
	return toStatus(err)
}

func (s *Server) ListModels(context.Context, *pb.ListModelsRequest) (*pb.ListModelsResponse, error) {
	out := &pb.ListModelsResponse{}
	for _, rt := range s.router.Routes() {
		out.Models = append(out.Models, &pb.ModelInfo{Alias: rt.Alias, Tier: pb.Tier(rt.Tier), Backends: rt.Backends})
	}
	return out, nil
}
