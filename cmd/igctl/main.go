// igctl is a tiny client for poking a running gateway from a terminal.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/erangamapa/infergate/gen/infergate/v1"
)

func main() {
	addr := flag.String("addr", "localhost:7443", "gateway address")
	model := flag.String("model", "", "route alias, empty means pick by tier")
	tier := flag.String("tier", "standard", "fast, standard or deep")
	stream := flag.Bool("stream", false, "stream the answer")
	list := flag.Bool("list", false, "list routes and exit")
	flag.Parse()

	key := os.Getenv("INFERGATE_KEY")
	if key == "" {
		fail("INFERGATE_KEY is not set")
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail(err.Error())
	}
	defer conn.Close()
	client := pb.NewInferenceClient(conn)
	ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+key), 2*time.Minute)
	defer cancel()

	if *list {
		resp, err := client.ListModels(ctx, &pb.ListModelsRequest{})
		if err != nil {
			fail(err.Error())
		}
		for _, m := range resp.Models {
			fmt.Printf("%-20s %-14s %s\n", m.Alias, m.Tier, strings.Join(m.Backends, " > "))
		}
		return
	}

	prompt := strings.Join(flag.Args(), " ")
	if prompt == "" {
		fail("usage: igctl [flags] prompt")
	}
	req := &pb.CompleteRequest{
		Model: *model, Tier: tierOf(*tier), MaxTokens: 512,
		Messages: []*pb.Message{{Role: pb.Role_ROLE_USER, Content: prompt}},
	}
	if !*stream {
		resp, err := client.Complete(ctx, req)
		if err != nil {
			fail(err.Error())
		}
		fmt.Println(resp.Text)
		fmt.Fprintf(os.Stderr, "served by %s, %d attempts, %d in %d out\n", resp.ServedBy, resp.Attempts, resp.Usage.InputTokens, resp.Usage.OutputTokens)
		return
	}
	st, err := client.StreamComplete(ctx, req)
	if err != nil {
		fail(err.Error())
	}
	for {
		c, err := st.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fail(err.Error())
		}
		fmt.Print(c.Delta)
		if c.Usage != nil {
			fmt.Fprintf(os.Stderr, "\nserved by %s, %d in %d out\n", c.ServedBy, c.Usage.InputTokens, c.Usage.OutputTokens)
		}
	}
}

func tierOf(s string) pb.Tier {
	switch s {
	case "fast":
		return pb.Tier_TIER_FAST
	case "deep":
		return pb.Tier_TIER_DEEP
	default:
		return pb.Tier_TIER_STANDARD
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
