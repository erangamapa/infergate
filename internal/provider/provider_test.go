package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var convo = Request{
	Messages: []Message{
		{Role: RoleSystem, Content: "you write short bet briefs"},
		{Role: RoleUser, Content: "arsenal v spurs"},
	},
	MaxTokens: 64,
}

func TestAnthropicLiftsSystemPrompt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-api-key") != "k" || r.Header.Get("anthropic-version") == "" {
			t.Errorf("missing auth headers")
		}
		var body antRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.System != "you write short bet briefs" || len(body.Messages) != 1 || body.Messages[0].Role != "user" {
			t.Errorf("system prompt not lifted, got %+v", body)
		}
		fmt.Fprint(w, `{"id":"msg_1","content":[{"type":"text","text":"tight game"}],"stop_reason":"end_turn","usage":{"input_tokens":12,"output_tokens":2}}`)
	}))
	defer srv.Close()
	a := NewAnthropic("sonnet", srv.URL, "k", "claude-sonnet-4-5", srv.Client())
	resp, err := a.Complete(context.Background(), convo)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "tight game" || resp.Usage.InputTokens != 12 || resp.Finish != FinishStop {
		t.Fatalf("got %+v", resp)
	}
}

func TestAnthropicStream(t *testing.T) {
	events := []string{
		`{"type":"message_start","message":{"usage":{"input_tokens":9,"output_tokens":0}}}`,
		`{"type":"content_block_delta","delta":{"type":"text_delta","text":"tight "}}`,
		`{"type":"content_block_delta","delta":{"type":"text_delta","text":"game"}}`,
		`{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":2}}`,
		`{"type":"message_stop"}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, e := range events {
			fmt.Fprintf(w, "event: x\ndata: %s\n\n", e)
		}
	}))
	defer srv.Close()
	a := NewAnthropic("sonnet", srv.URL, "k", "m", srv.Client())
	var text strings.Builder
	var last Chunk
	err := a.Stream(context.Background(), convo, func(c Chunk) error {
		text.WriteString(c.Delta)
		last = c
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if text.String() != "tight game" || last.Usage.InputTokens != 9 || last.Usage.OutputTokens != 2 || last.Finish != FinishLength {
		t.Fatalf("got %q %+v", text.String(), last)
	}
}

func TestOpenAICompatible(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path %s", r.URL.Path)
		}
		var body oaiRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Model != "qwen2.5-1.5b" || len(body.Messages) != 2 {
			t.Errorf("bad body %+v", body)
		}
		fmt.Fprint(w, `{"id":"c1","choices":[{"message":{"role":"assistant","content":"HOME"},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":1}}`)
	}))
	defer srv.Close()
	o := NewOpenAI("qwen-vllm", srv.URL+"/", "", "qwen2.5-1.5b", srv.Client())
	resp, err := o.Complete(context.Background(), convo)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Text != "HOME" || resp.Usage.InputTokens != 20 {
		t.Fatalf("got %+v", resp)
	}
}

func TestOpenAIStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"HO\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ME\"},\"finish_reason\":\"stop\"}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()
	o := NewOpenAI("qwen-vllm", srv.URL, "", "m", srv.Client())
	var text strings.Builder
	var last Chunk
	if err := o.Stream(context.Background(), convo, func(c Chunk) error { text.WriteString(c.Delta); last = c; return nil }); err != nil {
		t.Fatal(err)
	}
	if text.String() != "HOME" || last.Usage.InputTokens != 5 {
		t.Fatalf("got %q %+v", text.String(), last)
	}
}

func TestStatusMapping(t *testing.T) {
	for status, retry := range map[int]bool{400: false, 401: false, 429: true, 500: true, 503: true, 529: true} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			fmt.Fprint(w, `{"error":"nope"}`)
		}))
		_, err := NewAnthropic("a", srv.URL, "k", "m", srv.Client()).Complete(context.Background(), convo)
		srv.Close()
		var pe *Error
		if !errors.As(err, &pe) || pe.Status != status || IsRetryable(err) != retry {
			t.Errorf("status %d want retry %v, got %v", status, retry, err)
		}
	}
}

func TestCanceledIsNotRetryable(t *testing.T) {
	if IsRetryable(context.Canceled) {
		t.Fatal("canceled should not be retried")
	}
}
