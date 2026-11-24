package provider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// OpenAI speaks the chat completions protocol. The same client serves OpenAI
// itself and the vLLM pods, which expose a compatible endpoint.
type OpenAI struct {
	name    string
	baseURL string
	apiKey  string
	model   string
	http    *http.Client
}

func NewOpenAI(name, baseURL, apiKey, model string, client *http.Client) *OpenAI {
	if client == nil {
		client = http.DefaultClient
	}
	return &OpenAI{name: name, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model, http: client}
}

func (o *OpenAI) Name() string { return o.name }

type oaiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type oaiRequest struct {
	Model         string       `json:"model"`
	Messages      []oaiMessage `json:"messages"`
	MaxTokens     int          `json:"max_tokens,omitempty"`
	Temperature   float32      `json:"temperature"`
	Stop          []string     `json:"stop,omitempty"`
	Stream        bool         `json:"stream,omitempty"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options,omitempty"`
}

type oaiUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type oaiResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Message      oaiMessage `json:"message"`
		Delta        oaiMessage `json:"delta"`
		FinishReason *string    `json:"finish_reason"`
	} `json:"choices"`
	Usage *oaiUsage `json:"usage"`
}

func (o *OpenAI) body(req Request, stream bool) oaiRequest {
	msgs := make([]oaiMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, oaiMessage{Role: string(m.Role), Content: m.Content})
	}
	b := oaiRequest{Model: o.model, Messages: msgs, MaxTokens: req.MaxTokens, Temperature: req.Temperature, Stop: req.Stop, Stream: stream}
	if stream {
		b.StreamOptions = &struct {
			IncludeUsage bool `json:"include_usage"`
		}{IncludeUsage: true}
	}
	return b
}

func (o *OpenAI) do(ctx context.Context, payload oaiRequest) (*http.Response, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.baseURL+"/v1/chat/completions", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	if o.apiKey != "" {
		hreq.Header.Set("Authorization", "Bearer "+o.apiKey)
	}
	resp, err := o.http.Do(hreq)
	if err != nil {
		return nil, &Error{Backend: o.name, Retryable: ctx.Err() == nil, Err: err}
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, &Error{Backend: o.name, Status: resp.StatusCode, Retryable: retryableStatus(resp.StatusCode), Err: errors.New(strings.TrimSpace(string(msg)))}
	}
	return resp, nil
}

func finishFrom(reason string) Finish {
	if reason == "length" {
		return FinishLength
	}
	return FinishStop
}

func (o *OpenAI) Complete(ctx context.Context, req Request) (Response, error) {
	resp, err := o.do(ctx, o.body(req, false))
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	var out oaiResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Response{}, &Error{Backend: o.name, Status: resp.StatusCode, Retryable: true, Err: fmt.Errorf("decode: %w", err)}
	}
	if len(out.Choices) == 0 {
		return Response{}, &Error{Backend: o.name, Status: resp.StatusCode, Retryable: true, Err: errors.New("no choices")}
	}
	r := Response{ID: out.ID, Text: out.Choices[0].Message.Content, Finish: FinishStop}
	if fr := out.Choices[0].FinishReason; fr != nil {
		r.Finish = finishFrom(*fr)
	}
	if out.Usage != nil {
		r.Usage = Usage{InputTokens: out.Usage.PromptTokens, OutputTokens: out.Usage.CompletionTokens}
	}
	return r, nil
}

func (o *OpenAI) Stream(ctx context.Context, req Request, emit func(Chunk) error) error {
	resp, err := o.do(ctx, o.body(req, true))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	finish := FinishStop
	var usage *Usage
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var ev oaiResponse
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return &Error{Backend: o.name, Status: 200, Err: fmt.Errorf("bad stream event: %w", err)}
		}
		if ev.Usage != nil {
			usage = &Usage{InputTokens: ev.Usage.PromptTokens, OutputTokens: ev.Usage.CompletionTokens}
		}
		for _, c := range ev.Choices {
			if c.FinishReason != nil {
				finish = finishFrom(*c.FinishReason)
			}
			if c.Delta.Content != "" {
				if err := emit(Chunk{Delta: c.Delta.Content}); err != nil {
					return err
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		return &Error{Backend: o.name, Status: 200, Err: err}
	}
	if usage == nil {
		usage = &Usage{}
	}
	return emit(Chunk{Usage: usage, Finish: finish})
}
