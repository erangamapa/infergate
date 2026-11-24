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

const anthropicVersion = "2023-06-01"

// Anthropic talks to the messages API. System messages are lifted out of the
// conversation because the API takes them as a separate field.
type Anthropic struct {
	name    string
	baseURL string
	apiKey  string
	model   string
	http    *http.Client
}

func NewAnthropic(name, baseURL, apiKey, model string, client *http.Client) *Anthropic {
	if client == nil {
		client = http.DefaultClient
	}
	if baseURL == "" {
		baseURL = "https://api.anthropic.com"
	}
	return &Anthropic{name: name, baseURL: strings.TrimRight(baseURL, "/"), apiKey: apiKey, model: model, http: client}
}

func (a *Anthropic) Name() string { return a.name }

type antMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type antRequest struct {
	Model         string       `json:"model"`
	System        string       `json:"system,omitempty"`
	Messages      []antMessage `json:"messages"`
	MaxTokens     int          `json:"max_tokens"`
	Temperature   float32      `json:"temperature"`
	StopSequences []string     `json:"stop_sequences,omitempty"`
	Stream        bool         `json:"stream,omitempty"`
}

type antUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type antResponse struct {
	ID      string `json:"id"`
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StopReason string   `json:"stop_reason"`
	Usage      antUsage `json:"usage"`
}

func (a *Anthropic) body(req Request, stream bool) antRequest {
	var system []string
	msgs := make([]antMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Role == RoleSystem {
			system = append(system, m.Content)
			continue
		}
		msgs = append(msgs, antMessage{Role: string(m.Role), Content: m.Content})
	}
	maxTokens := req.MaxTokens
	if maxTokens == 0 {
		maxTokens = 1024
	}
	return antRequest{
		Model: a.model, System: strings.Join(system, "\n\n"), Messages: msgs,
		MaxTokens: maxTokens, Temperature: req.Temperature, StopSequences: req.Stop, Stream: stream,
	}
}

func (a *Anthropic) do(ctx context.Context, payload antRequest) (*http.Response, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("x-api-key", a.apiKey)
	hreq.Header.Set("anthropic-version", anthropicVersion)
	resp, err := a.http.Do(hreq)
	if err != nil {
		return nil, &Error{Backend: a.name, Retryable: ctx.Err() == nil, Err: err}
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, &Error{Backend: a.name, Status: resp.StatusCode, Retryable: retryableStatus(resp.StatusCode), Err: errors.New(strings.TrimSpace(string(msg)))}
	}
	return resp, nil
}

func stopFrom(reason string) Finish {
	if reason == "max_tokens" {
		return FinishLength
	}
	return FinishStop
}

func (a *Anthropic) Complete(ctx context.Context, req Request) (Response, error) {
	resp, err := a.do(ctx, a.body(req, false))
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	var out antResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Response{}, &Error{Backend: a.name, Status: resp.StatusCode, Retryable: true, Err: fmt.Errorf("decode: %w", err)}
	}
	var text strings.Builder
	for _, c := range out.Content {
		if c.Type == "text" {
			text.WriteString(c.Text)
		}
	}
	return Response{
		ID: out.ID, Text: text.String(), Finish: stopFrom(out.StopReason),
		Usage: Usage{InputTokens: out.Usage.InputTokens, OutputTokens: out.Usage.OutputTokens},
	}, nil
}

type antEvent struct {
	Type    string `json:"type"`
	Message *struct {
		Usage antUsage `json:"usage"`
	} `json:"message"`
	Delta *struct {
		Type       string `json:"type"`
		Text       string `json:"text"`
		StopReason string `json:"stop_reason"`
	} `json:"delta"`
	Usage *antUsage `json:"usage"`
}

func (a *Anthropic) Stream(ctx context.Context, req Request, emit func(Chunk) error) error {
	resp, err := a.do(ctx, a.body(req, true))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	usage := Usage{}
	finish := FinishStop
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var ev antEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			return &Error{Backend: a.name, Status: 200, Err: fmt.Errorf("bad stream event: %w", err)}
		}
		switch ev.Type {
		case "message_start":
			if ev.Message != nil {
				usage.InputTokens = ev.Message.Usage.InputTokens
			}
		case "content_block_delta":
			if ev.Delta != nil && ev.Delta.Type == "text_delta" {
				if err := emit(Chunk{Delta: ev.Delta.Text}); err != nil {
					return err
				}
			}
		case "message_delta":
			if ev.Delta != nil && ev.Delta.StopReason != "" {
				finish = stopFrom(ev.Delta.StopReason)
			}
			if ev.Usage != nil {
				usage.OutputTokens = ev.Usage.OutputTokens
			}
		case "error":
			return &Error{Backend: a.name, Status: 200, Retryable: true, Err: errors.New("upstream stream error")}
		}
	}
	if err := sc.Err(); err != nil {
		return &Error{Backend: a.name, Status: 200, Err: err}
	}
	return emit(Chunk{Usage: &usage, Finish: finish})
}
