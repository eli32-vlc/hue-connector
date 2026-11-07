package t3link

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeWire stands in for the environment's client wire: it records
// what Hue sent and answers with a coded success or a refusal.
type fakeWire struct {
	mu       sync.Mutex
	sent     []map[string]any
	tags     []string
	answer   string
	refuse   bool
	closed   bool
	readable chan struct{}
}

func newFakeWire(answer string) *fakeWire {
	f := &fakeWire{answer: answer, readable: make(chan struct{})}
	close(f.readable)
	return f
}

func (f *fakeWire) WriteMessage(_ int, data []byte) error {
	var frame map[string]any
	if err := json.Unmarshal(data, &frame); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, frame)
	if tag, ok := frame["tag"].(string); ok {
		f.tags = append(f.tags, tag)
	}
	return nil
}

func (f *fakeWire) ReadMessage() (int, []byte, error) {
	f.mu.Lock()
	sent := len(f.sent)
	refuse := f.refuse
	answer := f.answer
	f.mu.Unlock()
	if sent == 0 {
		return 0, nil, errors.New("read before write")
	}

	id := f.sent[sent-1]["id"]
	if refuse {
		out, _ := json.Marshal(map[string]any{
			"_tag":      "Exit",
			"requestId": id,
			"exit": map[string]any{
				"_tag": "Failure",
				"cause": []any{
					map[string]any{"_tag": "Fail", "message": "The authenticated token is missing required scope: orchestration:operate."},
				},
			},
		})
		return 1, out, nil
	}
	out, _ := json.Marshal(map[string]any{
		"_tag":      "Exit",
		"requestId": id,
		"exit": map[string]any{
			"_tag":  "Success",
			"value": json.RawMessage(answer),
		},
	})
	return 1, out, nil
}

func (f *fakeWire) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeWire) last() map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sent) == 0 {
		return nil
	}
	return f.sent[len(f.sent)-1]
}

// wireClient builds a Client whose client wire is the fake.
func wireClient(t *testing.T, wire *fakeWire) *Client {
	t.Helper()
	c := NewClient(filepath.Join(t.TempDir(), "t3.json"))
	c.DialWS = func(_ context.Context, _ string, _ http.Header) (wsConn, *http.Response, error) {
		return wire, nil, nil
	}
	return c
}

// A send must reach the wire as a real message.dispatch, not as an
// MCP tools/call: that command is what carries no mode ceiling.
func TestThreadSendDispatchesCommand(t *testing.T) {
	wire := newFakeWire(`{}`)
	c := wireClient(t, wire)

	text, err := c.threadSend(t.Context(), "http://127.0.0.1:1", "tok", map[string]interface{}{
		"threadId": "thread:abc",
		"message":  "hello there",
		"mode":     "queue",
	})
	if err != nil {
		t.Fatalf("threadSend: %v", err)
	}
	if !strings.Contains(text, "thread:abc") {
		t.Fatalf("text = %q", text)
	}

	last := wire.last()
	if last == nil {
		t.Fatal("nothing reached the wire")
	}
	if got := last["_tag"]; got != "Request" {
		t.Fatalf("frame _tag = %v, want Request", got)
	}
	if got := last["tag"]; got != string(rpcDispatchCommand) {
		t.Fatalf("tag = %v, want %s", got, rpcDispatchCommand)
	}
	payload, ok := last["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload = %T", last["payload"])
	}
	if payload["type"] != "message.dispatch" {
		t.Fatalf("command type = %v", payload["type"])
	}
	if payload["threadId"] != "thread:abc" || payload["text"] != "hello there" {
		t.Fatalf("command = %v", payload)
	}
	// `queue` is a dispatch mode, not a delivery intent: the
	// environment accepts only auto/steer/restart as an intent.
	mode, ok := payload["dispatchMode"].(map[string]any)
	if !ok || mode["type"] != "queue_after_active" {
		t.Fatalf("dispatchMode = %v", payload["dispatchMode"])
	}
	// A message id is required for the environment to project the
	// turn; without it the send is accepted and never appears.
	if payload["messageId"] == nil || payload["messageId"] == "" {
		t.Fatalf("messageId missing: %v", payload)
	}
	if payload["commandId"] == nil {
		t.Fatalf("commandId missing: %v", payload)
	}
}

// A refusal is an answer. It must not be retried on the MCP wire,
// where the same write would be attempted again.
func TestRefusalDoesNotFallBackToMCP(t *testing.T) {
	wire := newFakeWire(`{}`)
	wire.refuse = true
	c := wireClient(t, wire)

	_, err := c.threadSend(t.Context(), "http://127.0.0.1:1", "tok", map[string]interface{}{
		"threadId": "thread:abc",
		"message":  "hi",
	})
	if err == nil {
		t.Fatal("a scope refusal was reported as success")
	}
	var rpcErr *rpcError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %T %v, want the environment's own refusal", err, err)
	}
	if !strings.Contains(err.Error(), "orchestration:operate") {
		t.Fatalf("refusal lost its reason: %v", err)
	}
	if fallsBackToMCP(err) {
		t.Fatal("a refusal would be retried on the other wire")
	}
}

// An unreachable wire may use MCP: that is a version or a port, not
// a permission answer.
func TestUnreachableWireFallsBack(t *testing.T) {
	c := NewClient(filepath.Join(t.TempDir(), "t3.json"))
	c.DialWS = func(_ context.Context, _ string, _ http.Header) (wsConn, *http.Response, error) {
		return nil, nil, errors.New("dial tcp: connection refused")
	}
	_, err := c.rpcConn(t.Context(), "http://127.0.0.1:1", "tok")
	if err == nil {
		t.Fatal("dial failure reported as success")
	}
	if !fallsBackToMCP(err) {
		t.Fatalf("an unopened wire should fall back: %v", err)
	}
}

// The wire URL keeps the environment's scheme, so a TLS origin
// does not silently downgrade to plaintext.
func TestWSURLKeepsTLS(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"https://box.example.com", "wss://box.example.com/ws?orchestrationProtocol=2"},
		{"http://127.0.0.1:3773", "ws://127.0.0.1:3773/ws?orchestrationProtocol=2"},
	} {
		if got := wsURL(tc.in); got != tc.want {
			t.Fatalf("wsURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// A read renders as prose, not JSON: the agent prompts describe
// these tools as returning readable text.
func TestThreadReadRendersProse(t *testing.T) {
	wire := newFakeWire(`{
		"thread": {"id":"thread:abc","title":"Fix the build","projectId":"project:1",
		           "runtimeMode":"full-access","interactionMode":"default"},
		"runs": [{"id":"run:1","status":"completed"}],
		"messages": [{"id":"m1","role":"user","content":"make it pass"},
		             {"id":"m2","role":"assistant","content":"done"}]
	}`)
	c := wireClient(t, wire)

	text, err := c.threadRead(t.Context(), "http://127.0.0.1:1", "tok", map[string]interface{}{
		"threadId": "thread:abc",
	})
	if err != nil {
		t.Fatalf("threadRead: %v", err)
	}
	for _, want := range []string{"Fix the build", "full-access", "completed", "make it pass", "done"} {
		if !strings.Contains(text, want) {
			t.Fatalf("prose missing %q:\n%s", want, text)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(text), "{") {
		t.Fatalf("read returned JSON, not prose:\n%s", text)
	}
}

// A malformed-argument complaint is this side's problem, not the
// environment's verdict, so the other wire still gets a turn.
func TestBadArgumentsFallBack(t *testing.T) {
	wire := newFakeWire(`{}`)
	c := wireClient(t, wire)
	_, err := c.threadSend(t.Context(), "http://127.0.0.1:1", "tok", map[string]interface{}{})
	if err == nil {
		t.Fatal("a send without a thread succeeded")
	}
	if !fallsBackToMCP(err) {
		t.Fatalf("an argument complaint should fall back: %v", err)
	}
}
