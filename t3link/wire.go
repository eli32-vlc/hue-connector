// Package t3link connects Hue to a T3 Code environment running
// on the user's own computer, through the connector.
//
// The two halves live side by side in this package:
//
//   - Client runs on the paired machine (inside hue-local). It
//     finds the local T3 Code server, mints a local session with
//     the t3 CLI (no OAuth, no public address), and speaks the
//     environment's MCP wire. The token never leaves the machine.
//
//   - Hub runs in the Hue server. It remembers each user's grant
//     and forwards {tool, args} operations over the connector
//     socket; results come back as text. The dashboard connects
//     and shows permissions; the agent does the rest.
package t3link

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// The environment's MCP wire: the protocol version its transport
// requires and the header every follow-up request must carry
// (without it, /mcp answers 400).
const (
	protocolVersion = "2025-06-18"
	mcpPath         = "/mcp"
	// callOutputCap bounds what one call may hand back to the
	// model, the way the connector bounds its other operations.
	callOutputCap = 128 * 1024
)

// mcpSession is one initialized MCP session for one Client. The
// id is minted by the server's initialize response and reused
// until the server forgets it (404) or the origin changes.
type mcpSession struct {
	mu     sync.Mutex
	origin string
	id     string
	inited bool
	nextID int
}

func (s *mcpSession) begin(origin string) (reuse bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inited && s.origin == origin {
		return true
	}
	s.origin = origin
	s.id = ""
	s.inited = false
	return false
}

func (s *mcpSession) next() (string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	return s.id, s.nextID
}

// snapshot reports the handshake state without disturbing the
// id sequence: callers only check whether this origin is live.
func (s *mcpSession) snapshot() (origin, id string, inited bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.origin, s.id, s.inited
}

// markReady records a completed handshake under its lock order:
// callers hold no lock when the HTTP round trip finishes.
func (s *mcpSession) markReady(origin, id string) {
	s.mu.Lock()
	s.origin = origin
	s.id = id
	s.inited = true
	s.mu.Unlock()
}

func (s *mcpSession) drop() {
	s.mu.Lock()
	s.inited = false
	s.id = ""
	s.mu.Unlock()
}

// mcpHTTP speaks the wire with no overall deadline: a wait tool
// legitimately holds its response open for minutes, and
// cancellation comes from the operation's context instead.
var mcpHTTP = &http.Client{
	Transport: &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
	},
}

type rpcEnvelope struct {
	ID     any             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type mcpToolResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError           bool            `json:"isError"`
	StructuredContent json.RawMessage `json:"structuredContent"`
}

// postRPC puts one JSON-RPC message on the wire and returns the
// status, response headers and raw body.
func postRPC(ctx context.Context, origin, bearer string, payload any, sessionID string, protocolHeader bool) (int, http.Header, []byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+mcpPath, bytes.NewReader(b))
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+bearer)
	if sessionID != "" {
		req.Header.Set("mcp-session-id", sessionID)
	}
	if protocolHeader {
		req.Header.Set("mcp-protocol-version", protocolVersion)
	}
	resp, err := mcpHTTP.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, resp.Header, nil, err
	}
	return resp.StatusCode, resp.Header, body, nil
}

// parseRPC reads a JSON-RPC answer in either shape a streamable
// HTTP server may send: a plain JSON body, or an SSE stream whose
// data lines carry the messages.
func parseRPC(body []byte) (rpcEnvelope, error) {
	trimmed := bytes.TrimLeft(body, " \t\r\n")
	if len(trimmed) == 0 {
		return rpcEnvelope{}, fmt.Errorf("empty response")
	}
	if trimmed[0] == '{' {
		var env rpcEnvelope
		if err := json.Unmarshal(trimmed, &env); err != nil {
			return rpcEnvelope{}, err
		}
		return env, nil
	}
	var dataLines []string
	flush := func() (rpcEnvelope, bool) {
		if len(dataLines) == 0 {
			return rpcEnvelope{}, false
		}
		payload := strings.Join(dataLines, "\n")
		dataLines = dataLines[:0]
		var env rpcEnvelope
		if err := json.Unmarshal([]byte(payload), &env); err != nil {
			return rpcEnvelope{}, false
		}
		if env.ID == nil && env.Result == nil && env.Error == nil {
			return rpcEnvelope{}, false
		}
		return env, true
	}
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		case line == "":
			if env, ok := flush(); ok {
				return env, nil
			}
		}
	}
	if env, ok := flush(); ok {
		return env, nil
	}
	return rpcEnvelope{}, fmt.Errorf("no message in the response")
}

// renderResult turns a tools/call answer into the text the model
// reads. A typed refusal from the environment (isError) is its
// own words to report, not a broken call.
func renderResult(env rpcEnvelope) (string, error) {
	if env.Error != nil {
		msg := env.Error.Message
		if msg == "" {
			msg = fmt.Sprintf("error %d", env.Error.Code)
		}
		return "", fmt.Errorf("T3 Code: %s", msg)
	}
	var res mcpToolResult
	parts := []string{}
	if len(env.Result) > 0 {
		if err := json.Unmarshal(env.Result, &res); err != nil {
			return "", fmt.Errorf("T3 Code's tool result could not be read")
		}
		for _, c := range res.Content {
			if c.Type == "text" || c.Type == "" {
				parts = append(parts, c.Text)
			}
		}
	}
	text := strings.Join(parts, "\n")
	if text == "" && len(res.StructuredContent) > 0 {
		text = string(res.StructuredContent)
	}
	if text == "" {
		text = "(no content)"
	}
	if len(text) > callOutputCap {
		text = text[:callOutputCap] + "\n…(truncated by Hue)"
	}
	return text, nil
}
