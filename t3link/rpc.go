package t3link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
)

// This file is Hue's second wire to T3 Code.
//
// The MCP wire (`/mcp`) is the documented, contract-stable way in,
// and it stays the default. But every session it accepts is capped:
// the environment reads an `mcp-client` session's runtime-mode
// ceiling, which `t3 auth session issue` cannot set, so the cap is
// always "approval-required". Threads are created at the project's
// default mode (full-access out of the box), and a client may never
// write above its own ceiling — so a Hue connection could never
// message a normal thread.
//
// The environment's own client wire has no such cap: it authorizes
// per method on scopes alone. A non-`mcp-client` session is refused
// by `/mcp` and accepted here, and `orchestration.dispatchCommand`
// asks only for `orchestration:operate` — which Hue already holds.
// That is the whole reason this file exists.
//
// The trade is a contract: this is T3 Code's internal wire, not a
// published one. `rpcCapabilities` records what we depend on, and a
// decoder that rejects surprises turns an upstream break into a
// clear error instead of a wrong action.

// rpcMethod is one entry of T3 Code's client RPC surface.
type rpcMethod string

const (
	rpcDispatchCommand  rpcMethod = "orchestration.dispatchCommand"
	rpcLaunchThread     rpcMethod = "orchestration.launchThread"
	rpcGetProjection    rpcMethod = "orchestration.getThreadProjection"
	rpcSearchThreads    rpcMethod = "orchestration.searchThreads"
	rpcSearchThread     rpcMethod = "orchestration.searchThread"
	rpcGetThreadTitles  rpcMethod = "orchestration.getThreadTitles"
	rpcForceStopThread  rpcMethod = "orchestration.forceStopThread"
	rpcStopWorkflow     rpcMethod = "orchestration.stopWorkflow"
	rpcListProjects     rpcMethod = "projects.listEntries"
	rpcPendingRequests  rpcMethod = "orchestration.getThreadProjection"
	rpcSubscribeShell   rpcMethod = "orchestration.subscribeShell"
	rpcSubscribeThread  rpcMethod = "orchestration.subscribeThread"
	rpcAcknowledgeInit  rpcMethod = "initialize"
	rpcClientCapability rpcMethod = "capabilities"
)

// rpcCapabilities is what this client needs from values it reads.
// Named so a test can pin them and a reader can see the contract.
type rpcCapabilities struct {
	// Envelope tags produced by the environment's JSON serialization.
	// A call is answered with Exit, whose own _tag is Success or
	// Failure; a Defect frame means the environment threw.
	RequestTag string
	ExitTag    string
	SuccessTag string
	FailureTag string
	DefectTag  string
	// Projections carry their thread and its runs under these keys.
	ThreadKey string
	RunsKey   string
}

var rpcContract = rpcCapabilities{
	RequestTag: "Request",
	ExitTag:    "Exit",
	SuccessTag: "Success",
	FailureTag: "Failure",
	DefectTag:  "Defect",
	ThreadKey:  "thread",
	RunsKey:    "runs",
}

// rpcRequest is the envelope the environment decodes. `tag` is the
// method, `payload` its arguments. `headers` is required even when
// empty; the decoder rejects a request without it.
type rpcRequest struct {
	Discriminator string      `json:"_tag"`
	Tag           string      `json:"tag"`
	ID            string      `json:"id"`
	Payload       any         `json:"payload"`
	Headers       [][2]string `json:"headers"`
	Discard       bool        `json:"discard,omitempty"`
}

// rpcEnvelopeIn is one decoded message from the server. The
// environment tags each frame and correlates it by `id` (responses)
// or `requestId` (stream chunks).
type rpcEnvelopeIn struct {
	Tag       string            `json:"_tag"`
	ID        json.RawMessage   `json:"id"`
	RequestID json.RawMessage   `json:"requestId"`
	Exit      *rpcExit          `json:"exit"`
	Defect    json.RawMessage   `json:"defect"`
	Values    []json.RawMessage `json:"values"`
}

// rpcExit is the result envelope inside an Exit frame: the environment
// answers a call with Exit{Success|Failure}, not with a bare value.
type rpcExit struct {
	Tag   string          `json:"_tag"`
	Value json.RawMessage `json:"value"`
	Cause json.RawMessage `json:"cause"`
}

// rpcError is a failure the environment reported for one call.
type rpcError struct {
	method  rpcMethod
	message string
}

func (e *rpcError) Error() string { return string(e.method) + ": " + e.message }

// dialRPC opens the environment's client wire. It carries the same
// bearer token the MCP wire used; the environment accepts a
// non-`mcp-client` session here and authorizes each method on its
// scopes alone.
func (c *Client) dialRPC(ctx context.Context, origin, token string) (*rpcConn, error) {
	if c.DialWS == nil {
		return nil, errors.New("no websocket dialer configured")
	}
	url := wsURL(origin)
	header := http.Header{}
	header.Set("authorization", "Bearer "+token)
	conn, resp, err := c.DialWS(ctx, url, header)
	if err != nil {
		if resp != nil && (resp.StatusCode == httpStatusUnauthorized || resp.StatusCode == httpStatusForbidden) {
			return nil, ErrLocalUnauthorized
		}
		// Never opened: an older environment without this wire, or one
		// not listening on it. The caller may use the MCP path instead.
		return nil, fmt.Errorf("%w: %v", errWireUnreachable, err)
	}
	return &rpcConn{conn: conn, next: 1}, nil
}

// wsURL turns the environment origin into its client-wire URL.
func wsURL(origin string) string {
	var base string
	switch {
	case len(origin) > 8 && origin[:8] == "https://":
		base = "wss://" + origin[8:] + "/ws"
	case len(origin) > 7 && origin[:7] == "http://":
		base = "ws://" + origin[7:] + "/ws"
	default:
		base = origin + "/ws"
	}
	return base + "?" + orchestrationProtocolQueryParam + "=" + orchestrationProtocolVersion
}

// The environment's client wire, as of orchestration protocol 2. The
// query parameter is mandatory: without it the upgrade is refused with
// 426 orchestration_protocol_incompatible, which reads like a broken
// server rather than an old client.
const (
	orchestrationProtocolQueryParam = "orchestrationProtocol"
	orchestrationProtocolVersion    = "2"
)

// rpcConn is one open client wire: one request in flight at a
// time, which is all a tool call needs. The mutex keeps two tools
// from interleaving their frames.
type rpcConn struct {
	conn interface {
		WriteMessage(int, []byte) error
		ReadMessage() (int, []byte, error)
		Close() error
	}
	mu   sync.Mutex
	next int
}

const wsTextMessage = 1

// call sends one request and waits for its result. A Failure
// envelope becomes an *rpcError so the caller can tell "the
// environment said no" from "the wire broke".
func (r *rpcConn) call(ctx context.Context, method rpcMethod, payload any) (json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := r.next
	r.next++

	frame, err := json.Marshal(rpcRequest{
		Discriminator: rpcContract.RequestTag,
		Tag:           string(method),
		ID:            fmt.Sprintf("%d", id),
		Payload:       payload,
		Headers:       [][2]string{},
	})
	if err != nil {
		return nil, fmt.Errorf("could not encode %s: %w", method, err)
	}
	if err := r.conn.WriteMessage(wsTextMessage, frame); err != nil {
		return nil, fmt.Errorf("could not send %s: %w", method, err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, raw, err := r.conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("T3 Code's client wire closed during %s: %w", method, err)
		}
		var env rpcEnvelopeIn
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, fmt.Errorf("T3 Code sent something this client cannot read: %w", err)
		}
		// Frames that are not this call's answer: subscriptions,
		// acknowledgements, pongs. Skip them; a tool call is not a
		// stream and reads exactly one answer.
		if !rpcEnvelopeAnswersCall(&env, id) {
			continue
		}
		switch env.Tag {
		case rpcContract.ExitTag:
			if env.Exit == nil {
				return nil, fmt.Errorf("T3 Code answered %s with an empty Exit", method)
			}
			if env.Exit.Tag == rpcContract.SuccessTag {
				return env.Exit.Value, nil
			}
			return nil, &rpcError{method: method, message: rpcCauseMessage(env.Exit.Cause)}
		case rpcContract.DefectTag:
			return nil, fmt.Errorf("T3 Code hit a defect on %s: %s", method, rpcDefectMessage(env.Defect))
		default:
			return nil, fmt.Errorf("T3 Code answered %s with an unknown frame %q", method, env.Tag)
		}
	}
}

// callFirstValue runs a streaming method and returns the first value
// it yields, cancelling the rest. The shell snapshot arrives that
// way: one snapshot frame then changes, and a listing needs only the
// snapshot.
func (r *rpcConn) callFirstValue(ctx context.Context, method rpcMethod, payload any) (json.RawMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := r.next
	r.next++
	frame, err := json.Marshal(rpcRequest{
		Discriminator: rpcContract.RequestTag,
		Tag:           string(method),
		ID:            fmt.Sprintf("%d", id),
		Payload:       payload,
		Headers:       [][2]string{},
	})
	if err != nil {
		return nil, fmt.Errorf("could not encode %s: %w", method, err)
	}
	if err := r.conn.WriteMessage(wsTextMessage, frame); err != nil {
		return nil, fmt.Errorf("could not send %s: %w", method, err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, raw, err := r.conn.ReadMessage()
		if err != nil {
			return nil, fmt.Errorf("T3 Code's client wire closed during %s: %w", method, err)
		}
		var env rpcEnvelopeIn
		if err := json.Unmarshal(raw, &env); err != nil {
			return nil, fmt.Errorf("T3 Code sent something this client cannot read: %w", err)
		}
		if !rpcEnvelopeAnswersCall(&env, id) {
			continue
		}
		switch env.Tag {
		case "Chunk":
			if len(env.Values) > 0 {
				return env.Values[0], nil
			}
			continue
		case rpcContract.ExitTag:
			if env.Exit != nil && env.Exit.Tag == rpcContract.SuccessTag {
				return env.Exit.Value, nil
			}
			if env.Exit != nil {
				return nil, &rpcError{method: method, message: rpcCauseMessage(env.Exit.Cause)}
			}
		case rpcContract.DefectTag:
			return nil, fmt.Errorf("T3 Code hit a defect on %s: %s", method, rpcDefectMessage(env.Defect))
		}
	}
}

// rpcEnvelopeAnswersCall reports whether env is the answer to
// request id. The environment correlates by `id` on responses and
// `requestId` on stream chunks; a frame with neither (a Pong, a
// server-initiated notification) is never this call's answer.
func rpcEnvelopeAnswersCall(env *rpcEnvelopeIn, id int) bool {
	for _, raw := range []json.RawMessage{env.ID, env.RequestID} {
		if len(raw) == 0 {
			continue
		}
		var asNumber int
		if err := json.Unmarshal(raw, &asNumber); err == nil && asNumber == id {
			return true
		}
		var asString string
		if err := json.Unmarshal(raw, &asString); err == nil && asString == fmt.Sprintf("%d", id) {
			return true
		}
	}
	return false
}

// rpcCauseMessage flattens the cause array the environment puts on
// a failure into one readable line.
func rpcCauseMessage(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "the environment refused it"
	}
	var causes []struct {
		Tag     string          `json:"_tag"`
		Message string          `json:"message"`
		Value   json.RawMessage `json:"value"`
		Failure json.RawMessage `json:"failure"`
	}
	if err := json.Unmarshal(raw, &causes); err != nil || len(causes) == 0 {
		return "the environment refused it"
	}
	for _, c := range causes {
		if c.Message != "" {
			return c.Message
		}
	}
	return "the environment refused it"
}

// close closes the wire.
func (r *rpcConn) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conn.Close()
}

// rpcDefectMessage reads the defect a Defect frame carries. It is
// always a string on this wire; anything else is reported as-is
// rather than guessed at.
func rpcDefectMessage(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "an unexplained defect"
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		return s
	}
	return string(raw)
}
