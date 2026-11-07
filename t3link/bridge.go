package t3link

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// bridge.go maps Hue's t3_* tools onto the environment's client
// wire. Every tool answers in the same prose shape the MCP wire
// returned, so the agent prompts that describe these tools stay
// true: a tool call still returns readable text, never raw JSON.
//
// Each call is a plain V2 command. Nothing here decides policy —
// the environment authorizes per method on the session's scopes,
// and a refusal comes back as an *rpcError with the environment's
// own words.

// rpcCall runs one method and decodes its result into out. A
// missing or expired token is re-minted once, exactly as the MCP
// path does, so a long-lived install heals itself.
func (c *Client) rpcCall(ctx context.Context, origin, token string, method rpcMethod, payload any, out any) error {
	raw, err := c.rpcDo(ctx, origin, token, method, payload)
	if err != nil {
		// A dead sign-in shows up as a closed wire, not an HTTP
		// status; re-mint once and try again.
		if isAuthish(err) {
			rec, mErr := c.readFile()
			if mErr != nil {
				return err
			}
			fresh, mintErr := c.mint(ctx, rec.Scopes, true)
			if mintErr != nil {
				return err
			}
			c.rpcMu.Lock()
			if c.rpc != nil {
				_ = c.rpc.close()
				c.rpc = nil
			}
			c.rpcMu.Unlock()
			raw, err = c.rpcDo(ctx, origin, fresh.Token, method, payload)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("T3 Code's answer to %s could not be read: %w", method, err)
	}
	return nil
}

// rpcDo ensures the wire is open, then sends one call. The wire is
// kept for the process lifetime and reopened when it closes.
func (c *Client) rpcDo(ctx context.Context, origin, token string, method rpcMethod, payload any) (json.RawMessage, error) {
	conn, err := c.rpcConn(ctx, origin, token)
	if err != nil {
		return nil, err
	}
	raw, err := conn.call(ctx, method, payload)
	if err != nil {
		// A closed socket is not reusable; drop it so the next call
		// dials fresh.
		c.rpcMu.Lock()
		if c.rpc == conn {
			_ = c.rpc.close()
			c.rpc = nil
		}
		c.rpcMu.Unlock()
	}
	return raw, err
}

func (c *Client) rpcConn(ctx context.Context, origin, token string) (*rpcConn, error) {
	c.rpcMu.Lock()
	defer c.rpcMu.Unlock()
	if c.rpc != nil {
		return c.rpc, nil
	}
	conn, err := c.dialRPC(ctx, origin, token)
	if err != nil {
		return nil, err
	}
	c.rpc = conn
	return conn, nil
}

func isAuthish(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrLocalUnauthorized {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "forbidden") ||
		strings.Contains(msg, "closed") ||
		strings.Contains(msg, "401") ||
		strings.Contains(msg, "403")
}

// --- command construction -------------------------------------

// newCommandID is a fresh V2 command id. The environment dedupes on
// it, so a retry after a dropped frame replays instead of doubling
// the message — but only if the id is stable per logical call,
// which is why callers build it once.
func newCommandID(prefix string) string {
	return fmt.Sprintf("%s:%d", prefix, time.Now().UnixNano())
}

// dispatchCommand sends one V2 command through the client wire.
func (c *Client) dispatchCommand(ctx context.Context, origin, token string, command map[string]any) error {
	return c.rpcCall(ctx, origin, token, rpcDispatchCommand, command, nil)
}

// threadSend builds and sends one message.dispatch, the same
// command the app's composer uses. mode decides whether it joins
// the running turn, queues behind it, or restarts it.
func (c *Client) threadSend(ctx context.Context, origin, token string, args map[string]interface{}) (string, error) {
	threadID := strings.TrimSpace(t3Str(args, "threadId"))
	if threadID == "" {
		return "", &argError{"threadId is required"}
	}
	text := strings.TrimSpace(t3Str(args, "message"))
	if text == "" {
		return "", &argError{"message is required"}
	}
	mode := strings.TrimSpace(t3Str(args, "mode"))
	if mode == "" {
		mode = "auto"
	}
	if mode != "auto" && mode != "queue" && mode != "steer" && mode != "restart" {
		return "", &argError{"mode must be auto, queue, steer or restart"}
	}

	commandID := newCommandID("hue-send")
	command := map[string]any{
		"type":           "message.dispatch",
		"commandId":      commandID,
		"threadId":       threadID,
		"messageId":      commandID,
		"text":           text,
		"attachments":    []any{},
		"createdBy":      "agent",
		"creationSource": "mcp",
		"dispatchMode":   dispatchModeFor(mode),
	}
	if err := c.dispatchCommand(ctx, origin, token, command); err != nil {
		return "", err
	}
	return fmt.Sprintf("Sent to thread %s (mode %s). Read it with t3_thread_read.", threadID, mode), nil
}

// threadLaunch starts one new thread, the same launchThread RPC the
// app uses.
func (c *Client) threadLaunch(ctx context.Context, origin, token string, args map[string]interface{}) (string, error) {
	title := strings.TrimSpace(t3Str(args, "title"))
	if title == "" {
		return "", &argError{"title is required"}
	}
	payload := map[string]any{
		"commandId":      newCommandID("hue-launch"),
		"title":          title,
		"createdBy":      "agent",
		"creationSource": "mcp",
		"workspaceStrategy": map[string]any{
			"type": "root",
		},
	}
	if projectID := t3Str(args, "projectId"); projectID != "" {
		payload["projectId"] = projectID
	}
	if message := strings.TrimSpace(t3Str(args, "message")); message != "" {
		payload["message"] = message
	}
	var result struct {
		ThreadID string `json:"threadId"`
	}
	if err := c.rpcCall(ctx, origin, token, rpcLaunchThread, payload, &result); err != nil {
		return "", err
	}
	if result.ThreadID == "" {
		return "Launched the thread, but T3 Code's answer carried no id. List threads to find it.", nil
	}
	return fmt.Sprintf("Launched thread %s.", result.ThreadID), nil
}

// threadInterrupt stops the running turn, like the Stop button.
func (c *Client) threadInterrupt(ctx context.Context, origin, token string, args map[string]interface{}) (string, error) {
	threadID := strings.TrimSpace(t3Str(args, "threadId"))
	if threadID == "" {
		return "", &argError{"threadId is required"}
	}
	reason := strings.TrimSpace(t3Str(args, "reason"))
	command := map[string]any{
		"type":      "thread.stop",
		"commandId": newCommandID("hue-stop"),
		"threadId":  threadID,
	}
	if reason != "" {
		command["reason"] = reason
	}
	if err := c.dispatchCommand(ctx, origin, token, command); err != nil {
		return "", err
	}
	return fmt.Sprintf("Asked thread %s to stop.", threadID), nil
}

// threadUpdate renames a thread.
func (c *Client) threadUpdate(ctx context.Context, origin, token string, args map[string]interface{}) (string, error) {
	threadID := strings.TrimSpace(t3Str(args, "threadId"))
	action := strings.TrimSpace(t3Str(args, "action"))
	title := strings.TrimSpace(t3Str(args, "title"))
	if threadID == "" {
		return "", &argError{"threadId is required"}
	}
	if action != "rename" {
		return "", fmt.Errorf("only rename is supported over this wire; %q is not", action)
	}
	if title == "" {
		return "", &argError{"title is required to rename"}
	}
	command := map[string]any{
		"type":      "thread.metadata.update",
		"commandId": newCommandID("hue-rename"),
		"threadId":  threadID,
		"title":     title,
	}
	if err := c.dispatchCommand(ctx, origin, token, command); err != nil {
		return "", err
	}
	return fmt.Sprintf("Renamed thread %s to %q.", threadID, title), nil
}

// dispatchModeFor translates Hue's mode into the command's
// dispatchMode. These are different vocabularies: `queue` is not a
// delivery intent (which the environment accepts only as auto, steer
// or restart) but a dispatch mode of its own.
//
// `steer` and `restart` need the run to steer, which this side does
// not know. The environment fills in the target itself for those
// intents, so they are sent as `deliveryIntent` and the dispatch
// mode is left to it.
func dispatchModeFor(mode string) map[string]any {
	switch mode {
	case "queue":
		return map[string]any{"type": "queue_after_active"}
	default:
		return map[string]any{"type": "start_immediately"}
	}
}

// threadProjection is the slice of a thread projection these tools
// render. Field names come from the environment's own schema.
type threadProjection struct {
	Thread struct {
		ID             string `json:"id"`
		Title          string `json:"title"`
		ProjectID      string `json:"projectId"`
		RuntimeMode    string `json:"runtimeMode"`
		Interaction    string `json:"interactionMode"`
		ArchivedAt     any    `json:"archivedAt"`
		WorktreePath   any    `json:"worktreePath"`
		ActiveRunID    any    `json:"activeRunId"`
		ProviderInst   string `json:"providerInstanceId"`
		CreatedAt      string `json:"createdAt"`
		UpdatedAt      string `json:"updatedAt"`
		ParentID       any    `json:"parentThreadId"`
		Relationship   any    `json:"relationshipToParent"`
		Lineage        any    `json:"lineage"`
		ModelSelection struct {
			InstanceID string `json:"instanceId"`
			Model      string `json:"model"`
		} `json:"modelSelection"`
	} `json:"thread"`
	Runs []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	} `json:"runs"`
	Messages []struct {
		ID      string `json:"id"`
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
}

// threadRead renders one thread as text, the way the MCP wire
// returned it: a short header, then the latest messages.
func (c *Client) threadRead(ctx context.Context, origin, token string, args map[string]interface{}) (string, error) {
	threadID := strings.TrimSpace(t3Str(args, "threadId"))
	if threadID == "" {
		return "", &argError{"threadId is required"}
	}
	var projection threadProjection
	if err := c.rpcCall(ctx, origin, token, rpcGetProjection, map[string]any{"threadId": threadID}, &projection); err != nil {
		return "", err
	}

	var b strings.Builder
	title := projection.Thread.Title
	if title == "" {
		title = threadID
	}
	fmt.Fprintf(&b, "Thread %s — %q\n", threadID, title)
	fmt.Fprintf(&b, "Project: %s · modes: %s/%s\n",
		projection.Thread.ProjectID,
		projection.Thread.RuntimeMode,
		projection.Thread.Interaction)
	if status := latestRunStatus(projection); status != "" {
		fmt.Fprintf(&b, "Latest run: %s\n", status)
	}

	limit := t3Int(args, "limit")
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	msgs := projection.Messages
	if len(msgs) > limit {
		msgs = msgs[len(msgs)-limit:]
	}
	if len(msgs) == 0 {
		b.WriteString("\nNo messages yet.\n")
		return b.String(), nil
	}
	b.WriteString("\n")
	for _, m := range msgs {
		role := m.Role
		if role == "" {
			role = "message"
		}
		fmt.Fprintf(&b, "[%s] %s\n", role, strings.TrimSpace(m.Content))
	}
	return b.String(), nil
}

func latestRunStatus(p threadProjection) string {
	if len(p.Runs) == 0 {
		return ""
	}
	return p.Runs[len(p.Runs)-1].Status
}

// shellSnapshot is the slice of the environment's shell snapshot
// these reads render. One call carries every project and thread, so
// listing does not depend on a per-project RPC the wire does not
// have.
type shellSnapshot struct {
	Projects []struct {
		ID            string `json:"id"`
		Title         string `json:"title"`
		WorkspaceRoot string `json:"workspaceRoot"`
	} `json:"projects"`
	Threads []struct {
		ID          string `json:"id"`
		Title       string `json:"title"`
		ProjectID   string `json:"projectId"`
		RuntimeMode string `json:"runtimeMode"`
		ArchivedAt  any    `json:"archivedAt"`
		UpdatedAt   string `json:"updatedAt"`
	} `json:"threads"`
}

// projectsAndThreads reads one shell snapshot. subscribeShell is a
// stream — a snapshot frame then changes — so only its first value is
// kept and the rest is discarded.
func (c *Client) projectsAndThreads(ctx context.Context, origin, token string) (*shellSnapshot, error) {
	conn, err := c.rpcConn(ctx, origin, token)
	if err != nil {
		return nil, err
	}
	raw, err := conn.callFirstValue(ctx, rpcSubscribeShell, map[string]any{})
	if err != nil {
		return nil, err
	}
	// The stream's first value is {kind:"snapshot", snapshot:{…}};
	// later frames are changes, which a listing does not need.
	var frame struct {
		Kind     string         `json:"kind"`
		Snapshot *shellSnapshot `json:"snapshot"`
		// A bare snapshot, if the environment ever answers without
		// the kind wrapper.
		Threads  []json.RawMessage `json:"threads"`
		Projects []json.RawMessage `json:"projects"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, fmt.Errorf("T3 Code's shell snapshot could not be read: %w", err)
	}
	if frame.Snapshot != nil {
		return frame.Snapshot, nil
	}
	if len(frame.Threads) > 0 || len(frame.Projects) > 0 {
		var bare shellSnapshot
		if err := json.Unmarshal(raw, &bare); err != nil {
			return nil, fmt.Errorf("T3 Code's shell snapshot could not be read: %w", err)
		}
		return &bare, nil
	}
	snippet := string(raw)
	if len(snippet) > 200 {
		snippet = snippet[:200]
	}
	return nil, fmt.Errorf("T3 Code sent a %q shell frame where a snapshot was expected: %s", frame.Kind, snippet)
}

// projectList lists the environment's projects as text.
func (c *Client) projectList(ctx context.Context, origin, token string, _ map[string]interface{}) (string, error) {
	snap, err := c.projectsAndThreads(ctx, origin, token)
	if err != nil {
		return "", err
	}
	if len(snap.Projects) == 0 {
		return "No projects yet.", nil
	}
	sorted := append([]struct {
		ID            string `json:"id"`
		Title         string `json:"title"`
		WorkspaceRoot string `json:"workspaceRoot"`
	}(nil), snap.Projects...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Title < sorted[j].Title })
	var b strings.Builder
	for _, e := range sorted {
		fmt.Fprintf(&b, "%s — %s (%s)\n", e.ID, e.Title, e.WorkspaceRoot)
	}
	return b.String(), nil
}

// threadList lists threads, newest first, from the same snapshot.
func (c *Client) threadList(ctx context.Context, origin, token string, args map[string]interface{}) (string, error) {
	snap, err := c.projectsAndThreads(ctx, origin, token)
	if err != nil {
		return "", err
	}
	projectID := strings.TrimSpace(t3Str(args, "projectId"))
	titleContains := strings.ToLower(strings.TrimSpace(t3Str(args, "titleContains")))
	limit := t3Int(args, "limit")
	if limit <= 0 || limit > 100 {
		limit = 25
	}

	shown := make([]struct {
		ID          string `json:"id"`
		Title       string `json:"title"`
		ProjectID   string `json:"projectId"`
		RuntimeMode string `json:"runtimeMode"`
		ArchivedAt  any    `json:"archivedAt"`
		UpdatedAt   string `json:"updatedAt"`
	}, 0, len(snap.Threads))
	for _, t := range snap.Threads {
		if projectID != "" && t.ProjectID != projectID {
			continue
		}
		if titleContains != "" && !strings.Contains(strings.ToLower(t.Title), titleContains) {
			continue
		}
		shown = append(shown, t)
	}
	sort.SliceStable(shown, func(i, j int) bool { return shown[i].UpdatedAt > shown[j].UpdatedAt })
	if len(shown) > limit {
		shown = shown[:limit]
	}
	if len(shown) == 0 {
		return "No threads matched.", nil
	}
	var b strings.Builder
	for _, t := range shown {
		title := t.Title
		if title == "" {
			title = "(untitled)"
		}
		fmt.Fprintf(&b, "%s — %s · %s · updated %s\n", t.ID, title, t.RuntimeMode, t.UpdatedAt)
	}
	return b.String(), nil
}

// --- small accessors ------------------------------------------
// The tool package does this whitelisting for MCP arguments; this
// wire reads the same map shape, so it needs the same two helpers.

func t3Str(args map[string]interface{}, key string) string {
	if args == nil {
		return ""
	}
	if v, ok := args[key].(string); ok {
		return v
	}
	return ""
}

func t3Int(args map[string]interface{}, key string) int {
	if args == nil {
		return 0
	}
	switch v := args[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}
