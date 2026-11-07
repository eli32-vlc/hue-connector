package t3link

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeMCP speaks the streamable-HTTP wire the way the real
// environment does: session id on initialize, protocol header on
// follow-ups, JSON or SSE answers, typed failures as isError
// results. It only ever accepts this machine's minted token.
type fakeMCP struct {
	srv *httptest.Server

	mu             sync.Mutex
	inits          int
	failInitOnce   int // answer 404 to this many tools/calls first
	failAuthOnCall int
}

func newFakeMCP() *fakeMCP {
	f := &fakeMCP{}
	mux := http.NewServeMux()
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			ID     any            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.Unmarshal(body, &req)

		if r.Header.Get("Authorization") != "Bearer tok-live" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		switch req.Method {
		case "initialize":
			f.mu.Lock()
			f.inits++
			f.mu.Unlock()
			w.Header().Set("mcp-session-id", "sess-1")
			writeRPC(w, req.ID, map[string]any{
				"protocolVersion": protocolVersion,
				"capabilities":    map[string]any{},
				"serverInfo":      map[string]any{"name": "T3 Code", "version": "test"},
			})
			return
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
			return
		}

		// Follow-ups carry the two headers the transport requires.
		if r.Header.Get("mcp-session-id") != "sess-1" ||
			r.Header.Get("mcp-protocol-version") != protocolVersion {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		failInit := f.failInitOnce > 0
		if failInit {
			f.failInitOnce--
		}
		failAuth := f.failAuthOnCall > 0
		if failAuth {
			f.failAuthOnCall--
		}
		f.mu.Unlock()
		if failInit {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if failAuth {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		name, _ := req.Params["name"].(string)
		switch name {
		case "sse":
			w.Header().Set("Content-Type", "text/event-stream")
			payload, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result": map[string]any{
					"content": []map[string]any{{"type": "text", "text": "from sse"}},
				},
			})
			_, _ = w.Write([]byte("event: message\ndata: " + string(payload) + "\n\n"))
		case "denied":
			writeRPC(w, req.ID, map[string]any{
				"isError": true,
				"content": []map[string]any{{"type": "text", "text": "read-only client"}},
			})
		case "expired":
			w.WriteHeader(http.StatusUnauthorized)
		case "huge":
			writeRPC(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": strings.Repeat("x", callOutputCap+1024)}},
			})
		default:
			writeRPC(w, req.ID, map[string]any{
				"content": []map[string]any{{"type": "text", "text": "echo:" + name}},
			})
		}
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func writeRPC(w http.ResponseWriter, id any, result any) {
	env := map[string]any{"jsonrpc": "2.0", "id": id}
	if result != nil {
		env["result"] = result
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(env)
}

func (f *fakeMCP) Close() { f.srv.Close() }

func (f *fakeMCP) initCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inits
}

// fakeCLI is a stand-in for `t3`: it records every invocation
// and answers `auth session issue` the way the real CLI does —
// pure JSON under --json, quiet otherwise.
type fakeCLI struct {
	path string
	log  string
}

func newFakeCLI(t *testing.T) *fakeCLI {
	t.Helper()
	dir := t.TempDir()
	f := &fakeCLI{
		path: filepath.Join(dir, "t3"),
		log:  filepath.Join(dir, "args.log"),
	}
	script := "#!/bin/sh\n" +
		"echo \"$@\" >> " + f.log + "\n" +
		"case \"$*\" in\n" +
		"  *\"session issue\"*)\n" +
		"    echo '{\"sessionId\":\"sid-1\",\"token\":\"tok-live\",\"scopes\":[\"orchestration:read\",\"orchestration:operate\"],\"expiresAt\":\"2099-01-01T00:00:00Z\"}'\n" +
		"    ;;\n" +
		"  *) echo ok ;;\n" +
		"esac\n"
	if err := os.WriteFile(f.path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return f
}

// calls lists recorded argument lines, optionally filtered.
func (f *fakeCLI) calls(needle string) []string {
	b, _ := os.ReadFile(f.log)
	out := []string{}
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" {
			continue
		}
		if needle == "" || strings.Contains(line, needle) {
			out = append(out, line)
		}
	}
	return out
}

// newTestClient wires a client to the fakes: the fake MCP
// server stands in for the discovered origin, the script for the
// t3 CLI.
func newTestClient(t *testing.T, mcp *fakeMCP, cli *fakeCLI) *Client {
	t.Helper()
	c := NewClient(filepath.Join(t.TempDir(), "t3.json"))
	c.Discover = func() (RuntimeState, error) {
		return RuntimeState{Origin: mcp.srv.URL, PID: 0, ServerVersion: "test"}, nil
	}
	c.FindCLI = func() (string, error) { return cli.path, nil }
	return c
}

// Authorize mints through the CLI with exactly the requested
// scopes under Hue's own subject, and stores the result without the
// token ever being needed above this machine.
func TestClientAuthorizeMintsLocally(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)

	res, err := c.Authorize(t.Context(), []string{"orchestration:read", "orchestration:operate"})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if res.Origin != mcp.srv.URL {
		t.Fatalf("origin = %q", res.Origin)
	}
	if res.ExpiresAt.Year() != 2099 {
		t.Fatalf("expires = %v", res.ExpiresAt)
	}

	issue := cli.calls("session issue")
	if len(issue) != 1 {
		t.Fatalf("mint ran %d times", len(issue))
	}
	// Not "mcp-client": that is the one subject the environment's own
	// client wire refuses, and /mcp caps an mcp-client session at
	// approval-required, which no thread at full-access accepts.
	if strings.Contains(issue[0], "--subject mcp-client") {
		t.Fatalf("session minted as mcp-client, which the client wire refuses: %s", issue[0])
	}
	if !strings.Contains(issue[0], "--subject hue") {
		t.Fatalf("subject missing: %s", issue[0])
	}
	if !strings.Contains(issue[0], "--scope orchestration:read") ||
		!strings.Contains(issue[0], "--scope orchestration:operate") {
		t.Fatalf("scopes missing: %s", issue[0])
	}

	info, err := os.Stat(c.TokenPath)
	if err != nil {
		t.Fatalf("token file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("token file mode = %o, want 600", perm)
	}
}

// Without a sign-in the call refuses with the one shared
// sentence — the tools repeat it to the model.
func TestClientCallUnauthorizedRefuses(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)

	if _, err := c.Call(t.Context(), "t3_thread_list", nil); err != ErrLocalUnauthorized {
		t.Fatalf("err = %v, want ErrLocalUnauthorized", err)
	}
}

// The whole path: authorize, then a tool call rides initialize
// once and answers in the environment's own words.
func TestClientCallHappyPath(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)
	if _, err := c.Authorize(t.Context(), []string{"orchestration:read"}); err != nil {
		t.Fatal(err)
	}

	text, err := c.Call(t.Context(), "t3_thread_list", map[string]interface{}{"allProjects": true})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if text != "echo:t3_thread_list" {
		t.Fatalf("text = %q", text)
	}
	text, err = c.Call(t.Context(), "t3_thread_send", nil)
	if err != nil || text != "echo:t3_thread_send" {
		t.Fatalf("second Call = (%q, %v)", text, err)
	}
	if got := mcp.initCount(); got != 1 {
		t.Fatalf("initialize ran %d times, want 1 across reuse", got)
	}

	// SSE answers parse; typed refusals stay results; output is
	// bounded.
	if text, err = c.Call(t.Context(), "sse", nil); err != nil || text != "from sse" {
		t.Fatalf("sse = (%q, %v)", text, err)
	}
	if text, err = c.Call(t.Context(), "denied", nil); err != nil || text != "read-only client" {
		t.Fatalf("denied = (%q, %v)", text, err)
	}
	if text, err = c.Call(t.Context(), "huge", nil); err != nil {
		t.Fatal(err)
	}
	if len(text) > callOutputCap+100 || !strings.Contains(text, "truncated by Hue") {
		t.Fatalf("output not bounded: %d bytes", len(text))
	}
}

// A dead token (expiry, revoke) re-mints with the stored scopes
// and retries — the machine heals itself, the dashboard never
// learns there was a hiccup.
func TestClientCallRemintsAfter401(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)
	if _, err := c.Authorize(t.Context(), []string{"orchestration:read"}); err != nil {
		t.Fatal(err)
	}
	mcp.mu.Lock()
	mcp.failAuthOnCall = 1
	mcp.mu.Unlock()

	text, err := c.Call(t.Context(), "t3_thread_list", nil)
	if err != nil || text != "echo:t3_thread_list" {
		t.Fatalf("Call = (%q, %v)", text, err)
	}
	if got := len(cli.calls("session issue")); got != 2 {
		t.Fatalf("mint ran %d times, want 2 (original + re-mint)", got)
	}
}

// A permanently refused token ends in the shared sentence, not
// an endless re-mint loop.
func TestClientCallPermanent401Refuses(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)
	if _, err := c.Authorize(t.Context(), []string{"orchestration:read"}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Call(t.Context(), "expired", nil); err != ErrLocalUnauthorized {
		t.Fatalf("err = %v, want ErrLocalUnauthorized", err)
	}
	if got := len(cli.calls("session issue")); got != 2 {
		t.Fatalf("mint ran %d times, want exactly 2 (one retry)", got)
	}
}

// A forgotten session after a restart is re-initialized once and
// the call retried; it never surfaces as a 404 to the model.
func TestClientCallRecoversForgottenSession(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)
	if _, err := c.Authorize(t.Context(), []string{"orchestration:read"}); err != nil {
		t.Fatal(err)
	}
	mcp.mu.Lock()
	mcp.failInitOnce = 1
	mcp.mu.Unlock()

	text, err := c.Call(t.Context(), "t3_thread_list", nil)
	if err != nil || text != "echo:t3_thread_list" {
		t.Fatalf("Call = (%q, %v)", text, err)
	}
	if got := mcp.initCount(); got != 2 {
		t.Fatalf("initialize ran %d times, want 2", got)
	}
}

// No live server record: the sentence that says to open the app.
func TestClientCallNotRunning(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)
	c.Discover = func() (RuntimeState, error) { return RuntimeState{}, ErrNotRunning }

	if _, err := c.Call(t.Context(), "t3_thread_list", nil); err != ErrNotRunning {
		t.Fatalf("err = %v, want ErrNotRunning", err)
	}
}

// Forget revokes the session through the CLI and removes the
// file either way.
func TestClientForgetRevokes(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)
	if _, err := c.Authorize(t.Context(), []string{"orchestration:read"}); err != nil {
		t.Fatal(err)
	}

	if err := c.Forget(); err != nil {
		t.Fatalf("Forget: %v", err)
	}
	if _, err := os.Stat(c.TokenPath); !os.IsNotExist(err) {
		t.Fatal("token file still present after Forget")
	}
	revokes := cli.calls("session revoke")
	if len(revokes) != 1 || !strings.Contains(revokes[0], "sid-1") {
		t.Fatalf("revokes = %v", revokes)
	}
	// And the next call refuses as never-connected.
	if _, err := c.Call(t.Context(), "t3_thread_list", nil); err != ErrLocalUnauthorized {
		t.Fatalf("after Forget: %v", err)
	}
}

// The runtime record is how the client finds the server: a dead
// pid is a crash leftover and must not be dialed.
func TestDiscoverRuntime(t *testing.T) {
	home := t.TempDir()
	t.Setenv("T3CODE_HOME", home)

	if _, err := DiscoverRuntime(); err != ErrNotRunning {
		t.Fatalf("no record: %v", err)
	}
	write := func(pid int) {
		b, _ := json.Marshal(RuntimeState{Origin: "http://127.0.0.1:3773", PID: pid})
		if err := os.MkdirAll(filepath.Join(home, "userdata"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(home, "userdata", "server-runtime.json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Our own pid is alive; a pid from the distant past is not.
	write(os.Getpid())
	rs, err := DiscoverRuntime()
	if err != nil || rs.Origin != "http://127.0.0.1:3773" {
		t.Fatalf("live record: %v %+v", err, rs)
	}
	write(1 << 30) // far beyond any real pid
	if _, err := DiscoverRuntime(); err != ErrNotRunning {
		t.Fatalf("dead record treated as live: %v", err)
	}
}

func TestDiagDialAgainstPlainHTTP(t *testing.T) {
	mcp := newFakeMCP()
	defer mcp.Close()
	cli := newFakeCLI(t)
	c := newTestClient(t, mcp, cli)
	if _, err := c.Authorize(t.Context(), []string{"orchestration:read"}); err != nil {
		t.Fatal(err)
	}
	rec, _ := c.readFile()
	_, err := c.callViaRPC(t.Context(), mcp.srv.URL, rec, "t3_thread_send", nil)
	t.Logf("callViaRPC err = %v ; fallsBack=%v", err, fallsBackToMCP(err))
}
