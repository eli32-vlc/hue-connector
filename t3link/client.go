package t3link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// Client is the half that runs on the paired machine, inside
// hue-local. It finds the local T3 Code server, mints a session
// with the machine owner's own t3 CLI (the local auth control
// plane — no OAuth, no public address, no approval page), and
// speaks the environment's MCP wire. The token lives in a 0600
// file next to the connector's config and never crosses the
// socket.
var (
	// ErrNotRunning: no live server record under the T3 home.
	ErrNotRunning = errors.New("T3 Code is not running on this computer. Open the T3 Code app and try again.")
	// ErrNoCLI: the t3 command the mint depends on is missing.
	ErrNoCLI = errors.New("T3 Code's t3 command was not found. Open the T3 Code app once to install it.")
	// ErrLocalUnauthorized: no sign-in has been minted for Hue.
	ErrLocalUnauthorized = errors.New("Hue is not connected to T3 Code yet. Connect it on the dashboard's T3 Code page.")
)

// RuntimeState mirrors what T3 Code persists about its running
// server: enough to find the origin and notice a dead pid.
type RuntimeState struct {
	Origin        string `json:"origin"`
	PID           int    `json:"pid"`
	ServerVersion string `json:"serverVersion"`
}

// tokenRec is the local sign-in. Scopes ride along so a re-mint
// after expiry grants exactly what the dashboard chose, never
// more.
type tokenRec struct {
	Origin    string    `json:"origin"`
	SessionID string    `json:"session_id"`
	Token     string    `json:"token"`
	Scopes    []string  `json:"scopes"`
	ExpiresAt time.Time `json:"expires_at"`
}

// AuthorizeResult reports a fresh sign-in for the dashboard
// without carrying the token itself.
type AuthorizeResult struct {
	Origin    string    `json:"origin"`
	Scopes    []string  `json:"scopes"`
	ExpiresAt time.Time `json:"expires_at"`
}

// refreshMargin re-mints this early so a call in flight never
// trips over a token expiring mid-flight.
const refreshMargin = 2 * time.Minute

// Client talks to one machine's local T3 Code. The hooks exist
// for tests; production fills them in NewClient.
type Client struct {
	TokenPath string
	Discover  func() (RuntimeState, error)
	FindCLI   func() (string, error)
	RunCLI    func(ctx context.Context, cli string, args ...string) ([]byte, error)
	// DialWS opens the environment's client wire. A hook so a test
	// can stand in for the server without a socket.
	DialWS func(ctx context.Context, url string, header http.Header) (wsConn, *http.Response, error)

	// initMu serializes the MCP handshake; mintMu serializes
	// minting so concurrent ops cannot stack sessions; rpcMu guards
	// the client wire, which carries one call at a time.
	initMu sync.Mutex
	mintMu sync.Mutex
	rpcMu  sync.Mutex
	rpc    *rpcConn
	sess   mcpSession
}

// wsConn is the slice of a websocket this package uses, so the
// test double needs no real network.
type wsConn interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, data []byte, err error)
	Close() error
}

// NewClient builds the local client. tokenPath is where the
// sign-in lives; hue-local points it beside its own config.
func NewClient(tokenPath string) *Client {
	return &Client{
		TokenPath: tokenPath,
		Discover:  DiscoverRuntime,
		FindCLI:   FindT3CLI,
		RunCLI:    runCLI,
		DialWS:    dialWebsocket,
	}
}

// dialWebsocket is the production dialer: gorilla's, with the
// bearer token on the upgrade request.
func dialWebsocket(ctx context.Context, url string, header http.Header) (wsConn, *http.Response, error) {
	d := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := d.DialContext(ctx, url, header)
	if err != nil {
		return nil, resp, err
	}
	return conn, resp, nil
}

// t3Home is the T3 Code data directory, honoring the same
// environment the t3 CLI itself honors.
func t3Home() string {
	if home := os.Getenv("T3CODE_HOME"); home != "" {
		return home
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(h, ".t3")
}

// DiscoverRuntime reads the running server's record. The state
// directory is "userdata", or "dev" for a development build.
// A record whose pid is dead is a crash leftover, not a server.
func DiscoverRuntime() (RuntimeState, error) {
	home := t3Home()
	if home == "" {
		return RuntimeState{}, ErrNotRunning
	}
	for _, dir := range []string{"userdata", "dev"} {
		b, err := os.ReadFile(filepath.Join(home, dir, "server-runtime.json"))
		if err != nil {
			continue
		}
		var rs RuntimeState
		if json.Unmarshal(b, &rs) != nil || rs.Origin == "" {
			continue
		}
		if rs.PID > 0 && !processAlive(rs.PID) {
			continue
		}
		return rs, nil
	}
	return RuntimeState{}, ErrNotRunning
}

// processAlive asks the kernel about a pid. EPERM still means
// alive — it is another user's process, which a server is not.
func processAlive(pid int) bool {
	if pid <= 0 {
		return true // unknown: trust the record
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// FindT3CLI resolves the t3 command: PATH first (an installed
// CLI), then the shim T3 Code writes into its own home.
func FindT3CLI() (string, error) {
	if p, err := exec.LookPath("t3"); err == nil {
		return p, nil
	}
	home := t3Home()
	if home == "" {
		return "", ErrNoCLI
	}
	p := filepath.Join(home, "bin", "t3")
	if info, err := os.Stat(p); err == nil && info.Mode()&0o111 != 0 {
		return p, nil
	}
	return "", ErrNoCLI
}

func runCLI(ctx context.Context, cli string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, cli, args...)
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errb.String())
		if len(msg) > 300 {
			msg = msg[:300]
		}
		if msg == "" {
			msg = err.Error()
		}
		return nil, errors.New(msg)
	}
	return []byte(out.String()), nil
}

// Authorize mints a fresh local session with exactly the given
// scopes — the dashboard's permission decision, enforced at the
// source — and stores it. Force-runs even while an older token
// is still valid, so switching levels takes effect at once.
func (c *Client) Authorize(ctx context.Context, scopes []string) (AuthorizeResult, error) {
	if len(scopes) == 0 {
		return AuthorizeResult{}, errors.New("no scopes requested")
	}
	rec, err := c.mint(ctx, scopes, true)
	if err != nil {
		return AuthorizeResult{}, err
	}
	return AuthorizeResult{Origin: rec.Origin, Scopes: rec.Scopes, ExpiresAt: rec.ExpiresAt}, nil
}

// Forget revokes the session on the environment (best effort)
// and removes the token file either way.
func (c *Client) Forget() error {
	if rec, err := c.readFile(); err == nil && rec.SessionID != "" {
		if cli, cerr := c.FindCLI(); cerr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, _ = c.RunCLI(ctx, cli, "auth", "session", "revoke", rec.SessionID)
			cancel()
		}
	}
	c.sess.drop()
	_ = os.Remove(c.TokenPath)
	return nil
}

// Call runs one MCP tool against the local environment: ensure a
// live token and session, dispatch, and heal both once when the
// server says they died (expired/revoked token, forgotten
// session after a restart).
func (c *Client) Call(ctx context.Context, tool string, args map[string]interface{}) (string, error) {
	rs, err := c.Discover()
	if err != nil {
		return "", err
	}
	rec, err := c.readFile()
	if err != nil {
		return "", err
	}
	if rec, err = c.refreshIfNeeded(ctx, rec); err != nil {
		return "", err
	}
	// The environment's own client wire first: it has no runtime-mode
	// ceiling, so a message can reach a thread created at the project's
	// usual full-access. The MCP wire stays as the fallback: when the
	// wire cannot be reached at all, and for any tool it does not
	// carry. A refusal from the environment is final — retrying that
	// on the other wire would be trying to get round an answer.
	if text, rpcErr := c.callViaRPC(ctx, rs.Origin, rec, tool, args); rpcErr == nil {
		return text, nil
	} else if !fallsBackToMCP(rpcErr) {
		return "", rpcErr
	}
	return c.callViaMCP(ctx, rs.Origin, rec, tool, args)
}

// callViaRPC routes one tool onto the environment's client wire.
// A tool this wire does not carry, or a client built without a
// dialer (tests, embedders), reports errUnknownMethod so the
// caller falls back to MCP — the contract-stable path.
func (c *Client) callViaRPC(ctx context.Context, origin string, rec *tokenRec, tool string, args map[string]interface{}) (string, error) {
	if c.DialWS == nil {
		return "", errUnknownMethod
	}
	switch tool {
	case "t3_thread_send":
		return c.threadSend(ctx, origin, rec.Token, args)
	case "t3_thread_launch":
		return c.threadLaunch(ctx, origin, rec.Token, args)
	case "t3_thread_interrupt":
		return c.threadInterrupt(ctx, origin, rec.Token, args)
	case "t3_thread_update":
		return c.threadUpdate(ctx, origin, rec.Token, args)
	case "t3_thread_read":
		return c.threadRead(ctx, origin, rec.Token, args)
	case "t3_project_list":
		return c.projectList(ctx, origin, rec.Token, args)
	case "t3_thread_list":
		return c.threadList(ctx, origin, rec.Token, args)
	default:
		return "", errUnknownMethod
	}
}

// errUnknownMethod means "this wire does not carry that tool".
var errUnknownMethod = &rpcError{method: "", message: "unknown method"}

// fallsBackToMCP is the one decision that keeps this honest: a wire
// we could not reach, or a tool it does not carry, may use the
// documented MCP path. A refusal — the environment saying no — may
// not, or a permission answer would be quietly retried elsewhere.
func fallsBackToMCP(err error) bool {
	if err == nil {
		return false
	}
	if err == errUnknownMethod {
		return true
	}
	// An environment refusal is final.
	var rpcErr *rpcError
	if errors.As(err, &rpcErr) {
		return false
	}
	if errBadArgs(err) {
		return true
	}
	return errors.Is(err, errWireUnreachable)
}

// errWireUnreachable wraps a dial that never opened: an older
// environment without this wire, or one not listening on it.
var errWireUnreachable = errors.New("client wire unreachable")

// argError is a problem with the call this side built, not an
// answer from the environment: these fall back rather than fail, so
// a caller reaching either wire keeps the behaviour it had.
type argError struct{ msg string }

func (e *argError) Error() string { return e.msg }

func errBadArgs(err error) bool {
	var a *argError
	return errors.As(err, &a)
}

// callViaMCP is the original path, unchanged.
func (c *Client) callViaMCP(ctx context.Context, origin string, rec *tokenRec, tool string, args map[string]interface{}) (string, error) {
	if err := c.ensureSession(ctx, origin, rec.Token); err != nil {
		return "", err
	}

	text, status, err := c.toolsCall(ctx, origin, rec.Token, tool, args)
	switch {
	case err != nil:
		return "", err
	case status == httpStatusUnauthorized || status == httpStatusForbidden:
		// The sign-in died (expiry, revoke). Re-mint with the
		// stored scopes and ask once more.
		if rec, err = c.mint(ctx, rec.Scopes, true); err != nil {
			return "", err
		}
		text, status, err = c.toolsCall(ctx, origin, rec.Token, tool, args)
		if err != nil {
			return "", err
		}
		if status == httpStatusUnauthorized || status == httpStatusForbidden {
			return "", ErrLocalUnauthorized
		}
	case status == httpStatusNotFound || status == httpStatusGone:
		// The environment restarted and forgot this session.
		c.sess.drop()
		if err := c.ensureSession(ctx, origin, rec.Token); err != nil {
			return "", err
		}
		text, status, err = c.toolsCall(ctx, origin, rec.Token, tool, args)
		if err != nil {
			return "", err
		}
	}
	if status < 200 || status >= 300 {
		return "", fmt.Errorf("T3 Code's server answered %d on %s", status, tool)
	}
	return text, nil
}

const (
	httpStatusUnauthorized = 401
	httpStatusForbidden    = 403
	httpStatusNotFound     = 404
	httpStatusGone         = 410
)

// ensureSession runs initialize once per origin. Concurrent
// calls line up here and share the id the first one mints.
func (c *Client) ensureSession(ctx context.Context, origin, token string) error {
	c.initMu.Lock()
	defer c.initMu.Unlock()
	// Already initialized against this origin (even without a
	// session id — a sessionless server still counts).
	if cur, _, ok := c.sess.snapshot(); ok && cur == origin {
		return nil
	}
	status, header, _, err := postRPC(ctx, origin, token, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]any{"name": "hue", "version": "1.0"},
		},
	}, "", false)
	if err != nil {
		return fmt.Errorf("could not reach T3 Code: %v", err)
	}
	if status == httpStatusUnauthorized || status == httpStatusForbidden {
		return ErrLocalUnauthorized
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("T3 Code refused the session (%d)", status)
	}
	c.sess.markReady(origin, header.Get("mcp-session-id"))
	// The initialized notification completes the handshake; a
	// non-2xx here is not fatal for servers that fold it in.
	_, _, _, _ = postRPC(ctx, origin, token, map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	}, c.sessID(), true)
	return nil
}

func (c *Client) sessID() string {
	_, id, _ := c.sess.snapshot()
	return id
}

// toolsCall dispatches one call and returns the HTTP status
// alongside so Call can decide between retry, expiry and answer.
func (c *Client) toolsCall(ctx context.Context, origin, token, tool string, args map[string]interface{}) (string, int, error) {
	if args == nil {
		args = map[string]interface{}{}
	}
	sid, id := c.sess.next()
	status, _, body, err := postRPC(ctx, origin, token, map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      tool,
			"arguments": args,
		},
	}, sid, true)
	if err != nil {
		return "", 0, fmt.Errorf("could not reach T3 Code: %v", err)
	}
	if status < 200 || status >= 300 {
		return "", status, nil
	}
	env, err := parseRPC(body)
	if err != nil {
		return "", status, fmt.Errorf("T3 Code's answer could not be read: %v", err)
	}
	text, err := renderResult(env)
	if err != nil {
		return "", status, err
	}
	return text, status, nil
}

// --- the token file ---

func (c *Client) readFile() (*tokenRec, error) {
	b, err := os.ReadFile(c.TokenPath)
	if err != nil {
		return nil, ErrLocalUnauthorized
	}
	var rec tokenRec
	if json.Unmarshal(b, &rec) != nil || rec.Token == "" {
		return nil, ErrLocalUnauthorized
	}
	return &rec, nil
}

func (c *Client) writeFile(rec *tokenRec) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.TokenPath), 0o700); err != nil {
		return err
	}
	return os.WriteFile(c.TokenPath, b, 0o600)
}

// refreshIfNeeded re-mints near expiry using the stored scopes.
func (c *Client) refreshIfNeeded(ctx context.Context, rec *tokenRec) (*tokenRec, error) {
	if time.Now().Add(refreshMargin).Before(rec.ExpiresAt) {
		return rec, nil
	}
	return c.mint(ctx, rec.Scopes, false)
}

// mint runs `t3 auth session issue` as the machine owner, under
// hueSubject (see its definition for why not mcp-client), and stores
// the result. The token never leaves this machine.
func (c *Client) mint(ctx context.Context, scopes []string, force bool) (*tokenRec, error) {
	c.mintMu.Lock()
	defer c.mintMu.Unlock()

	if !force {
		// Another operation may have re-minted while we waited.
		if rec, err := c.readFile(); err == nil && time.Now().Add(refreshMargin).Before(rec.ExpiresAt) {
			return rec, nil
		}
	}
	rs, err := c.Discover()
	if err != nil {
		return nil, err
	}
	cli, err := c.FindCLI()
	if err != nil {
		return nil, err
	}
	args := []string{"auth", "session", "issue", "--json", "--subject", hueSubject, "--label", "hue"}
	for _, s := range scopes {
		args = append(args, "--scope", s)
	}
	out, err := c.RunCLI(ctx, cli, args...)
	if err != nil {
		return nil, fmt.Errorf("could not sign in to T3 Code: %v", err)
	}
	var issued struct {
		SessionID string   `json:"sessionId"`
		Token     string   `json:"token"`
		Scopes    []string `json:"scopes"`
		ExpiresAt string   `json:"expiresAt"`
	}
	if err := json.Unmarshal(out, &issued); err != nil || issued.Token == "" {
		return nil, errors.New("T3 Code's sign-in answer could not be read")
	}
	expires, _ := time.Parse(time.RFC3339, issued.ExpiresAt)
	rec := &tokenRec{
		Origin:    rs.Origin,
		SessionID: issued.SessionID,
		Token:     issued.Token,
		Scopes:    issued.Scopes,
		ExpiresAt: expires,
	}
	if err := c.writeFile(rec); err != nil {
		return nil, fmt.Errorf("could not store the sign-in: %v", err)
	}
	return rec, nil
}

// hueSubject is the session subject Hue mints under.
//
// It used to be "mcp-client", because that was the only subject the
// MCP endpoint would accept. But `/mcp` reads an `mcp-client`
// session's runtime-mode ceiling, and `t3 auth session issue` has no
// flag to set one, so that ceiling was always "approval-required".
// Threads are created at the project's default mode — full-access
// out of the box — and a client may not write above its own
// ceiling, so a connection minted that way could never message a
// normal thread.
//
// The environment's own client wire has no such ceiling: it
// authorizes per method on scopes alone, and `orchestration:operate`
// — which Hue already holds — is all `orchestration.dispatchCommand`
// asks for. It refuses `mcp-client` sessions by name
// ("MCP client sessions are only accepted by the MCP endpoint"), so
// naming this session after the product is what makes that wire
// reachable.
const hueSubject = "hue"
