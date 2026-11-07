// Command hue-local bridges this computer to a Hue
// bot: it pairs with a one-time code from the dashboard, then
// stays connected and runs the operations the user approves in
// chat.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/eli32-vlc/hue-connector/t3link"
)

// config is what pairing leaves behind.
type config struct {
	Server string `json:"server"`
	Token  string `json:"token"`
	Name   string `json:"name"`
}

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".hue-local", "config.json")
}

func loadConfig() (config, error) {
	b, err := os.ReadFile(configPath())
	if err != nil {
		return config{}, fmt.Errorf("not paired: run 'hue-local pair <server> <code>' first")
	}
	var c config
	if err := json.Unmarshal(b, &c); err != nil {
		return config{}, err
	}
	return c, nil
}

func saveConfig(c config) error {
	path := configPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(path, b, 0o600)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "pair":
		pairCmd(os.Args[2:])
	case "run":
		runCmd()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  hue-local pair <server-url> <code>   pair with a code from the dashboard")
	fmt.Println("  hue-local run                        stay connected and run approved actions")
}

func pairCmd(args []string) {
	if len(args) < 2 {
		fmt.Println("usage: hue-local pair <server-url> <code>")
		os.Exit(2)
	}
	server := strings.TrimRight(args[0], "/")
	code := args[1]

	name, _ := os.Hostname()
	if name == "" {
		name = "local"
	}
	body, _ := json.Marshal(map[string]string{"code": code, "name": name})
	resp, err := http.Post(server+"/connector/redeem", "application/json", bytes.NewReader(body))
	if err != nil {
		fatal("could not reach the server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var msg []byte = make([]byte, 200)
		n, _ := resp.Body.Read(msg)
		fatal("pairing refused: %s", strings.TrimSpace(string(msg[:n])))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		fatal("bad answer from the server: %v", err)
	}
	if err := saveConfig(config{Server: server, Token: out.Token, Name: name}); err != nil {
		fatal("could not save the config: %v", err)
	}
	fmt.Printf("Paired with %s as %q.\nRun 'hue-local run' to stay connected.\n", server, name)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// The wire protocol: one JSON object per message.
type wire struct {
	Type   string            `json:"type"`
	ID     string            `json:"id,omitempty"`
	Kind   string            `json:"kind,omitempty"`
	Args   map[string]string `json:"args,omitempty"`
	OK     bool              `json:"ok,omitempty"`
	Output string            `json:"output,omitempty"`
	Err    string            `json:"err,omitempty"`
}

func runCmd() {
	c, err := loadConfig()
	if err != nil {
		fatal("%v", err)
	}
	// The bot is the server and this computer should keep working
	// through its absence, so a drop is retried. The retry is
	// backed off and jittered so a server that is down does not get
	// hammered, and it is capped so a laptop that is really gone
	// stops spinning in the background. A revoked token or a
	// replaced session is not a transient failure: those stop.
	backoff := time.Second
	attempt := 0
	for {
		attempt++
		upSince := time.Now()
		err := serve(c)
		if errors.Is(err, errStop) {
			fatal("%v", err)
		}
		// A link that held for a while before dropping was healthy;
		// start the next attempt over instead of escalating.
		if time.Since(upSince) >= stableWindow {
			backoff = time.Second
			attempt = 1
		}
		wait := jitter(backoff)
		log.Printf("disconnected: %v; reconnecting in %s (attempt %d)", err, wait.Round(time.Millisecond), attempt)
		time.Sleep(wait)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// jitter spreads retries by ±20% so many machines coming back
// after the same outage do not arrive in one burst.
func jitter(d time.Duration) time.Duration {
	spread := int64(d) / 5
	delta := rand.Int64N(spread*2+1) - spread
	wait := d + time.Duration(delta)
	if wait < 0 {
		return 0
	}
	return wait
}

// errStop marks a failure retrying cannot fix: the machine was
// unpaired or another process took over the link.
var errStop = errors.New("stop")

func serve(c config) error {
	u, err := url.Parse(c.Server)
	if err != nil {
		return err
	}
	u.Scheme = strings.Replace(u.Scheme, "http", "ws", 1)
	u.Path = "/local/ws"

	header := http.Header{"Authorization": []string{"Bearer " + c.Token}}
	conn, resp, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		// A 401 means the server no longer knows this token: the
		// machine was removed from the dashboard. Retrying would
		// spin forever against a closed door.
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("%w: this computer is no longer paired; run 'hue-local pair' again", errStop)
		}
		return err
	}
	defer conn.Close()
	log.Printf("connected to %s as %q", c.Server, c.Name)

	// Liveness: the server pings every 30s and expects a pong. The
	// client must answer a ping the instant it arrives, on a
	// connection it owns for the life of the link, or the server
	// reads the silence as a dead socket and closes it. It also
	// keeps its own read deadline: a path that black-holes packets
	// sends no FIN, so without a deadline this loop would wait
	// forever instead of reconnecting. Pings and messages both
	// refresh it.
	conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeTimeout))
	})

	// Writes come from the read loop and from every operation
	// goroutine at once, and the protocol allows only one writer
	// at a time, so they are serialized behind this lock.
	var writeMu sync.Mutex
	writeJSON := func(v any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(v)
	}

	// Per-operation cancellations, driven by the server.
	cancels := map[string]context.CancelFunc{}
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			// A replacement close is deliberate: another session
			// owns this machine now, and reconnecting would only
			// evict it in turn.
			if websocket.IsCloseError(err, closeReplaced) {
				return fmt.Errorf("%w: another hue-local session took over this computer", errStop)
			}
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		var msg wire
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "cancel":
			if cancel, ok := cancels[msg.ID]; ok {
				cancel()
			}
		case "op":
			id := msg.ID
			ctx, cancel := context.WithCancel(context.Background())
			cancels[id] = cancel
			go func() {
				defer delete(cancels, id)
				// One bad operation must never take the whole
				// bridge down with it: a panic here is answered
				// as a failed op, not as a dead process.
				defer func() {
					if r := recover(); r != nil {
						_ = writeJSON(wire{Type: "result", ID: id, OK: false, Err: fmt.Sprintf("internal error: %v", r)})
					}
				}()
				res := execute(ctx, msg)
				_ = writeJSON(wire{Type: "result", ID: id, OK: res.OK, Output: res.Output, Err: res.Err})
			}()
		}
	}
}

const (
	// maxBackoff caps the reconnect wait so a long outage does
	// not space retries far apart, and stableWindow is how long a
	// link must hold before a drop is treated as a fresh start
	// rather than a continuation of a failing streak.
	maxBackoff   = 30 * time.Second
	stableWindow = 60 * time.Second
	writeTimeout = 10 * time.Second
	// readTimeout mirrors the server's deadAfter: three missed
	// pings mean the path is gone, so stop waiting and reconnect.
	readTimeout = 90 * time.Second
	// closeReplaced matches locald's close code for a session
	// displaced by a newer one.
	closeReplaced = 4001
	execOutputCap = 64 * 1024
	fileReadCap   = 512 * 1024
	fetchCap      = 256 * 1024
	// exportCap bounds a file carried to the sandbox. Base64 is
	// four bytes for three, so 12 MB of file is 16 MB on the
	// wire: one frame both websockets carry comfortably.
	exportCap = 12 << 20
)

// t3Client is this machine's T3 Code link: one shared client so
// the token and MCP session are minted once for every operation
// that needs them.
var t3Client = sync.OnceValue(func() *t3link.Client {
	return t3link.NewClient(filepath.Join(filepath.Dir(configPath()), "t3.json"))
})

// t3Op runs one T3 Code action for the server: mint or revoke
// the local sign-in, or call one of the environment's tools.
// The token never leaves this machine — only results cross the
// socket, and the scopes come from the dashboard's choice.
func t3Op(ctx context.Context, out *wire, args map[string]string) {
	c := t3Client()
	switch args["action"] {
	case "authorize":
		res, err := c.Authorize(ctx, strings.Fields(args["scopes"]))
		if err != nil {
			out.OK, out.Err = false, err.Error()
			return
		}
		b, _ := json.Marshal(res)
		out.Output = string(b)
	case "forget":
		if err := c.Forget(); err != nil {
			out.OK, out.Err = false, err.Error()
			return
		}
		out.Output = `{"ok":true}`
	case "call":
		var parsed map[string]any
		if raw := args["args"]; raw != "" {
			if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
				out.OK, out.Err = false, "bad arguments: "+err.Error()
				return
			}
		}
		text, err := c.Call(ctx, args["tool"], parsed)
		if err != nil {
			out.OK, out.Err = false, err.Error()
			return
		}
		b, _ := json.Marshal(map[string]string{"text": text})
		out.Output = string(b)
	default:
		out.OK, out.Err = false, "unknown t3 action: "+args["action"]
	}
}

func execute(ctx context.Context, op wire) wire {
	out := wire{Type: "result", ID: op.ID, OK: true}
	switch op.Kind {
	case "exec":
		timeout := 60
		fmt.Sscanf(op.Args["timeout"], "%d", &timeout)
		ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "/bin/sh", "-c", op.Args["command"])
		buf := &cappedBuffer{cap: execOutputCap}
		cmd.Stdout = buf
		cmd.Stderr = buf
		err := cmd.Run()
		out.Output = buf.String()
		if err != nil {
			out.OK = false
			out.Err = err.Error()
		}
	case "read":
		b, err := os.ReadFile(op.Args["path"])
		if err != nil {
			out.OK, out.Err = false, err.Error()
			break
		}
		if len(b) > fileReadCap {
			b = b[:fileReadCap]
		}
		out.Output = string(b)
	case "write":
		flag := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
		if op.Args["mode"] == "append" {
			flag = os.O_CREATE | os.O_WRONLY | os.O_APPEND
		}
		f, err := os.OpenFile(op.Args["path"], flag, 0o644)
		if err != nil {
			out.OK, out.Err = false, err.Error()
			break
		}
		n, err := f.WriteString(op.Args["content"])
		f.Close()
		if err != nil {
			out.OK, out.Err = false, err.Error()
			break
		}
		out.Output = fmt.Sprintf("wrote %d bytes", n)
	case "export":
		exportFile(op, &out)
	case "fetch":
		method := op.Args["method"]
		if method == "" {
			method = "GET"
		}
		req, err := http.NewRequestWithContext(ctx, method, op.Args["url"], nil)
		if err != nil {
			out.OK, out.Err = false, err.Error()
			break
		}
		for key, val := range op.Args {
			if strings.HasPrefix(key, "header:") {
				req.Header.Set(strings.TrimPrefix(key, "header:"), val)
			}
		}
		client := &http.Client{Timeout: 60 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			out.OK, out.Err = false, err.Error()
			break
		}
		defer resp.Body.Close()
		buf := &cappedBuffer{cap: fetchCap}
		_, _ = io.Copy(buf, resp.Body)
		out.Output = fmt.Sprintf("status: %d\ncontent-type: %s\n\n%s", resp.StatusCode, resp.Header.Get("Content-Type"), buf.String())
	case "t3":
		t3Op(ctx, &out, op.Args)
	default:
		out.OK, out.Err = false, "unknown operation: "+op.Kind
	}
	return out
}

// exportFile answers one export op: the file's bytes, base64 so
// a binary file survives the JSON frame. The cap is enforced here
// before anything is read, so an oversized file fails as a named
// refusal instead of crossing the wire as a truncated copy that
// the sandbox would then store as complete.
func exportFile(op wire, out *wire) {
	path := op.Args["path"]
	max := int64(exportCap)
	if v := op.Args["max"]; v != "" {
		var n int64
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 && n < max {
			max = n
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		out.OK, out.Err = false, err.Error()
		return
	}
	if info.IsDir() {
		out.OK, out.Err = false, "that path is a folder, not a file"
		return
	}
	if info.Size() > max {
		out.OK = false
		out.Err = fmt.Sprintf("the file is %d bytes and the limit is %d; pick a smaller file or copy it in parts", info.Size(), max)
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		out.OK, out.Err = false, err.Error()
		return
	}
	// Read exactly the size checked, never a file that grew
	// between the stat and the read.
	if int64(len(b)) > max {
		out.OK = false
		out.Err = fmt.Sprintf("the file is %d bytes and the limit is %d; pick a smaller file or copy it in parts", len(b), max)
		return
	}
	out.Output = base64.StdEncoding.EncodeToString(b)
}

// cappedBuffer keeps a copy bounded, so one huge command output
// cannot stall the pipe. The write goes through the embedded
// Builder explicitly: b.Write would resolve back to this method
// and recurse until the stack overflowed, killing the process on
// the first byte of any output.
type cappedBuffer struct {
	strings.Builder
	cap int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	// Report the full length even when the stored copy is cut,
	// so the writer never retries the remainder.
	written := len(p)
	if b.Len() < b.cap {
		room := b.cap - b.Len()
		if len(p) > room {
			p = p[:room]
		}
		_, _ = b.Builder.Write(p)
	}
	return written, nil
}
