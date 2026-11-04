// Command hue-connector bridges this computer to a Hue
// bot: it pairs with a one-time code from the dashboard, then
// stays connected and runs the operations the user approves in
// chat.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// config is what pairing leaves behind.
type config struct {
	Server string `json:"server"`
	Token  string `json:"token"`
	Name   string `json:"name"`
}

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".hue-connector", "config.json")
}

func loadConfig() (config, error) {
	b, err := os.ReadFile(configPath())
	if err != nil {
		return config{}, fmt.Errorf("not paired: run 'hue-connector pair <server> <code>' first")
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
	case "status":
		statusCmd()
	case "version":
		fmt.Println(version)
	default:
		usage()
		os.Exit(2)
	}
}

var version = "dev"

func statusCmd() {
	c, err := loadConfig()
	if err != nil {
		fatal("%v", err)
	}
	fmt.Printf("server: %s\nname: %s\n", c.Server, c.Name)
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  hue-connector pair <server-url> <code>   pair with a code from the dashboard")
	fmt.Println("  hue-connector run                        stay connected and run approved actions")
}

func pairCmd(args []string) {
	if len(args) < 2 {
		fmt.Println("usage: hue-connector pair <server-url> <code>")
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
	fmt.Printf("Paired with %s as %q.\nRun 'hue-connector run' to stay connected.\n", server, name)
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
	// backed off so a server that is down does not get hammered,
	// and it is capped so a laptop that is really gone stops
	// spinning in the background.
	backoff := time.Second
	for {
		upSince := time.Now()
		err := serve(c)
		// A link that held for a while before dropping was healthy;
		// start the next attempt over instead of escalating.
		if time.Since(upSince) >= stableWindow {
			backoff = time.Second
		}
		log.Printf("disconnected: %v; retrying in %s", err, backoff)
		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func serve(c config) error {
	u, err := url.Parse(c.Server)
	if err != nil {
		return err
	}
	u.Scheme = strings.Replace(u.Scheme, "http", "ws", 1)
	u.Path = "/local/ws"

	header := http.Header{"Authorization": []string{"Bearer " + c.Token}}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("connected to %s as %q", c.Server, c.Name)

	// Liveness: the server pings every 30s and expects a pong. The
	// client must answer a ping the instant it arrives, on a
	// connection it owns for the life of the link, or the server
	// reads the silence as a dead socket and closes it. So the
	// pong is written straight from the ping handler, and the
	// client carries no read deadline of its own: an idle but
	// healthy link is not the client's call to give up on.
	conn.SetPingHandler(func(appData string) error {
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
			return err
		}
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
	maxBackoff    = 30 * time.Second
	stableWindow  = 60 * time.Second
	writeTimeout  = 10 * time.Second
	execOutputCap = 64 * 1024
	fileReadCap   = 512 * 1024
	fetchCap      = 256 * 1024
)

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
	default:
		out.OK, out.Err = false, "unknown operation: "+op.Kind
	}
	return out
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
