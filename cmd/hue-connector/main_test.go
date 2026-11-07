package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Any command that prints something goes through cappedBuffer.
// A Write that recursed instead of reaching the embedded Builder
// overflowed the stack and killed the whole process on the first
// byte of output, so every real operation took the bridge down
// with it. This pins the fix: output is captured, the cap holds,
// and the process survives.
func TestExecuteCapturesOutput(t *testing.T) {
	out := execute(t.Context(), wire{
		Type: "op", ID: "1", Kind: "exec",
		Args: map[string]string{"command": "printf 'hello world'", "timeout": "10"},
	})
	if !out.OK {
		t.Fatalf("exec failed: %s", out.Err)
	}
	if out.Output != "hello world" {
		t.Fatalf("output = %q, want %q", out.Output, "hello world")
	}
}

// Output past the cap is truncated, not recursed on.
func TestCappedBufferTruncates(t *testing.T) {
	b := &cappedBuffer{cap: 8}
	n, err := b.Write([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 16 {
		t.Fatalf("n = %d, want 16 so the writer never retries", n)
	}
	if got := b.String(); got != "01234567" {
		t.Fatalf("stored %q, want the first 8 bytes", got)
	}
}

// An export hands back the file's bytes, base64, so a binary
// file survives the JSON frame with nothing lost.
func TestExecuteExportIsBinarySafe(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blob.bin")
	// A NUL, a high byte and a newline: everything a text round
	// trip would mangle.
	body := []byte{0x00, 0xff, 0x0a, 'a', 'b'}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}

	out := execute(t.Context(), wire{
		Type: "op", ID: "1", Kind: "export",
		Args: map[string]string{"path": path},
	})
	if !out.OK {
		t.Fatalf("export failed: %s", out.Err)
	}
	got, err := base64.StdEncoding.DecodeString(out.Output)
	if err != nil {
		t.Fatalf("output is not base64: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("bytes = %v, want %v", got, body)
	}
}

// A file past the bound is refused by name before any of it
// crosses: a truncated copy would land in the sandbox looking
// complete.
func TestExportRefusesOversize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.bin")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 2048), 0o644); err != nil {
		t.Fatal(err)
	}

	out := execute(t.Context(), wire{
		Type: "op", ID: "1", Kind: "export",
		Args: map[string]string{"path": path, "max": "1024"},
	})
	if out.OK {
		t.Fatal("an oversize file was exported")
	}
	if !strings.Contains(out.Err, "limit is 1024") {
		t.Fatalf("err = %q, want the limit named", out.Err)
	}
	if out.Output != "" {
		t.Fatalf("partial payload crossed: %d bytes", len(out.Output))
	}
}

// A folder is refused, not read as a zero-byte file.
func TestExportRefusesDirectory(t *testing.T) {
	out := execute(t.Context(), wire{
		Type: "op", ID: "1", Kind: "export",
		Args: map[string]string{"path": t.TempDir()},
	})
	if out.OK || !strings.Contains(out.Err, "folder") {
		t.Fatalf("out = %+v", out)
	}
}

// A missing file reports the reason instead of an empty success.
func TestExportMissingFile(t *testing.T) {
	out := execute(t.Context(), wire{
		Type: "op", ID: "1", Kind: "export",
		Args: map[string]string{"path": filepath.Join(t.TempDir(), "nope.bin")},
	})
	if out.OK || out.Err == "" {
		t.Fatalf("out = %+v", out)
	}
}

// The bound the server asks for is honoured, and an absent or
// nonsensical one falls back to the connector's own cap rather
// than to no limit at all.
func TestExportHonoursMax(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mid.bin")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, max := range []string{"", "0", "-5", "banana"} {
		out := execute(t.Context(), wire{
			Type: "op", ID: "1", Kind: "export",
			Args: map[string]string{"path": path, "max": max},
		})
		if !out.OK {
			t.Fatalf("max %q: export failed: %s", max, out.Err)
		}
	}
}
