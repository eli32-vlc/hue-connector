package main

import "testing"

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
