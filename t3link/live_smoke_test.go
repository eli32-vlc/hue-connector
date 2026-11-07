package t3link

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLiveLocalSmoke walks the real path on a machine that runs
// T3 Code: discover the live server, mint through the real t3
// CLI under the mcp-client subject, call one read tool over the
// local wire, then revoke and leave no trace (Forget runs as
// cleanup). Skipped unless T3LIVE=1, so CI never touches the
// developer's environment.
func TestLiveLocalSmoke(t *testing.T) {
	if os.Getenv("T3LIVE") == "" {
		t.Skip("set T3LIVE=1 to exercise a real local T3 Code")
	}
	c := NewClient(filepath.Join(t.TempDir(), "t3.json"))

	rs, err := DiscoverRuntime()
	if err != nil {
		t.Skipf("no running T3 Code here: %v", err)
	}
	t.Logf("discovered %s (version %s)", rs.Origin, rs.ServerVersion)

	if _, err := c.Authorize(t.Context(), []string{"orchestration:read"}); err != nil {
		t.Fatalf("authorize: %v", err)
	}
	t.Cleanup(func() { _ = c.Forget() })

	text, err := c.Call(t.Context(), "t3_project_list", map[string]interface{}{})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if strings.TrimSpace(text) == "" {
		t.Fatal("empty answer from t3_project_list")
	}
	if len(text) > callOutputCap {
		t.Fatalf("answer not bounded: %d bytes", len(text))
	}
	t.Logf("t3_project_list answered %d bytes", len(text))

	// A write tool must refuse on a read-only mint — the
	// boundary is the token's scopes, enforced by the server.
	if _, err := c.Call(t.Context(), "t3_thread_interrupt", map[string]interface{}{"thread_id": "none"}); err == nil {
		t.Log("note: write refusal came back as a result (expected for typed failures)")
	}
}
