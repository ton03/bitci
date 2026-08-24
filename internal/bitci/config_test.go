package bitci

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestConfigContract(t *testing.T) {
	configPath := writeConfig(t, `{
		"version": 1,
		"resources": {"browser": 1},
		"tasks": {
			"unit": {"run": ["unit"], "paths": ["src/**"]},
			"browser": {"run": ["browser"], "needs": ["unit"], "resources": ["browser"], "paths": ["e2e/**"]}
		}
	}`)
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := config.Plan([]string{"e2e/login_test.go"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(plan, ","), "unit,browser"; got != want {
		t.Fatalf("plan = %q, want %q", got, want)
	}
	if _, err := LoadConfig(writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["x"],"raw":"no"}}}`)); err == nil {
		t.Fatal("unknown config key passed")
	}
}

func TestStackExamplesValidate(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	for _, name := range []string{"go-backend", "node-backend", "nx-monorepo"} {
		if _, err := LoadConfig(filepath.Join(root, "examples", name+".bitci.json")); err != nil {
			t.Fatalf("%s example: %v", name, err)
		}
	}
}

func TestDefaultStateDirStaysOutsideCheckout(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "bitci.json")
	stateDir := DefaultStateDir(configPath, "")
	if strings.HasPrefix(stateDir, filepath.Dir(configPath)+string(filepath.Separator)) {
		t.Fatalf("state directory %q is inside checkout", stateDir)
	}
}

func TestOpenRejectsStateInsideGitMetadata(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	controller, err := Open(configPath, filepath.Join(checkout, ".git"))
	if err == nil {
		controller.Close()
		t.Fatal("opened state inside Git metadata")
	}
	if !strings.Contains(err.Error(), "state directory must not overlap Git metadata") {
		t.Fatalf("Open error = %v", err)
	}
}

func TestOpenStateRejectsStateInsideGitMetadata(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	controller, err := OpenState(configPath, filepath.Join(checkout, ".git"))
	if err == nil {
		controller.Close()
		t.Fatal("opened state inside Git metadata")
	}
	if !strings.Contains(err.Error(), "state directory must not overlap Git metadata") {
		t.Fatalf("OpenState error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(checkout, ".git", "bitci.db")); !os.IsNotExist(err) {
		t.Fatalf("created rejected state database: %v", err)
	}
}

func TestOpenRejectsStateInsideGitMetadataThroughUncreatedSymlinkDescendant(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	link := filepath.Join(checkout, "meta")
	if err := os.Symlink(".git", link); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(link, "new-state")
	controller, err := Open(configPath, stateDir)
	if err == nil {
		controller.Close()
		t.Fatal("opened state inside Git metadata through a symlink")
	}
	if !strings.Contains(err.Error(), "state directory must not overlap Git metadata") {
		t.Fatalf("Open error = %v", err)
	}
	if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
		t.Fatalf("created rejected state directory: %v", err)
	}
}

func TestQueueContract(t *testing.T) {
	configPath := writeConfig(t, `{
		"version": 1,
		"resources": {"browser": 1},
		"tasks": {
			"unit": {"run": ["true"]},
			"browser": {"run": ["true"], "resources": ["browser"]}
		}
	}`)
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"browser", "unit"}, "abc")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := jobs[0].Task+","+jobs[1].Task, "browser,unit"; got != want {
		t.Fatalf("queue order = %q, want %q", got, want)
	}
	for range jobs {
		if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
			t.Fatalf("run once = %v, %v", ran, err)
		}
	}
	got, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range got {
		if job.State != "passed" {
			t.Fatalf("job %#v did not pass", job)
		}
	}
}

func TestResourceLeaseBlocksSecondClaim(t *testing.T) {
	configPath := writeConfig(t, `{
		"version": 1,
		"resources": {"browser": 1},
		"tasks": {"browser": {"run": ["true"], "resources": ["browser"]}}
	}`)
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"browser"}, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Submit([]string{"browser"}, "two"); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := controller.claim(2); err != nil || !claimed {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	if _, claimed, err := controller.claim(2); err != nil || claimed {
		t.Fatalf("second claim = %v, %v", claimed, err)
	}
}

func TestResourceLeaseUsesLowestActiveSnapshotLimit(t *testing.T) {
	configPath := writeConfig(t, `{
		"version": 1,
		"resources": {"browser": 1},
		"tasks": {"browser": {"run": ["true"], "resources": ["browser"]}}
	}`)
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"browser"}, "one"); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := controller.claim(2); err != nil || !claimed {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	newer := `{"version":1,"resources":{"browser":2},"tasks":{"browser":{"run":["true"],"resources":["browser"]}}}`
	if _, err := controller.db.Exec("INSERT INTO jobs(batch, task, ref, config_json, state, created_at) VALUES ('newer', 'browser', 'two', ?, 'queued', ?)", newer, time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := controller.claim(2); err != nil || claimed {
		t.Fatalf("newer snapshot bypassed held limit = %v, %v", claimed, err)
	}
}

func TestRecoverInterruptedReleasesLease(t *testing.T) {
	configPath := writeConfig(t, `{
		"version": 1,
		"resources": {"browser": 1},
		"tasks": {
			"prepare": {"run": ["true"], "resources": ["browser"]},
			"post": {"run": ["true"], "needs": ["prepare"]}
		}
	}`)
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"post"}, "one"); err != nil {
		t.Fatal(err)
	}
	first, claimed, err := controller.claim(1)
	if err != nil || !claimed {
		t.Fatalf("first claim = %v, %v", claimed, err)
	}
	if err := controller.RecoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 125 {
		t.Fatalf("recovered job = %#v", jobs[0])
	}
	if jobs[0].ID != first.ID {
		t.Fatalf("recovered job ID = %d, want %d", jobs[0].ID, first.ID)
	}
	if jobs[1].State != "cancelled" {
		t.Fatalf("unfinished batch job = %#v, want cancelled", jobs[1])
	}
	if _, err := controller.Submit([]string{"prepare"}, "two"); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := controller.claim(1); err != nil || !claimed {
		t.Fatalf("claim after recovery = %v, %v", claimed, err)
	}
}

func TestRecoverInterruptedRemovesJobWorktree(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := controller.claim(1); err != nil || !claimed {
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	path := filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", jobs[0].ID))
	if _, err := controller.git(context.Background(), "worktree", "add", "--detach", path, jobs[0].Ref); err != nil {
		t.Fatal(err)
	}
	if err := controller.RecoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("interrupted worktree remains: %v", err)
	}
}

func TestRecoverInterruptedRemovesWorktreeAfterCheckoutDisappears(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		controller.Close()
		t.Fatal(err)
	}
	if _, claimed, err := controller.claim(1); err != nil || !claimed {
		controller.Close()
		t.Fatalf("claim = %v, %v", claimed, err)
	}
	path := filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", jobs[0].ID))
	if _, err := controller.git(context.Background(), "worktree", "add", "--detach", path, jobs[0].Ref); err != nil {
		controller.Close()
		t.Fatal(err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	movedCheckout := checkout + "-moved"
	if err := os.Rename(checkout, movedCheckout); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(checkout, 0o700); err != nil {
		t.Fatal(err)
	}
	git(t, filepath.Dir(checkout), "init", "-q")
	controller, err = OpenState(configPath, filepath.Join(filepath.Dir(path), ".."))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.RecoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("worktree remains after replaced checkout: %v", err)
	}
	var pending int
	if err := controller.db.QueryRow("SELECT cleanup_pending FROM jobs WHERE id = ?", jobs[0].ID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("cleanup pending after replaced checkout = %d", pending)
	}
}

func TestRecoverInterruptedRemovesPendingWorktree(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", jobs[0].ID))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.git(context.Background(), "worktree", "add", "--detach", path, jobs[0].Ref); err != nil {
		t.Fatal(err)
	}
	if err := controller.finish(jobs[0], 125, true); err != nil {
		t.Fatal(err)
	}
	if err := controller.RecoverInterrupted(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("pending worktree remains: %v", err)
	}
	var pending int
	if err := controller.db.QueryRow("SELECT cleanup_pending FROM jobs WHERE id = ?", jobs[0].ID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("cleanup pending = %d, want 0", pending)
	}
}

func TestOpenStateMigratesLegacyJobs(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", filepath.Join(stateDir, "bitci.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`CREATE TABLE jobs (id INTEGER PRIMARY KEY, batch TEXT NOT NULL, task TEXT NOT NULL, ref TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, exit_code INTEGER, log_path TEXT); INSERT INTO jobs(batch, task, ref, state, created_at) VALUES ('batch', 'unit', 'ref', 'passed', '2026-01-01T00:00:00Z');`)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	controller, err := OpenState(filepath.Join(t.TempDir(), "bitci.json"), stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].TestedSHA != "" {
		t.Fatalf("migrated jobs = %#v", jobs)
	}
}

func TestOwnerSocketRPCAndStaleRecovery(t *testing.T) {
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["true"]}}}`)
	stateDir := filepath.Join(t.TempDir(), "state")
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	socketPath := fmt.Sprintf("/tmp/bitci-%d.sock", time.Now().UnixNano())
	defer os.Remove(socketPath)
	raw, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	raw.SetUnlinkOnClose(false)
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := controller.Listen(socketPath)
	if err != nil {
		t.Fatalf("recover stale socket: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- controller.ServeRPC(ctx, listener) }()
	var jobs []Job
	if err := Call(socketPath, "status", struct{}{}, &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("jobs = %#v, want none", jobs)
	}
	cancel()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("RPC server did not stop")
	}
}

func TestDuplicateServeDoesNotRecoverRunningJobs(t *testing.T) {
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["true"]}}}`)
	stateDir := filepath.Join(t.TempDir(), "state")
	owner, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	socketPath := fmt.Sprintf("/tmp/bitci-%d.sock", time.Now().UnixNano())
	defer os.Remove(socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- owner.Serve(ctx, 1, time.Hour, socketPath) }()
	deadline := time.Now().Add(time.Second)
	for {
		var jobs []Job
		if Call(socketPath, "status", struct{}{}, &jobs) == nil {
			break
		}
		select {
		case err := <-ownerDone:
			t.Fatalf("owner Serve error = %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("owner did not accept status requests")
		}
		time.Sleep(time.Millisecond)
	}
	result, err := owner.db.Exec("INSERT INTO jobs(batch, task, ref, state, created_at) VALUES (?, ?, ?, 'running', ?)", "batch", "unit", strings.Repeat("a", 40), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Close()
	if err := duplicate.Serve(context.Background(), 1, time.Hour, socketPath); err == nil || !strings.Contains(err.Error(), "already owns socket") {
		t.Fatalf("duplicate Serve error = %v", err)
	}
	var state string
	if err := owner.db.QueryRow("SELECT state FROM jobs WHERE id = ?", id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "running" {
		t.Fatalf("duplicate serve recovered live job as %q", state)
	}
	cancel()
	select {
	case err := <-ownerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner did not stop")
	}
}

func TestMCPReadOnlyTools(t *testing.T) {
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["true"]}}}`)
	stateDir := filepath.Join(t.TempDir(), "state")
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	socketPath := fmt.Sprintf("/tmp/bitci-%d.sock", time.Now().UnixNano())
	defer os.Remove(socketPath)
	listener, err := controller.Listen(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- controller.ServeRPC(ctx, listener) }()
	readOnly := mcpToolNames(t, socketPath, false)
	for _, name := range []string{"status", "plan", "tail_logs", "search_logs", "doctor"} {
		if !readOnly[name] {
			t.Fatalf("MCP tool %q missing", name)
		}
	}
	for _, name := range []string{"submit", "cancel", "retry"} {
		if readOnly[name] {
			t.Fatalf("read-only MCP exposed %q", name)
		}
	}
	for _, name := range []string{"submit", "cancel", "retry"} {
		if !mcpToolNames(t, socketPath, true)[name] {
			t.Fatalf("run-control MCP tool %q missing", name)
		}
	}
	cancel()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ownerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner RPC server did not stop")
	}
}

func TestMCPStatusIncludesTestedSHA(t *testing.T) {
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["true"]}}}`)
	stateDir := filepath.Join(t.TempDir(), "state")
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	sha := "0123456789012345678901234567890123456789"
	if _, err := controller.db.Exec("INSERT INTO jobs(batch, task, ref, tested_sha, state, created_at) VALUES ('batch', 'unit', ?, ?, 'passed', '2026-01-01T00:00:00Z')", sha, sha); err != nil {
		t.Fatal(err)
	}
	socketPath := fmt.Sprintf("/tmp/bitci-%d.sock", time.Now().UnixNano())
	defer os.Remove(socketPath)
	listener, err := controller.Listen(socketPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- controller.ServeRPC(ctx, listener) }()
	status := mcpStatus(t, socketPath)
	if len(status.Jobs) != 1 || status.Jobs[0].TestedSHA != sha {
		t.Fatalf("MCP status = %#v", status)
	}
	cancel()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ownerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("owner RPC server did not stop")
	}
}

func mcpStatus(t *testing.T, socketPath string) MCPStatus {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=TestMCPHelper", "--")
	command.Env = append(os.Environ(), "GO_WANT_MCP_HELPER=1", "BITCI_TEST_SOCKET="+socketPath, "BITCI_TEST_ALLOW_RUNS=0")
	client := mcp.NewClient(&mcp.Implementation{Name: "bitci-test", Version: "0.1.0"}, nil)
	session, err := client.Connect(context.Background(), &mcp.CommandTransport{Command: command}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "status"})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.GetError(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var status MCPStatus
	if err := json.Unmarshal(encoded, &status); err != nil {
		t.Fatal(err)
	}
	return status
}

func mcpToolNames(t *testing.T, socketPath string, allowRuns bool) map[string]bool {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=TestMCPHelper", "--")
	allowRunsValue := "0"
	if allowRuns {
		allowRunsValue = "1"
	}
	command.Env = append(os.Environ(), "GO_WANT_MCP_HELPER=1", "BITCI_TEST_SOCKET="+socketPath, "BITCI_TEST_ALLOW_RUNS="+allowRunsValue)
	client := mcp.NewClient(&mcp.Implementation{Name: "bitci-test", Version: "0.1.0"}, nil)
	session, err := client.Connect(context.Background(), &mcp.CommandTransport{Command: command}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "status"})
	if err != nil {
		t.Fatal(err)
	}
	if err := result.GetError(); err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool, len(tools.Tools))
	for _, tool := range tools.Tools {
		names[tool.Name] = true
	}
	return names
}

func TestConfiguredArgvDoesNotUseShell(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "shell-ran")
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["`+os.Args[0]+`","-test.run=TestHelperProcess","--","hello;touch `+sentinel+`"]}}}`)
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
		t.Fatalf("shell interpreted configured argv: %v", err)
	}
}

func TestRunControlAndLogs(t *testing.T) {
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["printf","first\\nerror second\\nthird"]}}}`)
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	queued, err := controller.Submit([]string{"unit"}, "old")
	if err != nil {
		t.Fatal(err)
	}
	if cancelled, err := controller.Cancel(queued[0].ID); err != nil || !cancelled {
		t.Fatalf("cancel = %v, %v", cancelled, err)
	}
	retried, err := controller.Retry(queued[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	lines, err := controller.TailLog(retried[0].ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(lines, ","), "error second,third"; got != want {
		t.Fatalf("tail = %q, want %q", got, want)
	}
	lines, err = controller.SearchLog(retried[0].ID, "error", 80)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(lines, ","), "error second"; got != want {
		t.Fatalf("search = %q, want %q", got, want)
	}
}

func TestRetryPreservesRecordedSHAAndConfiguration(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	initialConfig := `{"version":1,"tasks":{"unit":{"run":["sh","-c","test \"$(cat marker)\" = initial"]}}}`
	if err := os.WriteFile(configPath, []byte(initialConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "marker"), []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	stateDir := filepath.Join(t.TempDir(), "state")
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	original, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["false"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "marker"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "changed")
	controller, err = Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	retried, err := controller.Retry(original[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried[0].Ref != original[0].Ref || retried[0].checkoutRoot != original[0].checkoutRoot || retried[0].configRelative != original[0].configRelative {
		t.Fatalf("retry changed recorded checkout: %#v", retried[0])
	}
	if cancelled, err := controller.Cancel(original[0].ID); err != nil || !cancelled {
		t.Fatalf("cancel original = %v, %v", cancelled, err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run retry = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[1].State != "passed" {
		t.Fatalf("retried job = %#v", jobs[1])
	}
}

func TestRetryPinsLegacyRecordedSHA(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	original, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.db.Exec("UPDATE jobs SET checkout_root = '', config_relative = '' WHERE id = ?", original[0].ID); err != nil {
		t.Fatal(err)
	}
	retried, err := controller.Retry(original[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	pinned := git(t, checkout, "rev-parse", "--verify", "refs/bitci/jobs/"+retried[0].Batch+"^{commit}")
	if pinned != original[0].Ref {
		t.Fatalf("pinned retry SHA = %q, want %q", pinned, original[0].Ref)
	}
}

func TestLiveLogAvailableBeforeJobFinishes(t *testing.T) {
	releasePath := filepath.Join(t.TempDir(), "release")
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	t.Setenv("GO_WANT_LIVE_LOG_RELEASE", releasePath)
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["`+os.Args[0]+`","-test.run=TestHelperProcess","--"]}}}`)
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := controller.RunOnce(context.Background(), 1)
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		lines, err := controller.TailLog(jobs[0].ID, 80)
		if err == nil && strings.Contains(strings.Join(lines, "\n"), "BitCI live log ready") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("live log unavailable: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(releasePath, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestJobRunsInRecordedCheckoutSHA(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["sh","-c","test \"$(cat marker)\" = initial"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "marker"), []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "marker")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	sha := git(t, checkout, "rev-parse", "HEAD")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"unit"}, "untrusted input")
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].Ref != sha {
		t.Fatalf("ref = %q, want verified SHA %q", jobs[0].Ref, sha)
	}
	if err := os.WriteFile(filepath.Join(checkout, "marker"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "add", "marker")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "changed")
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	got, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if got[0].State != "passed" || got[0].TestedSHA != sha {
		lines, _ := controller.TailLog(got[0].ID, 80)
		t.Fatalf("recorded checkout job = %#v\n%s", got[0], strings.Join(lines, "\n"))
	}
	if _, err := os.Stat(filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", jobs[0].ID))); !os.IsNotExist(err) {
		t.Fatalf("job worktree remains: %v", err)
	}
}

func TestJobRunsInRecordedSHA256Checkout(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("git", "-C", checkout, "init", "--object-format=sha256", "-q")
	if output, err := command.CombinedOutput(); err != nil {
		t.Skipf("Git does not support SHA-256 repositories: %s", output)
	}
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	sha := git(t, checkout, "rev-parse", "HEAD")
	if len(sha) != 64 {
		t.Fatalf("SHA-256 checkout SHA length = %d, want 64", len(sha))
	}
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].Ref != sha {
		t.Fatalf("ref = %q, want recorded SHA-256 %q", jobs[0].Ref, sha)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	got, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if got[0].State != "passed" || got[0].TestedSHA != sha {
		t.Fatalf("SHA-256 checkout job = %#v", got[0])
	}
}

func TestSubmitPinsRecordedCheckoutSHA(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	pinned := git(t, checkout, "rev-parse", "--verify", "refs/bitci/jobs/"+jobs[0].Batch+"^{commit}")
	if pinned != jobs[0].Ref {
		t.Fatalf("pinned SHA = %q, want %q", pinned, jobs[0].Ref)
	}
}

func TestPathWithinUsesFilesystemIdentity(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "BitCI")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "bitci")
	if _, err := os.Stat(alias); os.IsNotExist(err) {
		t.Skip("case-sensitive filesystem")
	} else if err != nil {
		t.Fatal(err)
	}
	if !pathWithin(root, filepath.Join(alias, "missing")) {
		t.Fatal("case alias should be inside checkout")
	}
	if relative, ok := relativeWithin(root, filepath.Join(alias, "state")); !ok || relative != "state" {
		t.Fatalf("relative case alias = %q, %v", relative, ok)
	}
}

func TestRecordedSHAPreparesEachWorktree(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"prepare":["sh","bootstrap"],"tasks":{"unit":{"run":["sh","runner"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "bootstrap"), []byte(": > ready\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "runner"), []byte("test -f ready\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", ".gitignore", "bootstrap", "runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "passed" {
		lines, _ := controller.TailLog(jobs[0].ID, 80)
		t.Fatalf("prepared checkout job = %#v\n%s", jobs[0], strings.Join(lines, "\n"))
	}
	if _, err := os.Stat(filepath.Join(checkout, "ready")); !os.IsNotExist(err) {
		t.Fatalf("prepare wrote mutable checkout: %v", err)
	}
}

func TestRecordedSHARechecksTaskExecutableAfterPrepare(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	primaryExecutable := filepath.Join(checkout, "escape")
	prepare, err := json.Marshal([]string{"sh", "-c", fmt.Sprintf("ln -sf %q runner", primaryExecutable)})
	if err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"version":1,"prepare":%s,"tasks":{"unit":{"run":["./runner"]}}}`, prepare)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "runner"), []byte("exit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	if err := os.WriteFile(primaryExecutable, []byte("touch escaped\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		lines, _ := controller.TailLog(jobs[0].ID, 80)
		t.Fatalf("replaced executable job = %#v\n%s", jobs[0], strings.Join(lines, "\n"))
	}
	if _, err := os.Stat(filepath.Join(checkout, "escaped")); !os.IsNotExist(err) {
		t.Fatalf("task executed mutable checkout target: %v", err)
	}
}

func TestJobRunsFromNestedConfigDirectoryWithRelativeState(t *testing.T) {
	checkout := t.TempDir()
	configDir := filepath.Join(checkout, "ci")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(configDir, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["sh","-c","test \"$(cat marker)\" = nested"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "marker"), []byte("nested"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "ci")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "nested")
	originalDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	stateParent := t.TempDir()
	if err := os.Chdir(stateParent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(originalDirectory) })
	controller, err := Open(configPath, "state")
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if !filepath.IsAbs(controller.stateDir) {
		t.Fatalf("state directory is relative: %q", controller.stateDir)
	}
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "passed" {
		lines, _ := controller.TailLog(jobs[0].ID, 80)
		t.Fatalf("nested config job = %#v\n%s", jobs[0], strings.Join(lines, "\n"))
	}
}

func TestOpenCapturesRelativeNestedConfig(t *testing.T) {
	checkout := t.TempDir()
	configDir := filepath.Join(checkout, "ci")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "bitci.json"), []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "ci")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	originalDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(checkout); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(originalDirectory)
	controller, err := Open(filepath.Join("ci", "bitci.json"), filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if controller.configRelative != "ci" {
		t.Fatalf("config relative = %q", controller.configRelative)
	}
}

func TestRecordedSHAUsesSubmittedCheckoutLocation(t *testing.T) {
	checkout := t.TempDir()
	configDir := filepath.Join(checkout, "ci")
	configPath := filepath.Join(configDir, "bitci.json")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["sh","-c","test \"$(cat marker)\" = initial"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "marker"), []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "ci")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "rm", "-qr", "ci")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=BitCI@example.test", "commit", "-qm", "remove config")
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "passed" {
		lines, _ := controller.TailLog(jobs[0].ID, 80)
		t.Fatalf("captured checkout job = %#v\n%s", jobs[0], strings.Join(lines, "\n"))
	}
}

func TestRecordedSHAAllowsWorktreeExecutableInsideCheckoutState(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".bitci/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["./runner"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "runner"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(checkout, ".bitci"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "passed" {
		lines, _ := controller.TailLog(jobs[0].ID, 80)
		t.Fatalf("in-checkout state job = %#v\n%s", jobs[0], strings.Join(lines, "\n"))
	}
}

func TestJobCheckoutFailureDoesNotPanicOnCleanup(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", jobs[0].ID)), 0o700); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	stored, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if stored[0].State != "failed" || stored[0].ExitCode == nil || *stored[0].ExitCode != 126 {
		t.Fatalf("failed checkout job = %#v", stored[0])
	}
	if _, err := os.Lstat(filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", jobs[0].ID))); !os.IsNotExist(err) {
		t.Fatalf("failed checkout worktree remains: %v", err)
	}
}

func TestQueuedJobUsesSubmittedConfiguration(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	initial := `{"version":1,"tasks":{"unit":{"run":["sh","-c","test \"$(cat marker)\" = initial"]}}}`
	if err := os.WriteFile(configPath, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "marker"), []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "marker")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	stateDir := filepath.Join(t.TempDir(), "state")
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		controller.Close()
		t.Fatal(err)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["false"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	controller, err = Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "passed" {
		t.Fatalf("submitted configuration job = %#v", jobs[0])
	}
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run current config = %v, %v", ran, err)
	}
	jobs, err = controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[1].State != "failed" {
		t.Fatalf("current configuration job = %#v", jobs[1])
	}
}

func TestQueuedJobUsesSubmittedDiskGuard(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"min_free_bytes":18446744073709551615,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		controller.Close()
		t.Fatal(err)
	}
	stateDir := controller.stateDir
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	controller, err = Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if ran, err := controller.RunOnce(context.Background(), 1); ran || err == nil || !strings.Contains(err.Error(), "disk guard") {
		t.Fatalf("run with submitted disk guard = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "queued" {
		t.Fatalf("disk-guarded job = %#v", jobs[0])
	}
}

func TestRecordedSHARejectsCheckoutAbsoluteExecutable(t *testing.T) {
	checkout := t.TempDir()
	runner := filepath.Join(checkout, "runner")
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(`{"version":1,"tasks":{"unit":{"run":[%q]}}}`, runner)), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("absolute executable job = %#v", jobs[0])
	}
}

func TestRecordedSHARejectsInterpreterScriptFromSubmittedCheckout(t *testing.T) {
	checkout := t.TempDir()
	runner := filepath.Join(checkout, "runner")
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(runner, []byte("touch escaped\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(`{"version":1,"tasks":{"unit":{"run":["sh",%q]}}}`, runner)), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("interpreter script job = %#v", jobs[0])
	}
	if _, err := os.Stat(filepath.Join(checkout, "escaped")); !os.IsNotExist(err) {
		t.Fatalf("task executed mutable checkout script: %v", err)
	}
}

func TestRecordedSHARejectsRelativeInterpreterScriptSymlink(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	primaryScript := filepath.Join(checkout, "primary-script")
	if err := os.Mkdir(filepath.Join(checkout, "scripts"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(primaryScript, []byte("touch escaped\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(primaryScript, filepath.Join(checkout, "scripts", "runner")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["sh","scripts/runner"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "scripts/runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("relative interpreter script job = %#v", jobs[0])
	}
	if _, err := os.Stat(filepath.Join(checkout, "escaped")); !os.IsNotExist(err) {
		t.Fatalf("task executed mutable checkout script: %v", err)
	}
}

func TestRecordedSHARejectsExecutableFromSubmittedCheckout(t *testing.T) {
	checkout := t.TempDir()
	runner := filepath.Join(checkout, "runner")
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(`{"version":1,"tasks":{"unit":{"run":[%q]}}}`, runner)), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	controller.checkoutRoot = t.TempDir()
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("submitted checkout executable job = %#v", jobs[0])
	}
}

func TestRecordedSHARejectsCheckoutPathExecutable(t *testing.T) {
	checkout := t.TempDir()
	runner := filepath.Join(checkout, "runner")
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(runner, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["runner"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	t.Setenv("PATH", checkout+string(os.PathListSeparator)+os.Getenv("PATH"))
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("PATH executable job = %#v", jobs[0])
	}
}

func TestRecordedSHARejectsCheckoutSymlinkExecutable(t *testing.T) {
	checkout := t.TempDir()
	runner := filepath.Join(checkout, "runner")
	truePath, err := exec.LookPath("true")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(truePath, runner); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(`{"version":1,"tasks":{"unit":{"run":[%q]}}}`, runner)), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "runner")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("symlink executable job = %#v", jobs[0])
	}
}

func TestRecordedSHARejectsCheckoutSymlinkDirectoryExecutable(t *testing.T) {
	checkout := t.TempDir()
	tools := filepath.Join(checkout, "tools")
	truePath, err := exec.LookPath("true")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(truePath), tools); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(fmt.Sprintf(`{"version":1,"tasks":{"unit":{"run":[%q]}}}`, filepath.Join(tools, filepath.Base(truePath)))), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", "tools")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("symlink directory executable job = %#v", jobs[0])
	}
}

func TestRecordedSHARejectsRelativeExecutableOutsideWorktree(t *testing.T) {
	checkout := t.TempDir()
	configDir := filepath.Join(checkout, "ci")
	configPath := filepath.Join(configDir, "bitci.json")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["../../outside"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "outside"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "ci", "outside")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if ran, err := controller.RunOnce(context.Background(), 1); err != nil || !ran {
		t.Fatalf("run once = %v, %v", ran, err)
	}
	jobs, err := controller.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	if jobs[0].State != "failed" || jobs[0].ExitCode == nil || *jobs[0].ExitCode != 126 {
		t.Fatalf("relative executable job = %#v", jobs[0])
	}
}

func TestNestedConfigChecksWholeCheckout(t *testing.T) {
	checkout := t.TempDir()
	configDir := filepath.Join(checkout, "ci")
	configPath := filepath.Join(configDir, "bitci.json")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".bitci[[]local]/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(checkout, ".bitci[local]"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.cleanCheckout(context.Background()); err != nil {
		t.Fatalf("clean nested checkout = %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "outside"), []byte("dirty"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := controller.cleanCheckout(context.Background()); err == nil {
		t.Fatal("nested config missed root-level checkout dirt")
	}
}

func TestCleanCheckoutRejectsTrackedStateFiles(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".bitci/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(checkout, ".bitci")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "tracked"), []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json", ".gitignore")
	git(t, checkout, "add", "-f", ".bitci/tracked")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.cleanCheckout(context.Background()); err == nil || !strings.Contains(err.Error(), "tracked files") {
		t.Fatalf("tracked state checkout error = %v", err)
	}
}

func TestStageProtectsStateFromTargetTree(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	stateDir := filepath.Join(checkout, ".bitci[local]")
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".bitci[[]local]/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := os.WriteFile(filepath.Join(stateDir, "poison"), []byte("tracked target"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "add", "-f", ".bitci[local]/poison")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "target state")
	sha := git(t, checkout, "rev-parse", "HEAD")
	if err := controller.protectStateFromTarget(context.Background(), sha); err == nil || !strings.Contains(err.Error(), "state files") {
		t.Fatalf("target state protection error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateDir, "bitci.db")); err != nil {
		t.Fatalf("BitCI state was replaced: %v", err)
	}
}

func TestCleanGeneratedNextUsesNestedConfigDirectory(t *testing.T) {
	checkout := t.TempDir()
	configDir := filepath.Join(checkout, "ci")
	configPath := filepath.Join(configDir, "bitci.json")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte("ci/.next/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	if err := os.MkdirAll(filepath.Join(configDir, ".next"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, ".next", "stale"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.cleanGeneratedNext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(configDir, ".next")); !os.IsNotExist(err) {
		t.Fatalf("nested generated files remain: %v", err)
	}
}

func TestCleanGeneratedNextRejectsStateOverlap(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".next/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	controller, err := Open(configPath, filepath.Join(checkout, ".next"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.cleanGeneratedNext(context.Background()); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("state overlap error = %v", err)
	}
}

func TestStageRejectsSymlinkedInCheckoutState(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".bitci/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	stateTarget := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(stateTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(checkout, ".bitci")
	if err := os.Symlink(stateTarget, stateDir); err != nil {
		t.Fatal(err)
	}
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if err := controller.cleanCheckout(context.Background()); err == nil || !strings.Contains(err.Error(), "must not use a symlink") {
		t.Fatalf("clean checkout error = %v", err)
	}
	sha := git(t, checkout, "rev-parse", "HEAD")
	if err := controller.protectStateFromTarget(context.Background(), sha); err == nil || !strings.Contains(err.Error(), "must not use a symlink") {
		t.Fatalf("target state protection error = %v", err)
	}
}

func TestStageProtectsStateWithSymlinkedConfigDirectory(t *testing.T) {
	checkout := t.TempDir()
	configDir := filepath.Join(checkout, "ci", "nested")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".bitci/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(configDir, "bitci.json"), []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	if err := os.Symlink(filepath.Join("ci", "nested"), filepath.Join(checkout, "alias")); err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(checkout, ".bitci")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "poison"), []byte("tracked target"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "add", "-f", ".bitci/poison")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "target state")
	controller, err := Open(filepath.Join(checkout, "alias", "bitci.json"), stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	sha := git(t, checkout, "rev-parse", "HEAD")
	if err := controller.protectStateFromTarget(context.Background(), sha); err == nil || !strings.Contains(err.Error(), "state files") {
		t.Fatalf("target state protection error = %v", err)
	}
}

func TestStagePRChecksTrustAndCleansNext(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".next/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "branch", "-M", "main")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "main")
	mainSHA := git(t, checkout, "rev-parse", "HEAD")
	remote := filepath.Join(t.TempDir(), "remote.git")
	git(t, t.TempDir(), "init", "--bare", "-q", remote)
	git(t, checkout, "remote", "add", "origin", remote)
	git(t, checkout, "push", "-q", "origin", "HEAD:refs/heads/main")
	if err := os.WriteFile(filepath.Join(checkout, "pr-file"), []byte("trusted"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "add", "pr-file")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "pr")
	prSHA := git(t, checkout, "rev-parse", "HEAD")
	git(t, checkout, "push", "-q", "origin", "HEAD:refs/pull/7/head")
	git(t, checkout, "reset", "--hard", "-q", mainSHA)
	if err := os.MkdirAll(filepath.Join(checkout, ".next", "types"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, ".next", "types", "stale"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing GitHub token")
		}
		fmt.Fprintf(writer, `{"head":{"sha":%q,"repo":{"full_name":"owner/repo"}},"base":{"repo":{"full_name":"owner/repo"}}}`, prSHA)
	}))
	defer server.Close()
	controller, err := Open(configPath, filepath.Join(checkout, ".bitci"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	controller.githubAPI = server.URL
	controller.githubRepo = "owner/repo"
	stage, err := controller.StagePR(context.Background(), 7, "test-token")
	if err != nil {
		t.Fatal(err)
	}
	if stage.SHA != prSHA {
		t.Fatalf("staged SHA = %q, want %q", stage.SHA, prSHA)
	}
	if _, err := os.Stat(filepath.Join(checkout, ".next")); !os.IsNotExist(err) {
		t.Fatalf("stale .next remains: %v", err)
	}
}

func TestStagePRRejectsFork(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "main")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(writer, `{"head":{"sha":"0123456789012345678901234567890123456789","repo":{"full_name":"fork/repo"}},"base":{"repo":{"full_name":"owner/repo"}}}`)
	}))
	defer server.Close()
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	controller.githubAPI = server.URL
	controller.githubRepo = "owner/repo"
	if _, err := controller.StagePR(context.Background(), 7, "test-token"); err == nil || !strings.Contains(err.Error(), "same-repository") {
		t.Fatalf("fork stage error = %v", err)
	}
}

func TestServicePlist(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("launchd applies on macOS")
	}
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["true"]}}}`)
	service, err := NewService(configPath, filepath.Join(t.TempDir(), "state"), 3)
	if err != nil {
		t.Fatal(err)
	}
	truePath, err := exec.LookPath("true")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(service.PathEnv, filepath.Dir(truePath)) {
		t.Fatalf("service PATH %q misses configured command directory", service.PathEnv)
	}
	plist := service.plist()
	for _, value := range []string{"<key>KeepAlive</key><true/>", "<string>serve</string>", service.ConfigPath, service.StateDir, "<key>EnvironmentVariables</key><dict><key>PATH</key>", service.PathEnv} {
		if !strings.Contains(plist, value) {
			t.Fatalf("plist missing %q", value)
		}
	}
}

func TestServicePathUsesPrepareExecutable(t *testing.T) {
	configPath := writeConfig(t, `{"version":1,"prepare":["sh","-c","true"],"tasks":{"unit":{"run":["true"]}}}`)
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	path, err := servicePath(config, filepath.Dir(configPath))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(path, filepath.Dir(sh)) {
		t.Fatalf("service PATH %q misses prepare command directory", path)
	}
}

func TestServicePathAllowsTaskExecutableCreatedByPrepare(t *testing.T) {
	checkout := t.TempDir()
	_, err := servicePath(Config{Prepare: []string{"true"}, Tasks: map[string]Task{"unit": {Run: []string{"./node_modules/.bin/unit"}}}}, checkout)
	if err != nil {
		t.Fatalf("service path error = %v", err)
	}
}

func TestServicePathRejectsMissingTaskExecutableWithoutPrepare(t *testing.T) {
	checkout := t.TempDir()
	_, err := servicePath(Config{Tasks: map[string]Task{"unit": {Run: []string{"./missing"}}}}, checkout)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service path error = %v", err)
	}
}

func TestServicePathReportsMissingPrepareCommand(t *testing.T) {
	checkout := t.TempDir()
	_, err := servicePath(Config{Prepare: []string{"./missing"}, Tasks: map[string]Task{"unit": {Run: []string{"true"}}}}, checkout)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("service path error = %v", err)
	}
}

func TestServiceRefusesActiveJobs(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("launchd applies on macOS")
	}
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["true"]}}}`)
	stateDir := filepath.Join(t.TempDir(), "state")
	service, err := NewService(configPath, stateDir, 1)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := Open(configPath, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if err := service.ensureNoActiveJobs(); err == nil {
		t.Fatal("service accepted queued job")
	}
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	if releasePath := os.Getenv("GO_WANT_LIVE_LOG_RELEASE"); releasePath != "" {
		fmt.Fprintln(os.Stdout, "BitCI live log ready")
		for {
			if _, err := os.Stat(releasePath); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	os.Exit(0)
}

func TestMCPHelper(t *testing.T) {
	if os.Getenv("GO_WANT_MCP_HELPER") != "1" {
		return
	}
	if err := RunMCP(context.Background(), MCPOptions{SocketPath: os.Getenv("BITCI_TEST_SOCKET"), AllowRuns: os.Getenv("BITCI_TEST_ALLOW_RUNS") == "1"}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bitci.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func git(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return strings.TrimSpace(string(output))
}
