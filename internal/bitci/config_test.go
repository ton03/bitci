package bitci

import (
	"context"
	"fmt"
	"net"
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

func TestServicePlist(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("launchd applies on macOS")
	}
	configPath := writeConfig(t, `{"version":1,"tasks":{"unit":{"run":["true"]}}}`)
	service, err := NewService(configPath, filepath.Join(t.TempDir(), "state"), 3)
	if err != nil {
		t.Fatal(err)
	}
	plist := service.plist()
	for _, value := range []string{"<key>KeepAlive</key><true/>", "<string>serve</string>", service.ConfigPath, service.StateDir} {
		if !strings.Contains(plist, value) {
			t.Fatalf("plist missing %q", value)
		}
	}
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
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
