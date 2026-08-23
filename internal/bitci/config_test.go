package bitci

import (
	"context"
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

func TestSubmitRecordsAndVerifiesCheckoutSHA(t *testing.T) {
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "add", "bitci.json")
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
	if got[0].State != "failed" || got[0].ExitCode == nil || *got[0].ExitCode != 126 {
		t.Fatalf("mismatched checkout job = %#v", got[0])
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
	controller, err := Open(configPath, filepath.Join(t.TempDir(), "state"))
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
