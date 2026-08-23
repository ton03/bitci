package bitci

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
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

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bitci.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
