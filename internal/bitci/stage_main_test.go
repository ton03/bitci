package bitci

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func stageMainFixture(t *testing.T) (*Controller, string, string, string) {
	t.Helper()
	checkout := t.TempDir()
	configPath := filepath.Join(checkout, "bitci.json")
	config := `{"version":1,"tasks":{"unit":{"run":["true"]}}}`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, ".gitignore"), []byte(".next/\n.cache/\n.bitci/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "marker"), []byte("initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "init", "-q")
	git(t, checkout, "branch", "-M", "main")
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "initial")
	initialSHA := git(t, checkout, "rev-parse", "HEAD")

	remote := filepath.Join(t.TempDir(), "origin.git")
	git(t, t.TempDir(), "init", "--bare", "-q", remote)
	git(t, checkout, "remote", "add", "origin", remote)
	git(t, checkout, "push", "-q", "origin", "HEAD:refs/heads/main")

	if err := os.WriteFile(configPath, []byte(`{"version":1,"tasks":{"unit":{"run":["true"]},"main-only":{"run":["true"]}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkout, "main-only"), []byte("trusted main"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "add", ".")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "main update")
	mainSHA := git(t, checkout, "rev-parse", "HEAD")
	git(t, checkout, "push", "-q", "origin", "HEAD:refs/heads/main")
	git(t, checkout, "reset", "--hard", "-q", initialSHA)

	controller, err := Open(configPath, filepath.Join(checkout, ".bitci"))
	if err != nil {
		t.Fatal(err)
	}
	controller.githubRepo = "owner/repo"
	t.Cleanup(func() { _ = controller.Close() })
	return controller, checkout, initialSHA, mainSHA
}

func TestStageMainFetchesExactMainAndCleansOnlyGeneratedNext(t *testing.T) {
	controller, checkout, originalSHA, mainSHA := stageMainFixture(t)
	for _, path := range []string{".next/types/stale", ".cache/keep"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(checkout, path)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(checkout, path), []byte("preserve"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	stage, err := controller.StageMain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stage.Ref != "main" || stage.SHA != mainSHA {
		t.Fatalf("stage = %#v, want main at %s", stage, mainSHA)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != mainSHA || got == originalSHA {
		t.Fatalf("checkout SHA = %s, want fetched main %s", got, mainSHA)
	}
	if _, err := os.Stat(filepath.Join(checkout, ".next")); !os.IsNotExist(err) {
		t.Fatalf("generated .next remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(checkout, ".cache", "keep")); err != nil {
		t.Fatalf("unrelated ignored file was removed: %v", err)
	}
	if staged, err := controller.stagedCheckoutSHA(); err != nil || staged != mainSHA {
		t.Fatalf("staged SHA = %q, %v", staged, err)
	}
	var sourceRef string
	if err := controller.db.QueryRow("SELECT source_ref FROM staged_checkouts WHERE id = 1").Scan(&sourceRef); err != nil || sourceRef != "refs/heads/main" {
		t.Fatalf("staged source ref = %q, %v", sourceRef, err)
	}
	jobs, err := controller.Submit([]string{"main-only"}, "")
	if err != nil || len(jobs) != 1 || jobs[0].Ref != mainSHA || jobs[0].SubmittedRef != mainSHA {
		t.Fatalf("submit staged main = %#v, %v", jobs, err)
	}
}

func TestStageMainRejectsDirtyCheckoutWithoutChangingIt(t *testing.T) {
	controller, checkout, originalSHA, _ := stageMainFixture(t)
	dirty := filepath.Join(checkout, "untracked.txt")
	if err := os.WriteFile(dirty, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.StageMain(context.Background()); err == nil || !strings.Contains(err.Error(), "dedicated checkout must be clean") {
		t.Fatalf("dirty checkout error = %v", err)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != originalSHA {
		t.Fatalf("checkout changed to %s, want %s", got, originalSHA)
	}
	if _, err := os.Stat(dirty); err != nil {
		t.Fatalf("dirty file was removed: %v", err)
	}
}

func TestStageMainRejectsQueuedJobs(t *testing.T) {
	controller, checkout, originalSHA, _ := stageMainFixture(t)
	if _, err := controller.Submit([]string{"unit"}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.StageMain(context.Background()); err == nil || !strings.Contains(err.Error(), "cannot stage while BitCI jobs are queued or running") {
		t.Fatalf("queued-job staging error = %v", err)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != originalSHA {
		t.Fatalf("checkout changed to %s, want %s", got, originalSHA)
	}
}

func TestStageMainRequiresConfiguredGitHubOrigin(t *testing.T) {
	controller, checkout, originalSHA, _ := stageMainFixture(t)
	controller.githubRepo = ""
	if _, err := controller.StageMain(context.Background()); err == nil || !strings.Contains(err.Error(), "origin must be a github.com repository") {
		t.Fatalf("invalid origin error = %v", err)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != originalSHA {
		t.Fatalf("checkout changed to %s, want %s", got, originalSHA)
	}
}

func TestStageBranchFetchesExactRemoteRefWithoutPullRequest(t *testing.T) {
	controller, checkout, originalSHA, mainSHA := stageMainFixture(t)
	git(t, checkout, "checkout", "--detach", "-q", mainSHA)
	git(t, checkout, "checkout", "-qb", "codex/candidate")
	if err := os.WriteFile(filepath.Join(checkout, "candidate-only"), []byte("candidate"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, checkout, "add", "candidate-only")
	git(t, checkout, "-c", "user.name=BitCI", "-c", "user.email=bitci@example.test", "commit", "-qm", "candidate")
	candidateSHA := git(t, checkout, "rev-parse", "HEAD")
	git(t, checkout, "push", "-q", "origin", "HEAD:refs/heads/codex/candidate")
	git(t, checkout, "checkout", "--detach", "-q", originalSHA)
	for _, path := range []string{".next/types/stale", ".cache/keep"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(checkout, path)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(checkout, path), []byte("preserve"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	stage, err := controller.StageBranch(context.Background(), "codex/candidate")
	if err != nil {
		t.Fatal(err)
	}
	if stage.Ref != "codex/candidate" || stage.SHA != candidateSHA {
		t.Fatalf("stage = %#v, want codex/candidate at %s", stage, candidateSHA)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != candidateSHA {
		t.Fatalf("checkout SHA = %s, want candidate %s", got, candidateSHA)
	}
	if _, err := os.Stat(filepath.Join(checkout, ".next")); !os.IsNotExist(err) {
		t.Fatalf("generated .next remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(checkout, ".cache", "keep")); err != nil {
		t.Fatalf("unrelated ignored file was removed: %v", err)
	}
	if staged, err := controller.stagedCheckoutSHA(); err != nil || staged != candidateSHA {
		t.Fatalf("staged SHA = %q, %v", staged, err)
	}
	var sourceRef string
	if err := controller.db.QueryRow("SELECT source_ref FROM staged_checkouts WHERE id = 1").Scan(&sourceRef); err != nil || sourceRef != "refs/heads/codex/candidate" {
		t.Fatalf("staged source ref = %q, %v", sourceRef, err)
	}
	jobs, err := controller.Submit([]string{"unit"}, "")
	if err != nil || len(jobs) != 1 || jobs[0].Ref != candidateSHA || jobs[0].SubmittedRef != candidateSHA {
		t.Fatalf("submit staged branch = %#v, %v", jobs, err)
	}
}

func TestStageBranchRejectsInvalidOrMissingRemoteRefWithoutChangingCheckout(t *testing.T) {
	controller, checkout, originalSHA, _ := stageMainFixture(t)
	for _, branch := range []string{"", " refs/heads/main", "refs/heads/main", "../main", "codex/../main"} {
		if _, err := controller.StageBranch(context.Background(), branch); err == nil {
			t.Errorf("StageBranch(%q) unexpectedly succeeded", branch)
		}
		if got := git(t, checkout, "rev-parse", "HEAD"); got != originalSHA {
			t.Fatalf("invalid branch %q changed checkout to %s", branch, got)
		}
	}
	if _, err := controller.StageBranch(context.Background(), "codex/missing"); err == nil || !strings.Contains(err.Error(), "read remote branch") {
		t.Fatalf("missing branch error = %v", err)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != originalSHA {
		t.Fatalf("missing branch changed checkout to %s", got)
	}
}

func TestStageBranchRequiresConfiguredGitHubOrigin(t *testing.T) {
	controller, checkout, originalSHA, _ := stageMainFixture(t)
	controller.githubRepo = ""
	if _, err := controller.StageBranch(context.Background(), "codex/candidate"); err == nil || !strings.Contains(err.Error(), "origin must be a github.com repository") {
		t.Fatalf("invalid origin error = %v", err)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != originalSHA {
		t.Fatalf("checkout changed to %s, want %s", got, originalSHA)
	}
}

func TestStageBranchRejectsDirtyCheckoutWithoutChangingIt(t *testing.T) {
	controller, checkout, originalSHA, mainSHA := stageMainFixture(t)
	git(t, checkout, "checkout", "--detach", "-q", mainSHA)
	git(t, checkout, "checkout", "-qb", "codex/candidate")
	git(t, checkout, "push", "-q", "origin", "HEAD:refs/heads/codex/candidate")
	git(t, checkout, "checkout", "--detach", "-q", originalSHA)
	dirty := filepath.Join(checkout, "untracked.txt")
	if err := os.WriteFile(dirty, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.StageBranch(context.Background(), "codex/candidate"); err == nil || !strings.Contains(err.Error(), "dedicated checkout must be clean") {
		t.Fatalf("dirty checkout error = %v", err)
	}
	if got := git(t, checkout, "rev-parse", "HEAD"); got != originalSHA {
		t.Fatalf("dirty checkout changed to %s, want %s", got, originalSHA)
	}
	if _, err := os.Stat(dirty); err != nil {
		t.Fatalf("dirty file was removed: %v", err)
	}
}

func TestMigrationBackfillsStagedPullRequestSource(t *testing.T) {
	stateDir := t.TempDir()
	database, err := sql.Open("sqlite", filepath.Join(stateDir, "bitci.db"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`
		CREATE TABLE staged_checkouts (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			pull_request INTEGER NOT NULL,
			sha TEXT NOT NULL,
			staged_at TEXT NOT NULL
		);
		INSERT INTO staged_checkouts(id, pull_request, sha, staged_at)
		VALUES (1, 42, '0123456789012345678901234567890123456789', '2026-09-28T00:00:00Z');
	`)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	controller, err := OpenState(filepath.Join(t.TempDir(), "missing.json"), stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	var sourceRef string
	if err := controller.db.QueryRow("SELECT source_ref FROM staged_checkouts WHERE id = 1").Scan(&sourceRef); err != nil || sourceRef != "refs/pull/42/head" {
		t.Fatalf("migrated source ref = %q, %v", sourceRef, err)
	}
}
