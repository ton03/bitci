package bitci

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

type Controller struct {
	config         Config
	configPath     string
	checkoutRoot   string
	configRelative string
	stateDir       string
	db             *sql.DB
	githubAPI      string
	githubRepo     string
	worktreeMu     sync.Mutex
	configMu       sync.RWMutex
	ownerMu        sync.Mutex
	ownerRelease   func()
	ownerCount     int
	stageInit      sync.Once
	stageGate      chan struct{}
	recoveryMu     sync.Mutex
	recovered      bool
	activeMu       sync.Mutex
	activeJobs     map[int64]struct{}
	retryMu        sync.Mutex
}

type Job struct {
	ID               int64  `json:"id"`
	Batch            string `json:"batch"`
	Task             string `json:"task"`
	SubmittedRef     string `json:"submitted_ref,omitempty"`
	Ref              string `json:"ref"`
	TestedSHA        string `json:"tested_sha,omitempty"`
	State            string `json:"state"`
	CreatedAt        string `json:"created_at"`
	StartedAt        string `json:"started_at,omitempty"`
	FinishedAt       string `json:"finished_at,omitempty"`
	ExitCode         *int   `json:"exit_code,omitempty"`
	LogPath          string `json:"log_path,omitempty"`
	Attempt          int    `json:"attempt"`
	RetryOf          *int64 `json:"retry_of,omitempty"`
	RetryRoot        int64  `json:"retry_root"`
	PriorExitCode    *int   `json:"prior_exit_code,omitempty"`
	QueueWaitSeconds int    `json:"queue_wait_seconds"`
	DurationSeconds  int    `json:"duration_seconds"`
	TerminalResult   string `json:"terminal_result,omitempty"`
	WorkerPID        *int   `json:"worker_pid,omitempty"`
	configJSON       string
	checkoutRoot     string
	configRelative   string
	startedAt        string
}

func Open(configPath, stateDir string) (*Controller, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	absoluteConfig, err := canonicalConfigPath(configPath, true)
	if err != nil {
		return nil, err
	}
	stateDir = DefaultStateDir(absoluteConfig, stateDir)
	absoluteState, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	controller, err := OpenState(absoluteConfig, absoluteState)
	if err != nil {
		return nil, err
	}
	controller.config = config
	if root, relative, err := checkoutLocation(controller.configPath); err == nil {
		controller.checkoutRoot = root
		controller.configRelative = relative
	}
	return controller, nil
}

// OpenState opens only local controller state. It does not read bitci.json.
// Use it for status from a process outside the configured checkout.
func OpenState(configPath, stateDir string) (*Controller, error) {
	configPath, err := canonicalConfigPath(configPath, false)
	if err != nil {
		return nil, err
	}
	stateDir = DefaultStateDir(configPath, stateDir)
	absoluteState, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	stateDir = absoluteState
	if gitDirectory, err := gitCommonDirectory(configPath); err == nil && pathsOverlap(stateDir, gitDirectory) {
		return nil, fmt.Errorf("state directory must not overlap Git metadata")
	}
	if stateInsideGitMetadata(resolvedPathForComparison(stateDir)) || pathHasGitMetadataAncestor(stateDir) {
		return nil, fmt.Errorf("state directory must not overlap Git metadata")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", filepath.Join(stateDir, "bitci.db"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	controller := &Controller{configPath: configPath, stateDir: stateDir, db: db}
	controller.initStageGate()
	if err := controller.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return controller, nil
}

// ValidateStateDir returns an absolute state path outside Git metadata.
func ValidateStateDir(configPath, stateDir string) (string, error) {
	absoluteConfig, err := canonicalConfigPath(configPath, false)
	if err != nil {
		return "", err
	}
	stateDir = DefaultStateDir(absoluteConfig, stateDir)
	absoluteState, err := filepath.Abs(stateDir)
	if err != nil {
		return "", err
	}
	if gitDirectory, err := gitCommonDirectory(absoluteConfig); err == nil && pathsOverlap(absoluteState, gitDirectory) {
		return "", fmt.Errorf("state directory must not overlap Git metadata")
	}
	if stateInsideGitMetadata(resolvedPathForComparison(absoluteState)) || pathHasGitMetadataAncestor(absoluteState) {
		return "", fmt.Errorf("state directory must not overlap Git metadata")
	}
	return absoluteState, nil
}

func DefaultStateDir(configPath, stateDir string) string {
	if stateDir != "" {
		return stateDir
	}
	absoluteConfig, err := canonicalConfigPath(configPath, false)
	if err != nil {
		absoluteConfig = configPath
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(filepath.Dir(absoluteConfig), ".bitci")
	}
	digest := sha256.Sum256([]byte(absoluteConfig))
	return filepath.Join(home, ".local", "state", "bitci", hex.EncodeToString(digest[:6]))
}

func canonicalConfigPath(configPath string, requireExists bool) (string, error) {
	absolute, err := filepath.Abs(configPath)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if requireExists || !os.IsNotExist(err) {
		return "", fmt.Errorf("resolve config file: %w", err)
	}
	return resolvedPathForComparison(absolute), nil
}

func stateInsideGitMetadata(path string) bool {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if strings.EqualFold(filepath.Base(current), ".git") {
			return true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
	}
}

func pathHasGitMetadataAncestor(path string) bool {
	for current := resolvedPathForComparison(path); ; current = filepath.Dir(current) {
		head, headErr := os.Stat(filepath.Join(current, "HEAD"))
		objects, objectsErr := os.Stat(filepath.Join(current, "objects"))
		if headErr == nil && !head.IsDir() && objectsErr == nil && objects.IsDir() {
			return true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
	}
}

func (controller *Controller) Close() error { return controller.db.Close() }

func (controller *Controller) Plan(changedPaths []string) ([]string, error) {
	return controller.configSnapshot().Plan(changedPaths)
}

func (controller *Controller) configSnapshot() Config {
	controller.configMu.RLock()
	defer controller.configMu.RUnlock()
	return controller.config
}

func (controller *Controller) migrate() error {
	_, err := controller.db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY,
			batch TEXT NOT NULL,
			task TEXT NOT NULL,
			ref TEXT NOT NULL,
			submitted_ref TEXT,
			state TEXT NOT NULL,
			created_at TEXT NOT NULL,
			started_at TEXT,
			finished_at TEXT,
			exit_code INTEGER,
			log_path TEXT,
			tested_sha TEXT,
			config_json TEXT,
			checkout_root TEXT,
			config_relative TEXT,
			cleanup_pending INTEGER NOT NULL DEFAULT 0,
			attempt INTEGER NOT NULL DEFAULT 1,
			retry_of INTEGER,
			retry_root INTEGER,
			prior_exit_code INTEGER,
			queue_wait_seconds INTEGER NOT NULL DEFAULT 0,
			duration_seconds INTEGER NOT NULL DEFAULT 0,
			terminal_result TEXT,
			worker_pid INTEGER
		);
		CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(state, id);
		CREATE TABLE IF NOT EXISTS leases (
			resource TEXT NOT NULL,
			job_id INTEGER NOT NULL REFERENCES jobs(id),
			PRIMARY KEY (resource, job_id)
		);
		CREATE TABLE IF NOT EXISTS batch_refs (
			batch TEXT PRIMARY KEY,
			checkout_root TEXT NOT NULL,
			ref TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS staged_checkouts (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			pull_request INTEGER NOT NULL,
			sha TEXT NOT NULL,
			staged_at TEXT NOT NULL
		);
	`)
	if err != nil {
		return err
	}
	if err := controller.addJobColumn("tested_sha", "TEXT"); err != nil {
		return err
	}
	if err := controller.addJobColumn("submitted_ref", "TEXT"); err != nil {
		return err
	}
	if err := controller.addJobColumn("config_json", "TEXT"); err != nil {
		return err
	}
	if err := controller.addJobColumn("checkout_root", "TEXT"); err != nil {
		return err
	}
	if err := controller.addJobColumn("config_relative", "TEXT"); err != nil {
		return err
	}
	for _, column := range []struct{ name, definition string }{
		{"cleanup_pending", "INTEGER NOT NULL DEFAULT 0"},
		{"attempt", "INTEGER NOT NULL DEFAULT 1"},
		{"retry_of", "INTEGER"},
		{"retry_root", "INTEGER"},
		{"prior_exit_code", "INTEGER"},
		{"queue_wait_seconds", "INTEGER NOT NULL DEFAULT 0"},
		{"duration_seconds", "INTEGER NOT NULL DEFAULT 0"},
		{"terminal_result", "TEXT"},
		{"worker_pid", "INTEGER"},
	} {
		if err := controller.addJobColumn(column.name, column.definition); err != nil {
			return err
		}
	}
	if err := controller.addBatchRefColumn("ref", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	_, err = controller.db.Exec("UPDATE batch_refs SET ref = (SELECT ref FROM jobs WHERE jobs.batch = batch_refs.batch LIMIT 1) WHERE ref = ''")
	return err
}

func (controller *Controller) addJobColumn(name, definition string) error {
	hasColumn, err := controller.hasJobColumn(name)
	if err != nil || hasColumn {
		return err
	}
	if _, alterErr := controller.db.Exec("ALTER TABLE jobs ADD COLUMN " + name + " " + definition); alterErr == nil {
		return nil
	} else {
		err = alterErr
	}
	hasColumn, checkErr := controller.hasJobColumn(name)
	if checkErr != nil {
		return checkErr
	}
	if hasColumn {
		return nil
	}
	return err
}

func (controller *Controller) hasJobColumn(name string) (bool, error) {
	if _, err := controller.db.Exec("SELECT " + name + " FROM jobs LIMIT 0"); err == nil {
		return true, nil
	} else if strings.Contains(err.Error(), "no such column: "+name) {
		return false, nil
	} else {
		return false, err
	}
}

func (controller *Controller) addBatchRefColumn(name, definition string) error {
	if _, err := controller.db.Exec("SELECT " + name + " FROM batch_refs LIMIT 0"); err == nil {
		return nil
	} else if !strings.Contains(err.Error(), "no such column: "+name) {
		return err
	}
	if _, err := controller.db.Exec("ALTER TABLE batch_refs ADD COLUMN " + name + " " + definition); err != nil {
		if _, checkErr := controller.db.Exec("SELECT " + name + " FROM batch_refs LIMIT 0"); checkErr != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) Submit(taskNames []string, ref string) ([]Job, error) {
	return controller.SubmitContext(context.Background(), taskNames, ref)
}

func (controller *Controller) SubmitContext(ctx context.Context, taskNames []string, ref string) ([]Job, error) {
	requestedRef := strings.TrimSpace(ref)
	checkoutRoot, configRelative := "", ""
	submittedRef := requestedRef
	if sha, err := controller.checkoutSHA(); err == nil {
		if requestedRef != "" && !isCommitRef(requestedRef) {
			return nil, fmt.Errorf("requested ref must be a commit SHA")
		}
		var locationErr error
		checkoutRoot, configRelative, locationErr = controller.checkoutLocation()
		if locationErr != nil {
			return nil, locationErr
		}
		stagedSHA, err := controller.stagedCheckoutSHA()
		if err != nil {
			return nil, err
		}
		if requestedRef == "" {
			ref = sha
			submittedRef = sha
		} else {
			ref, err = resolveCommitSHA(ctx, checkoutRoot, requestedRef)
			if err != nil {
				return nil, fmt.Errorf("requested checkout SHA is unavailable: %w", err)
			}
		}
		if stagedSHA != "" && !strings.EqualFold(stagedSHA, ref) {
			return nil, fmt.Errorf("staged checkout SHA %s does not match requested SHA %s", stagedSHA, ref)
		}
		if err := recordedDirectoryExists(checkoutRoot, ref, configRelative); err != nil {
			return nil, err
		}
	} else if isCommitRef(requestedRef) {
		return nil, fmt.Errorf("cannot submit requested checkout SHA: checkout is unavailable")
	}
	return controller.submit(ctx, controller.configSnapshot(), taskNames, ref, submittedRef, checkoutRoot, configRelative, nil)
}

func resolveCommitSHA(ctx context.Context, checkoutRoot, ref string) (string, error) {
	resolved, err := gitAt(ctx, checkoutRoot, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	resolved = strings.TrimSpace(resolved)
	if !isCheckoutSHA(resolved) {
		return "", fmt.Errorf("Git returned an invalid commit SHA")
	}
	return strings.ToLower(resolved), nil
}

func recordedDirectoryExists(checkoutRoot, ref, relative string) error {
	if relative == "." || relative == "" {
		return nil
	}
	objectType, err := gitAt(context.Background(), checkoutRoot, "cat-file", "-t", ref+":"+filepath.ToSlash(relative))
	if err != nil || strings.TrimSpace(objectType) != "tree" {
		return fmt.Errorf("config directory does not exist at recorded checkout SHA")
	}
	return nil
}

type retryInfo struct {
	Attempt       int
	RetryOf       int64
	RetryRoot     int64
	PriorExitCode *int
}

func (controller *Controller) submit(ctx context.Context, config Config, taskNames []string, ref, submittedRef, checkoutRoot, configRelative string, retries map[string]retryInfo) ([]Job, error) {
	releaseStage, err := controller.acquireStageLock(ctx)
	if err != nil {
		return nil, fmt.Errorf("lock checkout for submit: %w", err)
	}
	defer releaseStage()
	ordered, err := config.Ordered(taskNames)
	if err != nil {
		return nil, err
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("submit requires at least one task")
	}
	batch, err := newBatch()
	if err != nil {
		return nil, err
	}
	recordedRef := checkoutRoot != "" && isCheckoutSHA(ref)
	if recordedRef {
		if _, err := controller.db.Exec("INSERT INTO batch_refs(batch, checkout_root, ref) VALUES (?, ?, ?)", batch, checkoutRoot, ref); err != nil {
			return nil, err
		}
	}
	committed := false
	defer func() {
		if recordedRef && !committed {
			_, _ = controller.db.Exec("DELETE FROM batch_refs WHERE batch = ?", batch)
		}
	}()
	transaction, err := controller.db.Begin()
	if err != nil {
		return nil, err
	}
	defer transaction.Rollback()
	jobs := make([]Job, 0, len(ordered))
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	for _, taskName := range ordered {
		retry := retries[taskName]
		if retry.Attempt == 0 {
			retry.Attempt = 1
		}
		createdAt := timestamp()
		result, err := transaction.Exec(
			"INSERT INTO jobs(batch, task, ref, submitted_ref, config_json, checkout_root, config_relative, attempt, retry_of, retry_root, prior_exit_code, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), NULLIF(?, 0), ?, 'queued', ?)",
			batch, taskName, ref, submittedRef, configJSON, checkoutRoot, configRelative, retry.Attempt, retry.RetryOf, retry.RetryRoot, retry.PriorExitCode, createdAt,
		)
		if err != nil {
			return nil, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return nil, err
		}
		if retry.RetryRoot == 0 {
			if _, err := transaction.Exec("UPDATE jobs SET retry_root = ? WHERE id = ?", id, id); err != nil {
				return nil, err
			}
			retry.RetryRoot = id
		}
		job := Job{ID: id, Batch: batch, Task: taskName, SubmittedRef: submittedRef, Ref: ref, State: "queued", CreatedAt: createdAt, Attempt: retry.Attempt, RetryRoot: retry.RetryRoot, PriorExitCode: retry.PriorExitCode, checkoutRoot: checkoutRoot, configRelative: configRelative}
		if retry.RetryOf != 0 {
			job.RetryOf = &retry.RetryOf
		}
		jobs = append(jobs, job)
	}
	refCreated := false
	if recordedRef {
		if _, err := gitAt(context.Background(), checkoutRoot, "update-ref", batchRef(batch), ref); err != nil {
			return nil, fmt.Errorf("pin recorded checkout SHA: %w", err)
		}
		refCreated = true
	}
	if err := transaction.Commit(); err != nil {
		if refCreated {
			_, _ = gitAt(context.Background(), checkoutRoot, "update-ref", "-d", batchRef(batch))
		}
		return nil, err
	}
	committed = true
	return jobs, nil
}

func (controller *Controller) RunOnce(ctx context.Context, maxWorkers int) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	release, err := controller.acquireOwner()
	if err != nil {
		return false, err
	}
	defer release()
	controller.recoveryMu.Lock()
	if !controller.recovered {
		err := controller.RecoverInterrupted()
		if err == nil {
			controller.recovered = true
		}
		controller.recoveryMu.Unlock()
		if err != nil {
			return false, err
		}
	} else {
		controller.recoveryMu.Unlock()
	}
	return controller.runOnce(ctx, maxWorkers)
}

func (controller *Controller) runOnce(ctx context.Context, maxWorkers int) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	job, claimed, err := controller.claimContext(ctx, maxWorkers)
	if err != nil || !claimed {
		return false, err
	}
	endActive := controller.markActive(job.ID)
	defer endActive()
	config, task, err := controller.jobConfig(job)
	if err != nil {
		return true, controller.finish(job, 127, false)
	}
	logFile, err := controller.startLog(&job)
	if err != nil {
		return true, controller.finish(job, 127, false)
	}
	if isCheckoutSHA(job.Ref) {
		if err := controller.writeJobLogHeader(logFile, job, task, maxWorkers); err != nil {
			logFile.Close()
			return true, controller.finish(job, 127, false)
		}
	}
	workDir := filepath.Dir(controller.configPath)
	worktreeRoot := ""
	var expectedOwner []byte
	cleanup := func() error { return nil }
	if isCheckoutSHA(job.Ref) {
		var err error
		workDir, cleanup, err = controller.jobCheckout(ctx, job)
		if err != nil {
			fmt.Fprintln(logFile, "BitCI could not stage recorded checkout SHA:", err)
			cleanupPending := false
			if cleanupErr := cleanup(); cleanupErr != nil {
				fmt.Fprintln(logFile, "BitCI could not remove job worktree:", cleanupErr)
				cleanupPending = true
			}
			logFile.Close()
			return true, controller.finish(job, 126, cleanupPending)
		}
		worktreeRoot = filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", job.ID))
		expectedOwner = jobCheckoutOwner(job)
		if _, err := fmt.Fprintf(logFile, "BitCI worktree=%s\n", worktreeRoot); err != nil {
			logFile.Close()
			return true, controller.finish(job, 127, true)
		}
		if owner, readErr := os.ReadFile(jobCheckoutOwnerPath(worktreeRoot)); readErr != nil || !bytes.Equal(owner, expectedOwner) {
			fmt.Fprintln(logFile, "BitCI could not record job worktree identity")
			cleanupPending := cleanup() != nil
			logFile.Close()
			return true, controller.finish(job, 126, cleanupPending)
		}
		if err := controller.db.QueryRow("SELECT COALESCE(tested_sha, '') FROM jobs WHERE id = ?", job.ID).Scan(&job.TestedSHA); err != nil {
			logFile.Close()
			return true, controller.finish(job, 127, false)
		}
		if _, err := fmt.Fprintf(logFile, "BitCI tested_sha=%s\n", job.TestedSHA); err != nil {
			logFile.Close()
			return true, controller.finish(job, 127, false)
		}
	}
	code := 0
	if isCheckoutSHA(job.Ref) && !controller.workDirWithinWorktree(worktreeRoot, workDir) {
		fmt.Fprintln(logFile, "BitCI refuses a task work directory outside the recorded worktree")
		code = 126
	} else if isCheckoutSHA(job.Ref) && len(config.Prepare) > 0 && controller.hasUnsafePrepareCommand(config.Prepare, job.checkoutRoot, worktreeRoot, workDir) {
		fmt.Fprintln(logFile, "BitCI refuses an unsafe checkout-local prepare argument for a recorded SHA job")
		code = 126
	} else {
		if isCheckoutSHA(job.Ref) && len(config.Prepare) > 0 {
			code = controller.executeCommandForJob(ctx, job.ID, config.Prepare, task.Timeout, logFile, workDir)
		}
		if code == 0 && isCheckoutSHA(job.Ref) {
			if err := controller.verifyJobWorktree(worktreeRoot, job.Ref, expectedOwner); err != nil {
				fmt.Fprintln(logFile, "BitCI worktree changed after prepare:", err)
				code = 126
			}
		}
		if code == 0 && isCheckoutSHA(job.Ref) && !controller.workDirWithinWorktree(worktreeRoot, workDir) {
			fmt.Fprintln(logFile, "BitCI refuses a task work directory outside the recorded worktree")
			code = 126
		}
		if code == 0 && isCheckoutSHA(job.Ref) && controller.hasUnsafeCheckoutCommand(task.Run, job.checkoutRoot, worktreeRoot, workDir, task.Env) {
			fmt.Fprintln(logFile, "BitCI refuses an unsafe checkout-local task argument for a recorded SHA job")
			code = 126
		}
		if code == 0 {
			code = controller.executeForJob(job.ID, ctx, task, logFile, workDir)
		}
		if code == 0 && isCheckoutSHA(job.Ref) {
			if err := controller.verifyJobWorktree(worktreeRoot, job.Ref, expectedOwner); err != nil {
				fmt.Fprintln(logFile, "BitCI worktree changed after task:", err)
				code = 126
			}
		}
	}
	cleanupPending := false
	if err := cleanup(); err != nil {
		fmt.Fprintln(logFile, "BitCI could not remove job worktree:", err)
		cleanupPending = true
		if code == 0 {
			code = 125
		}
	}
	if err := logFile.Close(); err != nil && code == 0 {
		code = 127
	}
	return true, controller.finish(job, code, cleanupPending)
}

func (controller *Controller) checkoutSHA() (string, error) {
	return checkoutSHA(controller.gitDirectory())
}

func (controller *Controller) gitDirectory() string {
	if controller.checkoutRoot != "" {
		return controller.checkoutRoot
	}
	return filepath.Dir(controller.configPath)
}

func (controller *Controller) checkoutLocation() (string, string, error) {
	if controller.checkoutRoot != "" {
		return controller.checkoutRoot, controller.configRelative, nil
	}
	return checkoutLocation(controller.configPath)
}

func (controller *Controller) jobLocation(job Job) (string, string, error) {
	if job.checkoutRoot != "" {
		return job.checkoutRoot, job.configRelative, nil
	}
	return controller.checkoutLocation()
}

func checkoutLocation(configPath string) (string, string, error) {
	resolvedConfig, err := filepath.EvalSymlinks(configPath)
	if err != nil {
		return "", "", fmt.Errorf("resolve config file: %w", err)
	}
	configDirectory, err := filepath.EvalSymlinks(filepath.Dir(resolvedConfig))
	if err != nil {
		return "", "", fmt.Errorf("resolve config directory: %w", err)
	}
	command := exec.Command("git", "-C", configDirectory, "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		return "", "", fmt.Errorf("find checkout root: %w", err)
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(string(output)))
	if err != nil {
		return "", "", fmt.Errorf("resolve checkout root: %w", err)
	}
	relative, ok := relativeWithin(root, configDirectory)
	if !ok {
		return "", "", fmt.Errorf("config must be inside its Git checkout")
	}
	return root, relative, nil
}

func gitCommonDirectory(configPath string) (string, error) {
	configDirectory := filepath.Dir(configPath)
	if resolvedConfig, err := filepath.EvalSymlinks(configPath); err == nil {
		configDirectory = filepath.Dir(resolvedConfig)
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("resolve config file: %w", err)
	}
	configDirectory, err := filepath.EvalSymlinks(configDirectory)
	if err != nil {
		return "", fmt.Errorf("resolve config directory: %w", err)
	}
	command := exec.Command("git", "-C", configDirectory, "rev-parse", "--git-common-dir")
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("find Git metadata directory: %w", err)
	}
	directory := strings.TrimSpace(string(output))
	if !filepath.IsAbs(directory) {
		directory = filepath.Join(configDirectory, directory)
	}
	directory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return "", fmt.Errorf("resolve Git metadata directory: %w", err)
	}
	return directory, nil
}

func checkoutSHA(directory string) (string, error) {
	command := exec.Command("git", "-C", directory, "rev-parse", "--verify", "HEAD^{commit}")
	output, err := command.Output()
	if err != nil {
		return "", err
	}
	sha := strings.TrimSpace(string(output))
	if !isCheckoutSHA(sha) {
		return "", fmt.Errorf("git returned invalid checkout SHA")
	}
	return sha, nil
}

func (controller *Controller) jobCheckout(ctx context.Context, job Job) (string, func() error, error) {
	controller.worktreeMu.Lock()
	defer controller.worktreeMu.Unlock()

	checkoutRoot, configRelative, err := controller.jobLocation(job)
	if err != nil {
		return "", func() error { return nil }, err
	}
	cleanup := func() error { return nil }
	root := filepath.Join(controller.stateDir, "worktrees")
	path := filepath.Join(root, fmt.Sprintf("job-%d", job.ID))
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", cleanup, err
	}
	if _, err := os.Lstat(path); err == nil {
		return "", cleanup, fmt.Errorf("job worktree already exists")
	} else if !os.IsNotExist(err) {
		return "", cleanup, err
	}
	if _, err := controller.db.Exec("UPDATE jobs SET cleanup_pending = 1 WHERE id = ?", job.ID); err != nil {
		return "", cleanup, err
	}
	cleanup = func() error { return controller.removeJobWorktree(job.ID, checkoutRoot) }
	if err := os.MkdirAll(filepath.Join(path, ".git"), 0o700); err != nil {
		return "", cleanup, err
	}
	if err := os.WriteFile(jobCheckoutOwnerPath(path), jobCheckoutOwner(job), 0o600); err != nil {
		return "", cleanup, err
	}
	objectFormat, err := gitAt(ctx, checkoutRoot, "rev-parse", "--show-object-format")
	if err != nil {
		return "", cleanup, fmt.Errorf("find source object format: %w", err)
	}
	initArgs := []string{"init", "--quiet"}
	if format := strings.TrimSpace(objectFormat); format != "" && format != "sha1" {
		initArgs = append(initArgs, "--object-format="+format)
	}
	if _, err := gitAt(ctx, path, initArgs...); err != nil {
		return "", cleanup, fmt.Errorf("initialize job worktree: %w", err)
	}
	objectCache, err := copyRecordedObjects(ctx, checkoutRoot, job.Ref, controller.stateDir, objectFormat)
	if err != nil {
		return "", cleanup, err
	}
	if err := os.WriteFile(filepath.Join(path, ".git", "objects", "info", "alternates"), []byte(objectCache+"\n"), 0o600); err != nil {
		return "", cleanup, fmt.Errorf("link recorded checkout objects: %w", err)
	}
	if _, err := gitAt(ctx, path, "checkout", "--quiet", "--detach", job.Ref); err != nil {
		return "", cleanup, fmt.Errorf("create job worktree: %w", err)
	}
	if err := copyCheckoutRefs(ctx, checkoutRoot, path); err != nil {
		return "", cleanup, fmt.Errorf("copy checkout refs: %w", err)
	}
	sha, err := checkoutSHA(path)
	if err != nil || !strings.EqualFold(sha, job.Ref) {
		return "", cleanup, fmt.Errorf("verify job worktree SHA")
	}
	if _, err := controller.db.Exec("UPDATE jobs SET tested_sha = ? WHERE id = ?", sha, job.ID); err != nil {
		return "", cleanup, err
	}
	return filepath.Join(path, configRelative), cleanup, nil
}

// copyCheckoutRefs preserves source branch and remote-tracking refs that point
// at objects in the recorded checkout. Tasks often use origin/main to find
// their comparison base, while each SHA job intentionally starts with fresh
// Git metadata.
func copyCheckoutRefs(ctx context.Context, sourceRoot, worktreeRoot string) error {
	output, err := gitAt(ctx, sourceRoot, "for-each-ref", "--format=%(refname) %(objectname) %(objecttype)", "refs/heads", "refs/remotes")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[2] != "commit" {
			continue
		}
		ref, sha := fields[0], fields[1]
		if _, err := gitAt(ctx, worktreeRoot, "cat-file", "-e", sha+"^{commit}"); err != nil {
			continue
		}
		if _, err := gitAt(ctx, worktreeRoot, "update-ref", ref, sha); err != nil {
			return fmt.Errorf("update %s: %w", ref, err)
		}
	}
	return nil
}

func copyRecordedObjects(ctx context.Context, checkoutRoot, ref, stateDir, objectFormat string) (string, error) {
	cacheRoot := objectCacheRoot(stateDir, checkoutRoot, ref, objectFormat)
	if _, err := os.Stat(cacheRoot); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(cacheRoot), 0o700); err != nil {
			return "", fmt.Errorf("create recorded checkout object cache: %w", err)
		}
		initArgs := []string{"init", "--bare", "--quiet"}
		if format := strings.TrimSpace(objectFormat); format != "" && format != "sha1" {
			initArgs = append(initArgs, "--object-format="+format)
		}
		if _, err := gitAt(ctx, filepath.Dir(cacheRoot), append(initArgs, cacheRoot)...); err != nil {
			return "", fmt.Errorf("initialize recorded checkout object cache: %w", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("inspect recorded checkout object cache: %w", err)
	}
	if _, err := gitAt(ctx, cacheRoot, "cat-file", "-e", ref+"^{commit}"); err == nil {
		return filepath.Join(cacheRoot, "objects"), nil
	}
	pack := exec.CommandContext(ctx, "git", "-C", checkoutRoot, "pack-objects", "--stdout", "--revs", "--delta-base-offset")
	pack.Stdin = strings.NewReader(ref + "\n")
	packOutput, err := pack.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("prepare recorded checkout objects: %w", err)
	}
	var packError bytes.Buffer
	pack.Stderr = &packError
	index := exec.CommandContext(ctx, "git", "-C", cacheRoot, "index-pack", "--stdin", "--fix-thin", "--keep")
	index.Stdin = packOutput
	var indexError bytes.Buffer
	index.Stderr = &indexError
	if err := index.Start(); err != nil {
		_ = packOutput.Close()
		return "", fmt.Errorf("start recorded checkout object import: %w", err)
	}
	if err := pack.Start(); err != nil {
		_ = packOutput.Close()
		_ = index.Wait()
		return "", fmt.Errorf("start recorded checkout object export: %w", err)
	}
	packErr := pack.Wait()
	indexErr := index.Wait()
	if packErr != nil {
		return "", fmt.Errorf("export recorded checkout objects: %s: %w", strings.TrimSpace(packError.String()), packErr)
	}
	if indexErr != nil {
		return "", fmt.Errorf("import recorded checkout objects: %s: %w", strings.TrimSpace(indexError.String()), indexErr)
	}
	return filepath.Join(cacheRoot, "objects"), nil
}

func objectCacheRoot(stateDir, checkoutRoot, ref, objectFormat string) string {
	cacheDigest := sha256.Sum256([]byte(resolvedPathForComparison(checkoutRoot) + "\x00" + strings.ToLower(strings.TrimSpace(objectFormat)) + "\x00" + strings.ToLower(ref)))
	return filepath.Join(stateDir, "object-cache", hex.EncodeToString(cacheDigest[:8]))
}

func (controller *Controller) pruneObjectCaches() error {
	releaseStage, err := controller.acquireStageLock(context.Background())
	if err != nil {
		return err
	}
	defer releaseStage()
	return controller.pruneObjectCachesUnlocked()
}

func (controller *Controller) pruneObjectCachesUnlocked() error {
	root := filepath.Join(controller.stateDir, "object-cache")
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	rows, err := controller.db.Query("SELECT DISTINCT checkout_root, ref FROM jobs WHERE state IN ('queued', 'running') OR cleanup_pending = 1")
	if err != nil {
		return err
	}
	keep := map[string]bool{}
	for rows.Next() {
		var checkoutRoot, ref string
		if err := rows.Scan(&checkoutRoot, &ref); err != nil {
			rows.Close()
			return err
		}
		if checkoutRoot == "" || !isCheckoutSHA(ref) {
			continue
		}
		objectFormat, err := gitAt(context.Background(), checkoutRoot, "rev-parse", "--show-object-format")
		if err != nil {
			rows.Close()
			return nil
		}
		keep[filepath.Base(objectCacheRoot(controller.stateDir, checkoutRoot, ref, objectFormat))] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || keep[entry.Name()] {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func jobCheckoutOwner(job Job) []byte {
	return []byte(fmt.Sprintf("%d\n%s\n", job.ID, strings.ToLower(job.Ref)))
}

func jobCheckoutOwnerPath(worktreeRoot string) string {
	return filepath.Join(worktreeRoot, ".git", "bitci-owner")
}

func verifyJobGitMetadata(gitDirectory string) error {
	root := filepath.Clean(gitDirectory)
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			relative, _ := filepath.Rel(root, path)
			return fmt.Errorf("worktree Git metadata contains symlink: %s", relative)
		}
		return nil
	})
}

func (controller *Controller) verifyJobWorktree(worktreeRoot, ref string, expectedOwner []byte) error {
	rootInfo, err := os.Lstat(worktreeRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("worktree root changed")
	}
	gitDirectory := filepath.Join(worktreeRoot, ".git")
	info, err := os.Lstat(gitDirectory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("worktree Git directory changed")
	}
	if err := verifyJobGitMetadata(gitDirectory); err != nil {
		return err
	}
	owner, err := os.ReadFile(jobCheckoutOwnerPath(worktreeRoot))
	if err != nil || !bytes.Equal(owner, expectedOwner) {
		return fmt.Errorf("worktree owner changed")
	}
	root, err := gitAt(context.Background(), worktreeRoot, "rev-parse", "--show-toplevel")
	if err != nil || !samePath(strings.TrimSpace(root), worktreeRoot) {
		return fmt.Errorf("worktree root changed")
	}
	common, err := gitAt(context.Background(), worktreeRoot, "rev-parse", "--path-format=absolute", "--git-common-dir")
	if err != nil || !samePath(strings.TrimSpace(common), gitDirectory) {
		return fmt.Errorf("worktree Git directory escaped job")
	}
	flags, err := gitAt(context.Background(), worktreeRoot, "ls-files", "-v", "-z")
	if err != nil {
		return fmt.Errorf("read worktree index flags: %w", err)
	}
	for _, entry := range strings.Split(flags, "\x00") {
		if entry != "" && (entry[0] == 'S' || 'a' <= entry[0] && entry[0] <= 'z') {
			return fmt.Errorf("worktree index flags changed")
		}
	}
	status, err := gitAt(context.Background(), worktreeRoot, "status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return fmt.Errorf("read worktree status: %w", err)
	}
	if status != "" {
		return fmt.Errorf("worktree tracked files changed: %s", strings.TrimSpace(status))
	}
	sha, err := checkoutSHA(worktreeRoot)
	if err != nil || !strings.EqualFold(sha, ref) {
		return fmt.Errorf("worktree SHA changed")
	}
	return nil
}

func (controller *Controller) workDirWithinWorktree(worktreeRoot, workDir string) bool {
	return worktreeRoot != "" && pathWithin(resolvedPathForComparison(worktreeRoot), resolvedPathForComparison(workDir))
}

func isCheckoutSHA(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !('0' <= character && character <= '9' || 'a' <= character && character <= 'f' || 'A' <= character && character <= 'F') {
			return false
		}
	}
	return true
}

func isCommitRef(value string) bool {
	if len(value) < 7 || len(value) > 64 {
		return false
	}
	for _, character := range value {
		if !('0' <= character && character <= '9' || 'a' <= character && character <= 'f' || 'A' <= character && character <= 'F') {
			return false
		}
	}
	return true
}

func timestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func (controller *Controller) Serve(ctx context.Context, maxWorkers int, interval time.Duration, socketPath string, httpAddresses ...string) error {
	if maxWorkers < 1 {
		return fmt.Errorf("max-workers must be positive")
	}
	if len(httpAddresses) > 1 {
		return fmt.Errorf("serve accepts at most one dashboard address")
	}
	release, err := controller.acquireOwner()
	if err != nil {
		return err
	}
	defer release()
	listener, err := controller.Listen(socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := controller.RecoverInterrupted(); err != nil {
		return err
	}
	httpAddress := ""
	if len(httpAddresses) > 0 {
		httpAddress = httpAddresses[0]
	}
	var dashboard *http.Server
	var dashboardListener net.Listener
	if httpAddress != "" {
		dashboardListener, err = listenDashboard(httpAddress)
		if err != nil {
			return err
		}
		dashboard = &http.Server{Handler: controller.DashboardHandler(), ReadHeaderTimeout: 5 * time.Second}
	}
	if interval <= 0 {
		interval = time.Second
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	errors := make(chan error, maxWorkers+2)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := controller.ServeRPC(runContext, listener); err != nil {
			errors <- err
		}
	}()
	if dashboard != nil {
		go func() {
			err := dashboard.Serve(dashboardListener)
			if err != nil && err != http.ErrServerClosed {
				errors <- err
			}
		}()
	}
	go controller.serveRecovery(runContext, interval, errors)
	for worker := 0; worker < maxWorkers; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			controller.serveWorker(runContext, maxWorkers, interval, errors)
		}()
	}
	shutdown := func() {
		cancel()
		_ = listener.Close()
		if dashboard != nil {
			shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = dashboard.Shutdown(shutdownContext)
			shutdownCancel()
			_ = dashboardListener.Close()
		}
		workers.Wait()
	}
	for {
		select {
		case <-ctx.Done():
			shutdown()
			return nil
		case err := <-errors:
			if err != nil {
				shutdown()
				return err
			}
		}
	}
}

const orphanRecoveryGrace = 5 * time.Second

func (controller *Controller) serveRecovery(ctx context.Context, interval time.Duration, errors chan<- error) {
	ticker := time.NewTicker(recoveryInterval(interval))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := controller.RecoverOrphaned(); err != nil {
				errors <- err
				return
			}
		}
	}
}

func recoveryInterval(interval time.Duration) time.Duration {
	if interval < time.Second {
		return time.Second
	}
	if interval > orphanRecoveryGrace {
		return orphanRecoveryGrace
	}
	return interval
}

// RecoverOrphaned fails running jobs whose recorded task process no longer exists.
// It never kills a process and leaves live jobs untouched.
func (controller *Controller) RecoverOrphaned() (int, error) {
	rows, err := controller.db.Query("SELECT id, batch, ref, COALESCE(checkout_root, ''), worker_pid, COALESCE(started_at, '') FROM jobs WHERE state = 'running'")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var orphaned []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Ref, &job.checkoutRoot, &job.WorkerPID, &job.startedAt); err != nil {
			return 0, err
		}
		if controller.isActive(job.ID) {
			continue
		}
		if !orphanGraceExpired(job.startedAt) {
			continue
		}
		if job.WorkerPID == nil {
			orphaned = append(orphaned, job)
			continue
		}
		alive, err := processAlive(*job.WorkerPID)
		if err != nil {
			return 0, err
		}
		if !alive {
			orphaned = append(orphaned, job)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	return controller.recoverJobs(orphaned, true)
}

// RecoverJob fails one running job only when its recorded task process is gone.
func (controller *Controller) RecoverJob(id int64) (bool, error) {
	var job Job
	if err := controller.db.QueryRow("SELECT id, batch, ref, COALESCE(checkout_root, ''), worker_pid, COALESCE(started_at, '') FROM jobs WHERE id = ? AND state = 'running'", id).Scan(&job.ID, &job.Batch, &job.Ref, &job.checkoutRoot, &job.WorkerPID, &job.startedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("job %d is not running", id)
		}
		return false, err
	}
	if job.WorkerPID == nil {
		if !orphanGraceExpired(job.startedAt) {
			return false, fmt.Errorf("job %d has no task process yet", id)
		}
		recovered, err := controller.recoverJobs([]Job{job}, false)
		return recovered == 1, err
	}
	alive, err := processAlive(*job.WorkerPID)
	if err != nil {
		return false, err
	}
	if alive {
		return false, fmt.Errorf("job %d task process is still running", id)
	}
	recovered, err := controller.recoverJobs([]Job{job}, false)
	return recovered == 1, err
}

func orphanGraceExpired(startedAt string) bool {
	parsed, err := time.Parse(time.RFC3339, startedAt)
	return err != nil || time.Since(parsed) >= orphanRecoveryGrace
}

func (controller *Controller) markActive(id int64) func() {
	controller.activeMu.Lock()
	if controller.activeJobs == nil {
		controller.activeJobs = make(map[int64]struct{})
	}
	controller.activeJobs[id] = struct{}{}
	controller.activeMu.Unlock()
	return func() {
		controller.activeMu.Lock()
		delete(controller.activeJobs, id)
		controller.activeMu.Unlock()
	}
}

func (controller *Controller) isActive(id int64) bool {
	controller.activeMu.Lock()
	_, ok := controller.activeJobs[id]
	controller.activeMu.Unlock()
	return ok
}

func (controller *Controller) recoverJobs(jobs []Job, skipChanged bool) (int, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	transaction, err := controller.db.Begin()
	if err != nil {
		return 0, err
	}
	defer transaction.Rollback()
	now := timestamp()
	recovered := make([]Job, 0, len(jobs))
	for _, job := range jobs {
		var result sql.Result
		var err error
		if job.WorkerPID == nil {
			result, err = transaction.Exec("UPDATE jobs SET state = 'failed', finished_at = ?, exit_code = 125, duration_seconds = COALESCE(CASE WHEN started_at IS NULL THEN 0 ELSE MAX(0, CAST(strftime('%s', ?) - strftime('%s', started_at) AS INTEGER)) END, 0), terminal_result = 'failed', worker_pid = NULL, cleanup_pending = ? WHERE id = ? AND state = 'running' AND worker_pid IS NULL", now, now, isCheckoutSHA(job.Ref), job.ID)
		} else {
			result, err = transaction.Exec("UPDATE jobs SET state = 'failed', finished_at = ?, exit_code = 125, duration_seconds = COALESCE(CASE WHEN started_at IS NULL THEN 0 ELSE MAX(0, CAST(strftime('%s', ?) - strftime('%s', started_at) AS INTEGER)) END, 0), terminal_result = 'failed', worker_pid = NULL, cleanup_pending = ? WHERE id = ? AND state = 'running' AND worker_pid = ?", now, now, isCheckoutSHA(job.Ref), job.ID, *job.WorkerPID)
		}
		if err != nil {
			return 0, err
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		if updated == 0 {
			if skipChanged {
				continue
			}
			return 0, fmt.Errorf("job %d changed before recovery", job.ID)
		}
		if _, err := transaction.Exec("DELETE FROM leases WHERE job_id = ?", job.ID); err != nil {
			return 0, err
		}
		if _, err := transaction.Exec("UPDATE jobs SET state = 'cancelled', finished_at = ?, queue_wait_seconds = MAX(0, CAST(strftime('%s', ?) - strftime('%s', created_at) AS INTEGER)), duration_seconds = 0, terminal_result = 'cancelled' WHERE state = 'queued' AND batch = ?", now, now, job.Batch); err != nil {
			return 0, err
		}
		recovered = append(recovered, job)
	}
	if err := transaction.Commit(); err != nil {
		return 0, err
	}
	var failures []error
	for _, job := range recovered {
		if !isCheckoutSHA(job.Ref) {
			continue
		}
		if err := controller.removeJobWorktree(job.ID, job.checkoutRoot); err != nil {
			failures = append(failures, err)
			continue
		}
		if _, err := controller.db.Exec("UPDATE jobs SET cleanup_pending = 0 WHERE id = ?", job.ID); err != nil {
			failures = append(failures, err)
			continue
		}
		if err := controller.releaseBatchRef(job.Batch, job.checkoutRoot); err != nil {
			failures = append(failures, err)
		}
	}
	if err := controller.releaseFinishedBatchRefs(); err != nil {
		failures = append(failures, err)
	}
	return len(recovered), errors.Join(failures...)
}

func processAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	err := syscall.Kill(-pid, 0)
	if err == nil || errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return false, err
}

func (controller *Controller) lockOwner() (func(), error) {
	path := filepath.Join(controller.stateDir, "controller.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("another controller owns state directory")
		}
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}

func (controller *Controller) acquireStageLock(ctx context.Context) (func(), error) {
	controller.initStageGate()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-controller.stageGate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	gitMetadata, err := gitCommonDirectory(controller.configPath)
	if err != nil {
		controller.stageGate <- struct{}{}
		return func() {}, nil
	}
	file, err := os.OpenFile(filepath.Join(gitMetadata, "bitci-stage.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		controller.stageGate <- struct{}{}
		return nil, err
	}
	release := func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		controller.stageGate <- struct{}{}
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return release, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			release()
			return nil, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			release()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (controller *Controller) initStageGate() {
	controller.stageInit.Do(func() {
		controller.stageGate = make(chan struct{}, 1)
		controller.stageGate <- struct{}{}
	})
}

func (controller *Controller) acquireOwner() (func(), error) {
	controller.ownerMu.Lock()
	defer controller.ownerMu.Unlock()
	if controller.ownerCount > 0 {
		controller.ownerCount++
		return controller.releaseOwner, nil
	}
	release, err := controller.lockOwner()
	if err != nil {
		return nil, err
	}
	controller.ownerRelease = release
	controller.ownerCount = 1
	return controller.releaseOwner, nil
}

func (controller *Controller) releaseOwner() {
	controller.ownerMu.Lock()
	defer controller.ownerMu.Unlock()
	controller.ownerCount--
	if controller.ownerCount == 0 {
		release := controller.ownerRelease
		controller.ownerRelease = nil
		release()
	}
}

func (controller *Controller) serveWorker(ctx context.Context, maxWorkers int, interval time.Duration, errors chan<- error) {
	for {
		ran, err := controller.runOnce(ctx, maxWorkers)
		if err != nil {
			errors <- err
			return
		}
		if ran {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func (controller *Controller) RecoverInterrupted() error {
	if err := controller.cleanupOrphanBatchRefs(); err != nil {
		return err
	}
	transaction, err := controller.db.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	rows, err := transaction.Query("SELECT id, batch, ref, COALESCE(checkout_root, ''), state, cleanup_pending FROM jobs WHERE state = 'running' OR cleanup_pending = 1")
	if err != nil {
		return err
	}
	interrupted := make([]Job, 0)
	for rows.Next() {
		var job Job
		var cleanupPending int
		if err := rows.Scan(&job.ID, &job.Batch, &job.Ref, &job.checkoutRoot, &job.State, &cleanupPending); err != nil {
			rows.Close()
			return err
		}
		if isCheckoutSHA(job.Ref) && (job.State == "running" || cleanupPending != 0) {
			interrupted = append(interrupted, job)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	now := timestamp()
	if _, err := transaction.Exec("UPDATE jobs SET state = 'cancelled', finished_at = ?, queue_wait_seconds = MAX(0, CAST(strftime('%s', ?) - strftime('%s', created_at) AS INTEGER)), duration_seconds = 0, terminal_result = 'cancelled' WHERE state = 'queued' AND batch IN (SELECT DISTINCT batch FROM jobs WHERE state = 'running')", now, now); err != nil {
		return err
	}
	if _, err := transaction.Exec("DELETE FROM leases WHERE job_id IN (SELECT id FROM jobs WHERE state = 'running')"); err != nil {
		return err
	}
	if _, err := transaction.Exec("UPDATE jobs SET state = 'failed', finished_at = ?, exit_code = 125, duration_seconds = CASE WHEN started_at IS NULL THEN 0 ELSE MAX(0, CAST(strftime('%s', ?) - strftime('%s', started_at) AS INTEGER)) END, terminal_result = 'failed' WHERE state = 'running'", now, now); err != nil {
		return err
	}
	for _, job := range interrupted {
		if _, err := transaction.Exec("UPDATE jobs SET cleanup_pending = 1 WHERE id = ?", job.ID); err != nil {
			return err
		}
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	var failures []error
	for _, job := range interrupted {
		if err := controller.terminateJobProcessGroup(job.ID); err != nil {
			failures = append(failures, err)
			continue
		}
		if err := controller.removeJobWorktree(job.ID, job.checkoutRoot); err != nil {
			failures = append(failures, err)
			continue
		}
		if _, err := controller.db.Exec("UPDATE jobs SET cleanup_pending = 0 WHERE id = ?", job.ID); err != nil {
			failures = append(failures, err)
			continue
		}
		if err := controller.releaseBatchRef(job.Batch, job.checkoutRoot); err != nil {
			failures = append(failures, err)
		}
	}
	if err := controller.pruneLogs(); err != nil {
		failures = append(failures, err)
	}
	if err := controller.releaseFinishedBatchRefs(); err != nil {
		failures = append(failures, err)
	}
	if err := controller.pruneObjectCaches(); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func batchRef(batch string) string {
	return "refs/bitci/jobs/" + batch
}

func (controller *Controller) cleanupOrphanBatchRefs() error {
	releaseStage, err := controller.acquireStageLock(context.Background())
	if err != nil {
		return err
	}
	defer releaseStage()
	rows, err := controller.db.Query("SELECT batch, checkout_root, ref FROM batch_refs")
	if err != nil {
		return err
	}
	type ownedBatchRef struct{ batch, checkoutRoot, expected string }
	var refs []ownedBatchRef
	for rows.Next() {
		var ref ownedBatchRef
		if err := rows.Scan(&ref.batch, &ref.checkoutRoot, &ref.expected); err != nil {
			rows.Close()
			return err
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, owned := range refs {
		var active int
		if err := controller.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE batch = ? AND (state IN ('queued', 'running') OR cleanup_pending = 1)", owned.batch).Scan(&active); err != nil {
			return err
		}
		actual, refErr := gitAt(context.Background(), owned.checkoutRoot, "rev-parse", "--verify", batchRef(owned.batch)+"^{commit}")
		if refErr != nil {
			if _, repoErr := gitAt(context.Background(), owned.checkoutRoot, "rev-parse", "--git-dir"); repoErr != nil {
				continue
			}
		}
		if active != 0 {
			if refErr != nil {
				if _, objectErr := gitAt(context.Background(), owned.checkoutRoot, "cat-file", "-e", owned.expected+"^{commit}"); objectErr != nil {
					continue
				}
				if _, err := gitAt(context.Background(), owned.checkoutRoot, "update-ref", batchRef(owned.batch), owned.expected); err != nil {
					return fmt.Errorf("restore missing batch ref: %w", err)
				}
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(actual), owned.expected) {
				return fmt.Errorf("batch ref %s changed unexpectedly", owned.batch)
			}
			continue
		}
		if refErr == nil && strings.EqualFold(strings.TrimSpace(actual), owned.expected) {
			if _, err := gitAt(context.Background(), owned.checkoutRoot, "update-ref", "-d", batchRef(owned.batch)); err != nil {
				return fmt.Errorf("remove orphan batch ref: %w", err)
			}
		}
		if _, err := controller.db.Exec("DELETE FROM batch_refs WHERE batch = ?", owned.batch); err != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) releaseBatchRef(batch, checkoutRoot string) error {
	return controller.releaseBatchRefContext(context.Background(), batch, checkoutRoot)
}

func (controller *Controller) releaseBatchRefContext(ctx context.Context, batch, checkoutRoot string) error {
	releaseStage, err := controller.acquireStageLock(ctx)
	if err != nil {
		return err
	}
	defer releaseStage()
	return controller.releaseBatchRefUnlockedContext(ctx, batch, checkoutRoot)
}

func (controller *Controller) releaseBatchRefUnlocked(batch, checkoutRoot string) error {
	return controller.releaseBatchRefUnlockedContext(context.Background(), batch, checkoutRoot)
}

func (controller *Controller) releaseBatchRefUnlockedContext(ctx context.Context, batch, checkoutRoot string) error {
	var protected int
	if err := controller.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE batch = ? AND (state IN ('queued', 'running') OR cleanup_pending = 1)", batch).Scan(&protected); err != nil || protected != 0 {
		return err
	}
	var expected string
	if err := controller.db.QueryRow("SELECT ref FROM jobs WHERE batch = ? LIMIT 1", batch).Scan(&expected); err != nil {
		return err
	}
	if !isCheckoutSHA(expected) {
		_, _ = controller.db.Exec("DELETE FROM batch_refs WHERE batch = ?", batch)
		return nil
	}
	if checkoutRoot == "" {
		var err error
		checkoutRoot, _, err = controller.checkoutLocation()
		if err != nil {
			return fmt.Errorf("find checkout for batch ref: %w", err)
		}
	}
	ref := batchRef(batch)
	actual, err := gitAt(ctx, checkoutRoot, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil || !strings.EqualFold(strings.TrimSpace(actual), expected) {
		return nil
	}
	_, err = gitAt(ctx, checkoutRoot, "update-ref", "-d", ref)
	if err == nil {
		_, _ = controller.db.Exec("DELETE FROM batch_refs WHERE batch = ?", batch)
	}
	return err
}

func (controller *Controller) releaseFinishedBatchRefs() error {
	releaseStage, err := controller.acquireStageLock(context.Background())
	if err != nil {
		return err
	}
	defer releaseStage()
	return controller.releaseFinishedBatchRefsUnlocked()
}

func (controller *Controller) releaseFinishedBatchRefsUnlocked() error {
	rows, err := controller.db.Query("SELECT batch, COALESCE(checkout_root, '') FROM jobs GROUP BY batch HAVING SUM(CASE WHEN state IN ('queued', 'running') OR cleanup_pending = 1 THEN 1 ELSE 0 END) = 0")
	if err != nil {
		return err
	}
	type batchLocation struct{ batch, checkoutRoot string }
	var batches []batchLocation
	for rows.Next() {
		var item batchLocation
		if err := rows.Scan(&item.batch, &item.checkoutRoot); err != nil {
			rows.Close()
			return err
		}
		batches = append(batches, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range batches {
		if err := controller.releaseBatchRefUnlocked(item.batch, item.checkoutRoot); err != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) removeJobWorktree(id int64, _ string) error {
	controller.worktreeMu.Lock()
	defer controller.worktreeMu.Unlock()

	path := filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", id))
	var ref string
	if err := controller.db.QueryRow("SELECT ref FROM jobs WHERE id = ?", id).Scan(&ref); err != nil {
		return err
	}
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	rootInfo, err := os.Lstat(path)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse to remove changed job worktree root")
	}
	owner, err := os.ReadFile(jobCheckoutOwnerPath(path))
	if err != nil || !bytes.Equal(owner, jobCheckoutOwner(Job{ID: id, Ref: ref})) {
		return fmt.Errorf("refuse to remove unverified job worktree")
	}
	info, err := os.Lstat(filepath.Join(path, ".git"))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refuse to remove job worktree with changed Git directory")
	}
	if err := verifyJobGitMetadata(filepath.Join(path, ".git")); err != nil {
		return fmt.Errorf("refuse to remove job worktree with changed Git metadata: %w", err)
	}
	return os.RemoveAll(path)
}

func (controller *Controller) claim(maxWorkers int) (Job, bool, error) {
	return controller.claimContext(context.Background(), maxWorkers)
}

func (controller *Controller) claimContext(ctx context.Context, maxWorkers int) (Job, bool, error) {
	releaseStage, err := controller.acquireStageLock(ctx)
	if err != nil {
		return Job{}, false, fmt.Errorf("lock checkout for claim: %w", err)
	}
	defer releaseStage()
	if maxWorkers < 1 {
		return Job{}, false, fmt.Errorf("max-workers must be positive")
	}
	transaction, err := controller.db.Begin()
	if err != nil {
		return Job{}, false, err
	}
	defer transaction.Rollback()
	var pinned *Job
	pinnedCreated := false
	committed := false
	defer func() {
		if pinned != nil && pinnedCreated && !committed {
			_ = controller.removeBatchRefIfOwned(*pinned)
		}
	}()
	var active int
	if err := transaction.QueryRow("SELECT COUNT(*) FROM jobs WHERE state = 'running'").Scan(&active); err != nil {
		return Job{}, false, err
	}
	if active >= maxWorkers {
		return Job{}, false, nil
	}
	rows, err := transaction.Query("SELECT id, batch, task, COALESCE(submitted_ref, ''), ref, COALESCE(config_json, ''), COALESCE(checkout_root, ''), COALESCE(config_relative, ''), state FROM jobs WHERE state = 'queued' ORDER BY id")
	if err != nil {
		return Job{}, false, err
	}
	defer rows.Close()
	changed := false
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.SubmittedRef, &job.Ref, &job.configJSON, &job.checkoutRoot, &job.configRelative, &job.State); err != nil {
			return Job{}, false, err
		}
		config, task, err := controller.jobConfig(job)
		if err != nil {
			if _, updateErr := transaction.Exec("UPDATE jobs SET state = 'failed', finished_at = ?, exit_code = 127, terminal_result = 'failed' WHERE id = ?", timestamp(), job.ID); updateErr != nil {
				return Job{}, false, updateErr
			}
			changed = true
			continue
		}
		if err := controller.diskOK(config.MinFreeBytes); err != nil {
			return Job{}, false, err
		}
		ready, blocked, err := controller.ready(transaction, job, task)
		if err != nil {
			return Job{}, false, err
		}
		if blocked {
			if _, err := transaction.Exec("UPDATE jobs SET state = 'cancelled', finished_at = ?, terminal_result = 'cancelled' WHERE id = ?", timestamp(), job.ID); err != nil {
				return Job{}, false, err
			}
			changed = true
			continue
		}
		if !ready {
			continue
		}
		if free, err := controller.resourcesFree(transaction, task, config); err != nil || !free {
			if err != nil {
				return Job{}, false, err
			}
			continue
		}
		if isCheckoutSHA(job.Ref) {
			created, err := controller.pinClaimedJob(ctx, transaction, &job)
			if err != nil {
				return Job{}, false, err
			}
			pinnedJob := job
			pinned = &pinnedJob
			pinnedCreated = created
		}
		now := timestamp()
		if _, err := transaction.Exec("UPDATE jobs SET state = 'running', started_at = ?, queue_wait_seconds = MAX(0, CAST(strftime('%s', ?) - strftime('%s', created_at) AS INTEGER)) WHERE id = ?", now, now, job.ID); err != nil {
			return Job{}, false, err
		}
		for _, resource := range task.Resources {
			if _, err := transaction.Exec("INSERT INTO leases(resource, job_id) VALUES (?, ?)", resource, job.ID); err != nil {
				return Job{}, false, err
			}
		}
		job.State = "running"
		if err := rows.Close(); err != nil {
			return Job{}, false, err
		}
		if err := transaction.Commit(); err != nil {
			return Job{}, false, err
		}
		committed = true
		return job, true, nil
	}
	if err := rows.Err(); err != nil {
		return Job{}, false, err
	}
	if changed {
		if err := transaction.Commit(); err != nil {
			return Job{}, false, err
		}
		return Job{}, false, controller.releaseFinishedBatchRefsUnlocked()
	}
	return Job{}, false, nil
}

func (controller *Controller) pinClaimedJob(ctx context.Context, transaction *sql.Tx, job *Job) (bool, error) {
	if job.checkoutRoot == "" {
		root, relative, err := controller.checkoutLocation()
		if err != nil {
			return false, fmt.Errorf("find checkout for recorded SHA: %w", err)
		}
		job.checkoutRoot, job.configRelative = root, relative
		if _, err := transaction.Exec("UPDATE jobs SET checkout_root = ?, config_relative = ? WHERE batch = ?", root, relative, job.Batch); err != nil {
			return false, err
		}
	}
	job.Ref = strings.ToLower(job.Ref)
	created := false
	var existing string
	if err := transaction.QueryRow("SELECT ref FROM batch_refs WHERE batch = ?", job.Batch).Scan(&existing); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		created = true
	} else if !strings.EqualFold(existing, job.Ref) {
		return false, fmt.Errorf("batch %s has unexpected recorded ref", job.Batch)
	}
	actual, err := gitAt(ctx, job.checkoutRoot, "rev-parse", "--verify", batchRef(job.Batch)+"^{commit}")
	if err == nil {
		if !strings.EqualFold(strings.TrimSpace(actual), job.Ref) {
			return false, fmt.Errorf("batch %s ref changed unexpectedly", job.Batch)
		}
	} else {
		created = true
	}
	if _, err := transaction.Exec("INSERT OR REPLACE INTO batch_refs(batch, checkout_root, ref) VALUES (?, ?, ?)", job.Batch, job.checkoutRoot, job.Ref); err != nil {
		return false, err
	}
	if _, err := gitAt(ctx, job.checkoutRoot, "update-ref", batchRef(job.Batch), job.Ref); err != nil {
		return false, fmt.Errorf("pin recorded checkout SHA: %w", err)
	}
	return created, nil
}

func (controller *Controller) removeBatchRefIfOwned(job Job) error {
	var protected int
	if err := controller.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE batch = ? AND (state IN ('queued', 'running') OR cleanup_pending = 1)", job.Batch).Scan(&protected); err != nil || protected != 0 {
		return err
	}
	actual, err := gitAt(context.Background(), job.checkoutRoot, "rev-parse", "--verify", batchRef(job.Batch)+"^{commit}")
	if err != nil || !strings.EqualFold(strings.TrimSpace(actual), job.Ref) {
		return nil
	}
	_, err = gitAt(context.Background(), job.checkoutRoot, "update-ref", "-d", batchRef(job.Batch))
	return err
}

func (controller *Controller) ready(transaction *sql.Tx, job Job, task Task) (bool, bool, error) {
	for _, need := range task.Needs {
		var state string
		err := transaction.QueryRow("SELECT state FROM jobs WHERE batch = ? AND task = ?", job.Batch, need).Scan(&state)
		if err != nil {
			return false, false, err
		}
		if state == "failed" || state == "cancelled" {
			return false, true, nil
		}
		if state != "passed" {
			return false, false, nil
		}
	}
	return true, false, nil
}

func (controller *Controller) resourcesFree(transaction *sql.Tx, task Task, config Config) (bool, error) {
	snapshots := map[string]Config{}
	for _, resource := range task.Resources {
		rows, err := transaction.Query("SELECT COALESCE(jobs.config_json, '') FROM leases JOIN jobs ON jobs.id = leases.job_id WHERE leases.resource = ?", resource)
		if err != nil {
			return false, err
		}
		limit := config.Resources[resource]
		held := 0
		for rows.Next() {
			var snapshot string
			if err := rows.Scan(&snapshot); err != nil {
				rows.Close()
				return false, err
			}
			held++
			leaseConfig, ok := snapshots[snapshot]
			if !ok {
				var err error
				leaseConfig, err = controller.snapshotConfig(snapshot)
				if err != nil {
					rows.Close()
					return false, err
				}
				snapshots[snapshot] = leaseConfig
			}
			if leaseConfig.Resources[resource] < limit {
				limit = leaseConfig.Resources[resource]
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return false, err
		}
		if err := rows.Close(); err != nil {
			return false, err
		}
		if held >= limit {
			return false, nil
		}
	}
	return true, nil
}

func (controller *Controller) jobConfig(job Job) (Config, Task, error) {
	config, err := controller.snapshotConfig(job.configJSON)
	if err != nil {
		return Config{}, Task{}, err
	}
	task, ok := config.Tasks[job.Task]
	if !ok {
		return Config{}, Task{}, fmt.Errorf("queued job references unknown task %q", job.Task)
	}
	return config, task, nil
}

func (controller *Controller) snapshotConfig(snapshot string) (Config, error) {
	if snapshot == "" {
		return controller.configSnapshot(), nil
	}
	var config Config
	if err := json.Unmarshal([]byte(snapshot), &config); err != nil {
		return Config{}, fmt.Errorf("decode queued job configuration: %w", err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate queued job configuration: %w", err)
	}
	return config, nil
}

func (controller *Controller) isCheckoutExecutable(command, checkoutRoot, worktreeRoot, workDir string, environment []string) bool {
	path := command
	relativeCheckoutPath := false
	if !filepath.IsAbs(path) {
		if strings.ContainsRune(path, filepath.Separator) {
			relativeCheckoutPath = true
			path = workDir + string(filepath.Separator) + path
			if worktreeRoot != "" && !pathWithin(worktreeRoot, path) {
				return true
			}
		} else {
			resolved, err := lookPath(command, environment, workDir)
			if err != nil {
				return false
			}
			path = resolved
			if !filepath.IsAbs(path) {
				path, err = filepath.Abs(path)
				if err != nil {
					return false
				}
			}
		}
	}
	if checkoutRoot == "" {
		checkoutRoot = controller.gitDirectory()
	}
	root := filepath.Clean(checkoutRoot)
	path = filepath.Clean(path)
	insideWorktree := worktreeRoot != "" && pathWithin(worktreeRoot, path)
	if pathWithin(root, path) && !insideWorktree {
		return true
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	if pathOrAncestorWithin(root, path, worktreeRoot) {
		return true
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	if relativeCheckoutPath && worktreeRoot != "" && !pathWithin(resolvedPathForComparison(worktreeRoot), resolvedPathForComparison(path)) {
		return true
	}
	if worktreeRoot != "" && pathWithin(worktreeRoot, path) {
		return !pathWithin(resolvedPathForComparison(worktreeRoot), resolvedPath)
	}
	return pathWithin(root, resolvedPath)
}

func (controller *Controller) hasUnsafeCheckoutCommand(argv []string, checkoutRoot, worktreeRoot, workDir string, overrides map[string]string) bool {
	if checkoutRoot == "" {
		checkoutRoot = controller.gitDirectory()
	}
	environment := taskEnvironment(overrides, workDir)
	if unsafeEvaluatorCommand(argv) || unsafeCommandEnvironment(argv, checkoutRoot) || unsafeTaskEnvironment(environment, overrides, checkoutRoot, worktreeRoot, workDir) || unsafeGitCommand(argv) {
		return true
	}
	if script, ok := interpreterScriptOperand(argv); ok && controller.isCheckoutScript(script, checkoutRoot, worktreeRoot, workDir, environment) {
		return true
	}
	for _, value := range argv {
		if containsCheckoutPath(value, checkoutRoot) || containsCheckoutPath(value, filepath.Dir(controller.configPath)) || unsafePathOperand(value, checkoutRoot, worktreeRoot, workDir) || controller.isCheckoutExecutable(value, checkoutRoot, worktreeRoot, workDir, environment) {
			return true
		}
	}
	return false
}

func (controller *Controller) hasUnsafePrepareCommand(argv []string, checkoutRoot, worktreeRoot, workDir string) bool {
	if controller.hasUnsafeCheckoutCommand(argv, checkoutRoot, worktreeRoot, workDir, nil) {
		return true
	}
	if worktreeRoot == "" || len(argv) == 0 {
		return false
	}
	return interpreterCommand(argv) || filepath.IsAbs(argv[0]) || strings.ContainsRune(argv[0], filepath.Separator)
}

func (controller *Controller) isCheckoutScript(script, checkoutRoot, worktreeRoot, workDir string, environment []string) bool {
	if !filepath.IsAbs(script) {
		script = filepath.Join(workDir, script)
		if worktreeRoot != "" && !pathWithin(worktreeRoot, script) {
			return true
		}
	}
	return controller.isCheckoutExecutable(script, checkoutRoot, worktreeRoot, workDir, environment)
}

func containsCheckoutPath(value, checkoutRoot string) bool {
	checkoutRoot = filepath.Clean(checkoutRoot)
	value = strings.ToLower(value)
	checkoutRoot = strings.ToLower(checkoutRoot)
	return strings.Contains(value, checkoutRoot+string(filepath.Separator)) || value == checkoutRoot
}

func unsafeEvaluatorCommand(argv []string) bool {
	for index, value := range argv {
		if strings.EqualFold(filepath.Base(value), "env") {
			for _, argument := range argv[index+1:] {
				if argument == "-S" || argument == "--split-string" || argument == "-C" || argument == "--chdir" || strings.HasPrefix(argument, "-C") || strings.HasPrefix(argument, "--chdir=") {
					return true
				}
			}
		}
		if !isInterpreter(value) {
			continue
		}
		for _, argument := range argv[index+1:] {
			if interpreterEvaluatorOption(argument) {
				return true
			}
		}
	}
	return false
}

func interpreterEvaluatorOption(argument string) bool {
	return argument == "-c" || argument == "-e" || argument == "--command" || argument == "--eval" ||
		(strings.HasPrefix(argument, "-c") && len(argument) > 2) ||
		(strings.HasPrefix(argument, "-e") && len(argument) > 2) ||
		strings.HasPrefix(argument, "--command=") || strings.HasPrefix(argument, "--eval=")
}

func unsafeCommandEnvironment(argv []string, checkoutRoot string) bool {
	for index, command := range argv {
		if !strings.EqualFold(filepath.Base(command), "env") {
			continue
		}
		for _, argument := range argv[index+1:] {
			name, value, ok := strings.Cut(argument, "=")
			if !ok {
				continue
			}
			upper := strings.ToUpper(name)
			if strings.HasPrefix(upper, "LD_") || strings.HasPrefix(upper, "DYLD_") || unsafeGitEnvironment(upper, value) || containsCheckoutPath(value, checkoutRoot) || pathHasParentTraversal(value) {
				return true
			}
		}
	}
	return false
}

func unsafePathOperand(value, checkoutRoot, worktreeRoot, workDir string) bool {
	if value == "" {
		return false
	}
	if strings.HasPrefix(value, "-") {
		_, optionValue, ok := strings.Cut(value, "=")
		if !ok {
			if strings.ContainsAny(value, `/\\`) {
				return true
			}
			for index := 2; index < len(value); index++ {
				if unsafePathOperand(value[index:], checkoutRoot, worktreeRoot, workDir) {
					return true
				}
			}
			return false
		}
		if optionValue == "" {
			return false
		}
		value = optionValue
	}
	if name, optionValue, ok := strings.Cut(value, "="); ok {
		name = strings.ToLower(strings.TrimLeft(name, "-"))
		switch name {
		case "if", "of", "input", "output", "file", "filename", "path", "target", "target-directory", "directory", "dir", "dest", "destination", "prefix":
			value = optionValue
		}
	}
	path := value
	relative := !filepath.IsAbs(path)
	if relative {
		path = filepath.Join(workDir, path)
	}
	if relative && !strings.ContainsRune(value, filepath.Separator) {
		if _, err := os.Lstat(path); err != nil {
			return !os.IsNotExist(err)
		}
	}
	path = filepath.Clean(path)
	insideWorktree := worktreeRoot != "" && pathWithin(worktreeRoot, path)
	if relative && worktreeRoot != "" && !insideWorktree {
		return true
	}
	if pathWithin(checkoutRoot, path) && !insideWorktree {
		return true
	}
	return insideWorktree && !pathWithin(resolvedPathForComparison(worktreeRoot), resolvedPathForComparison(path))
}

func interpreterScriptOperand(argv []string) (string, bool) {
	if !interpreterCommand(argv) {
		return "", false
	}
	start := 1
	if strings.EqualFold(filepath.Base(argv[0]), "busybox") {
		start = 2
	}
	for _, value := range argv[start:] {
		if interpreterEvaluatorOption(value) {
			return "", false
		}
		if value == "--" {
			continue
		}
		if strings.HasPrefix(value, "-") {
			continue
		}
		return value, true
	}
	return "", false
}

func isInterpreter(command string) bool {
	base := strings.ToLower(filepath.Base(command))
	for _, interpreter := range []string{"ash", "bash", "dash", "fish", "ksh", "node", "nodejs", "perl", "php", "python", "ruby", "sh", "zsh"} {
		if base == interpreter {
			return true
		}
		if strings.HasPrefix(base, interpreter) {
			suffix := strings.TrimPrefix(base, interpreter)
			if suffix != "" && strings.Trim(suffix, ".0123456789") == "" {
				return true
			}
		}
	}
	return false
}

func interpreterCommand(argv []string) bool {
	if len(argv) < 2 {
		return false
	}
	if isInterpreter(argv[0]) {
		return true
	}
	return strings.EqualFold(filepath.Base(argv[0]), "busybox") && len(argv) > 1 && isInterpreter(argv[1])
}

func unsafeTaskEnvironment(environment []string, overrides map[string]string, checkoutRoot, worktreeRoot, workDir string) bool {
	for _, item := range environment {
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		upper := strings.ToUpper(name)
		_, overridden := overrides[name]
		unsafeLoader := strings.HasPrefix(upper, "LD_") || strings.HasPrefix(upper, "DYLD_")
		unsafeConfiguredPath := overridden && upper != "PWD" && upper != "OLDPWD" && containsCheckoutPath(value, checkoutRoot)
		unsafePath := upper == "PATH" && unsafeTaskPath(value, checkoutRoot, worktreeRoot, workDir)
		if unsafeLoader || unsafeGitEnvironment(upper, value) || unsafeConfiguredPath || unsafePath {
			return true
		}
	}
	return false
}

func unsafeTaskPath(value, checkoutRoot, worktreeRoot, workDir string) bool {
	root := resolvedPathForComparison(checkoutRoot)
	worktree := resolvedPathForComparison(worktreeRoot)
	for _, entry := range filepath.SplitList(value) {
		path := entry
		if path == "" {
			path = workDir
		} else if !filepath.IsAbs(path) {
			path = filepath.Join(workDir, path)
		}
		path = filepath.Clean(path)
		resolved := resolvedPathForComparison(path)
		insideWorktree := worktreeRoot != "" && pathWithin(worktreeRoot, path)
		if pathWithin(checkoutRoot, path) && !insideWorktree {
			return true
		}
		if worktreeRoot != "" && insideWorktree && !pathWithin(worktree, resolved) {
			return true
		}
		if pathWithin(root, resolved) && (worktreeRoot == "" || !pathWithin(worktree, resolved)) {
			return true
		}
	}
	return false
}

func unsafeGitCommand(argv []string) bool {
	readOnly := map[string]bool{"annotate": true, "blame": true, "cat-file": true, "check-attr": true, "check-ignore": true, "check-mailmap": true, "check-ref-format": true, "count-objects": true, "describe": true, "diff": true, "diff-files": true, "diff-index": true, "diff-tree": true, "for-each-ref": true, "grep": true, "log": true, "ls-files": true, "ls-remote": true, "ls-tree": true, "merge-base": true, "name-rev": true, "rev-list": true, "rev-parse": true, "show": true, "show-ref": true, "status": true, "verify-commit": true, "verify-pack": true, "verify-tag": true, "version": true, "whatchanged": true}
	safeGlobal := map[string]bool{"--glob-pathspecs": true, "--help": true, "--icase-pathspecs": true, "--literal-pathspecs": true, "--no-optional-locks": true, "--no-pager": true, "--noglob-pathspecs": true, "--version": true, "-P": true}
	unsafeOption := map[string]bool{"--ext-diff": true, "--filters": true, "--open-files-in-pager": true, "--output": true, "--receive-pack": true, "--textconv": true, "--upload-pack": true}
	for index, value := range argv {
		if !strings.EqualFold(filepath.Base(value), "git") {
			continue
		}
		foundSubcommand := false
		for _, argument := range argv[index+1:] {
			if strings.HasPrefix(argument, "-") {
				name, _, _ := strings.Cut(argument, "=")
				if !foundSubcommand && !safeGlobal[name] {
					return true
				}
				if foundSubcommand && unsafeOption[name] {
					return true
				}
				continue
			}
			if !foundSubcommand && !readOnly[argument] {
				return true
			}
			foundSubcommand = true
		}
	}
	return false
}

func unsafeGitEnvironment(name, value string) bool {
	if !strings.HasPrefix(name, "GIT_") {
		return false
	}
	pathVariables := map[string]bool{
		"GIT_ALTERNATE_OBJECT_DIRECTORIES": true,
		"GIT_CEILING_DIRECTORIES":          true,
		"GIT_COMMON_DIR":                   true,
		"GIT_CONFIG_GLOBAL":                true,
		"GIT_CONFIG_SYSTEM":                true,
		"GIT_DIR":                          true,
		"GIT_INDEX_FILE":                   true,
		"GIT_OBJECT_DIRECTORY":             true,
		"GIT_WORK_TREE":                    true,
	}
	return pathVariables[name] || gitExecutionEnvironment(name) || filepath.IsAbs(value) || strings.ContainsAny(value, "/\\") || pathHasParentTraversal(value)
}

func gitExecutionEnvironment(name string) bool {
	switch name {
	case "GIT_ASKPASS", "GIT_CONFIG_COUNT", "GIT_CONFIG_PARAMETERS", "GIT_EDITOR", "GIT_EXTERNAL_DIFF", "GIT_PAGER", "GIT_PROXY_COMMAND", "GIT_SEQUENCE_EDITOR", "GIT_SSH", "GIT_SSH_COMMAND":
		return true
	default:
		return strings.HasPrefix(name, "GIT_CONFIG_KEY_") || strings.HasPrefix(name, "GIT_CONFIG_VALUE_")
	}
}

func pathHasParentTraversal(path string) bool {
	for _, part := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return true
		}
	}
	return false
}

func lookPath(file string, environment []string, base string) (string, error) {
	if len(environment) == 0 {
		return exec.LookPath(file)
	}
	path := ""
	for _, value := range environment {
		if name, value, ok := strings.Cut(value, "="); ok && name == "PATH" {
			path = value
			break
		}
	}
	for _, directory := range filepath.SplitList(path) {
		if directory == "" {
			directory = base
		} else if !filepath.IsAbs(directory) {
			directory = filepath.Join(base, directory)
		}
		candidate := filepath.Join(directory, file)
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return candidate, nil
		}
	}
	return "", exec.ErrNotFound
}

func pathOrAncestorWithin(root, path, worktreeRoot string) bool {
	for current := path; ; current = filepath.Dir(current) {
		if resolved, err := filepath.EvalSymlinks(current); err == nil {
			if worktreeRoot != "" && samePath(resolved, worktreeRoot) {
				return false
			}
			if pathWithin(root, resolved) && (worktreeRoot == "" || !pathWithin(resolvedPathForComparison(worktreeRoot), resolved)) {
				return true
			}
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
	}
}

func pathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return true
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return false
	}
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if info, err := os.Stat(current); err == nil && os.SameFile(rootInfo, info) {
			return true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return false
		}
	}
}

func (controller *Controller) startLog(job *Job) (*os.File, error) {
	if err := controller.pruneLogs(); err != nil {
		return nil, err
	}
	job.LogPath = filepath.Join(controller.stateDir, "logs", fmt.Sprintf("job-%d.log", job.ID))
	if err := os.MkdirAll(filepath.Dir(job.LogPath), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(job.LogPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err := controller.db.Exec("UPDATE jobs SET log_path = ? WHERE id = ?", job.LogPath, job.ID); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func (controller *Controller) writeJobLogHeader(output io.Writer, job Job, task Task, maxWorkers int) error {
	resources := strings.Join(task.Resources, ",")
	diskFree := "unknown"
	if free, err := controller.diskFree(); err == nil {
		diskFree = strconv.FormatUint(free, 10)
	}
	_, err := fmt.Fprintf(output, "BitCI job id=%d task=%s submitted_ref=%s ref=%s timeout_seconds=%d max_workers=%d resources=%s disk_free_bytes=%s\n", job.ID, job.Task, job.SubmittedRef, job.Ref, task.Timeout, maxWorkers, resources, diskFree)
	return err
}

func (controller *Controller) pruneLogs() error {
	config := controller.configSnapshot()
	if config.LogRetention == 0 {
		return nil
	}
	rows, err := controller.db.Query("SELECT id, COALESCE(log_path, '') FROM jobs WHERE state IN ('passed', 'failed', 'cancelled') AND COALESCE(log_path, '') != '' ORDER BY finished_at DESC, id DESC LIMIT -1 OFFSET ?", config.LogRetention)
	if err != nil {
		return err
	}
	type logRecord struct {
		id   int64
		path string
	}
	var records []logRecord
	for rows.Next() {
		var record logRecord
		if err := rows.Scan(&record.id, &record.path); err != nil {
			rows.Close()
			return err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, record := range records {
		if err := os.Remove(record.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		if _, err := controller.db.Exec("UPDATE jobs SET log_path = NULL WHERE id = ?", record.id); err != nil {
			return err
		}
	}
	return nil
}

func (controller *Controller) execute(parent context.Context, task Task, output io.Writer, directory string) int {
	return controller.executeCommandWithEnvForJob(parent, 0, task.Run, task.Timeout, output, directory, task.Env)
}

func (controller *Controller) executeCommand(parent context.Context, argv []string, timeout int, output io.Writer, directory string) int {
	return controller.executeCommandWithEnvForJob(parent, 0, argv, timeout, output, directory, nil)
}

func (controller *Controller) executeCommandWithEnv(parent context.Context, argv []string, timeout int, output io.Writer, directory string, environment map[string]string) int {
	return controller.executeCommandWithEnvForJob(parent, 0, argv, timeout, output, directory, environment)
}

func (controller *Controller) executeForJob(jobID int64, parent context.Context, task Task, output io.Writer, directory string) int {
	return controller.executeCommandWithEnvForJob(parent, jobID, task.Run, task.Timeout, output, directory, task.Env)
}

func (controller *Controller) executeCommandForJob(parent context.Context, jobID int64, argv []string, timeout int, output io.Writer, directory string) int {
	return controller.executeCommandWithEnvForJob(parent, jobID, argv, timeout, output, directory, nil)
}

func (controller *Controller) executeCommandWithEnvForJob(parent context.Context, jobID int64, argv []string, timeout int, output io.Writer, directory string, environment map[string]string) int {
	if len(argv) == 0 || argv[0] == "" {
		fmt.Fprintln(output, "BitCI job has no configured command")
		return 127
	}
	ctx := parent
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	env := taskEnvironment(environment, directory)
	program := argv[0]
	if !strings.ContainsRune(program, filepath.Separator) {
		resolved, err := lookPath(program, env, directory)
		if err != nil {
			fmt.Fprintf(output, "BitCI could not start task: %v\n", err)
			return 127
		}
		program = resolved
	}
	command := exec.CommandContext(ctx, program, argv[1:]...)
	command.Dir = directory
	command.Env = env
	if output == io.Discard {
		// A non-file writer makes os/exec create pipes. A background descendant
		// can keep those pipes open after the task exits and block Wait.
		command.Stdout = nil
		command.Stderr = nil
	} else {
		command.Stdout = output
		command.Stderr = output
	}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		fmt.Fprintf(output, "BitCI could not start task: %v\n", err)
		return 127
	}
	processPath, persistProcess := controller.processGroupPath(directory)
	if persistProcess {
		if err := os.WriteFile(processPath, []byte(strconv.Itoa(command.Process.Pid)), 0o600); err != nil {
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			_ = command.Wait()
			fmt.Fprintf(output, "BitCI could not record task process group: %v\n", err)
			return 127
		}
	}
	if jobID > 0 {
		result, err := controller.db.Exec("UPDATE jobs SET worker_pid = ? WHERE id = ? AND state = 'running'", command.Process.Pid, jobID)
		if err == nil {
			updated, rowsErr := result.RowsAffected()
			if rowsErr != nil {
				err = rowsErr
			} else if updated != 1 {
				err = fmt.Errorf("job %d is no longer running", jobID)
			}
		}
		if err != nil {
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			_ = command.Wait()
			if persistProcess {
				_ = os.Remove(processPath)
			}
			fmt.Fprintf(output, "BitCI could not record task process group: %v\n", err)
			return 127
		}
	}
	err := command.Wait()
	groupErr := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	if persistProcess && (groupErr == nil || errors.Is(groupErr, syscall.ESRCH)) {
		_ = os.Remove(processPath)
	}
	if groupErr != nil && !errors.Is(groupErr, syscall.ESRCH) {
		fmt.Fprintf(output, "BitCI could not stop task descendants: %v\n", groupErr)
		if err == nil {
			writeTaskExitCode(output, 127)
			return 127
		}
	}
	if jobID > 0 {
		if _, clearErr := controller.db.Exec("UPDATE jobs SET worker_pid = NULL WHERE id = ? AND worker_pid = ?", jobID, command.Process.Pid); clearErr != nil && err == nil {
			fmt.Fprintf(output, "BitCI could not clear task process group: %v\n", clearErr)
			writeTaskExitCode(output, 127)
			return 127
		}
	}
	if err == nil {
		writeTaskExitCode(output, 0)
		return 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		code := exitError.ExitCode()
		writeTaskExitCode(output, code)
		return code
	}
	fmt.Fprintf(output, "BitCI could not start task: %v\n", err)
	writeTaskExitCode(output, 127)
	return 127
}

func writeTaskExitCode(output io.Writer, code int) {
	fmt.Fprintf(output, "\nBitCI task_exit_code=%d\n", code)
}

func (controller *Controller) processGroupPath(directory string) (string, bool) {
	if controller.stateDir == "" {
		return "", false
	}
	root := filepath.Join(controller.stateDir, "worktrees")
	relative, err := filepath.Rel(root, filepath.Clean(directory))
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	parts := strings.Split(relative, string(filepath.Separator))
	if len(parts) == 0 || !strings.HasPrefix(parts[0], "job-") {
		return "", false
	}
	if _, err := strconv.ParseInt(strings.TrimPrefix(parts[0], "job-"), 10, 64); err != nil {
		return "", false
	}
	jobRoot := filepath.Join(root, parts[0])
	info, err := os.Lstat(jobRoot)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", false
	}
	return filepath.Join(jobRoot, "bitci-process-group"), true
}

func (controller *Controller) terminateJobProcessGroup(id int64) error {
	path := filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", id), "bitci-process-group")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid < 1 {
		return fmt.Errorf("invalid recorded job process group")
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("stop recorded job process group: %w", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := syscall.Kill(-pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return os.Remove(path)
		}
		if err == nil || errors.Is(err, syscall.EPERM) {
			output, psErr := exec.Command("ps", "-o", "pid=,pgid=,stat=", "-g", strconv.Itoa(pid)).Output()
			if psErr != nil || allProcessGroupEntriesZombie(output) {
				return os.Remove(path)
			}
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("check recorded job process group: %w", err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("recorded job process group %d did not stop", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func allProcessGroupEntriesZombie(output []byte) bool {
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return true
	}
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 || !strings.HasPrefix(fields[2], "Z") {
			return false
		}
	}
	return true
}

func taskEnvironment(overrides map[string]string, directory string) []string {
	values := map[string]string{}
	for _, value := range os.Environ() {
		name, value, ok := strings.Cut(value, "=")
		if ok && !gitExecutionEnvironment(strings.ToUpper(name)) {
			values[name] = value
		}
	}
	for name, value := range overrides {
		values[name] = value
	}
	values["PWD"] = directory
	values["OLDPWD"] = directory
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	environment := make([]string, 0, len(names))
	for _, name := range names {
		environment = append(environment, name+"="+values[name])
	}
	return environment
}

func (controller *Controller) finish(job Job, code int, cleanupPending bool) error {
	state := "passed"
	if code != 0 {
		state = "failed"
	}
	now := timestamp()
	result, err := controller.db.Exec(
		"UPDATE jobs SET state = ?, finished_at = ?, exit_code = ?, log_path = ?, cleanup_pending = ?, duration_seconds = CASE WHEN started_at IS NULL THEN 0 ELSE MAX(0, CAST(strftime('%s', ?) - strftime('%s', started_at) AS INTEGER)) END, terminal_result = ?, worker_pid = NULL WHERE id = ? AND state IN ('queued', 'running')",
		state, now, code, job.LogPath, cleanupPending, now, state, job.ID,
	)
	if err != nil {
		return err
	}
	if updated, err := result.RowsAffected(); err != nil {
		return err
	} else if updated == 0 {
		var recoveredState string
		var recoveredCode int
		var recoveredPID *int
		if err := controller.db.QueryRow("SELECT state, COALESCE(exit_code, 0), worker_pid FROM jobs WHERE id = ?", job.ID).Scan(&recoveredState, &recoveredCode, &recoveredPID); err != nil {
			return err
		}
		if recoveredState == "failed" && recoveredCode == 125 && recoveredPID == nil {
			return nil
		}
		return fmt.Errorf("job %d is no longer finishable", job.ID)
	}
	terminalLogErr := appendTerminalLog(job.LogPath, state, code)
	pruneErr := controller.pruneLogs()
	_, leaseErr := controller.db.Exec("DELETE FROM leases WHERE job_id = ?", job.ID)
	refErr := controller.releaseBatchRef(job.Batch, job.checkoutRoot)
	cacheErr := controller.pruneObjectCaches()
	return errors.Join(terminalLogErr, pruneErr, leaseErr, refErr, cacheErr)
}

func appendTerminalLog(path, state string, code int) error {
	if path == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(file, "BitCI terminal_state=%s exit_code=%d\n", state, code)
	closeErr := file.Close()
	return errors.Join(writeErr, closeErr)
}

func (controller *Controller) Jobs() ([]Job, error) {
	rows, err := controller.db.Query("SELECT id, batch, task, COALESCE(submitted_ref, ''), ref, COALESCE(tested_sha, ''), state, created_at, COALESCE(started_at, ''), COALESCE(finished_at, ''), exit_code, COALESCE(log_path, ''), COALESCE(config_json, ''), COALESCE(attempt, 1), retry_of, COALESCE(retry_root, id), prior_exit_code, COALESCE(queue_wait_seconds, 0), COALESCE(duration_seconds, 0), COALESCE(terminal_result, ''), worker_pid FROM jobs ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.SubmittedRef, &job.Ref, &job.TestedSHA, &job.State, &job.CreatedAt, &job.StartedAt, &job.FinishedAt, &job.ExitCode, &job.LogPath, &job.configJSON, &job.Attempt, &job.RetryOf, &job.RetryRoot, &job.PriorExitCode, &job.QueueWaitSeconds, &job.DurationSeconds, &job.TerminalResult, &job.WorkerPID); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (controller *Controller) Cancel(id int64) (bool, error) {
	return controller.CancelContext(context.Background(), id)
}

func (controller *Controller) CancelContext(ctx context.Context, id int64) (bool, error) {
	var batch, checkoutRoot string
	if err := controller.db.QueryRow("SELECT batch, COALESCE(checkout_root, '') FROM jobs WHERE id = ?", id).Scan(&batch, &checkoutRoot); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	now := timestamp()
	result, err := controller.db.Exec(
		"UPDATE jobs SET state = 'cancelled', finished_at = ?, queue_wait_seconds = MAX(0, CAST(strftime('%s', ?) - strftime('%s', created_at) AS INTEGER)), duration_seconds = 0, terminal_result = 'cancelled' WHERE id = ? AND state = 'queued'",
		now, now, id,
	)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return false, err
	}
	return true, controller.releaseBatchRefContext(ctx, batch, checkoutRoot)
}

func (controller *Controller) Retry(id int64) ([]Job, error) {
	return controller.RetryContext(context.Background(), id)
}

func (controller *Controller) RetryContext(ctx context.Context, id int64) ([]Job, error) {
	controller.retryMu.Lock()
	defer controller.retryMu.Unlock()
	var batch, task, ref, submittedRef, configJSON, checkoutRoot, configRelative, state string
	if err := controller.db.QueryRow("SELECT batch, task, ref, COALESCE(submitted_ref, ''), COALESCE(config_json, ''), COALESCE(checkout_root, ''), COALESCE(config_relative, ''), state FROM jobs WHERE id = ?", id).Scan(&batch, &task, &ref, &submittedRef, &configJSON, &checkoutRoot, &configRelative, &state); err != nil {
		return nil, err
	}
	if submittedRef == "" {
		submittedRef = ref
	}
	if state == "queued" || state == "running" {
		return nil, fmt.Errorf("job %d is not finished", id)
	}
	config := controller.configSnapshot()
	if configJSON != "" {
		config = Config{}
		if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
			return nil, fmt.Errorf("decode retried job configuration: %w", err)
		}
		if err := config.Validate(); err != nil {
			return nil, fmt.Errorf("validate retried job configuration: %w", err)
		}
	}
	if isCheckoutSHA(ref) && checkoutRoot == "" {
		var err error
		checkoutRoot, configRelative, err = controller.checkoutLocation()
		if err != nil {
			return nil, fmt.Errorf("find checkout for retried SHA: %w", err)
		}
		if err := recordedDirectoryExists(checkoutRoot, ref, configRelative); err != nil {
			return nil, fmt.Errorf("validate retried checkout configuration: %w", err)
		}
	}
	ordered, err := config.Ordered([]string{task})
	if err != nil {
		return nil, err
	}
	type history struct {
		id       int64
		attempt  int
		root     int64
		exitCode *int
	}
	historyByTask := map[string]history{}
	rows, err := controller.db.Query("SELECT id, task, COALESCE(attempt, 1), COALESCE(retry_root, id), exit_code FROM jobs WHERE batch = ?", batch)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var item history
		var name string
		if err := rows.Scan(&item.id, &name, &item.attempt, &item.root, &item.exitCode); err != nil {
			rows.Close()
			return nil, err
		}
		historyByTask[name] = item
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	retries := make(map[string]retryInfo, len(ordered))
	for _, name := range ordered {
		item, ok := historyByTask[name]
		if !ok {
			return nil, fmt.Errorf("retry history lacks task %q", name)
		}
		var latest history
		var latestState string
		if err := controller.db.QueryRow("SELECT id, COALESCE(attempt, 1), exit_code, state FROM jobs WHERE COALESCE(retry_root, id) = ? ORDER BY COALESCE(attempt, 1) DESC, id DESC LIMIT 1", item.root).Scan(&latest.id, &latest.attempt, &latest.exitCode, &latestState); err != nil {
			return nil, err
		}
		if latestState == "queued" || latestState == "running" {
			return nil, fmt.Errorf("task %q already has a pending retry", name)
		}
		if config.Tasks[name].MaxRetries > 0 && latest.attempt > config.Tasks[name].MaxRetries {
			return nil, fmt.Errorf("task %q reached max_retries %d", name, config.Tasks[name].MaxRetries)
		}
		retries[name] = retryInfo{Attempt: latest.attempt + 1, RetryOf: latest.id, RetryRoot: item.root, PriorExitCode: latest.exitCode}
	}
	return controller.submit(ctx, config, []string{task}, ref, submittedRef, checkoutRoot, configRelative, retries)
}

func (controller *Controller) TailLog(id int64, limit int) ([]string, error) {
	file, redact, err := controller.logFile(id)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return scanLog(file, limit, func(string) bool { return true }, redact)
}

func (controller *Controller) SearchLog(id int64, query string, limit int) ([]string, error) {
	if query == "" {
		return nil, fmt.Errorf("search query must not be empty")
	}
	file, redact, err := controller.logFile(id)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return scanLog(file, limit, func(line string) bool { return strings.Contains(line, query) }, redact)
}

const maxLogReadBytes = 1024 * 1024

type LogCursorOutput struct {
	Lines  []string `json:"lines"`
	Cursor int64    `json:"cursor"`
	State  string   `json:"state"`
}

// ReadLog returns capped complete lines. A terminal partial line is returned
// once. Oversized lines are skipped across bounded calls.
func (controller *Controller) ReadLog(id, cursor int64, limit int) (LogCursorOutput, error) {
	var logPath, state, configJSON string
	if err := controller.db.QueryRow("SELECT COALESCE(log_path, ''), state, COALESCE(config_json, '') FROM jobs WHERE id = ?", id).Scan(&logPath, &state, &configJSON); err != nil {
		return LogCursorOutput{}, err
	}
	output := LogCursorOutput{Lines: []string{}, Cursor: cursor, State: state}
	if cursor < 0 {
		return LogCursorOutput{}, fmt.Errorf("log cursor must not be negative")
	}
	if logPath == "" {
		return output, nil
	}
	redact, err := controller.logRedaction(configJSON)
	if err != nil {
		return LogCursorOutput{}, fmt.Errorf("load job log redaction: %w", err)
	}
	file, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return output, nil
		}
		return LogCursorOutput{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return LogCursorOutput{}, err
	}
	if cursor > info.Size() {
		return LogCursorOutput{}, fmt.Errorf("log cursor %d exceeds log size %d", cursor, info.Size())
	}
	if _, err := file.Seek(cursor, io.SeekStart); err != nil {
		return LogCursorOutput{}, err
	}
	reader := bufio.NewReader(file)
	readCursor := cursor
	skippingLine := false
	if cursor > 0 {
		var previous [1]byte
		if _, err := file.ReadAt(previous[:], cursor-1); err != nil {
			return LogCursorOutput{}, err
		}
		skippingLine = previous[0] != '\n'
	}
	limit = logLimit(limit)
	for len(output.Lines) < limit && readCursor-cursor < maxLogReadBytes {
		remaining := maxLogReadBytes - (readCursor - cursor)
		line, consumed, complete, err := readLogLine(reader, remaining)
		readCursor += consumed
		if consumed == 0 && err == io.EOF {
			break
		}
		if skippingLine {
			output.Cursor = readCursor
			if complete {
				skippingLine = false
				continue
			}
			break
		}
		if complete && consumed > remaining {
			output.Cursor = readCursor
			break
		}
		if complete {
			output.Lines = append(output.Lines, redactLogLine(strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), redact))
			output.Cursor = readCursor
		} else if len(line) > 0 && terminalJobState(state) && readCursor == info.Size() {
			if consumed <= remaining {
				output.Lines = append(output.Lines, redactLogLine(strings.TrimSuffix(line, "\r"), redact))
			}
			output.Cursor = readCursor
		}
		if !complete && !terminalJobState(state) && err == bufio.ErrBufferFull {
			output.Cursor = readCursor - consumed
			break
		}
		if err != nil {
			if err == bufio.ErrBufferFull {
				output.Cursor = readCursor
				break
			}
			if err == io.EOF {
				break
			}
			return LogCursorOutput{}, err
		}
	}
	return output, nil
}

func readLogLine(reader *bufio.Reader, maxBytes int64) (string, int64, bool, error) {
	var line []byte
	var consumed int64
	for {
		fragment, err := reader.ReadSlice('\n')
		consumed += int64(len(fragment))
		if consumed <= maxBytes {
			line = append(line, fragment...)
		}
		if err == bufio.ErrBufferFull {
			if consumed >= maxBytes {
				return string(line), consumed, false, bufio.ErrBufferFull
			}
			continue
		}
		return string(line), consumed, err == nil, err
	}
}

func terminalJobState(state string) bool {
	return state == "passed" || state == "failed" || state == "cancelled"
}

func (controller *Controller) logFile(id int64) (*os.File, []string, error) {
	var logPath, configJSON string
	if err := controller.db.QueryRow("SELECT COALESCE(log_path, ''), COALESCE(config_json, '') FROM jobs WHERE id = ?", id).Scan(&logPath, &configJSON); err != nil {
		return nil, nil, err
	}
	if logPath == "" {
		return nil, nil, fmt.Errorf("job %d has no log", id)
	}
	redact, err := controller.logRedaction(configJSON)
	if err != nil {
		return nil, nil, fmt.Errorf("load job log redaction: %w", err)
	}
	file, err := os.Open(logPath)
	if err != nil {
		return nil, nil, err
	}
	return file, redact, nil
}

func (controller *Controller) logRedaction(configJSON string) ([]string, error) {
	config, err := controller.snapshotConfig(configJSON)
	if err != nil {
		return nil, err
	}
	redact := append([]string{}, config.Redact...)
	redact = append(redact, controller.configSnapshot().Redact...)
	sort.SliceStable(redact, func(left, right int) bool { return len(redact[left]) > len(redact[right]) })
	return redact, nil
}

func scanLog(file *os.File, limit int, include func(string) bool, redact []string) ([]string, error) {
	limit = logLimit(limit)
	lines := make([]string, 0, limit)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !include(line) {
			continue
		}
		line = redactLogLine(line, redact)
		if len(lines) == limit {
			copy(lines, lines[1:])
			lines[len(lines)-1] = line
			continue
		}
		lines = append(lines, line)
	}
	return lines, scanner.Err()
}

func redactLogLine(line string, redact []string) string {
	for _, value := range redact {
		line = strings.ReplaceAll(line, value, "[REDACTED]")
	}
	return line
}

func logLimit(limit int) int {
	if limit < 1 {
		return 80
	}
	if limit > 80 {
		return 80
	}
	return limit
}

func (controller *Controller) DiskOK() error {
	return controller.diskOK(controller.configSnapshot().MinFreeBytes)
}

func (controller *Controller) diskOK(minFreeBytes uint64) error {
	if minFreeBytes == 0 {
		return nil
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(controller.stateDir, &stat); err != nil {
		return err
	}
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	if free < minFreeBytes {
		return fmt.Errorf("disk guard: %d bytes free, need %d", free, minFreeBytes)
	}
	return nil
}

func newBatch() (string, error) {
	bytes := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func SplitPaths(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
