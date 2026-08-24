package bitci

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
}

type Job struct {
	ID             int64  `json:"id"`
	Batch          string `json:"batch"`
	Task           string `json:"task"`
	Ref            string `json:"ref"`
	TestedSHA      string `json:"tested_sha,omitempty"`
	State          string `json:"state"`
	ExitCode       *int   `json:"exit_code,omitempty"`
	LogPath        string `json:"log_path,omitempty"`
	WorkerPID      *int   `json:"worker_pid,omitempty"`
	configJSON     string
	checkoutRoot   string
	configRelative string
	startedAt      string
}

func Open(configPath, stateDir string) (*Controller, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	absoluteConfig, err := filepath.Abs(configPath)
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
	absoluteConfig, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}
	configPath = absoluteConfig
	stateDir = DefaultStateDir(configPath, stateDir)
	absoluteState, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, err
	}
	stateDir = absoluteState
	if gitDirectory, err := gitCommonDirectory(configPath); err == nil && pathsOverlap(stateDir, gitDirectory) {
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
	if err := controller.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return controller, nil
}

func DefaultStateDir(configPath, stateDir string) string {
	if stateDir != "" {
		return stateDir
	}
	absoluteConfig, err := filepath.Abs(configPath)
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

func (controller *Controller) Close() error { return controller.db.Close() }

func (controller *Controller) Plan(changedPaths []string) ([]string, error) {
	return controller.config.Plan(changedPaths)
}

func (controller *Controller) migrate() error {
	_, err := controller.db.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY,
			batch TEXT NOT NULL,
			task TEXT NOT NULL,
			ref TEXT NOT NULL,
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
			worker_pid INTEGER
		);
		CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(state, id);
		CREATE TABLE IF NOT EXISTS leases (
			resource TEXT NOT NULL,
			job_id INTEGER NOT NULL REFERENCES jobs(id),
			PRIMARY KEY (resource, job_id)
		);
	`)
	if err != nil {
		return err
	}
	if err := controller.addJobColumn("tested_sha", "TEXT"); err != nil {
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
	if err := controller.addJobColumn("cleanup_pending", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	return controller.addJobColumn("worker_pid", "INTEGER")
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

func (controller *Controller) Submit(taskNames []string, ref string) ([]Job, error) {
	checkoutRoot, configRelative := "", ""
	if sha, err := controller.checkoutSHA(); err == nil {
		if isCheckoutSHA(ref) && ref != sha {
			return nil, fmt.Errorf("requested checkout SHA does not match checkout HEAD")
		}
		ref = sha
		var locationErr error
		checkoutRoot, configRelative, locationErr = controller.checkoutLocation()
		if locationErr != nil {
			return nil, locationErr
		}
	} else if isCheckoutSHA(ref) {
		return nil, fmt.Errorf("cannot submit requested checkout SHA: %w", err)
	}
	return controller.submit(controller.config, taskNames, ref, checkoutRoot, configRelative)
}

func (controller *Controller) submit(config Config, taskNames []string, ref, checkoutRoot, configRelative string) ([]Job, error) {
	ordered, err := config.Ordered(taskNames)
	if err != nil {
		return nil, err
	}
	batch, err := newBatch()
	if err != nil {
		return nil, err
	}
	pinnedRef := ""
	if checkoutRoot != "" && isCheckoutSHA(ref) {
		// A commit stored only in SQLite can disappear after a force-push and Git
		// garbage collection. Keep it reachable for the lifetime of its job records.
		if _, err := gitAt(context.Background(), checkoutRoot, "update-ref", "refs/bitci/jobs/"+batch, ref); err != nil {
			return nil, fmt.Errorf("pin recorded checkout SHA: %w", err)
		}
		pinnedRef = "refs/bitci/jobs/" + batch
	}
	transaction, err := controller.db.Begin()
	if err != nil {
		if pinnedRef != "" {
			_, _ = gitAt(context.Background(), checkoutRoot, "update-ref", "-d", pinnedRef)
		}
		return nil, err
	}
	committed := false
	defer func() {
		transaction.Rollback()
		if !committed && pinnedRef != "" {
			_, _ = gitAt(context.Background(), checkoutRoot, "update-ref", "-d", pinnedRef)
		}
	}()
	jobs := make([]Job, 0, len(ordered))
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	for _, taskName := range ordered {
		result, err := transaction.Exec(
			"INSERT INTO jobs(batch, task, ref, config_json, checkout_root, config_relative, state, created_at) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?)",
			batch, taskName, ref, configJSON, checkoutRoot, configRelative, time.Now().UTC().Format(time.RFC3339),
		)
		if err != nil {
			return nil, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, Job{ID: id, Batch: batch, Task: taskName, Ref: ref, State: "queued", checkoutRoot: checkoutRoot, configRelative: configRelative})
	}
	if err := transaction.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return jobs, nil
}

func (controller *Controller) RunOnce(ctx context.Context, maxWorkers int) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	job, claimed, err := controller.claim(maxWorkers)
	if err != nil || !claimed {
		return false, err
	}
	config, task, err := controller.jobConfig(job)
	if err != nil {
		return true, controller.finish(job, 127, false)
	}
	logFile, err := controller.startLog(&job)
	if err != nil {
		return true, controller.finish(job, 127, false)
	}
	workDir := filepath.Dir(controller.configPath)
	worktreeRoot := ""
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
	}
	code := 0
	if isCheckoutSHA(job.Ref) && len(config.Prepare) > 0 && controller.hasUnsafeCheckoutArgument(config.Prepare, job.checkoutRoot, worktreeRoot, workDir) {
		fmt.Fprintln(logFile, "BitCI refuses an unsafe checkout-local prepare argument for a recorded SHA job")
		code = 126
	} else {
		if isCheckoutSHA(job.Ref) && len(config.Prepare) > 0 {
			code = controller.executeCommand(ctx, job.ID, config.Prepare, task.Timeout, logFile, workDir)
		}
		if code == 0 && isCheckoutSHA(job.Ref) {
			sha, err := checkoutSHA(worktreeRoot)
			if err != nil || sha != job.Ref {
				fmt.Fprintln(logFile, "BitCI worktree SHA changed after prepare")
				code = 126
			}
		}
		if code == 0 && isCheckoutSHA(job.Ref) && controller.hasUnsafeCheckoutArgument(task.Run, job.checkoutRoot, worktreeRoot, workDir) {
			fmt.Fprintln(logFile, "BitCI refuses an unsafe checkout-local task argument for a recorded SHA job")
			code = 126
		}
		if code == 0 {
			code = controller.execute(job.ID, ctx, task, logFile, workDir)
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
	configDirectory, err := filepath.EvalSymlinks(filepath.Dir(configPath))
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
	relative, err := filepath.Rel(root, configDirectory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("config must be inside its Git checkout")
	}
	return root, relative, nil
}

func gitCommonDirectory(configPath string) (string, error) {
	configDirectory, err := filepath.EvalSymlinks(filepath.Dir(configPath))
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
	cleanup := func() error { return controller.removeJobWorktree(job.ID, checkoutRoot) }
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
	if _, err := gitAt(ctx, checkoutRoot, "worktree", "add", "--detach", path, job.Ref); err != nil {
		return "", cleanup, fmt.Errorf("create job worktree: %w", err)
	}
	sha, err := checkoutSHA(path)
	if err != nil || sha != job.Ref {
		return "", cleanup, fmt.Errorf("verify job worktree SHA")
	}
	if _, err := controller.db.Exec("UPDATE jobs SET tested_sha = ? WHERE id = ?", sha, job.ID); err != nil {
		return "", cleanup, err
	}
	return filepath.Join(path, configRelative), cleanup, nil
}

func isCheckoutSHA(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !('0' <= character && character <= '9' || 'a' <= character && character <= 'f') {
			return false
		}
	}
	return true
}

func (controller *Controller) Serve(ctx context.Context, maxWorkers int, interval time.Duration, socketPath string) error {
	if maxWorkers < 1 {
		return fmt.Errorf("max-workers must be positive")
	}
	listener, err := controller.Listen(socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := controller.RecoverInterrupted(); err != nil {
		return err
	}
	if interval <= 0 {
		interval = time.Second
	}
	errors := make(chan error, maxWorkers+2)
	go func() { errors <- controller.ServeRPC(ctx, listener) }()
	go controller.serveRecovery(ctx, interval, errors)
	for worker := 0; worker < maxWorkers; worker++ {
		go controller.serveWorker(ctx, maxWorkers, interval, errors)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errors:
			if err != nil {
				return err
			}
		}
	}
}

func (controller *Controller) serveRecovery(ctx context.Context, interval time.Duration, errors chan<- error) {
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
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

func (controller *Controller) serveWorker(ctx context.Context, maxWorkers int, interval time.Duration, errors chan<- error) {
	for {
		ran, err := controller.RunOnce(ctx, maxWorkers)
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
	transaction, err := controller.db.Begin()
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	rows, err := transaction.Query("SELECT id, ref, COALESCE(checkout_root, ''), state, cleanup_pending FROM jobs WHERE state = 'running' OR cleanup_pending = 1")
	if err != nil {
		return err
	}
	interrupted := make([]Job, 0)
	for rows.Next() {
		var job Job
		var cleanupPending int
		if err := rows.Scan(&job.ID, &job.Ref, &job.checkoutRoot, &job.State, &cleanupPending); err != nil {
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
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := transaction.Exec("UPDATE jobs SET state = 'cancelled', finished_at = ? WHERE state = 'queued' AND batch IN (SELECT DISTINCT batch FROM jobs WHERE state = 'running')", now); err != nil {
		return err
	}
	if _, err := transaction.Exec("DELETE FROM leases WHERE job_id IN (SELECT id FROM jobs WHERE state = 'running')"); err != nil {
		return err
	}
	if _, err := transaction.Exec("UPDATE jobs SET state = 'failed', finished_at = ?, exit_code = 125 WHERE state = 'running'", now); err != nil {
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
		if err := controller.removeJobWorktree(job.ID, job.checkoutRoot); err != nil {
			failures = append(failures, err)
			continue
		}
		if _, err := controller.db.Exec("UPDATE jobs SET cleanup_pending = 0 WHERE id = ?", job.ID); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// RecoverOrphaned fails running jobs whose recorded task process no longer exists.
// It never kills a process and leaves live jobs untouched.
const orphanRecoveryGrace = 5 * time.Second

func (controller *Controller) RecoverOrphaned() (int, error) {
	rows, err := controller.db.Query("SELECT id, batch, ref, COALESCE(checkout_root, ''), worker_pid, COALESCE(started_at, '') FROM jobs WHERE state = 'running' AND worker_pid IS NOT NULL")
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
		startedAt, err := time.Parse(time.RFC3339, job.startedAt)
		if err == nil && time.Since(startedAt) < orphanRecoveryGrace {
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
	return controller.recoverJobs(orphaned)
}

// RecoverJob fails one running job only when its recorded task process is gone.
func (controller *Controller) RecoverJob(id int64) (bool, error) {
	var job Job
	if err := controller.db.QueryRow("SELECT id, batch, ref, COALESCE(checkout_root, ''), worker_pid FROM jobs WHERE id = ? AND state = 'running'", id).Scan(&job.ID, &job.Batch, &job.Ref, &job.checkoutRoot, &job.WorkerPID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("job %d is not running", id)
		}
		return false, err
	}
	if job.WorkerPID == nil {
		return false, fmt.Errorf("job %d has no recorded task process", id)
	}
	alive, err := processAlive(*job.WorkerPID)
	if err != nil {
		return false, err
	}
	if alive {
		return false, fmt.Errorf("job %d task process is still running", id)
	}
	recovered, err := controller.recoverJobs([]Job{job})
	return recovered == 1, err
}

func (controller *Controller) recoverJobs(jobs []Job) (int, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	transaction, err := controller.db.Begin()
	if err != nil {
		return 0, err
	}
	defer transaction.Rollback()
	now := time.Now().UTC().Format(time.RFC3339)
	recovered := make([]Job, 0, len(jobs))
	for _, job := range jobs {
		result, err := transaction.Exec("UPDATE jobs SET state = 'failed', finished_at = ?, exit_code = 125, worker_pid = NULL, cleanup_pending = ? WHERE id = ? AND state = 'running' AND worker_pid = ?", now, isCheckoutSHA(job.Ref), job.ID, *job.WorkerPID)
		if err != nil {
			return 0, err
		}
		updated, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		if updated == 0 {
			return 0, fmt.Errorf("job %d changed before recovery", job.ID)
		}
		if _, err := transaction.Exec("DELETE FROM leases WHERE job_id = ?", job.ID); err != nil {
			return 0, err
		}
		if _, err := transaction.Exec("UPDATE jobs SET state = 'cancelled', finished_at = ? WHERE state = 'queued' AND batch = ?", now, job.Batch); err != nil {
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
		}
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

func (controller *Controller) removeJobWorktree(id int64, checkoutRoot string) error {
	controller.worktreeMu.Lock()
	defer controller.worktreeMu.Unlock()

	path := filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", id))
	if checkoutRoot == "" {
		checkoutRoot = controller.gitDirectory()
	}
	var failures []error
	checkoutMissing := false
	if _, err := os.Stat(checkoutRoot); os.IsNotExist(err) {
		checkoutMissing = true
	} else if err != nil {
		failures = append(failures, err)
	} else if topLevel, err := gitAt(context.Background(), checkoutRoot, "rev-parse", "--show-toplevel"); err != nil || resolvedPathForComparison(strings.TrimSpace(topLevel)) != resolvedPathForComparison(checkoutRoot) {
		checkoutMissing = true
	}
	if _, err := os.Lstat(path); err == nil {
		if !checkoutMissing {
			if _, err := gitAt(context.Background(), checkoutRoot, "worktree", "remove", "--force", path); err != nil {
				failures = append(failures, err)
			}
		}
	} else if !os.IsNotExist(err) {
		failures = append(failures, err)
	}
	if err := os.RemoveAll(path); err != nil {
		failures = append(failures, err)
	}
	if !checkoutMissing {
		if _, err := gitAt(context.Background(), checkoutRoot, "worktree", "prune"); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (controller *Controller) claim(maxWorkers int) (Job, bool, error) {
	if maxWorkers < 1 {
		return Job{}, false, fmt.Errorf("max-workers must be positive")
	}
	transaction, err := controller.db.Begin()
	if err != nil {
		return Job{}, false, err
	}
	defer transaction.Rollback()
	var active int
	if err := transaction.QueryRow("SELECT COUNT(*) FROM jobs WHERE state = 'running'").Scan(&active); err != nil {
		return Job{}, false, err
	}
	if active >= maxWorkers {
		return Job{}, false, nil
	}
	rows, err := transaction.Query("SELECT id, batch, task, ref, COALESCE(config_json, ''), COALESCE(checkout_root, ''), COALESCE(config_relative, ''), state FROM jobs WHERE state = 'queued' ORDER BY id")
	if err != nil {
		return Job{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.Ref, &job.configJSON, &job.checkoutRoot, &job.configRelative, &job.State); err != nil {
			return Job{}, false, err
		}
		config, task, err := controller.jobConfig(job)
		if err != nil {
			return Job{}, false, err
		}
		if err := controller.diskOK(config.MinFreeBytes); err != nil {
			return Job{}, false, err
		}
		if ready, err := controller.ready(transaction, job, task); err != nil || !ready {
			if err != nil {
				return Job{}, false, err
			}
			continue
		}
		if free, err := controller.resourcesFree(transaction, task, config); err != nil || !free {
			if err != nil {
				return Job{}, false, err
			}
			continue
		}
		if _, err := transaction.Exec("UPDATE jobs SET state = 'running', started_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339), job.ID); err != nil {
			return Job{}, false, err
		}
		for _, resource := range task.Resources {
			if _, err := transaction.Exec("INSERT INTO leases(resource, job_id) VALUES (?, ?)", resource, job.ID); err != nil {
				return Job{}, false, err
			}
		}
		job.State = "running"
		return job, true, transaction.Commit()
	}
	return Job{}, false, rows.Err()
}

func (controller *Controller) ready(transaction *sql.Tx, job Job, task Task) (bool, error) {
	for _, need := range task.Needs {
		var state string
		err := transaction.QueryRow("SELECT state FROM jobs WHERE batch = ? AND task = ?", job.Batch, need).Scan(&state)
		if err != nil {
			return false, err
		}
		if state != "passed" {
			return false, nil
		}
	}
	return true, nil
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
		return controller.config, nil
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

func (controller *Controller) isCheckoutExecutable(command, checkoutRoot, worktreeRoot, workDir string) bool {
	path := command
	if !filepath.IsAbs(path) {
		if strings.ContainsRune(path, filepath.Separator) {
			path = filepath.Clean(filepath.Join(workDir, path))
			if worktreeRoot != "" && !pathWithin(worktreeRoot, path) {
				return true
			}
		} else {
			resolved, err := exec.LookPath(command)
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
	if pathOrAncestorWithin(root, path) && !insideWorktree {
		return true
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	if worktreeRoot != "" {
		if resolvedWorktree, worktreeErr := filepath.EvalSymlinks(worktreeRoot); worktreeErr == nil && pathWithin(resolvedWorktree, resolvedPath) {
			return false
		}
	}
	return pathWithin(root, resolvedPath)
}

func (controller *Controller) hasUnsafeCheckoutArgument(argv []string, checkoutRoot, worktreeRoot, workDir string) bool {
	if checkoutRoot == "" {
		checkoutRoot = controller.gitDirectory()
	}
	for _, value := range argv {
		if containsCheckoutPath(value, checkoutRoot) || containsCheckoutPath(value, filepath.Dir(controller.configPath)) || controller.isCheckoutExecutable(value, checkoutRoot, worktreeRoot, workDir) {
			return true
		}
	}
	return false
}

func containsCheckoutPath(value, checkoutRoot string) bool {
	checkoutRoot = filepath.Clean(checkoutRoot)
	return strings.Contains(value, checkoutRoot+string(filepath.Separator)) || value == checkoutRoot
}

func pathOrAncestorWithin(root, path string) bool {
	for current := path; ; current = filepath.Dir(current) {
		if resolved, err := filepath.EvalSymlinks(current); err == nil && pathWithin(root, resolved) {
			return true
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

func (controller *Controller) pruneLogs() error {
	rows, err := controller.db.Query("SELECT id, COALESCE(log_path, '') FROM jobs WHERE state IN ('passed', 'failed', 'cancelled') AND COALESCE(log_path, '') != '' ORDER BY finished_at DESC, id DESC LIMIT -1 OFFSET ?", controller.config.LogRetention)
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

func (controller *Controller) execute(jobID int64, parent context.Context, task Task, output io.Writer, directory string) int {
	return controller.executeCommandWithEnv(parent, jobID, task.Run, task.Timeout, output, directory, task.Env)
}

func (controller *Controller) executeCommand(parent context.Context, jobID int64, argv []string, timeout int, output io.Writer, directory string) int {
	return controller.executeCommandWithEnv(parent, jobID, argv, timeout, output, directory, nil)
}

func (controller *Controller) executeCommandWithEnv(parent context.Context, jobID int64, argv []string, timeout int, output io.Writer, directory string, environment map[string]string) int {
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
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	command.Dir = directory
	command.Env = taskEnvironment(environment)
	command.Stdout = output
	command.Stderr = output
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		return syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	if err := command.Start(); err != nil {
		fmt.Fprintf(output, "BitCI could not start task: %v\n", err)
		return 127
	}
	pid := command.Process.Pid
	if _, err := controller.db.Exec("UPDATE jobs SET worker_pid = ? WHERE id = ? AND state = 'running'", pid, jobID); err != nil {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = command.Wait()
		fmt.Fprintf(output, "BitCI could not record task process: %v\n", err)
		return 127
	}
	err := command.Wait()
	if _, clearErr := controller.db.Exec("UPDATE jobs SET worker_pid = NULL WHERE id = ? AND worker_pid = ?", jobID, pid); clearErr != nil && err == nil {
		fmt.Fprintf(output, "BitCI could not clear task process: %v\n", clearErr)
		return 127
	}
	if err == nil {
		return 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return exitError.ExitCode()
	}
	fmt.Fprintf(output, "BitCI could not start task: %v\n", err)
	return 127
}

func taskEnvironment(overrides map[string]string) []string {
	if len(overrides) == 0 {
		return nil
	}
	values := map[string]string{}
	for _, value := range os.Environ() {
		name, value, ok := strings.Cut(value, "=")
		if ok {
			values[name] = value
		}
	}
	for name, value := range overrides {
		values[name] = value
	}
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
	result, err := controller.db.Exec(
		"UPDATE jobs SET state = ?, finished_at = ?, exit_code = ?, log_path = ?, cleanup_pending = ?, worker_pid = NULL WHERE id = ? AND state IN ('queued', 'running')",
		state, time.Now().UTC().Format(time.RFC3339), code, job.LogPath, cleanupPending, job.ID,
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
	_, err = controller.db.Exec("DELETE FROM leases WHERE job_id = ?", job.ID)
	return err
}

func (controller *Controller) Jobs() ([]Job, error) {
	rows, err := controller.db.Query("SELECT id, batch, task, ref, COALESCE(tested_sha, ''), state, exit_code, COALESCE(log_path, ''), worker_pid FROM jobs ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.Ref, &job.TestedSHA, &job.State, &job.ExitCode, &job.LogPath, &job.WorkerPID); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (controller *Controller) Cancel(id int64) (bool, error) {
	result, err := controller.db.Exec(
		"UPDATE jobs SET state = 'cancelled', finished_at = ? WHERE id = ? AND state = 'queued'",
		time.Now().UTC().Format(time.RFC3339), id,
	)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (controller *Controller) Retry(id int64) ([]Job, error) {
	var task, ref, configJSON, checkoutRoot, configRelative string
	if err := controller.db.QueryRow("SELECT task, ref, COALESCE(config_json, ''), COALESCE(checkout_root, ''), COALESCE(config_relative, '') FROM jobs WHERE id = ?", id).Scan(&task, &ref, &configJSON, &checkoutRoot, &configRelative); err != nil {
		return nil, err
	}
	config := controller.config
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
	}
	return controller.submit(config, []string{task}, ref, checkoutRoot, configRelative)
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

// ReadLog returns complete lines written after cursor. The cursor advances only
// after a newline, so a later call can safely read a line still being written.
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
	reader := bufio.NewReader(io.LimitReader(file, maxLogReadBytes))
	readCursor := cursor
	limit = logLimit(limit)
	for len(output.Lines) < limit {
		line, err := reader.ReadString('\n')
		readCursor += int64(len(line))
		if len(line) > 0 && strings.HasSuffix(line, "\n") {
			output.Lines = append(output.Lines, redactLogLine(strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), redact))
			output.Cursor = readCursor
		} else if len(line) > 0 && err == io.EOF && terminalJobState(state) && readCursor == info.Size() {
			output.Lines = append(output.Lines, redactLogLine(strings.TrimSuffix(line, "\r"), redact))
			output.Cursor = readCursor
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return LogCursorOutput{}, err
		}
	}
	return output, nil
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
	return append(redact, controller.config.Redact...), nil
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
	return controller.diskOK(controller.config.MinFreeBytes)
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
