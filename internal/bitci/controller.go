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
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

type Controller struct {
	config     Config
	configPath string
	stateDir   string
	db         *sql.DB
	githubAPI  string
	githubRepo string
}

type Job struct {
	ID         int64  `json:"id"`
	Batch      string `json:"batch"`
	Task       string `json:"task"`
	Ref        string `json:"ref"`
	TestedSHA  string `json:"tested_sha,omitempty"`
	State      string `json:"state"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	LogPath    string `json:"log_path,omitempty"`
	configJSON string
}

func Open(configPath, stateDir string) (*Controller, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	controller, err := OpenState(configPath, stateDir)
	if err != nil {
		return nil, err
	}
	controller.config = config
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
			config_json TEXT
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
	return controller.addJobColumn("config_json", "TEXT")
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
	ordered, err := controller.config.Ordered(taskNames)
	if err != nil {
		return nil, err
	}
	if sha, err := controller.checkoutSHA(); err == nil {
		ref = sha
	}
	batch, err := newBatch()
	if err != nil {
		return nil, err
	}
	transaction, err := controller.db.Begin()
	if err != nil {
		return nil, err
	}
	defer transaction.Rollback()
	jobs := make([]Job, 0, len(ordered))
	configJSON, err := json.Marshal(controller.config)
	if err != nil {
		return nil, err
	}
	for _, taskName := range ordered {
		result, err := transaction.Exec(
			"INSERT INTO jobs(batch, task, ref, config_json, state, created_at) VALUES (?, ?, ?, ?, 'queued', ?)",
			batch, taskName, ref, configJSON, time.Now().UTC().Format(time.RFC3339),
		)
		if err != nil {
			return nil, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, Job{ID: id, Batch: batch, Task: taskName, Ref: ref, State: "queued"})
	}
	if err := transaction.Commit(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (controller *Controller) RunOnce(ctx context.Context, maxWorkers int) (bool, error) {
	if ctx.Err() != nil {
		return false, nil
	}
	if err := controller.DiskOK(); err != nil {
		return false, err
	}
	job, claimed, err := controller.claim(maxWorkers)
	if err != nil || !claimed {
		return false, err
	}
	config, task, err := controller.jobConfig(job)
	if err != nil {
		return true, controller.finish(job, 127)
	}
	logFile, err := controller.startLog(&job)
	if err != nil {
		return true, controller.finish(job, 127)
	}
	workDir := filepath.Dir(controller.configPath)
	cleanup := func() error { return nil }
	if isCheckoutSHA(job.Ref) {
		var err error
		workDir, cleanup, err = controller.jobCheckout(ctx, job)
		if err != nil {
			fmt.Fprintln(logFile, "BitCI could not stage recorded checkout SHA:", err)
			if cleanupErr := cleanup(); cleanupErr != nil {
				fmt.Fprintln(logFile, "BitCI could not remove job worktree:", cleanupErr)
			}
			logFile.Close()
			return true, controller.finish(job, 126)
		}
	}
	code := 0
	if isCheckoutSHA(job.Ref) && len(config.Prepare) > 0 && controller.isCheckoutExecutable(config.Prepare[0]) {
		fmt.Fprintln(logFile, "BitCI refuses a checkout-local absolute prepare executable for a recorded SHA job")
		code = 126
	} else if isCheckoutSHA(job.Ref) && controller.isCheckoutExecutable(task.Run[0]) {
		fmt.Fprintln(logFile, "BitCI refuses a checkout-local absolute task executable for a recorded SHA job")
		code = 126
	} else {
		if isCheckoutSHA(job.Ref) && len(config.Prepare) > 0 {
			code = controller.executeCommand(ctx, config.Prepare, task.Timeout, logFile, workDir)
		}
		if code == 0 {
			code = controller.execute(ctx, task, logFile, workDir)
		}
	}
	if err := cleanup(); err != nil {
		fmt.Fprintln(logFile, "BitCI could not remove job worktree:", err)
		if code == 0 {
			code = 125
		}
	}
	if err := logFile.Close(); err != nil && code == 0 {
		code = 127
	}
	return true, controller.finish(job, code)
}

func (controller *Controller) checkoutSHA() (string, error) {
	return checkoutSHA(filepath.Dir(controller.configPath))
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
	cleanup = func() error {
		var failures []error
		if _, err := os.Lstat(path); err == nil {
			if _, err := controller.git(context.Background(), "worktree", "remove", "--force", path); err != nil {
				failures = append(failures, err)
			}
		} else if !os.IsNotExist(err) {
			failures = append(failures, err)
		}
		if err := os.RemoveAll(path); err != nil {
			failures = append(failures, err)
		}
		if _, err := controller.git(context.Background(), "worktree", "prune"); err != nil {
			failures = append(failures, err)
		}
		return errors.Join(failures...)
	}
	if _, err := controller.git(ctx, "worktree", "add", "--detach", path, job.Ref); err != nil {
		return "", cleanup, fmt.Errorf("create job worktree: %w", err)
	}
	sha, err := checkoutSHA(path)
	if err != nil || sha != job.Ref {
		return "", cleanup, fmt.Errorf("verify job worktree SHA")
	}
	if _, err := controller.db.Exec("UPDATE jobs SET tested_sha = ? WHERE id = ?", sha, job.ID); err != nil {
		return "", cleanup, err
	}
	checkout, err := controller.git(ctx, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", cleanup, fmt.Errorf("find checkout root: %w", err)
	}
	checkoutRoot, err := filepath.EvalSymlinks(strings.TrimSpace(checkout))
	if err != nil {
		return "", cleanup, fmt.Errorf("resolve checkout root: %w", err)
	}
	configDirectory, err := filepath.EvalSymlinks(filepath.Dir(controller.configPath))
	if err != nil {
		return "", cleanup, fmt.Errorf("resolve config directory: %w", err)
	}
	relative, err := filepath.Rel(checkoutRoot, configDirectory)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", cleanup, fmt.Errorf("config must be inside its Git checkout")
	}
	return filepath.Join(path, relative), cleanup, nil
}

func isCheckoutSHA(value string) bool {
	if len(value) != 40 {
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
	if err := controller.RecoverInterrupted(); err != nil {
		return err
	}
	listener, err := controller.Listen(socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	if interval <= 0 {
		interval = time.Second
	}
	errors := make(chan error, maxWorkers+1)
	go func() { errors <- controller.ServeRPC(ctx, listener) }()
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
	rows, err := transaction.Query("SELECT id, ref FROM jobs WHERE state = 'running'")
	if err != nil {
		return err
	}
	var interrupted []int64
	for rows.Next() {
		var id int64
		var ref string
		if err := rows.Scan(&id, &ref); err != nil {
			rows.Close()
			return err
		}
		if isCheckoutSHA(ref) {
			interrupted = append(interrupted, id)
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
	if err := transaction.Commit(); err != nil {
		return err
	}
	var failures []error
	for _, id := range interrupted {
		if err := controller.removeJobWorktree(id); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (controller *Controller) removeJobWorktree(id int64) error {
	path := filepath.Join(controller.stateDir, "worktrees", fmt.Sprintf("job-%d", id))
	var failures []error
	if _, err := os.Lstat(path); err == nil {
		if _, err := controller.git(context.Background(), "worktree", "remove", "--force", path); err != nil {
			failures = append(failures, err)
		}
	} else if !os.IsNotExist(err) {
		failures = append(failures, err)
	}
	if err := os.RemoveAll(path); err != nil {
		failures = append(failures, err)
	}
	if _, err := controller.git(context.Background(), "worktree", "prune"); err != nil {
		failures = append(failures, err)
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
	rows, err := transaction.Query("SELECT id, batch, task, ref, COALESCE(config_json, ''), state FROM jobs WHERE state = 'queued' ORDER BY id")
	if err != nil {
		return Job{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.Ref, &job.configJSON, &job.State); err != nil {
			return Job{}, false, err
		}
		config, task, err := controller.jobConfig(job)
		if err != nil {
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
	for _, resource := range task.Resources {
		var held int
		if err := transaction.QueryRow("SELECT COUNT(*) FROM leases WHERE resource = ?", resource).Scan(&held); err != nil {
			return false, err
		}
		if held >= config.Resources[resource] {
			return false, nil
		}
	}
	return true, nil
}

func (controller *Controller) jobConfig(job Job) (Config, Task, error) {
	var config Config
	if job.configJSON == "" {
		config = controller.config
	} else {
		if err := json.Unmarshal([]byte(job.configJSON), &config); err != nil {
			return Config{}, Task{}, fmt.Errorf("decode queued job configuration: %w", err)
		}
		if err := config.Validate(); err != nil {
			return Config{}, Task{}, fmt.Errorf("validate queued job configuration: %w", err)
		}
	}
	task, ok := config.Tasks[job.Task]
	if !ok {
		return Config{}, Task{}, fmt.Errorf("queued job references unknown task %q", job.Task)
	}
	return config, task, nil
}

func (controller *Controller) isCheckoutExecutable(command string) bool {
	if !filepath.IsAbs(command) {
		return false
	}
	checkout, err := controller.git(context.Background(), "rev-parse", "--show-toplevel")
	if err != nil {
		return false
	}
	root, err := filepath.EvalSymlinks(strings.TrimSpace(checkout))
	if err != nil {
		return false
	}
	path, err := filepath.EvalSymlinks(command)
	if err != nil {
		path = command
	}
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func (controller *Controller) startLog(job *Job) (*os.File, error) {
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

func (controller *Controller) execute(parent context.Context, task Task, output io.Writer, directory string) int {
	return controller.executeCommand(parent, task.Run, task.Timeout, output, directory)
}

func (controller *Controller) executeCommand(parent context.Context, argv []string, timeout int, output io.Writer, directory string) int {
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
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	if err == nil {
		return 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return exitError.ExitCode()
	}
	fmt.Fprintf(output, "BitCI could not start task: %v\n", err)
	return 127
}

func (controller *Controller) finish(job Job, code int) error {
	state := "passed"
	if code != 0 {
		state = "failed"
	}
	_, err := controller.db.Exec(
		"UPDATE jobs SET state = ?, finished_at = ?, exit_code = ?, log_path = ? WHERE id = ?",
		state, time.Now().UTC().Format(time.RFC3339), code, job.LogPath, job.ID,
	)
	if err != nil {
		return err
	}
	_, err = controller.db.Exec("DELETE FROM leases WHERE job_id = ?", job.ID)
	return err
}

func (controller *Controller) Jobs() ([]Job, error) {
	rows, err := controller.db.Query("SELECT id, batch, task, ref, COALESCE(tested_sha, ''), state, exit_code, COALESCE(log_path, '') FROM jobs ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.Ref, &job.TestedSHA, &job.State, &job.ExitCode, &job.LogPath); err != nil {
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
	var task, ref string
	if err := controller.db.QueryRow("SELECT task, ref FROM jobs WHERE id = ?", id).Scan(&task, &ref); err != nil {
		return nil, err
	}
	return controller.Submit([]string{task}, ref)
}

func (controller *Controller) TailLog(id int64, limit int) ([]string, error) {
	file, err := controller.logFile(id)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return scanLog(file, limit, func(string) bool { return true })
}

func (controller *Controller) SearchLog(id int64, query string, limit int) ([]string, error) {
	if query == "" {
		return nil, fmt.Errorf("search query must not be empty")
	}
	file, err := controller.logFile(id)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return scanLog(file, limit, func(line string) bool { return strings.Contains(line, query) })
}

func (controller *Controller) logFile(id int64) (*os.File, error) {
	var logPath string
	if err := controller.db.QueryRow("SELECT COALESCE(log_path, '') FROM jobs WHERE id = ?", id).Scan(&logPath); err != nil {
		return nil, err
	}
	if logPath == "" {
		return nil, fmt.Errorf("job %d has no log", id)
	}
	file, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func scanLog(file *os.File, limit int, include func(string) bool) ([]string, error) {
	limit = logLimit(limit)
	lines := make([]string, 0, limit)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !include(line) {
			continue
		}
		if len(lines) == limit {
			copy(lines, lines[1:])
			lines[len(lines)-1] = line
			continue
		}
		lines = append(lines, line)
	}
	return lines, scanner.Err()
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
	if controller.config.MinFreeBytes == 0 {
		return nil
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(controller.stateDir, &stat); err != nil {
		return err
	}
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	if free < controller.config.MinFreeBytes {
		return fmt.Errorf("disk guard: %d bytes free, need %d", free, controller.config.MinFreeBytes)
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
