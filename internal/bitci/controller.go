package bitci

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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
}

type Job struct {
	ID       int64  `json:"id"`
	Batch    string `json:"batch"`
	Task     string `json:"task"`
	Ref      string `json:"ref"`
	State    string `json:"state"`
	ExitCode *int   `json:"exit_code,omitempty"`
	LogPath  string `json:"log_path,omitempty"`
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
	stateDir = DefaultStateDir(configPath, stateDir)
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
	if stateDir == "" {
		return filepath.Join(filepath.Dir(configPath), ".bitci")
	}
	return stateDir
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
			log_path TEXT
		);
		CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(state, id);
		CREATE TABLE IF NOT EXISTS leases (
			resource TEXT NOT NULL,
			job_id INTEGER NOT NULL REFERENCES jobs(id),
			PRIMARY KEY (resource, job_id)
		);
	`)
	return err
}

func (controller *Controller) Submit(taskNames []string, ref string) ([]Job, error) {
	ordered, err := controller.config.Ordered(taskNames)
	if err != nil {
		return nil, err
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
	for _, taskName := range ordered {
		result, err := transaction.Exec(
			"INSERT INTO jobs(batch, task, ref, state, created_at) VALUES (?, ?, ?, 'queued', ?)",
			batch, taskName, ref, time.Now().UTC().Format(time.RFC3339),
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
	task := controller.config.Tasks[job.Task]
	job.LogPath = filepath.Join(controller.stateDir, "logs", fmt.Sprintf("job-%d.log", job.ID))
	if err := os.MkdirAll(filepath.Dir(job.LogPath), 0o700); err != nil {
		return true, controller.finish(job, 127, err.Error())
	}
	code, output := controller.execute(ctx, task)
	return true, controller.finish(job, code, output)
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
	return transaction.Commit()
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
	rows, err := transaction.Query("SELECT id, batch, task, ref, state FROM jobs WHERE state = 'queued' ORDER BY id")
	if err != nil {
		return Job{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.Ref, &job.State); err != nil {
			return Job{}, false, err
		}
		if ready, err := controller.ready(transaction, job); err != nil || !ready {
			if err != nil {
				return Job{}, false, err
			}
			continue
		}
		if free, err := controller.resourcesFree(transaction, job); err != nil || !free {
			if err != nil {
				return Job{}, false, err
			}
			continue
		}
		if _, err := transaction.Exec("UPDATE jobs SET state = 'running', started_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339), job.ID); err != nil {
			return Job{}, false, err
		}
		for _, resource := range controller.config.Tasks[job.Task].Resources {
			if _, err := transaction.Exec("INSERT INTO leases(resource, job_id) VALUES (?, ?)", resource, job.ID); err != nil {
				return Job{}, false, err
			}
		}
		job.State = "running"
		return job, true, transaction.Commit()
	}
	return Job{}, false, rows.Err()
}

func (controller *Controller) ready(transaction *sql.Tx, job Job) (bool, error) {
	for _, need := range controller.config.Tasks[job.Task].Needs {
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

func (controller *Controller) resourcesFree(transaction *sql.Tx, job Job) (bool, error) {
	for _, resource := range controller.config.Tasks[job.Task].Resources {
		var held int
		if err := transaction.QueryRow("SELECT COUNT(*) FROM leases WHERE resource = ?", resource).Scan(&held); err != nil {
			return false, err
		}
		if held >= controller.config.Resources[resource] {
			return false, nil
		}
	}
	return true, nil
}

func (controller *Controller) execute(parent context.Context, task Task) (int, string) {
	ctx := parent
	var cancel context.CancelFunc
	if task.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, time.Duration(task.Timeout)*time.Second)
		defer cancel()
	}
	command := exec.CommandContext(ctx, task.Run[0], task.Run[1:]...)
	command.Dir = filepath.Dir(controller.configPath)
	output, err := command.CombinedOutput()
	if err == nil {
		return 0, string(output)
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return exitError.ExitCode(), string(output)
	}
	return 127, fmt.Sprintf("BitCI could not start task: %v\n%s", err, output)
}

func (controller *Controller) finish(job Job, code int, output string) error {
	if err := os.WriteFile(job.LogPath, []byte(output), 0o600); err != nil {
		return err
	}
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
	rows, err := controller.db.Query("SELECT id, batch, task, ref, state, exit_code, COALESCE(log_path, '') FROM jobs ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Batch, &job.Task, &job.Ref, &job.State, &job.ExitCode, &job.LogPath); err != nil {
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
