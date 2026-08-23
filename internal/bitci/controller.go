package bitci

import (
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
	if stateDir == "" {
		stateDir = filepath.Join(filepath.Dir(configPath), ".bitci")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", filepath.Join(stateDir, "bitci.db"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	controller := &Controller{config: config, configPath: configPath, stateDir: stateDir, db: db}
	if err := controller.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return controller, nil
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

func (controller *Controller) Serve(ctx context.Context, maxWorkers int, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		for worker := 0; worker < maxWorkers; worker++ {
			go controller.RunOnce(ctx, maxWorkers) //nolint:errcheck // job state captures execution errors.
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
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
