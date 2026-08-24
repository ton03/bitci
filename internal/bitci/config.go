package bitci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Config struct {
	Version      int             `json:"version"`
	Resources    map[string]int  `json:"resources"`
	MinFreeBytes uint64          `json:"min_free_bytes"`
	Prepare      []string        `json:"prepare"`
	LogRetention int             `json:"log_retention"`
	Redact       []string        `json:"redact"`
	Tasks        map[string]Task `json:"tasks"`
}

type Task struct {
	Run        []string          `json:"run"`
	Env        map[string]string `json:"env"`
	Needs      []string          `json:"needs"`
	Resources  []string          `json:"resources"`
	Paths      []string          `json:"paths"`
	Timeout    int               `json:"timeout_seconds"`
	MaxRetries int               `json:"max_retries"`
}

func LoadConfig(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode %s: %w", filename, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("decode %s: extra JSON value", filename)
		}
		return Config{}, fmt.Errorf("decode %s: %w", filename, err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config Config) Validate() error {
	if config.Version != 1 {
		return fmt.Errorf("version must be 1")
	}
	if len(config.Tasks) == 0 {
		return fmt.Errorf("tasks must not be empty")
	}
	if len(config.Prepare) > 0 && config.Prepare[0] == "" {
		return fmt.Errorf("prepare needs a command argv")
	}
	if config.LogRetention < 0 {
		return fmt.Errorf("log_retention must not be negative")
	}
	for _, value := range config.Redact {
		if value == "" {
			return fmt.Errorf("redact values must not be empty")
		}
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("redact values must not contain line breaks")
		}
	}
	for name, limit := range config.Resources {
		if name == "" || limit < 1 {
			return fmt.Errorf("resource %q must have a positive limit", name)
		}
	}
	for name, task := range config.Tasks {
		if name == "" || len(task.Run) == 0 || task.Run[0] == "" {
			return fmt.Errorf("task %q needs a command argv", name)
		}
		if task.Timeout < 0 {
			return fmt.Errorf("task %q timeout_seconds must not be negative", name)
		}
		for variable, value := range task.Env {
			if !validEnvironmentName(variable) {
				return fmt.Errorf("task %q has invalid environment variable %q", name, variable)
			}
			if strings.ContainsRune(value, '\x00') {
				return fmt.Errorf("task %q environment variable %q contains NUL", name, variable)
			}
		}
		if task.MaxRetries < 0 {
			return fmt.Errorf("task %q max_retries must not be negative", name)
		}
		for _, need := range task.Needs {
			if _, ok := config.Tasks[need]; !ok {
				return fmt.Errorf("task %q needs unknown task %q", name, need)
			}
		}
		for _, resource := range task.Resources {
			if _, ok := config.Resources[resource]; !ok {
				return fmt.Errorf("task %q uses unknown resource %q", name, resource)
			}
		}
	}
	_, err := config.ordered(config.TaskNames())
	return err
}

func validEnvironmentName(value string) bool {
	if value == "" {
		return false
	}
	for index, character := range value {
		if character == '_' || 'A' <= character && character <= 'Z' || 'a' <= character && character <= 'z' || index > 0 && '0' <= character && character <= '9' {
			continue
		}
		return false
	}
	return true
}

func (config Config) TaskNames() []string {
	names := make([]string, 0, len(config.Tasks))
	for name := range config.Tasks {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (config Config) Ordered(names []string) ([]string, error) {
	return config.ordered(names)
}

func (config Config) ordered(names []string) ([]string, error) {
	seen := map[string]bool{}
	visiting := map[string]bool{}
	var ordered []string
	var visit func(string) error
	visit = func(name string) error {
		if seen[name] {
			return nil
		}
		if visiting[name] {
			return fmt.Errorf("task dependency cycle at %q", name)
		}
		task, ok := config.Tasks[name]
		if !ok {
			return fmt.Errorf("unknown task %q", name)
		}
		visiting[name] = true
		for _, need := range task.Needs {
			if err := visit(need); err != nil {
				return err
			}
		}
		visiting[name] = false
		seen[name] = true
		ordered = append(ordered, name)
		return nil
	}
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func (config Config) Plan(changedPaths []string) ([]string, error) {
	if len(changedPaths) == 0 {
		return config.Ordered(config.TaskNames())
	}
	var selected []string
	for _, name := range config.TaskNames() {
		task := config.Tasks[name]
		if len(task.Paths) == 0 || matchesAny(task.Paths, changedPaths) {
			selected = append(selected, name)
		}
	}
	return config.Ordered(selected)
}

func matchesAny(patterns, paths []string) bool {
	for _, pattern := range patterns {
		for _, candidate := range paths {
			candidate = filepath.ToSlash(candidate)
			if pattern == "**" || pattern == "*" || pattern == candidate {
				return true
			}
			if strings.HasSuffix(pattern, "/**") && strings.HasPrefix(candidate, strings.TrimSuffix(pattern, "**")) {
				return true
			}
			if matched, _ := filepath.Match(pattern, candidate); matched {
				return true
			}
		}
	}
	return false
}
