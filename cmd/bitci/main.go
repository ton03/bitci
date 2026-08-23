package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/ton03/bitci/internal/bitci"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bitci:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("use validate, plan, submit, worker, serve, status, cancel, retry, logs, or doctor")
	}
	command := args[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "bitci.json", "config file")
	stateDir := flags.String("state-dir", "", "controller state directory")
	paths := flags.String("paths", "", "comma-separated changed paths")
	ref := flags.String("ref", "", "source reference")
	maxWorkers := flags.Int("max-workers", 1, "maximum running tasks")
	interval := flags.Duration("interval", time.Second, "queue poll interval")
	jsonOutput := flags.Bool("json", false, "JSON output")
	logLimit := flags.Int("tail", 80, "maximum log lines, capped at 80")
	search := flags.String("search", "", "log text to search")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if command == "validate" {
		_, err := bitci.LoadConfig(*configPath)
		return err
	}
	controller, err := bitci.Open(*configPath, *stateDir)
	if err != nil {
		return err
	}
	defer controller.Close()
	switch command {
	case "plan":
		plan, err := controller.Plan(bitci.SplitPaths(*paths))
		if err != nil {
			return err
		}
		return printValue(plan, *jsonOutput)
	case "submit":
		if flags.NArg() == 0 {
			return fmt.Errorf("submit needs one or more task IDs")
		}
		jobs, err := controller.Submit(flags.Args(), *ref)
		if err != nil {
			return err
		}
		return printValue(jobs, *jsonOutput)
	case "worker":
		_, err := controller.RunOnce(context.Background(), *maxWorkers)
		return err
	case "serve":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return controller.Serve(ctx, *maxWorkers, *interval)
	case "status":
		jobs, err := controller.Jobs()
		if err != nil {
			return err
		}
		return printValue(jobs, *jsonOutput)
	case "cancel":
		id, err := jobID(flags.Args())
		if err != nil {
			return err
		}
		cancelled, err := controller.Cancel(id)
		if err != nil {
			return err
		}
		if !cancelled {
			return fmt.Errorf("job %d is not queued", id)
		}
		return printValue(map[string]any{"id": id, "state": "cancelled"}, *jsonOutput)
	case "retry":
		id, err := jobID(flags.Args())
		if err != nil {
			return err
		}
		jobs, err := controller.Retry(id)
		if err != nil {
			return err
		}
		return printValue(jobs, *jsonOutput)
	case "logs":
		id, err := jobID(flags.Args())
		if err != nil {
			return err
		}
		var lines []string
		if *search == "" {
			lines, err = controller.TailLog(id, *logLimit)
		} else {
			lines, err = controller.SearchLog(id, *search, *logLimit)
		}
		if err != nil {
			return err
		}
		return printValue(lines, *jsonOutput)
	case "doctor":
		if err := controller.DiskOK(); err != nil {
			return err
		}
		fmt.Println("config OK")
		fmt.Println("state OK:", filepath.Clean(*stateDir))
		return nil
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func jobID(args []string) (int64, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("command needs one job ID")
	}
	return strconv.ParseInt(args[0], 10, 64)
}

func printValue(value any, jsonOutput bool) error {
	if jsonOutput {
		encoded, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(encoded))
		return nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}
