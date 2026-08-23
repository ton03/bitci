package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
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
		return fmt.Errorf("use validate, plan, submit, worker, serve, status, or doctor")
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
