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
		return fmt.Errorf("use version, validate, plan, submit, worker, serve, start, stop, service, status, cancel, retry, recover, logs, doctor, mcp, or stage-pr")
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
	socketPath := flags.String("socket", "", "owner Unix socket path")
	httpAddress := flags.String("http", "", "loopback dashboard address")
	allowRuns := flags.Bool("allow-runs", false, "enable MCP run-control tools")
	jsonOutput := flags.Bool("json", false, "JSON output")
	logLimit := flags.Int("tail", 80, "maximum log lines, capped at 80")
	logCursor := flags.Int64("cursor", -1, "read log lines after this byte cursor")
	search := flags.String("search", "", "log text to search")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if command == "version" {
		if flags.NArg() != 0 {
			return fmt.Errorf("version takes no arguments")
		}
		fmt.Println(bitci.Version)
		return nil
	}
	if command == "validate" {
		_, err := bitci.LoadConfig(*configPath)
		return err
	}
	if command == "service" {
		return runService(flags.Args(), *configPath, *stateDir, *maxWorkers, *httpAddress)
	}
	if command == "start" || command == "stop" {
		if flags.NArg() != 0 {
			return fmt.Errorf("%s takes no arguments", command)
		}
		return runService([]string{command}, *configPath, *stateDir, *maxWorkers, *httpAddress)
	}
	if command == "mcp" {
		if flags.NArg() != 0 {
			return fmt.Errorf("mcp takes no arguments")
		}
		if *socketPath == "" {
			*socketPath = bitci.DefaultSocketPath(*configPath, *stateDir)
		}
		return bitci.RunMCP(context.Background(), bitci.MCPOptions{SocketPath: *socketPath, AllowRuns: *allowRuns})
	}
	var controller *bitci.Controller
	var err error
	if command == "status" {
		controller, err = bitci.OpenState(*configPath, *stateDir)
	} else {
		controller, err = bitci.Open(*configPath, *stateDir)
	}
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
	case "stage-pr":
		number, err := pullRequestNumber(flags.Args())
		if err != nil {
			return err
		}
		stage, err := controller.StagePR(context.Background(), number, os.Getenv("BITCI_GITHUB_TOKEN"))
		if err != nil {
			return err
		}
		return printValue(stage, *jsonOutput)
	case "worker":
		_, err := controller.RunOnce(context.Background(), *maxWorkers)
		return err
	case "serve":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return controller.Serve(ctx, *maxWorkers, *interval, *socketPath, *httpAddress)
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
	case "recover":
		id, err := jobID(flags.Args())
		if err != nil {
			return err
		}
		recovered, err := controller.RecoverJob(id)
		if err != nil {
			return err
		}
		return printValue(map[string]any{"id": id, "state": "failed", "recovered": recovered}, *jsonOutput)
	case "logs":
		id, err := jobID(flags.Args())
		if err != nil {
			return err
		}
		if *logCursor < -1 {
			return fmt.Errorf("log cursor must not be less than -1")
		}
		if *logCursor >= 0 {
			if *search != "" {
				return fmt.Errorf("logs --cursor cannot combine with --search")
			}
			output, err := controller.ReadLog(id, *logCursor, *logLimit)
			if err != nil {
				return err
			}
			return printValue(output, *jsonOutput)
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

func runService(args []string, configPath, stateDir string, maxWorkers int, httpAddress string) error {
	if len(args) != 1 {
		return fmt.Errorf("service needs install, start, stop, status, or uninstall")
	}
	if args[0] == "stop" {
		service, err := bitci.NewServiceForStop(configPath, stateDir)
		if err != nil {
			return err
		}
		if err := service.Stop(); err != nil {
			return err
		}
		fmt.Println("stopped", service.Label)
		return nil
	}
	if args[0] == "start" {
		service, err := bitci.NewServiceForStart(configPath, stateDir, maxWorkers, httpAddress)
		if err != nil {
			return err
		}
		started, err := service.Start()
		if err != nil {
			return err
		}
		if started {
			fmt.Println("started", service.Label)
		} else {
			fmt.Println("already running", service.Label)
		}
		return nil
	}
	service, err := bitci.NewServiceWithHTTP(configPath, stateDir, maxWorkers, httpAddress)
	if err != nil {
		return err
	}
	switch args[0] {
	case "install":
		if err := service.Install(); err != nil {
			return err
		}
		fmt.Println("installed", service.Label)
		return nil
	case "status":
		output, err := service.Status()
		if err != nil {
			return err
		}
		fmt.Print(output)
		return nil
	case "uninstall":
		if err := service.Uninstall(); err != nil {
			return err
		}
		fmt.Println("uninstalled", service.Label)
		return nil
	default:
		return fmt.Errorf("unknown service command %q", args[0])
	}
}

func jobID(args []string) (int64, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("command needs one job ID")
	}
	return strconv.ParseInt(args[0], 10, 64)
}

func pullRequestNumber(args []string) (int, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("stage-pr needs one pull request number")
	}
	number, err := strconv.Atoi(args[0])
	if err != nil || number < 1 {
		return 0, fmt.Errorf("stage-pr needs a positive pull request number")
	}
	return number, nil
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
